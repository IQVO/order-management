//go:build integration

package kafkapathcapacity

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestNewConsumer_TwoInstancesInARow_BothReplayFully is the real
// regression test for the shared-consumer-group bug: a second process
// must independently replay the full topic history rather than resume
// from a previous process's committed offset. The test owns its broker,
// so CI executes it rather than silently skipping when no external
// Kafka is available.
func TestNewConsumer_TwoInstancesInARow_BothReplayFully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-pathcapacity-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.itest-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value: []byte(fmt.Sprintf(`{"event_type":"PathCapacityChanged","data":{"path_id":"pick","cutoff_at":%q,"remaining_units":25,"known":true}}`, cutoff.Format(time.RFC3339))),
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	for instance := 0; instance < 2; instance++ {
		consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
		if err != nil {
			t.Fatalf("NewConsumerForTopic instance %d: %v", instance, err)
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- consumer.Run(runCtx) }()
		if err := consumer.WaitReady(ctx); err != nil {
			stop()
			t.Fatalf("consumer instance %d never became ready: %v", instance, err)
		}
		units, known := consumer.Remaining(shared.PathId("pick"), "sp1-1800", cutoff)
		if !known || units != 25 {
			stop()
			t.Fatalf("consumer instance %d did not replay the capacity event: units=%d known=%v", instance, units, known)
		}
		stop()
		if err := <-done; err != nil {
			t.Fatalf("consumer instance %d stopped with error: %v", instance, err)
		}
		if err := consumer.Close(); err != nil {
			t.Fatalf("close consumer instance %d: %v", instance, err)
		}
	}
}

// TestNewConsumer_AlreadyCaughtUpGroup_ReadyImmediately is the
// regression test for the "readiness never fires because nothing new
// arrives" bug.
func TestNewConsumer_AlreadyCaughtUpGroup_ReadyImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-pathcapacity-itest2"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.itest2-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	cutoff := time.Date(2026, 9, 13, 8, 0, 0, 0, time.UTC)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value: []byte(fmt.Sprintf(`{"event_type":"PathCapacityChanged","data":{"path_id":"singles","cutoff_at":%q,"remaining_units":0,"known":false}}`, cutoff.Format(time.RFC3339))),
	}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	defer func() { _ = consumer.Close() }()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = consumer.Run(runCtx) }()

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	if err := consumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("consumer never became ready: %v", err)
	}
	// The pre-existing event itself reported known=false (e.g. a
	// FlowFed path) -- Remaining must faithfully report known=false,
	// proving replay happened without fabricating a figure.
	if _, known := consumer.Remaining(shared.PathId("singles"), "sp1-0800", cutoff); known {
		t.Fatal("expected known=false: the replayed event itself reported known=false")
	}
}

// TestPromisePolicy_EndToEnd_RealCapacityConsumer proves the full path:
// a real PathCapacityChanged message decoded by this consumer correctly
// constrains (and does not constrain) a real order.PromisePolicy.Promise
// decision, satisfying order.CapacitySource directly (no adapter glue).
func TestPromisePolicy_EndToEnd_RealCapacityConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-pathcapacity-e2e"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.e2e-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	now := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)
	earlyCutoff := now.Add(4 * time.Hour)
	lateCutoff := now.Add(8 * time.Hour)

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Value: []byte(fmt.Sprintf(
			`{"event_type":"PathCapacityChanged","data":{"path_id":"pick","cutoff_at":%q,"remaining_units":10,"known":true}}`,
			earlyCutoff.Format(time.RFC3339)))},
		kafkago.Message{Value: []byte(fmt.Sprintf(
			`{"event_type":"PathCapacityChanged","data":{"path_id":"pick","cutoff_at":%q,"remaining_units":100,"known":true}}`,
			lateCutoff.Format(time.RFC3339)))},
	); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	defer func() { _ = consumer.Close() }()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = consumer.Run(runCtx) }()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := consumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("consumer never became ready: %v", err)
	}

	earlyWindow := order.CPTWindow{CptId: "early", CutoffAt: earlyCutoff, EligiblePathIds: []string{"pick"}}
	lateWindow := order.CPTWindow{CptId: "late", CutoffAt: lateCutoff, EligiblePathIds: []string{"pick"}}

	policy := order.PromisePolicy{
		Schedule: fakeScheduleFor(earlyWindow, lateWindow),
		Capability: fakeCapabilityFor(map[shared.PathId]time.Duration{
			"pick": 1 * time.Hour,
		}),
		Capacity: consumer, // the REAL Kafka-fed adapter, satisfying order.CapacitySource directly
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	// 50 units requested: the early window only has 10 remaining
	// (insufficient), so the policy must skip it and land on the late
	// window, which has enough. This proves capacity CORRECTLY
	// constrains the decision when known=true and insufficient.
	o := newAllocatedOrderForCapacityTest(t, "pick", 50)
	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.CptId != "late" {
		t.Fatalf("CptId = %q, want %q (early window lacked capacity per the real Kafka-fed adapter)", got.CptId, "late")
	}

	// A path/cutoff this consumer never observed reports known=false,
	// which PromisePolicy treats as "not a constraint" -- proving
	// capacity correctly does NOT constrain an unobserved window.
	neverObservedWindow := order.CPTWindow{CptId: "never-observed", CutoffAt: now.Add(2 * time.Hour), EligiblePathIds: []string{"pick"}}
	policy2 := order.PromisePolicy{
		Schedule:   fakeScheduleFor(neverObservedWindow),
		Capability: fakeCapabilityFor(map[shared.PathId]time.Duration{"pick": 1 * time.Hour}),
		Capacity:   consumer,
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}
	o2 := newAllocatedOrderForCapacityTest(t, "pick", 999999) // absurd quantity -- would fail if capacity were mistakenly "known"
	got2, ok2 := policy2.Promise(now, o2)
	if !ok2 {
		t.Fatal("expected ok=true")
	}
	if got2.Basis != order.BasisCapability || got2.CptId != "never-observed" {
		t.Fatalf("got = %+v, want a Capability-basis promise against the unobserved window (capacity unknown must not block it)", got2)
	}
}

// --- small local test doubles + helpers for the end-to-end assertion ---

type fakeSchedule struct{ windows []order.CPTWindow }

func fakeScheduleFor(windows ...order.CPTWindow) *fakeSchedule {
	return &fakeSchedule{windows: windows}
}

func (f *fakeSchedule) NextCutoffs(_ string, _ time.Time, n int) ([]order.CPTWindow, bool) {
	w := f.windows
	if len(w) > n {
		w = w[:n]
	}
	return w, true
}

type fakeCapability struct {
	cycleTimes map[shared.PathId]time.Duration
}

func fakeCapabilityFor(m map[shared.PathId]time.Duration) *fakeCapability {
	return &fakeCapability{cycleTimes: m}
}

func (f *fakeCapability) CycleTimeP95(pathID shared.PathId) (time.Duration, bool) {
	d, ok := f.cycleTimes[pathID]
	return d, ok
}

func newAllocatedOrderForCapacityTest(t *testing.T, path shared.PathId, qty int) *order.Order {
	t.Helper()
	line := order.RehydrateOrderLine(1, "SKU-1", qty, path, false, order.LineAllocated, nil)
	return order.Rehydrate("ord-1", []*order.OrderLine{line}, true, nil, nil, nil)
}

func createTopic(ctx context.Context, brokers []string, topic string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial Kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("Kafka controller: %w", err)
	}
	controllerConn, err := kafkago.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial Kafka controller: %w", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		return fmt.Errorf("create Kafka topic: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Kafka topic %q leader was not ready: %w", topic, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestNewConsumer_DualShapeSameEventId_ConvergesToOneConsistentEntry is
// the ADR-0021 Phase 2 Task 2b dual-write-safety regression: during the
// eventual dual-write bake period, ONE domain PathCapacityChanged event
// produces TWO physical Kafka messages on the same topic/key (same
// event id) -- a flat-shaped one and a CloudEvents-shaped one, carrying
// byte-identical `data`. This consumer is a full-replay LOCAL-CACHE
// consumer (see the fleet skill's "idempotent by construction" note),
// not a processed_events-gated use case: it has no dedup ledger, so
// "fires exactly once" here means the two physical messages -- decoded
// via two different wire shapes -- must converge on exactly the SAME
// final cache entry rather than corrupt or duplicate state. That is
// what this test proves: after both messages are consumed, exactly one
// logical (path, cutoff) entry exists and its value is correct,
// regardless of which physical message the reader saw first.
func TestNewConsumer_DualShapeSameEventId_ConvergesToOneConsistentEntry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-pathcapacity-dualshape"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.work-planning.events.dualshape-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	cutoff := time.Date(2026, 9, 26, 2, 0, 0, 0, time.UTC)
	eventID := "5c7d3f92-1b64-4a08-9e73-2f6a8c1d5b40" // same id/event_id on both physical messages
	flatValue := []byte(fmt.Sprintf(
		`{"event_id":%q,"event_type":"PathCapacityChanged","occurred_at":"2026-09-26T02:00:00Z","source":"wes-work-planning","data":{"path_id":"pick-to-tote","cutoff_at":%q,"remaining_units":17,"known":true}}`,
		eventID, cutoff.Format(time.RFC3339)))
	cloudEventsValue := []byte(fmt.Sprintf(
		`{"specversion":"1.0","id":%q,"type":"com.warehouse.wes.work-planning.workpool.PathCapacityChanged","source":"/warehouse/wes-work-planning","subject":"pick-to-tote","time":"2026-09-26T02:00:00Z","datacontenttype":"application/json","data":{"path_id":"pick-to-tote","cutoff_at":%q,"remaining_units":17,"known":true}}`,
		eventID, cutoff.Format(time.RFC3339)))

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte(eventID), Value: flatValue},
		kafkago.Message{Key: []byte(eventID), Value: cloudEventsValue},
	); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	defer func() { _ = consumer.Close() }()
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = consumer.Run(runCtx) }()

	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
	defer waitCancel()
	if err := consumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("consumer never became ready: %v", err)
	}

	units, known := consumer.Remaining(shared.PathId("pick-to-tote"), "sp1-0200", cutoff)
	if !known {
		t.Fatal("expected known=true after consuming both physical shapes of the same event")
	}
	if units != 17 {
		t.Fatalf("units = %d, want 17 -- both physical messages carry byte-identical data, so the converged entry must match", units)
	}

	c := consumer
	c.mu.RLock()
	entryCount := len(c.entries)
	c.mu.RUnlock()
	if entryCount != 1 {
		t.Fatalf("cache entries = %d, want exactly 1 -- two physical messages for the same logical event must converge, not duplicate", entryCount)
	}
}
