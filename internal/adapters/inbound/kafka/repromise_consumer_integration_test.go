//go:build integration

package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// capturingPublisher records every event RepromiseOrder publishes, so
// the test can assert the exact OrderRepromised fact a real broker
// message drove — end to end, no fake reader.
type capturingPublisher struct {
	events []shared.DomainEvent
}

func (p *capturingPublisher) Publish(_ context.Context, e shared.DomainEvent) error {
	p.events = append(p.events, e)
	return nil
}

var _ ports.EventPublisher = (*capturingPublisher)(nil)

// TestRepromiseConsumer_RealTaskCPTMissedMessage_DrivesRepromiseOrder
// proves the full ADR 0014 §5 / ADR 0018 wire path end to end: a real
// TaskCPTMissed message, published to a real Kafka broker in
// fulfillment-execution's exact shipped envelope/payload shape (ADR
// 0025 §7), with an order_ref that is a real WorkUnitId-shaped
// reference, drives a real RepromiseOrder.Execute call that recomputes
// the promise, saves the new group breakdown, and publishes a real
// OrderRepromised event.
func TestRepromiseConsumer_RealTaskCPTMissedMessage_DrivesRepromiseOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.fulfillment.events.itest-%d", time.Now().UnixNano())
	createRepromiseTopic(t, ctx, brokers, topic)

	// Seed a real order, allocated+released on line 1, with a real
	// persisted PromiseGroup breakdown at a LeadTime-basis cutoff.
	orders := memory.NewOrderRepo()
	l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	orderID := shared.OrderId(fmt.Sprintf("ord-itest-%d", time.Now().UnixNano()))
	o, err := order.New(orderID, []*order.OrderLine{l}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := o.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	originalCutoff := now.Add(24 * time.Hour)
	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: order.Promise{CutoffAt: originalCutoff, Basis: order.BasisLeadTime}},
	})
	if err := orders.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A DIFFERENT lead-time policy than whatever produced the seeded
	// promise: a real, discriminating "the promise moved" input.
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	events := &capturingPublisher{}
	processed := memory.NewRepromiseProcessedEventsRepo()
	clock := memory.NewFixedClock(now)
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: orders, Promise: movedPromise, Events: events, Clock: clock, Processed: processed,
	}

	consumer := inboundkafka.NewRepromiseConsumerForTopic(
		brokers,
		fmt.Sprintf("order-management-repromise-integration-test-%d", time.Now().UnixNano()),
		topic,
		repromiseOrder,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	orderRef := usecases.WorkUnitID(orderID, 1)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(fmt.Sprintf("evt-%d", time.Now().UnixNano())),
		Value: mustTaskCPTMissedEnvelopeJSON(t, orderRef),
	}); err != nil {
		t.Fatalf("publish TaskCPTMissed: %v", err)
	}

	waitForRepromise(t, ctx, orders, orderID, originalCutoff)
	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	if len(events.events) != 1 {
		t.Fatalf("published events = %d, want 1 OrderRepromised", len(events.events))
	}
	repromised, ok := events.events[0].(shared.OrderRepromised)
	if !ok {
		t.Fatalf("published event = %T, want shared.OrderRepromised", events.events[0])
	}
	if repromised.OrderID != orderID {
		t.Errorf("OrderRepromised.OrderID = %q, want %q", repromised.OrderID, orderID)
	}
	if repromised.Reason != "TaskCPTMissed" {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, "TaskCPTMissed")
	}
}

// mustTaskCPTMissedEnvelopeJSON builds fulfillment-execution's real,
// shipped TaskCPTMissed wire envelope (ADR 0025 §7), verbatim, with
// orderRef as data.order_ref.
func mustTaskCPTMissedEnvelopeJSON(t *testing.T, orderRef string) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"task_id":   "task-itest-1",
		"order_ref": orderRef,
		"task_type": "PICK",
		"cpt":       time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := map[string]any{
		"event_id":    fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		"event_type":  "TaskCPTMissed",
		"occurred_at": time.Now().UTC().Format(time.RFC3339),
		"source":      "fulfillment-execution",
		"data":        json.RawMessage(data),
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

func waitForRepromise(t *testing.T, ctx context.Context, orders *memory.OrderRepo, orderID shared.OrderId, originalCutoff time.Time) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		stored, err := orders.FindByID(ctx, orderID)
		if err == nil && stored != nil {
			groups := stored.PromiseGroups()
			if len(groups) == 1 && !groups[0].Promise.CutoffAt.Equal(originalCutoff) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("order %s was never repromised within the deadline", orderID)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done while waiting for repromise: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func createRepromiseTopic(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready", topic)
}

// TestRepromiseConsumer_DualShapeSameEventId_FiresRepromiseExactlyOnce
// is the ADR-0027 / ADR-0021 Phase 2 Task 2e dual-write-safety
// regression: during the eventual dual-write bake period, ONE domain
// TaskCPTMissed event produces TWO physical Kafka messages on the same
// topic/key (same event_id/id) -- a flat-shaped one and a CloudEvents-
// shaped one. RepromiseOrder's own event_id idempotency gate
// (ports.RepromiseProcessedEvents.MarkProcessed) must make the SECOND
// physical message a no-op, so the use case fires exactly once
// regardless of which physical shape the consumer decodes first.
func TestRepromiseConsumer_DualShapeSameEventId_FiresRepromiseExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-dualshape-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.fulfillment.events.dualshape-itest-%d", time.Now().UnixNano())
	createRepromiseTopic(t, ctx, brokers, topic)

	orders := memory.NewOrderRepo()
	l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	orderID := shared.OrderId(fmt.Sprintf("ord-dualshape-%d", time.Now().UnixNano()))
	o, err := order.New(orderID, []*order.OrderLine{l}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := o.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	originalCutoff := now.Add(24 * time.Hour)
	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: order.Promise{CutoffAt: originalCutoff, Basis: order.BasisLeadTime}},
	})
	if err := orders.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	events := &capturingPublisher{}
	processed := memory.NewRepromiseProcessedEventsRepo()
	clock := memory.NewFixedClock(now)
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: orders, Promise: movedPromise, Events: events, Clock: clock, Processed: processed,
	}

	consumer := inboundkafka.NewRepromiseConsumerForTopic(
		brokers,
		fmt.Sprintf("order-management-repromise-dualshape-itest-%d", time.Now().UnixNano()),
		topic,
		repromiseOrder,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	orderRef := usecases.WorkUnitID(orderID, 1)
	eventID := fmt.Sprintf("evt-dualshape-%d", time.Now().UnixNano()) // SAME event_id/id on both physical messages
	cpt := time.Now().UTC().Format(time.RFC3339)

	flatData, err := json.Marshal(map[string]any{
		"task_id": "task-dualshape-1", "order_ref": orderRef, "task_type": "PICK", "cpt": cpt,
	})
	if err != nil {
		t.Fatalf("marshal flat data: %v", err)
	}
	flatValue, err := json.Marshal(map[string]any{
		"event_id": eventID, "event_type": "TaskCPTMissed", "occurred_at": cpt,
		"source": "fulfillment-execution", "data": json.RawMessage(flatData),
	})
	if err != nil {
		t.Fatalf("marshal flat envelope: %v", err)
	}
	ceData, err := json.Marshal(map[string]any{
		"task_id": "task-dualshape-1", "order_ref": orderRef, "task_type": "PICK", "cpt": cpt,
	})
	if err != nil {
		t.Fatalf("marshal CloudEvents data: %v", err)
	}
	ceValue, err := json.Marshal(map[string]any{
		"specversion": "1.0", "id": eventID,
		"type":   "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
		"source": "/warehouse/fulfillment-execution", "subject": "task-dualshape-1",
		"time": cpt, "datacontenttype": "application/json", "data": json.RawMessage(ceData),
	})
	if err != nil {
		t.Fatalf("marshal CloudEvents envelope: %v", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte(eventID), Value: flatValue},
		kafkago.Message{Key: []byte(eventID), Value: ceValue},
	); err != nil {
		t.Fatalf("publish dual-shape TaskCPTMissed pair: %v", err)
	}

	waitForRepromise(t, ctx, orders, orderID, originalCutoff)
	// Give the second physical message time to be consumed too (it
	// arrives on the same partition right after the first) before
	// asserting the published-event count is stable at exactly 1.
	time.Sleep(3 * time.Second)

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	if len(events.events) != 1 {
		t.Fatalf("published events = %d, want exactly 1 -- the use case's event_id idempotency gate must dedupe the second physical (dual-write) message", len(events.events))
	}
	repromised, ok := events.events[0].(shared.OrderRepromised)
	if !ok {
		t.Fatalf("published event = %T, want shared.OrderRepromised", events.events[0])
	}
	if repromised.OrderID != orderID {
		t.Errorf("OrderRepromised.OrderID = %q, want %q", repromised.OrderID, orderID)
	}
}
