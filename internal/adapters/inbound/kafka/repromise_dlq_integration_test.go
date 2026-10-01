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

// alwaysFailingProcessedEventsFor wraps a real
// ports.RepromiseProcessedEvents so MarkProcessed fails with a genuine
// (non-ErrConcurrentModification) infrastructure error for exactly
// poisonEventID, on EVERY call, while every other CloudEvents id is delegated
// unchanged -- letting one poison message coexist in the SAME test with
// a normal, successfully-processed message on the SAME partition. This
// is deliberately the FIRST thing RepromiseOrder.Execute calls (see
// repromise_order.go), so the injected failure never has a side effect
// to undo, keeping each of the 3 retry attempts cleanly idempotent.
type alwaysFailingProcessedEventsFor struct {
	ports.RepromiseProcessedEvents
	poisonEventID string
}

func (p *alwaysFailingProcessedEventsFor) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	if eventID == p.poisonEventID {
		return false, fmt.Errorf("simulated poison-message infrastructure failure for event %s", eventID)
	}
	return p.RepromiseProcessedEvents.MarkProcessed(ctx, eventID)
}

// TestRepromiseConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition
// is the ADR-0025 §DLQ acceptance test: a message whose handler ALWAYS
// fails (simulated infrastructure error, not
// ports.ErrConcurrentModification -- that sentinel has its own, separate
// leave-uncommitted-for-redelivery path, not the DLQ path under test
// here) must, after exactly maxHandlerAttempts (3) in-process retries,
// land on "<topic>.dlq" with the raw original payload plus error
// context, and the consumer must commit past it and keep processing --
// a well-formed message published right after the poison one must be
// handled without delay, proving the partition was never blocked on the
// one bad message.
func TestRepromiseConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-dlq-itest-%d", time.Now().UnixNano())))
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
	topic := fmt.Sprintf("warehouse.fulfillment.events.dlq-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	// Seed ONE real, healthy order the "well-formed message published
	// right after" will target. The poison message targets a
	// poisonOrderID/poisonEventID pair whose CloudEvents id is
	// unconditionally failed by alwaysFailingProcessedEventsFor below,
	// so the failure is guaranteed deterministic across every retry
	// regardless of what a real repo would have said about the order.
	orders := memory.NewOrderRepo()
	healthyLine, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	healthyOrderID := shared.OrderId(fmt.Sprintf("ord-dlq-healthy-%d", time.Now().UnixNano()))
	healthyOrder, err := order.New(healthyOrderID, []*order.OrderLine{healthyLine}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := healthyOrder.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := healthyOrder.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	originalCutoff := now.Add(24 * time.Hour)
	healthyOrder.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: order.Promise{CutoffAt: originalCutoff, Basis: order.BasisLeadTime}},
	})
	if err := orders.Save(ctx, healthyOrder); err != nil {
		t.Fatalf("Save: %v", err)
	}

	poisonEventID := fmt.Sprintf("evt-dlq-poison-%d", time.Now().UnixNano())
	poisonOrderID := shared.OrderId(fmt.Sprintf("ord-dlq-poison-%d", time.Now().UnixNano()))
	processed := memory.NewRepromiseProcessedEventsRepo()
	failingProcessed := &alwaysFailingProcessedEventsFor{RepromiseProcessedEvents: processed, poisonEventID: poisonEventID}

	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	events := &capturingPublisher{}
	clock := memory.NewFixedClock(now)
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: orders, Promise: movedPromise, Events: events, Clock: clock, Processed: failingProcessed,
	}

	consumer := inboundkafka.NewRepromiseConsumerForTopic(
		brokers,
		fmt.Sprintf("order-management-repromise-dlq-itest-%d", time.Now().UnixNano()),
		topic,
		repromiseOrder,
		nil,
	)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	// Start reading the DLQ topic BEFORE publishing, so the poison
	// message's eventual dead-letter write is never missed to a race.
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	poisonRef := usecases.WorkUnitID(poisonOrderID, 1)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(poisonEventID),
		Value: mustTaskCPTMissedEnvelopeJSONWithEventID(t, poisonEventID, poisonRef),
	}); err != nil {
		t.Fatalf("publish poison TaskCPTMissed: %v", err)
	}

	// Assert the poison message lands on the DLQ topic with the raw
	// payload and error context, after the retry budget is exhausted.
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Key) != poisonEventID {
		t.Errorf("DLQ message key = %q, want %q (raw key preserved)", string(dlqMsg.Key), poisonEventID)
	}
	var dlqPayload map[string]any
	if err := json.Unmarshal(dlqMsg.Value, &dlqPayload); err != nil {
		t.Fatalf("DLQ message value is not the raw original JSON payload: %v", err)
	}
	if dlqPayload["id"] != poisonEventID {
		t.Errorf("DLQ payload id = %v, want %q -- payload must be byte-identical to the original for manual replay", dlqPayload["id"], poisonEventID)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if h := headerValue(dlqMsg.Headers, "x-dlq-error"); h == "" {
		t.Error("DLQ message missing x-dlq-error header with failure context")
	}
	if h := headerValue(dlqMsg.Headers, "x-dlq-failed-at"); h == "" {
		t.Error("DLQ message missing x-dlq-failed-at header")
	}

	// Now publish a well-formed message for the healthy order right
	// after the poison one, and confirm it is processed without delay
	// -- proving the partition was not blocked behind the poison
	// message.
	goodRef := usecases.WorkUnitID(healthyOrderID, 1)
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(fmt.Sprintf("evt-dlq-good-%d", time.Now().UnixNano())),
		Value: mustTaskCPTMissedEnvelopeJSON(t, goodRef),
	}); err != nil {
		t.Fatalf("publish well-formed TaskCPTMissed: %v", err)
	}
	waitForRepromise(t, ctx, orders, healthyOrderID, originalCutoff)

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil && ctx.Err() == nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}

	// The healthy order's repromise must be the ONLY published event --
	// the poison message never got to publish anything, no matter how
	// many times it was retried.
	if len(events.events) != 1 {
		t.Fatalf("published events = %d, want exactly 1 (only the healthy order's repromise)", len(events.events))
	}
	repromised, ok := events.events[0].(shared.OrderRepromised)
	if !ok {
		t.Fatalf("published event = %T, want shared.OrderRepromised", events.events[0])
	}
	if repromised.OrderID != healthyOrderID {
		t.Errorf("OrderRepromised.OrderID = %q, want %q", repromised.OrderID, healthyOrderID)
	}

	// The poison order was never repromised -- the DLQ path commits the
	// offset (so the partition advances) without ever having driven the
	// use case successfully.
	poisonStored, err := orders.FindByID(ctx, poisonOrderID)
	if err == nil && poisonStored != nil {
		t.Errorf("poison order %s should not exist in the order repo (it was only ever referenced by the poison message)", poisonOrderID)
	}
}

func assertHeader(t *testing.T, headers []kafkago.Header, key, want string) {
	t.Helper()
	got := headerValue(headers, key)
	if got != want {
		t.Errorf("header %q = %q, want %q", key, got, want)
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// TestRepromiseConsumer_LegacyFlatMessage_DeadLetteredWithoutRetry proves
// against a real broker that a retired flat-envelope message is a
// deterministic poison message: it is dead-lettered byte-for-byte, never
// parsed or retried, and the CloudEvents message behind it on the same
// partition is still processed.
func TestRepromiseConsumer_LegacyFlatMessage_DeadLetteredWithoutRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-legacy-itest-%d", time.Now().UnixNano())))
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
	topic := fmt.Sprintf("warehouse.fulfillment.events.legacy-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	orders := memory.NewOrderRepo()
	line, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	orderID := shared.OrderId(fmt.Sprintf("ord-legacy-%d", time.Now().UnixNano()))
	o, err := order.New(orderID, []*order.OrderLine{line}, false)
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

	events := &capturingPublisher{}
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: orders, Promise: order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)},
		Events: events, Clock: memory.NewFixedClock(now), Processed: memory.NewRepromiseProcessedEventsRepo(),
	}
	consumer := inboundkafka.NewRepromiseConsumerForTopic(brokers,
		fmt.Sprintf("order-management-repromise-legacy-itest-%d", time.Now().UnixNano()), topic, repromiseOrder, nil)
	defer func() { _ = consumer.Close() }()
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: dlqTopic,
		GroupID:     fmt.Sprintf("dlq-legacy-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	orderRef := usecases.WorkUnitID(orderID, 1)
	legacy := []byte(fmt.Sprintf(`{"event_id":"legacy-1","event_type":"TaskCPTMissed","occurred_at":"2026-09-01T00:00:00Z","source":"fulfillment-execution","data":{"task_id":"t1","order_ref":%q}}`, orderRef))
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte("legacy-1"), Value: legacy},
		kafkago.Message{Key: []byte("ce-1"), Value: mustTaskCPTMissedEnvelopeJSON(t, orderRef)},
	); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Value) != string(legacy) {
		t.Errorf("DLQ value = %s, want the raw legacy message byte-for-byte", dlqMsg.Value)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)

	// The CloudEvents message behind it drives exactly one repromise; the
	// legacy message drove none (it was never parsed).
	waitForRepromise(t, ctx, orders, orderID, originalCutoff)
	time.Sleep(2 * time.Second)
	if len(events.events) != 1 {
		t.Fatalf("published events = %d, want exactly 1 (only the CloudEvents message)", len(events.events))
	}
}
