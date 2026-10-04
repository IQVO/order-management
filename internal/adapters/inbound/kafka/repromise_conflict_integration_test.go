//go:build integration

package kafka_test

import (
	"context"
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

// alwaysConflictingRepo rejects every Save of one order with
// ports.ErrConcurrentModification (a writer that keeps winning the
// version race), delegating everything else. The in-memory repo hands
// out its stored pointer, so the failed attempt's in-place mutation
// would leak into the retry; fresh rebuilds the persisted state on every
// read, as a real repo does.
type alwaysConflictingRepo struct {
	ports.OrderRepo
	conflictOrderID shared.OrderId
	fresh           func() *order.Order
}

func (r *alwaysConflictingRepo) FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	if id == r.conflictOrderID {
		return r.fresh(), nil
	}
	return r.OrderRepo.FindByID(ctx, id)
}

// alwaysNewProcessedFor reports isNew=true for one event id on every call,
// emulating the Postgres UnitOfWork rolling the idempotency mark back
// when the surrounding scope fails (the in-memory repo has no rollback,
// so a retry would otherwise be skipped as "already processed").
type alwaysNewProcessedFor struct {
	ports.RepromiseProcessedEvents
	eventID string
}

func (p *alwaysNewProcessedFor) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	if eventID == p.eventID {
		return true, nil
	}
	return p.RepromiseProcessedEvents.MarkProcessed(ctx, eventID)
}

func (r *alwaysConflictingRepo) Save(ctx context.Context, o *order.Order) error {
	if o.ID() == r.conflictOrderID {
		return ports.ErrConcurrentModification
	}
	return r.OrderRepo.Save(ctx, o)
}

// TestRepromiseConsumer_PersistentVersionConflict_DeadLetteredNotSwallowed
// proves against a real broker (ADR-0024): a message whose Save keeps
// losing the version race is retried in-process, then dead-lettered with
// the raw payload and its offset committed — never silently skipped — and
// the message behind it on the same partition is still processed.
func TestRepromiseConsumer_PersistentVersionConflict_DeadLetteredNotSwallowed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-conflict-itest-%d", time.Now().UnixNano())))
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
	topic := fmt.Sprintf("warehouse.fulfillment.events.conflict-itest-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	now := time.Now().UTC().Truncate(time.Second)
	originalCutoff := now.Add(24 * time.Hour)
	orders := memory.NewOrderRepo()
	seed := func(id shared.OrderId) *order.Order {
		l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
		if err != nil {
			t.Fatalf("NewOrderLine: %v", err)
		}
		o, err := order.New(id, []*order.OrderLine{l}, false)
		if err != nil {
			t.Fatalf("order.New: %v", err)
		}
		if err := o.Allocate(1, "res-1"); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
		if err := o.Release(1); err != nil {
			t.Fatalf("Release: %v", err)
		}
		o.SetPromiseGroups([]order.PromiseGroup{
			{LineNos: []int{1}, Promise: order.Promise{CutoffAt: originalCutoff, Basis: order.BasisLeadTime}},
		})
		return o
	}
	conflictOrderID := shared.OrderId(fmt.Sprintf("ord-conflict-%d", time.Now().UnixNano()))
	healthyOrderID := shared.OrderId(fmt.Sprintf("ord-conflict-healthy-%d", time.Now().UnixNano()))
	if err := orders.Save(ctx, seed(healthyOrderID)); err != nil {
		t.Fatalf("Save: %v", err)
	}

	conflictEventID := fmt.Sprintf("evt-conflict-%d", time.Now().UnixNano())
	events := &capturingPublisher{}
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: &alwaysConflictingRepo{
			OrderRepo: orders, conflictOrderID: conflictOrderID,
			fresh: func() *order.Order { return seed(conflictOrderID) },
		},
		Promise:   order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)},
		Events:    events,
		Clock:     memory.NewFixedClock(now),
		Processed: &alwaysNewProcessedFor{RepromiseProcessedEvents: memory.NewRepromiseProcessedEventsRepo(), eventID: conflictEventID},
	}
	consumer := inboundkafka.NewRepromiseConsumerForTopic(brokers,
		fmt.Sprintf("order-management-repromise-conflict-itest-%d", time.Now().UnixNano()), topic, repromiseOrder, nil)
	defer func() { _ = consumer.Close() }()
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	go func() { _ = consumer.Run(consumeCtx) }()

	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: dlqTopic,
		GroupID:     fmt.Sprintf("dlq-conflict-reader-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	defer func() { _ = dlqReader.Close() }()

	conflictPayload := mustTaskCPTMissedEnvelopeJSONWithEventID(t, conflictEventID, usecases.WorkUnitID(conflictOrderID, 1))
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte(conflictEventID), Value: conflictPayload},
		kafkago.Message{Key: []byte("good"), Value: mustTaskCPTMissedEnvelopeJSON(t, usecases.WorkUnitID(healthyOrderID, 1))},
	); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 60*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: the conflicting message was swallowed, not dead-lettered: %v", err)
	}
	if string(dlqMsg.Value) != string(conflictPayload) {
		t.Errorf("DLQ value = %s, want the raw original payload", dlqMsg.Value)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)
	if headerValue(dlqMsg.Headers, "x-dlq-error") == "" {
		t.Error("DLQ message missing x-dlq-error header")
	}

	// The message behind it is processed: the partition was not blocked.
	waitForRepromise(t, ctx, orders, healthyOrderID, originalCutoff)
}
