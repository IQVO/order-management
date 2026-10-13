//go:build integration

package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// parkingUnitOfWork parks every atomic scope until the handler's context is
// cancelled, then reports that cancellation -- exactly what the Postgres
// UnitOfWork does when the pod is told to stop mid-transaction.
type parkingUnitOfWork struct {
	entered chan struct{}
}

func (u *parkingUnitOfWork) Execute(ctx context.Context, _ func(ctx context.Context) error) error {
	select {
	case u.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

// syncBuffer is a goroutine-safe log sink: the consumer logs from its Run
// goroutine while the test reads the captured output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type shutdownConsumer interface {
	Run(ctx context.Context) error
	Close() error
}

// TestConsumers_ShutdownMidHandle_IsCleanAndDeadLettersNothing pins the
// shutdown contract for both retry-then-dead-letter consumers: cancelling
// Run's context while a healthy message is being handled is a clean stop
// (Run returns nil), and that message is NOT dead-lettered -- no dead-letter
// attempt, nothing on "<topic>.dlq". Its offset stays uncommitted, so it is
// redelivered on the next start and the use case's processed-events claim
// absorbs the replay. Before the fix the bounded retry's cancellation error
// was routed into the dead-letter path as if the message were poison.
func TestConsumers_ShutdownMidHandle_IsCleanAndDeadLettersNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-shutdown-itest-%d", time.Now().UnixNano())))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}

	t.Run("RepromiseConsumer", func(t *testing.T) {
		topic := fmt.Sprintf("warehouse.fulfillment.events.shutdown-itest-%d", time.Now().UnixNano())
		uow := &parkingUnitOfWork{entered: make(chan struct{}, 1)}
		logs := &syncBuffer{}
		newConsumer := func() shutdownConsumer {
			return inboundkafka.NewRepromiseConsumerForTopic(brokers,
				fmt.Sprintf("om-repromise-shutdown-itest-%d", time.Now().UnixNano()), topic,
				&usecases.RepromiseOrder{
					Orders:     memory.NewOrderRepo(),
					Processed:  memory.NewRepromiseProcessedEventsRepo(),
					UnitOfWork: uow,
				},
				slog.New(slog.NewTextHandler(logs, nil)))
		}
		eventID := fmt.Sprintf("evt-repromise-shutdown-%d", time.Now().UnixNano())
		assertCleanShutdownMidHandle(t, ctx, brokers, topic, newConsumer, uow, logs, kafkago.Message{
			Key: []byte(eventID),
			Value: mustTaskCPTMissedEnvelopeJSONWithEventID(t, eventID,
				usecases.WorkUnitID(shared.OrderId(fmt.Sprintf("ord-shutdown-%d", time.Now().UnixNano())), 1)),
		})
	})

	t.Run("PlannedCapacityConsumer", func(t *testing.T) {
		topic := fmt.Sprintf("warehouse.warehouse-planning.events.shutdown-itest-%d", time.Now().UnixNano())
		uow := &parkingUnitOfWork{entered: make(chan struct{}, 1)}
		logs := &syncBuffer{}
		newConsumer := func() shutdownConsumer {
			return inboundkafka.NewPlannedCapacityConsumerForTopic(brokers,
				fmt.Sprintf("om-planned-capacity-shutdown-itest-%d", time.Now().UnixNano()), topic,
				&usecases.ApplyPlannedCapacity{
					Windows:    memory.NewPlannedCapacityRepo(),
					Processed:  memory.NewPlannedCapacityProcessedEventsRepo(),
					UnitOfWork: uow,
				},
				slog.New(slog.NewTextHandler(logs, nil)))
		}
		assertCleanShutdownMidHandle(t, ctx, brokers, topic, newConsumer, uow, logs,
			pcItEvent(t, pcItTypePub, fmt.Sprintf("evt-plan-shutdown-%d", time.Now().UnixNano()), pcItPlanID,
				time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC), pcItData(pcItPlanID, 4000)))
	})
}

// assertCleanShutdownMidHandle publishes one healthy message, waits until the
// consumer's use case is parked on it, cancels Run, and asserts the clean
// shutdown contract.
func assertCleanShutdownMidHandle(t *testing.T, ctx context.Context, brokers []string, topic string,
	newConsumer func() shutdownConsumer, uow *parkingUnitOfWork, logs *syncBuffer, msg kafkago.Message) {
	t.Helper()
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	createRepromiseTopic(t, ctx, brokers, dlqTopic)

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	// The writer's metadata cache can lag the just-created topic for a moment
	// (UnknownTopicOrPartition): retry the publish until the deadline.
	publishDeadline := time.Now().Add(30 * time.Second)
	for {
		err := writer.WriteMessages(ctx, msg)
		if err == nil {
			break
		}
		if time.Now().After(publishDeadline) {
			t.Fatalf("publish healthy message: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Build the consumer only now: a kafka-go group reader joins as soon as
	// it is constructed, and one that joins before its topic exists is never
	// assigned a partition. A new group starts at the first offset.
	consumer := newConsumer()
	defer func() { _ = consumer.Close() }()
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	select {
	case <-uow.entered:
	case <-time.After(60 * time.Second):
		t.Fatalf("the consumer never started handling the message; consumer logs:\n%s", logs.String())
	}
	runCancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run after its context was cancelled mid-handle = %v, want nil (clean shutdown)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	if out := logs.String(); strings.Contains(out, "dead-letter") {
		t.Errorf("a healthy message only interrupted by shutdown was routed to the dead-letter path:\n%s", out)
	}
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer func() { _ = dlqReader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	if dead, err := dlqReader.ReadMessage(readCtx); err == nil {
		t.Fatalf("a healthy message was dead-lettered on shutdown: key=%q headers=%v", string(dead.Key), dead.Headers)
	}
}
