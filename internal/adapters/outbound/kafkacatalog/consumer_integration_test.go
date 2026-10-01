//go:build integration

package kafkacatalog

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestNewConsumer_TwoInstancesInARow_BothReplayFully is the real regression
// test for the shared-consumer-group bug: a second process must independently
// replay the full topic history rather than resume from a previous process's
// committed offset. The test owns its broker, so CI executes it rather than
// silently skipping when no external Kafka is available.
func TestNewConsumer_TwoInstancesInARow_BothReplayFully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-catalogue-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value: envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated", json.RawMessage(`{"path_id":"ITEST2","match_prefix":"itest2"}`)).Value,
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
		if !consumer.IsActive(shared.PathId("itest2-zone-a")) {
			stop()
			t.Fatalf("consumer instance %d did not replay catalogue", instance)
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

// TestNewConsumer_AlreadyCaughtUpGroup_ReadyImmediately is the regression
// test for the "readiness never fires because nothing new arrives" bug: a
// brand-new consumer whose captured target offset is already satisfied by
// messages that exist BEFORE it starts consuming must become ready as part
// of its normal catch-up replay, not hang waiting for a message that will
// never come. This exercises the same replay path as the two-instance test
// above but asserts specifically on the readiness timing, not just on the
// final cache content.
func TestNewConsumer_AlreadyCaughtUpGroup_ReadyImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-catalogue-itest2"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest2-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx, kafkago.Message{
		Value: envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated", json.RawMessage(`{"path_id":"ITEST3","match_prefix":"itest3"}`)).Value,
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
	if !consumer.IsActive(shared.PathId("itest3")) {
		t.Fatal("expected the pre-existing message to have been replayed")
	}
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
