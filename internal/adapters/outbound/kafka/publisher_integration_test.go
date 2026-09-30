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

	adapter "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// This verifies the real Kafka writer rather than a fake Writer. The broker is
// started by Testcontainers, never taken from KAFKA_BROKERS or localhost, so
// CI cannot silently skip the published-event contract.
func TestPublisherPublishesAllocationEnvelopeToKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("order-management-kafka-itest"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.order-management.events.itest-%d", time.Now().UnixNano())
	createKafkaTopic(t, ctx, brokers, topic)

	writer := adapter.NewWriterForTopic(topic, brokers...)
	t.Cleanup(func() { _ = writer.Close() })
	publisher := adapter.NewPublisher(writer)
	occurredAt := time.Now().UTC().Truncate(time.Second)
	event := shared.NewOrderAllocated(occurredAt, "order-itest", occurredAt.Add(24*time.Hour), nil)
	if err := publisher.Publish(ctx, event); err != nil {
		t.Fatalf("publish allocation event: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     10 * time.Second,
	})
	t.Cleanup(func() { _ = reader.Close() })
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read published message: %v", err)
	}

	var envelope struct {
		EventType string `json:"event_type"`
		Source    string `json:"source"`
	}
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		t.Fatalf("unmarshal published envelope: %v", err)
	}
	if envelope.EventType != "OrderAllocated" || envelope.Source != adapter.Source {
		t.Fatalf("published envelope = %+v, want OrderAllocated from %q", envelope, adapter.Source)
	}
}

func createKafkaTopic(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	createKafkaTopicWithPartitions(t, ctx, brokers, topic, 1)
}

// createKafkaTopicWithPartitions creates topic with the given partition
// count. Used by TestPublisherKeysMessagesForSameOrderOntoTheSamePartition
// to exercise the exact "1->8 partitions" scaleup scenario that exposed the
// missing-Key bug.
func createKafkaTopicWithPartitions(t *testing.T, ctx context.Context, brokers []string, topic string, numPartitions int) {
	t.Helper()
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: numPartitions, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) >= numPartitions {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q never became ready with %d partitions", topic, numPartitions)
}

// TestPublisherKeysMessagesForSameOrderOntoTheSamePartition is the
// real-Kafka-level guarantee behind the fix: on an 8-partition topic
// (mirroring the Phase 3 partition scaleup, warehouse-infra PR #42),
// every integration event published for the SAME order id must land on
// the SAME partition, and events for a DIFFERENT order id are free to
// land elsewhere. This is exactly what Kafka's default partitioner
// provides once a non-nil Key is set — no custom partitioner needed.
func TestPublisherKeysMessagesForSameOrderOntoTheSamePartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID("order-management-kafka-itest-partitioning"),
	)
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate Kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve Kafka brokers: %v", err)
	}
	const numPartitions = 8
	topic := fmt.Sprintf("warehouse.order-management.events.itest-part-%d", time.Now().UnixNano())
	createKafkaTopicWithPartitions(t, ctx, brokers, topic, numPartitions)

	writer := adapter.NewWriterForTopic(topic, brokers...)
	t.Cleanup(func() { _ = writer.Close() })
	publisher := adapter.NewPublisher(writer)

	occurredAt := time.Now().UTC().Truncate(time.Second)
	const sameOrder = "order-itest-same-partition"
	const otherOrder = "order-itest-other-partition"

	// Publish 3 events for sameOrder (mirrors the real fleet ordering
	// concern: allocation, then a later re-promise) plus 1 for a
	// different order, to prove the key -- not accident -- drives
	// partition placement.
	events := []struct {
		orderID string
		event   shared.DomainEvent
	}{
		{sameOrder, shared.NewOrderAllocated(occurredAt, sameOrder, occurredAt.Add(24*time.Hour), nil)},
		{sameOrder, shared.NewOrderPartiallyAllocated(occurredAt, sameOrder, 1, 1, occurredAt.Add(6*time.Hour), nil)},
		{sameOrder, shared.NewOrderRepromised(occurredAt, sameOrder, "sp1-1200", "sp1-1800", "TaskCPTMissed")},
		{otherOrder, shared.NewOrderAllocated(occurredAt, otherOrder, occurredAt.Add(24*time.Hour), nil)},
	}
	for _, e := range events {
		if err := publisher.Publish(ctx, e.event); err != nil {
			t.Fatalf("publish event for %s: %v", e.orderID, err)
		}
	}

	// Read every message back with its partition using one reader per
	// partition (kafka-go's GenericGroup/single reader keeps offsets per
	// partition, so reading from partition 0 with a plain Reader would
	// only see that partition's slice -- read all partitions explicitly).
	partitionOf := map[string]int{}
	for p := 0; p < numPartitions; p++ {
		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p,
			MaxWait:   2 * time.Second,
		})
		func() {
			defer func() { _ = reader.Close() }()
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			defer readCancel()
			for {
				msg, err := reader.ReadMessage(readCtx)
				if err != nil {
					return // timeout: no more messages on this partition
				}
				partitionOf[string(msg.Key)] = p
			}
		}()
	}

	if len(partitionOf) != 2 {
		t.Fatalf("observed keys->partition = %v, want exactly 2 distinct keys (sameOrder, otherOrder)", partitionOf)
	}
	samePartition, ok := partitionOf[sameOrder]
	if !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", sameOrder, partitionOf)
	}
	if _, ok := partitionOf[otherOrder]; !ok {
		t.Fatalf("no message observed with key %q; partitionOf = %v", otherOrder, partitionOf)
	}

	// Re-verify by reading every partition again and counting how many of
	// sameOrder's 3 messages landed on samePartition -- all 3 must be
	// there, proving the guarantee isn't a one-message coincidence.
	countOnSamePartition := 0
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   samePartition,
		StartOffset: kafkago.FirstOffset,
		MaxWait:     2 * time.Second,
	})
	defer func() { _ = reader.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	for {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			break
		}
		if string(msg.Key) == sameOrder {
			countOnSamePartition++
		}
	}
	if countOnSamePartition != 3 {
		t.Errorf("found %d of sameOrder's 3 messages on partition %d, want 3 (all events for one order must share a partition)", countOnSamePartition, samePartition)
	}
}
