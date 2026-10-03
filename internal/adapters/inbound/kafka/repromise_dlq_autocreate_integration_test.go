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
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestRepromiseConsumer_PoisonMessage_DLQTopicMissing_AutoCreatedAndConsumptionContinues
// is the regression test for the fleet incident where a poison message
// stopped a consumer because "<topic>.dlq" had never been created: the
// DLQ writer did not set AllowAutoTopicCreation, so the publish failed
// with "[3] Unknown Topic Or Partition" and Run returned, halting
// consumption. Unlike the other DLQ tests in this package, this one
// deliberately does NOT pre-create the dead-letter topic.
//
// It proves, against a real broker: the .dlq topic does not exist up
// front; a non-CloudEvents poison message is dead-lettered anyway (the
// writer auto-creates the topic, the fleet convention for every writer);
// the valid message behind it on the same partition is still processed;
// and Run never returned an error.
func TestRepromiseConsumer_PoisonMessage_DLQTopicMissing_AutoCreatedAndConsumptionContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1",
		tckafka.WithClusterID(fmt.Sprintf("om-repromise-dlq-autocreate-%d", time.Now().UnixNano())))
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

	topic := fmt.Sprintf("warehouse.fulfillment.events.dlq-autocreate-%d", time.Now().UnixNano())
	dlqTopic := topic + ".dlq"
	createRepromiseTopic(t, ctx, brokers, topic)
	// Only the source topic exists. Listing ALL topics (no topic name in
	// the metadata request) never triggers broker-side auto-creation, so
	// this check cannot itself create the DLQ topic.
	if topicExists(t, brokers, dlqTopic) {
		t.Fatalf("precondition: DLQ topic %q must not exist before the poison message", dlqTopic)
	}

	orders := memory.NewOrderRepo()
	line, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	orderID := shared.OrderId(fmt.Sprintf("ord-dlq-autocreate-%d", time.Now().UnixNano()))
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
		fmt.Sprintf("order-management-repromise-dlq-autocreate-%d", time.Now().UnixNano()), topic, repromiseOrder, nil)
	defer func() { _ = consumer.Close() }()

	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(consumeCtx) }()

	poison := []byte("this is not a CloudEvent")
	orderRef := usecases.WorkUnitID(orderID, 1)
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic}
	defer func() { _ = writer.Close() }()
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: []byte("poison-1"), Value: poison},
		kafkago.Message{Key: []byte("ce-1"), Value: mustTaskCPTMissedEnvelopeJSON(t, orderRef)},
	); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The valid message sits BEHIND the poison one on the same single
	// partition, so it can only be processed if the consumer survived the
	// dead-letter publish. Fail fast if Run returns in the meantime.
	waitCtx, waitCancel := context.WithTimeout(ctx, 60*time.Second)
	defer waitCancel()
	for {
		stored, err := orders.FindByID(ctx, orderID)
		if err == nil && stored != nil && len(stored.PromiseGroups()) == 1 &&
			!stored.PromiseGroups()[0].Promise.CutoffAt.Equal(originalCutoff) {
			break
		}
		select {
		case err := <-runErr:
			t.Fatalf("consumer stopped after the poison message (DLQ topic missing?): %v", err)
		case <-waitCtx.Done():
			t.Fatalf("valid message behind the poison one was never processed")
		case <-time.After(200 * time.Millisecond):
		}
	}

	select {
	case err := <-runErr:
		t.Fatalf("consumer must still be running, but Run returned: %v", err)
	default:
	}

	if !topicExists(t, brokers, dlqTopic) {
		t.Fatalf("DLQ topic %q was not auto-created by the dead-letter publish", dlqTopic)
	}
	dlqReader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6,
	})
	defer func() { _ = dlqReader.Close() }()
	if err := dlqReader.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatalf("seek DLQ reader: %v", err)
	}
	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.ReadMessage(dlqCtx)
	if err != nil {
		t.Fatalf("read DLQ message: %v", err)
	}
	if string(dlqMsg.Value) != string(poison) {
		t.Errorf("DLQ value = %q, want the raw poison payload %q", dlqMsg.Value, poison)
	}
	assertHeader(t, dlqMsg.Headers, "x-dlq-source-topic", topic)

	if len(events.events) != 1 {
		t.Fatalf("published events = %d, want exactly 1 (only the valid message)", len(events.events))
	}

	consumeCancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("run consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
}

// topicExists lists every topic on the cluster WITHOUT naming one, so the
// metadata request can never auto-create the topic it is checking for.
func topicExists(t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka broker: %v", err)
	}
	defer func() { _ = conn.Close() }()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		t.Fatalf("list Kafka topics: %v", err)
	}
	for _, p := range partitions {
		if p.Topic == topic {
			return true
		}
	}
	return false
}
