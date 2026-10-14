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

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestConsumer_RealBroker_IntakeDrivesPathSelection (ADR-0042) replays the
// catalogue process-path-management publishes after its ADR 0034 seed
// through a REAL Kafka broker (testcontainers) and feeds the consumer to
// order.PathSelectionPolicy: PACK sorts first and RELOCATE is fastest, but
// only PICK declares CUSTOMER_ORDER, so a customer order line resolves to
// PICK. A second topic carrying the same four paths with no intake (a
// producer older than ADR 0034) must fall back to shared.DefaultPathId,
// never PACK.
func TestConsumer_RealBroker_IntakeDrivesPathSelection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-catalogue-intake-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}

	seeded := []string{
		`{"path_id":"PACK","match_prefix":"pack","cycle_time_p95":"2h0m0s","eligibility":{},"intake":[]}`,
		`{"path_id":"PICK","match_prefix":"pick","cycle_time_p95":"2h0m0s","eligibility":{},"intake":["CUSTOMER_ORDER"]}`,
		`{"path_id":"REBIN","match_prefix":"rebin","cycle_time_p95":"2h0m0s","eligibility":{},"intake":[]}`,
		`{"path_id":"RELOCATE","match_prefix":"relocate","cycle_time_p95":"10m0s","eligibility":{},"intake":["SLOT_RELOCATION"]}`,
	}
	consumer := replayedConsumer(ctx, t, brokers, "seeded", seeded)
	var policy order.PathSelectionPolicy
	if got, ok := policy.Select("SKU-1", 1, false, nil, consumer); !ok || got != "PICK" {
		t.Fatalf("seeded catalogue: Select = (%q, %v), want (PICK, true)", got, ok)
	}

	legacy := []string{
		`{"path_id":"PACK","match_prefix":"pack","cycle_time_p95":"2h0m0s","eligibility":{}}`,
		`{"path_id":"PICK","match_prefix":"pick","cycle_time_p95":"2h0m0s","eligibility":{}}`,
		`{"path_id":"REBIN","match_prefix":"rebin","cycle_time_p95":"2h0m0s","eligibility":{}}`,
		`{"path_id":"SLAM","match_prefix":"slam","cycle_time_p95":"2h0m0s","eligibility":{}}`,
	}
	consumer = replayedConsumer(ctx, t, brokers, "legacy", legacy)
	if got, ok := policy.Select("SKU-1", 1, false, nil, consumer); !ok || got != shared.DefaultPathId {
		t.Fatalf("pre-ADR-0034 catalogue: Select = (%q, %v), want (%q, true), never PACK", got, ok, shared.DefaultPathId)
	}
}

// replayedConsumer publishes one ProcessPathCreated per payload to a fresh
// topic, then returns a consumer that has replayed it (ready).
func replayedConsumer(ctx context.Context, t *testing.T, brokers []string, name string, payloads []string) *Consumer {
	t.Helper()
	topic := fmt.Sprintf("warehouse.process-path-management.events.itest-intake-%s-%d", name, time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}
	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false, RequiredAcks: kafkago.RequireAll}
	for i, p := range payloads {
		msg := envelopeMsg(t, 0, int64(i), ceTypeCreated, json.RawMessage(p))
		if err := writeWithRetry(ctx, writer, kafkago.Message{Value: msg.Value}); err != nil {
			t.Fatalf("publish %s: %v", p, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	consumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("NewConsumerForTopic: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
		_ = consumer.Close()
	})
	if err := consumer.WaitReady(ctx); err != nil {
		t.Fatalf("consumer never became ready: %v", err)
	}
	return consumer
}

// writeWithRetry retries a write while a just-created topic's leader
// metadata propagates (a cold broker answers "Unknown Topic Or Partition"
// for a moment after CreateTopics), bounded to 15s.
func writeWithRetry(ctx context.Context, w *kafkago.Writer, msg kafkago.Message) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := w.WriteMessages(ctx, msg)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
}
