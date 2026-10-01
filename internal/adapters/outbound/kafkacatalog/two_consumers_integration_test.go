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

	"github.com/claudioed/order-management/internal/adapters/outbound/kafkacptschedule"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestTwoIndependentConsumers_SameTopic_BothReplayCorrectly is the new
// scenario ADR-0014 step A introduces: kafkacatalog.Consumer and
// kafkacptschedule.Consumer are two INDEPENDENT consumers — different
// consumer group families, different event-type filters — reading the
// SAME topic on the SAME broker. Each must replay only the event types
// it understands and reach readiness independently of what the other one
// is doing. This is the one most likely place for a subtle bug (shared
// broker, shared topic, different groups) so it gets its own dedicated,
// real-broker test rather than relying on each package's own isolated
// integration tests to imply this works together.
func TestTwoIndependentConsumers_SameTopic_BothReplayCorrectly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("om-two-consumers-itest"))
	if err != nil {
		t.Fatalf("start Kafka container: %v", err)
	}
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Kafka brokers: %v", err)
	}
	topic := fmt.Sprintf("warehouse.process-path-management.events.two-itest-%d", time.Now().UnixNano())
	if err := createTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	writer := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, AllowAutoTopicCreation: false}
	if err := writer.WriteMessages(ctx,
		kafkago.Message{
			Value: envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated", json.RawMessage(`{"path_id":"TWOTEST","match_prefix":"twotest","cycle_time_p95":"45m0s","eligibility":{"max_units_per_line":1}}`)).Value,
		},
		kafkago.Message{
			Value: envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged", json.RawMessage(`{"site_id":"two-site","timezone":"UTC","cutoffs":[{"cpt_id":"sp1-1800","local_time":"18:00","eligible_path_ids":["twotest"]}]}`)).Value,
		},
	); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close Kafka writer: %v", err)
	}

	catalogueConsumer, err := NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("kafkacatalog.NewConsumerForTopic: %v", err)
	}
	defer func() { _ = catalogueConsumer.Close() }()

	scheduleConsumer, err := kafkacptschedule.NewConsumerForTopic(ctx, brokers, topic, nil)
	if err != nil {
		t.Fatalf("kafkacptschedule.NewConsumerForTopic: %v", err)
	}
	defer func() { _ = scheduleConsumer.Close() }()

	catalogueRunCtx, stopCatalogue := context.WithCancel(ctx)
	defer stopCatalogue()
	scheduleRunCtx, stopSchedule := context.WithCancel(ctx)
	defer stopSchedule()
	go func() { _ = catalogueConsumer.Run(catalogueRunCtx) }()
	go func() { _ = scheduleConsumer.Run(scheduleRunCtx) }()

	waitCtx, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	defer waitCancel()
	if err := catalogueConsumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("catalogue consumer never became ready: %v", err)
	}
	if err := scheduleConsumer.WaitReady(waitCtx); err != nil {
		t.Fatalf("schedule consumer never became ready: %v", err)
	}

	// The catalogue consumer must see the ProcessPathCreated event and
	// ignore CPTScheduleChanged (it doesn't know that event type).
	if !catalogueConsumer.IsActive(shared.PathId("twotest-zone-a")) {
		t.Fatal("expected the catalogue consumer to have replayed ProcessPathCreated")
	}
	cycleTime, known := catalogueConsumer.CycleTimeP95(shared.PathId("twotest-zone-a"))
	if !known || cycleTime != 45*time.Minute {
		t.Fatalf("CycleTimeP95 = %v, known=%v, want 45m/true", cycleTime, known)
	}

	// The schedule consumer must see the CPTScheduleChanged event and
	// ignore ProcessPathCreated (it doesn't know that event type either).
	windows, known := scheduleConsumer.NextCutoffs("two-site", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), 1)
	if !known || len(windows) != 1 || windows[0].CptId != "sp1-1800" {
		t.Fatalf("NextCutoffs = %+v, known=%v, want one sp1-1800 window", windows, known)
	}
}
