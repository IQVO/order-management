package kafkapathcapacity

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/domain/shared"
)

type fakeReader struct {
	messages []kafkago.Message
	pos      int
}

func (r *fakeReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	if r.pos < len(r.messages) {
		m := r.messages[r.pos]
		r.pos++
		return m, nil
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) Close() error { return nil }

func envelopeMsg(t *testing.T, partition int, offset int64, eventType string, data any) kafkago.Message {
	t.Helper()
	rawData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := envelope{EventType: eventType, Data: rawData}
	rawEnv, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return kafkago.Message{Partition: partition, Offset: offset, Value: rawEnv}
}

func newTestConsumer(reader Reader, target targetOffsets) *Consumer {
	c := &Consumer{
		Reader:  reader,
		Logger:  slog.Default(),
		entries: make(map[capacityKey]capacityEntry),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		c.markReady()
	}
	return c
}

func TestConsumer_NoTargetOffsets_IsReadyImmediately(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if !c.Ready() {
		t.Fatal("expected a consumer with no readiness target to be ready immediately")
	}
}

func TestConsumer_AppliesPathCapacityChanged_Known(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 42, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	units, known := c.Remaining(shared.PathId("pick"), "sp1-1800", cutoff)
	if !known {
		t.Fatal("expected known=true for an observed path+cutoff")
	}
	if units != 42 {
		t.Fatalf("units = %d, want 42", units)
	}
}

func TestConsumer_AppliesPathCapacityChanged_KnownFalse(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, eventTypeChanged, capacityData{
				PathId: "flowfed", CutoffAt: cutoff, RemainingUnits: 0, Known: false,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	// A FlowFed path (or unset WIP limit) reports Known=false on the
	// wire itself — this consumer must faithfully report known=false
	// too, not fabricate a figure.
	_, known := c.Remaining(shared.PathId("flowfed"), "sp1-1800", cutoff)
	if known {
		t.Fatal("expected known=false when the source event itself reported known=false")
	}
}

func TestConsumer_Remaining_UnobservedPathCutoff_ReturnsUnknown(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	_, known := c.Remaining(shared.PathId("never-seen"), "sp1-1800", time.Now())
	if known {
		t.Fatal("expected known=false for a path+cutoff this consumer has never observed")
	}
}

func TestConsumer_Remaining_DifferentCutoffSamePath_IsUnknown(t *testing.T) {
	seenCutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	otherCutoff := time.Date(2026, 9, 13, 20, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: seenCutoff, RemainingUnits: 5, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	// Same path, a DIFFERENT cutoff instant this consumer never saw a
	// PathCapacityChanged for -- must be unknown, not a false match.
	_, known := c.Remaining(shared.PathId("pick"), "sp1-2000", otherCutoff)
	if known {
		t.Fatal("expected known=false for a cutoff the consumer never observed for this path")
	}
}

func TestConsumer_LaterPathCapacityChangedReplacesEarlierValue(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 100, Known: true,
			}),
			envelopeMsg(t, 0, 1, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 3, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	units, known := c.Remaining(shared.PathId("pick"), "sp1-1800", cutoff)
	if !known || units != 3 {
		t.Fatalf("units=%d known=%v, want 3/true (the later event should win)", units, known)
	}
}

func TestConsumer_UnknownEventType_IsIgnored(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, "ShiftPlanCommitted", map[string]any{"shift_plan_id": "sp-1"}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout even for an event type this consumer ignores, got: %v", err)
	}
}

func TestConsumer_MalformedMessage_LoggedAndSkipped(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			{Partition: 0, Offset: 0, Value: []byte("not json")},
			envelopeMsg(t, 0, 1, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC), RemainingUnits: 7, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout despite one malformed message, got: %v", err)
	}
	units, known := c.Remaining(shared.PathId("pick"), "sp1-1800", time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC))
	if !known || units != 7 {
		t.Fatalf("units=%d known=%v, want 7/true -- the valid message after the malformed one should still apply", units, known)
	}
}

// --- ADR-0021 Phase 2 Task 2b: CloudEvents dual-read tests ---

func cloudEventsMsg(t *testing.T, partition int, offset int64, ceType string, data any) kafkago.Message {
	t.Helper()
	rawData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	ce := map[string]any{
		"specversion":     "1.0",
		"id":              "5c7d3f92-1b64-4a08-9e73-2f6a8c1d5b40",
		"type":            ceType,
		"source":          "/warehouse/wes-work-planning",
		"subject":         "pick-to-tote",
		"time":            "2026-08-21T22:40:00Z",
		"datacontenttype": "application/json",
		"data":            json.RawMessage(rawData),
	}
	rawEnv, err := json.Marshal(ce)
	if err != nil {
		t.Fatalf("marshal CloudEvents envelope: %v", err)
	}
	return kafkago.Message{Partition: partition, Offset: offset, Value: rawEnv}
}

func TestConsumer_DecodesCloudEventsShapedMessage_SameResultAsFlat(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		messages: []kafkago.Message{
			cloudEventsMsg(t, 0, 0, cloudEventsTypePathCapacityChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 42, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	units, known := c.Remaining(shared.PathId("pick"), "sp1-1800", cutoff)
	if !known {
		t.Fatal("expected known=true for a CloudEvents-shaped PathCapacityChanged")
	}
	if units != 42 {
		t.Fatalf("units = %d, want 42 -- must match the equivalent flat-shaped fixture's result", units)
	}
}

func TestConsumer_CloudEventsUnrecognizedSpecversion_FailsSoftAndSkips(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	malformed := kafkago.Message{
		Partition: 0, Offset: 0,
		Value: []byte(`{"specversion":"2.0","type":"com.warehouse.wes.work-planning.workpool.PathCapacityChanged","data":{"path_id":"pick","cutoff_at":"2026-09-13T18:00:00Z","remaining_units":42,"known":true}}`),
	}
	reader := &fakeReader{
		messages: []kafkago.Message{
			malformed,
			envelopeMsg(t, 0, 1, eventTypeChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 9, Known: true,
			}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout despite one unrecognized-specversion message, got: %v", err)
	}
	// The malformed-specversion message must be skipped (fail soft),
	// not applied and not fatal to the consumer -- the valid message
	// after it must still apply.
	units, known := c.Remaining(shared.PathId("pick"), "sp1-1800", cutoff)
	if !known || units != 9 {
		t.Fatalf("units=%d known=%v, want 9/true -- an unrecognized specversion must fail soft, not wedge the consumer", units, known)
	}
}
