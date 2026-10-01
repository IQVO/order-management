package kafkapathcapacity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"

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

func envelopeMsg(t *testing.T, partition int, offset int64, ceType string, data any) kafkago.Message {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("evt-%d-%d", partition, offset))
	e.SetSource("/warehouse/wes-work-planning")
	e.SetType(ceType)
	e.SetSubject("subject-1")
	e.SetTime(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC))
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal CloudEvent: %v", err)
	}
	return kafkago.Message{Partition: partition, Offset: offset, Value: raw}
}

// legacyFlatMsg is the retired pre-CloudEvents flat envelope; the consumer
// must reject it (never parse it) and keep going.
func legacyFlatMsg(t *testing.T, partition int, offset int64, eventType string, data any) kafkago.Message {
	t.Helper()
	rawData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	raw, err := json.Marshal(map[string]any{
		"event_id": "legacy", "event_type": eventType, "occurred_at": time.Now(),
		"source": "legacy", "data": json.RawMessage(rawData),
	})
	if err != nil {
		t.Fatalf("marshal flat: %v", err)
	}
	return kafkago.Message{Partition: partition, Offset: offset, Value: raw}
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
			envelopeMsg(t, 0, 0, ceTypeChanged, capacityData{
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
			envelopeMsg(t, 0, 0, ceTypeChanged, capacityData{
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
			envelopeMsg(t, 0, 0, ceTypeChanged, capacityData{
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
			envelopeMsg(t, 0, 0, ceTypeChanged, capacityData{
				PathId: "pick", CutoffAt: cutoff, RemainingUnits: 100, Known: true,
			}),
			envelopeMsg(t, 0, 1, ceTypeChanged, capacityData{
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
			envelopeMsg(t, 0, 0, "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted", map[string]any{"shift_plan_id": "sp-1"}),
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
			envelopeMsg(t, 0, 1, ceTypeChanged, capacityData{
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

func TestConsumer_LegacyFlatEnvelope_RejectedAndSkipped(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	err := c.handle(legacyFlatMsg(t, 0, 0, "PathCapacityChanged", capacityData{PathId: "pick", CutoffAt: cutoff, RemainingUnits: 5, Known: true}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if _, known := c.Remaining(shared.PathId("pick"), "", cutoff); known {
		t.Fatal("legacy flat message must not populate the capacity cache")
	}
}

func TestConsumer_ShortTypeName_NotDispatched(t *testing.T) {
	cutoff := time.Date(2026, 9, 13, 18, 0, 0, 0, time.UTC)
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, "PathCapacityChanged", capacityData{PathId: "pick", CutoffAt: cutoff, RemainingUnits: 5, Known: true})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if _, known := c.Remaining(shared.PathId("pick"), "", cutoff); known {
		t.Fatal("a bare short type must not be dispatched")
	}
}
