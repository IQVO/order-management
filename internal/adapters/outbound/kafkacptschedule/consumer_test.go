package kafkacptschedule

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
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
	e.SetSource("/warehouse/process-path-management")
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
		Reader:    reader,
		schedules: make(map[string]scheduleData),
		readyCh:   make(chan struct{}),
		target:    target,
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

func TestConsumer_AppliesCPTScheduleChanged(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeChanged, scheduleData{
				SiteId:   "site-1",
				Timezone: "UTC",
				Cutoffs: []cutoffData{
					{CptId: "sp1-1800", LocalTime: "18:00", EligiblePathIds: []string{"pick"}},
				},
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

	from := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)
	windows, known := c.NextCutoffs("site-1", from, 1)
	if !known {
		t.Fatal("expected known=true for a site with a replayed schedule")
	}
	if len(windows) != 1 || windows[0].CptId != "sp1-1800" {
		t.Fatalf("unexpected windows: %+v", windows)
	}
}

func TestConsumer_NextCutoffs_UnknownSite(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	_, known := c.NextCutoffs("never-seen", time.Now(), 3)
	if known {
		t.Fatal("expected known=false for a site with no replayed schedule")
	}
}

func TestConsumer_LaterCPTScheduleChangedReplacesEarlierSnapshot(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeChanged, scheduleData{
				SiteId: "site-1", Timezone: "UTC",
				Cutoffs: []cutoffData{{CptId: "old", LocalTime: "18:00"}},
			}),
			envelopeMsg(t, 0, 1, ceTypeChanged, scheduleData{
				SiteId: "site-1", Timezone: "UTC",
				Cutoffs: []cutoffData{{CptId: "new", LocalTime: "20:00"}},
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

	from := time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)
	windows, known := c.NextCutoffs("site-1", from, 5)
	if !known {
		t.Fatal("expected known=true")
	}
	for _, w := range windows {
		if w.CptId == "old" {
			t.Fatalf("expected the later CPTScheduleChanged to fully replace the schedule, still found %q", w.CptId)
		}
	}
}

func TestConsumer_UnknownEventType_IsIgnored(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated", map[string]any{"path_id": "PICK"}),
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

func TestConsumer_LegacyFlatEnvelope_RejectedAndSkipped(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	err := c.handle(legacyFlatMsg(t, 0, 0, "CPTScheduleChanged", scheduleData{SiteId: "sp1", Timezone: "UTC"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if _, ok := c.NextCutoffs("sp1", time.Now(), 1); ok {
		t.Fatal("legacy flat message must not populate the schedule cache")
	}
}

func TestConsumer_ShortTypeName_NotDispatched(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, "CPTScheduleChanged", scheduleData{SiteId: "sp1", Timezone: "UTC"})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if _, ok := c.NextCutoffs("sp1", time.Now(), 1); ok {
		t.Fatal("a bare short type must not be dispatched")
	}
}
