package kafkacatalog

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

	"github.com/claudioed/order-management/internal/domain/processpath"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fakeReader replays a fixed sequence of messages, then blocks until ctx
// is cancelled -- mirrors a real Kafka reader that has caught up and is
// now waiting for new messages.
type fakeReader struct {
	messages []kafkago.Message
	pos      int
	closed   bool
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

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

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
		Reader:  reader,
		paths:   make(map[string]processpath.PathDefinition),
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

func TestConsumer_Run_BecomesReadyAfterCatchingUpSinglePartition(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, pathData{PathId: "PICK", MatchPrefix: "pick"}),
			envelopeMsg(t, 0, 1, ceTypeCreated, pathData{PathId: "PACK", MatchPrefix: "pack"}),
		},
	}
	// target[0] = 2 means "caught up once offset 1 has been processed"
	// (last is exclusive: 2 messages at offsets 0 and 1).
	c := newTestConsumer(reader, targetOffsets{0: 2})

	if c.Ready() {
		t.Fatal("expected not ready before Run starts")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx)
		close(done)
	}()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}
	cancel()
	<-done

	if !c.IsActive(shared.PathId("pick")) {
		t.Fatal("expected PICK to be active")
	}
	if !c.IsActive(shared.PathId("pack")) {
		t.Fatal("expected PACK to be active")
	}
}

func TestConsumer_MultiPartition_ReadyOnlyAfterBothCaughtUp(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, pathData{PathId: "PICK", MatchPrefix: "pick"}),
			// Partition 1 not yet caught up (target[1]=1, need offset 0).
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1, 1: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err == nil {
		t.Fatal("expected WaitReady to time out -- partition 1 never caught up")
	}
}

func TestConsumer_Deactivated_RemovesPathFromCache(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, pathData{PathId: "PICK", MatchPrefix: "pick"}),
			envelopeMsg(t, 0, 1, ceTypeDeactivated, pathData{PathId: "PICK"}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	if c.IsActive(shared.PathId("pick")) {
		t.Fatal("expected PICK to be inactive after deactivation")
	}
}

func TestConsumer_Revised_UpdatesMatchPrefix(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, pathData{PathId: "PICK", MatchPrefix: "pick"}),
			envelopeMsg(t, 0, 1, ceTypeUpdated, pathData{PathId: "PICK", MatchPrefix: "pick-zone-a"}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout, got: %v", err)
	}

	if !c.IsActive(shared.PathId("pick-zone-a")) {
		t.Fatal("expected pick-zone-a to resolve as active after revision")
	}
	// The revision REPLACED matchPrefix from "pick" to "pick-zone-a", so
	// the bare old prefix no longer matches -- this is expected, not a
	// bug: MatchPrefix is a full replacement on ProcessPathUpdated, not
	// an additive widening.
	if c.IsActive(shared.PathId("pick")) {
		t.Fatal("expected bare pick to no longer resolve after its matchPrefix was replaced")
	}
}

func TestConsumer_DestinationLocationRole_DecodesFromRealPublishedPayloadShape(t *testing.T) {
	// Proves this consumer decodes destination_location_role off a
	// process-path-management ProcessPathCreated payload shaped exactly
	// like that service's own real published wire format (see
	// process-path-management's kafka.ProcessPathData / publisher_test.go
	// TestPublish_ProcessPathCreated_WithDestinationLocationRole_IsOnTheWire)
	// -- this repo never imports that service's Go types, so the message
	// below is built from a raw map, not a struct, to exercise the wire
	// contract rather than any shared Go type.
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
				"path_id":                   "PACK",
				"match_prefix":              "pack",
				"cycle_time_p95":            "2h0m0s",
				"destination_location_role": "Drop",
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

	def, err := c.lookup(shared.PathId("pack"))
	if err != nil {
		t.Fatalf("lookup(pack): %v", err)
	}
	if def.DestinationLocationRole != "Drop" {
		t.Fatalf("DestinationLocationRole = %q, want %q", def.DestinationLocationRole, "Drop")
	}
}

func TestConsumer_DestinationLocationRole_OmittedOnWire_DecodesAsUnset(t *testing.T) {
	// A path that never declared a destination role omits the field
	// entirely on the wire (process-path-management's own omitempty
	// discipline) -- this must decode as the empty string, not error or
	// panic, and must not be confused with "unknown path".
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, ceTypeCreated, map[string]any{
				"path_id":      "PICK",
				"match_prefix": "pick",
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

	def, err := c.lookup(shared.PathId("pick"))
	if err != nil {
		t.Fatalf("lookup(pick): %v", err)
	}
	if def.DestinationLocationRole != "" {
		t.Fatalf("DestinationLocationRole = %q, want empty (unset)", def.DestinationLocationRole)
	}
}

func TestConsumer_UnknownEventType_IsIgnored(t *testing.T) {
	reader := &fakeReader{
		messages: []kafkago.Message{
			envelopeMsg(t, 0, 0, "com.warehouse.wes.process-path-management.processpath.SomeFutureEvent", map[string]any{}),
		},
	}
	c := newTestConsumer(reader, targetOffsets{0: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("expected Ready before timeout even for an unrecognized event type, got: %v", err)
	}
}

func TestConsumer_IsActive_UnknownPath_ReturnsFalse(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if c.IsActive(shared.PathId("never-declared")) {
		t.Fatal("expected an undeclared path id to be inactive")
	}
}

// TestConsumer_ListActive_EmptyCache_ReturnsEmptySlice (ADR-0021): a
// not-yet-populated cache must fail open with an empty slice, never nil
// treated as an error and never a panic on the caller's range loop.
func TestConsumer_ListActive_EmptyCache_ReturnsEmptySlice(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	got := c.ListActive()
	if len(got) != 0 {
		t.Fatalf("ListActive() = %v, want empty", got)
	}
}

// TestConsumer_ListActive_ReturnsEveryUpsertedPath (ADR-0021): every path
// upserted via ProcessPathCreated/Updated is enumerable, carrying the
// same CycleTimeP95/Eligibility IsActive/CycleTimeP95/Eligibility would
// each report individually for that id.
func TestConsumer_ListActive_ReturnsEveryUpsertedPath(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})

	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, pathData{
		PathId: "PICK", MatchPrefix: "pick", CycleTimeP95: "30m0s",
		Eligibility: &eligibilityData{},
	})); err != nil {
		t.Fatalf("handle PICK: %v", err)
	}
	if err := c.handle(envelopeMsg(t, 0, 1, ceTypeCreated, pathData{
		PathId: "SINGLES", MatchPrefix: "singles", CycleTimeP95: "10m0s",
		Eligibility: &eligibilityData{},
	})); err != nil {
		t.Fatalf("handle SINGLES: %v", err)
	}

	got := c.ListActive()
	if len(got) != 2 {
		t.Fatalf("ListActive() returned %d entries, want 2: %+v", len(got), got)
	}
	byID := map[shared.PathId]shared.ActivePathCandidate{}
	for _, a := range got {
		byID[a.PathId] = a
	}
	pick, ok := byID["PICK"]
	if !ok || !pick.CycleTimeKnown || pick.CycleTimeP95 != 30*time.Minute {
		t.Fatalf("ListActive() PICK entry = %+v, want cycleTime=30m known=true", pick)
	}
	singles, ok := byID["SINGLES"]
	if !ok || !singles.CycleTimeKnown || singles.CycleTimeP95 != 10*time.Minute {
		t.Fatalf("ListActive() SINGLES entry = %+v, want cycleTime=10m known=true", singles)
	}
}

// TestConsumer_ListActive_DeactivatedPath_IsExcluded (ADR-0021): a
// deactivated path must disappear from ListActive, the same as it already
// disappears from IsActive/CycleTimeP95/Eligibility.
func TestConsumer_ListActive_DeactivatedPath_IsExcluded(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})

	if err := c.handle(envelopeMsg(t, 0, 0, ceTypeCreated, pathData{
		PathId: "PICK", MatchPrefix: "pick", CycleTimeP95: "30m0s",
		Eligibility: &eligibilityData{},
	})); err != nil {
		t.Fatalf("handle create: %v", err)
	}
	if err := c.handle(envelopeMsg(t, 0, 1, ceTypeDeactivated, pathData{PathId: "PICK"})); err != nil {
		t.Fatalf("handle deactivate: %v", err)
	}

	if got := c.ListActive(); len(got) != 0 {
		t.Fatalf("ListActive() = %v, want empty after deactivation", got)
	}
}

func TestConsumer_LegacyFlatEnvelope_RejectedAndSkipped(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	err := c.handle(legacyFlatMsg(t, 0, 0, "ProcessPathCreated", pathData{PathId: "PICK", MatchPrefix: "pick"}))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if c.IsActive(shared.PathId("pick-1")) {
		t.Fatal("legacy flat message must not populate the catalogue")
	}

	// A legacy message in the stream is skipped and the consumer still
	// catches up and applies the CloudEvents message behind it.
	reader := &fakeReader{messages: []kafkago.Message{
		legacyFlatMsg(t, 0, 0, "ProcessPathCreated", pathData{PathId: "PICK", MatchPrefix: "pick"}),
		envelopeMsg(t, 0, 1, ceTypeCreated, pathData{PathId: "PACK", MatchPrefix: "pack"}),
	}}
	c = newTestConsumer(reader, targetOffsets{0: 2})
	c.Logger = slog.Default()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = c.Run(ctx) }()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if !c.IsActive(shared.PathId("pack-1")) || c.IsActive(shared.PathId("pick-1")) {
		t.Fatal("expected only the CloudEvents path to be applied")
	}
}

func TestConsumer_ShortTypeName_NotDispatched(t *testing.T) {
	c := newTestConsumer(&fakeReader{}, targetOffsets{})
	if err := c.handle(envelopeMsg(t, 0, 0, "ProcessPathCreated", pathData{PathId: "PICK", MatchPrefix: "pick"})); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if c.IsActive(shared.PathId("pick-1")) {
		t.Fatal("a bare short type must not be dispatched")
	}
}
