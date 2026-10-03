package kafka_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
)

// projCall captures one projection-store method invocation.
type projCall struct {
	method        string
	eventID       string
	pathID        string
	at            time.Time
	promiseBasis  string
	cutoffAt      *time.Time
	splitShipment bool
}

// fakeProjection records the calls the consumer makes so a test can assert the
// envelope was routed to the right method with the right fields. It implements
// report.ProjectionStore.
type fakeProjection struct {
	calls []projCall
}

func (f *fakeProjection) record(method, eventID, pathID string, at time.Time) error {
	f.calls = append(f.calls, projCall{method: method, eventID: eventID, pathID: pathID, at: at})
	return nil
}

func (f *fakeProjection) recordPromise(method, eventID, pathID string, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) error {
	f.calls = append(f.calls, projCall{
		method: method, eventID: eventID, pathID: pathID, at: at,
		promiseBasis: basis, cutoffAt: cutoffAt, splitShipment: splitShipment,
	})
	return nil
}

func (f *fakeProjection) ApplyOrderReceived(_ context.Context, e, p string, at time.Time) error {
	return f.record("received", e, p, at)
}
func (f *fakeProjection) ApplyOrderAllocated(_ context.Context, e, p string, at time.Time, basis string, cutoffAt *time.Time, split bool) error {
	return f.recordPromise("allocated", e, p, at, basis, cutoffAt, split)
}
func (f *fakeProjection) ApplyOrderPartiallyAllocated(_ context.Context, e, p string, at time.Time, basis string, cutoffAt *time.Time, split bool) error {
	return f.recordPromise("partially", e, p, at, basis, cutoffAt, split)
}
func (f *fakeProjection) ApplyOrderAllocationFailed(_ context.Context, e, p string, at time.Time) error {
	return f.record("failed", e, p, at)
}
func (f *fakeProjection) ApplyOrderReleased(_ context.Context, e, p string, at time.Time) error {
	return f.record("released", e, p, at)
}
func (f *fakeProjection) ApplyOrderCancelled(_ context.Context, e, p string, at time.Time) error {
	return f.record("cancelled", e, p, at)
}
func (f *fakeProjection) ApplyLineAllocated(_ context.Context, e, p string, at time.Time) error {
	return f.record("line-allocated", e, p, at)
}
func (f *fakeProjection) ApplyLineBackordered(_ context.Context, e, p string, at time.Time) error {
	return f.record("line-backordered", e, p, at)
}
func (f *fakeProjection) ApplyLineReleased(_ context.Context, e, p string, at time.Time) error {
	return f.record("line-released", e, p, at)
}
func (f *fakeProjection) ApplyOrderRepromised(_ context.Context, e string, at time.Time) error {
	return f.record("repromised", e, "", at)
}

// fakeProcessed is an in-memory ProcessedEvents.
type fakeProcessed struct {
	seen map[string]bool
}

func newFakeProcessed() *fakeProcessed { return &fakeProcessed{seen: map[string]bool{}} }

func (p *fakeProcessed) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	if p.seen[eventID] {
		return false, nil
	}
	p.seen[eventID] = true
	return true, nil
}

const omTypePrefix = "com.warehouse.wes.order-management.order."

// analyticsEnvelope builds a CloudEvents 1.0 analytics event for eventName
// exactly as this service's own analytics publisher emits it.
func analyticsEnvelope(t *testing.T, eventID, eventName string, at time.Time, data map[string]any) []byte {
	t.Helper()
	return ceEvent(t, ceSpec{
		id: eventID, source: "/warehouse/order-management", ceType: omTypePrefix + eventName,
		subject: "o1", at: at, dataschema: "urn:warehouse:order-management:analytics:" + eventName + ":v1",
	}, data)
}

func TestAnalyticsConsumer_RoutesEachEventType(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		eventType  string
		wantMethod string
	}{
		{"received", "OrderReceived", "received"},
		{"allocated", "OrderAllocated", "allocated"},
		{"partially", "OrderPartiallyAllocated", "partially"},
		{"failed", "OrderAllocationPartiallyFailed", "failed"},
		{"released", "OrderReleased", "released"},
		{"cancelled", "OrderCancelled", "cancelled"},
		{"line-allocated", "OrderLineAllocated", "line-allocated"},
		{"line-backordered", "OrderLineBackordered", "line-backordered"},
		{"line-released", "OrderLineReleased", "line-released"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj := &fakeProjection{}
			processed := newFakeProcessed()
			c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

			raw := analyticsEnvelope(t, "e-"+tt.name, tt.eventType, at, map[string]any{"order_id": "o1", "path_id": "pick"})
			if err := c.HandleMessage(context.Background(), raw); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			if len(proj.calls) != 1 {
				t.Fatalf("calls = %d, want 1", len(proj.calls))
			}
			got := proj.calls[0]
			if got.method != tt.wantMethod {
				t.Errorf("method = %q, want %q", got.method, tt.wantMethod)
			}
			if got.pathID != "pick" {
				t.Errorf("pathID = %q, want pick", got.pathID)
			}
			if !got.at.Equal(at) {
				t.Errorf("at = %v, want %v", got.at, at)
			}
		})
	}
}

func TestAnalyticsConsumer_Idempotent(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := analyticsEnvelope(t, "dup", "OrderReceived", at, map[string]any{"order_id": "o1", "path_id": "pick"})
	for range 2 {
		if err := c.HandleMessage(context.Background(), raw); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if len(proj.calls) != 1 {
		t.Fatalf("expected 1 apply for duplicate delivery, got %d", len(proj.calls))
	}
}

func TestAnalyticsConsumer_IgnoresUnknownEventType(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := analyticsEnvelope(t, "e1", "SomethingElse", time.Now(), map[string]any{"order_id": "o1"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("expected unknown event to make no call, got %d", len(proj.calls))
	}
	// An event with no projection method must NOT be marked processed, so a
	// later contract change could reprocess it.
	if processed.seen["e1"] {
		t.Error("non-projecting event should not be marked processed")
	}
}

// TestAnalyticsConsumer_RoutesOrderRepromised covers ADR 0018/0019's
// path_id-free counter: OrderRepromised has no path_id in its payload, and
// the consumer must still route it to ApplyOrderRepromised (not silently
// drop it as unknown).
func TestAnalyticsConsumer_RoutesOrderRepromised(t *testing.T) {
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := analyticsEnvelope(t, "e-repromised", "OrderRepromised", at, map[string]any{
		"order_id": "o1", "cpt_id_old": "sp1-1200", "cpt_id_new": "sp1-1800", "reason": "TaskCPTMissed",
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(proj.calls))
	}
	got := proj.calls[0]
	if got.method != "repromised" {
		t.Errorf("method = %q, want repromised", got.method)
	}
	if !got.at.Equal(at) {
		t.Errorf("at = %v, want %v", got.at, at)
	}
}

// TestAnalyticsConsumer_RoutesPromiseKPIFields covers ADR 0014 §6:
// OrderAllocated's promise_basis/promise_cutoff_at/split_shipment fields
// must reach ApplyOrderAllocated alongside the funnel counter.
func TestAnalyticsConsumer_RoutesPromiseKPIFields(t *testing.T) {
	at := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	cutoff := at.Add(6 * time.Hour)
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	raw := analyticsEnvelope(t, "e-alloc", "OrderAllocated", at, map[string]any{
		"order_id": "o1", "path_id": "pick",
		"promise_basis": "Capability", "promise_cutoff_at": cutoff.Format(time.RFC3339Nano),
		"split_shipment": true,
	})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(proj.calls))
	}
	got := proj.calls[0]
	if got.promiseBasis != "Capability" {
		t.Errorf("promiseBasis = %q, want Capability", got.promiseBasis)
	}
	if got.cutoffAt == nil || !got.cutoffAt.Equal(cutoff) {
		t.Errorf("cutoffAt = %v, want %v", got.cutoffAt, cutoff)
	}
	if !got.splitShipment {
		t.Error("splitShipment = false, want true")
	}
}

// TestAnalyticsConsumer_RejectsLegacyFlatEnvelope proves the retired flat
// analytics envelope (event_id/event_type/occurred_at/schema_version) is
// rejected as a non-CloudEvent and never projected or marked processed.
func TestAnalyticsConsumer_RejectsLegacyFlatEnvelope(t *testing.T) {
	proj := &fakeProjection{}
	processed := newFakeProcessed()
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: processed, Logger: slog.Default()}

	err := c.HandleMessage(context.Background(), legacyFlatMessage("legacy-1", "OrderReceived", `{"order_id":"o1","path_id":"pick"}`))
	if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("err = %v, want ErrNotCloudEvent", err)
	}
	if len(proj.calls) != 0 || len(processed.seen) != 0 {
		t.Fatalf("legacy message must not be applied: calls=%d seen=%v", len(proj.calls), processed.seen)
	}
}

// TestAnalyticsConsumer_ShortTypeNotDispatched proves dispatch is on the
// FULL CloudEvents type: a valid CloudEvent whose type is the bare short
// name is ignored as unknown.
func TestAnalyticsConsumer_ShortTypeNotDispatched(t *testing.T) {
	proj := &fakeProjection{}
	c := &inboundkafka.AnalyticsConsumer{Projection: proj, Processed: newFakeProcessed(), Logger: slog.Default()}
	raw := ceEvent(t, ceSpec{id: "x", source: "/warehouse/order-management", ceType: "OrderReceived", subject: "o1", at: time.Now()}, map[string]any{"path_id": "pick"})
	if err := c.HandleMessage(context.Background(), raw); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if len(proj.calls) != 0 {
		t.Fatalf("short type must not dispatch, got %d calls", len(proj.calls))
	}
}
