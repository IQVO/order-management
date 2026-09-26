package usecases_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fakeProcessedEvents is a scripted ports.RepromiseProcessedEvents. By
// default every event_id is new; alreadyProcessed pre-seeds ids that
// must report isNew=false, and err makes every call fail (an
// infrastructure failure, distinct from "already processed").
type fakeProcessedEvents struct {
	mu               sync.Mutex
	alreadyProcessed map[string]bool
	err              error
	calls            []string
}

func newFakeProcessedEvents() *fakeProcessedEvents {
	return &fakeProcessedEvents{alreadyProcessed: map[string]bool{}}
}

func (f *fakeProcessedEvents) MarkProcessed(_ context.Context, eventId string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, eventId)
	if f.err != nil {
		return false, f.err
	}
	if f.alreadyProcessed[eventId] {
		return false, nil
	}
	f.alreadyProcessed[eventId] = true
	return true, nil
}

// repromiseFixture bundles a real ReceiveOrder-created, fully-released
// order plus the fake adapters RepromiseOrder needs, so tests exercise
// the real domain PromiseGroups() breakdown rather than a hand-built
// stub.
type repromiseFixture struct {
	f         *fixture
	processed *fakeProcessedEvents
}

func newRepromiseFixture() *repromiseFixture {
	return &repromiseFixture{f: newFixture(), processed: newFakeProcessedEvents()}
}

func (rf *repromiseFixture) repromiseOrder(promise order.PromisePolicy) *usecases.RepromiseOrder {
	return &usecases.RepromiseOrder{
		Orders:    rf.f.orders,
		Promise:   promise,
		Events:    rf.f.events,
		Clock:     rf.f.clock,
		Processed: rf.processed,
	}
}

func TestRepromiseOrder_PromiseMoved_PublishesOrderRepromisedAndUpdatesGroups(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if o.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusReleased)
	}
	original := o.PromiseGroups()
	if len(original) != 1 {
		t.Fatalf("original PromiseGroups = %d, want 1: %+v", len(original), original)
	}
	originalCutoff := original[0].Promise.CutoffAt

	// A capability input change since intake: "pick" now has a SHORTER
	// lead time (as if a faster path/schedule were now in force). Same
	// shape of PromisePolicy (fallback only, no Schedule/Capability —
	// exactly what a real order-management deployment has today), a
	// genuinely different result.
	movedLeadTime := order.NewLeadTimePolicy(6*time.Hour, nil)
	movedPromise := order.PromisePolicy{Fallback: movedLeadTime}

	err := rf.repromiseOrder(movedPromise).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	stored, err := rf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	updated := stored.PromiseGroups()
	if len(updated) != 1 {
		t.Fatalf("updated PromiseGroups = %d, want 1: %+v", len(updated), updated)
	}
	wantCutoff := rf.f.clock.Now().Add(6 * time.Hour)
	if !updated[0].Promise.CutoffAt.Equal(wantCutoff) {
		t.Errorf("updated cutoff = %v, want %v", updated[0].Promise.CutoffAt, wantCutoff)
	}
	if updated[0].Promise.CutoffAt.Equal(originalCutoff) {
		t.Fatal("cutoff did not actually move; test fixture is not discriminating")
	}

	repromised := findOrderRepromised(t, rf.f.events)
	if repromised.OrderID != o.ID() {
		t.Errorf("OrderRepromised.OrderID = %q, want %q", repromised.OrderID, o.ID())
	}
	if repromised.Reason != "TaskCPTMissed" {
		t.Errorf("OrderRepromised.Reason = %q, want %q", repromised.Reason, "TaskCPTMissed")
	}
	// Both old and new promises are LeadTime-basis: neither has a CPT
	// identity, so both are empty strings — the move is visible via
	// the domain's persisted CutoffAt, not via this event's CptId
	// fields, exactly as ADR 0014's Promise value object documents.
	if repromised.CptIdOld != "" || repromised.CptIdNew != "" {
		t.Errorf("OrderRepromised CptIdOld/New = %q/%q, want both empty for a LeadTime-basis promise", repromised.CptIdOld, repromised.CptIdNew)
	}
}

func TestRepromiseOrder_PromiseUnchanged_NoEventPublished(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))

	// Identical policy shape to what produced the order's promise at
	// intake: nothing has actually changed, so nothing should move.
	err := rf.repromiseOrder(rf.f.promise).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "PackageManifested",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertEventNames(t, rf.f.events) // no events published
}

func TestRepromiseOrder_AlreadyProcessed_NoOp(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	rf.processed.alreadyProcessed["evt-1"] = true

	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)}
	err := rf.repromiseOrder(movedPromise).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertEventNames(t, rf.f.events) // no events published, no re-evaluation
}

func TestRepromiseOrder_OrderNotFound_NoOp(t *testing.T) {
	rf := newRepromiseFixture()
	err := rf.repromiseOrder(order.PromisePolicy{}).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: "ord-does-not-exist", LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v, want nil (fail-soft)", err)
	}
	assertEventNames(t, rf.f.events)
}

func TestRepromiseOrder_LineNotInAnyCurrentGroup_NoOp(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))

	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)}
	// Line 99 does not exist on this order at all.
	err := rf.repromiseOrder(movedPromise).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 99, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v, want nil (fail-soft)", err)
	}
	assertEventNames(t, rf.f.events)
}

func TestRepromiseOrder_NoFreshPromiseAvailable_NoOp(t *testing.T) {
	rf := newRepromiseFixture()

	// Build an order directly (not through ReceiveOrder) whose line is
	// still Pending — never allocated — but which already carries a
	// (now stale) persisted PromiseGroup breakdown covering that line,
	// mirroring the only way PromisePolicy.PromiseGroups' own ok=false
	// branch is reachable: no line currently allocated at all.
	l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	staleCutoff := rf.f.clock.Now().Add(24 * time.Hour)
	o, err := order.New("ord-stale-1", []*order.OrderLine{l}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: order.Promise{CutoffAt: staleCutoff, Basis: order.BasisLeadTime}}})
	if err := rf.f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	err = rf.repromiseOrder(order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)}).
		Execute(context.Background(), usecases.RepromiseOrderRequest{
			SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
		})
	if err != nil {
		t.Fatalf("Execute: %v, want nil (fail-soft)", err)
	}
	assertEventNames(t, rf.f.events)

	// The stale group breakdown must be untouched — nothing was saved.
	stored, err := rf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	if got := stored.PromiseGroups()[0].Promise.CutoffAt; !got.Equal(staleCutoff) {
		t.Errorf("PromiseGroups()[0].Promise.CutoffAt = %v, want unchanged %v", got, staleCutoff)
	}
}

func TestRepromiseOrder_ProcessedMarkError_ReturnsError(t *testing.T) {
	rf := newRepromiseFixture()
	rf.processed.err = errBoom
	err := rf.repromiseOrder(order.PromisePolicy{}).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: "ord-1", LineNo: 1, Reason: "TaskCPTMissed",
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Execute err = %v, want errBoom", err)
	}
}

func TestRepromiseOrder_SaveError_ReturnsError(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))

	failing := &failingRepo{inner: rf.f.orders, saveErr: errBoom}
	uc := &usecases.RepromiseOrder{
		Orders: failing, Promise: order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)},
		Events: rf.f.events, Clock: rf.f.clock, Processed: rf.processed,
	}
	err := uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Execute err = %v, want errBoom", err)
	}
}

func TestRepromiseOrder_PublishError_ReturnsError(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	rf.f.events.failAfter(0, errBoom)

	uc := &usecases.RepromiseOrder{
		Orders: rf.f.orders, Promise: order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)},
		Events: rf.f.events, Clock: rf.f.clock, Processed: rf.processed,
	}
	err := uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Execute err = %v, want errBoom", err)
	}
}

// findOrderRepromised returns the first shared.OrderRepromised event
// published to p, failing the test if none was published.
func findOrderRepromised(t *testing.T, p *recordingPublisher) shared.OrderRepromised {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if r, ok := e.(shared.OrderRepromised); ok {
			return r
		}
	}
	t.Fatalf("no OrderRepromised event was published; events = %v", p.names())
	return shared.OrderRepromised{}
}

var _ ports.RepromiseProcessedEvents = (*fakeProcessedEvents)(nil)

// recordingSlogHandler captures every record a use case emits so a test
// can assert the fail-soft paths log a Warn with their event_id —
// "logged and treated as a no-op" is RepromiseOrder's stated contract.
type recordingSlogHandler struct {
	records []slog.Record
}

func (h *recordingSlogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingSlogHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *recordingSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }

func (h *recordingSlogHandler) WithGroup(name string) slog.Handler { return h }

// warnMessages returns the message of every WARN-level record captured.
func (h *recordingSlogHandler) warnMessages() []string {
	out := make([]string, 0, len(h.records))
	for _, r := range h.records {
		if r.Level == slog.LevelWarn {
			out = append(out, r.Message)
		}
	}
	return out
}

// TestRepromiseOrder_FailSoftPaths_LogWarn pins the fail-soft contract:
// every "signal doesn't map to a live, promotable line" condition is
// logged at WARN (with the event_id) and returned as nil, never as an
// error. A nil Logger stays silent — that case is exercised by every
// other test in this file.
func TestRepromiseOrder_FailSoftPaths_LogWarn(t *testing.T) {
	tests := []struct {
		name    string
		setUp   func(t *testing.T, rf *repromiseFixture) usecases.RepromiseOrderRequest
		wantMsg string
	}{
		{
			name: "event already processed",
			setUp: func(t *testing.T, rf *repromiseFixture) usecases.RepromiseOrderRequest {
				o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
				rf.processed.alreadyProcessed["evt-1"] = true
				return usecases.RepromiseOrderRequest{SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1}
			},
			wantMsg: "repromise: event already processed, skipping",
		},
		{
			name: "order not found",
			setUp: func(t *testing.T, rf *repromiseFixture) usecases.RepromiseOrderRequest {
				return usecases.RepromiseOrderRequest{SourceEventId: "evt-1", OrderId: "ord-nope", LineNo: 1}
			},
			wantMsg: "repromise: order not found, skipping",
		},
		{
			name: "line not in any current group",
			setUp: func(t *testing.T, rf *repromiseFixture) usecases.RepromiseOrderRequest {
				o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
				return usecases.RepromiseOrderRequest{SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 99}
			},
			wantMsg: "repromise: line not found in any current promise group, skipping",
		},
		{
			name: "no fresh promise available",
			setUp: func(t *testing.T, rf *repromiseFixture) usecases.RepromiseOrderRequest {
				// An order with no allocated line anymore but a stale
				// persisted breakdown covering it — the only shape under
				// which PromiseGroups' own ok=false is reachable.
				l, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
				if err != nil {
					t.Fatalf("NewOrderLine: %v", err)
				}
				o, err := order.New("ord-no-fresh", []*order.OrderLine{l}, false)
				if err != nil {
					t.Fatalf("order.New: %v", err)
				}
				o.SetPromiseGroups([]order.PromiseGroup{{
					LineNos: []int{1},
					Promise: order.Promise{CutoffAt: rf.f.clock.Now().Add(24 * time.Hour), Basis: order.BasisLeadTime},
				}})
				if err := rf.f.orders.Save(context.Background(), o); err != nil {
					t.Fatalf("Save: %v", err)
				}
				return usecases.RepromiseOrderRequest{SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1}
			},
			wantMsg: "repromise: no fresh promise available for this order, skipping",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := newRepromiseFixture()
			handler := &recordingSlogHandler{}
			req := tt.setUp(t, rf)
			uc := rf.repromiseOrder(order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)})
			uc.Logger = slog.New(handler)

			if err := uc.Execute(context.Background(), req); err != nil {
				t.Fatalf("Execute: %v, want nil (fail-soft)", err)
			}
			msgs := handler.warnMessages()
			if len(msgs) != 1 || msgs[0] != tt.wantMsg {
				t.Fatalf("warn messages = %v, want exactly [%q]", msgs, tt.wantMsg)
			}
			assertEventNames(t, rf.f.events) // fail-soft never publishes
		})
	}
}

// TestRepromiseOrder_LineMissingFromFreshGroups_NoOp covers the branch
// where the line IS in a persisted group but a recompute no longer
// produces a group for it (here: the line was cancelled after the
// breakdown was persisted, so PromiseGroups covers only the surviving
// allocated line). That is a stale-signal fact: logged, no-op, no event.
func TestRepromiseOrder_LineMissingFromFreshGroups_NoOp(t *testing.T) {
	rf := newRepromiseFixture()
	handler := &recordingSlogHandler{}

	// Partial-shipment order: line 1 allocated, line 2 cancelled, with a
	// stale persisted breakdown that still covers BOTH lines (built
	// directly, mirroring the only way this state arises: the breakdown
	// predates the cancellation).
	allocated := order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LineAllocated, nil)
	cancelled := order.RehydrateOrderLine(2, "SKU-2", 1, "pick", false, order.LineCancelled, nil)
	o, err := order.New("ord-stale-groups", []*order.OrderLine{allocated, cancelled}, true)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	o.SetPromiseGroups([]order.PromiseGroup{{
		LineNos: []int{1, 2},
		Promise: order.Promise{CutoffAt: rf.f.clock.Now().Add(24 * time.Hour), Basis: order.BasisLeadTime},
	}})
	if err := rf.f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	uc := rf.repromiseOrder(order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)})
	uc.Logger = slog.New(handler)
	err = uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 2, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v, want nil (fail-soft)", err)
	}

	msgs := handler.warnMessages()
	if len(msgs) != 1 || msgs[0] != "repromise: line not found in the freshly recomputed promise groups, skipping" {
		t.Fatalf("warn messages = %v, want the fresh-groups skip message", msgs)
	}
	assertEventNames(t, rf.f.events)

	// The stale breakdown is untouched.
	stored, err := rf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	groups := stored.PromiseGroups()
	if len(groups) != 1 || len(groups[0].LineNos) != 2 {
		t.Fatalf("stored groups = %+v, want the original single 2-line group untouched", groups)
	}
}

// TestRepromiseOrder_FindError_ReturnsError proves an infrastructure
// failure reading the order back is a genuine error (commit-and-skip
// applies), never a fail-soft no-op.
func TestRepromiseOrder_FindError_ReturnsError(t *testing.T) {
	rf := newRepromiseFixture()
	uc := &usecases.RepromiseOrder{
		Orders:  &findFails{inner: rf.f.orders, findErr: errBoom},
		Promise: order.PromisePolicy{Fallback: order.NewLeadTimePolicy(1*time.Hour, nil)},
		Events:  rf.f.events, Clock: rf.f.clock, Processed: rf.processed,
	}
	err := uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: "ord-1", LineNo: 1, Reason: "TaskCPTMissed",
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Execute err = %v, want errBoom", err)
	}
}
