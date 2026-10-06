package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// The OrderLineReleased / OrderReleased facts were declared (domain events,
// AsyncAPI, analytics projector) but never raised, so the Order Funnel's
// linesReleased/ordersReleased were always 0. They are raised at the one real
// release transition — the release leg of allocateAndRelease — in the same
// UnitOfWork scope as the aggregate save, so the outbox carries them.

func countEvents(p *recordingPublisher, name string) int {
	n := 0
	for _, got := range p.names() {
		if got == name {
			n++
		}
	}
	return n
}

func lineReleasedEvents(t *testing.T, p *recordingPublisher) []shared.OrderLineReleased {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []shared.OrderLineReleased
	for _, e := range p.events {
		if ev, ok := e.(shared.OrderLineReleased); ok {
			out = append(out, ev)
		}
	}
	return out
}

func TestReceiveOrder_ShipComplete_RaisesLineAndOrderReleased(t *testing.T) {
	f := newFixture()
	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 2, "pick"),
	}, false)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}

	if got := countEvents(f.events, "OrderLineReleased"); got != 2 {
		t.Fatalf("OrderLineReleased = %d, want 2 (events: %v)", got, f.events.names())
	}
	if got := countEvents(f.events, "OrderReleased"); got != 1 {
		t.Fatalf("OrderReleased = %d, want exactly 1 (events: %v)", got, f.events.names())
	}

	// The line event carries the frozen work-unit id contract and the
	// line's own path.
	for i, ev := range lineReleasedEvents(t, f.events) {
		lineNo := i + 1
		if ev.OrderID != o.ID() || ev.LineNo != lineNo {
			t.Fatalf("event %d = %+v, want order %s line %d", i, ev, o.ID(), lineNo)
		}
		if want := usecases.WorkUnitID(o.ID(), lineNo); ev.WorkUnitID != want {
			t.Fatalf("WorkUnitID = %q, want %q", ev.WorkUnitID, want)
		}
		if ev.PathID != o.Lines()[i].PathID() {
			t.Fatalf("PathID = %q, want the line's own path %q", ev.PathID, o.Lines()[i].PathID())
		}
		if !ev.OccurredAt().Equal(now()) {
			t.Fatalf("OccurredAt = %v, want the clock's now %v", ev.OccurredAt(), now())
		}
	}

	// OrderReleased is the LAST fact of the pass: every line event precedes it.
	names := f.events.names()
	if names[len(names)-1] != "OrderReleased" {
		t.Fatalf("events = %v, want OrderReleased last", names)
	}
}

func TestReceiveOrder_PartialShipment_RaisesLineReleasedButNotOrderReleasedUntilComplete(t *testing.T) {
	f := newFixture()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, true)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	assertLineStatuses(t, o, order.LineReleased, order.LineBackordered)

	if got := countEvents(f.events, "OrderLineReleased"); got != 1 {
		t.Fatalf("OrderLineReleased = %d, want 1 (events: %v)", got, f.events.names())
	}
	if got := countEvents(f.events, "OrderReleased"); got != 0 {
		t.Fatalf("OrderReleased = %d, want 0: line 2 is still backordered, so not every line is released (events: %v)", got, f.events.names())
	}

	// Stock arrives: the retry releases the last line, and ONLY now is the
	// order fully released. Line 1 must not be announced a second time.
	f.events.reset()
	delete(f.inventory.reserveErrBySKU, "SKU-2")
	if _, err := f.retryAllocation().Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("RetryAllocation: %v", err)
	}
	released := lineReleasedEvents(t, f.events)
	if len(released) != 1 || released[0].LineNo != 2 {
		t.Fatalf("OrderLineReleased after retry = %+v, want exactly line 2", released)
	}
	if got := countEvents(f.events, "OrderReleased"); got != 1 {
		t.Fatalf("OrderReleased after retry = %d, want 1 (events: %v)", got, f.events.names())
	}
}

func TestReceiveOrder_ShipCompleteBlockedByBR3_RaisesNoReleaseFacts(t *testing.T) {
	f := newFixture()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, false)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	assertLineStatuses(t, o, order.LineAllocated, order.LineBackordered)

	if n := countEvents(f.events, "OrderLineReleased") + countEvents(f.events, "OrderReleased"); n != 0 {
		t.Fatalf("a BR3-blocked order released nothing, but release facts were raised: %v", f.events.names())
	}
}

func TestReceiveOrder_Held_RaisesNoReleaseFactsUntilReleaseHeldOrder(t *testing.T) {
	f, o := heldOrderFixture(t)
	if n := countEvents(f.events, "OrderLineReleased") + countEvents(f.events, "OrderReleased"); n != 0 {
		t.Fatalf("a held order put nothing on the floor, but release facts were raised: %v", f.events.names())
	}

	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock,
		Inventory: f.inventory, Promise: f.promise,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("ReleaseHeldOrder: %v", err)
	}
	if got := countEvents(f.events, "OrderLineReleased"); got != 1 {
		t.Fatalf("OrderLineReleased after release = %d, want 1 (events: %v)", got, f.events.names())
	}
	if got := countEvents(f.events, "OrderReleased"); got != 1 {
		t.Fatalf("OrderReleased after release = %d, want 1 (events: %v)", got, f.events.names())
	}

	// A second release is the existing silent no-op: no duplicate facts.
	afterFirst := len(f.events.names())
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("second ReleaseHeldOrder: %v", err)
	}
	if n := len(f.events.names()); n != afterFirst {
		t.Fatalf("second release raised %v, want nothing", f.events.names()[afterFirst:])
	}
}

func TestReceiveOrder_LostReservationAtReconfirm_RaisesNoReleaseFacts(t *testing.T) {
	// A held ship-complete order that lost its reservation before release is
	// re-backordered by the reconfirm step: nothing is released, so nothing
	// may be announced as released.
	f, o := heldOrderFixture(t)
	f.inventory.reserveErr = ports.ErrInsufficientStock
	f.events.reset()

	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock,
		Inventory: f.inventory, Promise: f.promise,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("ReleaseHeldOrder: %v", err)
	}
	if n := countEvents(f.events, "OrderLineReleased") + countEvents(f.events, "OrderReleased"); n != 0 {
		t.Fatalf("nothing was released, but release facts were raised: %v", f.events.names())
	}
}

func TestReceiveOrder_ReleaseFactPublishFailure_DoesNotFailReceiveOrderNorRaiseOrderReleased(t *testing.T) {
	// The release facts join the allocation pass: a failure publishing one is
	// handled like any other publish failure in the best-effort leg (ReceiveOrder
	// still succeeds) and OrderReleased — the LAST fact — is never raised after it.
	f := newFixture()
	f.events.failAfter(5, errBoom) // OrderReceived, LineAllocated x2, OrderAllocated, then the first OrderLineReleased fails
	if _, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, false); err != nil {
		t.Fatalf("ReceiveOrder must still succeed (best-effort allocation leg): %v", err)
	}
	if got := countEvents(f.events, "OrderReleased"); got != 0 {
		t.Fatalf("OrderReleased = %d after a failed OrderLineReleased publish, want 0", got)
	}
}
