package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Regression: releasing a held order released its lines on the aggregate
// but published NOTHING. ReleaseHeldOrder re-enters allocateAndRelease with
// every line already Allocated, so this pass allocates 0 lines and
// publishOrderAllocationOutcome's "nothing allocated, nothing to say"
// early return swallowed the release. wes-work-planning learns about
// released lines only from OrderAllocated.Lines, so a released held order
// never became work and never shipped -- observed live in the warehouse-day
// simulation (held orders #006 and #023: Released in order-management,
// 0 work units in WES, reservation left ACTIVE forever).
func TestReleaseHeldOrder_PublishesTheReleasedLines(t *testing.T) {
	f, o := heldOrderFixture(t)
	before := len(f.events.names())

	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock,
		Inventory: f.inventory, Promise: f.promise,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var released []shared.ReleasedLine
	f.events.mu.Lock()
	for _, e := range f.events.events[before:] {
		if a, ok := e.(shared.OrderAllocated); ok {
			released = append(released, a.Lines...)
		}
	}
	f.events.mu.Unlock()
	if len(released) != 1 {
		t.Fatalf("OrderAllocated after the release carried %d released lines, want 1 (events since release: %v)", len(released), f.events.names()[before:])
	}
}

// Releasing an already-released held order stays a silent no-op: no
// duplicate work may be published.
func TestReleaseHeldOrder_SecondReleasePublishesNothing(t *testing.T) {
	f, o := heldOrderFixture(t)
	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock,
		Inventory: f.inventory, Promise: f.promise,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("first release: %v", err)
	}
	afterFirst := len(f.events.names())
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if n := len(f.events.names()); n != afterFirst {
		t.Fatalf("second release published %v, want nothing", f.events.names()[afterFirst:])
	}
}
