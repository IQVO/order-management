package order_test

import (
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
)

// shipTestOrder builds one order and drives each line to the status the
// case names, using the aggregate's own transitions (Allocate/Release/
// Ship/Cancel) so no test reaches around the boundary. The statuses are
// given per line, in line order; len(statuses) == len(specs).
func shipTestOrder(t *testing.T, statuses ...order.LineStatus) *order.Order {
	t.Helper()
	specs := make([]lineSpec, len(statuses))
	for i := range statuses {
		specs[i] = lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"}
	}
	o := newOrder(t, true, specs...)
	for i, want := range statuses {
		switch want {
		case order.LineAllocated:
			mustShipAllocate(t, o, i+1)
		case order.LineReleased:
			mustShipAllocate(t, o, i+1)
			if err := o.Release(i + 1); err != nil {
				t.Fatalf("Release(%d): %v", i+1, err)
			}
		case order.LineShipped:
			mustShipAllocate(t, o, i+1)
			if err := o.Release(i + 1); err != nil {
				t.Fatalf("Release(%d): %v", i+1, err)
			}
			if _, err := o.Ship(i + 1); err != nil {
				t.Fatalf("Ship(%d): %v", i+1, err)
			}
		case order.LineBackordered:
			if err := o.MarkBackordered(i + 1); err != nil {
				t.Fatalf("MarkBackordered(%d): %v", i+1, err)
			}
		case order.LinePending:
			// nothing to do
		default:
			t.Fatalf("unsupported fixture status %q", want)
		}
	}
	return o
}

func mustShipAllocate(t *testing.T, o *order.Order, lineNo int) {
	t.Helper()
	if err := o.Allocate(lineNo, "res-1"); err != nil {
		t.Fatalf("Allocate(%d): %v", lineNo, err)
	}
}

func TestShipOnlyReleasedLineShips(t *testing.T) {
	tests := []struct {
		name    string
		status  order.LineStatus
		wantErr error
	}{
		{name: "pending line is rejected", status: order.LinePending, wantErr: order.ErrLineNotReleased},
		{name: "allocated line is rejected", status: order.LineAllocated, wantErr: order.ErrLineNotReleased},
		{name: "backordered line is rejected", status: order.LineBackordered, wantErr: order.ErrLineNotReleased},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := shipTestOrder(t, tt.status)
			moved, err := o.Ship(1)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Ship err = %v, want %v", err, tt.wantErr)
			}
			if moved {
				t.Fatal("Ship reported a move on a rejected line")
			}
			if o.Lines()[0].Status() != tt.status {
				t.Fatalf("rejected Ship changed the line to %q", o.Lines()[0].Status())
			}
		})
	}
}

func TestShipReleasedLineTransitionsAndIsIdempotent(t *testing.T) {
	o := shipTestOrder(t, order.LineReleased)
	moved, err := o.Ship(1)
	if err != nil {
		t.Fatalf("Ship: %v", err)
	}
	if !moved {
		t.Fatal("first Ship of a Released line must report the move")
	}
	if o.Lines()[0].Status() != order.LineShipped {
		t.Fatalf("line status = %q, want Shipped", o.Lines()[0].Status())
	}
	// Idempotent re-delivery: the second Ship of the same line is a
	// no-op success, not an error and not a second transition.
	moved, err = o.Ship(1)
	if err != nil {
		t.Fatalf("re-delivered Ship: %v", err)
	}
	if moved {
		t.Fatal("re-delivered Ship must report no move")
	}
	if o.Lines()[0].Status() != order.LineShipped {
		t.Fatalf("line status = %q, want Shipped after re-delivery", o.Lines()[0].Status())
	}
}

func TestShipUnknownLine(t *testing.T) {
	o := shipTestOrder(t, order.LineReleased)
	if _, err := o.Ship(9); !errors.Is(err, order.ErrLineNotFound) {
		t.Fatalf("Ship(9) err = %v, want ErrLineNotFound", err)
	}
}

func TestShipAllShipsEveryReleasedLine(t *testing.T) {
	o := shipTestOrder(t, order.LineReleased, order.LineShipped, order.LineReleased)
	shipped, err := o.ShipAll()
	if err != nil {
		t.Fatalf("ShipAll: %v", err)
	}
	if len(shipped) != 2 || shipped[0] != 1 || shipped[1] != 3 {
		t.Fatalf("shipped lines = %v, want [1 3]", shipped)
	}
	for _, l := range o.Lines() {
		if l.Status() != order.LineShipped {
			t.Fatalf("line %d status = %q, want Shipped", l.LineNo(), l.Status())
		}
	}
}

// TestShipAllMixedOrderShipsOnlyReleasedLines pins the partial path: a
// Backordered line neither blocks the released line nor transitions.
func TestShipAllMixedOrderShipsOnlyReleasedLines(t *testing.T) {
	o := shipTestOrder(t, order.LineReleased, order.LineBackordered)
	shipped, err := o.ShipAll()
	if err != nil {
		t.Fatalf("ShipAll: %v", err)
	}
	if len(shipped) != 1 || shipped[0] != 1 {
		t.Fatalf("shipped lines = %v, want [1]", shipped)
	}
	if o.Lines()[0].Status() != order.LineShipped || o.Lines()[1].Status() != order.LineBackordered {
		t.Fatalf("statuses = %q/%q, want Shipped/Backordered", o.Lines()[0].Status(), o.Lines()[1].Status())
	}
	if o.Status() != order.StatusPartiallyShipped {
		t.Fatalf("status = %q, want PartiallyShipped", o.Status())
	}
}

func TestShipAllRedeliveryChangesNothing(t *testing.T) {
	o := shipTestOrder(t, order.LineShipped, order.LineShipped)
	shipped, err := o.ShipAll()
	if err != nil {
		t.Fatalf("ShipAll on an already-shipped order: %v", err)
	}
	if len(shipped) != 0 {
		t.Fatalf("re-delivered ShipAll moved %v, want none", shipped)
	}
	if o.Status() != order.StatusShipped {
		t.Fatalf("status = %q, want Shipped", o.Status())
	}
}

func TestShipAllNotShippableByState(t *testing.T) {
	tests := []struct {
		name     string
		statuses []order.LineStatus
	}{
		{name: "received", statuses: []order.LineStatus{order.LinePending}},
		{name: "allocated", statuses: []order.LineStatus{order.LineAllocated}},
		{name: "backordered", statuses: []order.LineStatus{order.LineBackordered}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := shipTestOrder(t, tt.statuses...)
			shipped, err := o.ShipAll()
			if !errors.Is(err, order.ErrOrderNotShippable) {
				t.Fatalf("ShipAll err = %v, want ErrOrderNotShippable", err)
			}
			if shipped != nil {
				t.Fatalf("ShipAll moved %v on a not-shippable order", shipped)
			}
			if o.Lines()[0].Status() != tt.statuses[0] {
				t.Fatalf("not-shippable ShipAll changed the line to %q", o.Lines()[0].Status())
			}
		})
	}
}

func TestShipAllCancelledOrderNotShippable(t *testing.T) {
	o := shipTestOrder(t, order.LinePending, order.LinePending)
	if err := o.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := o.ShipAll(); !errors.Is(err, order.ErrOrderNotShippable) {
		t.Fatalf("ShipAll err = %v, want ErrOrderNotShippable", err)
	}
	if o.Status() != order.StatusCancelled {
		t.Fatalf("status = %q, want Cancelled", o.Status())
	}
}

// TestShippedStatusDerivation pins the Status() precedence table for the
// shipped half (ADR 0051), by derived order status.
func TestShippedStatusDerivation(t *testing.T) {
	t.Run("all lines shipped is Shipped", func(t *testing.T) {
		o := shipTestOrder(t, order.LineShipped, order.LineShipped)
		if o.Status() != order.StatusShipped {
			t.Fatalf("status = %q, want Shipped", o.Status())
		}
	})
	t.Run("shipped beside unshipped is PartiallyShipped", func(t *testing.T) {
		// BR6 forbids cancelling after release, so a cancelled line can
		// never sit beside a shipped one; the "some not shipped" side of
		// the rule is exercised with a still-pending line.
		specs := []lineSpec{{sku: "SKU-1", qty: 1, pathID: "pick"}, {sku: "SKU-2", qty: 1, pathID: "pick"}}
		o := newOrder(t, true, specs...)
		if err := o.Allocate(1, "res-1"); err != nil {
			t.Fatal(err)
		}
		if err := o.Release(1); err != nil {
			t.Fatal(err)
		}
		if _, err := o.Ship(1); err != nil {
			t.Fatal(err)
		}
		if o.Status() != order.StatusPartiallyShipped {
			t.Fatalf("status = %q, want PartiallyShipped with one shipped line and one pending", o.Status())
		}
	})
	t.Run("some shipped some released is PartiallyShipped", func(t *testing.T) {
		o := shipTestOrder(t, order.LineShipped, order.LineReleased)
		if o.Status() != order.StatusPartiallyShipped {
			t.Fatalf("status = %q, want PartiallyShipped", o.Status())
		}
	})
	t.Run("some shipped some allocated is PartiallyShipped", func(t *testing.T) {
		o := shipTestOrder(t, order.LineShipped, order.LineAllocated)
		if o.Status() != order.StatusPartiallyShipped {
			t.Fatalf("status = %q, want PartiallyShipped", o.Status())
		}
	})
	t.Run("all released nothing shipped stays Released", func(t *testing.T) {
		o := shipTestOrder(t, order.LineReleased, order.LineReleased)
		if o.Status() != order.StatusReleased {
			t.Fatalf("status = %q, want Released", o.Status())
		}
	})
	t.Run("ship all of a released order lands on Shipped", func(t *testing.T) {
		o := shipTestOrder(t, order.LineReleased, order.LineReleased)
		if _, err := o.ShipAll(); err != nil {
			t.Fatalf("ShipAll: %v", err)
		}
		if o.Status() != order.StatusShipped {
			t.Fatalf("status = %q, want Shipped", o.Status())
		}
	})
	t.Run("ship one of two released lands on PartiallyShipped then Shipped", func(t *testing.T) {
		o := shipTestOrder(t, order.LineReleased, order.LineReleased)
		if _, err := o.Ship(1); err != nil {
			t.Fatalf("Ship(1): %v", err)
		}
		if o.Status() != order.StatusPartiallyShipped {
			t.Fatalf("status = %q, want PartiallyShipped", o.Status())
		}
		if _, err := o.Ship(2); err != nil {
			t.Fatalf("Ship(2): %v", err)
		}
		if o.Status() != order.StatusShipped {
			t.Fatalf("status = %q, want Shipped", o.Status())
		}
	})
}

// TestReleasedLineCannotBeReallocatedAfterShipping pins that Shipped is
// terminal: the pre-release transitions reject a Shipped line by their
// own error names, so a late duplicate upstream fact cannot walk a
// shipped line backwards.
func TestReleasedLineCannotBeReallocatedAfterShipping(t *testing.T) {
	o := shipTestOrder(t, order.LineShipped)
	if err := o.Allocate(1, "res-2"); !errors.Is(err, order.ErrLineNotPending) {
		t.Fatalf("Allocate on shipped err = %v, want ErrLineNotPending", err)
	}
	if err := o.Release(1); !errors.Is(err, order.ErrLineNotAllocated) {
		t.Fatalf("Release on shipped err = %v, want ErrLineNotAllocated", err)
	}
	if err := o.MarkBackordered(1); !errors.Is(err, order.ErrLineNotPending) {
		t.Fatalf("MarkBackordered on shipped err = %v, want ErrLineNotPending", err)
	}
}
