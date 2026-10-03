package order_test

import (
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
)

func TestReconfirmReservation_ReplacesTheReservationOfAnAllocatedLine(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"})
	if err := o.Allocate(1, "res-old"); err != nil {
		t.Fatal(err)
	}
	if err := o.ReconfirmReservation(1, "res-new"); err != nil {
		t.Fatalf("ReconfirmReservation: %v", err)
	}
	l := o.Lines()[0]
	if l.Status() != order.LineAllocated || l.ReservationID() == nil || *l.ReservationID() != "res-new" {
		t.Fatalf("line = %s/%v, want Allocated/res-new", l.Status(), l.ReservationID())
	}
}

func TestReconfirmAndLoseReservation_OnlyApplyToAllocatedLines(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(o *order.Order)
	}{
		{"pending", func(*order.Order) {}},
		{"backordered", func(o *order.Order) { _ = o.MarkBackordered(1) }},
		{"released", func(o *order.Order) { _ = o.Allocate(1, "r"); _ = o.Release(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"})
			tc.setup(o)
			if err := o.ReconfirmReservation(1, "x"); !errors.Is(err, order.ErrLineNotAllocated) {
				t.Fatalf("ReconfirmReservation err = %v, want ErrLineNotAllocated", err)
			}
			if err := o.LoseReservation(1); !errors.Is(err, order.ErrLineNotAllocated) {
				t.Fatalf("LoseReservation err = %v, want ErrLineNotAllocated", err)
			}
		})
	}
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"})
	if err := o.ReconfirmReservation(9, "x"); err == nil {
		t.Fatal("unknown line must be rejected")
	}
	if err := o.LoseReservation(9); err == nil {
		t.Fatal("unknown line must be rejected")
	}
}

func TestLoseReservation_BackordersTheLineAndBlocksShipComplete(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"}, lineSpec{sku: "SKU-2", qty: 1, pathID: "pick"})
	_ = o.Allocate(1, "r1")
	_ = o.Allocate(2, "r2")
	if err := o.LoseReservation(1); err != nil {
		t.Fatalf("LoseReservation: %v", err)
	}
	l := o.Lines()[0]
	if l.Status() != order.LineBackordered || l.ReservationID() != nil {
		t.Fatalf("line = %s/%v, want Backordered with no reservation", l.Status(), l.ReservationID())
	}
	if o.Lines()[1].Status() != order.LineAllocated {
		t.Fatal("the other line must be untouched")
	}
	if err := o.EnsureReleasable(); !errors.Is(err, order.ErrShipCompleteBlocked) {
		t.Fatalf("EnsureReleasable err = %v, want ErrShipCompleteBlocked", err)
	}
	if err := o.RetryAllocate(1, "r1b"); err != nil {
		t.Fatalf("a lost line must be retryable: %v", err)
	}
}
