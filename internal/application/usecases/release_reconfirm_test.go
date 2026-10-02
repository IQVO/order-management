package usecases_test

import (
	"context"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// Regression: inventory-storage reservations expire (30 min TTL). A
// ship-complete order that sat Backordered for longer than that still held
// "Allocated" lines whose reservations had silently EXPIRED; retry-allocation
// re-reserved ONLY the backordered line and then released the whole order,
// putting work on the floor for stock nothing held any more. Observed live
// (warehouse-day order #003): lines 1-2 released with EXPIRED reservations,
// picked anyway (confirm-pick -> 409 reservation-already-resolved).
//
// Before releasing, every already-Allocated line must be re-confirmed
// against inventory-storage (idempotent while the reservation is still
// active: inventory-storage hands back the same reservation for the same
// demandRef+sku+quantity).
func TestRetryAllocation_ReconfirmsAlreadyAllocatedLinesBeforeRelease(t *testing.T) {
	f, o := backorderedFixture(t, false)
	delete(f.inventory.reserveErrBySKU, "SKU-2")
	before := len(f.inventory.reserveCalls)

	got, err := f.retryAllocation().Execute(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	calls := f.inventory.reserveCalls[before:]
	skus := map[string]bool{}
	for _, c := range calls {
		skus[c.SKU.String()] = true
	}
	if !skus["SKU-1"] || !skus["SKU-2"] {
		t.Fatalf("retry reserved %v, want both the backordered line AND the already-allocated line re-confirmed", skus)
	}
	if got.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want Released", got.Status())
	}
	// The released line must carry the reservation inventory-storage
	// confirmed NOW, not the one taken at intake.
	l1 := got.Lines()[0]
	if l1.ReservationID() == nil || *l1.ReservationID() == "res-1" {
		t.Fatalf("line 1 still carries the intake reservation %v; want the re-confirmed one", l1.ReservationID())
	}
}

// If an expired line's stock is gone by release time, it is backordered
// again and a ship-complete order releases nothing.
func TestRetryAllocation_LostReservationBackordersTheLineAndBlocksShipComplete(t *testing.T) {
	f, o := backorderedFixture(t, false)
	delete(f.inventory.reserveErrBySKU, "SKU-2")
	f.inventory.reserveErrBySKU["SKU-1"] = ports.ErrInsufficientStock

	got, err := f.retryAllocation().Execute(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if n := len(got.LinesWithStatus(order.LineReleased)); n != 0 {
		t.Fatalf("released %d lines, want 0: ship-complete with a lost reservation must not release", n)
	}
	if got.Lines()[0].Status() != order.LineBackordered {
		t.Fatalf("line 1 status %q, want Backordered (its reservation was lost)", got.Lines()[0].Status())
	}
}

// A held order's release re-confirms its reservations too.
func TestReleaseHeldOrder_ReconfirmsReservations(t *testing.T) {
	f, o := heldOrderFixture(t)
	before := len(f.inventory.reserveCalls)
	uc := &usecases.ReleaseHeldOrder{Orders: f.orders, Events: f.events, Clock: f.clock, Inventory: f.inventory, Promise: f.promise}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if n := len(f.inventory.reserveCalls) - before; n != 1 {
		t.Fatalf("release made %d reserve calls, want 1 (re-confirming the held line)", n)
	}
}
