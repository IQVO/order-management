package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

func TestRetryAllocationClearsABackorderAndUnblocksShipComplete(t *testing.T) {
	f, o := backorderedFixture(t, false)
	delete(f.inventory.reserveErrBySKU, "SKU-2")
	reservesBeforeRetry := len(f.inventory.reserveCalls)

	got, err := f.retryAllocation().Execute(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Per the choreographed-release redesign, a successful retry that
	// clears the last backorder on a ship-complete order releases it in
	// the SAME call — there is no longer a separate release step.
	if got.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want %q", got.Status(), order.StatusReleased)
	}
	assertLineStatuses(t, got, order.LineReleased, order.LineReleased)
	assertEventNames(t, f.events, "OrderLineAllocated", "OrderAllocated", "OrderLineReleased", "OrderLineReleased", "OrderReleased")

	// The backordered line is retried, and the already-allocated line is
	// re-confirmed (not blindly trusted) before release: its reservation
	// may have expired upstream while the order waited on the backorder.
	// See release_reconfirm_test.go.
	if got := len(f.inventory.reserveCalls) - reservesBeforeRetry; got != 2 {
		t.Fatalf("retry made %d reserve calls, want 2 (retry the backordered line, re-confirm the allocated one)", got)
	}
	if first := f.inventory.reserveCalls[reservesBeforeRetry]; first.SKU != "SKU-2" {
		t.Fatalf("retry reserved %q first, want the backordered SKU-2", first.SKU)
	}

	// The promise date was recomputed over both allocated lines.
	want := now().Add(24 * time.Hour)
	if d := got.PromiseDate(); d == nil || !d.Equal(want) {
		t.Fatalf("PromiseDate() = %v, want %v", d, want)
	}
}

func TestRetryAllocationLeavesAStillShortLineBackordered(t *testing.T) {
	f, o := backorderedFixture(t, false)

	got, err := f.retryAllocation().Execute(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got.Status() != order.StatusBackordered {
		t.Fatalf("Status() = %q, want %q", got.Status(), order.StatusBackordered)
	}
	assertLineStatuses(t, got, order.LineAllocated, order.LineBackordered)
	assertEventNames(t, f.events, "OrderLineBackordered")
}

// On a partial-shipment order, backorderedFixture's line 1 is already
// Released by the time ReceiveOrder returns (EnsureReleasable always
// permits release on a partial-shipment order). A retry that still cannot
// clear line 2's backorder republishes OrderPartiallyAllocated reflecting
// that reality: the order overall reads PartiallyReleased.
func TestRetryAllocationPartialShipmentRepublishesPartiallyAllocated(t *testing.T) {
	f, o := backorderedFixture(t, true)
	assertLineStatuses(t, o, order.LineReleased, order.LineBackordered)

	got, err := f.retryAllocation().Execute(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.Status() != order.StatusPartiallyReleased {
		t.Fatalf("Status() = %q, want %q", got.Status(), order.StatusPartiallyReleased)
	}
	assertEventNames(t, f.events, "OrderLineBackordered", "OrderPartiallyAllocated")
}

func TestRetryAllocationErrorPaths(t *testing.T) {
	t.Run("unknown order", func(t *testing.T) {
		f := newFixture()
		_, err := f.retryAllocation().Execute(context.Background(), "ord-missing")
		if !errors.Is(err, usecases.ErrOrderNotFound) {
			t.Fatalf("err = %v, want %v", err, usecases.ErrOrderNotFound)
		}
	})

	t.Run("repository lookup fails", func(t *testing.T) {
		f := newFixture()
		f.orders = &failingRepo{inner: f.orders, findErr: errBoom}
		_, err := f.retryAllocation().Execute(context.Background(), "ord-1")
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})

	t.Run("an order with no backordered line is rejected", func(t *testing.T) {
		f := newFixture()
		o := f.mustReceive(t, false, line("SKU-1", 1, "pick"))

		_, err := f.retryAllocation().Execute(context.Background(), o.ID())
		if !errors.Is(err, usecases.ErrNoBackorderedLines) {
			t.Fatalf("err = %v, want %v", err, usecases.ErrNoBackorderedLines)
		}
		if len(f.inventory.reserveCalls) != 1 {
			t.Fatalf("a rejected retry must not call inventory-storage again, calls = %d", len(f.inventory.reserveCalls))
		}
	})

	t.Run("fails closed on a non-business error", func(t *testing.T) {
		f, o := backorderedFixture(t, true)
		f.inventory.reserveErrBySKU["SKU-2"] = errBoom

		_, err := f.retryAllocation().Execute(context.Background(), o.ID())
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}

		stored, getErr := f.getOrder().Execute(context.Background(), o.ID())
		if getErr != nil {
			t.Fatalf("GetOrder: %v", getErr)
		}
		// Line 1 was already Released by ReceiveOrder's implicit release
		// (partial shipment); line 2's retry never got past Reserve, so
		// it remains Backordered, unchanged.
		assertLineStatuses(t, stored, order.LineReleased, order.LineBackordered)
	})

	t.Run("save fails", func(t *testing.T) {
		f, o := backorderedFixture(t, true)
		delete(f.inventory.reserveErrBySKU, "SKU-2")
		f.orders = &failingRepo{inner: f.orders, saveErr: errBoom}

		_, err := f.retryAllocation().Execute(context.Background(), o.ID())
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})

	t.Run("save fails while persisting partial progress after a hard failure", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErr = ports.ErrInsufficientStock
		o := f.mustReceive(t, true, line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"))
		// Both lines are fully backordered at this point (universal
		// reserveErr) — nothing was released since nothing allocated.
		assertLineStatuses(t, o, order.LineBackordered, order.LineBackordered)

		// Line 1 now retries successfully; line 2 hits a hard failure.
		f.inventory.reserveErr = nil
		f.inventory.reserveErrBySKU["SKU-2"] = errBoom
		f.orders = &failingRepo{inner: f.orders, saveErr: errBoom}

		_, err := f.retryAllocation().Execute(context.Background(), o.ID())
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})

	t.Run("publisher fails", func(t *testing.T) {
		f, o := backorderedFixture(t, true)
		delete(f.inventory.reserveErrBySKU, "SKU-2")
		f.events.err = errBoom

		_, err := f.retryAllocation().Execute(context.Background(), o.ID())
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})
}
