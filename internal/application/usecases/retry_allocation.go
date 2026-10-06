package usecases

import (
	"context"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// RetryAllocation re-attempts allocation for the order's Backordered lines
// and ONLY those lines. It is the single sanctioned route from Backordered
// back to Allocated (Order.RetryAllocate) — nothing else in the system may
// perform that transition.
//
// It is also what unblocks a ship-complete order: while any line is
// Backordered, BR3 holds the whole order back from release, and a
// successful retry is the event that clears it.
//
// A line that is still short of stock stays Backordered; the same
// 409-is-a-business-fact / anything-else-is-ambiguous rule as allocation
// applies.
//
// Per the choreographed-release redesign (ADR 0005), RetryAllocation now
// ALSO attempts release in the same call once allocation succeeds — the
// same allocateAndRelease flow ReceiveOrder uses. UNLIKE ReceiveOrder,
// RetryAllocation is an explicit, on-purpose operator/recovery action: its
// caller's whole intent IS "attempt allocation (and, if that clears BR3,
// release) right now". So — unlike ReceiveOrder's best-effort, error-
// swallowing treatment of a hard failure — RetryAllocation keeps
// propagating a hard failure to its own caller exactly as it did
// pre-redesign, matching its existing error-return behaviour.
type RetryAllocation struct {
	Orders           ports.OrderRepo
	Inventory        ports.InventoryReservationClient
	Events           ports.EventPublisher
	Clock            ports.Clock
	Promise          order.PromisePolicy
	DemandProjection DemandProjectionPolicy
	UnitOfWork       ports.UnitOfWork
}

func (uc *RetryAllocation) Execute(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	o, err := uc.Orders.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrOrderNotFound
	}

	backordered := o.LinesWithStatus(order.LineBackordered)
	if len(backordered) == 0 {
		return nil, ErrNoBackorderedLines
	}

	deps := allocationDeps{Orders: uc.Orders, Inventory: uc.Inventory, Events: uc.Events, Clock: uc.Clock, Promise: uc.Promise, DemandProjection: uc.DemandProjection, UnitOfWork: uc.UnitOfWork}
	if _, err := allocateAndRelease(ctx, deps, o, backordered, true, o.ReleaseOnAllocation()); err != nil {
		return nil, err
	}
	return o, nil
}
