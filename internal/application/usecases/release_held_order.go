package usecases

import (
	"context"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ReleaseHeldOrder releases the already-allocated lines of an order
// received with releaseOnAllocation=false (ADR 0020 §1). It is the
// second half of the hold: intake allocates and stops, a caller decides,
// and this commits that decision to the floor.
//
// It reuses allocateAndRelease's release leg rather than reimplementing
// release, by calling it with an EMPTY line set to allocate and
// releaseOnAllocation=true. Nothing is allocated (the lines are already
// Allocated), BR3 is enforced by the same EnsureReleasable gate as every
// other release, the promise is recomputed by the same policy, and the
// same OrderAllocated/OrderReleased outcome event fires with the same
// per-line payload. A second implementation of release would be a second
// thing to keep in sync with ADR 0017's promise-group payload.
//
// IDEMPOTENT for the case that matters: a HELD order whose lines are
// already Released returns success with no state change and no event.
// The caller may be retrying after a network failure or a lost response,
// and must not be punished for it — an external caller holding a
// fill-or-kill deadline cannot distinguish "my release was lost" from
// "my release failed", so the safe action for it must be to retry.
//
// An order that was never held is a different question and answers 409,
// even though it too has no Allocated lines: that is a caller with the
// wrong id or the wrong mental model, and the two cases must not share
// an answer.
//
// It deliberately does NOT clear the hold flag. The flag records what
// the order was received as, not where it is now; line statuses already
// carry the current state. Clearing it would make the order's history
// unreadable and would break the retry-safety it exists to provide.
type ReleaseHeldOrder struct {
	Orders     ports.OrderRepo
	Inventory  ports.InventoryReservationClient
	Events     ports.EventPublisher
	Clock      ports.Clock
	Promise    order.PromisePolicy
	UnitOfWork ports.UnitOfWork
}

func (uc *ReleaseHeldOrder) Execute(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	o, err := uc.Orders.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrOrderNotFound
	}

	// An order that was never held has nothing to release on demand: its
	// lines release themselves at allocation. Reaching here means the
	// caller has the wrong order id or a wrong mental model, and silently
	// returning success would hide that.
	//
	// Checked BEFORE the idempotency branch, deliberately. The two
	// branches overlap for every unheld order, because an unheld order's
	// lines are Released (or Backordered) and never Allocated — so with
	// the idempotency check first, this one was unreachable for anything
	// intake can produce, and "release an order that was never held"
	// answered 200. Verified against the running cluster: a plain order
	// returned 200 from POST /orders/{id}/release, which is precisely
	// the wrong mental model this error exists to surface.
	//
	// Ordering them this way costs nothing for the case idempotency
	// protects: a HELD order that was already released still reaches the
	// branch below and still returns success, because its flag is false.
	if o.ReleaseOnAllocation() {
		return nil, ErrOrderNotHeld
	}

	// Already released: succeed without touching anything. The caller may
	// be retrying after a network failure or a lost response, and must
	// not be punished for it — an external caller holding a fill-or-kill
	// deadline cannot distinguish "my release was lost" from "my release
	// failed", so the safe action for it must be to retry.
	if len(o.LinesWithStatus(order.LineAllocated)) == 0 {
		return o, nil
	}

	deps := allocationDeps{Orders: uc.Orders, Inventory: uc.Inventory, Events: uc.Events, Clock: uc.Clock, Promise: uc.Promise, UnitOfWork: uc.UnitOfWork}
	if _, err := allocateAndRelease(ctx, deps, o, nil, false, true); err != nil {
		return nil, err
	}
	return o, nil
}
