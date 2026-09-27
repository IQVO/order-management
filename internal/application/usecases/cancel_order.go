package usecases

import (
	"context"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// CancelOrder revokes every allocated line's reservation on
// inventory-storage (DELETE /reservations/{id}) and cancels the order.
//
// BR6: cancellation is legal ONLY while no line has reached Released. The
// boundary is checked BEFORE any reservation is revoked, so a rejected
// cancellation leaves inventory-storage completely untouched.
//
// v1 does not claw back work already released to wes-work-planning. That
// is a deliberate, documented gap — see ADR 0004 — not an oversight: this
// context has no compensating command on that Supplier's published
// contract to call.
type CancelOrder struct {
	Orders     ports.OrderRepo
	Inventory  ports.InventoryReservationClient
	Events     ports.EventPublisher
	Clock      ports.Clock
	UnitOfWork ports.UnitOfWork
}

func (uc *CancelOrder) Execute(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	o, err := uc.Orders.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrOrderNotFound
	}

	// Check the release boundary first: no upstream side effect happens
	// for an order that may not be cancelled.
	if err := o.EnsureCancellable(); err != nil {
		return nil, err
	}

	reservationIDs := o.AllocatedReservationIDs()
	for _, reservationID := range reservationIDs {
		if err := uc.Inventory.RevokeReservation(ctx, reservationID); err != nil {
			// Fail closed: a reservation this context believes it holds
			// could not be given back, so the order is NOT marked
			// cancelled. Retrying the cancellation is safe — the lines
			// are untouched and the ids are still recorded.
			return nil, err
		}
	}

	if err := o.Cancel(); err != nil {
		return nil, err
	}
	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewOrderCancelled(uc.Clock.Now(), o.ID(), len(reservationIDs)))
	}); err != nil {
		return nil, err
	}
	return o, nil
}
