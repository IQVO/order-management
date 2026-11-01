// Package usecases: allocation salvage, reservation reconfirmation and the release leg.
package usecases

import (
	"context"
	"errors"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// salvageAllocationFailure persists the partial allocation that genuinely
// happened before a hard (non-business) allocation failure, so a real
// upstream reservation is never stranded with nothing in this context able
// to revoke it, and publishes OrderAllocationPartiallyFailed for
// visibility. It returns nil when the partial progress was persisted —
// the hard failure itself travels to the caller via allocateAndRelease's
// reportErr, letting the transaction COMMIT the progress — and any save
// or publish failure is returned joined onto allocErr so
// errors.Is(err, allocErr) still holds.
func salvageAllocationFailure(ctx context.Context, deps allocationDeps, o *order.Order, outcome allocationOutcome, allocErr error) error {
	deps.setPromiseDate(o)
	if saveErr := deps.Orders.Save(ctx, o); saveErr != nil {
		return errors.Join(allocErr, saveErr)
	}
	// The genuinely-allocated lines carry site demand exactly as a clean
	// pass would; suppressing the projection here would hide real
	// reservations from the site. Joined like every other best-effort
	// step below so errors.Is(err, allocErr) still holds.
	newlyAllocated := linesWithStatus(o.Lines(), order.LineAllocated)
	if projErr := publishActiveDemand(ctx, deps, o, newlyAllocated); projErr != nil {
		return errors.Join(allocErr, projErr)
	}
	// Best-effort visibility: a failure publishing this event must never
	// mask or replace allocErr, the real failure — it is joined in
	// exactly like the saveErr case above so errors.Is(err, allocErr)
	// still holds.
	remaining := len(o.LinesWithStatus(order.LinePending))
	if pubErr := deps.Events.Publish(ctx, shared.NewOrderAllocationPartiallyFailed(
		deps.Clock.Now(), o.ID(), outcome.allocated, remaining, truncateCause(allocErr.Error()),
	)); pubErr != nil {
		return errors.Join(allocErr, pubErr)
	}
	return nil
}

// releaseAllocatedLines performs allocateAndRelease's release leg: when
// the aggregate passes the BR3 gate (o.EnsureReleasable), every line it
// currently reports Allocated is released via the pure domain transition
// o.Release, and its released-line detail — attributed to the specific
// PromiseGroup the line belongs to (ADR 0014 §3 / ADR 0017) — is
// collected for the integration event payload.
//
// A nil slice with a nil error means release did not run or had nothing
// to release: EnsureReleasable refusing a ship-complete order that still
// has unallocated lines is a gate, not a failure.
func releaseAllocatedLines(o *order.Order, promiseByLine map[int]order.PromiseGroup) ([]shared.ReleasedLine, error) {
	if err := o.EnsureReleasable(); err != nil {
		return nil, nil
	}
	class := o.FulfillmentClass().String()
	var released []shared.ReleasedLine
	for _, line := range o.LinesWithStatus(order.LineAllocated) {
		if err := o.Release(line.LineNo()); err != nil {
			return nil, err
		}
		rl := shared.ReleasedLine{
			LineNo: line.LineNo(), SKU: line.SKU(), PathID: line.PathID(), GiftWrap: line.GiftWrap(),
			FulfillmentClass: class,
		}
		if g, ok := promiseByLine[line.LineNo()]; ok {
			cutoffAt := g.Promise.CutoffAt
			basis := g.Promise.Basis.String()
			rl.PromiseCutoffAt = &cutoffAt
			rl.PromiseBasis = &basis
			if g.Promise.CptId != "" {
				cptId := g.Promise.CptId
				rl.PromiseCptId = &cptId
			}
		}
		released = append(released, rl)
	}
	return released, nil
}

// reconfirmAllocatedLines re-reserves every Allocated line NOT in justReserved
// right before release. inventory-storage reservations expire (30 min TTL by
// default), so a line allocated long ago -- an order that sat Backordered or
// held -- may no longer hold any stock. Re-reserving is idempotent while the
// reservation is still active (inventory-storage hands back the same one for
// the same demandRef, SKU and quantity) and takes a fresh one if it lapsed.
// If the stock is gone the line goes back to Backordered. It returns how many
// lines lost their reservation.
//
// Observed live (warehouse-day order #003): a ship-complete order waited
// Backordered for longer than the TTL; retry-allocation re-reserved only
// the backordered line and released all three, two of them on EXPIRED
// reservations, so the floor picked stock nothing held.
func reconfirmAllocatedLines(
	ctx context.Context,
	inventory ports.InventoryReservationClient,
	events ports.EventPublisher,
	clock ports.Clock,
	o *order.Order,
	justReserved []*order.OrderLine,
) (int, error) {
	skip := make(map[int]bool, len(justReserved))
	for _, l := range justReserved {
		skip[l.LineNo()] = true
	}
	lost := 0
	for _, line := range o.LinesWithStatus(order.LineAllocated) {
		if skip[line.LineNo()] {
			continue
		}
		result, err := inventory.Reserve(ctx, ports.ReservationRequest{
			SKU:       line.SKU(),
			Quantity:  line.Quantity(),
			DemandRef: o.ID(),
			LineNo:    line.LineNo(),
			Attempt:   o.Version(),
		})
		switch {
		case err == nil:
			if err := o.ReconfirmReservation(line.LineNo(), result.ReservationID); err != nil {
				return lost, err
			}
		case errors.Is(err, ports.ErrInsufficientStock):
			if err := o.LoseReservation(line.LineNo()); err != nil {
				return lost, err
			}
			lost++
			if err := events.Publish(ctx, shared.NewOrderLineBackordered(
				clock.Now(), o.ID(), line.LineNo(), line.SKU(), line.Quantity(),
			)); err != nil {
				return lost, err
			}
		default:
			return lost, err
		}
	}
	return lost, nil
}

// reconfirmBeforeRelease runs reconfirmAllocatedLines and, when a
// ship-complete order lost a reservation, saves and publishes the
// re-backordered state instead of releasing. blocked reports that the
// caller must stop (release is not allowed this pass).
func reconfirmBeforeRelease(ctx context.Context, deps allocationDeps, o *order.Order, justReserved []*order.OrderLine, outcome *allocationOutcome) (bool, error) {
	lost, err := reconfirmAllocatedLines(ctx, deps.Inventory, deps.Events, deps.Clock, o, justReserved)
	if err != nil {
		return true, err
	}
	outcome.backordered += lost
	if lost == 0 || o.AllowPartialShipment() {
		return false, nil
	}
	if err := deps.Orders.Save(ctx, o); err != nil {
		return true, err
	}
	return true, publishOrderAllocationOutcome(ctx, deps.Events, deps.Clock, o, *outcome, nil)
}
