// Package usecases: allocation and release event publication.
package usecases

import (
	"context"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// maxCauseLen bounds OrderAllocationPartiallyFailed.Cause so a verbose
// upstream error (or an accidentally-embedded response body) never turns a
// visibility event into an unbounded log payload.
const maxCauseLen = 500

// truncateCause defensively bounds an error string for safe logging. It
// never returns more than maxCauseLen runes.
func truncateCause(s string) string {
	r := []rune(s)
	if len(r) <= maxCauseLen {
		return s
	}
	return string(r[:maxCauseLen]) + "…"
}

// publishOrderAllocationOutcome emits the order-level event that matches
// the aggregate's derived status after an allocation-then-release pass. A
// fully backordered order emits no order-level event: its per-line
// OrderLineBackordered facts already carry the whole story.
//
// StatusReleased/StatusPartiallyReleased are included alongside
// StatusAllocated/StatusPartiallyAllocated because allocateAndRelease runs
// release in the SAME pass, right after allocation succeeds: by the time
// this function reads o.Status(), a ship-complete order that just became
// fully allocated has typically already been carried through to
// StatusReleased, and a partial-shipment order to StatusPartiallyReleased.
// OrderAllocated/OrderPartiallyAllocated are the SAME facts as before,
// just now additionally carrying the lines released in this pass — they
// are not renamed, because from an integration consumer's point of view
// "this order's allocation pass concluded, and here is what was released
// as part of it" is exactly the same milestone as pre-redesign
// OrderAllocated, just enriched.
func publishOrderAllocationOutcome(
	ctx context.Context,
	events ports.EventPublisher,
	clock ports.Clock,
	o *order.Order,
	outcome allocationOutcome,
	released []shared.ReleasedLine,
) error {
	// Nothing to say only when this pass neither changed allocation NOR
	// released anything. A held order's release (ADR 0020) re-enters with
	// every line already Allocated -- allocated == 0 -- but releases them;
	// wes-work-planning learns about released lines only from this event's
	// Lines, so suppressing it left released held orders with no work ever
	// created (they never shipped).
	if outcome.allocated == 0 && outcome.backordered == 0 && len(released) == 0 {
		return nil
	}

	var promiseDate time.Time
	if d := o.PromiseDate(); d != nil {
		promiseDate = *d
	}
	var promiseCptId string
	if id := o.PromiseCptId(); id != nil {
		promiseCptId = *id
	}
	var promiseBasis string
	if b := o.PromiseBasis(); b != nil {
		promiseBasis = b.String()
	}

	switch o.Status() {
	case order.StatusAllocated, order.StatusReleased:
		return events.Publish(ctx, shared.NewOrderAllocatedWithPromise(clock.Now(), o.ID(), promiseDate, promiseCptId, promiseBasis, released))
	case order.StatusPartiallyAllocated, order.StatusPartiallyReleased:
		return events.Publish(ctx, shared.NewOrderPartiallyAllocatedWithPromise(
			clock.Now(), o.ID(), outcome.allocated, outcome.backordered, promiseDate, promiseCptId, promiseBasis, released,
		))
	default:
		return nil
	}
}

// publishReleasePassEvents publishes everything a release-on-allocation pass
// announces once the aggregate is saved: the order-level outcome event
// (OrderAllocated/OrderPartiallyAllocated, carrying the released lines for
// wes-work-planning) followed by the analytics release facts.
func publishReleasePassEvents(
	ctx context.Context,
	deps allocationDeps,
	o *order.Order,
	outcome allocationOutcome,
	released []shared.ReleasedLine,
) error {
	if err := publishOrderAllocationOutcome(ctx, deps.Events, deps.Clock, o, outcome, released); err != nil {
		return err
	}
	return publishReleaseFacts(ctx, deps.Events, deps.Clock, o, released)
}

// publishReleaseFacts raises the release facts for the lines released in
// THIS pass — the real Allocated -> Released transition performed by
// releaseAllocatedLines. One OrderLineReleased per line released now (never
// for a line released in an earlier pass), then OrderReleased when that pass
// left every line of the order Released. Both are analytics-only (the
// integration topic still carries just OrderAllocated/OrderPartiallyAllocated/
// OrderRepromised) and feed the Order Funnel's linesReleased/ordersReleased
// (ADR 0006). They are published inside the same atomically() scope as the
// aggregate Save, so with the transactional outbox they commit with it.
//
// released is empty — and nothing is raised — when the release leg did not
// run or released nothing: a BR3-blocked ship-complete order, a held order, or
// a held order whose reconfirm lost a reservation.
func publishReleaseFacts(
	ctx context.Context,
	events ports.EventPublisher,
	clock ports.Clock,
	o *order.Order,
	released []shared.ReleasedLine,
) error {
	if len(released) == 0 {
		return nil
	}
	for _, rl := range released {
		if err := events.Publish(ctx, shared.NewOrderLineReleased(
			clock.Now(), o.ID(), rl.LineNo, rl.PathID, WorkUnitID(o.ID(), rl.LineNo),
		)); err != nil {
			return err
		}
	}
	if o.Status() == order.StatusReleased {
		return events.Publish(ctx, shared.NewOrderReleased(clock.Now(), o.ID()))
	}
	return nil
}

// publishActiveDemand emits an ACTIVE projection only for lines allocated in
// the current pass, each with its OWN promise-group cutoff as due_at. It
// runs after the aggregate Save but inside the same UnitOfWork, so the
// order mutation and its outbox rows commit together.
func publishActiveDemand(ctx context.Context, deps allocationDeps, o *order.Order, lines []*order.OrderLine) error {
	return deps.DemandProjection.publishForGroups(ctx, deps.Events, deps.Clock.Now(), o, lines, shared.SiteSkuDemandActive)
}
