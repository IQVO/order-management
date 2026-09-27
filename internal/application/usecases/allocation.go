// Package usecases: shared allocation and release machinery.
//
// Before the choreographed-release redesign, allocation and release were
// two separately invoked public use cases (AllocateOrder, ReleaseOrder)
// driven by their own REST verbs. That exposed internal saga-step
// mechanics as public API: a caller had to know to call /allocate then
// /release, when all it actually wants is to place an order. Both are now
// folded into ONE flow — allocateAndRelease — invoked from inside
// ReceiveOrder (implicitly, right after intake) and RetryAllocation
// (explicitly, as an operator/recovery action). See ADR 0005.
package usecases

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// allocationOutcome counts what one allocation pass achieved.
type allocationOutcome struct {
	allocated   int
	backordered int
}

// allocateLines runs the reserve-per-line loop shared by every caller that
// attempts allocation. retry selects which domain transition is legal for
// the lines being processed: Order.Allocate for Pending lines, and
// Order.RetryAllocate — the only route out of Backordered — for retries.
//
// The 409-vs-everything-else distinction is the heart of this function:
//
//   - 409 (ports.ErrInsufficientStock) is a BUSINESS FACT — that specific
//     line is Backordered, and allocation continues with the next line.
//   - a transport failure, a 5xx, or any other non-2xx is NOT a business
//     fact. It is ambiguous: the reservation may or may not exist upstream.
//     The whole call fails; no line is silently marked Backordered.
func allocateLines(
	ctx context.Context,
	inventory ports.InventoryReservationClient,
	events ports.EventPublisher,
	clock ports.Clock,
	o *order.Order,
	lines []*order.OrderLine,
	retry bool,
) (allocationOutcome, error) {
	var outcome allocationOutcome

	for _, line := range lines {
		result, err := inventory.Reserve(ctx, ports.ReservationRequest{
			SKU:       line.SKU(),
			Quantity:  line.Quantity(),
			DemandRef: o.ID(),
		})

		switch {
		case err == nil:
			// fall through to the allocate transition below

		case errors.Is(err, ports.ErrInsufficientStock):
			if err := o.MarkBackordered(line.LineNo()); err != nil {
				return outcome, err
			}
			outcome.backordered++
			if err := events.Publish(ctx, shared.NewOrderLineBackordered(
				clock.Now(), o.ID(), line.LineNo(), line.SKU(), line.Quantity(),
			)); err != nil {
				return outcome, err
			}
			continue

		default:
			// Ambiguous: not a business fact. Fail closed.
			return outcome, err
		}

		transition := o.Allocate
		if retry {
			transition = o.RetryAllocate
		}
		if err := transition(line.LineNo(), result.ReservationID); err != nil {
			return outcome, err
		}
		outcome.allocated++
		if err := events.Publish(ctx, shared.NewOrderLineAllocated(
			clock.Now(), o.ID(), line.LineNo(), line.SKU(), line.Quantity(), result.ReservationID,
		)); err != nil {
			return outcome, err
		}
	}

	return outcome, nil
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
	if outcome.allocated == 0 && outcome.backordered == 0 {
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

// allocationDeps bundles the outbound dependencies allocateAndRelease
// needs. ReceiveOrder and RetryAllocation each pass their own struct
// fields through one value rather than a long positional parameter list.
//
// Promise is order.PromisePolicy (ADR 0014), which computes a
// capability-derived CPT-window promise when the underlying inputs are
// available, falling back to its own embedded LeadTimePolicy otherwise.
// A zero-value PromisePolicy (Schedule/Capability both nil) always falls
// back, so existing wiring/tests that only set Fallback keep working
// unchanged.
//
// UnitOfWork brackets every Save + Publish allocateAndRelease makes in
// one atomic scope (transactional outbox). Optional: nil means "no
// transactional backing", the in-memory / log-publisher dev
// configuration — atomically() treats that identically to every other
// use case in this package.
type allocationDeps struct {
	Orders     ports.OrderRepo
	Inventory  ports.InventoryReservationClient
	Events     ports.EventPublisher
	Clock      ports.Clock
	Promise    order.PromisePolicy
	UnitOfWork ports.UnitOfWork
}

// setPromiseDate applies deps.Promise (PromisePolicy, ADR 0014/ADR 0017)
// to o, recording the full per-shipment-group promise breakdown via
// Order.SetPromiseGroups.
//
// DESIGN DECISION (documented per the task brief): this always calls the
// new PromiseGroups/SetPromiseGroups path, for BOTH ship-complete and
// partial-shipment orders, rather than branching on
// o.AllowPartialShipment() to keep calling the old Promise/SetPromise
// path for ship-complete orders. This is deliberate and smaller-diff:
// PromisePolicy.PromiseGroups degrades to exactly one group covering
// every allocated line for AllowPartialShipment=false, computed via the
// UNCHANGED Promise(now, o) search (see promise_policy.go's doc
// comment), and Order.SetPromiseGroups called with that single group
// reproduces SetPromise's own field assignment byte for byte. The
// OUTCOME for a ship-complete order is therefore provably identical
// either way; calling one method here (rather than two branches, one of
// which would need to keep being manually kept in sync with the other)
// is the smaller, more obviously-correct change against this call site.
func (deps allocationDeps) setPromiseDate(o *order.Order) {
	// ADR 0020 §2: an order carrying an externally-dictated deadline is
	// promised by FeasibleBy, not by PromiseGroups. This is the routing
	// that makes FeasibleBy reachable at all — without it the method
	// exists but nothing in the service ever calls it, which was true
	// between phase 2 and this change.
	//
	// The two differ in KIND, not just in inputs. PromiseGroups asks
	// "what is the earliest we can manage?" and always answers with
	// something, falling back to lead-time when capability data is
	// missing. FeasibleBy asks "is there a window at or before a date
	// somebody else set?" and answers false rather than guessing,
	// because its true is a fill-or-kill commitment an external party
	// measures us on.
	//
	// An infeasible order is therefore left with NO promise, deliberately.
	// The alternative — writing an optimistic promise we already know
	// breaks the deadline — would tell the caller we can do something we
	// cannot, and the caller's whole reason for sending a deadline is to
	// find out before committing.
	if deadline := o.RequiredShipBy(); deadline != nil {
		if p, ok := deps.Promise.FeasibleBy(deps.Clock.Now(), o, *deadline); ok {
			o.SetPromiseGroups([]order.PromiseGroup{{
				LineNos: allocatedLineNos(o),
				Promise: p,
			}})
		}
		return
	}

	if groups, ok := deps.Promise.PromiseGroups(deps.Clock.Now(), o); ok {
		o.SetPromiseGroups(groups)
	}
}

// allocatedLineNos lists the line numbers FeasibleBy actually reasoned
// about. FeasibleBy is all-or-nothing across the allocated set — network
// demand is ship-complete (ADR 0020 §4), so there is exactly one group
// and it covers every allocated line.
func allocatedLineNos(o *order.Order) []int {
	var out []int
	for _, l := range o.Lines() {
		if l.Status() == order.LineAllocated {
			out = append(out, l.LineNo())
		}
	}
	return out
}

// promiseGroupByLine indexes o.PromiseGroups() (ADR 0014 §3 / ADR 0017)
// by line number, so allocateAndRelease can attribute each released line
// to the specific PromiseGroup it belongs to when building the
// integration event payload. Returns an empty map for an order with no
// group breakdown yet (e.g. the promise was never computed, or was set
// via the legacy single-Promise SetPromise path) — callers treat a
// missing entry as "no per-line promise detail available", exactly like
// a pre-ADR-0017 event.
func promiseGroupByLine(o *order.Order) map[int]order.PromiseGroup {
	out := make(map[int]order.PromiseGroup)
	for _, g := range o.PromiseGroups() {
		for _, lineNo := range g.LineNos {
			out[lineNo] = g
		}
	}
	return out
}

// allocateAndRelease is the ONE shared flow this redesign folds allocation
// and release into. Both ReceiveOrder (implicitly, right after intake) and
// RetryAllocation (explicitly) call it with the lines eligible for THIS
// pass (Pending for ReceiveOrder, Backordered for RetryAllocation).
//
// Steps:
//  1. allocateLines — the reserve-per-line loop (unchanged behaviour).
//  2. On a hard (non-business) failure, persist whatever was genuinely
//     reserved before the failure (never stranding a real reservation
//     with nothing in this context able to revoke it) and publish
//     OrderAllocationPartiallyFailed for visibility if anything was
//     reserved. The hard failure is returned to the caller either way —
//     it is each CALLER's decision (see ReceiveOrder vs RetryAllocation)
//     whether to propagate that error to ITS OWN caller, since ReceiveOrder
//     must not fail just because its implicit allocation attempt did.
//  3. On success, apply the promise-date policy, then attempt release:
//     o.EnsureReleasable() enforces BR3 (a ship-complete order releases
//     nothing while any line is unallocated); when it passes, every
//     currently-Allocated line is released via the pure domain transition
//     o.Release, and its released-line detail is collected for the
//     integration event payload.
//  4. Save the order ONCE — covering both the allocation and the release
//     state changes in a single write.
//  5. publishOrderAllocationOutcome exactly once, carrying the lines
//     released in this pass (or nil when release did not run/had nothing
//     to release).
//
// releaseOnAllocation (ADR 0020 §1) gates step 3's release leg. Callers
// pass o.ReleaseOnAllocation(), so the intent is read from the aggregate
// rather than from ambient call-site knowledge — this is what makes a
// LATER RetryAllocation on a held order safe: it re-reads the order from
// the repository and sees the hold, instead of releasing work onto the
// floor for an order nobody committed to. It stays an explicit parameter
// rather than being read from o in here so the release decision is
// visible at every call site.
//
// TRANSACTIONAL OUTBOX: every Repo.Save and EventPublisher.Publish this
// function makes — the per-line OrderLineAllocated/OrderLineBackordered
// events raised inside allocateLines, the final aggregate Save, and the
// order-level outcome event — are bracketed in ONE atomically(deps.UnitOfWork)
// scope, so the aggregate write and every event about it commit together
// or not at all. The one deliberate exception is the ORIGINAL hard
// allocation failure itself (a business-visible error from
// Inventory.Reserve, e.g. a transport failure): that error is captured
// OUTSIDE the atomically closure's own return value (see reportErr below)
// so that "persist whatever was genuinely reserved before the failure"
// (the existing, documented behaviour) still COMMITS — the transaction
// only rolls back on a genuine infrastructure failure persisting or
// publishing that partial progress, never on the upstream Reserve
// failure by itself. deps.Inventory.Reserve (an outbound HTTP call) runs
// INSIDE this transaction's scope, same as PPM's reference design has no
// occasion to consider — accepted trade-off (a held DB connection for
// the duration of the reservation calls) documented in this service's
// own ADR, in exchange for the stronger atomicity guarantee.
func allocateAndRelease(
	ctx context.Context,
	deps allocationDeps,
	o *order.Order,
	lines []*order.OrderLine,
	retry bool,
	releaseOnAllocation bool,
) (allocationOutcome, error) {
	var outcome allocationOutcome
	// reportErr is the hard allocation failure to surface to THIS
	// function's own caller. It is captured outside the atomically
	// closure's return value — see the doc comment above.
	var reportErr error

	txErr := atomically(ctx, deps.UnitOfWork, func(ctx context.Context) error {
		var allocErr error
		outcome, allocErr = allocateLines(ctx, deps.Inventory, deps.Events, deps.Clock, o, lines, retry)
		if allocErr != nil {
			reportErr = allocErr
			// Persist whatever was genuinely reserved upstream before
			// surfacing the hard failure — see the allocateLines doc.
			if outcome.allocated > 0 {
				deps.setPromiseDate(o)
				if err := deps.Orders.Save(ctx, o); err != nil {
					return errors.Join(allocErr, err)
				}
				// Best-effort visibility: a failure publishing this event
				// must never mask or replace allocErr, the real failure —
				// it is joined in exactly like the Save-error case above
				// so errors.Is(err, allocErr) still holds on the final
				// returned error.
				remaining := len(o.LinesWithStatus(order.LinePending))
				if err := deps.Events.Publish(ctx, shared.NewOrderAllocationPartiallyFailed(
					deps.Clock.Now(), o.ID(), outcome.allocated, remaining, truncateCause(allocErr.Error()),
				)); err != nil {
					return errors.Join(allocErr, err)
				}
			}
			// Let the transaction commit whatever partial progress was
			// just persisted above; reportErr still carries the hard
			// failure back to this function's own caller.
			return nil
		}

		deps.setPromiseDate(o)

		// ADR 0020 §1: when the caller held the order at intake, stop after
		// allocation. Lines stay Allocated, inventory reservations genuinely
		// exist, and the promise above is computed and attached exactly as
		// for any other order — so a holder can read the promise and decide.
		// Nothing is released, so wes-work-planning sees no work and no task
		// reaches the floor.
		//
		// OrderAllocated/OrderPartiallyAllocated is still published below:
		// the allocation genuinely happened, and suppressing the event would
		// hide a real state change from every other context.
		if !releaseOnAllocation {
			if err := deps.Orders.Save(ctx, o); err != nil {
				return err
			}
			return publishOrderAllocationOutcome(ctx, deps.Events, deps.Clock, o, outcome, nil)
		}

		// BR3-gated release: EnsureReleasable enforces that a ship-complete
		// order releases nothing while any line is still unallocated. When it
		// passes, every line the aggregate now reports Allocated (this pass's
		// newly-allocated lines, and any already-Allocated from an earlier
		// pass) is eligible and is released right here, in the same flow.
		promiseByLine := promiseGroupByLine(o)
		var released []shared.ReleasedLine
		if err := o.EnsureReleasable(); err == nil {
			class := o.FulfillmentClass().String()
			for _, line := range o.LinesWithStatus(order.LineAllocated) {
				if err := o.Release(line.LineNo()); err != nil {
					return err
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
		}

		if err := deps.Orders.Save(ctx, o); err != nil {
			return err
		}
		return publishOrderAllocationOutcome(ctx, deps.Events, deps.Clock, o, outcome, released)
	})
	if txErr != nil {
		return outcome, txErr
	}
	if reportErr != nil {
		return outcome, reportErr
	}
	return outcome, nil
}

// WorkUnitID builds the deterministic, deliberately-never-transmitted id
// (order_id, line_no) => work unit reference this context and
// wes-work-planning's Kafka consumer BOTH independently derive from the
// exact same formula: `{orderID}-line-{lineNo}`. It is no longer sent
// anywhere over the wire — the choreographed-release redesign carries no
// synchronous call to wes-work-planning at all — but the formula itself is
// the frozen contract that lets that service's consumer reconstruct the
// same identifier for idempotent processing of the lines[] entries on the
// OrderAllocated/OrderPartiallyAllocated Kafka event. It MUST match
// byte-for-byte; do not change this format without coordinating both
// sides.
func WorkUnitID(orderID shared.OrderId, lineNo int) string {
	return fmt.Sprintf("%s-line-%d", orderID.String(), lineNo)
}

// workUnitIDLineMarker is the literal separator WorkUnitID's format
// string embeds between the order id and the 1-based line number. It is
// pulled out as a named constant purely so ParseWorkUnitID's reversal
// and WorkUnitID's construction visibly share the same literal — there
// is still exactly one place either side of this contract could drift,
// and this constant is it.
const workUnitIDLineMarker = "-line-"

// ParseWorkUnitID reverses WorkUnitID: given a wire value shaped like
// "{orderID}-line-{lineNo}" (fulfillment-execution's real TaskCPTMissed/
// PackageManifested order_ref, itself sourced from wes-work-planning's
// WorkUnitId at task-creation time — see ADR 0018), it recovers the
// OrderId and 1-based line number RepromiseOrder needs to find the
// affected PromiseGroup.
//
// It splits on the LAST occurrence of the "-line-" marker, not the
// first: order-management mints OrderId as "ord-" + a UUID (see
// OrderRepo.NextID), which is hex digits and hyphens only and can never
// itself contain the literal substring "line" — so first-vs-last split
// is not observable against this repo's real OrderId values today.
// Splitting on the last occurrence is still the deliberately safer
// choice: it degrades correctly even against a hypothetical future
// OrderId shape that legitimately contains "-line-" as a substring,
// where a first-occurrence split would silently truncate the order id
// and misattribute the line number.
//
// ok is false — never a panic — for any input that is not a lineNo
// trailing an order id via that exact marker: no marker present, an
// empty order-id portion, a non-numeric or non-positive line number.
// The caller (the repromise Kafka consumer) treats ok=false as "skip
// this event, log it", mirroring how every other consumer in this fleet
// handles a malformed inbound message.
func ParseWorkUnitID(workUnitID string) (orderID shared.OrderId, lineNo int, ok bool) {
	idx := strings.LastIndex(workUnitID, workUnitIDLineMarker)
	if idx <= 0 {
		return "", 0, false
	}
	orderPart := workUnitID[:idx]
	lineNoPart := workUnitID[idx+len(workUnitIDLineMarker):]
	if lineNoPart == "" {
		return "", 0, false
	}
	n, err := strconv.Atoi(lineNoPart)
	if err != nil || n <= 0 {
		return "", 0, false
	}
	return shared.OrderId(orderPart), n, true
}
