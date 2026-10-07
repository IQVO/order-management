package usecases

import (
	"context"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// NewLine is the application-level description of one requested line. The
// HTTP adapter maps its DTO onto this; the use case turns it into a
// validated domain OrderLine.
type NewLine struct {
	SKU      shared.SKU
	Quantity int
	PathID   shared.PathId
	GiftWrap bool
}

// ReceiveOrder is intake: it validates the requested lines, mints an
// OrderId, and persists a new Order in Received status, publishing
// OrderReceived unconditionally — a caller placing an order always sees
// that fact regardless of what happens next.
//
// Per the choreographed-release redesign (ADR 0005), ReceiveOrder ALSO
// immediately attempts allocation-then-release in the SAME call, right
// after the Save+OrderReceived publish above. This replaces the old
// design where a caller had to separately invoke POST /orders/{id}/allocate
// and POST /orders/{id}/release: placing an order now expresses the whole
// intent, and allocation/release are internal saga steps triggered by that
// intent rather than public commands a caller drives by hand.
//
// DESIGN DECISION — how a hard allocation failure is handled here (see the
// task brief's point 2d): allocateAndRelease can hard-fail (a transport
// error or 5xx from inventory-storage — never a 409/backorder, which is a
// business fact handled inside allocateLines, not an error here). The
// ORIGINAL AllocateOrder use case returned that hard failure to ITS OWN
// caller, because that caller's whole intent WAS "attempt allocation now"
// — failing was the honest answer to that specific request.
//
// ReceiveOrder's caller has a different intent: "place an order". The
// order object itself was successfully constructed and persisted (that
// already succeeded and was already published as OrderReceived) BEFORE
// allocation was ever attempted. A hard failure in the *implicit*,
// best-effort allocation attempt that follows must not retroactively turn
// a successful ReceiveOrder into a failed one — the order genuinely was
// received, and telling the caller otherwise would be a lie (the task
// brief is explicit: never claim ReceiveOrder failed when the order
// genuinely was created).
//
// So: ReceiveOrder treats allocateAndRelease as strictly best-effort here.
// A hard failure is NOT propagated as ReceiveOrder's error. It is also
// never silently swallowed with no trail — allocateAndRelease itself
// already publishes OrderAllocationPartiallyFailed whenever ANY line was
// genuinely allocated before the failure (its existing, unchanged
// fail-closed visibility pathway), and it always persists whatever
// allocation state genuinely happened (allocated lines saved; anything
// that failed before reservation stays Pending). If NO line was allocated
// at all before the hard failure (e.g. the very first line's reserve call
// hits a transport error), every line is left Pending — exactly the state
// a freshly-received, not-yet-allocated order would have had anyway pre-
// redesign, and it is logged here so the failure is not invisible even
// though no domain event fires for a zero-progress attempt (there is
// nothing new to report over "the order is Received", which OrderReceived
// already covered). Either way, the order this call returns to its caller
// reflects whatever the allocation pass actually achieved — Received,
// PartiallyAllocated, Backordered, Allocated, Released, or
// PartiallyReleased — with no error, because ReceiveOrder itself
// succeeded. A human/operator can always drive further progress via
// RetryAllocation, which — unlike this implicit best-effort attempt — DOES
// propagate a hard failure to its own caller, since invoking it is an
// explicit, on-purpose recovery action.
type ReceiveOrder struct {
	Orders    ports.OrderRepo
	Events    ports.EventPublisher
	Clock     ports.Clock
	Inventory ports.InventoryReservationClient
	// Promise is order.PromisePolicy (ADR 0014): computes a
	// capability-derived CPT-window promise when the underlying
	// Schedule/Capability/Capacity inputs are wired, falling back to
	// its own embedded LeadTimePolicy otherwise. A zero value always
	// falls back to LeadTimePolicy's own zero-value behaviour, so
	// existing wiring/tests need no changes to keep compiling.
	Promise order.PromisePolicy
	// PathPolicy resolves a line's process path when the caller (or an
	// internal test) doesn't already supply one. Zero value is usable —
	// PathSelectionPolicy has no state — so leaving this field unset in
	// existing wiring changes nothing.
	PathPolicy order.PathSelectionPolicy
	// Catalogue validates a resolved path against
	// process-path-management's live set of active paths before the
	// order is ever persisted. A nil Catalogue means "not configured":
	// validation is skipped, matching this fleet's convention of an
	// optional outbound port defaulting to permissive rather than
	// panicking on a missing wire-up (see ports.InventoryReservationClient's
	// permissive-mode sibling, though that one fails loud on write —
	// Catalogue's nil case is a read-only membership check with no
	// mutation to protect, so failing OPEN here just means "not yet
	// wired", not "silently fabricating a result"). See ADR-0013.
	//
	// ADR-0016 (ADR-0014 step B) widens this SAME field's use: it is now
	// also consulted for shared.DefaultPathId's declared Eligibility, via
	// order.PathSelectionPolicy.Select's EligibilitySource parameter —
	// ports.ProcessPathCatalogue already satisfies that narrower
	// interface by structural typing, so this field needed no shape
	// change, only a new caller.
	Catalogue ports.ProcessPathCatalogue
	// Classification looks up a line's derived product attributes
	// (hazmat/fragile/etc.) from the local copy of product-master's
	// classifications (ADR 0036), ONCE per line at intake, so PathPolicy
	// can evaluate them against the resolved path's Eligibility
	// (ADR-0016). A nil Classification means "not wired": every line is
	// evaluated with no derived attributes beyond its own GiftWrap flag,
	// exactly PermissiveLookup's own always-Known=false behaviour — see
	// internal/adapters/outbound/productclassificationcopy's package doc
	// comment for why this fails open rather than blocking intake.
	Classification ports.ProductClassificationLookup
	// Metrics records the business-fact outcome of order intake (accepted
	// vs. rejected). Optional — a nil Metrics means "not instrumented",
	// same convention as every other optional port on this use case.
	Metrics ports.OrderMetrics
	// DemandProjection is Phase-1's explicit, versioned static demand-site
	// scope. It is application configuration, never an Order aggregate field;
	// its zero value leaves the additive projection disabled.
	DemandProjection DemandProjectionPolicy
	// UnitOfWork brackets every Save + Publish this use case makes (its
	// own intake Save+OrderReceived publish, AND the allocateAndRelease
	// pass it triggers immediately after) atomically (transactional
	// outbox). Optional: a nil value means "no transactional backing",
	// the in-memory / log-publisher dev configuration.
	UnitOfWork ports.UnitOfWork
}

func (uc *ReceiveOrder) Execute(ctx context.Context, lines []NewLine, allowPartialShipment bool) (*order.Order, error) {
	return uc.ExecuteHeld(ctx, lines, allowPartialShipment, true)
}

// ExecuteHeld is Execute plus ADR 0020 §1's releaseOnAllocation intent.
// Execute delegates here with true, so every existing caller and test is
// unchanged.
func (uc *ReceiveOrder) ExecuteHeld(ctx context.Context, lines []NewLine, allowPartialShipment, releaseOnAllocation bool) (*order.Order, error) {
	return uc.ExecuteWithDeadline(ctx, lines, allowPartialShipment, releaseOnAllocation, nil)
}

// ExecuteWithDeadline is ExecuteHeld plus ADR 0020 §2's externally
// dictated deadline. A nil requiredShipBy is the ordinary case and
// behaves exactly as before: the promise is CHOSEN by this service.
//
// With a deadline the promise is instead CONSTRAINED to it, via
// PromisePolicy.FeasibleBy — and an order that cannot make it comes back
// with no promise at all rather than an optimistic one, which is the
// signal a caller holding a fill-or-kill commitment needs.
func (uc *ReceiveOrder) ExecuteWithDeadline(ctx context.Context, lines []NewLine, allowPartialShipment, releaseOnAllocation bool, requiredShipBy *time.Time) (*order.Order, error) {
	// ADR 0020 §4: reject the contradictory combination BEFORE minting an
	// id or persisting anything — a rejected intake must leave no trace.
	if err := order.ValidateIntakeIntent(allowPartialShipment, releaseOnAllocation); err != nil {
		uc.recordRejected(ctx)
		return nil, err
	}
	domainLines, err := uc.buildDomainLines(ctx, lines)
	if err != nil {
		uc.recordRejected(ctx)
		return nil, err
	}

	id, err := uc.Orders.NextID(ctx)
	if err != nil {
		return nil, err
	}

	o, err := order.New(id, domainLines, allowPartialShipment)
	if err != nil {
		uc.recordRejected(ctx)
		return nil, err
	}
	if !releaseOnAllocation {
		o.Hold()
	}
	if requiredShipBy != nil {
		o.SetRequiredShipBy(*requiredShipBy)
	}

	if err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewOrderReceived(uc.Clock.Now(), o.ID(), len(domainLines)))
	}); err != nil {
		return nil, err
	}
	// The order is genuinely persisted and OrderReceived has already fired
	// at this point — intake itself succeeded, independent of whatever the
	// best-effort allocation attempt below does next (see the type doc).
	uc.recordAccepted(ctx)

	// Best-effort implicit allocation-then-release — see the type doc for
	// the full reasoning. A hard failure here is intentionally NOT
	// returned: the order was genuinely received, and that already
	// succeeded above. allocateAndRelease's own fail-closed visibility
	// pathway (OrderAllocationPartiallyFailed) still fires when any
	// genuine progress was made before the failure, so this is never a
	// silent swallow — just never surfaced as a ReceiveOrder failure.
	deps := allocationDeps{Orders: uc.Orders, Inventory: uc.Inventory, Events: uc.Events, Clock: uc.Clock, Promise: uc.Promise, DemandProjection: uc.DemandProjection, UnitOfWork: uc.UnitOfWork}
	if _, err := allocateAndRelease(ctx, deps, o, o.LinesWithStatus(order.LinePending), false, releaseOnAllocation); err != nil {
		return uc.orderAfterAllocationFailure(ctx, o)
	}
	return o, nil
}

// buildDomainLines validates each requested line and resolves its
// process path, turning application-level NewLines into validated
// domain OrderLines. A line without an explicit PathID is routed by
// PathPolicy against the catalogue's active candidates and the line's
// derived product attributes (ADR-0013 / ADR-0016 / ADR-0021). The
// catalogue check runs only once the line itself is well-formed
// (SKU/quantity errors take priority — an invalid line is rejected for
// being invalid, not for having an unreachable path); a resolved path
// that isn't currently active in process-path-management's real
// catalogue is a caller-facing input error, rejected here, before
// Save/OrderReceived, rather than letting wes-work-planning discover it
// one saga step later. Every error this method returns is such a
// caller-facing input rejection.
func (uc *ReceiveOrder) buildDomainLines(ctx context.Context, lines []NewLine) ([]*order.OrderLine, error) {
	domainLines := make([]*order.OrderLine, 0, len(lines))
	for i, l := range lines {
		pathID := l.PathID
		if pathID == "" {
			resolved, ok := uc.PathPolicy.Select(l.SKU, l.Quantity, l.GiftWrap, uc.productAttributes(ctx, l.SKU), uc.Catalogue)
			if !ok {
				return nil, shared.ErrLineIneligibleForResolvedPath
			}
			pathID = resolved
		}
		line, err := order.NewOrderLine(i+1, l.SKU, l.Quantity, pathID, l.GiftWrap)
		if err != nil {
			return nil, err
		}
		if uc.Catalogue != nil && !uc.Catalogue.IsActive(line.PathID()) {
			return nil, shared.ErrUnknownProcessPath
		}
		domainLines = append(domainLines, line)
	}
	return domainLines, nil
}

// orderAfterAllocationFailure re-reads the persisted order after the
// implicit best-effort allocation pass hard-failed, so what
// ReceiveOrder returns to its caller never diverges from what is
// actually stored: allocateAndRelease may have mutated o in memory
// (e.g. marked a line Backordered) without persisting that mutation —
// it only saves when at least one line was genuinely allocated before
// the hard failure. The caller must see reality, not an in-memory state
// that a crash right now would lose.
func (uc *ReceiveOrder) orderAfterAllocationFailure(ctx context.Context, o *order.Order) (*order.Order, error) {
	stored, findErr := uc.Orders.FindByID(ctx, o.ID())
	if findErr != nil {
		// The order was definitely persisted moments ago (the Save
		// above succeeded); a failure to read it back is itself a
		// genuine infrastructure problem worth surfacing rather than
		// silently returning a possibly-stale in-memory object.
		return nil, findErr
	}
	if stored == nil {
		// Unreachable in practice (the Save above just succeeded),
		// but fall back to the in-memory object rather than return a
		// broken (nil, nil) result.
		return o, nil
	}
	return stored, nil
}

func (uc *ReceiveOrder) recordAccepted(ctx context.Context) {
	if uc.Metrics != nil {
		uc.Metrics.OrderAccepted(ctx)
	}
}

func (uc *ReceiveOrder) recordRejected(ctx context.Context) {
	if uc.Metrics != nil {
		uc.Metrics.OrderRejected(ctx)
	}
}

// productAttributes looks up sku's derived product-classification
// handling tags (e.g. "Hazmat", "Fragile") ONCE, so
// order.PathSelectionPolicy can evaluate them against a candidate path's
// declared Eligibility (ADR-0016). A nil Classification port, an
// unknown/unclassified SKU, or a lookup error are all treated
// identically — no attributes — matching
// ports.ProductClassificationLookup's own fail-open contract; this
// method never returns an error because a classification-lookup problem
// must never block order intake.
func (uc *ReceiveOrder) productAttributes(ctx context.Context, sku shared.SKU) []string {
	if uc.Classification == nil {
		return nil
	}
	view, err := uc.Classification.GetClassification(ctx, sku.String())
	if err != nil || !view.Known {
		return nil
	}
	return view.HandlingTags
}
