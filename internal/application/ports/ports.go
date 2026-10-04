// Package ports declares the outbound interfaces the application layer
// depends on. Adapters implement these; the application never imports an
// adapter package.
//
// InventoryReservationClient is a cross-context call into inventory-
// storage's published HTTP contract. It is expressed in this context's own
// types on purpose: Order Management is the Customer in a Customer/Supplier
// relationship and does not import a single Go package from that Supplier.
// See ADR 0002.
//
// Release, by contrast, is no longer a synchronous outbound call at all —
// since the choreographed-release redesign (see ADR 0005), a released line
// is announced as a fact on the enriched OrderAllocated /
// OrderPartiallyAllocated integration events, published to Kafka via
// ports.EventPublisher. There is deliberately no WorkReleaseClient-shaped
// port here any more.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

var (
	// ErrInsufficientStock is the BUSINESS FACT behind inventory-storage's
	// HTTP 409 on POST /reservations: there is not enough usable stock for
	// this line right now. Only an InventoryReservationClient that
	// actually saw a 409 may return it — a transport failure or a 5xx is
	// NOT this error, and must never be silently turned into a backorder.
	ErrInsufficientStock = errors.New("insufficient usable stock for this order line")

	// ErrDownstreamNotConfigured is returned by the permissive (no-op)
	// outbound clients. Unlike the fail-open classification lookups
	// elsewhere in this fleet, reserving real stock and releasing real
	// work must never appear to succeed against a no-op, so the permissive
	// clients fail loudly instead of fabricating a result. Only
	// MODE=http is suitable for a real integration test or deployment.
	ErrDownstreamNotConfigured = errors.New("downstream client is running in permissive (no-op) mode and cannot perform this operation")

	// ErrConcurrentModification is OrderRepo.Save's optimistic-
	// concurrency-control sentinel (see the version-column ADR): it is
	// returned when Save's version-guarded write against the orders
	// table affects zero rows for an order that DOES exist — i.e. the
	// in-memory aggregate being saved was loaded at a version some
	// OTHER writer has already advanced past. The caller lost the
	// race; its whole read-modify-write attempt must be treated as
	// failed (never partially applied — the version check runs before
	// order_lines is touched, so a failed check leaves every row
	// exactly as some other, concurrent writer left it). The inbound
	// HTTP adapter maps this to 409; the Kafka-driven RepromiseOrder
	// consumer's normal commit-and-skip-on-error handling treats it as
	// any other infrastructure error from Orders.Save — see that
	// consumer's own handling and the ADR for why a safe redelivery,
	// not a dropped message, is the result.
	ErrConcurrentModification = errors.New("order was concurrently modified by another writer; reload and retry")
)

// OrderRepo persists and retrieves Order aggregates.
type OrderRepo interface {
	Save(ctx context.Context, o *order.Order) error
	FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error)
	NextID(ctx context.Context) (shared.OrderId, error)
}

// EventPublisher publishes domain events. v1 wires a log publisher; the
// signature is deliberately the shape a Kafka producer satisfies, so the
// deferred broker integration is purely additive.
type EventPublisher interface {
	Publish(ctx context.Context, event shared.DomainEvent) error
}

// UnitOfWork brackets a use case's state change and the domain event(s)
// it raises so both commit or neither does (transactional outbox — see
// the ADR registered alongside this port). Execute runs fn inside one
// atomic scope: every Repo.Save and EventPublisher.Publish made with the
// ctx handed to fn is bound to that same scope. If fn returns an error
// the scope is rolled back and nothing — neither the aggregate rows nor
// the outbox rows — is visible afterwards.
//
// A nested Execute (fn invoked with a ctx that already carries a scope)
// joins the outer scope rather than opening a second one, so a use case
// that calls another helper which itself wraps its work in Execute never
// deadlocks or double-commits.
//
// Adapters with no transactional backing (the in-memory repo, the log
// publisher) satisfy this with a pass-through that simply calls fn; the
// use cases stay adapter-agnostic either way — see atomically() in the
// usecases package, which every publishing use case calls through
// rather than invoking Execute directly.
type UnitOfWork interface {
	Execute(ctx context.Context, fn func(ctx context.Context) error) error
}

// Clock abstracts current time so use cases and tests are deterministic.
type Clock interface {
	Now() time.Time
}

// OrderMetrics records order-intake outcomes so the business signal (how
// much demand is arriving, and how much of it is immediately accepted vs.
// rejected as invalid at intake) is observable independently of HTTP
// traffic. Use cases treat a nil value as "not instrumented", so wiring it
// is optional. Mirrors inventory-storage's ports.ReservationMetrics shape
// exactly: two named methods over a single counter distinguished by an
// `outcome` attribute, per the fleet-standard-metrics ADR's Tier-2
// convention.
type OrderMetrics interface {
	OrderAccepted(ctx context.Context)
	OrderRejected(ctx context.Context)
}

// ReservationRequest is this context's request shape for
// inventory-storage's POST /reservations. DemandRef carries the OrderId —
// which is precisely the identity this bounded context was created to own.
//
// LineNo and Attempt exist ONLY so an InventoryReservationClient
// implementation can derive a stable, deterministic Idempotency-Key for
// inventory-storage's POST /reservations (see its own idempotency.go /
// ADR mirrored from this repo's ADR 0023): inventory-storage requires
// that header and replays the cached response for a key it has already
// seen, so a caller MUST send the SAME key on every retry of the SAME
// logical reservation attempt and a genuinely DIFFERENT key on a new
// attempt. LineNo scopes the key to one line within the order (two
// lines of the same order must never collide); Attempt is the calling
// use case's order.Order.Version() at the moment this line's Reserve
// call is issued — inert optimistic-concurrency metadata already on the
// aggregate (ADR 0024) that is guaranteed to stay constant across every
// Reserve call within ONE allocation pass (nothing bumps it until that
// pass's Save commits) and to have moved on by the time a genuinely
// later, independent pass re-attempts the same line — exactly the
// "same across a retry of this attempt, different for a new attempt"
// shape the header contract requires. Neither field is meaningful to
// inventory-storage's own request body; they are transport-adapter
// concerns of the OUTBOUND client only.
type ReservationRequest struct {
	SKU       shared.SKU
	Quantity  int
	DemandRef shared.OrderId
	LineNo    int
	Attempt   int
}

// ReservationResult carries back the only piece of inventory-storage's
// reservation this context stores: its id, needed to revoke on
// cancellation. Reservation state itself stays owned by inventory-storage.
type ReservationResult struct {
	ReservationID string
}

// InventoryReservationClient is the outbound port for inventory-storage's
// published reservation contract.
//
// Reserve MUST return ErrInsufficientStock (and only that) for a 409, and
// a distinct error for anything else — transport failure, 4xx other than
// 409, or 5xx — so AllocateOrder can fail closed on ambiguity instead of
// recording a backorder that inventory-storage never asserted.
type InventoryReservationClient interface {
	Reserve(ctx context.Context, req ReservationRequest) (ReservationResult, error)
	RevokeReservation(ctx context.Context, reservationID string) error
}

// ProcessPathCatalogue is the read-only outbound port for
// process-path-management's live catalogue of currently active process
// paths. ADR-0013 originally kept this port to ONLY the membership
// question ReceiveOrder needs at intake — "is this path active right
// now" — deliberately deferring the catalogue's fuller shape (cycle
// time, eligibility) until a real consumer needed it.
//
// ADR-0014 step A is that consumer: PromisePolicy needs a path's
// CycleTimeP95 and Eligibility to decide whether an allocated line can
// make a given CPT window. This is a NON-BREAKING additive widening
// (two new methods), not a boundary violation — ADR-0014 explicitly asks
// for it. See internal/adapters/outbound/kafkacatalog's package doc
// comment for the adapter that implements this against a live Kafka
// feed, mirroring the same port already proven in wes-work-planning /
// fulfillment-execution / workforce-management.
type ProcessPathCatalogue interface {
	// IsActive reports whether pathId currently names an active,
	// declared process path. Matching semantics (exact id, or a
	// declared MatchPrefix + "-" family) are the adapter's concern, not
	// the port's — ReceiveOrder only needs the yes/no answer.
	IsActive(pathId shared.PathId) bool

	// CycleTimeP95 returns the path's 95th-percentile cycle time —
	// how long it takes a unit to move through this path once picked
	// up — and whether that value is currently known. known=false
	// covers both "path is unknown/inactive" and "path is active but
	// the wire event never carried a parseable cycle_time_p95" (see
	// kafkacatalog's decoder): PromisePolicy treats both the same way,
	// as "cannot compute a capability-basis promise for this line".
	CycleTimeP95(pathId shared.PathId) (cycleTime time.Duration, known bool)

	// Eligibility returns the path's declared eligibility rule and
	// whether it is currently known (false for an unknown/inactive
	// path). Nothing in step A consumes this for a routing decision —
	// it is made available now so step B (eligibility-driven
	// PathSelectionPolicy) does not need another catalogue widening.
	Eligibility(pathId shared.PathId) (shared.Eligibility, bool)

	// ListActive returns every currently active path this catalogue
	// knows about (ADR-0021), so order.PathSelectionPolicy can enumerate
	// and rank candidates instead of only ever evaluating one hardcoded
	// id. Order is unspecified — callers needing a deterministic pick
	// must sort themselves. A source with an empty or not-yet-ready
	// cache returns an empty slice, never an error, matching this port's
	// existing "missing data fails open" convention. Returns the
	// domain's own shared.ActivePathCandidate (rather than a
	// ports-local type) so this method's return type is structurally
	// identical to order.EligibilitySource.ListActive's — required for
	// this adapter to satisfy that domain-owned interface directly, the
	// same pattern PromisePolicy's CapabilitySource/ScheduleSource/
	// CapacitySource already rely on.
	ListActive() []shared.ActivePathCandidate
}

// CPTScheduleCache is the read-only outbound port for
// process-path-management's per-site CPT schedule (PPM ADR 0010's
// CPTScheduleChanged event). It is backed by a Kafka-fed local cache
// (internal/adapters/outbound/kafkacptschedule), mirroring
// ProcessPathCatalogue's own Kafka-cache pattern exactly — including its
// per-process-unique consumer group and readiness-gate design — but as
// a SEPARATE consumer instance on the SAME topic, since CPTScheduleChanged
// and ProcessPathCreated/Updated/Deactivated are independent event types
// this service reacts to independently.
//
// NextCutoffs returns order.CPTWindow (a domain type) rather than a
// ports-owned type: PromisePolicy is pure domain logic per ADR 0014 and
// declares its own minimal order.ScheduleSource interface with this
// exact signature, so this port's adapter (kafkacptschedule.Consumer)
// satisfies both this port AND order.ScheduleSource without any
// translation glue.
type CPTScheduleCache interface {
	// NextCutoffs returns up to n upcoming concrete cutoff instants for
	// siteId, computed from the schedule's recurring (localTime,
	// daysOfWeek, timezone) rule, from time `from` onward, each paired
	// with the cptId and eligiblePathIds that applied. Returns
	// known=false if no schedule exists yet for siteId (e.g. the Kafka
	// cache has not yet observed a CPTScheduleChanged event for it).
	NextCutoffs(siteId string, from time.Time, n int) (cutoffs []order.CPTWindow, known bool)
}

// ProductClassification is this context's own minimal view of a SKU's
// product classification, as looked up from inventory-storage's
// GET /products/{sku}/classification (see ProductClassificationLookup).
// It carries only what order.PathSelectionPolicy's eligibility evaluation
// needs -- the raw handling tags as free-form strings from the fleet's
// existing product-classification vocabulary (e.g. "Hazmat", "Fragile",
// "TemperatureSensitive") -- not inventory-storage's fuller
// ProductClassification resource (temperatureClass, dotHazardClass are
// omitted; narrow port, ADR-0013's own stated philosophy). Known
// distinguishes "SKU has no registered classification, or the lookup is
// unavailable" (Known=false, both treated identically -- permissive/
// fail-open, mirroring wes-work-planning's ADR-0009) from "classification
// confirmed" (Known=true).
type ProductClassification struct {
	SKU          string
	HandlingTags []string
	Known        bool
}

// HasTag reports whether tag (e.g. "Hazmat", "Fragile") is present among
// HandlingTags.
func (c ProductClassification) HasTag(tag string) bool {
	for _, t := range c.HandlingTags {
		if t == tag {
			return true
		}
	}
	return false
}

// ProductClassificationLookup is the outbound port for the synchronous
// cross-context read from inventory-storage's product-classification
// endpoint (GET /products/{sku}/classification), used at intake time so
// order.PathSelectionPolicy can evaluate a line's derived product
// attributes (hazmat/fragile/etc.) against a candidate path's declared
// Eligibility -- see ADR-0016 (ADR-0014 step B). This mirrors
// wes-work-planning's own ports.ProductClassificationLookup (its
// ADR-0009) exactly in shape and intent; order-management never imports
// that service's Go packages (see .claude/rules/bounded-context-boundary.md)
// -- this is an independently-owned copy of the same pattern, calling the
// same real inventory-storage endpoint.
//
// GetClassification MUST fail open on anything short of a real
// classification: a 404 (unclassified SKU), a transport error, or any
// non-2xx/404 status are all Known=false, never an error returned to the
// caller -- see PermissiveLookup and Client in
// internal/adapters/outbound/productclassification for the two
// implementations. This is a soft routing/enrichment input, not a stock
// reservation, so it follows this fleet's "fail loud for anything that
// mutates real state, fail quiet/open for a soft enrichment input" rule
// -- the opposite of InventoryReservationClient above.
type ProductClassificationLookup interface {
	GetClassification(ctx context.Context, sku string) (ProductClassification, error)
}

// PathCapacity is the read-only outbound port for remaining capacity per
// (path, CPT) bucket. Two implementations exist: UnknownPathCapacity
// (always known=false, the pre-ADR-0015 default and the dev-mode/
// fallback option today) and kafkapathcapacity.Consumer (ADR-0015), a
// Kafka-fed cache of wes-work-planning's PathCapacityChanged event.
// PromisePolicy treats known=false as "capacity is not a constraint"
// per ADR-0014's explicit condition (c) — this is unchanged by ADR-0015;
// what changes is that a real figure is now available whenever
// wes-work-planning has reported one for the exact path+cutoff asked
// about.
//
// Remaining's signature carries cutoffAt (ADR-0015), not just cptId: the
// wire event wes-work-planning actually publishes carries its own native
// CutoffAt instant (a time.Time), never process-path-management's cptId
// string. The one caller of this port, order.PromisePolicy.linesFitWindow,
// already has both cptId and cutoffAt in scope from the CPTWindow it is
// evaluating (see kafkacptschedule's NextCutoffs), so passing cutoffAt
// costs the caller nothing and lets a Kafka-fed adapter answer without
// inventing its own cptId<->cutoffAt resolution. See ADR-0015 for the
// full reasoning and the alternative considered.
type PathCapacity interface {
	// Remaining reports how many units remain available for pathId at
	// the CPT identified by cptId (kept for logging/observability —
	// implementations correlate on cutoffAt, not cptId) and cutoffAt,
	// and whether that figure is currently known.
	Remaining(pathId shared.PathId, cptId string, cutoffAt time.Time) (units int, known bool)
}

// RepromiseProcessedEvents is RepromiseOrder's idempotency gate (ADR
// 0014 §5 / ADR 0018): it records which Kafka event_ids have already
// been evaluated, so fulfillment-execution's TaskCPTMissed sweep —
// which re-fires on EVERY sweep pass for as long as a task stays
// overdue, by that service's own ADR 0025 design — never re-evaluates
// the same message twice. This is a NEW, OLTP-side port: the only
// existing ProcessedEvents-shaped interface in this repo
// (adapters/inbound/kafka.ProcessedEvents) is declared local to the
// analytics consumer specifically so the analytics side owns its own
// port and the OLTP application layer stays untouched — see that
// file's doc comment. RepromiseOrder is OLTP-side (it mutates the real
// Order aggregate and publishes a real integration event), so it gets
// its own, distinctly-named port rather than reusing that one.
//
// The idempotency key is the Kafka message's event_id alone, not a
// composite (orderId, event_id) key. ADR 0014 §5 states the requirement
// as "idempotent on (orderId, sourceEventId)", but event_id is already
// globally unique per message (every publisher in this fleet mints it
// with uuid.NewString()), so scoping the check by orderId too is
// redundant — two different orders can never coincidentally share the
// same event_id, and the SAME event_id always refers to the SAME
// order. This simplification is documented explicitly, not silently
// substituted — see the RepromiseOrder ADR.
type RepromiseProcessedEvents interface {
	// MarkProcessed records eventId if absent, returning true iff this
	// call newly recorded it.
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// PlannedCapacityRepo is order-management's LOCAL read model of
// warehouse-planning's CapacityPlans (ADR 0031): a mirror of the events
// that service publishes, never a live lookup. It is written only by the
// ApplyPlannedCapacity use case (inside a UnitOfWork, atomically with the
// idempotency claim) and read by the order-annotation and
// GET /planned-capacity use cases.
type PlannedCapacityRepo interface {
	// Upsert stores w under w.PlanID with last-writer-wins semantics
	// (order.PlannedCapacityWindow.Supersedes): a stale or downgrading
	// write is a no-op. It reports whether the row was written.
	Upsert(ctx context.Context, w order.PlannedCapacityWindow) (applied bool, err error)

	// ListByLocation returns every stored window at location (any
	// status) whose End is after endingAfter, ordered by Start then
	// PlanID. endingAfter is a coarse prefilter that keeps the read
	// bounded; the authoritative overlap rule stays in the domain
	// (PlannedCapacityWindow.Overlaps).
	ListByLocation(ctx context.Context, location string, endingAfter time.Time) ([]order.PlannedCapacityWindow, error)
}

// PlannedCapacityProcessedEvents is ApplyPlannedCapacity's idempotency
// gate: it records which CloudEvents ids have been applied. MarkProcessed
// MUST be called inside the same UnitOfWork as the Upsert it guards, so a
// rolled-back handling un-claims the id and the redelivery is processed
// rather than silently skipped. Its own table
// (planned_capacity_processed_events) so it never collides with
// RepromiseProcessedEvents.
type PlannedCapacityProcessedEvents interface {
	// MarkProcessed records eventId if absent, returning true iff this
	// call newly recorded it.
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}
