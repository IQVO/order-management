package shared

import "time"

// DomainEvent is a past-tense fact produced by the Order aggregate.
// Adapters (outbound/events) serialize and publish these; the domain never
// depends on the publishing mechanism.
type DomainEvent interface {
	EventName() string
	OccurredAt() time.Time
}

type base struct {
	Name string
	At   time.Time
}

func (b base) EventName() string     { return b.Name }
func (b base) OccurredAt() time.Time { return b.At }

func newBase(name string, occurredAt time.Time) base {
	return base{Name: name, At: occurredAt}
}

// OrderReceived: an order was accepted into the building with its lines.
type OrderReceived struct {
	base
	OrderID   OrderId
	LineCount int
}

func NewOrderReceived(occurredAt time.Time, orderID OrderId, lineCount int) OrderReceived {
	return OrderReceived{base: newBase("OrderReceived", occurredAt), OrderID: orderID, LineCount: lineCount}
}

// OrderLineAllocated: inventory-storage accepted a reservation for a line.
// ReservationID is inventory-storage's id — this context stores the
// reference only; inventory-storage remains the sole owner of reservation
// state.
type OrderLineAllocated struct {
	base
	OrderID       OrderId
	LineNo        int
	SKU           SKU
	Quantity      int
	ReservationID string
}

func NewOrderLineAllocated(occurredAt time.Time, orderID OrderId, lineNo int, sku SKU, quantity int, reservationID string) OrderLineAllocated {
	return OrderLineAllocated{
		base:    newBase("OrderLineAllocated", occurredAt),
		OrderID: orderID, LineNo: lineNo, SKU: sku, Quantity: quantity, ReservationID: reservationID,
	}
}

// OrderLineBackordered: inventory-storage reported insufficient usable
// stock (HTTP 409) for a line. This is a BUSINESS FACT — it is never
// produced from a transport or 5xx failure.
type OrderLineBackordered struct {
	base
	OrderID  OrderId
	LineNo   int
	SKU      SKU
	Quantity int
}

func NewOrderLineBackordered(occurredAt time.Time, orderID OrderId, lineNo int, sku SKU, quantity int) OrderLineBackordered {
	return OrderLineBackordered{
		base:    newBase("OrderLineBackordered", occurredAt),
		OrderID: orderID, LineNo: lineNo, SKU: sku, Quantity: quantity,
	}
}

// ReleasedLine is one line released as part of the same allocation pass
// that produced an OrderAllocated or OrderPartiallyAllocated fact. Its
// core fields (LineNo, SKU, PathID, GiftWrap, FulfillmentClass) are part
// of the frozen wes-work-planning Kafka integration contract (see the
// Kafka integration section of CLAUDE.md): field names and shapes here
// are mirrored byte-for-byte by wes-work-planning's consumer and must
// not be changed casually.
//
// PromiseCptId/PromiseBasis/PromiseCutoffAt are ADR 0017's additive
// per-line promise fields (ADR 0014 §3's per-shipment-group promising):
// which group this specific line belongs to, carried alongside the
// order-level PromiseDate/PromiseCptId/PromiseBasis summary on
// OrderAllocated/OrderPartiallyAllocated. All three are pointers —
// nil/omitted for an order whose promise was set via the legacy
// SetPromise path (no group breakdown to attribute a line to) — mirroring
// this repo's existing pointer-field convention for "may not apply"
// (see OrderLine.ReservationID). wes-work-planning's current consumer
// does not read these fields; this is a future phase's work, not this
// one's — see ADR 0017.
type ReleasedLine struct {
	LineNo           int
	SKU              SKU
	PathID           PathId
	GiftWrap         bool
	FulfillmentClass string
	PromiseCptId     *string
	PromiseBasis     *string
	PromiseCutoffAt  *time.Time
}

// OrderAllocated: every line on the order is allocated — and, per the
// choreographed-release redesign, every one of those lines that was
// eligible for release (EnsureReleasable passed) was released as part of
// this SAME fact. Lines carries exactly the lines released in this pass
// (empty when release was blocked, e.g. a ship-complete order that just
// became fully allocated but somehow could not release — in practice this
// does not happen since a fully-Allocated order is always releasable, but
// the field is never nil-vs-empty-ambiguous either way). This event is
// also forwarded to Kafka as an integration event — see the Kafka
// integration section of CLAUDE.md.
//
// PromiseCptId and PromiseBasis are ADR 0014's additive fields: the CPT
// identity the promise targets (empty for a LeadTime-basis promise, which
// has no departure identity) and which policy produced PromiseDate
// (Capability or LeadTime). PromiseDate itself is unchanged — it is the
// promise's CutoffAt either way, kept on the wire for backward
// compatibility.
type OrderAllocated struct {
	base
	OrderID      OrderId
	PromiseDate  time.Time
	PromiseCptId string
	PromiseBasis string
	Lines        []ReleasedLine
}

func NewOrderAllocated(occurredAt time.Time, orderID OrderId, promiseDate time.Time, lines []ReleasedLine) OrderAllocated {
	return OrderAllocated{base: newBase("OrderAllocated", occurredAt), OrderID: orderID, PromiseDate: promiseDate, Lines: lines}
}

// NewOrderAllocatedWithPromise is NewOrderAllocated plus ADR 0014's
// promiseCptId/promiseBasis. Kept as a separate constructor rather than
// widening NewOrderAllocated's signature so every existing call site
// (and every existing test asserting on that signature) keeps compiling
// unchanged; new call sites (allocateAndRelease) use this one.
func NewOrderAllocatedWithPromise(occurredAt time.Time, orderID OrderId, promiseDate time.Time, promiseCptId, promiseBasis string, lines []ReleasedLine) OrderAllocated {
	return OrderAllocated{
		base: newBase("OrderAllocated", occurredAt), OrderID: orderID, PromiseDate: promiseDate,
		PromiseCptId: promiseCptId, PromiseBasis: promiseBasis, Lines: lines,
	}
}

// OrderPartiallyAllocated: some lines allocated, some backordered, on an
// order that allows partial shipment. Lines carries only the lines
// released THIS pass (i.e. o.LinesWithStatus(LineAllocated) restricted to
// what was actually released now) — never previously-released lines from
// an earlier pass, and never the still-Backordered lines. This event is
// also forwarded to Kafka as an integration event — see the Kafka
// integration section of CLAUDE.md.
//
// PromiseCptId/PromiseBasis are ADR 0014's additive fields — see
// OrderAllocated's doc comment.
type OrderPartiallyAllocated struct {
	base
	OrderID          OrderId
	AllocatedLines   int
	BackorderedLines int
	PromiseDate      time.Time
	PromiseCptId     string
	PromiseBasis     string
	Lines            []ReleasedLine
}

func NewOrderPartiallyAllocated(occurredAt time.Time, orderID OrderId, allocated, backordered int, promiseDate time.Time, lines []ReleasedLine) OrderPartiallyAllocated {
	return OrderPartiallyAllocated{
		base:    newBase("OrderPartiallyAllocated", occurredAt),
		OrderID: orderID, AllocatedLines: allocated, BackorderedLines: backordered, PromiseDate: promiseDate, Lines: lines,
	}
}

// NewOrderPartiallyAllocatedWithPromise is NewOrderPartiallyAllocated
// plus ADR 0014's promiseCptId/promiseBasis — see
// NewOrderAllocatedWithPromise's doc comment for why this is a separate
// constructor rather than a widened signature.
func NewOrderPartiallyAllocatedWithPromise(occurredAt time.Time, orderID OrderId, allocated, backordered int, promiseDate time.Time, promiseCptId, promiseBasis string, lines []ReleasedLine) OrderPartiallyAllocated {
	return OrderPartiallyAllocated{
		base:    newBase("OrderPartiallyAllocated", occurredAt),
		OrderID: orderID, AllocatedLines: allocated, BackorderedLines: backordered, PromiseDate: promiseDate,
		PromiseCptId: promiseCptId, PromiseBasis: promiseBasis, Lines: lines,
	}
}

// OrderLineReleased: a line's work was enqueued onto a wes-work-planning
// process path.
type OrderLineReleased struct {
	base
	OrderID    OrderId
	LineNo     int
	PathID     PathId
	WorkUnitID string
}

func NewOrderLineReleased(occurredAt time.Time, orderID OrderId, lineNo int, pathID PathId, workUnitID string) OrderLineReleased {
	return OrderLineReleased{
		base:    newBase("OrderLineReleased", occurredAt),
		OrderID: orderID, LineNo: lineNo, PathID: pathID, WorkUnitID: workUnitID,
	}
}

// OrderReleased: every line on the order has been released as work.
type OrderReleased struct {
	base
	OrderID OrderId
}

func NewOrderReleased(occurredAt time.Time, orderID OrderId) OrderReleased {
	return OrderReleased{base: newBase("OrderReleased", occurredAt), OrderID: orderID}
}

// OrderCancelled: the order was cancelled before any line was released,
// and every allocated line's reservation was revoked upstream.
type OrderCancelled struct {
	base
	OrderID             OrderId
	RevokedReservations int
}

func NewOrderCancelled(occurredAt time.Time, orderID OrderId, revoked int) OrderCancelled {
	return OrderCancelled{base: newBase("OrderCancelled", occurredAt), OrderID: orderID, RevokedReservations: revoked}
}

// OrderAllocationPartiallyFailed: AllocateOrder hit a hard (non-business,
// non-409) failure partway through allocating an order's lines —
// AllocatedLines were genuinely reserved upstream before the failure, and
// RemainingLines are still Pending. The already-succeeded reservations ARE
// kept — this is deliberate (see ADR-0003): discarding them would strand
// real reservations inside inventory-storage that nothing in this context
// could then revoke, and a subsequent AllocateOrder call safely resumes by
// skipping the already-Allocated lines. This event exists purely so that
// outcome is operationally visible, rather than only discoverable by
// reading a source code comment.
type OrderAllocationPartiallyFailed struct {
	base
	OrderID        OrderId
	AllocatedLines int
	RemainingLines int
	// Cause is a short, safe-to-log description of the failure (the
	// error's Error() string, defensively truncated) — never the full
	// error internals or an upstream response body.
	Cause string
}

func NewOrderAllocationPartiallyFailed(occurredAt time.Time, orderID OrderId, allocatedLines, remainingLines int, cause string) OrderAllocationPartiallyFailed {
	return OrderAllocationPartiallyFailed{
		base:    newBase("OrderAllocationPartiallyFailed", occurredAt),
		OrderID: orderID, AllocatedLines: allocatedLines, RemainingLines: remainingLines, Cause: cause,
	}
}

// OrderRepromised: the promise for this order moved, discovered by
// reacting to a downstream fulfillment-execution fact (a task still open
// past its CPT, or a SLAM pass) — ADR 0014 §5's feedback loop, closed by
// ADR 0018's RepromiseOrder use case. This is the fleet's "your delivery
// is delayed" trigger; nothing here contacts a customer, it is the
// trigger, not the notification.
//
// CptIdOld/CptIdNew are the CPT identity (e.g. "sp1-1800") the affected
// PromiseGroup targeted before and after the recompute. Either — or both
// — may be empty: a LeadTime-basis promise has no CPT departure
// identity, only a computed cutoff instant (see order.Promise's doc
// comment), so an empty string here means "that basis had no CPT
// identity", never "no promise existed". Reason names the
// fulfillment-execution event that triggered the recompute —
// "TaskCPTMissed" or "PackageManifested" (the event name) — so a downstream
// reader can tell which kind of signal moved the promise without a
// second lookup.
type OrderRepromised struct {
	base
	OrderID  OrderId
	CptIdOld string
	CptIdNew string
	Reason   string
}

func NewOrderRepromised(occurredAt time.Time, orderID OrderId, cptIdOld, cptIdNew, reason string) OrderRepromised {
	return OrderRepromised{
		base:    newBase("OrderRepromised", occurredAt),
		OrderID: orderID, CptIdOld: cptIdOld, CptIdNew: cptIdNew, Reason: reason,
	}
}
