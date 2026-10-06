// Package report holds the order-management "Order Funnel & Allocation
// Health" read model: the shapes of the analytical report the data product
// serves, the query that selects it, and the outbound ports the writer and
// reader adapters implement. It is a read-model region that depends on
// nothing else in this module — the OLTP domain and application layers must
// not import it, and it must not import them (ADR-0006).
package report

import "time"

// Granularity is the time-bucket resolution a report is rolled up to. Only
// hourly buckets are modelled for this round.
type Granularity string

const (
	// GranularityHour rolls rows up into UTC hour buckets.
	GranularityHour Granularity = "hour"
)

// RowKey identifies a single funnel row: the wes-work-planning process path
// (PathId) the order/lines are bound to, and the UTC hour bucket the row
// aggregates. HourBucket is the bucket start, truncated to the hour in UTC.
type RowKey struct {
	PathId     string
	HourBucket time.Time
}

// Row is one aggregated funnel row for a (pathId, hourBucket) key. It
// captures the order-level funnel — received → allocated → released — with
// cancellations and backorders as leakage, plus the line-level allocation
// counters. Every field is a simple per-bucket count.
type Row struct {
	Key RowKey

	// --- order-level funnel counters -------------------------------------

	// OrdersReceived is the number of OrderReceived events in this bucket.
	OrdersReceived int
	// OrdersAllocated is the number of OrderAllocated events (every line
	// allocated) in this bucket.
	OrdersAllocated int
	// OrdersPartiallyAllocated is the number of OrderPartiallyAllocated
	// events (some lines allocated, some backordered) in this bucket.
	OrdersPartiallyAllocated int
	// OrdersAllocationFailed is the number of OrderAllocationPartiallyFailed
	// events (a hard, non-business allocation failure) in this bucket.
	OrdersAllocationFailed int
	// OrdersReleased is the number of OrderReleased events (every line
	// released as work) in this bucket.
	OrdersReleased int
	// OrdersCancelled is the number of OrderCancelled events in this bucket —
	// funnel leakage before release.
	OrdersCancelled int

	// --- line-level counters ---------------------------------------------

	// LinesAllocated is the number of OrderLineAllocated events in this
	// bucket.
	LinesAllocated int
	// LinesBackordered is the number of OrderLineBackordered events in this
	// bucket — line-level leakage.
	LinesBackordered int
	// LinesReleased is the number of OrderLineReleased events in this bucket.
	LinesReleased int

	// --- promise KPI counters (ADR 0014 §6 / ADR 0019) --------------------
	//
	// These are populated from the promise fields ADR 0014 added to
	// OrderAllocated/OrderPartiallyAllocated (promise_basis,
	// promise_cutoff_at, split_shipment) and from the new OrderRepromised
	// analytics fact (ADR 0018/0019). They share this row's (path_id,
	// hour_bucket) grain except OrdersRepromised — see its own comment.

	// PromiseBasisCapability is the number of OrderAllocated/
	// OrderPartiallyAllocated events in this bucket whose promise basis was
	// "Capability" (a real CPT window derived from fulfillment capability —
	// order.BasisCapability).
	PromiseBasisCapability int
	// PromiseBasisLeadTime is the same count for basis "LeadTime" (the
	// tagged fallback — order.BasisLeadTime). Together with
	// PromiseBasisCapability these two counters ARE the promise basis
	// distribution ADR 0014 §6 asks for; a caller computes the split as a
	// percentage itself, mirroring this read model's existing convention of
	// exposing raw counts rather than precomputed rates.
	PromiseBasisLeadTime int
	// PromiseBasisNetwork is the same count for basis "Network" (ADR 0020:
	// a promise DICTATED by an external party's deadline rather than chosen
	// by us — order.BasisNetwork). It is its own bucket because ADR 0019's
	// KPIs measure how well OUR chosen promises track reality, and mixing
	// in promises we never chose would corrupt that signal. Capability +
	// LeadTime + Network is the complete promise-basis distribution.
	PromiseBasisNetwork int
	// OrdersRepromised is the number of OrderRepromised events in this
	// bucket. UNLIKE every other counter on this row, it carries NO path_id
	// dimension: OrderRepromised (ADR 0018) does not carry a process path,
	// and this data product deliberately does not add an OrderRepo lookup
	// to resolve one (see ADR 0019's documented choice). Every
	// OrderRepromised event therefore lands on the row whose Key.PathId is
	// the empty string "" — the same "path unknown/inapplicable" sentinel
	// this read model already uses when an order lookup misses (see the
	// analytics publisher's best-effort enrichment). A caller wanting the
	// fleet-wide re-promise count for an hour should read the ""-path row;
	// filtering ReportQuery.PathId to a real path will never surface it.
	OrdersRepromised int
	// OrdersSplitShipment is the number of OrderAllocated/
	// OrderPartiallyAllocated events in this bucket whose order had more
	// than one order.PromiseGroup at allocation time (ADR 0014 §3 / ADR
	// 0017) — i.e. lines were promised to different cutoffs. This IS
	// path_id-dimensioned like the funnel counters above (unlike
	// OrdersRepromised) because the triggering event already carries a
	// path.
	OrdersSplitShipment int
	// PromiseToCutoffGapSeconds is the MEAN of (cutoffAt - allocatedAt), in
	// seconds, across every OrderAllocated/OrderPartiallyAllocated event in
	// this bucket that carried a Capability-basis promise with a real
	// cutoffAt. It is zero when no such event landed in this bucket —
	// mirroring fulfillment-execution's own AvgClaimToCompleteSeconds
	// "zero when no completion had a claim" convention. LeadTime-basis
	// promises are excluded: their "cutoff" is just now-plus-a-configured-
	// duration, so the gap would trivially reproduce the configured lead
	// time rather than measure anything about real fulfillment capability.
	PromiseToCutoffGapSeconds float64
	// PromiseToCutoffGapSamples is how many events contributed to
	// PromiseToCutoffGapSeconds's mean for this bucket. Exposed
	// specifically so a caller aggregating PromiseToCutoffGapSeconds
	// across MULTIPLE rows (e.g. get_promise_health summarising a
	// multi-hour/multi-path window) can compute a correctly WEIGHTED
	// mean — averaging several rows' already-computed means unweighted
	// would silently under- or over-count buckets with different sample
	// volumes.
	PromiseToCutoffGapSamples int
}

// FunnelReport is the full result of a report query: the matching rows.
type FunnelReport struct {
	Rows []Row
}

// ReportQuery selects and filters the rows a report covers. From is
// inclusive and To is exclusive, both compared against a row's HourBucket.
// PathId is an optional exact-match filter (empty means "no filter on this
// dimension").
type ReportQuery struct {
	From        time.Time
	To          time.Time
	PathId      string
	Granularity Granularity
}
