// Package analyticsstore provides the outbound adapters that persist and
// serve the order-management "Order Funnel & Allocation Health" read model:
// an in-memory implementation (MemoryStore) for tests and local runs, and
// Postgres implementations (a writer projection and a read-only reader) for
// deployment. All satisfy the report.ProjectionStore and/or
// report.ReportStore ports.
package analyticsstore

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/order-management/internal/analytics/report"
)

// MemoryStore is an in-memory implementation of both report.ProjectionStore
// (write) and report.ReportStore (read), backed by maps. It is idempotent
// per eventId via a seen-set, so a duplicate delivery is a no-op. It is safe
// for concurrent use.
type MemoryStore struct {
	// Now supplies the current time for FreshnessLag; defaults to time.Now
	// when nil so lag is deterministic under test.
	Now func() time.Time

	mu   sync.Mutex
	seen map[string]struct{}
	rows map[report.RowKey]*rowAcc
	// latest is the OccurredAt of the most recently applied event, used to
	// compute FreshnessLag.
	latest time.Time
}

// rowAcc accumulates the running counts for one funnel row.
type rowAcc struct {
	ordersReceived           int
	ordersAllocated          int
	ordersPartiallyAllocated int
	ordersAllocationFailed   int
	ordersReleased           int
	ordersCancelled          int
	linesAllocated           int
	linesBackordered         int
	linesReleased            int

	// --- promise KPIs (ADR 0014 §6 / ADR 0019) ---
	promiseBasisCapability    int
	promiseBasisLeadTime      int
	promiseBasisNetwork       int
	ordersRepromised          int
	ordersSplitShipment       int
	promiseToCutoffGapSum     float64
	promiseToCutoffGapSamples int
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		seen: map[string]struct{}{},
		rows: map[report.RowKey]*rowAcc{},
	}
}

func hourBucket(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// firstApply marks eventId as seen and reports whether this is the first
// time (so the caller should apply the effect) or a duplicate (skip). It
// also advances the freshness watermark. The caller must hold s.mu.
func (s *MemoryStore) firstApply(eventId string, at time.Time) bool {
	if _, dup := s.seen[eventId]; dup {
		return false
	}
	s.seen[eventId] = struct{}{}
	if at.After(s.latest) {
		s.latest = at
	}
	return true
}

func (s *MemoryStore) row(k report.RowKey) *rowAcc {
	r, ok := s.rows[k]
	if !ok {
		r = &rowAcc{}
		s.rows[k] = r
	}
	return r
}

// bump applies fn to the (pathId, hour) row for eventId, unless eventId is a
// duplicate. It centralises the lock + idempotency + row-lookup shared by
// every Apply* method.
func (s *MemoryStore) bump(eventId, pathId string, at time.Time, fn func(*rowAcc)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.firstApply(eventId, at) {
		return
	}
	fn(s.row(report.RowKey{PathId: pathId, HourBucket: hourBucket(at)}))
}

// ApplyOrderReceived counts one received order. Idempotent on eventId.
func (s *MemoryStore) ApplyOrderReceived(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.ordersReceived++ })
	return nil
}

// applyPromiseKPIs mutates r with the promise KPI facts shared by
// ApplyOrderAllocated/ApplyOrderPartiallyAllocated — see
// report.ProjectionStore.ApplyOrderAllocated's doc comment for why these
// facts are applied in the same call as the funnel counter, not a separate
// method.
func applyPromiseKPIs(r *rowAcc, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) {
	switch basis {
	case "Capability":
		r.promiseBasisCapability++
	case "LeadTime":
		r.promiseBasisLeadTime++
	case "Network":
		r.promiseBasisNetwork++
	}
	if splitShipment {
		r.ordersSplitShipment++
	}
	if basis == "Capability" && cutoffAt != nil {
		r.promiseToCutoffGapSum += cutoffAt.Sub(at).Seconds()
		r.promiseToCutoffGapSamples++
	}
}

// ApplyOrderAllocated counts one fully-allocated order and its promise KPI
// facts. Idempotent on eventId.
func (s *MemoryStore) ApplyOrderAllocated(_ context.Context, eventId, pathId string, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) {
		r.ordersAllocated++
		applyPromiseKPIs(r, at, basis, cutoffAt, splitShipment)
	})
	return nil
}

// ApplyOrderPartiallyAllocated counts one partially-allocated order and its
// promise KPI facts. Idempotent on eventId.
func (s *MemoryStore) ApplyOrderPartiallyAllocated(_ context.Context, eventId, pathId string, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) {
		r.ordersPartiallyAllocated++
		applyPromiseKPIs(r, at, basis, cutoffAt, splitShipment)
	})
	return nil
}

// ApplyOrderAllocationFailed counts one hard allocation failure. Idempotent
// on eventId.
func (s *MemoryStore) ApplyOrderAllocationFailed(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.ordersAllocationFailed++ })
	return nil
}

// ApplyOrderReleased counts one fully-released order. Idempotent on eventId.
func (s *MemoryStore) ApplyOrderReleased(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.ordersReleased++ })
	return nil
}

// ApplyOrderCancelled counts one cancelled order. Idempotent on eventId.
func (s *MemoryStore) ApplyOrderCancelled(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.ordersCancelled++ })
	return nil
}

// ApplyLineAllocated counts one allocated order line. Idempotent on eventId.
func (s *MemoryStore) ApplyLineAllocated(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.linesAllocated++ })
	return nil
}

// ApplyLineBackordered counts one backordered order line. Idempotent on eventId.
func (s *MemoryStore) ApplyLineBackordered(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.linesBackordered++ })
	return nil
}

// ApplyLineReleased counts one released order line. Idempotent on eventId.
func (s *MemoryStore) ApplyLineReleased(_ context.Context, eventId, pathId string, at time.Time) error {
	s.bump(eventId, pathId, at, func(r *rowAcc) { r.linesReleased++ })
	return nil
}

// ApplyOrderRepromised counts one OrderRepromised event onto the
// path_id=""-row for the hour — deliberately not path_id dimensioned, see
// report.ProjectionStore.ApplyOrderRepromised's doc comment. Idempotent on
// eventId.
func (s *MemoryStore) ApplyOrderRepromised(_ context.Context, eventId string, at time.Time) error {
	s.bump(eventId, "", at, func(r *rowAcc) { r.ordersRepromised++ })
	return nil
}

// Query returns the rows matching q. From is inclusive, To is exclusive,
// both compared against a row's HourBucket; empty PathId means no filter on
// that dimension.
func (s *MemoryStore) Query(_ context.Context, q report.ReportQuery) (report.FunnelReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := report.FunnelReport{}
	for k, r := range s.rows {
		if k.HourBucket.Before(q.From) || !k.HourBucket.Before(q.To) {
			continue
		}
		if q.PathId != "" && k.PathId != q.PathId {
			continue
		}
		row := report.Row{
			Key:                      k,
			OrdersReceived:           r.ordersReceived,
			OrdersAllocated:          r.ordersAllocated,
			OrdersPartiallyAllocated: r.ordersPartiallyAllocated,
			OrdersAllocationFailed:   r.ordersAllocationFailed,
			OrdersReleased:           r.ordersReleased,
			OrdersCancelled:          r.ordersCancelled,
			LinesAllocated:           r.linesAllocated,
			LinesBackordered:         r.linesBackordered,
			LinesReleased:            r.linesReleased,
			PromiseBasisCapability:   r.promiseBasisCapability,
			PromiseBasisLeadTime:     r.promiseBasisLeadTime,
			PromiseBasisNetwork:      r.promiseBasisNetwork,
			OrdersRepromised:         r.ordersRepromised,
			OrdersSplitShipment:      r.ordersSplitShipment,
		}
		if r.promiseToCutoffGapSamples > 0 {
			row.PromiseToCutoffGapSeconds = r.promiseToCutoffGapSum / float64(r.promiseToCutoffGapSamples)
		}
		row.PromiseToCutoffGapSamples = r.promiseToCutoffGapSamples
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

// FreshnessLag returns how far the read model lags real time: now minus the
// OccurredAt of the most recently applied event. Zero when nothing has been
// applied yet, and never negative (a future-dated event clamps to zero).
func (s *MemoryStore) FreshnessLag(_ context.Context) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latest.IsZero() {
		return 0, nil
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	lag := now.Sub(s.latest)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

// Compile-time assertions that MemoryStore satisfies both ports.
var (
	_ report.ProjectionStore = (*MemoryStore)(nil)
	_ report.ReportStore     = (*MemoryStore)(nil)
)
