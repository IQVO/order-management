package analyticsstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/analytics/report"
)

func TestMemoryStore_FunnelCountersIdempotent(t *testing.T) {
	base := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()

	apply := func() {
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
		}
		must(s.ApplyOrderReceived(ctx, "r1", "pick", base))
		must(s.ApplyLineAllocated(ctx, "la1", "pick", base.Add(time.Minute)))
		must(s.ApplyOrderAllocated(ctx, "a1", "pick", base.Add(2*time.Minute), "", nil, false))
		must(s.ApplyLineReleased(ctx, "lr1", "pick", base.Add(3*time.Minute)))
		must(s.ApplyOrderReleased(ctx, "rel1", "pick", base.Add(4*time.Minute)))
	}

	// Apply the full sequence twice with the SAME event ids (duplicate
	// delivery): the counters must reflect one logical occurrence.
	apply()
	apply()

	rep, err := s.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	checks := []struct {
		name string
		got  int
		want int
	}{
		{"OrdersReceived", row.OrdersReceived, 1},
		{"LinesAllocated", row.LinesAllocated, 1},
		{"OrdersAllocated", row.OrdersAllocated, 1},
		{"LinesReleased", row.LinesReleased, 1},
		{"OrdersReleased", row.OrdersReleased, 1},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want 1 (idempotent)", c.name, c.got)
		}
	}
}

func TestMemoryStore_LeakageCounters(t *testing.T) {
	base := time.Date(2026, 4, 1, 9, 30, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()

	if err := s.ApplyLineBackordered(ctx, "bo-1", "pick", base); err != nil {
		t.Fatalf("line backordered: %v", err)
	}
	// Duplicate backorder ignored.
	if err := s.ApplyLineBackordered(ctx, "bo-1", "pick", base); err != nil {
		t.Fatalf("line backordered dup: %v", err)
	}
	if err := s.ApplyOrderCancelled(ctx, "cx-1", "singles", base); err != nil {
		t.Fatalf("order cancelled: %v", err)
	}
	if err := s.ApplyOrderAllocationFailed(ctx, "af-1", "singles", base); err != nil {
		t.Fatalf("allocation failed: %v", err)
	}

	rep, err := s.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	var pickRow, singlesRow *report.Row
	for i := range rep.Rows {
		switch rep.Rows[i].Key.PathId {
		case "pick":
			pickRow = &rep.Rows[i]
		case "singles":
			singlesRow = &rep.Rows[i]
		}
	}
	if pickRow == nil || pickRow.LinesBackordered != 1 {
		t.Errorf("pick LinesBackordered = %v, want 1", pickRow)
	}
	if singlesRow == nil || singlesRow.OrdersCancelled != 1 || singlesRow.OrdersAllocationFailed != 1 {
		t.Errorf("singles leakage = %v, want cancelled=1 failed=1", singlesRow)
	}
}

func TestMemoryStore_FreshnessLag(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	s := analyticsstore.NewMemoryStore()
	s.Now = func() time.Time { return now }

	// No events yet: lag is zero.
	lag, err := s.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag != 0 {
		t.Errorf("empty lag = %v, want 0", lag)
	}

	// An event 10 minutes old makes the lag 10 minutes.
	if err := s.ApplyOrderReceived(ctx, "r", "pick", now.Add(-10*time.Minute)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	lag, err = s.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag != 10*time.Minute {
		t.Errorf("lag = %v, want 10m", lag)
	}
}

// Compile-time assertions that MemoryStore satisfies both ports.
var (
	_ report.ProjectionStore = (*analyticsstore.MemoryStore)(nil)
	_ report.ReportStore     = (*analyticsstore.MemoryStore)(nil)
)

// TestMemoryStore_PromiseKPIs covers ADR 0014 §6 / ADR 0019: basis
// distribution, split-shipment, promise-to-cutoff-gap mean, and the
// path_id-free re-promise counter.
func TestMemoryStore_PromiseKPIs(t *testing.T) {
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()

	cutoff1 := base.Add(2 * time.Hour)
	cutoff2 := base.Add(4 * time.Hour)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	// Two Capability-basis allocations with different gaps, one LeadTime
	// fallback, and one split-shipment order.
	must(s.ApplyOrderAllocated(ctx, "e1", "pick", base, "Capability", &cutoff1, false))
	must(s.ApplyOrderAllocated(ctx, "e2", "pick", base, "Capability", &cutoff2, true))
	must(s.ApplyOrderPartiallyAllocated(ctx, "e3", "pick", base, "LeadTime", nil, false))
	// Duplicate delivery of e1 must not double-count.
	must(s.ApplyOrderAllocated(ctx, "e1", "pick", base, "Capability", &cutoff1, false))
	// Re-promise events, path-free.
	must(s.ApplyOrderRepromised(ctx, "r1", base))
	must(s.ApplyOrderRepromised(ctx, "r1", base)) // duplicate, ignored
	must(s.ApplyOrderRepromised(ctx, "r2", base))

	rep, err := s.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	var pickRow, repromiseRow *report.Row
	for i := range rep.Rows {
		switch rep.Rows[i].Key.PathId {
		case "pick":
			pickRow = &rep.Rows[i]
		case "":
			repromiseRow = &rep.Rows[i]
		}
	}
	if pickRow == nil {
		t.Fatal("expected a pick row")
	}
	if pickRow.PromiseBasisCapability != 2 {
		t.Errorf("PromiseBasisCapability = %d, want 2", pickRow.PromiseBasisCapability)
	}
	if pickRow.PromiseBasisLeadTime != 1 {
		t.Errorf("PromiseBasisLeadTime = %d, want 1", pickRow.PromiseBasisLeadTime)
	}
	if pickRow.OrdersSplitShipment != 1 {
		t.Errorf("OrdersSplitShipment = %d, want 1", pickRow.OrdersSplitShipment)
	}
	// Mean of (2h, 4h) = 3h = 10800s.
	wantGap := 3 * time.Hour.Seconds()
	if pickRow.PromiseToCutoffGapSeconds != wantGap {
		t.Errorf("PromiseToCutoffGapSeconds = %v, want %v", pickRow.PromiseToCutoffGapSeconds, wantGap)
	}
	if pickRow.PromiseToCutoffGapSamples != 2 {
		t.Errorf("PromiseToCutoffGapSamples = %d, want 2", pickRow.PromiseToCutoffGapSamples)
	}

	if repromiseRow == nil {
		t.Fatal("expected a path_id=\"\" repromise row")
	}
	if repromiseRow.OrdersRepromised != 2 {
		t.Errorf("OrdersRepromised = %d, want 2 (idempotent)", repromiseRow.OrdersRepromised)
	}
}

// ADR 0019/0020: a Network-basis promise (dictated by an external deadline)
// has its own bucket — it is neither dropped nor folded into
// Capability/LeadTime, and contributes no promise-to-cutoff-gap sample.
func TestMemoryStore_NetworkBasisHasItsOwnBucket(t *testing.T) {
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	ctx := context.Background()
	s := analyticsstore.NewMemoryStore()
	cutoff := base.Add(2 * time.Hour)

	for _, err := range []error{
		s.ApplyOrderAllocated(ctx, "n1", "pick", base, "Network", &cutoff, false),
		s.ApplyOrderPartiallyAllocated(ctx, "n2", "pick", base, "Network", &cutoff, false),
		s.ApplyOrderAllocated(ctx, "n1", "pick", base, "Network", &cutoff, false), // duplicate
		s.ApplyOrderAllocated(ctx, "c1", "pick", base, "Capability", &cutoff, false),
	} {
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	rep, err := s.Query(ctx, report.ReportQuery{
		From: base.Add(-time.Hour), To: base.Add(time.Hour), Granularity: report.GranularityHour,
	})
	if err != nil || len(rep.Rows) != 1 {
		t.Fatalf("Query: rows=%v err=%v, want 1 row", rep.Rows, err)
	}
	row := rep.Rows[0]
	if row.PromiseBasisNetwork != 2 {
		t.Errorf("PromiseBasisNetwork = %d, want 2 (idempotent)", row.PromiseBasisNetwork)
	}
	if row.PromiseBasisCapability != 1 || row.PromiseBasisLeadTime != 0 {
		t.Errorf("capability/leadTime = %d/%d, want 1/0", row.PromiseBasisCapability, row.PromiseBasisLeadTime)
	}
	if row.PromiseToCutoffGapSamples != 1 {
		t.Errorf("gap samples = %d, want 1 (Network is excluded from the gap)", row.PromiseToCutoffGapSamples)
	}
}
