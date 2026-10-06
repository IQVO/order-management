//go:build integration

package analyticsstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/analytics/report"
)

// requireAnalyticsURL returns the connection string of the package's shared,
// already-migrated analytics Postgres (testcontainers — see
// main_integration_test.go; it never reads ANALYTICS_DATABASE_URL, never
// skips, never hardcodes localhost) with every table emptied. Tests must not
// run in parallel: they share one database.
func requireAnalyticsURL(t *testing.T) string {
	t.Helper()
	sharedPGOnce.Do(startSharedAnalyticsPostgres)
	if sharedPGErr != nil {
		t.Fatalf("shared analytics postgres: %v", sharedPGErr)
	}
	truncateAllTables(t, sharedPGURL)
	return sharedPGURL
}

func TestPostgresProjectionAndReport_RoundTrip(t *testing.T) {
	url := requireAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	// Isolate this run's rows with a unique path id.
	path := "pick-int-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM funnel_rollup WHERE path_id = $1`, path)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// received -> allocated -> released twice with the same event ids:
	// idempotent.
	apply := func() {
		must(proj.ApplyOrderReceived(ctx, "int-received", path, base))
		must(proj.ApplyOrderAllocated(ctx, "int-allocated", path, base.Add(time.Minute), "", nil, false))
		must(proj.ApplyOrderReleased(ctx, "int-released", path, base.Add(2*time.Minute)))
	}
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		PathId:      path,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.OrdersReceived != 1 || row.OrdersAllocated != 1 || row.OrdersReleased != 1 {
		t.Errorf("funnel = received %d allocated %d released %d, want 1/1/1 (idempotent)",
			row.OrdersReceived, row.OrdersAllocated, row.OrdersReleased)
	}

	lag, err := rdr.FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag: %v", err)
	}
	if lag < 0 {
		t.Errorf("lag = %v, want >= 0", lag)
	}
}

// TestReadOnlyPool_RejectsWrites asserts the reader pool is genuinely
// read-only: an attempt to write through it must be rejected by Postgres.
func TestReadOnlyPool_RejectsWrites(t *testing.T) {
	url := requireAnalyticsURL(t)

	roPool, err := analyticsstore.NewReadOnlyPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewReadOnlyPool: %v", err)
	}
	t.Cleanup(roPool.Close)

	ctx := context.Background()
	_, err = roPool.Exec(ctx,
		`INSERT INTO funnel_rollup (path_id, hour_bucket) VALUES ($1, $2)`,
		"ro-path", time.Now().UTC().Truncate(time.Hour))
	if err == nil {
		t.Fatal("expected read-only pool to reject INSERT, but it succeeded")
	}

	// The read side still works over the same read-only pool.
	rdr := analyticsstore.NewPostgresReport(roPool)
	if _, err := rdr.FreshnessLag(ctx); err != nil {
		t.Fatalf("FreshnessLag over read-only pool: %v", err)
	}
}

// TestFreshnessLag_EmptyStore covers the NULL path: max(occurred_at) over an
// empty table returns a single NULL row (not zero rows), which must be read as
// a zero lag rather than a scan error (pilot bug #2).
func TestFreshnessLag_EmptyStore(t *testing.T) {
	url := requireAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	// Ensure the processed-events table is empty so max() yields NULL.
	if _, err := pool.Exec(ctx, `TRUNCATE analytics_processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	lag, err := analyticsstore.NewPostgresReport(pool).FreshnessLag(ctx)
	if err != nil {
		t.Fatalf("FreshnessLag on empty store: %v", err)
	}
	if lag != 0 {
		t.Fatalf("empty-store lag = %v, want 0", lag)
	}
}

// TestPostgresProjectionAndReport_PromiseKPIs proves ADR 0014 §6 / ADR
// 0019's new columns/rollup round-trip for real against a live Postgres:
// promise basis distribution, split-shipment, promise-to-cutoff-gap mean
// (sum+count divided in the reader), and the path_id-free re-promise
// counter merged in from the separate repromise_rollup table.
func TestPostgresProjectionAndReport_PromiseKPIs(t *testing.T) {
	url := requireAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	path := "pick-promise-int-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM funnel_rollup WHERE path_id = $1`, path)
		_, _ = pool.Exec(ctx, `DELETE FROM repromise_rollup WHERE hour_bucket = $1`, base)
	})

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	cutoff1 := base.Add(2 * time.Hour)
	cutoff2 := base.Add(4 * time.Hour)

	apply := func() {
		must(proj.ApplyOrderAllocated(ctx, "pk-alloc-1", path, base, "Capability", &cutoff1, false))
		must(proj.ApplyOrderAllocated(ctx, "pk-alloc-2", path, base, "Capability", &cutoff2, true))
		must(proj.ApplyOrderPartiallyAllocated(ctx, "pk-partial-1", path, base, "LeadTime", nil, false))
		must(proj.ApplyOrderRepromised(ctx, "pk-repromise-1", base))
	}
	// Apply twice with the SAME event ids: idempotent.
	apply()
	apply()

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		PathId:      path,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query (path-filtered): %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("path-filtered rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.PromiseBasisCapability != 2 {
		t.Errorf("PromiseBasisCapability = %d, want 2", row.PromiseBasisCapability)
	}
	if row.PromiseBasisLeadTime != 1 {
		t.Errorf("PromiseBasisLeadTime = %d, want 1", row.PromiseBasisLeadTime)
	}
	if row.OrdersSplitShipment != 1 {
		t.Errorf("OrdersSplitShipment = %d, want 1", row.OrdersSplitShipment)
	}
	wantGap := 3 * time.Hour.Seconds() // mean of (2h, 4h)
	if row.PromiseToCutoffGapSeconds != wantGap {
		t.Errorf("PromiseToCutoffGapSeconds = %v, want %v", row.PromiseToCutoffGapSeconds, wantGap)
	}
	if row.PromiseToCutoffGapSamples != 2 {
		t.Errorf("PromiseToCutoffGapSamples = %d, want 2", row.PromiseToCutoffGapSamples)
	}
	// Path-filtered query must never surface the repromise counter — it
	// lives on the path_id="" row only.
	if row.OrdersRepromised != 0 {
		t.Errorf("path-filtered OrdersRepromised = %d, want 0", row.OrdersRepromised)
	}

	// Unfiltered query must surface a path_id="" row carrying the
	// idempotent re-promise count.
	repAll, err := rdr.Query(ctx, report.ReportQuery{
		From:        base.Add(-time.Hour),
		To:          base.Add(time.Hour),
		Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query (unfiltered): %v", err)
	}
	var repromiseRow *report.Row
	for i := range repAll.Rows {
		if repAll.Rows[i].Key.PathId == "" && repAll.Rows[i].Key.HourBucket.Equal(base) {
			repromiseRow = &repAll.Rows[i]
			break
		}
	}
	if repromiseRow == nil {
		t.Fatal("expected a path_id=\"\" row carrying the repromise counter")
	}
	if repromiseRow.OrdersRepromised != 1 {
		t.Errorf("OrdersRepromised = %d, want 1 (idempotent)", repromiseRow.OrdersRepromised)
	}
}

// TestPostgresProjectionAndReport_NetworkBasisBucket proves ADR 0019/0020
// end to end against a live Postgres: a promise DICTATED by an external
// deadline (basis "Network") lands in its own promise_basis_network
// column — never dropped (the projector used to have no case for it) and
// never folded into Capability/LeadTime or the promise-to-cutoff gap —
// and survives the report query, including for an unfiltered query that
// also synthesizes a repromise-only row (the UNION ALL column count).
func TestPostgresProjectionAndReport_NetworkBasisBucket(t *testing.T) {
	url := requireAnalyticsURL(t)

	pool, err := analyticsstore.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	path := "pick-network-int"

	proj := analyticsstore.NewPostgresProjection(pool)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	cutoff := base.Add(3 * time.Hour)
	apply := func() {
		must(proj.ApplyOrderAllocated(ctx, "nw-alloc-1", path, base, "Network", &cutoff, false))
		must(proj.ApplyOrderAllocated(ctx, "nw-alloc-2", path, base, "Network", &cutoff, false))
		must(proj.ApplyOrderPartiallyAllocated(ctx, "nw-partial-1", path, base, "Network", &cutoff, false))
		must(proj.ApplyOrderAllocated(ctx, "nw-cap-1", path, base, "Capability", &cutoff, false))
		must(proj.ApplyOrderAllocated(ctx, "nw-lead-1", path, base, "LeadTime", nil, false))
		must(proj.ApplyOrderRepromised(ctx, "nw-repromise-1", base))
	}
	apply()
	apply() // same event ids: idempotent

	rdr := analyticsstore.NewPostgresReport(pool)
	rep, err := rdr.Query(ctx, report.ReportQuery{
		From: base.Add(-time.Hour), To: base.Add(time.Hour), PathId: path, Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query (path-filtered): %v", err)
	}
	if len(rep.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rep.Rows))
	}
	row := rep.Rows[0]
	if row.PromiseBasisNetwork != 3 {
		t.Errorf("PromiseBasisNetwork = %d, want 3 (2 allocated + 1 partially allocated)", row.PromiseBasisNetwork)
	}
	if row.PromiseBasisCapability != 1 || row.PromiseBasisLeadTime != 1 {
		t.Errorf("capability/leadTime = %d/%d, want 1/1 (Network must not leak into them)",
			row.PromiseBasisCapability, row.PromiseBasisLeadTime)
	}
	// Only the single Capability promise contributes a gap sample.
	if row.PromiseToCutoffGapSamples != 1 || row.PromiseToCutoffGapSeconds != 3*time.Hour.Seconds() {
		t.Errorf("gap = %vs over %d samples, want 10800s over 1 (Network is excluded)",
			row.PromiseToCutoffGapSeconds, row.PromiseToCutoffGapSamples)
	}

	// Unfiltered: the UNION ALL branch synthesizes the repromise-only row;
	// its placeholder column count must line up with the new column.
	all, err := rdr.Query(ctx, report.ReportQuery{
		From: base.Add(-time.Hour), To: base.Add(time.Hour), Granularity: report.GranularityHour,
	})
	if err != nil {
		t.Fatalf("Query (unfiltered): %v", err)
	}
	var sawNetwork, sawRepromise bool
	for _, r := range all.Rows {
		if r.Key.PathId == path && r.PromiseBasisNetwork == 3 {
			sawNetwork = true
		}
		if r.Key.PathId == "" && r.OrdersRepromised == 1 {
			sawRepromise = true
		}
	}
	if !sawNetwork || !sawRepromise {
		t.Fatalf("unfiltered report network=%v repromise=%v, rows=%+v", sawNetwork, sawRepromise, all.Rows)
	}
}
