package analyticsstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/analytics/report"
)

// PostgresReport is the READER implementation of report.ReportStore, backed
// by a pgxpool over the analytical database. The pool it is given is expected
// to be pinned to a read-only role / default_transaction_read_only=on, so a
// bug in the reader cannot mutate the read model (ADR-0006). The reader never
// issues writes.
type PostgresReport struct {
	pool *pgxpool.Pool
}

// NewPostgresReport constructs a PostgresReport over pool.
func NewPostgresReport(pool *pgxpool.Pool) *PostgresReport {
	return &PostgresReport{pool: pool}
}

// Query returns the funnel rows matching q. From is inclusive, To is
// exclusive; an empty PathId disables that filter.
//
// Promise KPIs (ADR 0014 §6 / ADR 0019): the basis-distribution,
// split-shipment and promise-to-cutoff-gap columns live on funnel_rollup at
// the SAME (path_id, hour_bucket) grain as the pre-existing counters, so
// they are simply extra columns in the same SELECT. OrdersRepromised is
// different: it lives in the separate repromise_rollup table, keyed by
// hour_bucket ONLY (see that table's own doc comment for why). The query
// below folds it onto the path_id="" row for each hour — merging into an
// existing ""-path funnel_rollup row when one exists (e.g. from a
// best-effort enrichment miss), or synthesizing a repromise-only row when
// none does — and only when the caller did not filter to a specific real
// path (a repromise count can never belong to one).
func (r *PostgresReport) Query(ctx context.Context, q report.ReportQuery) (report.FunnelReport, error) {
	rows, err := r.pool.Query(ctx,
		`WITH repromise AS (
			SELECT hour_bucket, orders_repromised
			FROM repromise_rollup
			WHERE hour_bucket >= $1 AND hour_bucket < $2
		 )
		 SELECT
			f.path_id, f.hour_bucket,
			f.orders_received, f.orders_allocated, f.orders_partially_allocated,
			f.orders_allocation_failed, f.orders_released, f.orders_cancelled,
			f.lines_allocated, f.lines_backordered, f.lines_released,
			f.promise_basis_capability, f.promise_basis_lead_time, f.promise_basis_network, f.orders_split_shipment,
			f.promise_to_cutoff_gap_seconds_sum, f.promise_to_cutoff_gap_samples,
			COALESCE(CASE WHEN f.path_id = '' THEN rep.orders_repromised END, 0) AS orders_repromised
		 FROM funnel_rollup f
		 LEFT JOIN repromise rep ON rep.hour_bucket = f.hour_bucket AND f.path_id = ''
		 WHERE f.hour_bucket >= $1 AND f.hour_bucket < $2
		   AND ($3 = '' OR f.path_id = $3)

		 UNION ALL

		 SELECT
			'' AS path_id, rep.hour_bucket,
			0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0,
			rep.orders_repromised
		 FROM repromise rep
		 WHERE $3 = ''
		   AND NOT EXISTS (
			SELECT 1 FROM funnel_rollup f2
			WHERE f2.hour_bucket = rep.hour_bucket AND f2.path_id = ''
		   )

		 ORDER BY hour_bucket, path_id`,
		q.From, q.To, q.PathId)
	if err != nil {
		return report.FunnelReport{}, fmt.Errorf("analyticsstore: query rollup: %w", err)
	}
	defer rows.Close()

	var out report.FunnelReport
	for rows.Next() {
		var (
			row           report.Row
			bucket        time.Time
			gapSecondsSum float64
			gapSamples    int
		)
		if err := rows.Scan(
			&row.Key.PathId, &bucket,
			&row.OrdersReceived, &row.OrdersAllocated, &row.OrdersPartiallyAllocated,
			&row.OrdersAllocationFailed, &row.OrdersReleased, &row.OrdersCancelled,
			&row.LinesAllocated, &row.LinesBackordered, &row.LinesReleased,
			&row.PromiseBasisCapability, &row.PromiseBasisLeadTime, &row.PromiseBasisNetwork, &row.OrdersSplitShipment,
			&gapSecondsSum, &gapSamples,
			&row.OrdersRepromised,
		); err != nil {
			return report.FunnelReport{}, fmt.Errorf("analyticsstore: scan row: %w", err)
		}
		row.Key.HourBucket = bucket.UTC()
		if gapSamples > 0 {
			row.PromiseToCutoffGapSeconds = gapSecondsSum / float64(gapSamples)
		}
		row.PromiseToCutoffGapSamples = gapSamples
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return report.FunnelReport{}, fmt.Errorf("analyticsstore: iterate rows: %w", err)
	}
	return out, nil
}

// FreshnessLag returns now minus the most recent event's occurred_at, i.e.
// how far the read model trails real time. Zero when the read model is empty
// or (defensively) when the latest event is future-dated.
func (r *PostgresReport) FreshnessLag(ctx context.Context) (time.Duration, error) {
	// max() over an empty table returns a single NULL row (not zero rows), so
	// scan into a nullable *time.Time and treat NULL as "read model empty".
	var latest *time.Time
	err := r.pool.QueryRow(ctx,
		`SELECT max(occurred_at) FROM analytics_processed_events`).Scan(&latest)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("analyticsstore: freshness query: %w", err)
	}
	if latest == nil || latest.IsZero() {
		return 0, nil
	}
	lag := time.Since(*latest)
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

// Compile-time assertion that PostgresReport satisfies the read port.
var _ report.ReportStore = (*PostgresReport)(nil)
