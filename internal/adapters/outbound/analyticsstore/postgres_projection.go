package analyticsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/analytics/report"
)

// PostgresProjection is the WRITER implementation of report.ProjectionStore,
// backed by a pgxpool over the analytical database. Every Apply* runs in a
// transaction that first claims the event id in analytics_processed_events
// (ON CONFLICT DO NOTHING); it only mutates the rollup when the claim is new,
// making each apply idempotent per eventId under Kafka's at-least-once
// delivery. It is the only writer of the analytical database.
type PostgresProjection struct {
	pool *pgxpool.Pool
}

// NewPostgresProjection constructs a PostgresProjection over pool.
func NewPostgresProjection(pool *pgxpool.Pool) *PostgresProjection {
	return &PostgresProjection{pool: pool}
}

// funnelColumn names the single rollup counter an Apply* method increments.
type funnelColumn string

const (
	colOrdersReceived           funnelColumn = "orders_received"
	colOrdersAllocated          funnelColumn = "orders_allocated"
	colOrdersPartiallyAllocated funnelColumn = "orders_partially_allocated"
	colOrdersAllocationFailed   funnelColumn = "orders_allocation_failed"
	colOrdersReleased           funnelColumn = "orders_released"
	colOrdersCancelled          funnelColumn = "orders_cancelled"
	colLinesAllocated           funnelColumn = "lines_allocated"
	colLinesBackordered         funnelColumn = "lines_backordered"
	colLinesReleased            funnelColumn = "lines_released"
)

// claim inserts eventId into analytics_processed_events, returning true iff
// this call newly recorded it (so the caller should apply the effect). It
// runs inside tx so the claim and the effect commit atomically.
func claim(ctx context.Context, tx pgx.Tx, eventId string, occurredAt time.Time) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO analytics_processed_events (event_id, occurred_at)
		 VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`,
		eventId, occurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// withClaim runs fn inside a transaction that has already claimed eventId.
// fn is called only when the claim is new; the transaction commits either
// way (a duplicate claim is a committed no-op, matching applyCount's
// existing behaviour). This centralises the claim+commit/rollback ceremony
// that applyCount and applyPromise both need.
func (p *PostgresProjection) withClaim(ctx context.Context, eventId string, at time.Time, fn func(tx pgx.Tx) error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	isNew, err := claim(ctx, tx, eventId, at)
	if err != nil {
		return fmt.Errorf("analyticsstore: claim event: %w", err)
	}
	if !isNew {
		committed = true
		return tx.Commit(ctx)
	}

	if err := fn(tx); err != nil {
		return err
	}

	committed = true
	return tx.Commit(ctx)
}

// applyCount is the whole of every simple counter Apply* method: in one
// transaction, claim the eventId and — only if the claim is new — increment
// column on the (path_id, hour_bucket) row. col is a fixed value from the
// funnelColumn constants above (never model input), so interpolating it into
// the SQL is safe.
func (p *PostgresProjection) applyCount(ctx context.Context, eventId, pathId string, at time.Time, col funnelColumn) error {
	return p.withClaim(ctx, eventId, at, func(tx pgx.Tx) error {
		bucket := at.UTC().Truncate(time.Hour)
		// #nosec G201 -- col is a fixed funnelColumn constant, never user input.
		sql := fmt.Sprintf(
			`INSERT INTO funnel_rollup (path_id, hour_bucket, %[1]s)
			 VALUES ($1, $2, 1)
			 ON CONFLICT (path_id, hour_bucket) DO UPDATE SET
			 	%[1]s = funnel_rollup.%[1]s + 1`, col)
		if _, err := tx.Exec(ctx, sql, pathId, bucket); err != nil {
			return fmt.Errorf("analyticsstore: upsert rollup %s: %w", col, err)
		}
		return nil
	})
}

// applyAllocationOutcome is the shared body of ApplyOrderAllocated and
// ApplyOrderPartiallyAllocated: increment the funnel-stage counter AND the
// promise KPI columns (basis distribution, split-shipment, promise-to-
// cutoff-gap sum/count) on the SAME (path_id, hour_bucket) row, inside the
// SAME eventId claim — see report.ProjectionStore.ApplyOrderAllocated's doc
// comment for why these are not two separate Apply* calls.
func (p *PostgresProjection) applyAllocationOutcome(
	ctx context.Context,
	eventId, pathId string,
	at time.Time,
	funnelCol funnelColumn,
	basis string,
	cutoffAt *time.Time,
	splitShipment bool,
) error {
	return p.withClaim(ctx, eventId, at, func(tx pgx.Tx) error {
		bucket := at.UTC().Truncate(time.Hour)

		var basisCapability, basisLeadTime, basisNetwork int
		switch basis {
		case "Capability":
			basisCapability = 1
		case "LeadTime":
			basisLeadTime = 1
		case "Network":
			// A promise dictated by an external deadline (ADR 0020): its own
			// bucket, never folded into Capability/LeadTime (ADR 0019), and
			// excluded from the promise-to-cutoff gap below.
			basisNetwork = 1
		}

		var split int
		if splitShipment {
			split = 1
		}

		var gapSeconds float64
		var gapSamples int
		if basis == "Capability" && cutoffAt != nil {
			gapSeconds = cutoffAt.Sub(at).Seconds()
			gapSamples = 1
		}

		// #nosec G201 -- funnelCol is a fixed funnelColumn constant, never user input.
		sql := fmt.Sprintf(
			`INSERT INTO funnel_rollup (
				path_id, hour_bucket, %[1]s,
				promise_basis_capability, promise_basis_lead_time, promise_basis_network, orders_split_shipment,
				promise_to_cutoff_gap_seconds_sum, promise_to_cutoff_gap_samples
			 )
			 VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (path_id, hour_bucket) DO UPDATE SET
			 	%[1]s = funnel_rollup.%[1]s + 1,
			 	promise_basis_capability = funnel_rollup.promise_basis_capability + $3,
			 	promise_basis_lead_time = funnel_rollup.promise_basis_lead_time + $4,
			 	promise_basis_network = funnel_rollup.promise_basis_network + $5,
			 	orders_split_shipment = funnel_rollup.orders_split_shipment + $6,
			 	promise_to_cutoff_gap_seconds_sum = funnel_rollup.promise_to_cutoff_gap_seconds_sum + $7,
			 	promise_to_cutoff_gap_samples = funnel_rollup.promise_to_cutoff_gap_samples + $8`,
			funnelCol)
		if _, err := tx.Exec(ctx, sql, pathId, bucket, basisCapability, basisLeadTime, basisNetwork, split, gapSeconds, gapSamples); err != nil {
			return fmt.Errorf("analyticsstore: upsert rollup %s with promise KPIs: %w", funnelCol, err)
		}
		return nil
	})
}

// ApplyOrderReceived counts one received order. Idempotent on eventId.
func (p *PostgresProjection) ApplyOrderReceived(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colOrdersReceived)
}

// ApplyOrderAllocated counts one fully-allocated order and its promise KPI
// facts. Idempotent on eventId.
func (p *PostgresProjection) ApplyOrderAllocated(ctx context.Context, eventId, pathId string, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) error {
	return p.applyAllocationOutcome(ctx, eventId, pathId, at, colOrdersAllocated, basis, cutoffAt, splitShipment)
}

// ApplyOrderPartiallyAllocated counts one partially-allocated order and its
// promise KPI facts. Idempotent on eventId.
func (p *PostgresProjection) ApplyOrderPartiallyAllocated(ctx context.Context, eventId, pathId string, at time.Time, basis string, cutoffAt *time.Time, splitShipment bool) error {
	return p.applyAllocationOutcome(ctx, eventId, pathId, at, colOrdersPartiallyAllocated, basis, cutoffAt, splitShipment)
}

// ApplyOrderAllocationFailed counts one hard allocation failure. Idempotent
// on eventId.
func (p *PostgresProjection) ApplyOrderAllocationFailed(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colOrdersAllocationFailed)
}

// ApplyOrderReleased counts one fully-released order. Idempotent on eventId.
func (p *PostgresProjection) ApplyOrderReleased(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colOrdersReleased)
}

// ApplyOrderCancelled counts one cancelled order. Idempotent on eventId.
func (p *PostgresProjection) ApplyOrderCancelled(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colOrdersCancelled)
}

// ApplyLineAllocated counts one allocated order line. Idempotent on eventId.
func (p *PostgresProjection) ApplyLineAllocated(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colLinesAllocated)
}

// ApplyLineBackordered counts one backordered order line. Idempotent on eventId.
func (p *PostgresProjection) ApplyLineBackordered(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colLinesBackordered)
}

// ApplyLineReleased counts one released order line. Idempotent on eventId.
func (p *PostgresProjection) ApplyLineReleased(ctx context.Context, eventId, pathId string, at time.Time) error {
	return p.applyCount(ctx, eventId, pathId, at, colLinesReleased)
}

// ApplyOrderRepromised counts one OrderRepromised event (ADR 0018) onto the
// (hour_bucket)-only repromise_rollup table — deliberately not path_id
// dimensioned, see report.ProjectionStore.ApplyOrderRepromised's doc
// comment. Idempotent on eventId, via the SAME analytics_processed_events
// claim table every other Apply* method uses.
func (p *PostgresProjection) ApplyOrderRepromised(ctx context.Context, eventId string, at time.Time) error {
	return p.withClaim(ctx, eventId, at, func(tx pgx.Tx) error {
		bucket := at.UTC().Truncate(time.Hour)
		if _, err := tx.Exec(ctx,
			`INSERT INTO repromise_rollup (hour_bucket, orders_repromised)
			 VALUES ($1, 1)
			 ON CONFLICT (hour_bucket) DO UPDATE SET
			 	orders_repromised = repromise_rollup.orders_repromised + 1`,
			bucket); err != nil {
			return fmt.Errorf("analyticsstore: upsert repromise rollup: %w", err)
		}
		return nil
	})
}

// Compile-time assertion that PostgresProjection satisfies the write port.
var _ report.ProjectionStore = (*PostgresProjection)(nil)
