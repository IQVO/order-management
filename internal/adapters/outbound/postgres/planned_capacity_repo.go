package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/domain/order"
)

// PlannedCapacityRepo is the pgxpool-backed ports.PlannedCapacityRepo
// (ADR 0031), over planned_capacity_windows (migration 0010). Inside a
// ports.UnitOfWork scope it joins the ctx transaction (so the row commits
// or rolls back with the idempotency claim); outside one it runs its own.
type PlannedCapacityRepo struct {
	pool *pgxpool.Pool
}

// NewPlannedCapacityRepo constructs a PlannedCapacityRepo over pool.
func NewPlannedCapacityRepo(pool *pgxpool.Pool) *PlannedCapacityRepo {
	return &PlannedCapacityRepo{pool: pool}
}

const plannedCapacityColumns = `plan_id, warehouse_id, location, path_id, window_start, window_end,
	assigned_demand, capacity_over_window, shortage, bottleneck_step, status, event_time`

func scanPlannedCapacity(row pgx.Row) (order.PlannedCapacityWindow, error) {
	var w order.PlannedCapacityWindow
	var status string
	err := row.Scan(&w.PlanID, &w.WarehouseID, &w.Location, &w.PathID, &w.Start, &w.End,
		&w.AssignedDemand, &w.CapacityOverWindow, &w.Shortage, &w.BottleneckStep, &status, &w.AsOf)
	w.Status = order.PlannedCapacityStatus(status)
	w.Start, w.End, w.AsOf = w.Start.UTC(), w.End.UTC(), w.AsOf.UTC()
	return w, err
}

// Upsert writes w under its plan id if it supersedes the stored row.
// The existing row is read FOR UPDATE and the decision is made by the one
// domain rule (order.PlannedCapacityWindow.Supersedes), so Postgres and the
// in-memory adapter cannot disagree.
func (r *PlannedCapacityRepo) Upsert(ctx context.Context, w order.PlannedCapacityWindow) (applied bool, err error) {
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = rollback(ctx)
		}
	}()

	prev, err := scanPlannedCapacity(tx.QueryRow(ctx,
		`SELECT `+plannedCapacityColumns+` FROM planned_capacity_windows WHERE plan_id = $1 FOR UPDATE`, w.PlanID))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		err = nil
	case err != nil:
		return false, err
	case !w.Supersedes(prev):
		return false, commit(ctx)
	}

	if _, err = tx.Exec(ctx, `
INSERT INTO planned_capacity_windows (`+plannedCapacityColumns+`, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
ON CONFLICT (plan_id) DO UPDATE SET
	warehouse_id = EXCLUDED.warehouse_id, location = EXCLUDED.location, path_id = EXCLUDED.path_id,
	window_start = EXCLUDED.window_start, window_end = EXCLUDED.window_end,
	assigned_demand = EXCLUDED.assigned_demand, capacity_over_window = EXCLUDED.capacity_over_window,
	shortage = EXCLUDED.shortage, bottleneck_step = EXCLUDED.bottleneck_step,
	status = EXCLUDED.status, event_time = EXCLUDED.event_time, updated_at = now()`,
		w.PlanID, w.WarehouseID, w.Location, w.PathID, w.Start, w.End,
		w.AssignedDemand, w.CapacityOverWindow, w.Shortage, w.BottleneckStep, string(w.Status), w.AsOf); err != nil {
		return false, err
	}
	return true, commit(ctx)
}

// ListByLocation returns the windows at location ending after endingAfter.
func (r *PlannedCapacityRepo) ListByLocation(ctx context.Context, location string, endingAfter time.Time) ([]order.PlannedCapacityWindow, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
SELECT `+plannedCapacityColumns+` FROM planned_capacity_windows
WHERE location = $1 AND window_end > $2
ORDER BY window_start, plan_id`, location, endingAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []order.PlannedCapacityWindow
	for rows.Next() {
		w, err := scanPlannedCapacity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// PlannedCapacityProcessedEventsRepo is the pgxpool-backed
// ports.PlannedCapacityProcessedEvents, over
// planned_capacity_processed_events (migration 0010).
type PlannedCapacityProcessedEventsRepo struct {
	pool *pgxpool.Pool
}

// NewPlannedCapacityProcessedEventsRepo constructs the repo over pool.
func NewPlannedCapacityProcessedEventsRepo(pool *pgxpool.Pool) *PlannedCapacityProcessedEventsRepo {
	return &PlannedCapacityProcessedEventsRepo{pool: pool}
}

// MarkProcessed records eventId if absent, returning true iff this call
// newly recorded it. It joins the ctx transaction when there is one, which
// is what makes the claim atomic with the work it guards; a concurrent
// duplicate blocks on the primary key until the other transaction ends.
func (r *PlannedCapacityProcessedEventsRepo) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO planned_capacity_processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
