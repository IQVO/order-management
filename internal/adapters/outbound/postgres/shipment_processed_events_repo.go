package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ShipmentProcessedEventsRepo is a pgxpool-backed implementation of
// ports.ShipmentProcessedEvents (ADR 0051), over the
// shipment_processed_events table (migration 0014).
type ShipmentProcessedEventsRepo struct {
	pool *pgxpool.Pool
}

// NewShipmentProcessedEventsRepo constructs a ShipmentProcessedEventsRepo
// over pool.
func NewShipmentProcessedEventsRepo(pool *pgxpool.Pool) *ShipmentProcessedEventsRepo {
	return &ShipmentProcessedEventsRepo{pool: pool}
}

// MarkProcessed records eventId in shipment_processed_events if absent,
// returning true iff this call newly recorded it.
func (r *ShipmentProcessedEventsRepo) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO shipment_processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
