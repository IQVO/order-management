package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RepromiseProcessedEventsRepo is a pgxpool-backed implementation of
// ports.RepromiseProcessedEvents (ADR 0014 §5 / ADR 0018), over the
// repromise_processed_events table (migration 0004).
type RepromiseProcessedEventsRepo struct {
	pool *pgxpool.Pool
}

// NewRepromiseProcessedEventsRepo constructs a RepromiseProcessedEventsRepo
// over pool.
func NewRepromiseProcessedEventsRepo(pool *pgxpool.Pool) *RepromiseProcessedEventsRepo {
	return &RepromiseProcessedEventsRepo{pool: pool}
}

// MarkProcessed records eventId in repromise_processed_events if absent,
// returning true iff this call newly recorded it.
func (r *RepromiseProcessedEventsRepo) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO repromise_processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
