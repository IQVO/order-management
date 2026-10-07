// Package productclassificationcopy implements
// ports.ProductClassificationLookup from a LOCAL copy of product-master's
// classifications (ADR 0036), plus the write-side ports the
// ApplyProductClassification use case fills that copy through.
//
// The copy is fed by inbound/kafka.ProductClassificationConsumer from
// product-master's ProductClassified events. There is no network call at
// lookup time: a stored row answers Known=true with its handling tags; an
// absent SKU — or an unreadable copy — answers Known=false with a nil
// error. That is exactly what the deleted HTTP client against
// inventory-storage returned for a 404 and for a transport error, so
// ReceiveOrder's fail-open handling (ADR 0016) is unchanged.
//
// Three implementations:
//
//   - PostgresStore: product_classification_copy (migration 0011).
//   - MemoryStore: the same behaviour in memory, for runs without
//     DATABASE_URL and for tests.
//   - PermissiveLookup: PRODUCT_CLASSIFICATION_MODE=permissive, always
//     Known=false, no consumer.
package productclassificationcopy

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/pgtx"
)

// querier is the subset of pgx both *pgxpool.Pool and pgx.Tx satisfy.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// querierFrom joins the UnitOfWork transaction bound to ctx (internal/pgtx,
// the service's one transaction-in-context mechanism) or falls back to the
// pool, so the claim and the upsert commit together when the use case runs
// them inside postgres.UnitOfWork.
func querierFrom(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := pgtx.TxFrom(ctx); ok {
		return tx
	}
	return pool
}

// PostgresStore is the pgxpool-backed copy: it answers the lookup port and
// is the ports.ProductClassificationCopy the consumer writes through.
type PostgresStore struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

var (
	_ ports.ProductClassificationLookup = (*PostgresStore)(nil)
	_ ports.ProductClassificationCopy   = (*PostgresStore)(nil)
)

// NewPostgresStore constructs a PostgresStore over pool. A nil logger
// discards the read-error warnings.
func NewPostgresStore(pool *pgxpool.Pool, logger *slog.Logger) *PostgresStore {
	return &PostgresStore{pool: pool, logger: logger}
}

// GetClassification reads sku's row. An absent row and a read error are
// both Known=false with a nil error (fail-open, see the package doc).
func (s *PostgresStore) GetClassification(ctx context.Context, sku string) (ports.ProductClassification, error) {
	var tags []string
	err := querierFrom(ctx, s.pool).QueryRow(ctx,
		`SELECT handling_tags FROM product_classification_copy WHERE sku = $1`, sku).Scan(&tags)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	case err != nil:
		if s.logger != nil {
			s.logger.WarnContext(ctx, "product classification copy unreadable; treating the SKU as unclassified", "sku", sku, "error", err)
		}
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	}
	return ports.ProductClassification{SKU: sku, HandlingTags: tags, Known: true}, nil
}

// Upsert inserts rec, or replaces the stored row only when its version is
// LOWER than rec.Version, in one statement: the conditional DO UPDATE makes
// the version guard atomic without a read-then-write.
func (s *PostgresStore) Upsert(ctx context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	tags := rec.HandlingTags
	if tags == nil {
		tags = []string{}
	}
	var temperature, hazard any
	if rec.TemperatureClass != "" {
		temperature = rec.TemperatureClass
	}
	if rec.DotHazardClass != 0 {
		hazard = rec.DotHazardClass
	}
	tag, err := querierFrom(ctx, s.pool).Exec(ctx, `
INSERT INTO product_classification_copy (sku, handling_tags, temperature_class, dot_hazard_class, version, updated_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (sku) DO UPDATE SET
	handling_tags = EXCLUDED.handling_tags,
	temperature_class = EXCLUDED.temperature_class,
	dot_hazard_class = EXCLUDED.dot_hazard_class,
	version = EXCLUDED.version,
	updated_at = now()
WHERE product_classification_copy.version < EXCLUDED.version`,
		rec.SKU, tags, temperature, hazard, rec.Version)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// PostgresProcessedEvents is the pgxpool-backed
// ports.ProductClassificationProcessedEvents, over
// product_classification_processed_events (migration 0011).
type PostgresProcessedEvents struct {
	pool *pgxpool.Pool
}

var _ ports.ProductClassificationProcessedEvents = (*PostgresProcessedEvents)(nil)

// NewPostgresProcessedEvents constructs the gate over pool.
func NewPostgresProcessedEvents(pool *pgxpool.Pool) *PostgresProcessedEvents {
	return &PostgresProcessedEvents{pool: pool}
}

// MarkProcessed records eventId if absent, returning true iff this call
// newly recorded it. It joins the ctx transaction when there is one, which
// is what makes the claim atomic with the upsert it guards.
func (r *PostgresProcessedEvents) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO product_classification_processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
