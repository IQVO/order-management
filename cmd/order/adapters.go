package main

import (
	"context"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/adapters/outbound/events"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/bootretry"
)

// buildRepoAdapters wires the Postgres adapters when DATABASE_URL is set,
// or falls back to the in-memory adapters for local development without a
// database. The event publisher defaults to that same memory/Postgres
// choice ("log"), or can be switched to the Kafka integration-events
// publisher via eventPublisher="kafka" (EVENT_PUBLISHER env), independent
// of which repos are in use — mirroring inventory-storage's
// EVENT_PUBLISHER=kafka|log pattern exactly. The returned *pgxpool.Pool is
// nil for the in-memory case; buildRepromiseProcessedEvents reuses it
// rather than opening a second pool against the same database.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below —
// the pgxpool opened just after it (and used for every subsequent
// request) always uses databaseURL. They are deliberately different
// connection strings in a PgBouncer-fronted environment: golang-migrate's
// postgres driver takes a session-scoped `SELECT pg_advisory_lock($1)` to
// serialize concurrent migration runs across replicas starting at the
// same time, and PgBouncer's transaction-pooling mode (this fleet's
// pool_mode for every OLTP DATABASE_URL, warehouse-infra PR #43) does not
// support session-scoped state — each statement in one logical client
// session can land on a different physical backend connection, so the
// advisory lock never behaves as a real mutex. Losing replicas crash-loop
// with `pq: unnamed prepared statement does not exist` / `pq: canceling
// statement due to statement timeout` until one wins the race. See ADR
// 0029-migrations-direct-postgres-connection.md for the full incident and
// fix. Callers pass MIGRATIONS_DATABASE_URL when set (warehouse-infra now
// provisions it as a direct, non-pooled DSN alongside DATABASE_URL) or
// fall back to databaseURL itself for any environment that doesn't
// provision the split (local dev, CI integration tests) — byte-identical
// to this function's behavior before this parameter existed in that case.
//
// Both the migration run and the post-open ping are RETRIED with
// exponential backoff (mirroring network-fulfillment PR #7): in this
// cluster every injected pod's first outbound TCP dial is reset ~10s
// after the app starts (Istio native sidecars run as an init container
// with restartPolicy=Always, so holdApplicationUntilProxyStarts is a
// no-op), and this service dials Postgres for migrations before it ever
// starts serving. A single attempt turns that known, transient condition
// into CrashLoopBackOff — confirmed live (129 restarts). The retry does
// NOT weaken the fail-closed rule: after the ~31s budget is exhausted
// this still returns the real underlying error and the caller still
// refuses to boot.
func buildRepoAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath, eventPublisher string, logger *slog.Logger) (
	ports.OrderRepo, ports.EventPublisher, *pgxpool.Pool, ports.UnitOfWork, *postgres.OutboxRelay, func(), error,
) {
	noop := func() {}

	var (
		orders     ports.OrderRepo
		pool       *pgxpool.Pool
		closeRepos = noop
	)

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		orders = memory.NewOrderRepo()
	} else {
		var err error
		pool, err = openPostgres(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
		if err != nil {
			return nil, nil, nil, nil, nil, noop, err
		}
		orders = postgres.NewOrderRepo(pool)
		// ADR-0032: the housekeeping sweeper (idempotency_keys TTL +
		// published outbox retention + outbox-lag gauge registration
		// when the outbox is the publish path) starts for EVERY
		// Postgres-backed process — idempotency keys are written
		// regardless of EVENT_PUBLISHER — and must be stopped BEFORE
		// the pool closes, which closeWithSweeper guarantees.
		closeRepos = closeWithSweeper(pool, strings.EqualFold(eventPublisher, "kafka"), logger)
	}

	if !strings.EqualFold(eventPublisher, "kafka") {
		// No transactional outbox without Kafka to relay onto: the log
		// publisher is used regardless of persistence mode (mirrors
		// process-path-management's buildEventPublisher convention) —
		// use cases run their Save+Publish back to back (uow=nil, see
		// atomically()) exactly as before this rollout. This also
		// retires the old postgres.EventPublisher (`events` table with
		// no relay ever draining it); Postgres persistence without Kafka
		// now always logs instead of silently accumulating undelivered
		// rows.
		return orders, events.NewLogPublisher(logger), pool, nil, nil, closeRepos, nil
	}

	publisher, uow, relay, closeAll := buildKafkaPublishing(orders, pool, closeRepos, logger)
	return orders, publisher, pool, uow, relay, closeAll, nil
}

// openPostgres runs the golang-migrate step (against migrationsDatabaseURL,
// see buildRepoAdapters), opens the request pool on databaseURL and pings
// it, retrying each boot-time dial per bootretry. On error the pool (if
// opened) is closed and nothing is returned.
func openPostgres(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (*pgxpool.Pool, error) {
	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return nil, err
	}
	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	// NewPool/ParseConfig do not themselves establish a connection,
	// so without this the first real failure would surface inside a
	// request rather than at boot — turning a misconfigured
	// deployment into an intermittent 500 instead of a refusal to
	// start.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// buildRepromiseProcessedEvents selects RepromiseOrder's idempotency-gate
// adapter (ADR 0014 §5 / ADR 0018): Postgres-backed when pool is non-nil
// (the same pool buildRepoAdapters already opened against DATABASE_URL —
// migration 0004 already ran as part of that same RunMigrations call), or
// in-memory for local development with no DATABASE_URL, mirroring every
// other repo adapter's memory/Postgres selection in this composition root.
func buildRepromiseProcessedEvents(pool *pgxpool.Pool, logger *slog.Logger) ports.RepromiseProcessedEvents {
	if pool == nil {
		logger.Info("database url not configured; using in-memory repromise idempotency gate")
		return memory.NewRepromiseProcessedEventsRepo()
	}
	return postgres.NewRepromiseProcessedEventsRepo(pool)
}
