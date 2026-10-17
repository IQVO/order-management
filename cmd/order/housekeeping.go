package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

// housekeepingSettings is the sweeper's configuration (ADR-0032). A zero
// duration disables the corresponding behaviour: interval 0 disables the
// whole sweeper, TTL/retention 0 keep that table's rows forever.
type housekeepingSettings struct {
	interval        time.Duration
	idempotencyTTL  time.Duration
	outboxRetention time.Duration
	// releaseConfirmationRetention is how long a CONFIRMED release
	// confirmation row is kept (ADR 0043); 0 keeps them forever.
	releaseConfirmationRetention time.Duration
	// flagUnconfirmed is the release-confirmation timeout sweep (ADR 0043),
	// run on every pass; nil when the loop is off or its timeout is 0.
	flagUnconfirmed func(context.Context) (int, error)
}

// housekeepingSettingsFromEnv reads HOUSEKEEPING_INTERVAL (default 1h),
// IDEMPOTENCY_KEY_TTL (default 24h) and OUTBOX_RETENTION (default 168h =
// 7d), mirroring inventory-storage's ADR-0026 env knobs exactly, plus
// RELEASE_CONFIRMATION_RETENTION (default 168h; ADR 0043). The
// release-confirmation timeout sweep itself is attached by closeWithSweeper.
func housekeepingSettingsFromEnv(logger *slog.Logger) housekeepingSettings {
	return housekeepingSettings{
		interval:                     housekeepingDurationEnv(logger, "HOUSEKEEPING_INTERVAL", postgres.DefaultSweepInterval),
		idempotencyTTL:               housekeepingDurationEnv(logger, "IDEMPOTENCY_KEY_TTL", postgres.DefaultIdempotencyKeyTTL),
		outboxRetention:              housekeepingDurationEnv(logger, "OUTBOX_RETENTION", postgres.DefaultOutboxRetention),
		releaseConfirmationRetention: housekeepingDurationEnv(logger, releaseConfirmationRetentionEnv, postgres.DefaultReleaseConfirmationRetention),
	}
}

// housekeepingDurationEnv parses a Go duration env var. Unset yields def;
// an invalid or negative value logs a warning and yields def. "0" is valid
// and means "disabled" to the caller (unlike durationEnv above, which is
// for knobs where 0 is meaningless).
func housekeepingDurationEnv(logger *slog.Logger, key string, def time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("ignoring invalid duration env var, using default", "key", key, "value", raw, "default", def.String())
		return def
	}
	return d
}

// sweeperShutdownBudget bounds how long shutdown waits for the current
// sweep pass to finish; a pass is one batched DELETE loop, well under a
// second on any healthy database.
const sweeperShutdownBudget = 10 * time.Second

// closeWithSweeper starts the housekeeping sweeper (ADR-0032) for a
// Postgres-backed process — idempotency keys are written regardless of
// EVENT_PUBLISHER, so it is not tied to the outbox — plus the
// outbox-lag gauge when the outbox is the publish path, and returns the
// closer that stops both BEFORE the pool closes.
func closeWithSweeper(pool *pgxpool.Pool, outboxEnabled bool, logger *slog.Logger) func() {
	settings := housekeepingSettingsFromEnv(logger)
	// ADR 0043: the release-confirmation timeout rides this same loop; there
	// is no second scheduler.
	settings.flagUnconfirmed = buildFlagUnconfirmed(pool, logger)
	stopSweeper := startSweeper(pool, settings, logger)
	var unregisterLagGauge func()
	if outboxEnabled {
		// order.outbox.lag_seconds (ADR-0032): only meaningful when the
		// outbox is the publish path; registered here so its callback
		// is guaranteed to be unregistered before pool.Close.
		reg, err := postgres.RegisterOutboxLagGauge(pool)
		if err != nil {
			logger.Error("outbox lag gauge unavailable; the relay will run without order.outbox.lag_seconds", "error", err)
		} else {
			unregisterLagGauge = func() {
				if err := reg.Unregister(); err != nil {
					logger.Warn("outbox lag gauge unregister failed", "error", err)
				}
			}
		}
	}
	return func() {
		stopSweeper()
		if unregisterLagGauge != nil {
			unregisterLagGauge()
		}
		pool.Close()
	}
}

// startSweeper runs the housekeeping Sweeper (ADR-0032) in a goroutine and
// returns a stop func that cancels it and waits, bounded by
// sweeperShutdownBudget, for the current pass to finish. With interval 0 it
// starts nothing and returns a no-op.
func startSweeper(pool *pgxpool.Pool, cfg housekeepingSettings, logger *slog.Logger) func() {
	if cfg.interval <= 0 {
		logger.Info("housekeeping sweeper disabled (HOUSEKEEPING_INTERVAL=0)")
		return func() {}
	}
	sweeper := postgres.NewSweeper(pool,
		postgres.WithSweeperLogger(logger),
		postgres.WithSweepInterval(cfg.interval),
		postgres.WithIdempotencyKeyTTL(cfg.idempotencyTTL),
		postgres.WithOutboxRetention(cfg.outboxRetention),
		postgres.WithReleaseConfirmationRetention(cfg.releaseConfirmationRetention),
		postgres.WithReleaseConfirmationSweep(cfg.flagUnconfirmed),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sweeper.Run(ctx) // only ever returns nil, on cancellation
	}()
	logger.Info("housekeeping sweeper started", "interval", cfg.interval,
		"idempotency_key_ttl", cfg.idempotencyTTL, "outbox_retention", cfg.outboxRetention,
		"release_confirmation_retention", cfg.releaseConfirmationRetention,
		"release_confirmation_sweep", cfg.flagUnconfirmed != nil)
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(sweeperShutdownBudget):
			logger.Warn("housekeeping sweeper did not stop before the shutdown budget")
		}
	}
}
