package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|error (case-insensitive) to the matching slog.Level,
// defaulting to Info for unset or unrecognized values.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// durationEnv reads a Go duration (e.g. "48h", "90m") from key, falling
// back to fallback for an unset or unparseable value.
func durationEnv(key string, fallback time.Duration, logger *slog.Logger) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		logger.Warn("ignoring invalid duration env var", "key", key, "value", raw, "fallback", fallback.String())
		return fallback
	}
	return d
}

// perPathLeadTimes parses PROMISE_PATH_LEAD_TIMES, a comma-separated list
// of pathId=duration pairs (e.g. "pick=24h,singles=6h"). Malformed entries
// are skipped with a warning rather than failing startup: a bad promise
// override should degrade to the default lead time, not take the service
// down.
func perPathLeadTimes(raw string, logger *slog.Logger) map[shared.PathId]time.Duration {
	if raw == "" {
		return nil
	}
	out := make(map[shared.PathId]time.Duration)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		name, value, found := strings.Cut(pair, "=")
		d, err := time.ParseDuration(strings.TrimSpace(value))
		if !found || strings.TrimSpace(name) == "" || err != nil || d <= 0 {
			logger.Warn("ignoring invalid entry in PROMISE_PATH_LEAD_TIMES", "entry", pair)
			continue
		}
		out[shared.PathId(strings.TrimSpace(name))] = d
	}
	return out
}

// dbConfig is the database-related env configuration.
type dbConfig struct {
	url            string
	migrationsURL  string
	migrationsPath string
}

// dbConfigFromEnv reads DATABASE_URL, MIGRATIONS_DATABASE_URL and
// MIGRATIONS_PATH.
//
// MIGRATIONS_DATABASE_URL, when set, is a DIRECT (non-pooled,
// session-mode) Postgres connection string used ONLY for the
// golang-migrate startup step — everything else (the pgxpool this
// process serves requests through) keeps using DATABASE_URL unchanged.
// See buildRepoAdapters' doc comment for the full "why":
// golang-migrate's postgres driver takes a session-scoped
// `SELECT pg_advisory_lock($1)` to serialize concurrent migration
// runs, which PgBouncer's transaction-pooling mode does not support
// (warehouse-infra's PgBouncer rollout, PR #43; this fallback closes
// the fleet-wide bug that rollout introduced — see ADR
// 0029-migrations-direct-postgres-connection.md). Falls back to
// DATABASE_URL when unset, which is every environment that doesn't
// provision the split (local dev, CI integration tests, and any
// cluster whose Terraform predates this fix) — byte-identical to
// this service's behavior before this change in that case.
func dbConfigFromEnv() dbConfig {
	url := os.Getenv("DATABASE_URL")
	return dbConfig{
		url:            url,
		migrationsURL:  getenv("MIGRATIONS_DATABASE_URL", url),
		migrationsPath: getenv("MIGRATIONS_PATH", "migrations"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
