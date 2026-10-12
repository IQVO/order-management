package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	kafkaadapter "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// newLogger builds the process-wide structured logger. LOG_LEVEL maps
// debug|info|warn|warning|error (case-insensitive) to the matching
// slog.Level, defaulting to Info for unset or unrecognized values.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
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

// nonNegativeIntEnv reads a non-negative integer from key, falling back to
// fallback for an unset value and, with a warning, for an unparseable or
// negative one. 0 is a valid value (OUTBOX_MAX_ATTEMPTS uses it for
// "unlimited").
func nonNegativeIntEnv(key string, fallback int, logger *slog.Logger) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		logger.Warn("ignoring invalid integer env var", "key", key, "value", raw, "fallback", fallback)
		return fallback
	}
	return n
}

// outboxMaxAttemptsEnv caps how many failed sends an outbox row may
// accumulate before the relay stops claiming it (ADR 0040). 0 = unlimited.
const (
	outboxMaxAttemptsEnv     = "OUTBOX_MAX_ATTEMPTS"
	defaultOutboxMaxAttempts = 20
)

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

// demandProjectionSiteEnv selects the static site every order line's
// demand projects to (the ADR 0035 Phase-1 seam). Unset disables the
// projection entirely — no SiteSkuDemandChanged is ever emitted,
// byte-identical to pre-projection behaviour.
const demandProjectionSiteEnv = "DEMAND_PROJECTION_SITE_ID"

// buildDemandProjection resolves the versioned static demand-site scope
// from the environment. The zero value it returns when
// DEMAND_PROJECTION_SITE_ID is unset leaves the additive projection
// disabled everywhere it is threaded through.
func buildDemandProjection(logger *slog.Logger) usecases.DemandProjectionPolicy {
	site := os.Getenv(demandProjectionSiteEnv)
	if site == "" {
		logger.Info("site/SKU demand projection disabled",
			"hint", "set "+demandProjectionSiteEnv+" to emit SiteSkuDemandChanged on "+kafkaadapter.Topic)
		return usecases.DemandProjectionPolicy{}
	}
	logger.Info("site/SKU demand projection enabled",
		"site_id", site, "assignment_version", usecases.StaticDemandAssignmentVersion)
	return usecases.DemandProjectionPolicy{SiteID: site, AssignmentVersion: usecases.StaticDemandAssignmentVersion}
}
