// Command a2a is the composition root for the Order Management A2A agent: it
// wires env config to outbound adapters, adapters to the GetOrder read use case
// and the analytics report store, and those to the inbound A2A adapter, then
// serves Agent2Agent JSON-RPC over HTTP with the Agent Card at the well-known
// path. It is a third, independent deployable alongside cmd/order (the HTTP
// service) and cmd/mcp, per ADR-0046.
//
// READ-ONLY, like cmd/mcp (ADR-0010): order-management's MCP surface offers no
// write, so the A2A write gate allows none here. This binary therefore wires
// only the read side and has no Kafka/outbox/publisher configuration at all.
//
// Unlike cmd/mcp it has no in-memory mode and no degraded analytics mode: a
// delegated answer built on an empty store would be a plausible-looking lie
// (all-zero KPIs), so DATABASE_URL and ANALYTICS_DATABASE_URL are mandatory. A
// missing mandatory variable is a boot error naming it, raised before
// telemetry, any connection and the listener.
//
// The agent is mounted unauthenticated: the fleet-wide REST/MCP static-bearer
// auth layer has been removed (ADR-0012).
//
// Environment:
//
//	DATABASE_URL             REQUIRED. OLTP Postgres DSN (orders)
//	ANALYTICS_DATABASE_URL   REQUIRED. Analytical Postgres DSN (read-only use)
//	A2A_AGENT_URL            REQUIRED. Absolute http(s) URL of this agent's
//	                         JSON-RPC endpoint, advertised on the Agent Card
//	MIGRATIONS_DATABASE_URL  direct DSN for the boot migrations (default DATABASE_URL)
//	MIGRATIONS_PATH          migrations directory (default migrations)
//	A2A_ADDR                 listen address (default :8093)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	inbounda2a "github.com/claudioed/order-management/internal/adapters/inbound/a2a"
	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/bootretry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("a2a agent exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadBootConfig()
	if err != nil {
		return err
	}

	serviceName := getenv("OTEL_SERVICE_NAME", "order-management-a2a")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(getenv("LOG_LEVEL", "info"))}))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	version := getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion)
	shutdownTelemetry, err := telemetry.Setup(
		context.Background(),
		serviceName,
		version,
		getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint),
	)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown reported an error", "error", err)
		}
	}()

	deps, closeDeps, err := buildDeps(context.Background(), cfg, logger)
	if err != nil {
		return err
	}
	defer closeDeps()

	card := inbounda2a.Card(inbounda2a.Config{AgentURL: cfg.agentURL, Version: version})
	handler := newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(deps)), card, serviceName)
	srv := &http.Server{Addr: getenv("A2A_ADDR", ":8093"), Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	return serve(srv, logger)
}

// serve runs the listener until SIGINT/SIGTERM (graceful shutdown) or until it
// fails on its own: a listener that could not bind ends the process non-zero
// instead of leaving a pod alive that serves nothing.
func serve(srv *http.Server, logger *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("a2a agent listening (JSON-RPC)", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-errCh:
		return fmt.Errorf("a2a listener failed: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// bootConfig is the mandatory configuration of cmd/a2a: real Postgres (OLTP and
// analytics) and the advertised agent URL, with no default and no degraded mode
// behind any of it.
type bootConfig struct {
	databaseURL  string
	analyticsURL string
	agentURL     string
}

// errNoAgentURL is the boot error for a missing A2A_AGENT_URL: the Agent Card
// must advertise the absolute URL clients dial (ADR-0046).
var errNoAgentURL = errors.New("A2A_AGENT_URL is required: the Agent Card must advertise the absolute URL of this agent's JSON-RPC endpoint (ADR-0046), e.g. http://<release>-a2a.<ns>.svc.cluster.local:8093/")

// checkAgentURL validates A2A_AGENT_URL: an absolute http(s) URL. The card is
// the discovery contract -- a relative or schemeless URL is a misconfiguration
// every client would hit, so it is refused at boot, never silently served.
func checkAgentURL(raw string) error {
	if raw == "" {
		return errNoAgentURL
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("A2A_AGENT_URL=%q is not an absolute http(s) URL: the Agent Card must advertise the endpoint clients dial (ADR-0046)", raw)
	}
	return nil
}

// loadBootConfig reads and validates the mandatory environment. Every problem
// is reported at once, each naming its variable, so the process fails fast at
// boot, before telemetry, any connection and the listener.
func loadBootConfig() (bootConfig, error) {
	cfg := bootConfig{
		databaseURL:  os.Getenv("DATABASE_URL"),
		analyticsURL: os.Getenv("ANALYTICS_DATABASE_URL"),
		agentURL:     strings.TrimSpace(os.Getenv("A2A_AGENT_URL")),
	}
	var errs []error
	if cfg.databaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required: this agent reads orders from real Postgres (no in-memory mode)"))
	}
	if cfg.analyticsURL == "" {
		errs = append(errs, errors.New("ANALYTICS_DATABASE_URL is required: promise_health reads the analytics data product, and an empty store would answer all-zero KPIs"))
	}
	if err := checkAgentURL(cfg.agentURL); err != nil {
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

// buildDeps wires the repositories and the analytics store into the A2A
// adapter's Deps. The A2A adapter reuses the SAME read use case the HTTP and
// MCP adapters use: GetOrder, over the same repo. No write use case is wired --
// see this file's package doc comment. The returned func releases the pools.
func buildDeps(ctx context.Context, cfg bootConfig, logger *slog.Logger) (inbounda2a.Deps, func(), error) {
	// See cmd/order/main.go's identical fallback and buildAdapters' doc comment
	// for the full "why" (session-scoped pg_advisory_lock vs PgBouncer
	// transaction-pooling incompatibility, ADR
	// 0029-migrations-direct-postgres-connection.md). This binary also runs
	// migrations on start, so it needs the same direct-connection split.
	migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", cfg.databaseURL)
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	orders, closeAdapters, err := buildAdapters(ctx, cfg.databaseURL, migrationsDatabaseURL, migrationsPath, logger)
	if err != nil {
		return inbounda2a.Deps{}, func() {}, err
	}

	// promise_health reads the analytics data product (ADR 0006/0014 §6) over
	// its own read-only pool, the SAME store the reports REST service queries
	// -- completely separate from the OLTP DATABASE_URL above.
	promiseHealthStore, closePromiseHealth, err := buildPromiseHealthStore(ctx, cfg.analyticsURL)
	if err != nil {
		closeAdapters()
		return inbounda2a.Deps{}, func() {}, err
	}

	deps := inbounda2a.Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: promiseHealthStore,
	}
	return deps, func() { closePromiseHealth(); closeAdapters() }, nil
}

// buildAdapters wires the Postgres OrderRepo. DATABASE_URL is mandatory
// (loadBootConfig refuses to boot without it), so unlike cmd/mcp and
// cmd/order there is NO in-memory fallback here; there is also no event
// publisher, since this agent is read-only.
//
// migrationsDatabaseURL is used ONLY for the golang-migrate step below,
// mirroring cmd/order/main.go's buildRepoAdapters exactly — see its doc
// comment for the full "why" a direct, non-pooled connection is needed
// here even though the pgxpool opened just after (databaseURL) stays on
// PgBouncer.
//
// Both the migration run and the post-open ping are RETRIED with
// exponential backoff (mirroring cmd/order/main.go and
// network-fulfillment PR #7): in this cluster every injected pod's first
// outbound TCP dial is reset ~10s after the app starts (Istio native
// sidecars run as an init container with restartPolicy=Always, so
// holdApplicationUntilProxyStarts is a no-op). A single attempt turns
// that known, transient condition into CrashLoopBackOff. The retry does
// NOT weaken the fail-closed rule: after the budget (5 attempts, 15s of
// backoff) is exhausted
// this still returns the real underlying error and the caller still
// refuses to boot.
func buildAdapters(ctx context.Context, databaseURL, migrationsDatabaseURL, migrationsPath string, logger *slog.Logger) (ports.OrderRepo, func(), error) {
	noop := func() {}

	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)
	}); err != nil {
		return nil, noop, err
	}

	pool, err := postgres.NewPool(ctx, databaseURL)
	if err != nil {
		return nil, noop, err
	}
	// NewPool/ParseConfig do not themselves establish a connection, so
	// without this the first real failure would surface inside a
	// request rather than at boot.
	if err := bootretry.Retry(ctx, logger, "ping database", func() error {
		return pool.Ping(ctx)
	}); err != nil {
		pool.Close()
		return nil, noop, err
	}

	return postgres.NewOrderRepo(pool), pool.Close, nil
}

// buildPromiseHealthStore wires promise_health's port
// (inbounda2a.PromiseHealthStore) to a read-only Postgres pool over the SAME
// analytical database cmd/order-reports reads (ADR-0006). Unlike cmd/mcp there
// is no empty-store fallback: ANALYTICS_DATABASE_URL is mandatory and a pool
// that cannot be configured is a boot error, because an empty store would
// answer all-zero KPIs as if they were facts. The returned adapter translates
// the real report.ReportStore into the A2A package's own PromiseHealthStore/
// PromiseHealthRow shapes at this wiring boundary (ADR-0008's dependency rule).
//
// NewReadOnlyPool/ParseConfig never dial the network, so the first real dial
// happens lazily on the first promise_health call, outside the ~10s post-start
// first-dial-reset window the bootretry budget exists to survive: no boot-time
// ping is performed here and a retry would add nothing.
func buildPromiseHealthStore(ctx context.Context, analyticsURL string) (inbounda2a.PromiseHealthStore, func(), error) {
	pool, err := analyticsstore.NewReadOnlyPool(ctx, analyticsURL)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open analytics read-only pool: %w", err)
	}
	return reportStoreAdapter{analyticsstore.NewPostgresReport(pool)}, pool.Close, nil
}

// reportStoreAdapter adapts a report.ReportStore into
// inbounda2a.PromiseHealthStore, translating report.Row into
// inbounda2a.PromiseHealthRow. This composition root is the one place
// allowed to depend on BOTH internal/analytics/report and
// internal/adapters/inbound/a2a, so the translation lives here rather than
// inside the A2A package itself.
type reportStoreAdapter struct {
	store report.ReportStore
}

func (a reportStoreAdapter) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]inbounda2a.PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      pathId,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]inbounda2a.PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, inbounda2a.PromiseHealthRow{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			PromiseBasisNetwork:       row.PromiseBasisNetwork,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
			PromiseToCutoffGapSamples: row.PromiseToCutoffGapSamples,
		})
	}
	return rows, nil
}

// logLevel mirrors cmd/order's newLogger level parsing (case-insensitive;
// "warning" is an alias of "warn"), kept local to this binary so both
// composition roots stay free-standing.
func logLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
