// Command mcp is the composition root for the Order Management MCP
// server: it wires env config to outbound adapters, adapters to the
// GetOrder read use case, and that to the inbound MCP adapter, then
// serves MCP over Streamable HTTP. It is a second, independent
// deployable alongside cmd/order (the HTTP service), per ADR-0010.
//
// order-management exposes no write use case over MCP: CancelOrder,
// ReceiveOrder, Allocate and RetryAllocate are all order-lifecycle
// commands with real business invariants enforced by the domain --
// none is a decision an MCP-calling agent should make on this context's
// behalf. This server therefore wires only the read side (GetOrder) and
// exposes only a read tool.
//
// The server is mounted unauthenticated: the fleet-wide REST/MCP
// static-bearer auth layer has been removed.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundmcp "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/bootretry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("mcp server exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	serviceName := getenv("OTEL_SERVICE_NAME", "order-management-mcp")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(getenv("LOG_LEVEL", "info"))}))
	slog.SetDefault(logger)

	// Same non-blocking telemetry setup as the HTTP service: an unreachable
	// Collector degrades to dropped telemetry, never a server that won't start.
	shutdownTelemetry, err := telemetry.Setup(
		context.Background(),
		serviceName,
		getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
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

	httpAddr := getenv("MCP_ADDR", ":8090")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	orders, closeAdapters, err := buildAdapters(context.Background(), databaseURL, migrationsPath, logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	// get_promise_health reads the analytics data product (ADR 0006/0014 §6)
	// over its own read-only pool, the SAME store the reports REST service
	// queries -- completely separate from the OLTP DATABASE_URL orders/
	// GetOrder use above. An unset ANALYTICS_DATABASE_URL falls back to an
	// empty in-memory store rather than failing the whole MCP server to
	// start: get_promise_health then simply reports all-zero KPIs, the same
	// "degrade, don't crash" posture buildAdapters already uses for
	// DATABASE_URL.
	promiseHealthStore, closePromiseHealth := buildPromiseHealthStore(context.Background(), logger)
	defer closePromiseHealth()

	// The MCP adapter reuses the SAME read use case the HTTP adapter uses:
	// GetOrder, over the same repo. No write use case is wired -- see this
	// file's own package doc comment.
	deps := inboundmcp.Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: promiseHealthStore,
	}
	server := inboundmcp.NewServer(deps)
	handler := inboundmcp.Handler(server)

	srv := &http.Server{Addr: httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		logger.Info("mcp server listening (Streamable HTTP)", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("mcp server failed", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// buildAdapters wires the Postgres OrderRepo when DATABASE_URL is set, or
// falls back to the in-memory repo for local development without a
// database -- exactly as cmd/order/main.go's buildRepoAdapters does
// (minus the event publisher, which only the write-side OLTP binary
// needs).
//
// Both the migration run and the post-open ping are RETRIED with
// exponential backoff (mirroring cmd/order/main.go and
// network-fulfillment PR #7): in this cluster every injected pod's first
// outbound TCP dial is reset ~10s after the app starts (Istio native
// sidecars run as an init container with restartPolicy=Always, so
// holdApplicationUntilProxyStarts is a no-op). A single attempt turns
// that known, transient condition into CrashLoopBackOff. The retry does
// NOT weaken the fail-closed rule: after the ~31s budget is exhausted
// this still returns the real underlying error and the caller still
// refuses to boot.
func buildAdapters(ctx context.Context, databaseURL, migrationsPath string, logger *slog.Logger) (ports.OrderRepo, func(), error) {
	noop := func() {}

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		return memory.NewOrderRepo(), noop, nil
	}

	if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
		return postgres.RunMigrations(databaseURL, migrationsPath)
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

// buildPromiseHealthStore wires get_promise_health's port
// (inboundmcp.PromiseHealthStore). ANALYTICS_DATABASE_URL set -> a
// read-only Postgres pool over the SAME analytical database
// cmd/order-reports reads (ADR-0006); unset -> an empty in-memory
// report.ReportStore, so the server still starts and the tool simply
// reports all-zero KPIs rather than the whole MCP server failing to boot
// over an optional analytics dependency. The returned adapter translates
// the real report.ReportStore into the MCP package's own
// PromiseHealthStore/PromiseHealthRow shapes at this wiring boundary — see
// inboundmcp.PromiseHealthStore's doc comment for why that indirection
// exists (ADR-0008's MCP adapter dependency fitness rule).
//
// Deliberately NOT wrapped in bootretry.Retry, unlike buildAdapters
// above: this path already degrades to an in-memory store on ANY error
// rather than failing boot, and NewReadOnlyPool/ParseConfig never
// dials the network itself (no boot-time ping is performed here) — the
// first real dial happens lazily on the first get_promise_health call,
// outside the ~10s post-start window the retry budget exists to survive.
// There is therefore no boot-time first-dial-reset bug on this path to
// fix; adding a retry here would only delay the graceful fallback.
func buildPromiseHealthStore(ctx context.Context, logger *slog.Logger) (inboundmcp.PromiseHealthStore, func()) {
	noop := func() {}

	analyticsURL := os.Getenv("ANALYTICS_DATABASE_URL")
	if analyticsURL == "" {
		logger.Info("analytics database url not configured; get_promise_health will report empty KPIs")
		return reportStoreAdapter{analyticsstore.NewMemoryStore()}, noop
	}

	pool, err := analyticsstore.NewReadOnlyPool(ctx, analyticsURL)
	if err != nil {
		logger.Warn("could not open analytics read-only pool; get_promise_health will report empty KPIs", "error", err)
		return reportStoreAdapter{analyticsstore.NewMemoryStore()}, noop
	}

	return reportStoreAdapter{analyticsstore.NewPostgresReport(pool)}, pool.Close
}

// reportStoreAdapter adapts a report.ReportStore into
// inboundmcp.PromiseHealthStore, translating report.Row into
// inboundmcp.PromiseHealthRow. This composition root is the one place
// allowed to depend on BOTH internal/analytics/report and
// internal/adapters/inbound/mcp, so the translation lives here rather than
// inside the MCP package itself.
type reportStoreAdapter struct {
	store report.ReportStore
}

func (a reportStoreAdapter) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]inboundmcp.PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      pathId,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]inboundmcp.PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, inboundmcp.PromiseHealthRow{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
			PromiseToCutoffGapSamples: row.PromiseToCutoffGapSamples,
		})
	}
	return rows, nil
}

// logLevel mirrors cmd/order/main.go's newLogger level parsing, kept local
// to this binary so both composition roots stay free-standing.
func logLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
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
