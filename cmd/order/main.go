// Command order is the composition root: it wires env config into
// adapters, adapters into use cases, and use cases into the HTTP router.
// It is the only package that reads environment variables and the only
// package that knows both a port and its implementation. The root is split
// by concern within package main:
//
//	main.go          run(): the startup sequence, in order
//	config.go        env readers (getenv, durations, DB/migration settings)
//	adapters.go      repo + event-publisher selection (memory vs Postgres)
//	kafka.go         Kafka publishing, outbox relay, RepromiseOrder consumer
//	outbound.go      inventory-storage client, catalogue, promise policy
//	product_classification.go  ADR 0036 local classification copy + consumer
//	http.go          inbound server + http.Server construction
//	housekeeping.go  idempotency/outbox sweeper
//	shutdown.go      serve loop and ADR-0025 graceful shutdown
//	planned_capacity.go, wiring.go  ADR 0031 / catalogue consumers
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
)

// DefaultSiteId is used when DEFAULT_SITE_ID is unset. ADR 0014 step A
// does not yet model which site an order ships from (a known
// simplification for this phase — see order.PromisePolicy's doc
// comment); every promise in this phase is computed against one
// configured site.
const DefaultSiteId = "site-1"

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := newLogger(getenv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	ctx := context.Background()

	serviceName, shutdownTelemetry, err := setupServiceTelemetry(ctx, logger)
	if err != nil {
		return err
	}
	defer flushTelemetry(shutdownTelemetry, logger)

	orderMetrics, err := telemetry.NewOrderMetrics()
	if err != nil {
		return err
	}

	httpAddr := getenv("HTTP_ADDR", ":8080")
	db := dbConfigFromEnv()

	// ADR 0036: validate PRODUCT_CLASSIFICATION_* before anything else is
	// built, so a stale PRODUCT_CLASSIFICATION_MODE=http fails boot first.
	classificationCfg, err := classificationConfigFromEnv()
	if err != nil {
		return err
	}

	orders, publisher, dbPool, uow, relay, closeAdapters, err := buildRepoAdapters(ctx, db.url, db.migrationsURL, db.migrationsPath, getenv("EVENT_PUBLISHER", "log"), logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	repromiseProcessed := buildRepromiseProcessedEvents(dbPool, logger)

	// readiness gates GET /readyz (ADR-0025); see drainUnderShutdown.
	readiness := &inboundhttp.Readiness{}

	inventory := wireInventoryClient(logger)
	classification := buildProductClassification(classificationCfg, dbPool, logger)

	catalogue, cptSchedule, capacity, closeCatalogue, err := wireCatalogue(ctx, logger)
	if err != nil {
		return err
	}
	defer closeCatalogue()

	promise := buildPromisePolicy(catalogue, cptSchedule, capacity, logger)

	clock := memory.SystemClock{}
	server := buildInboundServer(orders, publisher, clock, promise, inventory, classification.lookup, catalogue, orderMetrics, uow, dbPool, readiness, logger)

	// ADR 0036: in kafka mode the local classification copy's consumer
	// runs for the process lifetime; its stop runs after the HTTP drain
	// (serveAndShutdown returns first) and before the pool closes.
	defer classification.startConsumer(logger)()

	// ADR 0031 planned capacity: a nil value (consumer-group env unset)
	// leaves everything below exactly as before.
	if pc := buildPlannedCapacity(dbPool, logger); pc != nil {
		pc.attach(server, clock)
		defer pc.startConsumer(logger)()
	}

	repromiseConsumerCtx, cancelRepromiseConsumer := context.WithCancel(context.Background())
	defer cancelRepromiseConsumer()
	repromiseOrder := newRepromiseOrder(orders, publisher, clock, promise, repromiseProcessed, uow, logger, buildDemandProjection(logger))
	repromiseConsumer := newRepromiseConsumer(repromiseOrder, logger)

	httpServer := buildHTTPServer(server, serviceName, logger)

	return serveAndShutdown(logger, httpAddr, httpServer, repromiseConsumer, repromiseConsumerCtx, cancelRepromiseConsumer, readiness, relay)
}

// setupServiceTelemetry wires OTel before any adapter is built, so every
// subsequent adapter is built against the real providers rather than the
// no-op globals. Export is non-blocking: an unreachable Collector costs
// telemetry, never availability. It returns the resolved service name
// (the HTTP router's metrics label needs it) alongside the shutdown
// flush, which the caller defers.
func setupServiceTelemetry(ctx context.Context, logger *slog.Logger) (string, func(context.Context) error, error) {
	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint)
	shutdownTelemetry, err := telemetry.Setup(ctx, serviceName, getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion), otlpEndpoint)
	if err != nil {
		return "", nil, err
	}
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		"environment", getenv("ENVIRONMENT", telemetry.DefaultEnvironment),
		"otlp_endpoint", otlpEndpoint,
	)
	return serviceName, shutdownTelemetry, nil
}

// flushTelemetry runs the OTel shutdown flush under a bounded context; a
// failed flush is logged, never fatal.
func flushTelemetry(shutdownTelemetry func(context.Context) error, logger *slog.Logger) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
	}
}
