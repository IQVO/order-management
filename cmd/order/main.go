// Command order is the composition root: it wires env config into
// adapters, adapters into use cases, and use cases into the HTTP router.
// It is the only file that reads environment variables and the only file
// that knows both a port and its implementation.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/events"
	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	kafkaadapter "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/adapters/outbound/productclassification"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/bootretry"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
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

	serviceName := getenv("OTEL_SERVICE_NAME", inboundhttp.DefaultServiceName)
	otlpEndpoint := getenv("OTEL_EXPORTER_OTLP_ENDPOINT", telemetry.DefaultEndpoint)

	// Telemetry comes up before any adapter, so every subsequent adapter is
	// built against the real providers rather than the no-op globals.
	// Export is non-blocking: an unreachable Collector costs telemetry,
	// never availability.
	shutdownTelemetry, err := telemetry.Setup(ctx, serviceName, getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion), otlpEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown did not flush cleanly", "error", err)
		}
	}()
	logger.Info("telemetry configured",
		"service_name", serviceName,
		"service_version", getenv("SERVICE_VERSION", telemetry.DefaultServiceVersion),
		"environment", getenv("ENVIRONMENT", telemetry.DefaultEnvironment),
		"otlp_endpoint", otlpEndpoint,
	)

	orderMetrics, err := telemetry.NewOrderMetrics()
	if err != nil {
		return err
	}

	httpAddr := getenv("HTTP_ADDR", ":8080")
	databaseURL := os.Getenv("DATABASE_URL")
	migrationsPath := getenv("MIGRATIONS_PATH", "migrations")

	orders, publisher, dbPool, closeAdapters, err := buildRepoAdapters(ctx, databaseURL, migrationsPath, getenv("EVENT_PUBLISHER", "log"), logger)
	if err != nil {
		return err
	}
	defer closeAdapters()

	repromiseProcessed := buildRepromiseProcessedEvents(dbPool, logger)

	inventory := buildInventoryClient(getenv("INVENTORY_STORAGE_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), logger)

	// The product-classification lookup (ADR-0016 / ADR-0014 step B)
	// shares INVENTORY_STORAGE_BASE_URL with the inventory reservation
	// client above -- both call the SAME downstream service, so there is
	// deliberately no second base-URL knob for it, only its own
	// independent PRODUCT_CLASSIFICATION_MODE switch, mirroring
	// wes-work-planning's exact convention.
	classification := buildClassificationLookup(getenv("PRODUCT_CLASSIFICATION_MODE", "permissive"), os.Getenv("INVENTORY_STORAGE_BASE_URL"), logger)

	// The process-path catalogue's SOURCE is selectable, defaulting to
	// "none" (validation skipped -- a nil ports.ProcessPathCatalogue is
	// this fleet's established "not yet wired" convention, see
	// ReceiveOrder's doc comment). Set PATH_CATALOGUE_SOURCE=kafka for a
	// real deployment, mirroring wes-work-planning/fulfillment-execution/
	// workforce-management's identical PATH_CATALOGUE_SOURCE convention.
	// Unlike those three services, order-management never had a
	// file-based catalogue to begin with (see ADR-0013's scope decision),
	// so there is no "file" mode here -- only "none" (skip) and "kafka"
	// (real validation).
	//
	// ADR-0014 step A extends this SAME switch: the CPT schedule cache
	// (kafkacptschedule) is a SEPARATE consumer instance on the SAME
	// topic/broker as the catalogue, so it is gated identically -- it
	// only runs when the catalogue does, since both need KAFKA_BROKERS.
	//
	// ADR-0015 extends it a THIRD time: the path capacity cache
	// (kafkapathcapacity) is yet another separate consumer instance, on
	// a DIFFERENT topic (wes-work-planning's warehouse.work-planning.
	// events, not process-path-management's), but the SAME broker and
	// the SAME PATH_CATALOGUE_SOURCE switch -- there is no new env knob
	// for an operator to learn. When the switch is "none" (or unset),
	// ports.PathCapacity stays wired to UnknownPathCapacity, exactly
	// today's behaviour.
	// wireProcessPathCatalogue also retries every boot-time Kafka dial it
	// makes (each NewConsumer call's newTargetOffsets dials the broker
	// directly before the real reader exists) with exponential backoff,
	// mirroring network-fulfillment PR #7 — this cluster resets every
	// injected pod's first outbound dial ~10s after start (Istio native
	// sidecars; holdApplicationUntilProxyStarts is a no-op for them), and
	// a single attempt turns that transient condition into
	// CrashLoopBackOff. See wiring.go's doc comment for the full detail.
	catalogue, cptSchedule, capacity, closeCatalogue, err := wireProcessPathCatalogue(
		ctx, getenv("PATH_CATALOGUE_SOURCE", "none"), os.Getenv("KAFKA_BROKERS"), logger)
	if err != nil {
		return err
	}
	defer closeCatalogue()

	leadTime := order.NewLeadTimePolicy(
		durationEnv("PROMISE_DEFAULT_LEAD_TIME", order.DefaultLeadTime, logger),
		perPathLeadTimes(os.Getenv("PROMISE_PATH_LEAD_TIMES"), logger),
	)

	// PromisePolicy (ADR 0014) is the primary policy; leadTime is its
	// fallback, unchanged in its own logic. Capability/Schedule are nil
	// when the Kafka catalogue source is not configured, in which case
	// PromisePolicy always falls back to leadTime -- exactly this
	// service's pre-ADR-0014 behaviour.
	promise := order.PromisePolicy{
		Schedule:   cptSchedule,
		Capability: catalogue,
		Capacity:   capacity,
		Fallback:   leadTime,
		SiteId:     getenv("DEFAULT_SITE_ID", DefaultSiteId),
	}

	clock := memory.SystemClock{}
	server := &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise, Catalogue: catalogue, Classification: classification, Metrics: orderMetrics},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock},
		GetOrder:        &usecases.GetOrder{Orders: orders},
	}

	// RepromiseOrder consumer (ADR 0014 §5 / ADR 0018) — the final
	// piece of ADR 0014's rollout. It is wired independently of
	// PATH_CATALOGUE_SOURCE/EVENT_PUBLISHER: it needs its own inbound
	// Kafka consumer on fulfillment-execution's warehouse.fulfillment.events
	// topic, gated on KAFKA_BROKERS alone, mirroring this repo's other
	// KAFKA_BROKERS-gated conditional constructions. A STABLE, shared
	// consumer group (kafka.RepromiseConsumerGroup) is used — this is a
	// normal at-least-once "process and commit" consumer, not a
	// full-replay local-cache one, so it must NOT use a
	// per-process-unique group (see that package's doc comment).
	repromiseOrder := &usecases.RepromiseOrder{
		Orders: orders, Promise: promise, Events: publisher, Clock: clock,
		Processed: repromiseProcessed, Logger: logger,
	}
	repromiseConsumerCtx, cancelRepromiseConsumer := context.WithCancel(context.Background())
	defer cancelRepromiseConsumer()
	var repromiseConsumer *inboundkafka.RepromiseConsumer
	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); kafkaBrokers != "" {
		repromiseConsumer = inboundkafka.NewRepromiseConsumer(strings.Split(kafkaBrokers, ","), repromiseOrder, logger)
		logger.Info("repromise consumer configured",
			"topic", inboundkafka.FulfillmentEventsTopic, "group_id", inboundkafka.RepromiseConsumerGroup)
	} else {
		logger.Warn("KAFKA_BROKERS not configured; RepromiseOrder consumer will not run, OrderRepromised will never fire",
			"hint", "set KAFKA_BROKERS for a real deployment")
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	if repromiseConsumer != nil {
		defer func() {
			if err := repromiseConsumer.Close(); err != nil {
				logger.Error("error closing repromise consumer", "error", err)
			}
		}()
		go func() {
			logger.Info("repromise consumer running", "topic", inboundkafka.FulfillmentEventsTopic)
			if err := repromiseConsumer.Run(repromiseConsumerCtx); err != nil {
				errCh <- err
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	cancelRepromiseConsumer()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

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
func buildRepoAdapters(ctx context.Context, databaseURL, migrationsPath, eventPublisher string, logger *slog.Logger) (
	ports.OrderRepo, ports.EventPublisher, *pgxpool.Pool, func(), error,
) {
	noop := func() {}

	var (
		orders     ports.OrderRepo
		defaultPub ports.EventPublisher
		pool       *pgxpool.Pool
		closeRepos = noop
	)

	if databaseURL == "" {
		logger.Info("database url not configured; using in-memory adapters")
		orders = memory.NewOrderRepo()
		defaultPub = events.NewLogPublisher(logger)
	} else {
		if err := bootretry.Retry(ctx, logger, "run migrations", func() error {
			return postgres.RunMigrations(databaseURL, migrationsPath)
		}); err != nil {
			return nil, nil, nil, noop, err
		}
		var err error
		pool, err = postgres.NewPool(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, noop, err
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
			return nil, nil, nil, noop, err
		}
		orders = postgres.NewOrderRepo(pool)
		defaultPub = postgres.NewEventPublisher(pool)
		closeRepos = pool.Close
	}

	if !strings.EqualFold(eventPublisher, "kafka") {
		return orders, defaultPub, pool, closeRepos, nil
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	// Integration publisher: forwards OrderAllocated/OrderPartiallyAllocated/
	// OrderRepromised onto warehouse.order-management.events. Left
	// exactly as-is.
	writer := kafkaadapter.NewWriter(brokers...)
	integration := kafkaadapter.NewPublisher(writer)

	// Analytics publisher: forwards the full report-input event set onto the
	// SEPARATE warehouse.order-management.analytics topic for the data product
	// (ADR-0006). It enriches each event with its process path via the order
	// repo. A single OLTP event stream fans out to both publishers.
	analytics := kafkaadapter.NewAnalyticsPublisher(brokers, orders, uuid.NewString)

	logger.Info("event publisher configured", "publisher", "kafka",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", brokers)

	fanOut := kafkaadapter.NewFanOutPublisher(integration, analytics)

	closeAll := func() {
		if err := analytics.Close(); err != nil {
			logger.Error("error closing analytics kafka writer", "error", err)
		}
		if err := writer.Close(); err != nil {
			logger.Error("error closing kafka writer", "error", err)
		}
		closeRepos()
	}

	return orders, fanOut, pool, closeAll, nil
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

// buildInventoryClient selects the outbound InventoryReservationClient via
// INVENTORY_STORAGE_MODE (http|permissive), defaulting to "permissive" so
// unit tests and CI never reach the network. Permissive does NOT mean
// fail-open: it refuses to allocate rather than fabricating a reservation.
func buildInventoryClient(mode, baseURL string, logger *slog.Logger) ports.InventoryReservationClient {
	if !strings.EqualFold(mode, "http") {
		logger.Warn("inventory-storage client in permissive (no-op) mode; allocation will refuse to run",
			"hint", "set INVENTORY_STORAGE_MODE=http and INVENTORY_STORAGE_BASE_URL for a real deployment")
		return inventorystorage.NewPermissiveClient()
	}
	logger.Info("inventory-storage client configured", "mode", "http", "base_url", baseURL)
	return inventorystorage.NewClient(baseURL, nil)
}

// buildClassificationLookup selects the outbound
// ports.ProductClassificationLookup adapter via PRODUCT_CLASSIFICATION_MODE
// (http|permissive), defaulting to "permissive" so existing tests, CI and
// deployments that do not set the env var are unaffected -- mirroring
// wes-work-planning's own buildClassificationLookup convention exactly
// (see ADR-0016 / ADR-0014 step B). Unlike buildInventoryClient's
// permissive mode (which fails LOUD because reserving real stock must
// never appear to succeed against a no-op), this permissive mode fails
// OPEN: a classification lookup is a soft routing/enrichment input, not a
// mutation of real state.
func buildClassificationLookup(mode, inventoryStorageBaseURL string, logger *slog.Logger) ports.ProductClassificationLookup {
	if !strings.EqualFold(mode, "http") {
		logger.Warn("product classification lookup in permissive (fail-open) mode; path eligibility routing will see no derived product attributes",
			"hint", "set PRODUCT_CLASSIFICATION_MODE=http and INVENTORY_STORAGE_BASE_URL for a real deployment")
		return productclassification.NewPermissiveLookup()
	}
	logger.Info("product classification lookup configured", "mode", "http", "inventory_storage_base_url", inventoryStorageBaseURL)
	return productclassification.NewClient(inventoryStorageBaseURL, nil)
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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
