package main

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// buildInboundServer wires every use case into the inbound HTTP server's
// dependency struct, including the idempotency pool and readiness gate
// of the graceful-shutdown/idempotency ADRs.
func buildInboundServer(
	orders ports.OrderRepo,
	publisher ports.EventPublisher,
	clock memory.SystemClock,
	promise order.PromisePolicy,
	inventory ports.InventoryReservationClient,
	classification ports.ProductClassificationLookup,
	catalogue ports.ProcessPathCatalogue,
	orderMetrics *telemetry.OrderMetrics,
	uow ports.UnitOfWork,
	dbPool *pgxpool.Pool,
	readiness *inboundhttp.Readiness,
	logger *slog.Logger,
) *inboundhttp.Server {
	demand := buildDemandProjection(logger)
	return &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise, Catalogue: catalogue, Classification: classification, Metrics: orderMetrics, DemandProjection: demand, UnitOfWork: uow},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise, UnitOfWork: uow},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise, DemandProjection: demand, UnitOfWork: uow},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, DemandProjection: demand, UnitOfWork: uow},
		GetOrder:        &usecases.GetOrder{Orders: orders},
		// IdempotencyPool reuses the SAME pool buildRepoAdapters opened
		// against DATABASE_URL (nil in the in-memory dev/test
		// configuration) — see Server.IdempotencyPool's doc comment and
		// the idempotency-key-middleware ADR.
		IdempotencyPool: dbPool,
		// readiness backs GET /readyz (ADR-0025 §graceful shutdown):
		// flipped to not-ready as the FIRST step of shutdown, below,
		// before anything else stops.
		Readiness: readiness,
	}
}

// buildHTTPServer builds the HTTP server around the inbound router.
func buildHTTPServer(server *inboundhttp.Server, serviceName string, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              getenv("HTTP_ADDR", ":8080"),
		Handler:           inboundhttp.NewRouter(server, logger, serviceName),
		ReadHeaderTimeout: 5 * time.Second,
	}
}
