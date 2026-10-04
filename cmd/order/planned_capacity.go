package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// Environment variables of the planned-capacity integration (ADR 0031).
const (
	// plannedCapacityGroupEnv is the Kafka consumer group id of the
	// planned-capacity consumer. It is the feature's on/off switch: unset
	// means the integration is entirely absent (no consumer, no route, no
	// order annotation, no extra query), so a deployment that never sets it
	// behaves exactly as before. There is deliberately NO default group, so
	// a locally run process can never join a live cluster's group by accident.
	plannedCapacityGroupEnv = "PLANNED_CAPACITY_CONSUMER_GROUP"
	// plannedCapacitySiteEnv names the planning `location` an order's promise
	// is matched against. Unset falls back to DEFAULT_SITE_ID, i.e. the site
	// the promise itself is computed for.
	plannedCapacitySiteEnv = "PLANNED_CAPACITY_SITE_ID"
)

// plannedCapacity bundles the wired planned-capacity read model.
type plannedCapacity struct {
	groupID   string
	siteID    string
	windows   ports.PlannedCapacityRepo
	processed ports.PlannedCapacityProcessedEvents
	uow       ports.UnitOfWork
}

// buildPlannedCapacity returns nil when PLANNED_CAPACITY_CONSUMER_GROUP is
// unset (the feature is off). Otherwise it selects the read-model adapters:
// Postgres when pool is non-nil (migration 0010 already ran in
// buildRepoAdapters), in-memory otherwise.
//
// The Postgres configuration ALWAYS gets its own UnitOfWork over pool — not
// the order use cases' one, which is nil unless EVENT_PUBLISHER=kafka — so
// the idempotency claim and the upsert commit atomically regardless of how
// events are published. (A nil *UnitOfWork must never be boxed into the
// interface: that would be a non-nil interface holding a nil pointer.)
func buildPlannedCapacity(pool *pgxpool.Pool, logger *slog.Logger) *plannedCapacity {
	groupID := os.Getenv(plannedCapacityGroupEnv)
	if groupID == "" {
		logger.Info("planned capacity integration disabled", "hint", "set "+plannedCapacityGroupEnv+" to consume warehouse-planning capacity plans")
		return nil
	}
	pc := &plannedCapacity{
		groupID: groupID,
		siteID:  getenv(plannedCapacitySiteEnv, getenv("DEFAULT_SITE_ID", DefaultSiteId)),
	}
	if pool == nil {
		logger.Info("database url not configured; using in-memory planned capacity read model")
		pc.windows = memory.NewPlannedCapacityRepo()
		pc.processed = memory.NewPlannedCapacityProcessedEventsRepo()
		return pc
	}
	pc.windows = postgres.NewPlannedCapacityRepo(pool)
	pc.processed = postgres.NewPlannedCapacityProcessedEventsRepo(pool)
	pc.uow = postgres.NewUnitOfWork(pool)
	return pc
}

// attach exposes the read model on the inbound HTTP server: the
// order-response annotation and GET /planned-capacity.
func (pc *plannedCapacity) attach(server *inboundhttp.Server, clock ports.Clock) {
	server.CapacityConstraints = &usecases.OrderCapacityConstraints{Windows: pc.windows, Clock: clock, SiteID: pc.siteID}
	server.PlannedCapacity = &usecases.GetPlannedCapacity{Windows: pc.windows, Clock: clock}
}

// startConsumer runs the Kafka consumer when KAFKA_BROKERS is configured and
// returns a stop function that cancels it and waits (bounded) for the loop
// to return, so the in-flight message's offset commit is not abandoned. It
// returns a no-op stop, with a warning, when there is no broker configured.
func (pc *plannedCapacity) startConsumer(logger *slog.Logger) (stop func()) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		logger.Warn("KAFKA_BROKERS not configured; planned capacity consumer will not run, the read model stays empty",
			"hint", "set KAFKA_BROKERS for a real deployment")
		return func() {}
	}
	apply := &usecases.ApplyPlannedCapacity{Windows: pc.windows, Processed: pc.processed, UnitOfWork: pc.uow, Logger: logger}
	consumer := inboundkafka.NewPlannedCapacityConsumer(strings.Split(brokers, ","), pc.groupID, apply, logger)
	logger.Info("planned capacity consumer configured",
		"topic", inboundkafka.PlanningEventsTopic, "group_id", pc.groupID, "site_id", pc.siteID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("planned capacity consumer stopped", "error", err)
		}
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("planned capacity consumer did not stop before the shutdown deadline")
		}
		if err := consumer.Close(); err != nil {
			logger.Error("error closing planned capacity consumer", "error", err)
		}
	}
}
