package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	kafkaadapter "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// newRepromiseOrder builds the use case behind the RepromiseOrder
// consumer (ADR 0014 §5 / ADR 0018).
func newRepromiseOrder(orders ports.OrderRepo, publisher ports.EventPublisher, clock memory.SystemClock, promise order.PromisePolicy, repromiseProcessed ports.RepromiseProcessedEvents, uow ports.UnitOfWork, logger *slog.Logger, demand usecases.DemandProjectionPolicy) *usecases.RepromiseOrder {
	return &usecases.RepromiseOrder{
		Orders: orders, Promise: promise, Events: publisher, Clock: clock,
		Processed: repromiseProcessed, Logger: logger, DemandProjection: demand, UnitOfWork: uow,
	}
}

// newRepromiseConsumer builds the RepromiseOrder consumer when
// KAFKA_BROKERS is configured, or nil (with a warning) when it is not.
// It is wired independently of PATH_CATALOGUE_SOURCE/EVENT_PUBLISHER:
// it needs its own inbound Kafka consumer on fulfillment-execution's
// warehouse.fulfillment.events topic, gated on KAFKA_BROKERS alone,
// mirroring this repo's other KAFKA_BROKERS-gated conditional
// constructions. A STABLE, shared consumer group
// (kafka.RepromiseConsumerGroup) is used — this is a normal
// at-least-once "process and commit" consumer, not a full-replay
// local-cache one, so it must NOT use a per-process-unique group (see
// that package's doc comment).
func newRepromiseConsumer(repromiseOrder *usecases.RepromiseOrder, logger *slog.Logger) *inboundkafka.RepromiseConsumer {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		logger.Warn("KAFKA_BROKERS not configured; RepromiseOrder consumer will not run, OrderRepromised will never fire",
			"hint", "set KAFKA_BROKERS for a real deployment")
		return nil
	}
	consumer := inboundkafka.NewRepromiseConsumer(strings.Split(kafkaBrokers, ","), repromiseOrder, logger)
	logger.Info("repromise consumer configured",
		"topic", inboundkafka.FulfillmentEventsTopic, "group_id", inboundkafka.RepromiseConsumerGroup)
	return consumer
}

// buildKafkaPublishing wires the EVENT_PUBLISHER=kafka outbound side, on
// top of whichever repos buildRepoAdapters already selected. Without
// Postgres (in-memory dev run) events publish straight to the broker via
// a fan-out publisher — exactly as before this rollout, there being no
// transaction to bind an outbox row to. With Postgres, every event is
// enqueued once per (event x encoder) — one row for the integration
// topic, one for the analytics topic — in the SAME transaction as the
// aggregate write (via ports.UnitOfWork), so the store and both topics
// can never diverge from what actually happened; a background relay
// (started in run()) drains the rows onto Kafka afterwards.
func buildKafkaPublishing(orders ports.OrderRepo, pool *pgxpool.Pool, closeRepos func(), logger *slog.Logger) (ports.EventPublisher, ports.UnitOfWork, *postgres.OutboxRelay, func()) {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	// Integration encoder/writer: forwards OrderAllocated/
	// OrderPartiallyAllocated/OrderRepromised onto
	// warehouse.order-management.events. Left exactly as-is.
	writer := kafkaadapter.NewWriter(brokers...)
	integration := kafkaadapter.NewPublisher(writer)

	// Analytics encoder/writer: forwards the full report-input event set
	// onto the SEPARATE warehouse.order-management.analytics topic for
	// the data product (ADR-0006). It enriches each event with its
	// process path via the order repo. A single OLTP event stream fans
	// out to both.
	analytics := kafkaadapter.NewAnalyticsPublisher(brokers, orders, uuid.NewString)

	logger.Info("event publisher configured", "publisher", "kafka",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic, "brokers", brokers)

	if pool == nil {
		// Kafka configured but no Postgres (in-memory dev run): publish
		// straight to the broker, exactly as before this rollout — there
		// is no transaction to bind an outbox row to.
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
		return fanOut, nil, nil, closeAll
	}

	uow := postgres.NewUnitOfWork(pool)
	outboxPublisher := postgres.NewOutboxPublisher(pool, integration, analytics)
	sink := kafkaadapter.NewRelaySink(brokers...)
	relay := postgres.NewOutboxRelay(pool, sink, logger,
		postgres.WithInterval(durationEnv("OUTBOX_RELAY_INTERVAL", time.Second, logger)))
	logger.Info("event publisher configured (transactional outbox)",
		"integration_topic", kafkaadapter.Topic, "analytics_topic", kafkaadapter.AnalyticsTopic)

	closeAll := func() {
		if err := sink.Close(); err != nil {
			logger.Error("error closing relay sink", "error", err)
		}
		if err := analytics.Close(); err != nil {
			logger.Error("error closing analytics kafka writer", "error", err)
		}
		if err := writer.Close(); err != nil {
			logger.Error("error closing kafka writer", "error", err)
		}
		closeRepos()
	}

	return outboxPublisher, uow, relay, closeAll
}
