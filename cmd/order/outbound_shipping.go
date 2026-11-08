package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/consumerdeath"
	"github.com/claudioed/order-management/internal/domain/order"
)

// Environment variables of the outbound-shipping consumer (ADR 0051).
const (
	// outboundShippingModeEnv is the consumer's on/off switch: "off"
	// (default) or "kafka". Off means nothing changes: no events are
	// consumed, no order ever leaves Released.
	outboundShippingModeEnv = "OUTBOUND_SHIPPING_CONSUMER_MODE"
	// outboundShippingGroupEnv is the STABLE Kafka consumer group of the
	// consumer of outbound-shipping's ShipmentConfirmed/PackageMissedLoad.
	// Required in kafka mode; there is deliberately NO default, so a
	// locally run process can never join a live cluster's group.
	outboundShippingGroupEnv = "OUTBOUND_SHIPPING_CONSUMER_GROUP"
)

const (
	outboundShippingModeOff   = "off"
	outboundShippingModeKafka = "kafka"
)

// errOutboundShippingConfig marks an OUTBOUND_SHIPPING_* configuration
// the service refuses to boot with.
var errOutboundShippingConfig = errors.New("invalid outbound shipping consumer configuration")

// outboundShippingConfig is the validated OUTBOUND_SHIPPING_* settings.
type outboundShippingConfig struct {
	mode    string
	groupID string
	brokers []string
}

// enabled reports whether the consumer is on.
func (c outboundShippingConfig) enabled() bool { return c.mode == outboundShippingModeKafka }

// outboundShippingEnv carries the raw values parseOutboundShippingConfig
// validates, so the rule is testable without touching the process
// environment.
type outboundShippingEnv struct {
	mode        string
	groupID     string
	brokers     string
	databaseURL string
}

// outboundShippingConfigFromEnv reads and validates the settings BEFORE
// any adapter is built, so a bad configuration fails boot first.
func outboundShippingConfigFromEnv() (outboundShippingConfig, error) {
	return parseOutboundShippingConfig(outboundShippingEnv{
		mode:        getenv(outboundShippingModeEnv, outboundShippingModeOff),
		groupID:     os.Getenv(outboundShippingGroupEnv),
		brokers:     os.Getenv("KAFKA_BROKERS"),
		databaseURL: os.Getenv("DATABASE_URL"),
	})
}

// parseOutboundShippingConfig validates mode and its prerequisites: off
// needs nothing; kafka needs a consumer group, brokers and a database
// (the shipment_processed_events idempotency gate lives in Postgres, and
// the consumer mutates the real order aggregate, not an in-memory copy);
// anything else is rejected.
func parseOutboundShippingConfig(in outboundShippingEnv) (outboundShippingConfig, error) {
	m := strings.ToLower(strings.TrimSpace(in.mode))
	if m == "" || m == outboundShippingModeOff {
		return outboundShippingConfig{mode: outboundShippingModeOff}, nil
	}
	if m != outboundShippingModeKafka {
		return outboundShippingConfig{}, fmt.Errorf("%w: unknown %s %q (want kafka or off)",
			errOutboundShippingConfig, outboundShippingModeEnv, in.mode)
	}
	if strings.TrimSpace(in.groupID) == "" {
		return outboundShippingConfig{}, fmt.Errorf("%w: %s=kafka requires %s (a stable consumer group id; there is no default)",
			errOutboundShippingConfig, outboundShippingModeEnv, outboundShippingGroupEnv)
	}
	brokers := splitBrokers(in.brokers)
	if len(brokers) == 0 {
		return outboundShippingConfig{}, fmt.Errorf("%w: %s=kafka requires KAFKA_BROKERS",
			errOutboundShippingConfig, outboundShippingModeEnv)
	}
	if strings.TrimSpace(in.databaseURL) == "" {
		return outboundShippingConfig{}, fmt.Errorf("%w: %s=kafka requires DATABASE_URL (the shipment_processed_events idempotency gate lives in Postgres)",
			errOutboundShippingConfig, outboundShippingModeEnv)
	}
	return outboundShippingConfig{mode: outboundShippingModeKafka, groupID: strings.TrimSpace(in.groupID), brokers: brokers}, nil
}

// startOutboundShippingConsumer runs the Kafka consumer of
// outbound-shipping's ShipmentConfirmed/PackageMissedLoad in kafka mode
// and returns a stop function that cancels it and waits (bounded) for
// the loop to return, so an in-flight offset commit is not abandoned.
// With the consumer off it starts nothing and returns a no-op (ADR
// 0051: the feature ships dark; enabling it is a deployment decision).
//
// The missed-load half shares the repromise machinery — the same
// newRepromiseOrder wiring the per-line consumer uses (same promise
// recompute, same repromise_processed_events idempotency gate, same
// demand projection) — so one CloudEvents id can never be evaluated
// twice by either path.
func startOutboundShippingConsumer(cfg outboundShippingConfig, pool *pgxpool.Pool, orders ports.OrderRepo, publisher ports.EventPublisher, uow ports.UnitOfWork, clock memory.SystemClock, promise order.PromisePolicy, repromiseProcessed ports.RepromiseProcessedEvents, demand usecases.DemandProjectionPolicy, logger *slog.Logger, deaths *consumerdeath.Deaths) (stop func()) {
	if !cfg.enabled() || pool == nil {
		return func() {}
	}
	confirm := &usecases.ConfirmShipment{
		Orders:     orders,
		Events:     publisher,
		Clock:      clock,
		Processed:  postgres.NewShipmentProcessedEventsRepo(pool),
		Logger:     logger,
		UnitOfWork: uow,
	}
	repromise := newRepromiseOrder(orders, publisher, clock, promise, repromiseProcessed, uow, logger, demand)
	consumer := inboundkafka.NewOutboundShippingConsumer(cfg.brokers, cfg.groupID, confirm, repromise, logger)
	logger.Info("outbound shipping consumer configured",
		"topic", inboundkafka.OutboundShippingEventsTopic, "group_id", cfg.groupID)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		deaths.Watch(ctx, "outbound shipping consumer", logger, consumer.Run)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("outbound shipping consumer did not stop before the shutdown deadline")
		}
		if err := consumer.Close(); err != nil {
			logger.Error("error closing outbound shipping consumer", "error", err)
		}
	}
}
