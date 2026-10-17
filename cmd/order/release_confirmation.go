package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// Environment variables of the release-confirmation loop (ADR 0043).
const (
	// releaseConfirmationModeEnv is the feature's on/off switch: "off"
	// (default) or "kafka". Off means nothing changes: no rows are opened, no
	// consumer runs, no timeout sweep runs.
	releaseConfirmationModeEnv = "RELEASE_CONFIRMATION_MODE"
	// releaseConfirmationGroupEnv is the STABLE Kafka consumer group of the
	// consumer of wes-work-planning's WorkUnitCreated. Required in kafka mode;
	// there is deliberately NO default, so a locally run process can never
	// join a live cluster's group.
	releaseConfirmationGroupEnv = "RELEASE_CONFIRMATION_CONSUMER_GROUP"
	// releaseConfirmationTimeoutEnv is how long a released line may stay
	// PENDING before the housekeeping sweep flags it UNCONFIRMED. 0 disables
	// the sweep. An operational parameter, not a business rule.
	releaseConfirmationTimeoutEnv = "RELEASE_CONFIRMATION_TIMEOUT"
	// releaseConfirmationRetentionEnv is how long a CONFIRMED row is kept
	// before the housekeeping sweep prunes it. 0 keeps them forever.
	releaseConfirmationRetentionEnv = "RELEASE_CONFIRMATION_RETENTION"
)

const (
	releaseConfirmationModeOff   = "off"
	releaseConfirmationModeKafka = "kafka"

	// defaultReleaseConfirmationTimeout covers wes-work-planning's outbox
	// relay, consumer retries and DLQ budget plus an ordinary restart or
	// rebalance; see ADR 0043 for the arithmetic.
	defaultReleaseConfirmationTimeout = 15 * time.Minute
)

// errReleaseConfirmationConfig marks a RELEASE_CONFIRMATION_* configuration
// the service refuses to boot with.
var errReleaseConfirmationConfig = errors.New("invalid release confirmation configuration")

// releaseConfirmationConfig is the validated RELEASE_CONFIRMATION_* settings.
type releaseConfirmationConfig struct {
	mode    string
	groupID string
	brokers []string
	timeout time.Duration
}

// enabled reports whether the loop is on.
func (c releaseConfirmationConfig) enabled() bool { return c.mode == releaseConfirmationModeKafka }

// releaseConfirmationEnv carries the raw values parseReleaseConfirmationConfig
// validates, so the rule is testable without touching the process environment.
type releaseConfirmationEnv struct {
	mode           string
	groupID        string
	brokers        string
	databaseURL    string
	eventPublisher string
}

// releaseConfirmationConfigFromEnv reads and validates the settings BEFORE any
// adapter is built, so a bad configuration fails boot first.
func releaseConfirmationConfigFromEnv(logger *slog.Logger) (releaseConfirmationConfig, error) {
	cfg, err := parseReleaseConfirmationConfig(releaseConfirmationEnv{
		mode:           getenv(releaseConfirmationModeEnv, releaseConfirmationModeOff),
		groupID:        os.Getenv(releaseConfirmationGroupEnv),
		brokers:        os.Getenv("KAFKA_BROKERS"),
		databaseURL:    os.Getenv("DATABASE_URL"),
		eventPublisher: getenv("EVENT_PUBLISHER", "log"),
	})
	if err != nil {
		return releaseConfirmationConfig{}, err
	}
	cfg.timeout = housekeepingDurationEnv(logger, releaseConfirmationTimeoutEnv, defaultReleaseConfirmationTimeout)
	return cfg, nil
}

// parseReleaseConfirmationConfig validates mode and its prerequisites: off
// needs nothing; kafka needs a consumer group, brokers, a database (the
// process-manager table) and EVENT_PUBLISHER=kafka (without it nothing is
// ever published to wes-work-planning, so no confirmation could arrive);
// anything else is rejected.
func parseReleaseConfirmationConfig(in releaseConfirmationEnv) (releaseConfirmationConfig, error) {
	m := strings.ToLower(strings.TrimSpace(in.mode))
	if m == "" || m == releaseConfirmationModeOff {
		return releaseConfirmationConfig{mode: releaseConfirmationModeOff}, nil
	}
	if m != releaseConfirmationModeKafka {
		return releaseConfirmationConfig{}, fmt.Errorf("%w: unknown %s %q (want kafka or off)",
			errReleaseConfirmationConfig, releaseConfirmationModeEnv, in.mode)
	}
	if strings.TrimSpace(in.groupID) == "" {
		return releaseConfirmationConfig{}, fmt.Errorf("%w: %s=kafka requires %s (a stable consumer group id; there is no default)",
			errReleaseConfirmationConfig, releaseConfirmationModeEnv, releaseConfirmationGroupEnv)
	}
	brokers := splitBrokers(in.brokers)
	if len(brokers) == 0 {
		return releaseConfirmationConfig{}, fmt.Errorf("%w: %s=kafka requires KAFKA_BROKERS",
			errReleaseConfirmationConfig, releaseConfirmationModeEnv)
	}
	if strings.TrimSpace(in.databaseURL) == "" {
		return releaseConfirmationConfig{}, fmt.Errorf("%w: %s=kafka requires DATABASE_URL (the release_confirmations process-manager table lives in Postgres)",
			errReleaseConfirmationConfig, releaseConfirmationModeEnv)
	}
	if !strings.EqualFold(strings.TrimSpace(in.eventPublisher), "kafka") {
		return releaseConfirmationConfig{}, fmt.Errorf("%w: %s=kafka requires EVENT_PUBLISHER=kafka (without it OrderAllocated never reaches wes-work-planning, so no WorkUnitCreated could confirm a release)",
			errReleaseConfirmationConfig, releaseConfirmationModeEnv)
	}
	return releaseConfirmationConfig{mode: releaseConfirmationModeKafka, groupID: strings.TrimSpace(in.groupID), brokers: brokers}, nil
}

// buildReleaseConfirmations returns the repo the release use cases open
// PENDING rows through, or nil when the loop is off. It returns a nil
// INTERFACE (never a typed nil) so the use cases' `!= nil` check is honest.
func buildReleaseConfirmations(cfg releaseConfirmationConfig, pool *pgxpool.Pool) ports.ReleaseConfirmations {
	if !cfg.enabled() || pool == nil {
		return nil
	}
	return postgres.NewReleaseConfirmationRepo(pool)
}

// newReleaseConfirmationMetrics builds the two counters, or nil (a logged
// warning, never a boot failure) if they cannot be registered.
func newReleaseConfirmationMetrics(logger *slog.Logger) ports.ReleaseConfirmationMetrics {
	m, err := telemetry.NewReleaseConfirmationMetrics()
	if err != nil {
		logger.Warn("release confirmation metrics unavailable; the loop runs un-instrumented", "error", err)
		return nil
	}
	return m
}

// buildFlagUnconfirmed returns the timeout sweep the housekeeping Sweeper
// runs each pass (ADR 0043), or nil when the loop is off, the timeout is 0 or
// there is no database. It is self-contained on purpose: the sweeper starts
// inside buildRepoAdapters, before the request-path publisher exists, so the
// hook builds its own transactional-outbox publisher over the same pool.
// That publisher only ever encodes OrderReleaseUnconfirmed (an analytics
// fact), which needs no order lookup, so its Orders repo stays nil.
func buildFlagUnconfirmed(pool *pgxpool.Pool, logger *slog.Logger) func(context.Context) (int, error) {
	cfg, err := releaseConfirmationConfigFromEnv(logger)
	if err != nil || !cfg.enabled() || cfg.timeout <= 0 || pool == nil {
		return nil
	}
	flag := &usecases.FlagUnconfirmedReleases{
		Confirmations: postgres.NewReleaseConfirmationRepo(pool),
		Events: postgres.NewOutboxPublisher(pool,
			kafkaadapter.NewPublisher(nil),
			&kafkaadapter.AnalyticsPublisher{NewID: uuid.NewString}),
		Clock:      memory.SystemClock{},
		UnitOfWork: postgres.NewUnitOfWork(pool),
		Metrics:    newReleaseConfirmationMetrics(logger),
		Logger:     logger,
		Timeout:    cfg.timeout,
	}
	return flag.Execute
}

// startReleaseConfirmationConsumer runs the Kafka consumer of WorkUnitCreated
// in kafka mode and returns a stop function that cancels it and waits
// (bounded) for the loop to return, so the in-flight message's offset commit
// is not abandoned. With the loop off it starts nothing and returns a no-op.
func startReleaseConfirmationConsumer(cfg releaseConfirmationConfig, pool *pgxpool.Pool, clock ports.Clock, logger *slog.Logger) (stop func()) {
	if !cfg.enabled() || pool == nil {
		return func() {}
	}
	confirm := &usecases.ConfirmRelease{
		Confirmations: postgres.NewReleaseConfirmationRepo(pool),
		Processed:     postgres.NewReleaseConfirmationProcessedEventsRepo(pool),
		Clock:         clock,
		UnitOfWork:    postgres.NewUnitOfWork(pool),
		Metrics:       newReleaseConfirmationMetrics(logger),
		Logger:        logger,
	}
	consumer := inboundkafka.NewReleaseConfirmationConsumer(cfg.brokers, cfg.groupID, confirm, logger)
	logger.Info("release confirmation consumer configured",
		"topic", inboundkafka.WorkPlanningEventsTopic, "group_id", cfg.groupID, "timeout", cfg.timeout)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Run(ctx); err != nil {
			logger.Error("release confirmation consumer stopped", "error", err)
		}
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			logger.Warn("release confirmation consumer did not stop before the shutdown deadline")
		}
		if err := consumer.Close(); err != nil {
			logger.Error("error closing release confirmation consumer", "error", err)
		}
	}
}
