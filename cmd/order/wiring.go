package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/kafkacatalog"
	"github.com/claudioed/order-management/internal/adapters/outbound/kafkacptschedule"
	"github.com/claudioed/order-management/internal/adapters/outbound/kafkapathcapacity"
	"github.com/claudioed/order-management/internal/adapters/outbound/pathcapacity"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/bootretry"
)

// wireProcessPathCatalogue selects the process-path catalogue source
// (PATH_CATALOGUE_SOURCE=none|kafka) and, when kafka is selected, builds
// all three Kafka-fed local caches (the process-path catalogue itself,
// the CPT schedule cache, and the path capacity cache — see
// cmd/order/main.go's ADR-0014/0015 comments for why all three ride the
// SAME switch).
//
// Every one of the three NewConsumer calls is retried with exponential
// backoff, mirroring network-fulfillment PR #7's fix exactly: each
// constructor's newTargetOffsets dials the broker directly (no consumer
// group) to read partition offsets before the real Reader is created, and
// in this cluster every injected pod's FIRST outbound TCP dial (Postgres,
// Kafka alike) is reset ~10s after the app starts (Istio native sidecars
// run as an init container with restartPolicy=Always, so
// holdApplicationUntilProxyStarts is a no-op). A single attempt turns that
// known, transient condition into CrashLoopBackOff — this is the exact bug
// class network-fulfillment PR #7 fixed for its own Postgres dial, applied
// here to order-management's Kafka boot-time dials.
//
// The retry does NOT weaken the existing fail-closed behaviour: after the
// budget is exhausted this still returns an error and the caller still
// refuses to boot, exactly as before — it only stops treating a sidecar
// warm-up as a permanent failure.
func wireProcessPathCatalogue(ctx context.Context, catalogueSource, kafkaBrokers string, logger *slog.Logger) (
	ports.ProcessPathCatalogue, ports.CPTScheduleCache, ports.PathCapacity, func(), error,
) {
	noop := func() {}

	if catalogueSource != "kafka" {
		logger.Warn("process-path catalogue source not configured; ReceiveOrder will skip path validation, and path capacity will remain unknown (UnknownPathCapacity)",
			"hint", "set PATH_CATALOGUE_SOURCE=kafka for a real deployment")
		return nil, nil, pathcapacity.NewUnknown(), noop, nil
	}

	if kafkaBrokers == "" {
		return nil, nil, nil, noop, fmt.Errorf("PATH_CATALOGUE_SOURCE=kafka requires KAFKA_BROKERS to be set")
	}
	brokerList := strings.Split(kafkaBrokers, ",")

	// consumerCtx/cancel bound the three Run goroutines below; the caller
	// gets cancel back as the returned cleanup func. Started BEFORE
	// WaitReady is called for each — otherwise nothing would ever be
	// consuming messages while this process waits, guaranteeing a
	// deadlock until WaitReadyTimeout.
	consumerCtx, cancel := context.WithCancel(context.Background())
	cleanup := func() { cancel() }

	kafkaCatalogue, err := startCatalogueConsumer(ctx, consumerCtx, brokerList, logger)
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced process-path catalogue: %w", err)
	}

	cptScheduleConsumer, err := startCPTScheduleConsumer(ctx, consumerCtx, brokerList, logger)
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced CPT schedule cache: %w", err)
	}

	pathCapacityConsumer, err := startPathCapacityConsumer(ctx, consumerCtx, brokerList, logger)
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced path capacity cache: %w", err)
	}

	if err := waitReady(logger, "waiting for the process-path catalogue to replay its initial history before accepting traffic",
		kafkacatalog.WaitReadyTimeout, kafkaCatalogue.WaitReady); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("process-path catalogue did not become ready within %s: %w", kafkacatalog.WaitReadyTimeout, err)
	}

	if err := waitReady(logger, "waiting for the CPT schedule cache to replay its initial history before accepting traffic",
		kafkacptschedule.WaitReadyTimeout, cptScheduleConsumer.WaitReady); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("CPT schedule cache did not become ready within %s: %w", kafkacptschedule.WaitReadyTimeout, err)
	}

	if err := waitReady(logger, "waiting for the path capacity cache to replay its initial history before accepting traffic",
		kafkapathcapacity.WaitReadyTimeout, pathCapacityConsumer.WaitReady); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("path capacity cache did not become ready within %s: %w", kafkapathcapacity.WaitReadyTimeout, err)
	}

	return kafkaCatalogue, cptScheduleConsumer, pathCapacityConsumer, cleanup, nil
}

// startCatalogueConsumer retries the Kafka-sourced process-path
// catalogue's constructor with exponential backoff (see
// wireProcessPathCatalogue's doc comment for why), then starts its Run
// loop on consumerCtx so the caller's WaitReady calls are answered by a
// consumer that is already draining the topic.
func startCatalogueConsumer(ctx, consumerCtx context.Context, brokerList []string, logger *slog.Logger) (*kafkacatalog.Consumer, error) {
	var kafkaCatalogue *kafkacatalog.Consumer
	if err := bootretry.Retry(ctx, logger, "start process-path catalogue consumer", func() error {
		var err error
		kafkaCatalogue, err = kafkacatalog.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		return nil, err
	}
	logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)
	go func() {
		logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
		if err := kafkaCatalogue.Run(consumerCtx); err != nil {
			logger.Error("process-path catalogue consumer stopped", "error", err)
		}
	}()
	return kafkaCatalogue, nil
}

// startCPTScheduleConsumer retries the Kafka-sourced CPT schedule cache's
// constructor with exponential backoff, then starts its Run loop on
// consumerCtx — the same boot sequence as the catalogue consumer, on the
// cache ADR-0014 step A added.
func startCPTScheduleConsumer(ctx, consumerCtx context.Context, brokerList []string, logger *slog.Logger) (*kafkacptschedule.Consumer, error) {
	var cptScheduleConsumer *kafkacptschedule.Consumer
	if err := bootretry.Retry(ctx, logger, "start CPT schedule cache consumer", func() error {
		var err error
		cptScheduleConsumer, err = kafkacptschedule.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		return nil, err
	}
	logger.Info("CPT schedule cache source configured", "source", "kafka", "topic", kafkacptschedule.Topic)
	go func() {
		logger.Info("CPT schedule consumer running", "topic", kafkacptschedule.Topic)
		if err := cptScheduleConsumer.Run(consumerCtx); err != nil {
			logger.Error("CPT schedule consumer stopped", "error", err)
		}
	}()
	return cptScheduleConsumer, nil
}

// startPathCapacityConsumer retries the Kafka-sourced path capacity
// cache's constructor with exponential backoff, then starts its Run loop
// on consumerCtx — the same boot sequence as the catalogue consumer, on
// the cache ADR-0015 added.
func startPathCapacityConsumer(ctx, consumerCtx context.Context, brokerList []string, logger *slog.Logger) (*kafkapathcapacity.Consumer, error) {
	var pathCapacityConsumer *kafkapathcapacity.Consumer
	if err := bootretry.Retry(ctx, logger, "start path capacity cache consumer", func() error {
		var err error
		pathCapacityConsumer, err = kafkapathcapacity.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		return nil, err
	}
	logger.Info("path capacity cache source configured", "source", "kafka", "topic", kafkapathcapacity.Topic)
	go func() {
		logger.Info("path capacity consumer running", "topic", kafkapathcapacity.Topic)
		if err := pathCapacityConsumer.Run(consumerCtx); err != nil {
			logger.Error("path capacity consumer stopped", "error", err)
		}
	}()
	return pathCapacityConsumer, nil
}

// waitReady waits for one cache's initial history replay under its own
// WaitReadyTimeout budget, emitting the same waiting message the original
// inline blocks did.
func waitReady(logger *slog.Logger, waitingMsg string, timeout time.Duration, ready func(context.Context) error) error {
	logger.Info(waitingMsg)
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return ready(waitCtx)
}
