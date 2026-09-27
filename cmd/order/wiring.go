package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

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

	var kafkaCatalogue *kafkacatalog.Consumer
	if err := bootretry.Retry(ctx, logger, "start process-path catalogue consumer", func() error {
		var err error
		kafkaCatalogue, err = kafkacatalog.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced process-path catalogue: %w", err)
	}
	logger.Info("process-path catalogue source configured", "source", "kafka", "topic", kafkacatalog.Topic)
	go func() {
		logger.Info("process-path catalogue consumer running", "topic", kafkacatalog.Topic)
		if err := kafkaCatalogue.Run(consumerCtx); err != nil {
			logger.Error("process-path catalogue consumer stopped", "error", err)
		}
	}()

	var cptScheduleConsumer *kafkacptschedule.Consumer
	if err := bootretry.Retry(ctx, logger, "start CPT schedule cache consumer", func() error {
		var err error
		cptScheduleConsumer, err = kafkacptschedule.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced CPT schedule cache: %w", err)
	}
	logger.Info("CPT schedule cache source configured", "source", "kafka", "topic", kafkacptschedule.Topic)
	go func() {
		logger.Info("CPT schedule consumer running", "topic", kafkacptschedule.Topic)
		if err := cptScheduleConsumer.Run(consumerCtx); err != nil {
			logger.Error("CPT schedule consumer stopped", "error", err)
		}
	}()

	var pathCapacityConsumer *kafkapathcapacity.Consumer
	if err := bootretry.Retry(ctx, logger, "start path capacity cache consumer", func() error {
		var err error
		pathCapacityConsumer, err = kafkapathcapacity.NewConsumer(context.Background(), brokerList, logger)
		return err
	}); err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("failed to start the Kafka-sourced path capacity cache: %w", err)
	}
	logger.Info("path capacity cache source configured", "source", "kafka", "topic", kafkapathcapacity.Topic)
	go func() {
		logger.Info("path capacity consumer running", "topic", kafkapathcapacity.Topic)
		if err := pathCapacityConsumer.Run(consumerCtx); err != nil {
			logger.Error("path capacity consumer stopped", "error", err)
		}
	}()

	logger.Info("waiting for the process-path catalogue to replay its initial history before accepting traffic")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), kafkacatalog.WaitReadyTimeout)
	err := kafkaCatalogue.WaitReady(waitCtx)
	waitCancel()
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("process-path catalogue did not become ready within %s: %w", kafkacatalog.WaitReadyTimeout, err)
	}

	logger.Info("waiting for the CPT schedule cache to replay its initial history before accepting traffic")
	scheduleWaitCtx, scheduleWaitCancel := context.WithTimeout(context.Background(), kafkacptschedule.WaitReadyTimeout)
	err = cptScheduleConsumer.WaitReady(scheduleWaitCtx)
	scheduleWaitCancel()
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("CPT schedule cache did not become ready within %s: %w", kafkacptschedule.WaitReadyTimeout, err)
	}

	logger.Info("waiting for the path capacity cache to replay its initial history before accepting traffic")
	capacityWaitCtx, capacityWaitCancel := context.WithTimeout(context.Background(), kafkapathcapacity.WaitReadyTimeout)
	err = pathCapacityConsumer.WaitReady(capacityWaitCtx)
	capacityWaitCancel()
	if err != nil {
		cleanup()
		return nil, nil, nil, noop, fmt.Errorf("path capacity cache did not become ready within %s: %w", kafkapathcapacity.WaitReadyTimeout, err)
	}

	return kafkaCatalogue, cptScheduleConsumer, pathCapacityConsumer, cleanup, nil
}
