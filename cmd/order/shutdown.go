package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/order-management/internal/adapters/inbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

// serveAndShutdown runs the HTTP server, the RepromiseOrder consumer and
// the outbox relay until SIGINT/SIGTERM (or a component failure), then
// drains all three under the ADR-0025 §graceful shutdown sequence. The
// repromiseConsumerDone bookkeeping lives here alongside its goroutine:
// the channel closes once the consumer's Run goroutine has returned —
// including having committed the offset for whatever message it was
// mid-handling when the cancel fires (see handleMessage's own
// commit-before-return shape) — so graceful shutdown can wait for a REAL
// stop, not just fire-and-forget the cancel.
func serveAndShutdown(
	logger *slog.Logger,
	httpAddr string,
	httpServer *http.Server,
	repromiseConsumer *inboundkafka.RepromiseConsumer,
	repromiseConsumerCtx context.Context,
	cancelRepromiseConsumer context.CancelFunc,
	readiness *inboundhttp.Readiness,
	relay *postgres.OutboxRelay,
) error {
	errCh := make(chan error, 3)
	startHTTPServer(logger, httpAddr, httpServer, errCh)
	repromiseConsumerDone := make(chan struct{})
	if repromiseConsumer != nil {
		defer func() {
			if err := repromiseConsumer.Close(); err != nil {
				logger.Error("error closing repromise consumer", "error", err)
			}
		}()
		startRepromiseConsumer(logger, repromiseConsumer, repromiseConsumerCtx, repromiseConsumerDone, errCh)
	} else {
		close(repromiseConsumerDone)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The outbox relay drains outbox_events onto Kafka alongside the HTTP
	// server, in the same process. It is only wired when both Postgres
	// and EVENT_PUBLISHER=kafka are configured (see buildRepoAdapters).
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(ctx)
	defer stopRelay()
	if relay != nil {
		startRelay(logger, relay, relayCtx, relayDone, errCh)
	} else {
		close(relayDone)
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	return drainUnderShutdown(logger, httpServer, readiness, drainDelayFromEnv(logger),
		stopRelay, relayDone,
		repromiseConsumer, repromiseConsumerDone, cancelRepromiseConsumer)
}

// startHTTPServer runs ListenAndServe in a goroutine; anything but a
// clean ErrServerClosed is reported on errCh.
func startHTTPServer(logger *slog.Logger, httpAddr string, httpServer *http.Server, errCh chan<- error) {
	go func() {
		logger.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
}

// startRepromiseConsumer runs the consumer loop in a goroutine and closes
// done once Run has returned (including its in-flight offset commit).
func startRepromiseConsumer(logger *slog.Logger, consumer *inboundkafka.RepromiseConsumer, ctx context.Context, done chan<- struct{}, errCh chan<- error) {
	go func() {
		defer close(done)
		logger.Info("repromise consumer running", "topic", inboundkafka.FulfillmentEventsTopic)
		if err := consumer.Run(ctx); err != nil {
			errCh <- err
		}
	}()
}

// startRelay runs the outbox relay in a goroutine and closes done once
// Run has returned; a cancellation is not an error.
func startRelay(logger *slog.Logger, relay *postgres.OutboxRelay, ctx context.Context, done chan<- struct{}, errCh chan<- error) {
	go func() {
		defer close(done)
		logger.Info("outbox relay running")
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
}

// DefaultShutdownDrainDelay is how long shutdown waits, after flipping
// /readyz to not-ready and before closing the listener, for Kubernetes'
// readinessProbe (periodSeconds 5) and the endpoint controller to
// observe the flip and stop routing NEW traffic to this pod (ADR-0025
// §8). SHUTDOWN_DRAIN_DELAY overrides it; "0" disables the wait (tests,
// local dev).
const DefaultShutdownDrainDelay = 5 * time.Second

// shutdownBudget bounds HTTP drain + consumer stop + relay final pass.
const shutdownBudget = 10 * time.Second

// drainDelayFromEnv reads SHUTDOWN_DRAIN_DELAY. Unlike durationEnv, 0 is
// a legal value (disable the delay); negative or unparsable values fall
// back to DefaultShutdownDrainDelay with a warning.
func drainDelayFromEnv(logger *slog.Logger) time.Duration {
	raw := os.Getenv("SHUTDOWN_DRAIN_DELAY")
	if raw == "" {
		return DefaultShutdownDrainDelay
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		logger.Warn("ignoring invalid duration env var", "key", "SHUTDOWN_DRAIN_DELAY", "value", raw, "fallback", DefaultShutdownDrainDelay.String())
		return DefaultShutdownDrainDelay
	}
	return d
}

// drainUnderShutdown performs the ADR-0025 §graceful shutdown sequence:
//
//  1. readiness.SetNotReady() — /readyz answers 503 so the readinessProbe
//     stops routing new traffic here;
//  2. wait drainDelay — time for the probe/endpoint controller to observe
//     the flip BEFORE the listener closes (otherwise requests still being
//     routed hit a closed port);
//  3. httpServer.Shutdown — drain in-flight requests (writer #1 of the
//     outbox);
//  4. stop and await the repromise consumer, including the commit of the
//     message it is mid-handling (writer #2 of the outbox);
//  5. stop and await the outbox relay LAST — it must make its final pass
//     only after every writer has stopped, otherwise an event committed
//     by the HTTP drain or the consumer's last message would be stranded
//     until the next pod boots (ADR-0022).
//
// Steps 3-5 share one shutdownBudget deadline; the drain delay is outside
// it (terminationGracePeriodSeconds must cover delay + budget).
func drainUnderShutdown(
	logger *slog.Logger,
	httpServer interface {
		Shutdown(ctx context.Context) error
	},
	readiness *inboundhttp.Readiness,
	drainDelay time.Duration,
	stopRelay context.CancelFunc,
	relayDone <-chan struct{},
	repromiseConsumer *inboundkafka.RepromiseConsumer,
	repromiseConsumerDone <-chan struct{},
	cancelRepromiseConsumer context.CancelFunc,
) error {
	readiness.SetNotReady()
	if drainDelay > 0 {
		logger.Info("shutdown: readiness flipped to not-ready; waiting for traffic to drain", "drain_delay", drainDelay.String())
		time.Sleep(drainDelay)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownBudget)
	defer cancel()
	err := httpServer.Shutdown(shutdownCtx)

	// Stop the repromise consumer's loop cleanly: cancel so no NEW
	// message is fetched, then wait (bounded) for any message already
	// being handled to finish — including its offset commit — before the
	// relay's final pass and the deferred repromiseConsumer.Close()/
	// closeAdapters() calls run.
	cancelRepromiseConsumer()
	if repromiseConsumer != nil {
		select {
		case <-repromiseConsumerDone:
		case <-shutdownCtx.Done():
			logger.Warn("repromise consumer did not stop before the shutdown deadline")
		}
	}

	// Every outbox writer has now stopped: let the relay finish its
	// in-flight pass so events committed by the last HTTP request or the
	// consumer's last message are not stranded until the next pod boots.
	stopRelay()
	select {
	case <-relayDone:
	case <-shutdownCtx.Done():
		logger.Warn("outbox relay did not stop before the shutdown deadline")
	}

	return err
}
