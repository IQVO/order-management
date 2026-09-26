// Package bootretry is the ONE shared implementation of this fleet's
// boot-time first-dial retry, used by every one of order-management's
// four composition roots (cmd/order, cmd/order-projector,
// cmd/order-reports, cmd/mcp).
//
// In this cluster every injected pod's FIRST outbound TCP dial
// (Postgres, Kafka) is reset ~10s after the app starts: Istio 1.30 runs
// native sidecars (istio-proxy is an init container with
// restartPolicy=Always), so holdApplicationUntilProxyStarts is a no-op
// for them. A binary that dials once and exits 1 on failure turns that
// known, transient condition into CrashLoopBackOff — confirmed live on
// order-management (129 restarts on one pod). Retry mirrors
// network-fulfillment PR #7's helper exactly (same budget, same
// last-error-on-exhaustion contract) so every binary here degrades the
// same way.
//
// This is deliberately a small shared package rather than one copy per
// cmd/*/main.go: four independent copies of the exact same helper is
// what this package exists to avoid.
package bootretry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// BootRetries and BootRetryDelay bound the startup retry budget.
//
// ~31s total (1+2+4+8+16), comfortably past the ~10s first-dial reset and
// still far inside the fleet's startupProbe convention (2s period x 30
// failures = 60s), so a genuinely unreachable dependency still fails the
// pod rather than hanging it.
const (
	BootRetries    = 5
	BootRetryDelay = time.Second
)

// Retry runs op with exponential backoff, returning the LAST error so a
// permanent failure still reports its real cause (e.g. "password
// authentication failed") rather than a generic timeout. This does NOT
// weaken any caller's fail-closed behaviour: after the budget is
// exhausted this still returns a non-nil error and the caller still
// refuses to boot — it only stops treating a sidecar warm-up as a
// permanent failure.
func Retry(ctx context.Context, logger *slog.Logger, what string, op func() error) error {
	return RetryWithDelay(ctx, logger, what, BootRetryDelay, op)
}

// RetryWithDelay is Retry with the base delay injected, so tests can
// exercise the give-up path without sleeping out the real ~31s budget.
func RetryWithDelay(ctx context.Context, logger *slog.Logger, what string, base time.Duration, op func() error) error {
	if logger == nil {
		logger = slog.Default()
	}
	delay := base
	var err error
	for attempt := 1; attempt <= BootRetries; attempt++ {
		if err = op(); err == nil {
			if attempt > 1 {
				logger.Info("succeeded after retry", "op", what, "attempt", attempt)
			}
			return nil
		}
		if attempt == BootRetries {
			break
		}
		logger.Warn("retrying", "op", what, "attempt", attempt, "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
	return fmt.Errorf("%s (after %d attempts): %w", what, BootRetries, err)
}
