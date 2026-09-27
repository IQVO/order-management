// breaker.go wraps Client with a per-dependency circuit breaker
// (sony/gobreaker/v2, ADR-0025) AND jittered retry
// (cenkalti/backoff/v4) — the ONLY outbound client in this service that
// retries, because GetClassification is a pure GET/read, safe to retry
// unlike inventory-storage's mutating POST/DELETE (see
// inventorystorage/breaker.go's doc comment for that decision, and the
// ADR for the full reasoning). While the breaker is OPEN, this falls
// back to PermissiveLookup's existing fail-OPEN behaviour (Known=false,
// nil error) — the SAME fallback this client already had for a
// transport error or a 500, just now also reachable via the breaker
// short-circuiting a call it never attempts.
package productclassification

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="product-classification"}).
const DependencyName = "product-classification"

// maxRetryAttempts caps the jittered retry at 3 total attempts (1
// original + 2 retries) per the plan's "max 3 attempts" bound.
const maxRetryAttempts = 3

// retryInitialInterval/retryMaxInterval bound the exponential-backoff-
// with-jitter schedule between attempts — short, because this whole call
// is already bounded by DefaultTimeout end to end (see Reserve's
// resilience.CallTimeout use, mirrored here).
const (
	retryInitialInterval = 50 * time.Millisecond
	retryMaxInterval     = 500 * time.Millisecond
)

// BreakerClient wraps Client with retry-then-circuit-breaker for
// GetClassification. Unlike inventorystorage.BreakerClient (which has
// two mutating methods sharing one breaker), this dependency has exactly
// one call, so this type stays single-purpose.
type BreakerClient struct {
	breaker  *gobreaker.CircuitBreaker[ports.ProductClassification]
	inner    *Client
	fallback *PermissiveLookup
}

var _ ports.ProductClassificationLookup = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically
// telemetry.CircuitBreakerMetrics) — nil is a valid, documented no-op.
func NewBreakerClient(inner *Client, recorder resilience.StateRecorder) *BreakerClient {
	return newBreakerClient(inner, recorder, resilience.DefaultTimeout)
}

// NewBreakerClientWithTimeout is NewBreakerClient with an explicit
// breaker cooldown (gobreaker.Settings.Timeout) instead of
// resilience.DefaultTimeout, so a half-open-recovery test does not have
// to sleep for the full production cooldown in real time. Production
// code should always use NewBreakerClient; this exists for tests.
func NewBreakerClientWithTimeout(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return newBreakerClient(inner, recorder, cooldown)
}

func newBreakerClient(inner *Client, recorder resilience.StateRecorder, cooldown time.Duration) *BreakerClient {
	return &BreakerClient{
		breaker: gobreaker.NewCircuitBreaker[ports.ProductClassification](gobreaker.Settings{
			Name:          DependencyName,
			MaxRequests:   resilience.DefaultMaxRequests,
			Interval:      resilience.DefaultInterval,
			Timeout:       cooldown,
			ReadyToTrip:   resilience.ReadyToTrip,
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner:    inner,
		fallback: NewPermissiveLookup(),
	}
}

// GetClassification derives its timeout from the inbound request's
// remaining deadline (capped at DefaultTimeout), retries fetch up to
// maxRetryAttempts times with jittered backoff, and runs the whole
// retry loop through the breaker as ONE logical call — a retry storm
// against an already-degraded dependency still only ever counts as one
// success/failure toward the breaker's trip condition, not N. While the
// breaker is OPEN (or half-open and saturated), this falls back to
// PermissiveLookup — the SAME fail-open contract GetClassification
// already had for a transport error, just reached via a different path.
func (c *BreakerClient) GetClassification(ctx context.Context, sku string) (ports.ProductClassification, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, DefaultTimeout)
	defer cancel()

	result, err := c.breaker.Execute(func() (ports.ProductClassification, error) {
		return c.retryingFetch(callCtx, sku)
	})
	if isBreakerRejection(err) {
		return c.fallback.GetClassification(ctx, sku)
	}
	if err != nil {
		// fetch/retryingFetch never returns a "fail open" nil error for
		// a real problem (see client.go's fetch doc comment) — any
		// error reaching here is genuine and, per this port's existing
		// contract (see ports.ProductClassificationLookup's doc
		// comment), must still resolve to Known=false/nil rather than
		// propagate, so a retry-exhausted or otherwise-erroring lookup
		// never blocks order intake.
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	}
	return result, nil
}

// retryingFetch retries inner.fetch with jittered exponential backoff,
// bounded to maxRetryAttempts total attempts and to callCtx's own
// deadline (whichever is tighter). A 404 (Known=false, nil error) is a
// legitimate answer, not a failure, so it returns on the first attempt
// like a 200 does — only a transport error or an unexpected status is
// retried.
func (c *BreakerClient) retryingFetch(callCtx context.Context, sku string) (ports.ProductClassification, error) {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxRetryAttempts-1), callCtx)

	return backoff.RetryNotifyWithData(func() (ports.ProductClassification, error) {
		return c.inner.fetch(callCtx, sku)
	}, bounded, nil)
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call — see inventorystorage.isBreakerRejection's identical
// doc comment for the reasoning; kept as a separate unexported copy
// rather than a shared helper because the two packages otherwise share
// nothing importable without inventing a cross-adapter dependency
// (adapters never depend on each other — see internal/architecture's
// hexagonal fitness test).
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
