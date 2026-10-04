// breaker.go wraps Client with a per-dependency circuit breaker
// (sony/gobreaker/v2, ADR-0025): trips on the shared
// resilience.ReadyToTrip condition, and while OPEN falls back to
// PermissiveClient's existing fail-LOUD behaviour (ErrDownstreamNotConfigured)
// rather than inventing a new fallback path — reserving real stock must
// never appear to succeed against a tripped breaker any more than it may
// against the permissive no-op (see permissive.go's doc comment). This is
// deliberately the ONLY outbound client in this service that never
// retries a call: POST /reservations and DELETE /reservations/{id} are
// real mutations, already covered by Phase 1's idempotency-key
// middleware (creates) and RevokeReservation's own 404-is-success
// idempotence (deletes) — see the ADR's "retry-only-on-reads" decision
// for why blind retry here would be the wrong kind of safety net.
package inventorystorage

import (
	"context"
	"errors"
	"fmt"
	"time"

	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/resilience"
)

// DependencyName labels this breaker's Prometheus gauge series
// (circuit_breaker_state{dependency="inventory-storage"}).
const DependencyName = "inventory-storage"

// BreakerClient wraps Client with a circuit breaker guarding BOTH Reserve
// and RevokeReservation through the SAME breaker instance (one breaker
// per downstream dependency, not one per HTTP verb) — sharing a
// gobreaker.CircuitBreaker[any] because the two methods have different
// return types; see the two Execute call sites below for how each
// recovers its own concrete type.
type BreakerClient struct {
	breaker  *gobreaker.CircuitBreaker[any]
	inner    *Client
	fallback *PermissiveClient
}

var _ ports.InventoryReservationClient = (*BreakerClient)(nil)

// NewBreakerClient builds a BreakerClient wrapping inner. recorder is
// resilience.StateRecorder (typically
// telemetry.CircuitBreakerMetrics) — nil is a valid, documented no-op
// (see resilience.RecordStateChange), so a test that does not care about
// the metric never needs to construct one.
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
		breaker: gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
			Name:        DependencyName,
			MaxRequests: resilience.DefaultMaxRequests,
			Interval:    resilience.DefaultInterval,
			Timeout:     cooldown,
			ReadyToTrip: resilience.ReadyToTrip,
			// ErrInsufficientStock (inventory-storage's real 409) is a
			// working, correct answer from a healthy dependency, not a
			// failure — it must never itself contribute to tripping
			// this breaker, or a run of ordinary backorders would trip
			// it exactly like a run of real outages would.
			IsSuccessful: func(err error) bool {
				return err == nil || errors.Is(err, ports.ErrInsufficientStock)
			},
			// The caller giving up (request cancelled) is not this
			// dependency's fault; don't let it count as a failure
			// against the breaker either way.
			IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
			OnStateChange: resilience.RecordStateChange(DependencyName, recorder),
		}),
		inner:    inner,
		fallback: NewPermissiveClient(),
	}
}

// Reserve derives its timeout from the inbound request's remaining
// deadline (capped at DefaultTimeout — see resilience.CallTimeout), then
// routes the call through the breaker. While the breaker is OPEN (or
// half-open and already saturated with probes), it falls back to
// PermissiveClient.Reserve — the EXISTING fail-loud behaviour, unchanged
// — rather than fabricating a reservation.
func (c *BreakerClient) Reserve(ctx context.Context, req ports.ReservationRequest) (ports.ReservationResult, error) {
	callCtx, cancel := resilience.CallTimeout(ctx, DefaultTimeout)
	defer cancel()

	v, err := c.breaker.Execute(func() (any, error) {
		return c.inner.Reserve(callCtx, req)
	})
	if isBreakerRejection(err) {
		res, ferr := c.fallback.Reserve(ctx, req)
		return res, breakerOpen(ferr)
	}
	if err != nil {
		return ports.ReservationResult{}, err
	}
	return v.(ports.ReservationResult), nil
}

// RevokeReservation mirrors Reserve's breaker/fallback/timeout shape.
func (c *BreakerClient) RevokeReservation(ctx context.Context, reservationID string) error {
	callCtx, cancel := resilience.CallTimeout(ctx, DefaultTimeout)
	defer cancel()

	_, err := c.breaker.Execute(func() (any, error) {
		return nil, c.inner.RevokeReservation(callCtx, reservationID)
	})
	if isBreakerRejection(err) {
		return breakerOpen(c.fallback.RevokeReservation(ctx, reservationID))
	}
	return err
}

// breakerOpen tags the permissive fallback's refusal while the circuit
// is open as ports.ErrDownstreamUnavailable too, keeping
// ports.ErrDownstreamNotConfigured reachable via errors.Is. Both map to
// 503; the extra tag lets the HTTP adapter report the accurate problem
// type (downstream-unavailable) instead of claiming the downstream is
// "not configured" when it is merely failing.
func breakerOpen(fallbackErr error) error {
	if fallbackErr == nil {
		return nil
	}
	return fmt.Errorf("%w: circuit open: %w", ports.ErrDownstreamUnavailable, fallbackErr)
}

// isBreakerRejection reports whether err is gobreaker refusing to even
// attempt the call (open, or half-open and already at its probe limit)
// — the ONLY case that means "fall back to the permissive behaviour"; a
// real error FROM a call gobreaker did let through must propagate
// unchanged, exactly as it did before this breaker existed.
func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}
