// Package resilience holds the small pieces of circuit-breaker/timeout
// policy shared by every outbound cross-context HTTP client in this
// service (ADR-0025): the shared trip condition, the Prometheus-gauge
// wiring contract, and the context-deadline-propagation helper. It is a
// top-level internal package (a sibling of internal/bootretry and
// internal/pgtx), not a domain/application/adapter layer itself, so the
// hexagonal fitness tests place no restriction on who may import it —
// only the outbound adapters (inventorystorage, productclassification,
// telemetry) do today.
//
// One gobreaker.CircuitBreaker instance is constructed PER downstream
// dependency (never one global breaker) — see
// internal/adapters/outbound/inventorystorage.NewBreakerClient and
// internal/adapters/outbound/productclassification.NewBreakerClient, the
// two call sites that build a breaker using this package's shared
// tuning. A slow or failing dependency's breaker tripping can never
// affect the other dependency's breaker or its own bulkheaded
// *http.Client.
package resilience

import (
	"time"

	gobreaker "github.com/sony/gobreaker/v2"
)

// Shared breaker tuning (ADR-0025), used by every per-dependency breaker
// in this service via each adapter package's own DefaultBreakerSettings
// helper:
//
//   - DefaultMaxRequests: only 1 probe request is let through per
//     half-open cycle, so a still-broken dependency is confirmed broken
//     again with minimal extra load.
//   - DefaultInterval: the closed-state window gobreaker uses to reset
//     its rolling Counts. Without this, a failure streak from an hour
//     ago could combine with a fresh one to trip the breaker on stale
//     history.
//   - DefaultTimeout: how long the breaker stays open before allowing a
//     half-open probe.
const (
	DefaultMaxRequests = 1
	DefaultInterval    = 30 * time.Second
	DefaultTimeout     = 30 * time.Second

	// minRequestVolumeForErrorRate guards ReadyToTrip's error-rate leg:
	// without a minimum sample size, one early failure (1 request, a
	// 100% error rate) would trip the breaker on its own — exactly what
	// the ConsecutiveFailures>=5 leg already exists to gate sensibly.
	// The error-rate leg only engages once there is enough traffic for
	// "over half failed" to mean something.
	minRequestVolumeForErrorRate = 10
)

// ReadyToTrip is the shared trip condition for every breaker in this
// service (plan section 2.2): open the breaker when either five
// consecutive requests have failed, or — given at least
// minRequestVolumeForErrorRate requests in the current closed-state
// window — more than half of them failed.
func ReadyToTrip(counts gobreaker.Counts) bool {
	if counts.ConsecutiveFailures >= 5 {
		return true
	}
	if counts.Requests < minRequestVolumeForErrorRate {
		return false
	}
	failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
	return failureRate > 0.5
}

// StateRecorder receives a breaker's state transitions so they can be
// exposed as the circuit_breaker.state gauge (re-published as Prometheus
// metric circuit_breaker_state{dependency="..."} by the OTel Collector's
// prometheus exporter — see this fleet's existing OTel/Prometheus
// pipeline, ADR-0009). The single implementation is
// internal/adapters/outbound/telemetry.CircuitBreakerMetrics, which
// reuses the SAME otel.Meter this service's other metrics already use
// rather than standing up a second registry.
//
// state follows gobreaker.State's own numbering verbatim (0=closed,
// 1=half-open, 2=open), so RecordStateChange needs no translation table.
type StateRecorder interface {
	SetState(dependency string, state int64)
}

// RecordStateChange adapts a StateRecorder into the shape
// gobreaker.Settings.OnStateChange expects for dependency. A nil
// recorder is a documented no-op (mirrors this repo's nil-Logger/nil-
// Metrics convention elsewhere), so a test that does not care about the
// metric never needs to construct one.
func RecordStateChange(dependency string, recorder StateRecorder) func(name string, from, to gobreaker.State) {
	return func(_ string, _, to gobreaker.State) {
		if recorder == nil {
			return
		}
		recorder.SetState(dependency, int64(to))
	}
}
