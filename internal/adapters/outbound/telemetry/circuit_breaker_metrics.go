package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// circuitBreakerGaugeName follows this fleet's OTel-metric-name-becomes-
// Prometheus-metric-name convention (see ADR-0009): the OTel Collector's
// prometheus exporter turns the dot-separated instrument name into the
// underscore-separated series `circuit_breaker_state` the plan
// (section 2.2) and the new ADR ask for, labelled
// dependency="inventory-storage" (the product-classification breaker was
// removed with its HTTP client by ADR 0036).
const circuitBreakerGaugeName = "circuit_breaker.state"

// dependencyKey is the one attribute this gauge carries: which
// downstream dependency's breaker changed state.
var dependencyKey = attribute.Key("dependency")

// CircuitBreakerMetrics implements resilience.StateRecorder against the
// SAME global MeterProvider OrderMetrics already uses (installed by
// telemetry.Setup) — this fleet has exactly one Prometheus-facing
// registry per service (the OTel Collector's own re-exporter), and every
// instrument in this package reuses it rather than standing up a second,
// parallel prometheus.Registry.
type CircuitBreakerMetrics struct {
	gauge metric.Int64Gauge
}

// NewCircuitBreakerMetrics registers the circuit_breaker.state gauge.
// Mirrors NewOrderMetrics' error contract: this only fails on an invalid
// instrument name (a programming error), so a caller that would rather
// run without the metric than not at all can pass a nil
// resilience.StateRecorder instead of propagating this error fatally.
func NewCircuitBreakerMetrics() (*CircuitBreakerMetrics, error) {
	gauge, err := otel.Meter(meterName).Int64Gauge(
		circuitBreakerGaugeName,
		metric.WithDescription("Circuit breaker state per downstream dependency (0=closed, 1=half-open, 2=open)."),
		metric.WithUnit("{state}"),
	)
	if err != nil {
		return nil, err
	}
	return &CircuitBreakerMetrics{gauge: gauge}, nil
}

// SetState implements resilience.StateRecorder. state is
// gobreaker.State's own int value (0/1/2), recorded verbatim — see
// resilience.RecordStateChange's doc comment for why no translation is
// needed here.
func (m *CircuitBreakerMetrics) SetState(dependency string, state int64) {
	m.gauge.Record(context.Background(), state, metric.WithAttributes(dependencyKey.String(dependency)))
}
