//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
)

// TestRegisterOutboxLagGauge_ReportsAgeOfStuckEvent proves the gauge is a
// live signal, not a stub: an event's outbox row inserted directly (no
// relay running) accumulates real age, and the gauge reports it on
// collection — the exact "stuck event" scenario that was previously
// unobservable (ADR-0032 closes the flagged follow-up).
func TestRegisterOutboxLagGauge_ReportsAgeOfStuckEvent(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(ctx) })

	prevMeter := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(prevMeter) })

	reg, err := postgres.RegisterOutboxLagGauge(pool)
	if err != nil {
		t.Fatalf("register gauge: %v", err)
	}
	t.Cleanup(func() { _ = reg.Unregister() })

	// Before any row exists, the gauge must report 0 (fully drained).
	if got := collectOutboxLag(t, reader); got != 0 {
		t.Fatalf("expected lag 0 with an empty outbox, got %v", got)
	}

	// Insert a row directly, backdating created_at, simulating an event
	// the relay has failed to drain for 90 seconds (a "stuck" event: the
	// relay is not running in this test, so nothing will ever clear it).
	if _, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (topic, event_type, value, created_at, attempts, last_error)
		VALUES ('t', 'Stuck', '\x7b7d', now() - interval '90 seconds', 3, 'broker down')
	`); err != nil {
		t.Fatalf("insert stuck outbox row: %v", err)
	}

	got := collectOutboxLag(t, reader)
	if got < 89 || got > 120 {
		t.Fatalf("expected lag around 90s for the stuck event, got %v", got)
	}

	// Mark it published: the gauge must drop back to 0 (relay recovered).
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE event_type = 'Stuck'`); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if got := collectOutboxLag(t, reader); got != 0 {
		t.Fatalf("expected lag back to 0 once the stuck event is published, got %v", got)
	}
}

// collectOutboxLag runs one collection pass and extracts
// order.outbox.lag_seconds' single data point.
func collectOutboxLag(t *testing.T, reader *metric.ManualReader) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "order.outbox.lag_seconds" {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok || len(g.DataPoints) == 0 {
				t.Fatalf("order.outbox.lag_seconds has no data points (data=%T)", m.Data)
			}
			return g.DataPoints[0].Value
		}
	}
	t.Fatal("order.outbox.lag_seconds was never observed")
	return 0
}
