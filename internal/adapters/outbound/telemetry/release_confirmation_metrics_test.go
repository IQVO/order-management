package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/claudioed/order-management/internal/adapters/outbound/telemetry"
	"github.com/claudioed/order-management/internal/application/ports"
)

// TestReleaseConfirmationMetrics_NamesAndAttributes pins the two instrument
// names (they are an operational contract: dashboards and alerts query them)
// and the `outcome` attribute of the events counter.
func TestReleaseConfirmationMetrics_NamesAndAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := telemetry.NewReleaseConfirmationMetrics()
	if err != nil {
		t.Fatalf("NewReleaseConfirmationMetrics: %v", err)
	}
	ctx := context.Background()
	m.Handled(ctx, ports.ReleaseConfirmationConfirmed)
	m.Handled(ctx, ports.ReleaseConfirmationConfirmed)
	m.Handled(ctx, ports.ReleaseConfirmationIgnoredNoReference)
	m.LineUnconfirmed(ctx)

	sums := collectSums(ctx, t, reader)

	events, ok := sums["order.release_confirmation.events"]
	if !ok {
		t.Fatalf("order.release_confirmation.events not recorded; got %v", sums)
	}
	byOutcome := countByOutcome(events)
	if byOutcome["confirmed"] != 2 || byOutcome["ignored_no_reference"] != 1 || len(byOutcome) != 2 {
		t.Fatalf("events by outcome = %v, want confirmed=2 ignored_no_reference=1", byOutcome)
	}

	unconfirmed, ok := sums["order.release_confirmation.unconfirmed"]
	if !ok || len(unconfirmed.DataPoints) != 1 || unconfirmed.DataPoints[0].Value != 1 {
		t.Fatalf("order.release_confirmation.unconfirmed = %+v, want one data point of 1", unconfirmed)
	}
}

// collectSums reads every int64 sum instrument the reader holds, by name.
func collectSums(ctx context.Context, t *testing.T, reader sdkmetric.Reader) map[string]metricdata.Sum[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	sums := map[string]metricdata.Sum[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if sum, ok := md.Data.(metricdata.Sum[int64]); ok {
				sums[md.Name] = sum
			}
		}
	}
	return sums
}

// countByOutcome totals a counter's data points per `outcome` attribute value.
func countByOutcome(sum metricdata.Sum[int64]) map[string]int64 {
	byOutcome := map[string]int64{}
	for _, dp := range sum.DataPoints {
		v, _ := dp.Attributes.Value(attribute.Key("outcome"))
		byOutcome[v.AsString()] += dp.Value
	}
	return byOutcome
}
