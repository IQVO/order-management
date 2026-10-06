package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
)

// collectedMetricNames installs a manual-reader MeterProvider globally for
// the duration of the test and returns a func that reports the names of
// every metric collected from it. It must be called BEFORE the router under
// test is built: otelchimetric binds to otel.GetMeterProvider() at
// construction time.
func collectedMetricNames(t *testing.T) func() map[string]bool {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	return func() map[string]bool {
		t.Helper()
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("collect metrics: %v", err)
		}
		names := map[string]bool{}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				names[m.Name] = true
			}
		}
		return names
	}
}

// ADR-0009 Tier 1: every HTTP server in the fleet emits
// http.server.request.duration (and active_requests) via otelchimetric.
// The reports router previously registered neither otelchi nor
// otelchimetric.
func TestReportsRouter_EmitsStandardHTTPServerMetrics(t *testing.T) {
	collect := collectedMetricNames(t)
	router := inboundhttp.NewReportsRouter(&inboundhttp.ReportsHandlers{}, nil, "order-reports")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}

	names := collect()
	for _, want := range []string{"http.server.request.duration", "http.server.active_requests"} {
		if !names[want] {
			t.Errorf("%s not emitted by the reports router; got %v", want, names)
		}
	}
}
