package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ADR-0009 Tier 1: the MCP server's HTTP surface emits
// http.server.request.duration and http.server.active_requests, and still
// serves the wrapped handler at "/" and sub-paths.
func TestNewRouter_EmitsStandardHTTPServerMetricsAndServesHandler(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	hit := 0
	router := newRouter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit++
		w.WriteHeader(http.StatusNoContent)
	}), "order-management-mcp")

	for _, path := range []string{"/", "/anything/else"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("POST %s = %d, want 204 from the wrapped handler", path, rec.Code)
		}
	}
	if hit != 2 {
		t.Fatalf("wrapped MCP handler hit %d times, want 2", hit)
	}

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
	for _, want := range []string{"http.server.request.duration", "http.server.active_requests"} {
		if !names[want] {
			t.Errorf("%s not emitted by the MCP router; got %v", want, names)
		}
	}
}
