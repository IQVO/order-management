package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	inbounda2a "github.com/claudioed/order-management/internal/adapters/inbound/a2a"
)

// newTestRouter builds the router exactly as run() does: the real executor
// (empty deps are fine -- no skill that touches a use case is invoked here)
// and the real Agent Card.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	card := inbounda2a.Card(inbounda2a.Config{AgentURL: testAgentURL, Version: "test"})
	return newRouter(inbounda2a.Handler(inbounda2a.NewExecutor(inbounda2a.Deps{})), card, "order-management-a2a")
}

func TestRouter_HealthzServesOK(t *testing.T) {
	h := newTestRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz: got %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"status":"ok"}` {
		t.Fatalf("GET /healthz body = %q, want {\"status\":\"ok\"}", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("GET /healthz Content-Type = %q, want application/json", ct)
	}
}

func TestRouter_ServesTheAgentCardAtTheWellKnownPath(t *testing.T) {
	h := newTestRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET agent card: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{`"name":"order-management"`, testAgentURL, `"order_status"`, `"promise_health"`} {
		if !strings.Contains(body, want) {
			t.Errorf("agent card lacks %s: %s", want, body)
		}
	}
}

// TestRouter_BothMountsReachTheJSONRPCHandler pins the fleet's
// <CTX>_A2A_ENDPOINT convention: the endpoint answers at "/" and at "/a2a".
func TestRouter_BothMountsReachTheJSONRPCHandler(t *testing.T) {
	h := newTestRouter(t)
	for _, path := range []string{"/", "/a2a"} {
		t.Run(path, func(t *testing.T) {
			// An unknown skill is answered in-band before any use case runs,
			// so a JSON-RPC result proves the request reached the SDK handler
			// at this mount point.
			body := `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"skill":"launch_rocket","arguments":{}}}]}}}`
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("A2A-Version", "1.0")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			raw, _ := io.ReadAll(rec.Body)
			if rec.Code == http.StatusNotFound || !strings.Contains(string(raw), `"jsonrpc"`) {
				t.Fatalf("POST %s: %d %q does not look like a JSON-RPC reply", path, rec.Code, raw)
			}
			if !strings.Contains(string(raw), "unknown-skill") {
				t.Fatalf("POST %s: reply %q lacks the in-band unknown-skill problem", path, raw)
			}
		})
	}
}

// ADR-0009 Tier 1: the A2A agent's HTTP surface emits
// http.server.request.duration and http.server.active_requests.
func TestNewRouter_EmitsStandardHTTPServerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	h := newTestRouter(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d", rec.Code)
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
			t.Errorf("%s not emitted by the A2A router; got %v", want, names)
		}
	}
}
