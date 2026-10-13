package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCORS_PreflightAllowsIdempotencyKey pins ADR 0038: the operator MFE
// (order_mgmt_mfe, served from another origin) sends Idempotency-Key on
// POST /orders, so the browser preflight must be answered with that header
// in Access-Control-Allow-Headers. Without it the browser never sends the
// POST at all.
func TestCORS_PreflightAllowsIdempotencyKey(t *testing.T) {
	env := newTestEnv(t)

	req := httptest.NewRequest(http.MethodOptions, "/orders", nil)
	req.Header.Set("Origin", "http://localhost:5181")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type,idempotency-key")
	rec := httptest.NewRecorder()

	env.handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5181" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want the MFE origin", got)
	}
	allowed := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	if !strings.Contains(allowed, "idempotency-key") {
		t.Fatalf("Access-Control-Allow-Headers = %q, want it to include Idempotency-Key", allowed)
	}
}
