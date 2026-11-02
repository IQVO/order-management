package http_test

import (
	"net/http"
	"testing"
)

// TestRetiredRoutesAreGone pins the REST API guideline cutover (ADR 0048):
// the verb-style command routes were REMOVED, not aliased. Each old route
// answers 404 or 405 whatever the method, and the noun routes that replaced
// them are alive.
func TestRetiredRoutesAreGone(t *testing.T) {
	e := newTestEnv(t)
	id := e.receiveOrder(t, false)

	retired := []struct {
		name   string
		method string
		path   string
	}{
		{"release POST", http.MethodPost, "/orders/" + id + "/release"},
		{"release PUT", http.MethodPut, "/orders/" + id + "/release"},
		{"release GET", http.MethodGet, "/orders/" + id + "/release"},
		{"retry-allocation POST", http.MethodPost, "/orders/" + id + "/retry-allocation"},
		{"retry-allocation PUT", http.MethodPut, "/orders/" + id + "/retry-allocation"},
		{"retry-allocation GET", http.MethodGet, "/orders/" + id + "/retry-allocation"},
	}
	for _, tt := range retired {
		t.Run(tt.name, func(t *testing.T) {
			rec := e.do(t, tt.method, tt.path, "")
			if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d, want 404 or 405 (body: %s)", tt.method, tt.path, rec.Code, rec.Body.String())
			}
		})
	}

	// The replacements exist: only POST is routed, so a GET is 405.
	for _, path := range []string{"/orders/" + id + "/releases", "/orders/" + id + "/allocation-retries"} {
		if rec := e.do(t, http.MethodGet, path, ""); rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s = %d, want 405 (the route exists and is POST-only)", path, rec.Code)
		}
	}
}
