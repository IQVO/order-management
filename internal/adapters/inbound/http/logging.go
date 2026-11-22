package http

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// sanitizeForLog strips CR/LF from an attacker-controlled value (here,
// the raw request path, reached via chiRoutePattern's 404 fallback)
// before it is written to a log line. Without this, a crafted path
// segment containing an encoded newline could forge a fake log entry
// that appears to be a separate, legitimate line (CWE-117 log injection)
// once the log record reaches a downstream viewer/aggregator that
// doesn't preserve slog's JSON string escaping.
func sanitizeForLog(s string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(s)
}

// RequestLogger emits one structured line per request: method, route
// pattern, status, byte count and duration, plus the RequestID middleware's
// id so a client-reported failure can be found in the logs.
//
// The route PATTERN is logged rather than the raw path, so an order id
// never becomes a distinct log dimension (and never leaks into a
// cardinality explosion or an aggregation key).
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			route := chiRoutePattern(r)
			logger.InfoContext(r.Context(), "http request",
				"method", sanitizeForLog(r.Method),
				"route", route,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", sanitizeForLog(middleware.GetReqID(r.Context())),
			)
		})
	}
}

// chiRoutePattern returns the matched route pattern, falling back to the
// sanitized raw path for requests that matched no route (404s) -- an
// unmatched request never went through a route's own value objects, so
// the raw path is still attacker-controlled input at this point.
func chiRoutePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			return pattern
		}
	}
	return sanitizeForLog(r.URL.Path)
}
