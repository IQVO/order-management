package http_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
)

// ADR-0003: an infrastructure failure of the inventory-storage call is the
// downstream's fault, never a 500 from this service. Outbound clients tag
// such failures with ports.ErrDownstreamUnavailable.
func unavailableErr() error {
	return fmt.Errorf("%w: %w", ports.ErrDownstreamUnavailable, errors.New("dial tcp: connection refused"))
}

func TestRetryAllocation_DownstreamUnavailableIs503(t *testing.T) {
	e := newTestEnv(t)
	e.inventory.reserveErr = ports.ErrInsufficientStock
	id := e.receiveOrder(t, false)

	e.inventory.reserveErr = unavailableErr()
	rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
	p := assertProblem(t, rec, http.StatusServiceUnavailable)
	if !strings.HasSuffix(p.Type, "/downstream-unavailable") {
		t.Fatalf("problem.type = %q, want .../downstream-unavailable", p.Type)
	}
}

func TestDeleteOrder_DownstreamUnavailableIs503(t *testing.T) {
	e := newTestEnv(t)
	// A held, ship-complete order allocates but is not released, so
	// cancelling it must revoke its reservations on inventory-storage.
	rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}],"releaseOnAllocation":false}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /orders status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	id := decodeOrder(t, rec).ID

	e.inventory.revokeErr = unavailableErr()
	del := e.do(t, http.MethodDelete, "/orders/"+id, "")
	p := assertProblem(t, del, http.StatusServiceUnavailable)
	if !strings.HasSuffix(p.Type, "/downstream-unavailable") {
		t.Fatalf("problem.type = %q, want .../downstream-unavailable", p.Type)
	}
}

// An open circuit breaker wraps BOTH sentinels; the accurate problem type
// is downstream-unavailable, not "not configured".
func TestRetryAllocation_CircuitOpenReportsUnavailableNotNotConfigured(t *testing.T) {
	e := newTestEnv(t)
	e.inventory.reserveErr = ports.ErrInsufficientStock
	id := e.receiveOrder(t, false)

	e.inventory.reserveErr = fmt.Errorf("%w: circuit open: %w", ports.ErrDownstreamUnavailable, ports.ErrDownstreamNotConfigured)
	rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
	p := assertProblem(t, rec, http.StatusServiceUnavailable)
	if !strings.HasSuffix(p.Type, "/downstream-unavailable") {
		t.Fatalf("problem.type = %q, want .../downstream-unavailable", p.Type)
	}
}
