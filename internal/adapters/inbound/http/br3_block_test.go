package http_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
)

// BR3 (ship-complete) is a HOLD, not an error (ADR 0003: "no line proceeds to
// release until RetryAllocation clears it"; ADR 0005: ship-complete-still-
// backordered-releases-nothing is an explicit success case; ADR 0020: the
// caller of a hold reads the outcome from the order, not from a 4xx).
//
// So no HTTP path returns a `ship-complete-blocked` problem: when BR3 blocks a
// release the call succeeds (201 on intake, 200 on retry / release-held) and the
// body is the order, still Backordered, with nothing released. These tests pin
// that contract at every endpoint that reaches the release leg, so the
// previously-mapped-but-unreachable 409 problem type cannot silently come back
// half-wired.

func assertBackorderedNotAProblem(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d — BR3 blocking a release is a hold, not an error (body: %s)", rec.Code, want, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); strings.Contains(ct, "problem+json") {
		t.Fatalf("Content-Type = %q: a BR3 hold must not be reported as a problem", ct)
	}
	body := decodeOrder(t, rec)
	if body.Status != string(order.StatusBackordered) {
		t.Fatalf("status = %q, want %q", body.Status, order.StatusBackordered)
	}
	for _, l := range body.Lines {
		if l.Status == string(order.LineReleased) {
			t.Fatalf("line %d is Released, but BR3 must release nothing: %+v", l.LineNo, body)
		}
	}
}

func TestBR3Block_PostOrders_Is201WithBackorderedOrder(t *testing.T) {
	e := newTestEnv(t)
	e.inventory.reserveErr = ports.ErrInsufficientStock

	rec := e.do(t, http.MethodPost, "/orders",
		`{"lines":[{"sku":"SKU-1","quantity":1},{"sku":"SKU-2","quantity":1}],"allowPartialShipment":false}`)
	assertBackorderedNotAProblem(t, rec, http.StatusCreated)
}

func TestBR3Block_PostRetryAllocation_StillShort_Is200WithBackorderedOrder(t *testing.T) {
	e := newTestEnv(t)
	e.inventory.reserveErr = ports.ErrInsufficientStock
	id := e.receiveOrder(t, false)

	// Stock is still short on the retry: the order stays Backordered, the call succeeds.
	rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
	assertBackorderedNotAProblem(t, rec, http.StatusOK)
}

func TestBR3Block_PostRelease_HeldOrderThatLostItsReservation_Is200WithBackorderedOrder(t *testing.T) {
	e := newTestEnv(t)

	// A held (releaseOnAllocation=false) ship-complete order allocates and stops.
	rec := e.do(t, http.MethodPost, "/orders",
		`{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":false,"releaseOnAllocation":false}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("setup: POST /orders status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	held := decodeOrder(t, rec)
	if held.Status != string(order.StatusAllocated) {
		t.Fatalf("setup: held order status = %q, want %q", held.Status, order.StatusAllocated)
	}

	// The reservation lapses before the release is requested: reconfirm gets a 409,
	// the line goes back to Backordered and BR3 blocks the release.
	e.inventory.reserveErr = ports.ErrInsufficientStock
	rec = e.do(t, http.MethodPost, "/orders/"+held.ID+"/release", "")
	assertBackorderedNotAProblem(t, rec, http.StatusOK)
}
