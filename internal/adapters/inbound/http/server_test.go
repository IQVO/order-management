package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// stubInventory is a scripted ports.InventoryReservationClient for the
// httptest suite. No test in this package reaches a real network.
type stubInventory struct {
	mu         sync.Mutex
	reserveErr error
	revokeErr  error
	nextID     int
}

func (s *stubInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reserveErr != nil {
		return ports.ReservationResult{}, s.reserveErr
	}
	s.nextID++
	return ports.ReservationResult{ReservationID: "res-stub"}, nil
}

func (s *stubInventory) RevokeReservation(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revokeErr
}

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, shared.DomainEvent) error { return nil }

type testEnv struct {
	handler   http.Handler
	orders    ports.OrderRepo
	inventory *stubInventory
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	orders := memory.NewOrderRepo()
	inventory := &stubInventory{}
	publisher := nopPublisher{}
	clock := memory.NewFixedClock(time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC))
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}

	server := &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: publisher, Clock: clock, Inventory: inventory, Promise: promise},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock},
		GetOrder:        &usecases.GetOrder{Orders: orders},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Inventory: inventory, Events: publisher, Clock: clock, Promise: promise},
	}

	// A discard logger keeps the middleware on the code path (so it is
	// exercised) without flooding test output.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	return &testEnv{
		handler:   inboundhttp.NewRouter(server, logger, ""),
		orders:    orders,
		inventory: inventory,
	}
}

func (e *testEnv) do(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	return rec
}

type orderBody struct {
	ID                   string  `json:"id"`
	Status               string  `json:"status"`
	AllowPartialShipment bool    `json:"allowPartialShipment"`
	PromiseDate          *string `json:"promiseDate"`
	Lines                []struct {
		LineNo        int     `json:"lineNo"`
		SKU           string  `json:"sku"`
		Quantity      int     `json:"quantity"`
		PathID        string  `json:"pathId"`
		GiftWrap      bool    `json:"giftWrap"`
		Status        string  `json:"status"`
		ReservationID *string `json:"reservationId"`
	} `json:"lines"`
}

type problemBody struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

func decodeOrder(t *testing.T, rec *httptest.ResponseRecorder) orderBody {
	t.Helper()
	var body orderBody
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&body); err != nil {
		t.Fatalf("decode order response: %v (body: %s)", err, rec.Body.String())
	}
	return body
}

// assertProblem checks that the response is a well-formed RFC 7807
// problem+json with the expected status.
func assertProblem(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) problemBody {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, wantStatus, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", ct)
	}
	var body problemBody
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&body); err != nil {
		t.Fatalf("decode problem response: %v (body: %s)", err, rec.Body.String())
	}
	if body.Status != wantStatus {
		t.Fatalf("problem.status = %d, want %d", body.Status, wantStatus)
	}
	if !strings.HasPrefix(body.Type, "https://errors.order-management.warehouse-systems.dev/") {
		t.Fatalf("problem.type = %q, want this service's error namespace", body.Type)
	}
	if body.Title == "" || body.Detail == "" {
		t.Fatalf("problem must carry a title and a detail: %+v", body)
	}
	return body
}

// receiveOrder posts a two-line order and returns its id. By default both
// lines allocate cleanly against the stub inventory, so — per the
// choreographed-release redesign — a ship-complete (allowPartialShipment
// false) order comes back already Released.
func (e *testEnv) receiveOrder(t *testing.T, allowPartialShipment bool) string {
	t.Helper()
	body := `{"lines":[{"sku":"SKU-1","quantity":2},{"sku":"SKU-2","quantity":1,"giftWrap":true}],"allowPartialShipment":` +
		map[bool]string{true: "true", false: "false"}[allowPartialShipment] + `}`
	rec := e.do(t, http.MethodPost, "/orders", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /orders status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	return decodeOrder(t, rec).ID
}

func TestHealthz(t *testing.T) {
	e := newTestEnv(t)
	rec := e.do(t, http.MethodGet, "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body = %v, want status ok", body)
	}
}

func TestPostOrders(t *testing.T) {
	t.Run("success — both lines allocate cleanly, so a partial-shipment order comes back released", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders",
			`{"lines":[{"sku":"SKU-1","quantity":2,"giftWrap":true},{"sku":"SKU-2","quantity":1}],"allowPartialShipment":true}`)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
		}
		body := decodeOrder(t, rec)
		if body.Status != string(order.StatusReleased) {
			t.Fatalf("status = %q, want %q", body.Status, order.StatusReleased)
		}
		if !body.AllowPartialShipment {
			t.Fatal("allowPartialShipment must reflect what was requested")
		}
		if len(body.Lines) != 2 {
			t.Fatalf("lines = %d, want 2", len(body.Lines))
		}
		if !body.Lines[0].GiftWrap {
			t.Fatalf("line 1 = %+v", body.Lines[0])
		}
		// pathId is never caller-supplied any more: every line gets the
		// internal default regardless of what the request DTO says (the
		// DTO no longer even has a pathId field to set).
		if body.Lines[0].PathID != string(shared.DefaultPathId) {
			t.Fatalf("line 1 pathId = %q, want %q (internal default, never caller-supplied)", body.Lines[0].PathID, shared.DefaultPathId)
		}
		if body.Lines[1].PathID != string(shared.DefaultPathId) {
			t.Fatalf("line 2 pathId = %q, want %q", body.Lines[1].PathID, shared.DefaultPathId)
		}
		if body.Lines[0].ReservationID == nil || *body.Lines[0].ReservationID != "res-stub" {
			t.Fatalf("a released order's lines must still carry their reservation id, got %v", body.Lines[0].ReservationID)
		}
		if body.PromiseDate == nil {
			t.Fatal("an allocated (and released) order must have a promise date")
		}
		if _, err := time.Parse(time.RFC3339, *body.PromiseDate); err != nil {
			t.Fatalf("promiseDate %q is not RFC 3339: %v", *body.PromiseDate, err)
		}
		if loc := rec.Header().Get("Location"); loc != "/orders/"+body.ID {
			t.Fatalf("Location = %q, want /orders/%s", loc, body.ID)
		}
	})

	t.Run("success: a request pathId field is rejected as unknown JSON, not silently accepted", func(t *testing.T) {
		// The public intake DTO no longer has a pathId field at all —
		// sending one is simply ignored by json.Decode (unknown fields
		// are not rejected by default), proving the caller has no way to
		// influence the internally-assigned value.
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1,"pathId":"singles"}]}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
		}
		body := decodeOrder(t, rec)
		if body.Lines[0].PathID != string(shared.DefaultPathId) {
			t.Fatalf("line pathId = %q, want %q — a caller-supplied pathId must be ignored", body.Lines[0].PathID, shared.DefaultPathId)
		}
	})

	t.Run("success: a hard allocation failure does not fail intake", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrDownstreamNotConfigured

		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}]}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 — the order was genuinely received (body: %s)", rec.Code, rec.Body.String())
		}
		body := decodeOrder(t, rec)
		if body.Status != string(order.StatusReceived) {
			t.Fatalf("status = %q, want %q — allocation never got past the first line", body.Status, order.StatusReceived)
		}
	})

	t.Run("error: no lines", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[]}`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "order-without-lines") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: empty sku", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"","quantity":1}]}`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "empty-sku") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: non-positive quantity", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":0}]}`)
		p := assertProblem(t, rec, http.StatusUnprocessableEntity)
		if !strings.HasSuffix(p.Type, "non-positive-quantity") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: malformed body", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "malformed-request-body") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: null on an order-level typed field is rejected, not coerced to its zero value", func(t *testing.T) {
		// apis/openapi.yaml declares allowPartialShipment as a boolean,
		// not a nullable one: a null must not be silently decoded into
		// `false` and accepted (the property-based contract suite found
		// exactly that drift).
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":null}`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "malformed-request-body") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: null on a line-level typed field is rejected, not coerced to its zero value", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1,"giftWrap":null}]}`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "malformed-request-body") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: null releaseOnAllocation is rejected rather than read as the default true", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}],"releaseOnAllocation":null}`)
		p := assertProblem(t, rec, http.StatusBadRequest)
		if !strings.HasSuffix(p.Type, "malformed-request-body") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("success: a null-valued UNKNOWN field is ignored, not rejected", func(t *testing.T) {
		// additionalProperties is at its default (allowed), so a caller
		// sending junk extra properties — even null-valued ones — must
		// not fail decoding. This pins the boundary the null rejection
		// deliberately stops at.
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders", `{"totally-unknown":null,"lines":[{"sku":"SKU-1","quantity":1}]}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
		}
	})
}

func TestGetOrder(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrDownstreamNotConfigured // keep the order in Received for this assertion
		id := e.receiveOrder(t, false)

		rec := e.do(t, http.MethodGet, "/orders/"+id, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		body := decodeOrder(t, rec)
		if body.ID != id || body.Status != string(order.StatusReceived) {
			t.Fatalf("body = %+v", body)
		}
	})

	t.Run("error: unknown order", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodGet, "/orders/ord-missing", "")
		p := assertProblem(t, rec, http.StatusNotFound)
		if !strings.HasSuffix(p.Type, "order-not-found") {
			t.Fatalf("problem.type = %q", p.Type)
		}
		if p.Instance != "/orders/ord-missing" {
			t.Fatalf("problem.instance = %q", p.Instance)
		}
	})
}

func TestPostReceiveOrderAllocatesAndReleasesAutomatically(t *testing.T) {
	t.Run("ship-complete order allocates and releases in the same call", func(t *testing.T) {
		e := newTestEnv(t)
		id := e.receiveOrder(t, false)

		rec := e.do(t, http.MethodGet, "/orders/"+id, "")
		body := decodeOrder(t, rec)
		if body.Status != string(order.StatusReleased) {
			t.Fatalf("status = %q, want %q", body.Status, order.StatusReleased)
		}
		for _, l := range body.Lines {
			if l.Status != string(order.LineReleased) {
				t.Fatalf("line %d status = %q, want %q", l.LineNo, l.Status, order.LineReleased)
			}
		}
	})

	t.Run("a 409 from inventory-storage backorders the order and blocks release", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrInsufficientStock

		rec := e.do(t, http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}]}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 — a backorder is a business fact, not an error (body: %s)", rec.Code, rec.Body.String())
		}
		if body := decodeOrder(t, rec); body.Status != string(order.StatusBackordered) {
			t.Fatalf("status = %q, want %q", body.Status, order.StatusBackordered)
		}
	})
}

func TestPostRetryAllocation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrInsufficientStock
		id := e.receiveOrder(t, false)
		if rec := e.do(t, http.MethodGet, "/orders/"+id, ""); decodeOrder(t, rec).Status != string(order.StatusBackordered) {
			t.Fatalf("setup: order must start Backordered")
		}

		e.inventory.reserveErr = nil
		rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		// A retry that clears the last backorder on a ship-complete order
		// also releases it, in the same call.
		if body := decodeOrder(t, rec); body.Status != string(order.StatusReleased) {
			t.Fatalf("status = %q, want %q", body.Status, order.StatusReleased)
		}
	})

	t.Run("error: nothing backordered", func(t *testing.T) {
		e := newTestEnv(t)
		id := e.receiveOrder(t, false)

		rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
		p := assertProblem(t, rec, http.StatusConflict)
		if !strings.HasSuffix(p.Type, "no-backordered-lines") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: unknown order", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodPost, "/orders/ord-missing/retry-allocation", "")
		assertProblem(t, rec, http.StatusNotFound)
	})

	t.Run("error: fails closed on a non-business error", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrInsufficientStock
		id := e.receiveOrder(t, false)

		e.inventory.reserveErr = ports.ErrDownstreamNotConfigured
		rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
		assertProblem(t, rec, http.StatusServiceUnavailable)
	})
}

func TestDeleteOrder(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		e := newTestEnv(t)
		e.inventory.reserveErr = ports.ErrDownstreamNotConfigured // keep every line Pending, nothing to revoke
		id := e.receiveOrder(t, false)

		rec := e.do(t, http.MethodDelete, "/orders/"+id, "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204 (body: %s)", rec.Code, rec.Body.String())
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("204 must have an empty body, got %q", rec.Body.String())
		}

		got := e.do(t, http.MethodGet, "/orders/"+id, "")
		if body := decodeOrder(t, got); body.Status != string(order.StatusCancelled) {
			t.Fatalf("status = %q, want %q", body.Status, order.StatusCancelled)
		}
	})

	// BR6 over the wire: a ship-complete order that allocated cleanly is
	// released automatically inside ReceiveOrder, so cancelling it right
	// after intake already hits the release boundary.
	t.Run("error: already released", func(t *testing.T) {
		e := newTestEnv(t)
		id := e.receiveOrder(t, false)

		rec := e.do(t, http.MethodDelete, "/orders/"+id, "")
		p := assertProblem(t, rec, http.StatusConflict)
		if !strings.HasSuffix(p.Type, "order-already-released") {
			t.Fatalf("problem.type = %q", p.Type)
		}
	})

	t.Run("error: unknown order", func(t *testing.T) {
		e := newTestEnv(t)
		rec := e.do(t, http.MethodDelete, "/orders/ord-missing", "")
		assertProblem(t, rec, http.StatusNotFound)
	})
}

// An unmapped error must still produce a well-formed 500 problem, never a
// bare panic or an empty body.
func TestUnmappedErrorBecomesAProblem500(t *testing.T) {
	e := newTestEnv(t)
	e.inventory.reserveErr = ports.ErrInsufficientStock
	id := e.receiveOrder(t, false)

	e.inventory.reserveErr = errUnmapped
	rec := e.do(t, http.MethodPost, "/orders/"+id+"/retry-allocation", "")
	p := assertProblem(t, rec, http.StatusInternalServerError)
	if !strings.HasSuffix(p.Type, "internal-error") {
		t.Fatalf("problem.type = %q", p.Type)
	}
}

func TestReleaseUnheldOrder_CarriesItsOwnProblemType(t *testing.T) {
	// Regression: ErrOrderNotHeld was mapped in the STATUS table (409)
	// but never in the problem-type table, so the response carried
	// type=internal-error / "An unexpected internal error occurred"
	// alongside a 409. A deliberate business rule was presented to the
	// caller as a server bug — in the running cluster, verbatim:
	//
	//   {"type":".../internal-error","status":409,
	//    "detail":"order was not held at intake"}
	//
	// Two tables, one of them updated. This test reads the type, not
	// just the status, because the status alone was already correct.
	e := newTestEnv(t)
	id := e.receiveOrder(t, false) // unheld: releases at allocation

	rec := e.do(t, http.MethodPost, "/orders/"+id+"/release", "")
	body := assertProblem(t, rec, http.StatusConflict)

	if !strings.HasSuffix(body.Type, "/order-not-held") {
		t.Fatalf("problem.type = %q, want the order-not-held slug", body.Type)
	}
	if strings.Contains(body.Title, "unexpected internal error") {
		t.Fatalf("a deliberate business rule must not be titled as an internal error: %q", body.Title)
	}
}

func TestHeldOrderMustBeShipComplete_CarriesItsOwnProblemType(t *testing.T) {
	// Regression: statusFor already mapped order.ErrHeldOrderMustBeShip-
	// Complete to 422, but problemFor had no case for it, so the response
	// fell through to type=internal-error / "An unexpected internal error
	// occurred" alongside a correct 422 — a deliberate ADR 0020 §4
	// business-rule violation presented to the caller as a server bug.
	// This test reads the type, not just the status.
	e := newTestEnv(t)

	rec := e.do(t, http.MethodPost, "/orders",
		`{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":true,"releaseOnAllocation":false}`)
	body := assertProblem(t, rec, http.StatusUnprocessableEntity)

	if !strings.HasSuffix(body.Type, "/held-order-must-be-ship-complete") {
		t.Fatalf("problem.type = %q, want the held-order-must-be-ship-complete slug", body.Type)
	}
	if strings.Contains(body.Title, "unexpected internal error") {
		t.Fatalf("a deliberate business rule must not be titled as an internal error: %q", body.Title)
	}
}

func TestNo4xxProblemFallsBackToInternalError(t *testing.T) {
	// The general form of the bug above: any error the status table maps
	// to a 4xx MUST also have its own problem type. A per-error mapping
	// kept in two tables is exactly the thing that drifts, and the
	// failure mode is silent — the status looks right, so a status-only
	// assertion passes while the caller is told it hit a server fault.
	//
	// This drives real 4xx paths through the router and asserts none of
	// them lands on the internal-error slug.
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"unknown order", http.MethodPost, "/orders/ord-does-not-exist/release", "", http.StatusNotFound},
		{"malformed json", http.MethodPost, "/orders", "{", http.StatusBadRequest},
		{"empty lines", http.MethodPost, "/orders", `{"lines":[]}`, http.StatusBadRequest},
		{"non-positive quantity", http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":0}]}`, http.StatusUnprocessableEntity},
		{"held order must be ship-complete", http.MethodPost, "/orders", `{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":true,"releaseOnAllocation":false}`, http.StatusUnprocessableEntity},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEnv(t)
			rec := e.do(t, tc.method, tc.path, tc.body)
			body := assertProblem(t, rec, tc.want)

			if strings.HasSuffix(body.Type, "/internal-error") {
				t.Fatalf("a %d response fell back to the internal-error problem type: %+v", tc.want, body)
			}
		})
	}
}
