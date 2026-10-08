package inventorystorage_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/application/ports"
)

// Decision 18, hop 1 (ADR 0037): POST /reservations carries the order line
// the reservation is for as the optional integer `lineNo`, so
// inventory-storage can store it and confirm a pick per line.

func reserveBody(t *testing.T, req ports.ReservationRequest) captured {
	t.Helper()
	var got captured
	srv := newServer(t, http.StatusCreated, `{"id":"res-1"}`, &got)
	if _, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), req); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	return got
}

func TestReserveSendsLineNoWhenKnown(t *testing.T) {
	got := reserveBody(t, ports.ReservationRequest{
		SKU: "SKU-1", Quantity: 3, DemandRef: "ord-7", LineNo: 2, Attempt: 1,
	})

	v, ok := got.body["lineNo"]
	if !ok {
		t.Fatalf("request body = %v, want a lineNo field", got.body)
	}
	// JSON numbers decode to float64; the wire value must be the integer 2.
	if v != float64(2) {
		t.Fatalf("lineNo = %v (%T), want the integer 2", v, v)
	}
	// The pre-existing fields are untouched.
	if got.body["sku"] != "SKU-1" || got.body["quantity"] != float64(3) || got.body["demandRef"] != "ord-7" {
		t.Fatalf("request body = %v, want {sku, quantity, demandRef, lineNo}", got.body)
	}
}

func TestReserveOmitsLineNoWhenUnknown(t *testing.T) {
	for _, lineNo := range []int{0, -1} {
		got := reserveBody(t, ports.ReservationRequest{
			SKU: "SKU-1", Quantity: 3, DemandRef: "ord-7", LineNo: lineNo, Attempt: 1,
		})
		if _, ok := got.body["lineNo"]; ok {
			t.Fatalf("LineNo=%d: request body = %v, want NO lineNo field (absent means unknown)", lineNo, got.body)
		}
		if got.body["sku"] != "SKU-1" || got.body["demandRef"] != "ord-7" {
			t.Fatalf("LineNo=%d: request body = %v lost its required fields", lineNo, got.body)
		}
	}
}

// The Idempotency-Key is a replay contract with inventory-storage
// (ADR 0028): putting lineNo in the body must not move the key by a byte.
func TestReserveIdempotencyKeyIsByteIdenticalWithLineNoInBody(t *testing.T) {
	cases := []struct {
		req  ports.ReservationRequest
		want string
	}{
		{ports.ReservationRequest{SKU: "SKU-1", Quantity: 3, DemandRef: "ord-7", LineNo: 1, Attempt: 1}, "res-ord-7-line-1-att-1"},
		{ports.ReservationRequest{SKU: "SKU-9", Quantity: 1, DemandRef: "ord-7", LineNo: 12, Attempt: 4}, "res-ord-7-line-12-att-4"},
		{ports.ReservationRequest{SKU: "SKU-1", Quantity: 3, DemandRef: "ord-7", LineNo: 0, Attempt: 2}, "res-ord-7-line-0-att-2"},
	}
	for _, tc := range cases {
		if got := reserveBody(t, tc.req).idempotencyKey; got != tc.want {
			t.Fatalf("Idempotency-Key = %q, want %q", got, tc.want)
		}
	}
}
