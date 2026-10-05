package inventorystorage_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/application/ports"
)

// ADR-0003: every infrastructure failure of an inventory-storage call
// (transport error, timeout, unexpected status, undecodable body, open
// circuit) carries ports.ErrDownstreamUnavailable so the HTTP adapter can
// answer 503. A 409 stays the insufficient-stock business fact and must
// NEVER be tagged unavailable.

var unavailableReq = ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"}

func TestReserveFailuresAreTaggedDownstreamUnavailable(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		responseBody string
		wantStatusEr bool // also wraps ErrUnexpectedStatus
	}{
		{"unexpected status", http.StatusInternalServerError, "", true},
		{"2xx without a reservation id", http.StatusCreated, `{}`, true},
		{"undecodable 2xx body", http.StatusCreated, `not json`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got captured
			srv := newServer(t, tt.status, tt.responseBody, &got)
			_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), unavailableReq)
			if !errors.Is(err, ports.ErrDownstreamUnavailable) {
				t.Fatalf("err = %v, want ErrDownstreamUnavailable", err)
			}
			if tt.wantStatusEr && !errors.Is(err, inventorystorage.ErrUnexpectedStatus) {
				t.Fatalf("err = %v, want it to also wrap ErrUnexpectedStatus", err)
			}
		})
	}
}

func TestRevokeUnexpectedStatusIsTaggedDownstreamUnavailable(t *testing.T) {
	var got captured
	srv := newServer(t, http.StatusBadGateway, "", &got)
	err := inventorystorage.NewClient(srv.URL, nil).RevokeReservation(context.Background(), "res-1")
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, inventorystorage.ErrUnexpectedStatus) {
		t.Fatalf("err = %v, want both ErrDownstreamUnavailable and ErrUnexpectedStatus", err)
	}
}

func TestTransportErrorIsTaggedAndKeepsItsCauseReachable(t *testing.T) {
	boom := errors.New("connection refused")
	client := inventorystorage.NewClient("http://example.invalid", errDoer{err: boom})

	_, err := client.Reserve(context.Background(), unavailableReq)
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, boom) {
		t.Fatalf("Reserve err = %v, want ErrDownstreamUnavailable wrapping %v", err, boom)
	}
	err = client.RevokeReservation(context.Background(), "res-1")
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, boom) {
		t.Fatalf("RevokeReservation err = %v, want ErrDownstreamUnavailable wrapping %v", err, boom)
	}
}

func TestInsufficientStock409IsNeverTaggedUnavailable(t *testing.T) {
	var got captured
	srv := newServer(t, http.StatusConflict, `{}`, &got)
	_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), unavailableReq)
	if !errors.Is(err, ports.ErrInsufficientStock) || errors.Is(err, ports.ErrDownstreamUnavailable) {
		t.Fatalf("err = %v, want ErrInsufficientStock only", err)
	}
}

func TestOpenCircuitIsUnavailableAndStillNotConfigured(t *testing.T) {
	boom := errors.New("connection refused")
	client := inventorystorage.NewBreakerClient(
		inventorystorage.NewClient("http://example.invalid", errDoer{err: boom}), &recordingRecorder{})
	for i := 0; i < 5; i++ { // 5 consecutive failures trip the breaker
		_, _ = client.Reserve(context.Background(), unavailableReq)
	}

	_, err := client.Reserve(context.Background(), unavailableReq)
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, ports.ErrDownstreamNotConfigured) {
		t.Fatalf("Reserve while open: err = %v, want ErrDownstreamUnavailable + ErrDownstreamNotConfigured", err)
	}
	err = client.RevokeReservation(context.Background(), "res-1")
	if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, ports.ErrDownstreamNotConfigured) {
		t.Fatalf("RevokeReservation while open: err = %v, want ErrDownstreamUnavailable + ErrDownstreamNotConfigured", err)
	}
}
