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
func TestInfrastructureFailuresAreTaggedDownstreamUnavailable(t *testing.T) {
	req := ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"}

	t.Run("unexpected status on Reserve", func(t *testing.T) {
		var got captured
		srv := newServer(t, http.StatusInternalServerError, "", &got)
		_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, inventorystorage.ErrUnexpectedStatus) {
			t.Fatalf("err = %v, want both ErrDownstreamUnavailable and ErrUnexpectedStatus", err)
		}
	})

	t.Run("unexpected status on RevokeReservation", func(t *testing.T) {
		var got captured
		srv := newServer(t, http.StatusBadGateway, "", &got)
		err := inventorystorage.NewClient(srv.URL, nil).RevokeReservation(context.Background(), "res-1")
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, inventorystorage.ErrUnexpectedStatus) {
			t.Fatalf("err = %v, want both ErrDownstreamUnavailable and ErrUnexpectedStatus", err)
		}
	})

	t.Run("2xx without a reservation id", func(t *testing.T) {
		var got captured
		srv := newServer(t, http.StatusCreated, `{}`, &got)
		_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrDownstreamUnavailable) {
			t.Fatalf("err = %v, want ErrDownstreamUnavailable", err)
		}
	})

	t.Run("undecodable 2xx body", func(t *testing.T) {
		var got captured
		srv := newServer(t, http.StatusCreated, `not json`, &got)
		_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrDownstreamUnavailable) {
			t.Fatalf("err = %v, want ErrDownstreamUnavailable", err)
		}
	})

	t.Run("transport error keeps its cause reachable", func(t *testing.T) {
		boom := errors.New("connection refused")
		client := inventorystorage.NewClient("http://example.invalid", errDoer{err: boom})

		_, err := client.Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, boom) {
			t.Fatalf("Reserve err = %v, want ErrDownstreamUnavailable wrapping %v", err, boom)
		}
		err = client.RevokeReservation(context.Background(), "res-1")
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, boom) {
			t.Fatalf("RevokeReservation err = %v, want ErrDownstreamUnavailable wrapping %v", err, boom)
		}
	})

	t.Run("409 is never tagged unavailable", func(t *testing.T) {
		var got captured
		srv := newServer(t, http.StatusConflict, `{}`, &got)
		_, err := inventorystorage.NewClient(srv.URL, nil).Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrInsufficientStock) || errors.Is(err, ports.ErrDownstreamUnavailable) {
			t.Fatalf("err = %v, want ErrInsufficientStock only", err)
		}
	})

	t.Run("open circuit is unavailable AND still not-configured", func(t *testing.T) {
		boom := errors.New("connection refused")
		client := inventorystorage.NewBreakerClient(
			inventorystorage.NewClient("http://example.invalid", errDoer{err: boom}), &recordingRecorder{})
		for i := 0; i < 5; i++ {
			_, _ = client.Reserve(context.Background(), req)
		}

		_, err := client.Reserve(context.Background(), req)
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, ports.ErrDownstreamNotConfigured) {
			t.Fatalf("Reserve while open: err = %v, want ErrDownstreamUnavailable + ErrDownstreamNotConfigured", err)
		}
		err = client.RevokeReservation(context.Background(), "res-1")
		if !errors.Is(err, ports.ErrDownstreamUnavailable) || !errors.Is(err, ports.ErrDownstreamNotConfigured) {
			t.Fatalf("RevokeReservation while open: err = %v, want ErrDownstreamUnavailable + ErrDownstreamNotConfigured", err)
		}
	})
}
