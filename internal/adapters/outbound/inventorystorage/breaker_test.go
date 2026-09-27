package inventorystorage_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/inventorystorage"
	"github.com/claudioed/order-management/internal/application/ports"
)

// recordingRecorder implements resilience.StateRecorder, capturing every
// state transition in order so a test can assert the breaker actually
// opened/closed at the expected point, not just that the call outcomes
// looked right.
type recordingRecorder struct {
	states []int64
}

func (r *recordingRecorder) SetState(_ string, state int64) {
	r.states = append(r.states, state)
}

func (r *recordingRecorder) last() int64 {
	if len(r.states) == 0 {
		return -1
	}
	return r.states[len(r.states)-1]
}

// The gobreaker.State values (0=closed,1=half-open,2=open) are
// duplicated here as untyped constants rather than importing gobreaker
// into this _test package, matching resilience.RecordStateChange's own
// documented "no translation table" contract -- this test asserts
// against that SAME numbering, not a re-derived one.
const (
	gobreakerClosed = 0
	gobreakerOpen   = 2
)

// countingErrDoer is errDoer (see client_test.go) plus a call counter,
// so a test can prove the breaker stopped calling the doer at all once
// open, not merely that its own errors kept propagating.
type countingErrDoer struct {
	err   error
	calls int32
}

func (d *countingErrDoer) Do(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&d.calls, 1)
	return nil, d.err
}

// TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback
// is the ADR-0025 acceptance test for inventorystorage's breaker: a
// fake HTTPDoer that always fails drives 5 consecutive Reserve
// failures (resilience.ReadyToTrip's ConsecutiveFailures>=5 leg), the
// breaker opens, and every call after that is short-circuited to
// PermissiveClient's EXISTING fail-loud behaviour
// (ports.ErrDownstreamNotConfigured) WITHOUT ever reaching the fake
// doer again — the same observable failure mode the permissive client
// already had, just reached via a different path once the breaker
// trips.
func TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback(t *testing.T) {
	boom := errors.New("connection refused")
	fake := &countingErrDoer{err: boom}
	inner := inventorystorage.NewClient("http://example.invalid", fake)
	recorder := &recordingRecorder{}
	client := inventorystorage.NewBreakerClient(inner, recorder)

	// 5 consecutive failures trips the breaker (ReadyToTrip).
	for i := 0; i < 5; i++ {
		_, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"})
		if !errors.Is(err, boom) {
			t.Fatalf("call %d: err = %v, want the real transport error %v (breaker still closed)", i, err, boom)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 consecutive failures = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}
	callsBeforeShortCircuit := atomic.LoadInt32(&fake.calls)

	// While open, calls short-circuit to the SAME fallback behaviour
	// the permissive client already had -- fail LOUD with
	// ErrDownstreamNotConfigured -- and never reach the doer again.
	_, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"})
	if !errors.Is(err, ports.ErrDownstreamNotConfigured) {
		t.Fatalf("while open, err = %v, want %v (the existing permissive fail-loud behaviour)", err, ports.ErrDownstreamNotConfigured)
	}
	if atomic.LoadInt32(&fake.calls) != callsBeforeShortCircuit {
		t.Fatalf("doer was called again while the breaker is open -- it must short-circuit instead")
	}

	if err := client.RevokeReservation(context.Background(), "res-1"); !errors.Is(err, ports.ErrDownstreamNotConfigured) {
		t.Fatalf("RevokeReservation while open: err = %v, want %v", err, ports.ErrDownstreamNotConfigured)
	}
}

// flakyThenOKDoer fails the first failCount calls with err, then
// delegates to ok — the "fake HTTPDoer that fails N times then
// succeeds" the ADR's breaker tests are built around, used here to
// drive the half-open probe recovery.
type flakyThenOKDoer struct {
	remaining int32
	err       error
	ok        interface {
		Do(*http.Request) (*http.Response, error)
	}
}

func (d *flakyThenOKDoer) Do(req *http.Request) (*http.Response, error) {
	if atomic.AddInt32(&d.remaining, -1) >= 0 {
		return nil, d.err
	}
	return d.ok.Do(req)
}

// TestBreakerClient_HalfOpenProbeRecoversToClosed proves the full
// state-machine round trip: open -> (cooldown) -> half-open probe
// succeeds -> closed, and that the successful probe is served by the
// REAL upstream (not the fallback) once recovered.
func TestBreakerClient_HalfOpenProbeRecoversToClosed(t *testing.T) {
	var got captured
	srv := newServer(t, http.StatusCreated, `{"id":"res-42"}`, &got)
	realDoer := http.DefaultClient
	fake := &flakyThenOKDoer{remaining: 5, err: errors.New("connection refused"), ok: realDoer}
	inner := inventorystorage.NewClient(srv.URL, fake)

	recorder := &recordingRecorder{}
	// A short breaker cooldown so the test does not sleep for
	// resilience.DefaultTimeout (30s) in real time.
	client := inventorystorage.NewBreakerClientWithTimeout(inner, recorder, 100*time.Millisecond)

	for i := 0; i < 5; i++ {
		if _, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"}); err == nil {
			t.Fatalf("call %d unexpectedly succeeded before the fake doer's failure budget was exhausted", i)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 failures = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}

	// Wait out the cooldown so the breaker allows a half-open probe.
	time.Sleep(150 * time.Millisecond)

	result, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"})
	if err != nil {
		t.Fatalf("half-open probe Reserve: %v (the fake doer's failure budget is exhausted, it should now succeed)", err)
	}
	if result.ReservationID != "res-42" {
		t.Fatalf("ReservationID = %q, want res-42 -- the probe must reach the REAL upstream, not the fallback", result.ReservationID)
	}
	if recorder.last() != gobreakerClosed {
		t.Fatalf("breaker state after a successful half-open probe = %d, want closed (%d)", recorder.last(), gobreakerClosed)
	}
}

// TestBreakerClient_InsufficientStockNeverTripsTheBreaker asserts
// ports.ErrInsufficientStock (a real, correct 409 answer from a healthy
// dependency) never counts as a breaker failure -- a long run of
// ordinary backorders must never open the breaker.
func TestBreakerClient_InsufficientStockNeverTripsTheBreaker(t *testing.T) {
	var got captured
	srv := newServer(t, http.StatusConflict,
		`{"type":"https://errors.inventory-storage.warehouse-systems.dev/insufficient-usable","status":409}`, &got)
	inner := inventorystorage.NewClient(srv.URL, nil)
	recorder := &recordingRecorder{}
	client := inventorystorage.NewBreakerClient(inner, recorder)

	for i := 0; i < 10; i++ {
		_, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"})
		if !errors.Is(err, ports.ErrInsufficientStock) {
			t.Fatalf("call %d: err = %v, want %v", i, err, ports.ErrInsufficientStock)
		}
	}
	if len(recorder.states) != 0 {
		t.Fatalf("breaker recorded %d state transitions from nothing but 409s, want 0 -- insufficient stock must never trip the breaker", len(recorder.states))
	}
}

// TestBreakerClient_NilRecorderIsANoOp confirms a nil StateRecorder
// (the documented convention) never panics.
func TestBreakerClient_NilRecorderIsANoOp(t *testing.T) {
	var got captured
	srv := newServer(t, http.StatusCreated, `{"id":"res-1"}`, &got)
	inner := inventorystorage.NewClient(srv.URL, nil)
	client := inventorystorage.NewBreakerClient(inner, nil)

	if _, err := client.Reserve(context.Background(), ports.ReservationRequest{SKU: "SKU-1", Quantity: 1, DemandRef: "ord-1"}); err != nil {
		t.Fatalf("Reserve with nil recorder: %v", err)
	}
}
