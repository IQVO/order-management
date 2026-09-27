package productclassification_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/productclassification"
)

// recordingRecorder implements resilience.StateRecorder, capturing every
// state transition in order.
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

// The gobreaker.State values (0=closed,1=half-open,2=open), duplicated
// as untyped constants for the same reason as the inventorystorage
// breaker tests: resilience.RecordStateChange's documented "no
// translation table" contract.
const (
	gobreakerClosed = 0
	gobreakerOpen   = 2
)

// countingFakeDoer wraps fakeDoer (see client_test.go) with a call
// counter, so a test can prove retry attempt counts and breaker
// short-circuiting precisely.
type countingFakeDoer struct {
	resp  func() *http.Response
	err   error
	calls int32
}

func (d *countingFakeDoer) Do(*http.Request) (*http.Response, error) {
	atomic.AddInt32(&d.calls, 1)
	if d.err != nil {
		return nil, d.err
	}
	return d.resp(), nil
}

// TestBreakerClient_RetriesTransportErrorsUpToMaxAttempts is the
// ADR-0025 retry acceptance test: a fake HTTPDoer that always fails is
// retried exactly maxRetryAttempts (3) times per GetClassification
// call -- not fewer (leaving retry budget on the table) and not more
// (an unbounded retry storm).
func TestBreakerClient_RetriesTransportErrorsUpToMaxAttempts(t *testing.T) {
	boom := errors.New("connection refused")
	doer := &countingFakeDoer{err: boom}
	inner := productclassification.NewClient("http://inventory-storage.local", doer)
	client := productclassification.NewBreakerClient(inner, nil)

	view, err := client.GetClassification(context.Background(), "SKU-1")
	if err != nil {
		t.Fatalf("GetClassification must fail OPEN (nil error) even after exhausting retries, got %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false after retry exhaustion")
	}
	if got := atomic.LoadInt32(&doer.calls); got != 3 {
		t.Fatalf("doer calls = %d, want exactly 3 (1 original + 2 retries)", got)
	}
}

// TestBreakerClient_SucceedsOnSecondAttempt proves a transient failure
// (1 failure then success) is transparently retried into a successful
// result, with no error and no fail-open fallback.
func TestBreakerClient_SucceedsOnSecondAttempt(t *testing.T) {
	var call int32
	doer := &countingFakeDoer{
		resp: func() *http.Response {
			if atomic.AddInt32(&call, 1) == 1 {
				return jsonResponse(http.StatusInternalServerError, "")
			}
			return jsonResponse(http.StatusOK, `{"sku":"SKU-1","handlingTags":["Fragile"]}`)
		},
	}
	inner := productclassification.NewClient("http://inventory-storage.local", doer)
	client := productclassification.NewBreakerClient(inner, nil)

	view, err := client.GetClassification(context.Background(), "SKU-1")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if !view.Known || !view.HasTag("Fragile") {
		t.Fatalf("expected a successful classified result after retrying past the first 500, got %+v", view)
	}
	if got := atomic.LoadInt32(&doer.calls); got != 2 {
		t.Fatalf("doer calls = %d, want exactly 2 (1 failure + 1 success)", got)
	}
}

// TestBreakerClient_404DoesNotRetry proves a 404 (a legitimate
// Known=false answer) returns on the FIRST attempt, exactly like a 200
// does — it must never consume retry budget or look like a failure.
func TestBreakerClient_404DoesNotRetry(t *testing.T) {
	doer := &countingFakeDoer{resp: func() *http.Response { return jsonResponse(http.StatusNotFound, "") }}
	inner := productclassification.NewClient("http://inventory-storage.local", doer)
	client := productclassification.NewBreakerClient(inner, nil)

	view, err := client.GetClassification(context.Background(), "SKU-unknown")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if view.Known {
		t.Fatalf("expected Known=false on 404")
	}
	if got := atomic.LoadInt32(&doer.calls); got != 1 {
		t.Fatalf("doer calls = %d, want exactly 1 -- a 404 must not be retried", got)
	}
}

// TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback
// mirrors inventorystorage's breaker test: enough failed
// GetClassification calls (each internally already retried 3x, so the
// breaker sees ONE failure per call, not three) trips the breaker, and
// once open, calls short-circuit to PermissiveLookup's existing
// fail-open contract WITHOUT reaching the doer again.
func TestBreakerClient_OpensAfterConsecutiveFailures_ShortCircuitsToFallback(t *testing.T) {
	boom := errors.New("connection refused")
	doer := &countingFakeDoer{err: boom}
	inner := productclassification.NewClient("http://inventory-storage.local", doer)
	recorder := &recordingRecorder{}
	client := productclassification.NewBreakerClient(inner, recorder)

	// 5 consecutive failed calls (each internally retried 3x) trips
	// the breaker -- ReadyToTrip only sees breaker-level Execute
	// outcomes, i.e. 5, not 15.
	for i := 0; i < 5; i++ {
		view, err := client.GetClassification(context.Background(), "SKU-1")
		if err != nil || view.Known {
			t.Fatalf("call %d: got (%+v, %v), want a fail-open Known=false/nil (breaker still closed, exhausted retries)", i, view, err)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 failing calls = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}
	callsBeforeOpen := atomic.LoadInt32(&doer.calls)
	if callsBeforeOpen != 15 {
		t.Fatalf("doer calls before open = %d, want exactly 15 (5 calls x 3 attempts each)", callsBeforeOpen)
	}

	// Once open, the doer must never be reached again.
	view, err := client.GetClassification(context.Background(), "SKU-1")
	if err != nil || view.Known {
		t.Fatalf("while open: got (%+v, %v), want fail-open Known=false/nil (the fallback, unchanged)", view, err)
	}
	if atomic.LoadInt32(&doer.calls) != callsBeforeOpen {
		t.Fatalf("doer was called again while the breaker is open -- it must short-circuit instead")
	}
}

// TestBreakerClient_HalfOpenProbeRecoversToClosed proves the state
// machine round trip for the retrying client too: the underlying
// dependency fails until it has absorbed the 5 tripping calls (each
// retried up to 3x), then recovers -- the eventual half-open probe
// must reach the now-healthy upstream and close the breaker.
func TestBreakerClient_HalfOpenProbeRecoversToClosed(t *testing.T) {
	boom := errors.New("connection refused")
	twoPhase := &twoPhaseDoer{failUntilCalls: 15, err: boom, okResp: jsonResponse(http.StatusOK, `{"sku":"SKU-1","handlingTags":[]}`)} //nolint:bodyclose
	inner := productclassification.NewClient("http://inventory-storage.local", twoPhase)
	recorder := &recordingRecorder{}
	client := productclassification.NewBreakerClientWithTimeout(inner, recorder, 100*time.Millisecond)

	for i := 0; i < 5; i++ {
		if view, err := client.GetClassification(context.Background(), "SKU-1"); err != nil || view.Known {
			t.Fatalf("call %d: got (%+v, %v), want fail-open (breaker still closed)", i, view, err)
		}
	}
	if recorder.last() != gobreakerOpen {
		t.Fatalf("breaker state after 5 failing calls = %d, want open (%d)", recorder.last(), gobreakerOpen)
	}

	time.Sleep(150 * time.Millisecond)

	view, err := client.GetClassification(context.Background(), "SKU-1")
	if err != nil {
		t.Fatalf("half-open probe: %v", err)
	}
	if !view.Known {
		t.Fatalf("half-open probe must reach the REAL upstream (now healthy), got Known=false")
	}
	if recorder.last() != gobreakerClosed {
		t.Fatalf("breaker state after a successful half-open probe = %d, want closed (%d)", recorder.last(), gobreakerClosed)
	}
}

// twoPhaseDoer fails every call (with err) until failUntilCalls calls
// have been made, then returns okResp for every call after that --
// deterministically models "the transient failure has now cleared" for
// a retrying+circuit-broken client, where a single logical
// GetClassification call can itself issue up to maxRetryAttempts doer
// calls.
type twoPhaseDoer struct {
	failUntilCalls int32
	calls          int32
	err            error
	okResp         *http.Response
}

func (d *twoPhaseDoer) Do(*http.Request) (*http.Response, error) {
	n := atomic.AddInt32(&d.calls, 1)
	if n <= d.failUntilCalls {
		return nil, d.err
	}
	return d.okResp, nil
}
