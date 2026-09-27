package resilience_test

import (
	"context"
	"testing"
	"time"

	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/order-management/internal/resilience"
)

func TestReadyToTrip_FiveConsecutiveFailuresTrips(t *testing.T) {
	if !resilience.ReadyToTrip(gobreaker.Counts{ConsecutiveFailures: 5}) {
		t.Fatal("5 consecutive failures must trip the breaker")
	}
	if resilience.ReadyToTrip(gobreaker.Counts{ConsecutiveFailures: 4}) {
		t.Fatal("4 consecutive failures must NOT trip the breaker")
	}
}

func TestReadyToTrip_ErrorRateOverHalfTripsGivenEnoughVolume(t *testing.T) {
	// 10 requests, 6 failures = 60% error rate, over the minimum
	// volume threshold -- must trip.
	if !resilience.ReadyToTrip(gobreaker.Counts{Requests: 10, TotalFailures: 6, ConsecutiveFailures: 1}) {
		t.Fatal("60%% error rate over the minimum volume must trip the breaker")
	}
	// Exactly 50% must NOT trip (the condition is strictly > 50%).
	if resilience.ReadyToTrip(gobreaker.Counts{Requests: 10, TotalFailures: 5, ConsecutiveFailures: 1}) {
		t.Fatal("exactly 50%% error rate must NOT trip the breaker")
	}
}

func TestReadyToTrip_ErrorRateIgnoredBelowMinimumVolume(t *testing.T) {
	// 1 request, 1 failure = 100% error rate, but below the minimum
	// sample size -- must NOT trip on the error-rate leg alone (and
	// ConsecutiveFailures is only 1, so the other leg doesn't fire
	// either).
	if resilience.ReadyToTrip(gobreaker.Counts{Requests: 1, TotalFailures: 1, ConsecutiveFailures: 1}) {
		t.Fatal("a single failed request must not trip the breaker on the error-rate leg")
	}
}

func TestCallTimeout_NoDeadline_CapsAtMaxPerCall(t *testing.T) {
	ctx, cancel := resilience.CallTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a derived deadline when the parent context has none")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 60*time.Millisecond {
		t.Fatalf("remaining = %v, want roughly 50ms (capped at maxPerCall)", remaining)
	}
}

func TestCallTimeout_TighterParentDeadline_IsPreservedUnchanged(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer parentCancel()

	ctx, cancel := resilience.CallTimeout(parent, 5*time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected the parent's own deadline to be inherited")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 15*time.Millisecond {
		t.Fatalf("remaining = %v, want roughly 10ms (the tighter parent deadline, not the 5s cap)", remaining)
	}
}

func TestCallTimeout_LooserParentDeadline_IsCappedAtMaxPerCall(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer parentCancel()

	ctx, cancel := resilience.CallTimeout(parent, 50*time.Millisecond)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a derived deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 60*time.Millisecond {
		t.Fatalf("remaining = %v, want roughly 50ms (the tighter cap, not the parent's 5s)", remaining)
	}
}

func TestCallTimeout_CancelFuncAlwaysCancels(t *testing.T) {
	ctx, cancel := resilience.CallTimeout(context.Background(), 5*time.Second)
	cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancel() must cancel the returned context")
	}
}

func TestRecordStateChange_NilRecorderIsANoOp(t *testing.T) {
	fn := resilience.RecordStateChange("dep", nil)
	// Must not panic.
	fn("dep", gobreaker.StateClosed, gobreaker.StateOpen)
}

type spyRecorder struct {
	dependency string
	state      int64
	calls      int
}

func (s *spyRecorder) SetState(dependency string, state int64) {
	s.dependency = dependency
	s.state = state
	s.calls++
}

func TestRecordStateChange_ForwardsDependencyAndState(t *testing.T) {
	spy := &spyRecorder{}
	fn := resilience.RecordStateChange("inventory-storage", spy)

	fn("ignored", gobreaker.StateClosed, gobreaker.StateOpen)
	if spy.dependency != "inventory-storage" {
		t.Fatalf("dependency = %q, want %q", spy.dependency, "inventory-storage")
	}
	if spy.state != int64(gobreaker.StateOpen) {
		t.Fatalf("state = %d, want %d (StateOpen)", spy.state, gobreaker.StateOpen)
	}
	if spy.calls != 1 {
		t.Fatalf("calls = %d, want 1", spy.calls)
	}
}
