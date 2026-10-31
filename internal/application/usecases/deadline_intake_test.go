package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Tests for ADR 0020 §2's externally-dictated deadline: the routing that
// makes PromisePolicy.FeasibleBy reachable from intake. Before this, the
// method existed but nothing in the service ever called it.

// deadlineSchedule is a ScheduleSource with a fixed window list. It lives
// here rather than reusing the domain package's fake because that one is
// unexported to internal/domain/order's own test binary.
type deadlineSchedule struct {
	windows []order.CPTWindow
}

func (s *deadlineSchedule) NextCutoffs(_ string, _ time.Time, _ int) ([]order.CPTWindow, bool) {
	if len(s.windows) == 0 {
		return nil, false
	}
	return s.windows, true
}

type deadlineCapability struct {
	cycleTimes map[shared.PathId]time.Duration
}

func (c *deadlineCapability) CycleTimeP95(pathId shared.PathId) (time.Duration, bool) {
	d, ok := c.cycleTimes[pathId]
	return d, ok
}

// deadlineFixture wires a fixture whose PromisePolicy has real schedule
// and capability data, so FeasibleBy can actually answer true. The
// default newFixture() has neither, which makes FeasibleBy correctly but
// uninterestingly return false for everything.
func deadlineFixture(t *testing.T, windows []order.CPTWindow) *fixture {
	t.Helper()
	f := newFixture()
	f.promise = order.PromisePolicy{
		Schedule:      &deadlineSchedule{windows: windows},
		Capability:    &deadlineCapability{cycleTimes: map[shared.PathId]time.Duration{"singles": time.Hour}},
		Fallback:      order.NewLeadTimePolicy(24*time.Hour, nil),
		DefaultSiteId: "site-1",
	}
	return f
}

func deadlineLine() usecases.NewLine {
	return usecases.NewLine{SKU: "SKU-1", Quantity: 1, PathID: "singles"}
}

func TestReceiveWithDeadline_FeasibleOrderIsPromisedWithNetworkBasis(t *testing.T) {
	cutoff := now().Add(6 * time.Hour)
	f := deadlineFixture(t, []order.CPTWindow{
		{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"singles"}},
	})

	deadline := now().Add(8 * time.Hour)
	o, err := f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}

	if o.PromiseDate() == nil {
		t.Fatal("expected a promise: a 6h window with 1h cycle time makes an 8h deadline")
	}
	if !o.PromiseDate().Equal(cutoff) {
		t.Fatalf("promiseDate = %v, want the window cutoff %v", o.PromiseDate(), cutoff)
	}

	// The basis is the whole point of ADR 0019's KPI split: a promise
	// DICTATED by someone else must never be scored as one we chose.
	if b := o.PromiseBasis(); b == nil || *b != order.BasisNetwork {
		t.Fatalf("basis = %v, want BasisNetwork", b)
	}
}

func TestReceiveWithDeadline_InfeasibleOrderGetsNoPromiseAtAll(t *testing.T) {
	// The only window is AFTER the deadline.
	f := deadlineFixture(t, []order.CPTWindow{
		{CptId: "sp1-late", CutoffAt: now().Add(20 * time.Hour), EligiblePathIds: []string{"singles"}},
	})

	deadline := now().Add(8 * time.Hour)
	o, err := f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}

	// No promise, deliberately. An optimistic promise we already know
	// breaks the deadline would tell the caller we can do something we
	// cannot — and finding that out is the caller's entire reason for
	// sending a deadline.
	if o.PromiseDate() != nil {
		t.Fatalf("promiseDate = %v, want nil: no window meets the deadline", o.PromiseDate())
	}

	// But the order still exists and is still allocated: "we cannot make
	// your date" is an answer, not a rejection.
	if len(o.LinesWithStatus(order.LineAllocated))+len(o.LinesWithStatus(order.LineReleased)) == 0 {
		t.Fatal("an infeasible order must still be created and allocated")
	}
}

func TestReceiveWithDeadline_NoLeadTimeFallbackWhenCapabilityDataIsMissing(t *testing.T) {
	f := newFixture() // no Schedule, no Capability — only the lead-time fallback

	deadline := now().Add(48 * time.Hour)
	o, err := f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}

	// A lead-time promise would comfortably "meet" a 48h deadline, and
	// that is exactly the trap: FeasibleBy has no fallback because its
	// true becomes a fill-or-kill commitment an external party measures.
	// An estimate dressed as a commitment is worse than admitting we
	// cannot tell.
	if o.PromiseDate() != nil {
		t.Fatalf("promiseDate = %v, want nil: FeasibleBy must not fall back to lead time", o.PromiseDate())
	}
}

func TestReceiveWithDeadline_PicksTheLatestWindowThatMeetsTheDeadline(t *testing.T) {
	early := now().Add(2 * time.Hour)
	late := now().Add(6 * time.Hour)
	tooLate := now().Add(20 * time.Hour)

	f := deadlineFixture(t, []order.CPTWindow{
		{CptId: "sp1-early", CutoffAt: early, EligiblePathIds: []string{"singles"}},
		{CptId: "sp1-late", CutoffAt: late, EligiblePathIds: []string{"singles"}},
		{CptId: "sp1-toolate", CutoffAt: tooLate, EligiblePathIds: []string{"singles"}},
	})

	deadline := now().Add(8 * time.Hour)
	o, err := f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}

	// The LATEST qualifying window, not the earliest: when the date is
	// fixed externally, shipping sooner buys nothing and burns capacity
	// other demand may need. This is the rule #81 corrected, now
	// observable end-to-end through intake rather than only in the
	// domain unit test.
	if o.PromiseDate() == nil || !o.PromiseDate().Equal(late) {
		t.Fatalf("promiseDate = %v, want the LATEST qualifying window %v", o.PromiseDate(), late)
	}
}

func TestReceiveWithoutDeadline_IsUnchanged(t *testing.T) {
	// The additive guarantee: absent a deadline, promising still runs
	// through PromiseGroups and still falls back to lead time.
	f := newFixture()

	o, err := f.receiveOrder().Execute(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o.PromiseDate() == nil {
		t.Fatal("an ordinary order must still get its lead-time promise")
	}
	if b := o.PromiseBasis(); b != nil && *b == order.BasisNetwork {
		t.Fatal("an order with no deadline must never carry BasisNetwork")
	}
	if o.RequiredShipBy() != nil {
		t.Fatal("an order received without a deadline must report none")
	}
}

func TestReceiveWithDeadline_DeadlineSurvivesARepositoryRoundTrip(t *testing.T) {
	cutoff := now().Add(6 * time.Hour)
	f := deadlineFixture(t, []order.CPTWindow{
		{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"singles"}},
	})

	deadline := now().Add(8 * time.Hour)
	o, err := f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}

	// A later RetryAllocation must re-promise against the SAME deadline.
	// If the deadline did not survive persistence, retry would silently
	// fall back to the ordinary earliest-window promise and quietly
	// break the external commitment.
	stored, err := f.orders.FindByID(context.Background(), o.ID())
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if stored.RequiredShipBy() == nil || !stored.RequiredShipBy().Equal(deadline) {
		t.Fatalf("requiredShipBy = %v, want %v to survive persistence", stored.RequiredShipBy(), deadline)
	}
}
