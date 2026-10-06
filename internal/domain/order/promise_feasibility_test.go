package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Tests for PromisePolicy.FeasibleBy — the deadline-feasibility dual of
// Promise introduced by ADR 0020 for network-originated demand. Reuses
// fakeSchedule / fakeCapability / fakeCapacity / newAllocatedOrder / pq
// from promise_policy_test.go deliberately: FeasibleBy must agree with
// Promise on the per-line rule, so testing it against different fakes
// would hide exactly the divergence worth catching.

// feasibleByPolicy builds a policy with one site and one window set.
func feasibleByPolicy(windows []order.CPTWindow, cycleTimes map[shared.PathId]time.Duration) order.PromisePolicy {
	return order.PromisePolicy{
		Schedule:   &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{"site-1": windows}},
		Capability: &fakeCapability{cycleTimes: cycleTimes},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}
}

func TestFeasibleBy_WindowBeforeDeadline_IsFeasibleWithNetworkBasis(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))
	cutoff := now.Add(6 * time.Hour)

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	got, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour))
	if !ok {
		t.Fatal("expected ok=true: a window 6h out, 2h cycle time, deadline 8h out")
	}
	if got.Basis != order.BasisNetwork {
		t.Fatalf("Basis = %q, want Network — a dictated promise must never be tagged as one we chose", got.Basis)
	}
	if got.CptId != "sp1-1800" {
		t.Fatalf("CptId = %q, want sp1-1800: the real window must be returned, not a synthesized one", got.CptId)
	}
	if !got.CutoffAt.Equal(cutoff) {
		t.Fatalf("CutoffAt = %v, want %v", got.CutoffAt, cutoff)
	}
}

func TestFeasibleBy_OnlyWindowIsAfterDeadline_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// The line COULD make this window (2h cycle vs 6h out) — the only
	// reason it fails is the deadline. This is the discriminating case:
	// a bug that ignored the deadline entirely would return ok=true.
	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(4*time.Hour)); ok {
		t.Fatal("expected ok=false: the only window cuts off 6h out but the deadline is 4h out")
	}
}

func TestFeasibleBy_WindowExactlyAtDeadline_IsFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))
	cutoff := now.Add(6 * time.Hour)

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	// Boundary: "at or before" includes exactly at. A departure that
	// leaves precisely at the promised instant has met the promise.
	if _, ok := policy.FeasibleBy(now, o, cutoff); !ok {
		t.Fatal("expected ok=true when the window cuts off exactly at the deadline")
	}
}

func TestFeasibleBy_PicksLatestQualifyingWindow_NotEarliest(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// Three windows, all within the deadline and all makeable. ADR 0020
	// §2 requires the LATEST — shipping earlier than a fixed external
	// date gains nothing and burns capacity other demand may need.
	policy := feasibleByPolicy([]order.CPTWindow{
		{CptId: "sp1-1000", CutoffAt: now.Add(4 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "sp1-1400", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}},
	}, map[shared.PathId]time.Duration{"pick": 2 * time.Hour})

	got, ok := policy.FeasibleBy(now, o, now.Add(9*time.Hour))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.CptId != "sp1-1800" {
		t.Fatalf("CptId = %q, want sp1-1800 (the latest qualifying window); picking the earliest wastes slack", got.CptId)
	}
}

func TestFeasibleBy_LatestQualifying_IgnoresUnmakeableLaterWindows(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 5))

	// The latest window is within the deadline but lacks capacity, so
	// the answer must fall back to the latest window that genuinely
	// qualifies — not simply "the last one before the deadline".
	policy := feasibleByPolicy([]order.CPTWindow{
		{CptId: "sp1-1400", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}},
	}, map[shared.PathId]time.Duration{"pick": 2 * time.Hour})
	policy.Capacity = &fakeCapacity{
		remaining: map[string]int{capacityKey("pick", "sp1-1800"): 1},
		known:     map[string]bool{capacityKey("pick", "sp1-1800"): true},
	}

	got, ok := policy.FeasibleBy(now, o, now.Add(9*time.Hour))
	if !ok {
		t.Fatal("expected ok=true: the earlier window still qualifies")
	}
	if got.CptId != "sp1-1400" {
		t.Fatalf("CptId = %q, want sp1-1400 — a later window that fails capacity must not be chosen", got.CptId)
	}
}

func TestFeasibleBy_SkipsLateWindowAndPicksEarlierFeasibleOne(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// Deliberately UNSORTED: the out-of-range window comes first.
	// FeasibleBy must not stop at the first out-of-range window — the
	// domain interface promises no ordering, only today's adapters do.
	policy := feasibleByPolicy([]order.CPTWindow{
		{CptId: "sp1-2200", CutoffAt: now.Add(10 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}},
	}, map[shared.PathId]time.Duration{"pick": 2 * time.Hour})

	got, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour))
	if !ok {
		t.Fatal("expected ok=true: the second window is within the deadline")
	}
	if got.CptId != "sp1-1800" {
		t.Fatalf("CptId = %q, want sp1-1800 — an unsorted horizon must not hide a feasible window", got.CptId)
	}
}

func TestFeasibleBy_CycleTimeOverrunsWindow_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// Window is within the deadline, but the work cannot finish in time.
	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(1 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 3 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour)); ok {
		t.Fatal("expected ok=false: 3h of work cannot make a window 1h away")
	}
}

func TestFeasibleBy_LineOnIneligiblePath_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pack", 2))

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pack": 2 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour)); ok {
		t.Fatal("expected ok=false: the line's path is not eligible for the window")
	}
}

func TestFeasibleBy_InsufficientCapacity_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 5))

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)
	policy.Capacity = &fakeCapacity{
		remaining: map[string]int{capacityKey("pick", "sp1-1800"): 3},
		known:     map[string]bool{capacityKey("pick", "sp1-1800"): true},
	}

	if _, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour)); ok {
		t.Fatal("expected ok=false: 5 units requested, 3 remaining on the path")
	}
}

func TestFeasibleBy_EveryLineMustFitTheSameWindow(t *testing.T) {
	now := testTime()
	// Network demand is ship-complete: one slow line disqualifies the
	// whole order rather than splitting into groups (ADR 0020 §2).
	o := newAllocatedOrder(t, pq("pick", 1), pq("pack", 1))

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(4 * time.Hour), EligiblePathIds: []string{"pick", "pack"}}},
		map[shared.PathId]time.Duration{"pick": 1 * time.Hour, "pack": 6 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour)); ok {
		t.Fatal("expected ok=false: the pack line cannot make the only window, so the order cannot")
	}
}

// --- the no-fallback contract -------------------------------------------
//
// These are the tests that matter most. Promise falls back to
// LeadTimePolicy on absent input; FeasibleBy must NOT, because its
// answer becomes an external fill-or-kill commitment. Every one of
// these would pass just as happily against a buggy implementation that
// fell back — except that a fallback would return ok=TRUE.

func TestFeasibleBy_NilSchedule_IsNotFeasible_NeverFallsBack(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	policy := order.PromisePolicy{
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 2 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	// Deadline is 48h out — a LeadTimePolicy fallback would comfortably
	// "fit" and report feasible. It must not.
	if _, ok := policy.FeasibleBy(now, o, now.Add(48*time.Hour)); ok {
		t.Fatal("expected ok=false with no schedule: a LeadTime guess must never become a network commitment")
	}
}

func TestFeasibleBy_NilCapability_IsNotFeasible_NeverFallsBack(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	if _, ok := policy.FeasibleBy(now, o, now.Add(48*time.Hour)); ok {
		t.Fatal("expected ok=false with no capability source")
	}
}

func TestFeasibleBy_ColdCache_IsNotFeasible_NeverFallsBack(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// No entry for site-1 — the documented cold-start case that "will
	// look like a bug the first time someone sees it".
	policy := order.PromisePolicy{
		Schedule:   &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 2 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	if _, ok := policy.FeasibleBy(now, o, now.Add(48*time.Hour)); ok {
		t.Fatal("expected ok=false on a cold schedule cache")
	}
}

func TestFeasibleBy_EmptyHorizon_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	policy := feasibleByPolicy(
		[]order.CPTWindow{},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(48*time.Hour)); ok {
		t.Fatal("expected ok=false when the schedule returns no windows")
	}
}

func TestFeasibleBy_UnknownCycleTime_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// Schedule and capability are both present; the specific path's
	// cycle time is simply unknown. Promise would fall back here.
	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(48*time.Hour)); ok {
		t.Fatal("expected ok=false when the path's cycle time is unknown")
	}
}

func TestFeasibleBy_NoAllocatedLines_IsNotFeasible(t *testing.T) {
	now := testTime()
	o := order.Rehydrate(order.OrderSnapshot{ID: "ord-1", Lines: []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LinePending, nil),
	}, AllowPartialShipment: true})

	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	if _, ok := policy.FeasibleBy(now, o, now.Add(8*time.Hour)); ok {
		t.Fatal("expected ok=false: nothing is allocated, so there is nothing to commit")
	}
}

// --- agreement with Promise ----------------------------------------------

func TestFeasibleBy_AgreesWithPromise_OnTheSingleFeasibleWindow(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// With exactly ONE qualifying window, "earliest" and "latest" are
	// the same window, so the two policies must name it identically.
	// This is the honest agreement test: where they can differ they
	// SHOULD differ (Promise takes the earliest, FeasibleBy the
	// latest), so asserting agreement on a multi-window horizon would
	// just re-encode a bug.
	policy := feasibleByPolicy(
		[]order.CPTWindow{{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		map[shared.PathId]time.Duration{"pick": 2 * time.Hour},
	)

	promised, pok := policy.Promise(now, o)
	if !pok || promised.Basis != order.BasisCapability {
		t.Fatalf("precondition failed: Promise ok=%v basis=%q", pok, promised.Basis)
	}

	feasible, fok := policy.FeasibleBy(now, o, now.Add(24*time.Hour))
	if !fok {
		t.Fatal("expected ok=true")
	}
	if feasible.CptId != promised.CptId || !feasible.CutoffAt.Equal(promised.CutoffAt) {
		t.Fatalf("FeasibleBy chose (%s, %v), Promise chose (%s, %v) — with one window they must agree",
			feasible.CptId, feasible.CutoffAt, promised.CptId, promised.CutoffAt)
	}
	if feasible.Basis == promised.Basis {
		t.Fatal("basis must differ: Promise chose this window, FeasibleBy was handed the deadline")
	}
}

func TestFeasibleBy_DivergesFromPromise_WhenSeveralWindowsQualify(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	// The inverse of the test above, and the one that pins ADR 0020 §2:
	// given a choice, Promise takes the EARLIEST and FeasibleBy the
	// LATEST. They must not agree here.
	policy := feasibleByPolicy([]order.CPTWindow{
		{CptId: "sp1-1000", CutoffAt: now.Add(4 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}},
	}, map[shared.PathId]time.Duration{"pick": 2 * time.Hour})

	promised, _ := policy.Promise(now, o)
	feasible, ok := policy.FeasibleBy(now, o, now.Add(9*time.Hour))
	if !ok {
		t.Fatal("expected ok=true")
	}
	if promised.CptId != "sp1-1000" {
		t.Fatalf("Promise chose %q, want the earliest sp1-1000", promised.CptId)
	}
	if feasible.CptId != "sp1-1800" {
		t.Fatalf("FeasibleBy chose %q, want the latest sp1-1800", feasible.CptId)
	}
}

func TestBasisNetwork_IsDistinctFromCapabilityAndLeadTime(t *testing.T) {
	// Cheap, but it protects ADR 0019's KPI split: if someone ever
	// collapses Network onto Capability, dictated promises start being
	// averaged into the metric that measures promises we chose.
	if order.BasisNetwork == order.BasisCapability || order.BasisNetwork == order.BasisLeadTime {
		t.Fatal("BasisNetwork must be a distinct value")
	}
	if order.BasisNetwork.String() != "Network" {
		t.Fatalf("BasisNetwork.String() = %q, want Network — it is persisted and read by analytics", order.BasisNetwork.String())
	}
}
