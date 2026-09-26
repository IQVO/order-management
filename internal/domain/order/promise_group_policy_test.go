package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// newAllocatedOrderShipComplete builds a rehydrated order with allocated
// lines and AllowPartialShipment=false, mirroring newAllocatedOrder from
// promise_policy_test.go but with the ship-complete flag flipped, since
// ADR 0017's whole point is that the two flag values now diverge.
func newAllocatedOrderShipComplete(t *testing.T, pathsAndQty ...struct {
	path shared.PathId
	qty  int
}) *order.Order {
	t.Helper()
	lines := make([]*order.OrderLine, 0, len(pathsAndQty))
	for i, pq := range pathsAndQty {
		lines = append(lines, order.RehydrateOrderLine(
			i+1, "SKU-1", pq.qty, pq.path, false, order.LineAllocated, nil,
		))
	}
	return order.Rehydrate("ord-1", lines, false, nil, nil, nil)
}

// TestPromiseGroups_ShipComplete_IsOneGroupIdenticalToPromise proves the
// ADR 0014 §3 / ADR 0017 hard requirement: a ship-complete order's
// PromiseGroups result is EXACTLY what Promise itself would return,
// wrapped as a single group covering every allocated line.
func TestPromiseGroups_ShipComplete_IsOneGroupIdenticalToPromise(t *testing.T) {
	now := testTime()
	o := newAllocatedOrderShipComplete(t, pq("pick", 1), pq("singles", 1))

	cutoff := now.Add(6 * time.Hour)
	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"pick", "singles"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			"pick":    1 * time.Hour,
			"singles": 1 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	wantPromise, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("Promise: expected ok=true")
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("PromiseGroups: expected ok=true")
	}
	if len(groups) != 1 {
		t.Fatalf("PromiseGroups = %d groups, want exactly 1 for a ship-complete order", len(groups))
	}
	if groups[0].Promise != wantPromise {
		t.Fatalf("PromiseGroups[0].Promise = %+v, want %+v (identical to Promise's own result)", groups[0].Promise, wantPromise)
	}
	wantLineNos := []int{1, 2}
	if len(groups[0].LineNos) != len(wantLineNos) {
		t.Fatalf("LineNos = %v, want %v", groups[0].LineNos, wantLineNos)
	}
	for i := range wantLineNos {
		if groups[0].LineNos[i] != wantLineNos[i] {
			t.Fatalf("LineNos = %v, want %v", groups[0].LineNos, wantLineNos)
		}
	}
}

// TestPromiseGroups_ShipComplete_FallbackIsOneGroup covers the fallback
// branch (no schedule/capability) for a ship-complete order: still one
// group, still identical to Promise's own fallback result.
func TestPromiseGroups_ShipComplete_FallbackIsOneGroup(t *testing.T) {
	now := testTime()
	o := newAllocatedOrderShipComplete(t, pq("pick", 1), pq("multis", 1))

	policy := order.PromisePolicy{
		Fallback: order.NewLeadTimePolicy(12*time.Hour, map[shared.PathId]time.Duration{
			"multis": 36 * time.Hour,
		}),
	}

	wantPromise, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("Promise: expected ok=true")
	}
	if wantPromise.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", wantPromise.Basis)
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("PromiseGroups: expected ok=true")
	}
	if len(groups) != 1 {
		t.Fatalf("PromiseGroups = %d groups, want 1", len(groups))
	}
	if groups[0].Promise != wantPromise {
		t.Fatalf("PromiseGroups[0].Promise = %+v, want %+v", groups[0].Promise, wantPromise)
	}
}

// TestPromiseGroups_NoAllocatedLines_ReturnsNotOK mirrors Promise's own
// contract for both ship-complete and partial-shipment orders.
func TestPromiseGroups_NoAllocatedLines_ReturnsNotOK(t *testing.T) {
	for _, allowPartial := range []bool{false, true} {
		o := order.Rehydrate("ord-1", []*order.OrderLine{
			order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LinePending, nil),
		}, allowPartial, nil, nil, nil)

		policy := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}
		groups, ok := policy.PromiseGroups(testTime(), o)
		if ok {
			t.Fatalf("allowPartial=%v: expected ok=false when no line is allocated", allowPartial)
		}
		if groups != nil {
			t.Fatalf("allowPartial=%v: expected nil groups, got %v", allowPartial, groups)
		}
	}
}

// TestPromiseGroups_PartialShipment_AllLinesShareOneCutoff proves that
// even with AllowPartialShipment=true, lines that all happen to make the
// SAME cutoff land in one group.
func TestPromiseGroups_PartialShipment_AllLinesShareOneCutoff(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("singles", 1))

	cutoff := now.Add(6 * time.Hour)
	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"pick", "singles"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			"pick":    1 * time.Hour,
			"singles": 1 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1 (both lines share the same cutoff)", len(groups))
	}
	if len(groups[0].LineNos) != 2 {
		t.Fatalf("LineNos = %v, want both lines in one group", groups[0].LineNos)
	}
	if groups[0].Promise.CptId != "sp1-1800" || groups[0].Promise.Basis != order.BasisCapability {
		t.Fatalf("Promise = %+v, want CptId=sp1-1800 Basis=Capability", groups[0].Promise)
	}
}

// TestPromiseGroups_PartialShipment_SplitsAcrossTwoRealCutoffs is the
// core new behaviour ADR 0014 §3 introduces: two lines that can only
// make two DIFFERENT real CPT windows land in two separate groups, each
// carrying only its own line.
func TestPromiseGroups_PartialShipment_SplitsAcrossTwoRealCutoffs(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("multis", 1))

	earlyWindow := order.CPTWindow{CptId: "sp1-1200", CutoffAt: now.Add(2 * time.Hour), EligiblePathIds: []string{"pick", "multis"}}
	lateWindow := order.CPTWindow{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick", "multis"}}

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {earlyWindow, lateWindow},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			// pick fits the early window (1h cycle, 2h out); multis does
			// not (5h cycle > 2h out) but does fit the late one (8h out).
			"pick":   1 * time.Hour,
			"multis": 5 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2 (pick makes the early cutoff, multis only the late one): %+v", len(groups), groups)
	}

	byCpt := make(map[string]order.PromiseGroup, len(groups))
	for _, g := range groups {
		byCpt[g.Promise.CptId] = g
	}

	early, ok := byCpt["sp1-1200"]
	if !ok || len(early.LineNos) != 1 || early.LineNos[0] != 1 {
		t.Fatalf("early group = %+v, want line 1 only", early)
	}
	late, ok := byCpt["sp1-1800"]
	if !ok || len(late.LineNos) != 1 || late.LineNos[0] != 2 {
		t.Fatalf("late group = %+v, want line 2 only", late)
	}
}

// TestPromiseGroups_PartialShipment_MixedBasis covers a partial-shipment
// order where one line gets a real Capability-basis cutoff and another
// falls back to LeadTime (e.g. its path is not eligible for any window
// in the horizon) — two clean groups, distinguishable by Basis.
func TestPromiseGroups_PartialShipment_MixedBasis(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("oversize", 1))

	window := order.CPTWindow{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}} // oversize NOT eligible

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {window},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			"pick":     1 * time.Hour,
			"oversize": 1 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(48*time.Hour, map[shared.PathId]time.Duration{
			"oversize": 72 * time.Hour,
		}),
		SiteId: "site-1",
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2 (one Capability, one LeadTime): %+v", len(groups), groups)
	}

	var capabilityGroup, leadTimeGroup *order.PromiseGroup
	for i := range groups {
		switch groups[i].Promise.Basis {
		case order.BasisCapability:
			capabilityGroup = &groups[i]
		case order.BasisLeadTime:
			leadTimeGroup = &groups[i]
		}
	}
	if capabilityGroup == nil || leadTimeGroup == nil {
		t.Fatalf("expected one Capability and one LeadTime group, got %+v", groups)
	}
	if len(capabilityGroup.LineNos) != 1 || capabilityGroup.LineNos[0] != 1 {
		t.Fatalf("capability group = %+v, want line 1 only", capabilityGroup)
	}
	if len(leadTimeGroup.LineNos) != 1 || leadTimeGroup.LineNos[0] != 2 {
		t.Fatalf("leadTime group = %+v, want line 2 only", leadTimeGroup)
	}
	wantLeadTimeCutoff := now.Add(72 * time.Hour)
	if !leadTimeGroup.Promise.CutoffAt.Equal(wantLeadTimeCutoff) {
		t.Fatalf("leadTime cutoff = %v, want %v", leadTimeGroup.Promise.CutoffAt, wantLeadTimeCutoff)
	}
}

// TestPromiseGroups_PartialShipment_MultipleFallbackLines is the
// trickiest edge case per the task brief: multiple lines that ALL fall
// back to LeadTime, some landing on the SAME computed instant (grouped
// together) and one landing on a DIFFERENT instant (separate group) —
// proving the grouping-by-identical-result rule also applies correctly
// within the fallback basis, not just within the capability basis.
func TestPromiseGroups_PartialShipment_MultipleFallbackLines(t *testing.T) {
	now := testTime()
	// pick and singles share the SAME configured lead time (12h) so they
	// fall back to the identical instant; multis has its own longer lead
	// time (36h) so it lands on a distinct instant. No Schedule/
	// Capability wired at all -> every line falls back immediately.
	o := newAllocatedOrder(t, pq("pick", 1), pq("singles", 1), pq("multis", 1))

	policy := order.PromisePolicy{
		Fallback: order.NewLeadTimePolicy(12*time.Hour, map[shared.PathId]time.Duration{
			"multis": 36 * time.Hour,
		}),
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2 (pick+singles share one instant, multis has its own): %+v", len(groups), groups)
	}

	var sharedGroup, soloGroup *order.PromiseGroup
	for i := range groups {
		if len(groups[i].LineNos) == 2 {
			sharedGroup = &groups[i]
		} else {
			soloGroup = &groups[i]
		}
	}
	if sharedGroup == nil || soloGroup == nil {
		t.Fatalf("expected one 2-line group and one 1-line group, got %+v", groups)
	}
	wantShared := []int{1, 2}
	if sharedGroup.LineNos[0] != wantShared[0] || sharedGroup.LineNos[1] != wantShared[1] {
		t.Fatalf("shared group LineNos = %v, want %v", sharedGroup.LineNos, wantShared)
	}
	if soloGroup.LineNos[0] != 3 {
		t.Fatalf("solo group LineNos = %v, want [3]", soloGroup.LineNos)
	}
	if !sharedGroup.Promise.CutoffAt.Equal(now.Add(12 * time.Hour)) {
		t.Fatalf("shared group cutoff = %v, want now+12h", sharedGroup.Promise.CutoffAt)
	}
	if !soloGroup.Promise.CutoffAt.Equal(now.Add(36 * time.Hour)) {
		t.Fatalf("solo group cutoff = %v, want now+36h", soloGroup.Promise.CutoffAt)
	}
	for _, g := range groups {
		if g.Promise.Basis != order.BasisLeadTime {
			t.Fatalf("group %+v: Basis = %q, want LeadTime", g, g.Promise.Basis)
		}
	}
}

// TestPromiseGroups_PartialShipment_ScheduleUnknown_FallsBackPerLine
// covers the per-line search path against a Schedule source that IS
// wired but cannot answer for the site (NextCutoffs known=false — a
// cold cache): every line's promiseForLines must fall back to
// LeadTime, never panic, and still group lines that share an instant.
func TestPromiseGroups_PartialShipment_ScheduleUnknown_FallsBackPerLine(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("singles", 1))

	policy := order.PromisePolicy{
		// No entry for "site-1": the schedule adapter is wired but has
		// nothing to say — the cold-cache case at startup.
		Schedule:   &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 1 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(12*time.Hour, nil),
		SiteId:     "site-1",
	}

	groups, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("expected ok=true even with an unknown schedule (fallback)")
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1 (both lines share the fallback instant): %+v", len(groups), groups)
	}
	if len(groups[0].LineNos) != 2 {
		t.Fatalf("LineNos = %v, want [1 2]", groups[0].LineNos)
	}
	if groups[0].Promise.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", groups[0].Promise.Basis)
	}
	if !groups[0].Promise.CutoffAt.Equal(now.Add(12 * time.Hour)) {
		t.Fatalf("cutoff = %v, want now+12h", groups[0].Promise.CutoffAt)
	}
}
