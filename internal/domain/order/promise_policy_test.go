package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// fakeSchedule is a scripted order.ScheduleSource.
type fakeSchedule struct {
	windowsBySite map[string][]order.CPTWindow
}

func (f *fakeSchedule) NextCutoffs(siteId string, _ time.Time, n int) ([]order.CPTWindow, bool) {
	windows, ok := f.windowsBySite[siteId]
	if !ok {
		return nil, false
	}
	if len(windows) > n {
		windows = windows[:n]
	}
	return windows, true
}

// fakeCapability is a scripted order.CapabilitySource.
type fakeCapability struct {
	cycleTimes map[shared.PathId]time.Duration
}

func (f *fakeCapability) CycleTimeP95(pathID shared.PathId) (time.Duration, bool) {
	d, ok := f.cycleTimes[pathID]
	return d, ok
}

// fakeCapacity is a scripted order.CapacitySource.
type fakeCapacity struct {
	remaining map[string]int // keyed by pathID.String()+"|"+cptId
	known     map[string]bool
}

func capacityKey(pathID shared.PathId, cptId string) string {
	return pathID.String() + "|" + cptId
}

func (f *fakeCapacity) Remaining(pathID shared.PathId, cptId string, _ time.Time) (int, bool) {
	key := capacityKey(pathID, cptId)
	if f.known == nil || !f.known[key] {
		return 0, false
	}
	return f.remaining[key], true
}

func newAllocatedOrder(t *testing.T, pathsAndQty ...struct {
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
	return order.Rehydrate("ord-1", lines, true, nil, nil, nil)
}

func pq(path shared.PathId, qty int) struct {
	path shared.PathId
	qty  int
} {
	return struct {
		path shared.PathId
		qty  int
	}{path: path, qty: qty}
}

func TestPromisePolicy_NoAllocatedLines_ReturnsNotOK(t *testing.T) {
	o := order.Rehydrate("ord-1", []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LinePending, nil),
	}, true, nil, nil, nil)

	policy := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}
	_, ok := policy.Promise(testTime(), o)
	if ok {
		t.Fatal("expected ok=false when no line is allocated")
	}
}

func TestPromisePolicy_CapabilityBasisSuccess(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 2))

	cutoff := now.Add(6 * time.Hour)
	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"pick"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 2 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Basis != order.BasisCapability {
		t.Fatalf("Basis = %q, want Capability", got.Basis)
	}
	if got.CptId != "sp1-1800" {
		t.Fatalf("CptId = %q, want sp1-1800", got.CptId)
	}
	if !got.CutoffAt.Equal(cutoff) {
		t.Fatalf("CutoffAt = %v, want %v", got.CutoffAt, cutoff)
	}
}

func TestPromisePolicy_FallsBackWhenNoScheduleForSite(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	policy := order.PromisePolicy{
		Schedule:   &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{}}, // no site-1 entry
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 2 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
	if got.CptId != "" {
		t.Fatalf("CptId = %q, want empty for a LeadTime-basis promise", got.CptId)
	}
	want := now.Add(24 * time.Hour)
	if !got.CutoffAt.Equal(want) {
		t.Fatalf("CutoffAt = %v, want %v", got.CutoffAt, want)
	}
}

func TestPromisePolicy_FallsBackWhenCycleTimeUnknownForPath(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		}},
		// "pick" is not in cycleTimes -> unknown.
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
}

func TestPromisePolicy_FallsBackWhenNoWindowFitsInHorizon(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	// The only window's cutoff is BEFORE now + cycleTime, so the line
	// cannot make it.
	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(1 * time.Hour), EligiblePathIds: []string{"pick"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 3 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
}

func TestPromisePolicy_FallsBackWhenPathNotEligibleForWindow(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			// The window is only eligible for "singles", not "pick".
			"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"singles"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 1 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
}

// TestPromisePolicy_MultipleLinesDifferentPaths_SameWindowGoverns is the
// "every line's path must fit the SAME window" spirit test: two lines on
// different paths, only one shared window fits both.
func TestPromisePolicy_MultipleLinesDifferentPaths_SameWindowGoverns(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("singles", 1))

	earlyWindow := order.CPTWindow{CptId: "sp1-1200", CutoffAt: now.Add(2 * time.Hour), EligiblePathIds: []string{"pick", "singles"}}
	// pick's cycle time (3h) does not fit the early window (2h out),
	// but does fit the later one (8h out); singles fits both.
	lateWindow := order.CPTWindow{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick", "singles"}}

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {earlyWindow, lateWindow},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			"pick":    3 * time.Hour,
			"singles": 1 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Basis != order.BasisCapability {
		t.Fatalf("Basis = %q, want Capability", got.Basis)
	}
	if got.CptId != "sp1-1800" {
		t.Fatalf("CptId = %q, want sp1-1800 (the only window both lines can make)", got.CptId)
	}
}

// TestPromisePolicy_MultipleLines_OneNeverFits_FallsBack proves that
// EVERY allocated line must fit the same window — if even one line
// cannot make ANY window in the horizon, the whole promise falls back.
func TestPromisePolicy_MultipleLines_OneNeverFits_FallsBack(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1), pq("oversize", 1))

	window := order.CPTWindow{CptId: "sp1-1800", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}} // oversize NOT eligible

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {window},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{
			"pick":     1 * time.Hour,
			"oversize": 1 * time.Hour,
		}},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
}

func TestPromisePolicy_CapacityUnknown_DoesNotBlockCapabilityBasis(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 100))

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 1 * time.Hour}},
		// No Capacity port wired at all (nil) -- condition (c) always
		// passes per ADR 0014, since today capacity is always unknown.
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Basis != order.BasisCapability {
		t.Fatalf("Basis = %q, want Capability even with unknown capacity", got.Basis)
	}
}

func TestPromisePolicy_CapacityKnownAndInsufficient_FallsBackFromThatWindow(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 50))

	earlyWindow := order.CPTWindow{CptId: "early", CutoffAt: now.Add(4 * time.Hour), EligiblePathIds: []string{"pick"}}
	lateWindow := order.CPTWindow{CptId: "late", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}}

	policy := order.PromisePolicy{
		Schedule: &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
			"site-1": {earlyWindow, lateWindow},
		}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 1 * time.Hour}},
		Capacity: &fakeCapacity{
			known: map[string]bool{
				capacityKey("pick", "early"): true,
				capacityKey("pick", "late"):  true,
			},
			remaining: map[string]int{
				capacityKey("pick", "early"): 10, // insufficient for qty 50
				capacityKey("pick", "late"):  100,
			},
		},
		Fallback: order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:   "site-1",
	}

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.CptId != "late" {
		t.Fatalf("CptId = %q, want late (early lacked capacity)", got.CptId)
	}
}

func TestPromisePolicy_ZeroValuePolicy_AlwaysFallsBack(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	var policy order.PromisePolicy
	policy.Fallback = order.NewLeadTimePolicy(12*time.Hour, nil)

	got, ok := policy.Promise(now, o)
	if !ok {
		t.Fatal("expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime", got.Basis)
	}
}

func TestPromiseBasisString(t *testing.T) {
	if order.BasisCapability.String() != "Capability" {
		t.Fatalf("BasisCapability.String() = %q", order.BasisCapability.String())
	}
	if order.BasisLeadTime.String() != "LeadTime" {
		t.Fatalf("BasisLeadTime.String() = %q", order.BasisLeadTime.String())
	}
}

// horizonRecorder wraps a fakeSchedule and records the n each
// NextCutoffs call was made with, so a test can assert exactly how far
// out PromisePolicy looked.
type horizonRecorder struct {
	inner fakeSchedule
	calls []int
}

func (h *horizonRecorder) NextCutoffs(siteId string, from time.Time, n int) ([]order.CPTWindow, bool) {
	h.calls = append(h.calls, n)
	return h.inner.NextCutoffs(siteId, from, n)
}

// TestPromisePolicy_HorizonBoundsNextCutoffsQuery pins horizon()'s two
// branches: an unset (zero) Horizon queries DefaultCPTHorizon windows,
// and an explicitly configured Horizon is passed through verbatim to
// ScheduleSource.NextCutoffs.
func TestPromisePolicy_HorizonBoundsNextCutoffsQuery(t *testing.T) {
	tests := []struct {
		name    string
		horizon int
		wantN   int
	}{
		{name: "zero horizon queries the default", horizon: 0, wantN: order.DefaultCPTHorizon},
		{name: "negative horizon falls back to the default too", horizon: -3, wantN: order.DefaultCPTHorizon},
		{name: "explicit horizon is passed through", horizon: 3, wantN: 3},
		{name: "horizon of one queries exactly one window", horizon: 1, wantN: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := testTime()
			o := newAllocatedOrder(t, pq("pick", 1))
			schedule := &horizonRecorder{inner: fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
				"site-1": {{CptId: "sp1-1800", CutoffAt: now.Add(6 * time.Hour), EligiblePathIds: []string{"pick"}}},
			}}}
			policy := order.PromisePolicy{
				Schedule:   schedule,
				Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 1 * time.Hour}},
				Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
				SiteId:     "site-1",
				Horizon:    tt.horizon,
			}

			if _, ok := policy.Promise(now, o); !ok {
				t.Fatal("Promise: expected ok=true")
			}
			if len(schedule.calls) != 1 {
				t.Fatalf("NextCutoffs called %d times, want 1", len(schedule.calls))
			}
			if got := schedule.calls[0]; got != tt.wantN {
				t.Fatalf("NextCutoffs n = %d, want %d", got, tt.wantN)
			}
		})
	}
}

// TestPromisePolicy_HorizonChangesTheAnswer proves the bound is not
// cosmetic: with the only window the line can make sitting BEYOND a
// short horizon, the promise falls back to LeadTime; widening the
// horizon to include that window yields the Capability-basis promise.
func TestPromisePolicy_HorizonChangesTheAnswer(t *testing.T) {
	now := testTime()
	o := newAllocatedOrder(t, pq("pick", 1))

	// Window 1 does not fit (cutoff before now+cycleTime); window 2 fits.
	windows := []order.CPTWindow{
		{CptId: "too-early", CutoffAt: now.Add(1 * time.Hour), EligiblePathIds: []string{"pick"}},
		{CptId: "fits", CutoffAt: now.Add(8 * time.Hour), EligiblePathIds: []string{"pick"}},
	}
	base := order.PromisePolicy{
		Schedule:   &fakeSchedule{windowsBySite: map[string][]order.CPTWindow{"site-1": windows}},
		Capability: &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 3 * time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}

	short := base
	short.Horizon = 1
	got, ok := short.Promise(now, o)
	if !ok {
		t.Fatal("Promise: expected ok=true (fallback)")
	}
	if got.Basis != order.BasisLeadTime {
		t.Fatalf("Basis = %q, want LeadTime — the only fitting window is beyond the horizon", got.Basis)
	}

	wide := base
	wide.Horizon = 2
	got, ok = wide.Promise(now, o)
	if !ok {
		t.Fatal("Promise: expected ok=true")
	}
	if got.Basis != order.BasisCapability || got.CptId != "fits" {
		t.Fatalf("promise = %+v, want Capability basis on window %q", got, "fits")
	}
}
