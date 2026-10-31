package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR 0047: an order is promised against its OWN caller-assigned
// fulfillment site (ADR 0044) when it carries one, else DefaultSiteId.
// Every fixture below gives the default site and SIM1 DIFFERENT schedules
// so a test that read the wrong site fails, rather than passing by luck.

// recordingSchedule is a fakeSchedule that also records every site asked.
type recordingSchedule struct {
	fakeSchedule
	asked []string
}

func (r *recordingSchedule) NextCutoffs(siteId string, from time.Time, n int) ([]order.CPTWindow, bool) {
	r.asked = append(r.asked, siteId)
	return r.fakeSchedule.NextCutoffs(siteId, from, n)
}

func twoSiteSchedule(now time.Time) *recordingSchedule {
	return &recordingSchedule{fakeSchedule: fakeSchedule{windowsBySite: map[string][]order.CPTWindow{
		"site-1": {{CptId: "site1-early", CutoffAt: now.Add(2 * time.Hour), EligiblePathIds: []string{"pick"}}},
		"SIM1":   {{CptId: "sim1-late", CutoffAt: now.Add(5 * time.Hour), EligiblePathIds: []string{"pick"}}},
	}}}
}

func twoSitePolicy(schedule order.ScheduleSource) order.PromisePolicy {
	return order.PromisePolicy{
		Schedule:      schedule,
		Capability:    &fakeCapability{cycleTimes: map[shared.PathId]time.Duration{"pick": 30 * time.Minute}},
		Fallback:      order.NewLeadTimePolicy(48*time.Hour, nil),
		DefaultSiteId: "site-1",
	}
}

// sitedOrder is an allocated single-line "pick" order carrying site (nil
// = none), ship-complete unless partial is true.
func sitedOrder(site *shared.FulfillmentSiteID, partial bool, lines ...int) *order.Order {
	if len(lines) == 0 {
		lines = []int{1}
	}
	var out []*order.OrderLine
	for _, n := range lines {
		out = append(out, order.RehydrateOrderLine(n, "SKU-1", 1, "pick", false, order.LineAllocated, nil))
	}
	return order.Rehydrate(order.OrderSnapshot{ID: "ord-site", Lines: out, AllowPartialShipment: partial, FulfillmentSiteID: site})
}

func siteID(code string) *shared.FulfillmentSiteID {
	s := shared.FulfillmentSiteID(code)
	return &s
}

func TestPromiseSiteFor_OrderSiteWinsOverDefault(t *testing.T) {
	if got := order.PromiseSiteFor(sitedOrder(siteID("SIM1"), false), "site-1"); got != "SIM1" {
		t.Fatalf("PromiseSiteFor(order at SIM1) = %q, want SIM1", got)
	}
}

func TestPromiseSiteFor_NoOrderSite_IsTheDefault(t *testing.T) {
	if got := order.PromiseSiteFor(sitedOrder(nil, false), "site-1"); got != "site-1" {
		t.Fatalf("PromiseSiteFor(order without site) = %q, want site-1", got)
	}
}

func TestPromise_OrderWithSite_IsPromisedAgainstItsOwnSiteSchedule(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	schedule := twoSiteSchedule(now)

	got, ok := twoSitePolicy(schedule).Promise(now, sitedOrder(siteID("SIM1"), false))
	if !ok || got.Basis != order.BasisCapability || got.CptId != "sim1-late" || !got.CutoffAt.Equal(now.Add(5*time.Hour)) {
		t.Fatalf("Promise = %+v, %v; want SIM1's sim1-late window on BasisCapability", got, ok)
	}
	if len(schedule.asked) != 1 || schedule.asked[0] != "SIM1" {
		t.Fatalf("schedule asked for %v, want exactly [SIM1]", schedule.asked)
	}
}

func TestPromise_OrderWithoutSite_IsPromisedAgainstTheDefaultSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	schedule := twoSiteSchedule(now)

	got, ok := twoSitePolicy(schedule).Promise(now, sitedOrder(nil, false))
	if !ok || got.CptId != "site1-early" {
		t.Fatalf("Promise = %+v, %v; want the default site's site1-early window", got, ok)
	}
	if len(schedule.asked) != 1 || schedule.asked[0] != "site-1" {
		t.Fatalf("schedule asked for %v, want exactly [site-1]", schedule.asked)
	}
}

// An order's site with no schedule yet must NOT borrow the default site's
// schedule: it falls back to LeadTimePolicy, exactly like a cold cache.
func TestPromise_OrderSiteWithoutSchedule_FallsBackNeverBorrowsTheDefaultSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	got, ok := twoSitePolicy(twoSiteSchedule(now)).Promise(now, sitedOrder(siteID("WH9"), false))
	if !ok || got.Basis != order.BasisLeadTime || got.CptId != "" {
		t.Fatalf("Promise = %+v, %v; want a LeadTime fallback, not another site's window", got, ok)
	}
}

func TestPromiseGroups_ShipComplete_UsesTheOrderSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	groups, ok := twoSitePolicy(twoSiteSchedule(now)).PromiseGroups(now, sitedOrder(siteID("SIM1"), false, 1, 2))
	if !ok || len(groups) != 1 || groups[0].Promise.CptId != "sim1-late" || len(groups[0].LineNos) != 2 {
		t.Fatalf("PromiseGroups = %+v, %v; want one SIM1 group of both lines", groups, ok)
	}
}

func TestPromiseGroups_PartialShipment_EveryLineUsesTheOrderSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	schedule := twoSiteSchedule(now)

	groups, ok := twoSitePolicy(schedule).PromiseGroups(now, sitedOrder(siteID("SIM1"), true, 1, 2))
	if !ok || len(groups) != 1 || groups[0].Promise.CptId != "sim1-late" {
		t.Fatalf("PromiseGroups = %+v, %v; want every line on SIM1's window", groups, ok)
	}
	for _, s := range schedule.asked {
		if s != "SIM1" {
			t.Fatalf("schedule asked for %v; every per-line lookup must use SIM1", schedule.asked)
		}
	}
}

func TestPromiseGroups_NoOrderSite_UsesTheDefaultSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	groups, ok := twoSitePolicy(twoSiteSchedule(now)).PromiseGroups(now, sitedOrder(nil, true, 1))
	if !ok || len(groups) != 1 || groups[0].Promise.CptId != "site1-early" {
		t.Fatalf("PromiseGroups = %+v, %v; want the default site's window", groups, ok)
	}
}

// FeasibleBy: the default site has a window before the deadline, SIM1 does
// not. An order at SIM1 must be infeasible — it must never commit to a
// departure at a building it is not fulfilled from.
func TestFeasibleBy_OrderSite_IsJudgedOnItsOwnScheduleOnly(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	policy := twoSitePolicy(twoSiteSchedule(now))
	deadline := now.Add(3 * time.Hour)

	if got, ok := policy.FeasibleBy(now, sitedOrder(siteID("SIM1"), false), deadline); ok {
		t.Fatalf("FeasibleBy(SIM1 order) = %+v, true; SIM1's only window is after the deadline", got)
	}
	got, ok := policy.FeasibleBy(now, sitedOrder(nil, false), deadline)
	if !ok || got.CptId != "site1-early" || got.Basis != order.BasisNetwork {
		t.Fatalf("FeasibleBy(order without site) = %+v, %v; want site1-early on BasisNetwork", got, ok)
	}
}

func TestFeasibleBy_OrderSite_FindsItsOwnWindow(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)

	got, ok := twoSitePolicy(twoSiteSchedule(now)).FeasibleBy(now, sitedOrder(siteID("SIM1"), false), now.Add(6*time.Hour))
	if !ok || got.CptId != "sim1-late" {
		t.Fatalf("FeasibleBy(SIM1 order) = %+v, %v; want sim1-late", got, ok)
	}
}

// A re-promise recomputes from the persisted order, so it must land on the
// same site the order was first promised at — even after that site's
// schedule changed and the default site's did not.
func TestPromiseGroups_Repromise_KeepsTheOrderSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	schedule := twoSiteSchedule(now)
	policy := twoSitePolicy(schedule)
	o := sitedOrder(siteID("SIM1"), false)

	first, ok := policy.PromiseGroups(now, o)
	if !ok {
		t.Fatal("first PromiseGroups not ok")
	}
	o.SetPromiseGroups(first)

	schedule.windowsBySite["SIM1"] = []order.CPTWindow{{CptId: "sim1-next", CutoffAt: now.Add(7 * time.Hour), EligiblePathIds: []string{"pick"}}}
	reloaded := order.Rehydrate(order.OrderSnapshot{ID: o.ID(), Lines: o.Lines(), PromiseGroups: o.PromiseGroups(), FulfillmentSiteID: o.FulfillmentSiteID()})

	again, ok := policy.PromiseGroups(now.Add(time.Hour), reloaded)
	if !ok || len(again) != 1 || again[0].Promise.CptId != "sim1-next" {
		t.Fatalf("re-promise = %+v, %v; want SIM1's new sim1-next, never site-1's site1-early", again, ok)
	}
}

// Partial shipment with one line that cannot make any window: that line
// falls back to LeadTime, and the other line is still promised against the
// order's own site.
func TestPromiseGroups_PartialShipment_MixedFallbackStillUsesTheOrderSite(t *testing.T) {
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	schedule := twoSiteSchedule(now)
	policy := twoSitePolicy(schedule)
	lines := []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LineAllocated, nil),
		order.RehydrateOrderLine(2, "SKU-2", 1, "slow", false, order.LineAllocated, nil),
	}
	o := order.Rehydrate(order.OrderSnapshot{ID: "ord-mixed", Lines: lines, AllowPartialShipment: true, FulfillmentSiteID: siteID("SIM1")})

	groups, ok := policy.PromiseGroups(now, o)
	if !ok || len(groups) != 2 {
		t.Fatalf("PromiseGroups = %+v, %v; want two groups", groups, ok)
	}
	if groups[0].Promise.CptId != "sim1-late" || groups[1].Promise.Basis != order.BasisLeadTime {
		t.Fatalf("groups = %+v; want line 1 on SIM1's window and line 2 on the LeadTime fallback", groups)
	}
}
