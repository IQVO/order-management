package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR 0047 at the use-case level: intake, re-promise and the planned-
// capacity annotation all read the order's own caller-assigned site, and
// fall back to the configured default only for an order without one.

// siteSchedule is a ScheduleSource keyed by site, recording each site asked.
type siteSchedule struct {
	bySite map[string][]order.CPTWindow
	asked  []string
}

func (s *siteSchedule) NextCutoffs(siteId string, _ time.Time, _ int) ([]order.CPTWindow, bool) {
	s.asked = append(s.asked, siteId)
	w, ok := s.bySite[siteId]
	return w, ok && len(w) > 0
}

func newSiteSchedule() *siteSchedule {
	return &siteSchedule{bySite: map[string][]order.CPTWindow{
		"site-1": {{CptId: "site1-1100", CutoffAt: now().Add(2 * time.Hour), EligiblePathIds: []string{"singles"}}},
		"SIM1":   {{CptId: "sim1-1400", CutoffAt: now().Add(5 * time.Hour), EligiblePathIds: []string{"singles"}}},
	}}
}

func sitePolicy(schedule *siteSchedule) order.PromisePolicy {
	return order.PromisePolicy{
		Schedule:      schedule,
		Capability:    &deadlineCapability{cycleTimes: map[shared.PathId]time.Duration{"singles": 30 * time.Minute}},
		Fallback:      order.NewLeadTimePolicy(24*time.Hour, nil),
		DefaultSiteId: "site-1",
	}
}

func fulfillmentSite(code string) *shared.FulfillmentSiteID {
	s := shared.FulfillmentSiteID(code)
	return &s
}

func receiveAtSite(t *testing.T, f *fixture, site *shared.FulfillmentSiteID, deadline *time.Time) *order.Order {
	t.Helper()
	o, err := f.receiveOrder().ExecuteWithSite(context.Background(), []usecases.NewLine{deadlineLine()}, false, true, deadline, site)
	if err != nil {
		t.Fatalf("ExecuteWithSite: %v", err)
	}
	return o
}

func TestReceiveOrder_WithSite_IsPromisedAtItsOwnSite(t *testing.T) {
	f := newFixture()
	schedule := newSiteSchedule()
	f.promise = sitePolicy(schedule)

	o := receiveAtSite(t, f, fulfillmentSite("SIM1"), nil)
	if c := o.PromiseCptId(); c == nil || *c != "sim1-1400" {
		t.Fatalf("PromiseCptId = %v, want SIM1's sim1-1400", c)
	}
	for _, s := range schedule.asked {
		if s != "SIM1" {
			t.Fatalf("schedule asked for %v; an order at SIM1 must never read another site", schedule.asked)
		}
	}
}

func TestReceiveOrder_WithoutSite_IsPromisedAtTheDefaultSite(t *testing.T) {
	f := newFixture()
	f.promise = sitePolicy(newSiteSchedule())

	o := receiveAtSite(t, f, nil, nil)
	if c := o.PromiseCptId(); c == nil || *c != "site1-1100" {
		t.Fatalf("PromiseCptId = %v, want the default site's site1-1100", c)
	}
}

func TestReceiveOrder_WithSiteAndDeadline_FeasibilityIsJudgedAtItsOwnSite(t *testing.T) {
	f := newFixture()
	f.promise = sitePolicy(newSiteSchedule())
	deadline := now().Add(3 * time.Hour) // site-1's window fits, SIM1's does not

	o := receiveAtSite(t, f, fulfillmentSite("SIM1"), &deadline)
	if st := o.Lines()[0].Status(); st != order.LineAllocated && st != order.LineReleased {
		t.Fatalf("line status = %q; the fixture must allocate so the promise is actually attempted", st)
	}
	if d := o.PromiseDate(); d != nil {
		t.Fatalf("PromiseDate = %v; SIM1 cannot make the deadline, so there must be no promise", d)
	}
}

func TestRepromiseOrder_KeepsTheOrderSite(t *testing.T) {
	rf := newRepromiseFixture()
	schedule := newSiteSchedule()
	rf.f.promise = sitePolicy(schedule)
	o := receiveAtSite(t, rf.f, fulfillmentSite("SIM1"), nil)
	rf.f.events.reset()

	// SIM1's schedule moves; site-1's stays. The re-promise must follow
	// SIM1, the site the order was received for.
	schedule.bySite["SIM1"] = []order.CPTWindow{{CptId: "sim1-1700", CutoffAt: now().Add(8 * time.Hour), EligiblePathIds: []string{"singles"}}}
	schedule.asked = nil

	err := rf.repromiseOrder(sitePolicy(schedule)).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-site-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	repromised := findOrderRepromised(t, rf.f.events)
	if repromised.CptIdOld != "sim1-1400" || repromised.CptIdNew != "sim1-1700" {
		t.Fatalf("OrderRepromised %q -> %q, want sim1-1400 -> sim1-1700", repromised.CptIdOld, repromised.CptIdNew)
	}
	for _, s := range schedule.asked {
		if s != "SIM1" {
			t.Fatalf("re-promise asked for %v; it must stay on SIM1", schedule.asked)
		}
	}
}

func TestRepromiseOrder_WithoutSite_StaysOnTheDefaultSite(t *testing.T) {
	rf := newRepromiseFixture()
	schedule := newSiteSchedule()
	rf.f.promise = sitePolicy(schedule)
	o := receiveAtSite(t, rf.f, nil, nil)
	rf.f.events.reset()

	// Only SIM1 moves: an order without a site is not affected.
	schedule.bySite["SIM1"] = []order.CPTWindow{{CptId: "sim1-0900", CutoffAt: now().Add(time.Hour), EligiblePathIds: []string{"singles"}}}

	err := rf.repromiseOrder(sitePolicy(schedule)).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-site-2", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertEventNames(t, rf.f.events) // promise unchanged at site-1: nothing published
}

func TestOrderCapacityConstraints_OrderSiteWinsOverTheDefault(t *testing.T) {
	s := newPCTxStore()
	s.windows["plan-0b7a"] = pcWindow() // location SIM1
	uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), DefaultSiteID: "SIM2"}
	o := pcPromisedOrder(t, pcEnd.Add(time.Hour))
	if err := o.AssignFulfillmentSite("SIM1"); err != nil {
		t.Fatalf("AssignFulfillmentSite: %v", err)
	}

	got, err := uc.For(context.Background(), o)
	if err != nil || len(got) != 1 {
		t.Fatalf("For = %v, %v; want the SIM1 shortage for an order at SIM1", got, err)
	}
	if s.listed.location != "SIM1" || uc.SiteFor(o) != "SIM1" {
		t.Fatalf("matched location %q / SiteFor %q, want SIM1", s.listed.location, uc.SiteFor(o))
	}
}

func TestOrderCapacityConstraints_SiteFor_WithoutOrderSiteIsTheDefault(t *testing.T) {
	uc := &usecases.OrderCapacityConstraints{DefaultSiteID: "SIM2"}
	if got := uc.SiteFor(pcPromisedOrder(t, pcEnd)); got != "SIM2" {
		t.Fatalf("SiteFor = %q, want the default SIM2", got)
	}
}
