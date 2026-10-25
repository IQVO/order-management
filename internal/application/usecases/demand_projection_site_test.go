package usecases_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR 0044: an order may carry a caller-assigned fulfillment site, which
// wins over the static DemandProjectionPolicy site and is projected under
// the order-site-v1 assignment version.

func siteID(t *testing.T, code string) *shared.FulfillmentSiteID {
	t.Helper()
	s, err := shared.NewFulfillmentSiteID(code)
	if err != nil {
		t.Fatalf("NewFulfillmentSiteID(%q): %v", code, err)
	}
	return &s
}

func receiveWithSite(t *testing.T, f *fixture, policy usecases.DemandProjectionPolicy, site *shared.FulfillmentSiteID, release bool, lines ...usecases.NewLine) *order.Order {
	t.Helper()
	uc := f.receiveOrder()
	uc.DemandProjection = policy
	o, err := uc.ExecuteWithSite(context.Background(), lines, false, release, nil, site)
	if err != nil {
		t.Fatalf("ExecuteWithSite: %v", err)
	}
	return o
}

func assertDemandSite(t *testing.T, d shared.SiteSkuDemandChanged, wantSite, wantVersion string) {
	t.Helper()
	if d.SiteID != wantSite || d.AssignmentVersion != wantVersion {
		t.Fatalf("demand site/version = %q/%q, want %q/%q", d.SiteID, d.AssignmentVersion, wantSite, wantVersion)
	}
}

func TestOrderSite_WinsOverTheStaticPolicy(t *testing.T) {
	f := newFixture()
	o := receiveWithSite(t, f, demandPolicy(), siteID(t, "WH2"), true, line("SKU-1", 2, "pick"))

	assertDemandSite(t, findSiteSkuDemandChanged(t, f.events), "WH2", usecases.OrderSiteDemandAssignmentVersion)
	if got := o.FulfillmentSiteID(); got == nil || *got != "WH2" {
		t.Fatalf("FulfillmentSiteID() = %v, want WH2", got)
	}
}

func TestOrderSite_ProjectsEvenWithNoStaticPolicyConfigured(t *testing.T) {
	f := newFixture()
	receiveWithSite(t, f, usecases.DemandProjectionPolicy{}, siteID(t, "WH2"), true, line("SKU-1", 2, "pick"))

	assertDemandSite(t, findSiteSkuDemandChanged(t, f.events), "WH2", "order-site-v1")
}

func TestOrderSite_AbsentFallsBackToTheStaticPolicy(t *testing.T) {
	f := newFixture()
	receiveWithSite(t, f, demandPolicy(), nil, true, line("SKU-1", 2, "pick"))

	assertDemandSite(t, findSiteSkuDemandChanged(t, f.events), "SIM1", "static-site-v1")
}

func TestOrderSite_NeitherSiteNorPolicyEmitsNothing(t *testing.T) {
	f := newFixture()
	receiveWithSite(t, f, usecases.DemandProjectionPolicy{}, nil, true, line("SKU-1", 2, "pick"))

	if got := findAllSiteSkuDemandChanged(t, f.events); len(got) != 0 {
		t.Fatalf("SiteSkuDemandChanged = %+v, want none", got)
	}
}

func TestOrderSite_AssignmentVersionsAreTheDocumentedValues(t *testing.T) {
	if usecases.OrderSiteDemandAssignmentVersion != "order-site-v1" || usecases.StaticDemandAssignmentVersion != "static-site-v1" {
		t.Fatalf("versions = %q / %q", usecases.OrderSiteDemandAssignmentVersion, usecases.StaticDemandAssignmentVersion)
	}
}

func TestOrderSite_HeldNetworkOrderProjectsToItsSiteAndKeepsItOnRelease(t *testing.T) {
	f := newFixture()
	o := receiveWithSite(t, f, demandPolicy(), siteID(t, "WH2"), false, line("SKU-1", 2, "pick"))
	assertDemandSite(t, findSiteSkuDemandChanged(t, f.events), "WH2", "order-site-v1")

	release := &usecases.ReleaseHeldOrder{Orders: f.orders, Inventory: f.inventory, Events: f.events, Clock: f.clock, Promise: f.promise}
	if _, err := release.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("ReleaseHeldOrder: %v", err)
	}
	assertStoredSite(t, f, o.ID(), "WH2")
}

func assertStoredSite(t *testing.T, f *fixture, id shared.OrderId, want string) {
	t.Helper()
	stored, err := f.orders.FindByID(context.Background(), id)
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	got := stored.FulfillmentSiteID()
	if want == "" {
		if got != nil {
			t.Fatalf("stored site = %q, want none", *got)
		}
		return
	}
	if got == nil || got.String() != want {
		t.Fatalf("stored site = %v, want %q", got, want)
	}
}

func TestOrderSite_RetryAllocationKeepsTheSiteAndProjectsToIt(t *testing.T) {
	f := newFixture()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o := receiveWithSite(t, f, demandPolicy(), siteID(t, "WH2"), true, line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"))
	f.events.reset()

	delete(f.inventory.reserveErrBySKU, "SKU-2")
	retry := f.retryAllocation()
	retry.DemandProjection = demandPolicy()
	if _, err := retry.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("RetryAllocation: %v", err)
	}

	demands := findAllSiteSkuDemandChanged(t, f.events)
	if len(demands) != 1 {
		t.Fatalf("demands = %+v, want one", demands)
	}
	assertDemandSite(t, demands[0], "WH2", "order-site-v1")
	assertStoredSite(t, f, o.ID(), "WH2")
}

func TestOrderSite_CancelRemovesDemandAtTheSameSiteAndKeepsIt(t *testing.T) {
	f := newFixture()
	o := receiveWithSite(t, f, demandPolicy(), siteID(t, "WH2"), false, line("SKU-1", 2, "pick"))
	f.events.reset()

	cancel := f.cancelOrder()
	cancel.DemandProjection = demandPolicy()
	if _, err := cancel.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	d := findSiteSkuDemandChanged(t, f.events)
	assertDemandSite(t, d, "WH2", "order-site-v1")
	if d.State != shared.SiteSkuDemandRemoved {
		t.Fatalf("state = %q, want REMOVED", d.State)
	}
	assertStoredSite(t, f, o.ID(), "WH2")
}

func TestOrderSite_RepromiseProjectsToTheSiteAndKeepsIt(t *testing.T) {
	rf := newRepromiseFixture()
	o := receiveWithSite(t, rf.f, usecases.DemandProjectionPolicy{}, siteID(t, "WH2"), true, line("SKU-1", 1, "pick"))
	rf.f.events.reset()

	moved := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	uc := rf.repromiseOrder(moved)
	uc.DemandProjection = demandPolicy()
	if err := uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	d := findSiteSkuDemandChanged(t, rf.f.events)
	assertDemandSite(t, d, "WH2", "order-site-v1")
	assertStoredSite(t, rf.f, o.ID(), "WH2")
}

func TestOrderWithoutSite_StaysWithoutOneThroughTheLifecycle(t *testing.T) {
	f := newFixture()
	o := receiveWithSite(t, f, demandPolicy(), nil, false, line("SKU-1", 2, "pick"))
	cancel := f.cancelOrder()
	cancel.DemandProjection = demandPolicy()
	if _, err := cancel.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	assertStoredSite(t, f, o.ID(), "")
}

// The frozen OrderAllocated / OrderPartiallyAllocated / OrderRepromised
// contracts (shared with wes-work-planning) must not change by a byte
// because an order carries a site: every non-demand event is DeepEqual
// to the one an identical order WITHOUT a site produces.
func TestOrderSite_DoesNotChangeAnyFrozenEvent(t *testing.T) {
	run := func(site *shared.FulfillmentSiteID) []shared.DomainEvent {
		f := newFixture()
		receiveWithSite(t, f, usecases.DemandProjectionPolicy{}, site, true, line("SKU-1", 2, "pick"), line("SKU-2", 1, "singles"))
		f.events.mu.Lock()
		defer f.events.mu.Unlock()
		var out []shared.DomainEvent
		for _, e := range f.events.events {
			if _, isDemand := e.(shared.SiteSkuDemandChanged); !isDemand {
				out = append(out, e)
			}
		}
		return out
	}
	with, without := run(siteID(t, "WH2")), run(nil)
	if len(with) == 0 || !reflect.DeepEqual(with, without) {
		t.Fatalf("non-demand events differ with a site:\nwith:    %+v\nwithout: %+v", with, without)
	}
}
