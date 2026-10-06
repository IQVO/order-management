package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// demandPolicy is the versioned static demand-site scope every test here
// enables: one explicitly configured site, the Phase-1 static assignment
// version.
func demandPolicy() usecases.DemandProjectionPolicy {
	return usecases.DemandProjectionPolicy{SiteID: "SIM1", AssignmentVersion: usecases.StaticDemandAssignmentVersion}
}

func TestReceiveOrder_ProjectsActiveSiteSkuDemandInTheAllocationTransaction(t *testing.T) {
	f := newFixture()
	uc := f.receiveOrder()
	uc.DemandProjection = demandPolicy()

	o, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 2, "pick")}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	demand := findSiteSkuDemandChanged(t, f.events)
	if demand.SourceOrderID != o.ID() || demand.LineNo != 1 || demand.SiteID != "SIM1" || demand.SKU != "SKU-1" || demand.DemandedUnits != 2 {
		t.Fatalf("demand = %+v", demand)
	}
	if demand.State != shared.SiteSkuDemandActive || demand.AssignmentVersion != usecases.StaticDemandAssignmentVersion {
		t.Fatalf("demand state/version = %q/%q", demand.State, demand.AssignmentVersion)
	}
	if want := now().Add(24 * time.Hour); !demand.DueAt.Equal(want) {
		t.Fatalf("due_at = %s, want %s", demand.DueAt, want)
	}

	// ADR 0034 (develop) appends the release facts after the allocation
	// events when the pass releases every line.
	assertEventNames(t, f.events, "OrderReceived", "OrderLineAllocated", "SiteSkuDemandChanged", "OrderAllocated", "OrderLineReleased", "OrderReleased")
}

func TestReceiveOrder_WithoutDemandProjection_PublishesNoSiteSkuDemand(t *testing.T) {
	f := newFixture()
	if _, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{line("SKU-1", 2, "pick")}, false); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, e := range f.events.names() {
		if e == "SiteSkuDemandChanged" {
			t.Fatalf("SiteSkuDemandChanged published with the projection disabled: %v", f.events.names())
		}
	}
}

func TestReceiveOrder_DemandCarriesTheLinesOwnGroupCutoff(t *testing.T) {
	// Two lines on paths with DIFFERENT lead times (6h vs 24h) on a
	// partial-shipment order: ADR 0017 gives each line its own promise
	// group, and the projection must carry the LINE's cutoff as due_at,
	// not the order-level latest cutoff.
	f := newFixture()
	uc := f.receiveOrder()
	uc.DemandProjection = demandPolicy()

	if _, err := uc.Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "singles"),
		line("SKU-2", 1, "pick"),
	}, true); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	demands := findAllSiteSkuDemandChanged(t, f.events)
	if len(demands) != 2 {
		t.Fatalf("SiteSkuDemandChanged count = %d, want 2 (one per line): %+v", len(demands), demands)
	}
	byLine := map[int]shared.SiteSkuDemandChanged{}
	for _, d := range demands {
		byLine[d.LineNo] = d
	}
	// "singles" carries a 6h lead time in the fixture, "pick" the 24h
	// default — and the order-level PromiseDate() is the LATEST (24h), so
	// line 1 proves per-line attribution rather than order-level copying.
	if want := now().Add(6 * time.Hour); !byLine[1].DueAt.Equal(want) {
		t.Fatalf("line 1 due_at = %s, want %s (its own group's cutoff)", byLine[1].DueAt, want)
	}
	if want := now().Add(24 * time.Hour); !byLine[2].DueAt.Equal(want) {
		t.Fatalf("line 2 due_at = %s, want %s", byLine[2].DueAt, want)
	}
}

func TestCancelOrder_ProjectsRemovedSiteSkuDemandForActiveHeldLine(t *testing.T) {
	f := newFixture()
	policy := demandPolicy()
	receive := f.receiveOrder()
	receive.DemandProjection = policy
	o, err := receive.ExecuteHeld(context.Background(), []usecases.NewLine{line("SKU-1", 2, "pick")}, false, false)
	if err != nil {
		t.Fatalf("ExecuteHeld: %v", err)
	}
	f.events.reset()

	cancel := f.cancelOrder()
	cancel.DemandProjection = policy
	if _, err := cancel.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	demand := findSiteSkuDemandChanged(t, f.events)
	if demand.State != shared.SiteSkuDemandRemoved || demand.SourceOrderID != o.ID() || demand.LineNo != 1 || demand.DemandedUnits != 2 {
		t.Fatalf("demand = %+v", demand)
	}
	assertEventNames(t, f.events, "SiteSkuDemandChanged", "OrderCancelled")
}

func TestCancelOrder_UnallocatedOrderProjectsNothing(t *testing.T) {
	// An order that never allocated (inventory down at intake) holds no
	// demand at the site: cancelling it must not fabricate a REMOVED.
	f := newFixture()
	f.inventory.reserveErr = errBoom
	policy := demandPolicy()
	receive := f.receiveOrder()
	receive.DemandProjection = policy
	o, err := receive.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 2, "pick")}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.events.reset()

	cancel := f.cancelOrder()
	cancel.DemandProjection = policy
	if _, err := cancel.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	for _, name := range f.events.names() {
		if name == "SiteSkuDemandChanged" {
			t.Fatalf("SiteSkuDemandChanged published for an order with no allocated lines: %v", f.events.names())
		}
	}
}

func TestRetryAllocation_ProjectsActiveForLinesThatReAllocate(t *testing.T) {
	// Ship-complete order blocked on SKU-2: line 1 is allocated but NOT
	// released (BR3 holds the whole order), line 2 backordered. The retry
	// clears the backorder — the re-allocated line 2 must project ACTIVE,
	// and line 1 (already carried by intake's own projection) must NOT
	// be re-emitted by this pass.
	f := newFixture()
	policy := demandPolicy()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	receive := f.receiveOrder()
	receive.DemandProjection = policy
	o, err := receive.Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"),
		line("SKU-2", 1, "pick"),
	}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	f.events.reset()

	delete(f.inventory.reserveErrBySKU, "SKU-2")
	retry := f.retryAllocation()
	retry.DemandProjection = policy
	if _, err := retry.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("RetryAllocation: %v", err)
	}

	demands := findAllSiteSkuDemandChanged(t, f.events)
	if len(demands) != 1 || demands[0].LineNo != 2 || demands[0].State != shared.SiteSkuDemandActive {
		t.Fatalf("demands = %+v, want exactly one ACTIVE for line 2", demands)
	}
}

func TestRepromiseOrder_ProjectsActiveForTheRePromisedGroup(t *testing.T) {
	rf := newRepromiseFixture()
	// Intake runs WITHOUT the projection: this test asserts the
	// re-promise emission alone.
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if o.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusReleased)
	}
	rf.f.events.reset()

	moved := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	uc := rf.repromiseOrder(moved)
	uc.DemandProjection = demandPolicy()
	if err := uc.Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	demand := findSiteSkuDemandChanged(t, rf.f.events)
	if demand.State != shared.SiteSkuDemandActive || demand.SourceOrderID != o.ID() || demand.LineNo != 1 {
		t.Fatalf("demand = %+v", demand)
	}
	if want := rf.f.clock.Now().Add(6 * time.Hour); !demand.DueAt.Equal(want) {
		t.Fatalf("due_at = %s, want the FRESH group cutoff %s", demand.DueAt, want)
	}
}

func TestRepromiseOrder_WithoutProjection_PublishesNoSiteSkuDemand(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	rf.f.events.reset()

	moved := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	if err := rf.repromiseOrder(moved).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, name := range rf.f.events.names() {
		if name == "SiteSkuDemandChanged" {
			t.Fatalf("SiteSkuDemandChanged published with the projection disabled: %v", rf.f.events.names())
		}
	}
}

func findSiteSkuDemandChanged(t *testing.T, publisher *recordingPublisher) shared.SiteSkuDemandChanged {
	t.Helper()
	all := findAllSiteSkuDemandChanged(t, publisher)
	if len(all) != 1 {
		t.Fatalf("want exactly 1 SiteSkuDemandChanged event, got %d", len(all))
	}
	return all[0]
}

func findAllSiteSkuDemandChanged(t *testing.T, publisher *recordingPublisher) []shared.SiteSkuDemandChanged {
	t.Helper()
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	var out []shared.SiteSkuDemandChanged
	for _, event := range publisher.events {
		if demand, ok := event.(shared.SiteSkuDemandChanged); ok {
			out = append(out, demand)
		}
	}
	return out
}
