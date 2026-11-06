package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// TestExecuteForMissedLoad_MovesTheWholeOrderPromise pins the order-wide
// re-promise: a PackageMissedLoad whose order holds promise groups gets
// EVERY group recomputed, the moved ones persisted, and one
// OrderRepromised per moved group with reason PackageMissedLoad.
func TestExecuteForMissedLoad_MovesTheWholeOrderPromise(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"))
	if o.Status() != order.StatusReleased {
		t.Fatalf("precondition status = %q, want Released", o.Status())
	}
	original := o.PromiseGroups()
	if len(original) != 1 {
		t.Fatalf("original groups = %d, want 1", len(original))
	}
	// A shorter lead time than intake's 24h: the recompute must move.
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}

	err := rf.repromiseOrder(movedPromise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-1", OrderId: o.ID(), Cutoff: time.Date(2026, 11, 6, 16, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ExecuteForMissedLoad: %v", err)
	}

	stored, _ := rf.f.orders.FindByID(context.Background(), o.ID())
	if stored == nil {
		t.Fatal("order disappeared")
	}
	updated := stored.PromiseGroups()
	if len(updated) != 1 {
		t.Fatalf("updated groups = %d, want 1", len(updated))
	}
	want := rf.f.clock.Now().Add(6 * time.Hour)
	if !updated[0].Promise.CutoffAt.Equal(want) {
		t.Fatalf("cutoff = %v, want %v", updated[0].Promise.CutoffAt, want)
	}
	if updated[0].Promise.CutoffAt.Equal(original[0].Promise.CutoffAt) {
		t.Fatal("cutoff did not move; fixture is not discriminating")
	}
	if got := rf.f.events.names(); len(got) != 1 || got[0] != "OrderRepromised" {
		t.Fatalf("events = %v, want exactly one OrderRepromised", got)
	}
}

func TestExecuteForMissedLoad_DuplicateIdChangesNothing(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	movedPromise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	req := usecases.RepromiseMissedLoadRequest{SourceEventId: "evt-miss-2", OrderId: o.ID()}
	if err := rf.repromiseOrder(movedPromise).ExecuteForMissedLoad(context.Background(), req); err != nil {
		t.Fatalf("first: %v", err)
	}
	first := rf.f.events.names()
	if err := rf.repromiseOrder(movedPromise).ExecuteForMissedLoad(context.Background(), req); err != nil {
		t.Fatalf("re-delivery: %v", err)
	}
	if got := len(rf.f.events.names()); got != len(first) {
		t.Fatalf("re-delivery published %d more events, want 0", got-len(first))
	}
}

func TestExecuteForMissedLoad_UnknownOrderIsFailSoft(t *testing.T) {
	rf := newRepromiseFixture()
	err := rf.repromiseOrder(rf.f.promise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-3", OrderId: "ord-never",
	})
	if err != nil {
		t.Fatalf("ExecuteForMissedLoad: %v (an unknown order must be fail-soft)", err)
	}
	if got := rf.f.events.names(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

// TestExecuteForMissedLoad_NothingLeftToPromiseIsFailSoft pins the
// recorded known gap (ADR 0051): an order whose lines are all Shipped (or
// otherwise outside Allocated/Released) has no group the policy can
// re-promise — the missed-load fact is late, nothing moves, no error.
func TestExecuteForMissedLoad_NothingLeftToPromiseIsFailSoft(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if _, err := o.ShipAll(); err != nil {
		t.Fatalf("ShipAll: %v", err)
	}
	if err := rf.f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	err := rf.repromiseOrder(rf.f.promise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-4", OrderId: o.ID(),
	})
	if err != nil {
		t.Fatalf("ExecuteForMissedLoad: %v (a shipped order must be fail-soft)", err)
	}
	if got := rf.f.events.names(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

func TestExecuteForMissedLoad_UnchangedPromisePublishesNothing(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	// Same policy as intake: the recompute lands on the same cutoff.
	err := rf.repromiseOrder(rf.f.promise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-5", OrderId: o.ID(),
	})
	if err != nil {
		t.Fatalf("ExecuteForMissedLoad: %v", err)
	}
	if got := rf.f.events.names(); len(got) != 0 {
		t.Fatalf("events = %v, want none for an unchanged promise", got)
	}
}

func TestExecuteForMissedLoad_NoGroupsIsFailSoft(t *testing.T) {
	rf := newRepromiseFixture()
	// A pending order (allocation blocked) never recorded promise groups.
	rf.f.inventory.reserveErr = errBoom
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if len(o.PromiseGroups()) != 0 {
		t.Fatalf("precondition groups = %d, want 0", len(o.PromiseGroups()))
	}
	err := rf.repromiseOrder(rf.f.promise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-6", OrderId: o.ID(),
	})
	if err != nil {
		t.Fatalf("ExecuteForMissedLoad: %v", err)
	}
	if got := rf.f.events.names(); len(got) != 0 {
		t.Fatalf("events = %v, want none", got)
	}
}

func TestExecuteForMissedLoad_InfrastructureErrorPropagates(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	rf.processed.err = errBoom
	err := rf.repromiseOrder(rf.f.promise).ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: "evt-miss-7", OrderId: o.ID(),
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the infrastructure failure", err)
	}
}
