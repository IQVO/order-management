package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// TestSetPromiseGroups_SingleGroup_MatchesSetPromise proves ADR 0017's
// backward-compatibility claim directly on the aggregate: calling
// SetPromiseGroups with a single group covering every line reproduces
// SetPromise's own legacy-field assignment byte for byte.
func TestSetPromiseGroups_SingleGroup_MatchesSetPromise(t *testing.T) {
	o1 := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	o2 := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})

	p := order.Promise{CptId: "sp1-1800", CutoffAt: testTime(), Basis: order.BasisCapability}

	o1.SetPromise(p)
	o2.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: p}})

	if got1, got2 := o1.PromiseDate(), o2.PromiseDate(); !got1.Equal(*got2) {
		t.Fatalf("PromiseDate diverged: SetPromise=%v SetPromiseGroups=%v", got1, got2)
	}
	if got1, got2 := o1.PromiseCptId(), o2.PromiseCptId(); *got1 != *got2 {
		t.Fatalf("PromiseCptId diverged: SetPromise=%v SetPromiseGroups=%v", *got1, *got2)
	}
	if got1, got2 := o1.PromiseBasis(), o2.PromiseBasis(); *got1 != *got2 {
		t.Fatalf("PromiseBasis diverged: SetPromise=%v SetPromiseGroups=%v", *got1, *got2)
	}
}

// TestSetPromiseGroups_MultipleGroups_LegacyFieldsProjectLatestCutoff
// proves ADR 0014 §3's stated projection rule: PromiseDate() (and, per
// ADR 0017's documented extension, PromiseCptId()/PromiseBasis()) come
// from the group with the LATEST CutoffAt, never an earlier one — "no
// existing reader sees an earlier date than before".
func TestSetPromiseGroups_MultipleGroups_LegacyFieldsProjectLatestCutoff(t *testing.T) {
	o := newOrder(t, true,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 1},
	)

	early := order.Promise{CptId: "sp1-1200", CutoffAt: testTime(), Basis: order.BasisCapability}
	late := order.Promise{CptId: "sp1-1800", CutoffAt: testTime().Add(6 * time.Hour), Basis: order.BasisLeadTime}

	// Deliberately pass the later group SECOND, to prove the projection
	// picks the latest CutoffAt regardless of slice order.
	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: early},
		{LineNos: []int{2}, Promise: late},
	})

	if got := o.PromiseDate(); got == nil || !got.Equal(late.CutoffAt) {
		t.Fatalf("PromiseDate() = %v, want the latest group's cutoff %v", got, late.CutoffAt)
	}
	if got := o.PromiseCptId(); got == nil || *got != late.CptId {
		t.Fatalf("PromiseCptId() = %v, want %q (the latest group's CptId)", got, late.CptId)
	}
	if got := o.PromiseBasis(); got == nil || *got != late.Basis {
		t.Fatalf("PromiseBasis() = %v, want %q (the latest group's Basis)", got, late.Basis)
	}

	groups := o.PromiseGroups()
	if len(groups) != 2 {
		t.Fatalf("PromiseGroups() = %d groups, want 2", len(groups))
	}
}

// TestPromiseGroups_EmptyUntilSet proves the accessor's documented
// zero-value contract.
func TestPromiseGroups_EmptyUntilSet(t *testing.T) {
	o := newOrder(t, true, lineSpec{sku: "SKU-1", qty: 1})
	if got := o.PromiseGroups(); len(got) != 0 {
		t.Fatalf("PromiseGroups() = %v, want empty before any promise is set", got)
	}
}

// TestPromiseGroups_ReturnsACopy proves the same "read is a copy"
// discipline Lines()/PromiseDate() already document — mutating the
// returned slice (or a group's LineNos) must not corrupt the aggregate.
func TestPromiseGroups_ReturnsACopy(t *testing.T) {
	o := newOrder(t, true, lineSpec{sku: "SKU-1", qty: 1})
	p := order.Promise{CptId: "sp1-1800", CutoffAt: testTime(), Basis: order.BasisCapability}
	o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: p}})

	got := o.PromiseGroups()
	got[0].LineNos[0] = 999
	_ = append(got, order.PromiseGroup{LineNos: []int{42}})

	again := o.PromiseGroups()
	if len(again) != 1 {
		t.Fatalf("aggregate state was mutated: len(PromiseGroups()) = %d, want 1", len(again))
	}
	if again[0].LineNos[0] != 1 {
		t.Fatalf("aggregate state was mutated through the returned slice: LineNos[0] = %d, want 1", again[0].LineNos[0])
	}
}

// TestRehydrateWithGroups_RoundTripsGroups covers the widened rehydrate
// constructor a repository adapter uses to restore the full breakdown.
func TestRehydrateWithGroups_RoundTripsGroups(t *testing.T) {
	line := order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LineAllocated, nil)
	promiseDate := testTime()
	cptId := "sp1-1800"
	basis := order.BasisCapability
	groups := []order.PromiseGroup{
		{LineNos: []int{1}, Promise: order.Promise{CptId: cptId, CutoffAt: promiseDate, Basis: basis}},
	}

	o := order.Rehydrate(order.OrderSnapshot{
		ID: "ord-1", Lines: []*order.OrderLine{line}, AllowPartialShipment: true,
		PromiseDate: &promiseDate, PromiseCptID: &cptId, PromiseBasis: &basis, PromiseGroups: groups,
	})

	got := o.PromiseGroups()
	if len(got) != 1 || len(got[0].LineNos) != 1 || got[0].LineNos[0] != 1 {
		t.Fatalf("PromiseGroups() = %+v, want the rehydrated single group", got)
	}
	if got[0].Promise.CptId != cptId {
		t.Fatalf("group CptId = %q, want %q", got[0].Promise.CptId, cptId)
	}
	// A snapshot with no PromiseGroups (a pre-ADR-0017 row) must still
	// produce an order with NO group breakdown, only the legacy summary
	// fields.
	legacy := order.Rehydrate(order.OrderSnapshot{
		ID: "ord-2", Lines: []*order.OrderLine{line}, AllowPartialShipment: true,
		PromiseDate: &promiseDate, PromiseCptID: &cptId, PromiseBasis: &basis,
	})
	if got := legacy.PromiseGroups(); len(got) != 0 {
		t.Fatalf("Rehydrate (no groups) PromiseGroups() = %v, want empty", got)
	}
	if got := legacy.PromiseDate(); got == nil || !got.Equal(promiseDate) {
		t.Fatalf("Rehydrate (no groups) PromiseDate() = %v, want %v", got, promiseDate)
	}
}
