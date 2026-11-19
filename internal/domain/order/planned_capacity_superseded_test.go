package order_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0052: CapacityPlanSuperseded moves a plan's row to a terminal
// SUPERSEDED status that never constrains an order.

const pcSuccessor = "plan-9c1d"

func pcSupersededWindow() order.PlannedCapacityWindow {
	w := pcWindow()
	w.Status, w.SupersededBy = order.PlannedCapacitySuperseded, pcSuccessor
	return w
}

func TestPlannedCapacityWindow_Validate_Superseded(t *testing.T) {
	mut := func(f func(*order.PlannedCapacityWindow)) order.PlannedCapacityWindow {
		w := pcSupersededWindow()
		f(&w)
		return w
	}
	tests := []struct {
		name    string
		w       order.PlannedCapacityWindow
		wantErr bool
	}{
		{"valid superseded", pcSupersededWindow(), false},
		{"a tombstone with zero quantities and no warehouse is valid", mut(func(w *order.PlannedCapacityWindow) {
			w.WarehouseID, w.AssignedDemand, w.CapacityOverWindow, w.Shortage, w.BottleneckStep = "", 0, 0, 0, ""
		}), false},
		{"superseded without a successor", mut(func(w *order.PlannedCapacityWindow) { w.SupersededBy = "" }), true},
		{"a plan superseding itself", mut(func(w *order.PlannedCapacityWindow) { w.SupersededBy = w.PlanID }), true},
		{"superseded still needs a location", mut(func(w *order.PlannedCapacityWindow) { w.Location = "" }), true},
		{"superseded still needs an event time", mut(func(w *order.PlannedCapacityWindow) { w.AsOf = time.Time{} }), true},
		{"a published row needs no successor", mut(func(w *order.PlannedCapacityWindow) {
			w.Status, w.SupersededBy = order.PlannedCapacityPublished, ""
		}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.w.Validate()
			if tt.wantErr != (err != nil) {
				t.Fatalf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, order.ErrInvalidPlannedCapacity) {
				t.Fatalf("Validate() = %v, want it to wrap ErrInvalidPlannedCapacity", err)
			}
		})
	}
}

func TestPlannedCapacityWindow_SupersededNeverConstrains(t *testing.T) {
	for _, shortage := range []float64{4000, 0.25, 0} {
		w := pcSupersededWindow()
		w.Shortage = shortage
		if w.Constraining() {
			t.Fatalf("a SUPERSEDED row with shortage %v must never constrain", shortage)
		}
	}
}

// Every (prev, next) status pair, each at an older, an equal and a newer
// event time than prev. The expectations are written out, not derived.
func TestPlannedCapacityWindow_Supersedes_EveryStatusPair(t *testing.T) {
	const (
		draft = order.PlannedCapacityDraft
		pub   = order.PlannedCapacityPublished
		sup   = order.PlannedCapacitySuperseded
	)
	older, equal, newer := pcAsOf.Add(-time.Second), pcAsOf, pcAsOf.Add(time.Second)
	tests := []struct {
		prev, next order.PlannedCapacityStatus
		at         time.Time
		want       bool
	}{
		// prev DRAFT: last writer wins; SUPERSEDED wins at any time.
		{draft, draft, older, false}, {draft, draft, equal, true}, {draft, draft, newer, true},
		{draft, pub, older, false}, {draft, pub, equal, true}, {draft, pub, newer, true},
		{draft, sup, older, true}, {draft, sup, equal, true}, {draft, sup, newer, true},
		// prev PUBLISHED: a DRAFT never wins; SUPERSEDED wins at any time.
		{pub, draft, older, false}, {pub, draft, equal, false}, {pub, draft, newer, false},
		{pub, pub, older, false}, {pub, pub, equal, true}, {pub, pub, newer, true},
		{pub, sup, older, true}, {pub, sup, equal, true}, {pub, sup, newer, true},
		// prev SUPERSEDED: terminal, nothing replaces it at any time.
		{sup, draft, older, false}, {sup, draft, equal, false}, {sup, draft, newer, false},
		{sup, pub, older, false}, {sup, pub, equal, false}, {sup, pub, newer, false},
		{sup, sup, older, false}, {sup, sup, equal, false}, {sup, sup, newer, false},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s over %s at %s", tt.next, tt.prev, tt.at.Sub(pcAsOf))
		t.Run(name, func(t *testing.T) {
			prev := pcWindow()
			prev.Status = tt.prev
			if tt.prev == sup {
				prev.SupersededBy = pcSuccessor
			}
			next := pcWindow()
			next.Status, next.AsOf = tt.next, tt.at
			if tt.next == sup {
				next.SupersededBy = "plan-other"
			}
			if got := next.Supersedes(prev); got != tt.want {
				t.Fatalf("Supersedes() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Superseding a known row keeps its facts and changes only status,
// successor and AsOf; the event's own (sparser) fields are not copied.
func TestPlannedCapacityWindow_ReplacementOf_SupersededKeepsTheKnownRow(t *testing.T) {
	prev := pcWindow()
	at := pcAsOf.Add(time.Hour)
	event := order.PlannedCapacityWindow{
		PlanID: prev.PlanID, Location: "SITE-FROM-EVENT", PathID: "other-path",
		Start: pcStart.Add(time.Hour), End: pcEnd.Add(time.Hour),
		Status: order.PlannedCapacitySuperseded, SupersededBy: pcSuccessor, AsOf: at,
	}
	got := event.ReplacementOf(prev)
	want := prev
	want.Status, want.SupersededBy, want.AsOf = order.PlannedCapacitySuperseded, pcSuccessor, at
	if got != want {
		t.Fatalf("ReplacementOf() = %+v\nwant          %+v", got, want)
	}
	if got.Constraining() {
		t.Fatal("the superseded row must not constrain")
	}
}

// Any other write replaces the row whole, window included.
func TestPlannedCapacityWindow_ReplacementOf_OtherWritesReplaceWhole(t *testing.T) {
	prev := pcWindow()
	for _, status := range []order.PlannedCapacityStatus{order.PlannedCapacityDraft, order.PlannedCapacityPublished} {
		next := pcWindow()
		next.Status, next.Location, next.Shortage = status, "SIM2", 10
		next.Start, next.End = pcStart.Add(time.Hour), pcEnd.Add(time.Hour)
		if got := next.ReplacementOf(prev); got != next {
			t.Fatalf("%s: ReplacementOf() = %+v, want the new write whole %+v", status, got, next)
		}
	}
}

func TestCapacityConstraints_SupersededPlanIsNeverAConstraint(t *testing.T) {
	o := promisedOrder(t, pcEnd.Add(time.Hour))
	newer := pcWindow()
	newer.PlanID = pcSuccessor
	windows := []order.PlannedCapacityWindow{pcSupersededWindow(), newer}
	got := order.CapacityConstraints(o, pcNow, "SIM1", windows)
	if len(got) != 1 || got[0].PlanID != pcSuccessor {
		t.Fatalf("constraints = %v, want only the newer plan %s", got, pcSuccessor)
	}
}
