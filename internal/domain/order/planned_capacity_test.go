package order_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// Distinct, non-zero fixtures: the planned shortage window is
// [08:00:00, 16:00:00) on 2026-10-05; "now" is the evening before.
var (
	pcNow   = time.Date(2026, 10, 4, 21, 15, 30, 0, time.UTC)
	pcStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	pcEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	pcAsOf  = time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
)

func pcWindow() order.PlannedCapacityWindow {
	return order.PlannedCapacityWindow{
		PlanID: "plan-0b7a", WarehouseID: "WH-7", Location: "SIM1", PathID: "pick-rebin-pack",
		Start: pcStart, End: pcEnd,
		AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", Status: order.PlannedCapacityPublished, AsOf: pcAsOf,
	}
}

func TestPlannedCapacityWindow_Validate(t *testing.T) {
	mut := func(f func(*order.PlannedCapacityWindow)) order.PlannedCapacityWindow {
		w := pcWindow()
		f(&w)
		return w
	}
	tests := []struct {
		name    string
		w       order.PlannedCapacityWindow
		wantErr bool
	}{
		{"valid published shortage", pcWindow(), false},
		{"valid draft", mut(func(w *order.PlannedCapacityWindow) { w.Status = order.PlannedCapacityDraft }), false},
		{"zero shortage is valid", mut(func(w *order.PlannedCapacityWindow) { w.Shortage = 0 }), false},
		{"zero demand and capacity are valid", mut(func(w *order.PlannedCapacityWindow) { w.AssignedDemand, w.CapacityOverWindow = 0, 0 }), false},
		{"one-second window is valid", mut(func(w *order.PlannedCapacityWindow) { w.End = w.Start.Add(time.Second) }), false},
		{"missing plan id", mut(func(w *order.PlannedCapacityWindow) { w.PlanID = "" }), true},
		{"missing location", mut(func(w *order.PlannedCapacityWindow) { w.Location = "" }), true},
		{"missing window start", mut(func(w *order.PlannedCapacityWindow) { w.Start = time.Time{} }), true},
		{"missing window end", mut(func(w *order.PlannedCapacityWindow) { w.End = time.Time{} }), true},
		{"end equal to start", mut(func(w *order.PlannedCapacityWindow) { w.End = w.Start }), true},
		{"end before start", mut(func(w *order.PlannedCapacityWindow) { w.End = w.Start.Add(-time.Second) }), true},
		{"negative demand", mut(func(w *order.PlannedCapacityWindow) { w.AssignedDemand = -1 }), true},
		{"negative capacity", mut(func(w *order.PlannedCapacityWindow) { w.CapacityOverWindow = -0.5 }), true},
		{"negative shortage", mut(func(w *order.PlannedCapacityWindow) { w.Shortage = -3 }), true},
		{"unknown status", mut(func(w *order.PlannedCapacityWindow) { w.Status = "ARCHIVED" }), true},
		{"missing event time", mut(func(w *order.PlannedCapacityWindow) { w.AsOf = time.Time{} }), true},
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

// The window is half-open [08:00:00, 16:00:00). Each case pins one exact
// boundary of Overlaps(from, until).
func TestPlannedCapacityWindow_Overlaps_Boundaries(t *testing.T) {
	w := pcWindow()
	tests := []struct {
		name        string
		from, until time.Time
		want        bool
	}{
		{"interval ends exactly when the window starts: no overlap", pcStart.Add(-4 * time.Hour), pcStart, false},
		{"interval ends one second into the window: overlap", pcStart.Add(-4 * time.Hour), pcStart.Add(time.Second), true},
		{"interval ends one second before the window starts: no overlap", pcStart.Add(-4 * time.Hour), pcStart.Add(-time.Second), false},
		{"interval starts exactly when the window ends: no overlap", pcEnd, pcEnd.Add(3 * time.Hour), false},
		{"interval starts one second before the window ends: overlap", pcEnd.Add(-time.Second), pcEnd.Add(3 * time.Hour), true},
		{"interval starts one second after the window ends: no overlap", pcEnd.Add(time.Second), pcEnd.Add(3 * time.Hour), false},
		{"interval equals the window: overlap", pcStart, pcEnd, true},
		{"interval strictly inside the window: overlap", pcStart.Add(time.Hour), pcEnd.Add(-time.Hour), true},
		{"interval strictly contains the window: overlap", pcStart.Add(-time.Hour), pcEnd.Add(time.Hour), true},
		{"entirely before the window: no overlap", pcNow, pcStart.Add(-time.Hour), false},
		{"entirely after the window: no overlap", pcEnd.Add(time.Hour), pcEnd.Add(2 * time.Hour), false},
		{"empty interval inside the window: no overlap", pcStart.Add(time.Hour), pcStart.Add(time.Hour), false},
		{"inverted interval spanning the window: no overlap", pcEnd.Add(time.Hour), pcStart.Add(-time.Hour), false},
		{"one-second interval inside the window: overlap", pcStart.Add(time.Hour), pcStart.Add(time.Hour + time.Second), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := w.Overlaps(tt.from, tt.until); got != tt.want {
				t.Fatalf("Overlaps(%s, %s) = %v, want %v", tt.from, tt.until, got, tt.want)
			}
		})
	}
}

func TestPlannedCapacityWindow_Constraining(t *testing.T) {
	tests := []struct {
		name     string
		status   order.PlannedCapacityStatus
		shortage float64
		want     bool
	}{
		{"published with a shortage", order.PlannedCapacityPublished, 4000, true},
		{"published with a fractional shortage", order.PlannedCapacityPublished, 0.25, true},
		{"published with no shortage", order.PlannedCapacityPublished, 0, false},
		{"draft with a shortage", order.PlannedCapacityDraft, 4000, false},
		{"draft with no shortage", order.PlannedCapacityDraft, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := pcWindow()
			w.Status, w.Shortage = tt.status, tt.shortage
			if got := w.Constraining(); got != tt.want {
				t.Fatalf("Constraining() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlannedCapacityWindow_Supersedes(t *testing.T) {
	prev := pcWindow()
	tests := []struct {
		name   string
		status order.PlannedCapacityStatus
		asOf   time.Time
		prev   order.PlannedCapacityStatus
		want   bool
	}{
		{"strictly newer event wins", order.PlannedCapacityPublished, pcAsOf.Add(time.Second), order.PlannedCapacityPublished, true},
		{"same-instant event wins (Published and ShortageDetected share a time)", order.PlannedCapacityPublished, pcAsOf, order.PlannedCapacityPublished, true},
		{"one second older event loses", order.PlannedCapacityPublished, pcAsOf.Add(-time.Second), order.PlannedCapacityPublished, false},
		{"published replaces an older draft", order.PlannedCapacityPublished, pcAsOf.Add(time.Minute), order.PlannedCapacityDraft, true},
		{"a draft never replaces a published row, even if newer", order.PlannedCapacityDraft, pcAsOf.Add(time.Hour), order.PlannedCapacityPublished, false},
		{"a newer draft replaces a draft", order.PlannedCapacityDraft, pcAsOf.Add(time.Second), order.PlannedCapacityDraft, true},
		{"an older draft loses to a draft", order.PlannedCapacityDraft, pcAsOf.Add(-time.Second), order.PlannedCapacityDraft, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := prev
			p.Status = tt.prev
			w := pcWindow()
			w.Status, w.AsOf = tt.status, tt.asOf
			if got := w.Supersedes(p); got != tt.want {
				t.Fatalf("Supersedes() = %v, want %v", got, tt.want)
			}
		})
	}
}

// promisedOrder builds an allocated order promised at cutoff.
func promisedOrder(t *testing.T, cutoff time.Time) *order.Order {
	t.Helper()
	o := newOrder(t, false, lineSpec{sku: "SKU-9", qty: 3, pathID: "pick"})
	if err := o.Allocate(1, "res-41"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	o.SetPromiseDate(cutoff)
	return o
}

func TestCapacityConstraints_PromiseWindowBoundaries(t *testing.T) {
	windows := []order.PlannedCapacityWindow{pcWindow()}
	tests := []struct {
		name   string
		cutoff time.Time
		want   int
	}{
		{"promise cutoff exactly at the window start", pcStart, 0},
		{"promise cutoff one second into the window", pcStart.Add(time.Second), 1},
		{"promise cutoff one second before the window", pcStart.Add(-time.Second), 0},
		{"promise cutoff after the window", pcEnd.Add(24 * time.Hour), 1},
		{"promise cutoff exactly at now (empty interval)", pcNow, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := order.CapacityConstraints(promisedOrder(t, tt.cutoff), pcNow, "SIM1", windows)
			if len(got) != tt.want {
				t.Fatalf("constraints = %d (%v), want %d", len(got), got, tt.want)
			}
		})
	}
}

func TestCapacityConstraints_NowAfterWindowEndNoLongerConstrains(t *testing.T) {
	o := promisedOrder(t, pcEnd.Add(48*time.Hour))
	if got := order.CapacityConstraints(o, pcEnd, "SIM1", []order.PlannedCapacityWindow{pcWindow()}); len(got) != 0 {
		t.Fatalf("at the exact window end the shortage is behind us, got %v", got)
	}
	if got := order.CapacityConstraints(o, pcEnd.Add(-time.Second), "SIM1", []order.PlannedCapacityWindow{pcWindow()}); len(got) != 1 {
		t.Fatalf("one second before the window end it still overlaps, got %v", got)
	}
}

func TestCapacityConstraints_Filters(t *testing.T) {
	o := promisedOrder(t, pcEnd.Add(time.Hour))
	mut := func(f func(*order.PlannedCapacityWindow)) []order.PlannedCapacityWindow {
		w := pcWindow()
		f(&w)
		return []order.PlannedCapacityWindow{w}
	}
	tests := []struct {
		name    string
		windows []order.PlannedCapacityWindow
		site    string
		want    int
	}{
		{"matching published shortage", mut(func(*order.PlannedCapacityWindow) {}), "SIM1", 1},
		{"other site", mut(func(*order.PlannedCapacityWindow) {}), "SIM2", 0},
		{"site match is exact, not case-insensitive", mut(func(*order.PlannedCapacityWindow) {}), "sim1", 0},
		{"window at another location", mut(func(w *order.PlannedCapacityWindow) { w.Location = "SIM2" }), "SIM1", 0},
		{"draft plan is never a constraint", mut(func(w *order.PlannedCapacityWindow) { w.Status = order.PlannedCapacityDraft }), "SIM1", 0},
		{"published plan without a shortage", mut(func(w *order.PlannedCapacityWindow) { w.Shortage = 0 }), "SIM1", 0},
		{"no windows", nil, "SIM1", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := order.CapacityConstraints(o, pcNow, tt.site, tt.windows); len(got) != tt.want {
				t.Fatalf("constraints = %d, want %d", len(got), tt.want)
			}
		})
	}
}

func TestCapacityConstraints_NoPromiseOrCancelled(t *testing.T) {
	windows := []order.PlannedCapacityWindow{pcWindow()}

	pending := newOrder(t, false, lineSpec{sku: "SKU-9", qty: 3, pathID: "pick"})
	if got := order.CapacityConstraints(pending, pcNow, "SIM1", windows); got != nil {
		t.Fatalf("an order with no promise is never constrained, got %v", got)
	}

	cancelled := promisedOrder(t, pcEnd.Add(time.Hour))
	if err := cancelled.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := order.CapacityConstraints(cancelled, pcNow, "SIM1", windows); got != nil {
		t.Fatalf("a cancelled order is never constrained, got %v", got)
	}
}

// With per-shipment-group promises (ADR 0017) every group's cutoff counts:
// the early group clears the window, the late one does not.
func TestCapacityConstraints_AnyPromiseGroupOverlapping(t *testing.T) {
	o := newOrder(t, true,
		lineSpec{sku: "SKU-1", qty: 1, pathID: "pick"},
		lineSpec{sku: "SKU-2", qty: 2, pathID: "pick"})
	for i := 1; i <= 2; i++ {
		if err := o.Allocate(i, "res-g"+string(rune('0'+i))); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
	}
	early := order.Promise{CutoffAt: pcStart.Add(-time.Hour), Basis: order.BasisCapability, CptId: "cpt-early"}
	late := order.Promise{CutoffAt: pcEnd.Add(time.Hour), Basis: order.BasisCapability, CptId: "cpt-late"}
	windows := []order.PlannedCapacityWindow{pcWindow()}

	o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1, 2}, Promise: early}})
	if got := order.CapacityConstraints(o, pcNow, "SIM1", windows); len(got) != 0 {
		t.Fatalf("a group that cuts off before the window is unconstrained, got %v", got)
	}

	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1}, Promise: early},
		{LineNos: []int{2}, Promise: late},
	})
	if got := order.CapacityConstraints(o, pcNow, "SIM1", windows); len(got) != 1 {
		t.Fatalf("one overlapping group constrains the order exactly once, got %v", got)
	}
}

func TestCapacityConstraints_OrderedByStartThenPlanID(t *testing.T) {
	o := promisedOrder(t, pcEnd.Add(72*time.Hour))
	mk := func(id string, start time.Time) order.PlannedCapacityWindow {
		w := pcWindow()
		w.PlanID, w.Start, w.End = id, start, start.Add(2*time.Hour)
		return w
	}
	windows := []order.PlannedCapacityWindow{
		mk("plan-c", pcStart.Add(5*time.Hour)),
		mk("plan-b", pcStart),
		mk("plan-a", pcStart),
		mk("plan-d", pcStart.Add(24*time.Hour)),
	}
	got := order.CapacityConstraints(o, pcNow, "SIM1", windows)
	var ids []string
	for _, w := range got {
		ids = append(ids, w.PlanID)
	}
	want := []string{"plan-a", "plan-b", "plan-c", "plan-d"}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}
}
