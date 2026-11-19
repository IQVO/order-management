package kafka

// ADR 0052: PlannedCapacityConsumer and CapacityPlanSuperseded. The fixture
// pcData()/pcMsg/newKPCFixture is planned_capacity_consumer_test.go's.

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

const (
	pcTypeSup   = ceTypeCapacityPlanSuperseded
	pcNewerPlan = "7e2f9a1c-3b4d-4e5f-8a6b-9c0d1e2f3a4b"
)

// pcSupersededData is the ADR 0024 wire shape: no location, warehouse or
// quantities; site_id is the site.
func pcSupersededData() map[string]any {
	return map[string]any{
		"plan_id": pcPlanID, "superseded_by": pcNewerPlan, "site_id": "SIM1", "path_id": "pick-rebin-pack",
		"window_start": pcWinFrom.Format(time.RFC3339), "window_end": pcWinTo.Format(time.RFC3339),
	}
}

func (f *kpcFixture) mustHandle(t *testing.T, offset int64, typ, id string, at time.Time, data any) {
	t.Helper()
	if err := f.handle(t, pcMsg(t, offset, typ, id, pcPlanID, at, data)); err != nil {
		t.Fatalf("offset %d (%s): %v", offset, typ, err)
	}
	if f.dlq.count() != 0 {
		t.Fatalf("offset %d (%s) was dead-lettered", offset, typ)
	}
}

func TestPlannedCapacityConsumer_SupersededAfterPublishedStopsConstraining(t *testing.T) {
	f := newKPCFixture()
	f.mustHandle(t, 1, pcTypePub, "evt-pub", pcEventAt, pcData())
	if w, _ := f.store.get(pcPlanID); !w.Constraining() {
		t.Fatal("precondition: the published shortage constrains")
	}
	supAt := pcEventAt.Add(25 * time.Minute)
	f.mustHandle(t, 2, pcTypeSup, "evt-sup", supAt, pcSupersededData())

	w, _ := f.store.get(pcPlanID)
	want := order.PlannedCapacityWindow{
		PlanID: pcPlanID, WarehouseID: "WH-1", Location: "SIM1", PathID: "pick-rebin-pack",
		Start: pcWinFrom, End: pcWinTo, AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", Status: order.PlannedCapacitySuperseded, SupersededBy: pcNewerPlan, AsOf: supAt,
	}
	if w != want {
		t.Fatalf("window = %+v\nwant     %+v", w, want)
	}
	if w.Constraining() {
		t.Fatal("a superseded plan must no longer constrain")
	}
	if got := f.committed(); len(got) != 2 {
		t.Fatalf("committed = %v, want both", got)
	}
}

func TestPlannedCapacityConsumer_PublishedRedeliveredAfterSupersededStaysSuperseded(t *testing.T) {
	f := newKPCFixture()
	f.mustHandle(t, 1, pcTypePub, "evt-pub", pcEventAt, pcData())
	f.mustHandle(t, 2, pcTypeSup, "evt-sup", pcEventAt.Add(time.Minute), pcSupersededData())
	// The same CloudEvents id again (the claim), then under fresh ids and a
	// later time (the domain rule): neither revives the plan.
	f.mustHandle(t, 3, pcTypePub, "evt-pub", pcEventAt, pcData())
	f.mustHandle(t, 4, pcTypePub, "evt-pub-replayed", pcEventAt.Add(time.Hour), pcData())
	f.mustHandle(t, 5, pcTypeShort, "evt-short-replayed", pcEventAt.Add(time.Hour), pcData())
	f.mustHandle(t, 6, pcTypeNew, "evt-created-late", pcEventAt.Add(time.Hour), pcData())

	w, _ := f.store.get(pcPlanID)
	if w.Status != order.PlannedCapacitySuperseded || w.SupersededBy != pcNewerPlan || w.Constraining() {
		t.Fatalf("window = %+v; want it still SUPERSEDED by %s and never constraining", w, pcNewerPlan)
	}
}

func TestPlannedCapacityConsumer_SupersededForAnUnseenPlanThenPublishedNeverConstrains(t *testing.T) {
	f := newKPCFixture()
	supAt := pcEventAt.Add(time.Minute)
	f.mustHandle(t, 1, pcTypeSup, "evt-sup", supAt, pcSupersededData())

	tombstone := order.PlannedCapacityWindow{
		PlanID: pcPlanID, Location: "SIM1", PathID: "pick-rebin-pack", Start: pcWinFrom, End: pcWinTo,
		Status: order.PlannedCapacitySuperseded, SupersededBy: pcNewerPlan, AsOf: supAt,
	}
	if w, ok := f.store.get(pcPlanID); !ok || w != tombstone {
		t.Fatalf("window = %+v (present %v)\nwant tombstone %+v", w, ok, tombstone)
	}
	// The plan's own events arrive late, even with a newer time.
	f.mustHandle(t, 2, pcTypePub, "evt-pub-late", pcEventAt.Add(time.Hour), pcData())
	f.mustHandle(t, 3, pcTypeShort, "evt-short-late", pcEventAt.Add(time.Hour), pcData())
	if w, _ := f.store.get(pcPlanID); w != tombstone || w.Constraining() {
		t.Fatalf("window = %+v; a late Published must never make the tombstone constraining", w)
	}
}

func TestPlannedCapacityConsumer_SupersededLookalikeTypesAreStillIgnored(t *testing.T) {
	for _, typ := range []string{
		"CapacityPlanSuperseded", // a short name is not the full type
		ceTypeCapacityPlanSuperseded + ".v2",
		"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanArchived",
	} {
		t.Run(typ, func(t *testing.T) {
			f := newKPCFixture()
			f.mustHandle(t, 9, typ, "evt-x", pcEventAt, pcSupersededData())
			if _, ok := f.store.get(pcPlanID); ok || f.store.isClaimed("evt-x") {
				t.Fatal("an ignored type must touch nothing")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 9 {
				t.Fatalf("committed = %v, want [9]", got)
			}
		})
	}
}

func TestPlannedCapacityConsumer_InvalidSupersededIsDeadLetteredNeverApplied(t *testing.T) {
	mut := func(f func(map[string]any)) map[string]any {
		d := pcSupersededData()
		f(d)
		return d
	}
	tests := []struct {
		name    string
		subject string
		data    any
	}{
		{"missing superseded_by", pcPlanID, mut(func(d map[string]any) { delete(d, "superseded_by") })},
		{"a plan superseding itself", pcPlanID, mut(func(d map[string]any) { d["superseded_by"] = pcPlanID })},
		{"missing site_id", pcPlanID, mut(func(d map[string]any) { delete(d, "site_id") })},
		{"missing plan_id", pcPlanID, mut(func(d map[string]any) { delete(d, "plan_id") })},
		{"inverted window", pcPlanID, mut(func(d map[string]any) { d["window_end"], d["window_start"] = d["window_start"], d["window_end"] })},
		{"unparseable window", pcPlanID, mut(func(d map[string]any) { d["window_end"] = "later" })},
		{"subject disagrees with plan_id", pcNewerPlan, pcSupersededData()},
		{"data is not an object", pcPlanID, "superseded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newKPCFixture()
			if err := f.handle(t, pcMsg(t, 7, pcTypeSup, "evt-bad", tt.subject, pcEventAt, tt.data)); err != nil {
				t.Fatalf("an invalid payload must be skipped without error, got %v", err)
			}
			if f.dlq.count() != 1 {
				t.Fatalf("dlq = %d, want 1", f.dlq.count())
			}
			if len(f.store.windows) != 0 || f.store.isClaimed("evt-bad") || f.store.upserts != 0 {
				t.Fatal("an invalid payload must not write or claim anything")
			}
			if got := f.committed(); len(got) != 1 || got[0] != 7 {
				t.Fatalf("committed = %v, want [7]", got)
			}
		})
	}
}
