package kafka

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0031 amendment. warehouse-planning raises CapacityPlanPublished
// (carrying site_id) and then CapacityShortageDetected (location only, no
// site_id) for one plan, and CapacityPlanSuperseded (site_id and location)
// when a newer plan replaces it. The read model upserts by plan id, so a
// write without a site_id must never erase the one already stored.

const (
	pcZone   = "PATH-ZONE-A"
	pcSiteID = "SIM1"
)

func pcZoneData(withSite bool) map[string]any {
	d := pcData()
	d["location"] = pcZone
	if withSite {
		d["site_id"] = pcSiteID
	}
	return d
}

func pcSiteSupersededData(withSite bool) map[string]any {
	d := map[string]any{
		"plan_id": pcPlanID, "superseded_by": "9d1f0c2a-7b3e-4f5a-8c6d-2e4b1a0f9c87", "warehouse_id": "WH-1",
		"location": pcZone, "path_id": "pick-rebin-pack",
		"window_start": pcWinFrom.Format(time.RFC3339), "window_end": pcWinTo.Format(time.RFC3339),
	}
	if withSite {
		d["site_id"] = pcSiteID
	}
	return d
}

// Published (site_id), then Shortage (none), then Superseded: the stored
// site_id survives each step, and the zone stays the row's location.
func TestPlannedCapacityConsumer_SiteIDSurvivesShortageAndSuperseded(t *testing.T) {
	for _, supersededCarriesSite := range []bool{true, false} {
		f := newKPCFixture()
		steps := []struct {
			offset int64
			typ    string
			id     string
			at     time.Time
			data   map[string]any
			status order.PlannedCapacityStatus
		}{
			{1, pcTypePub, "evt-pub", pcEventAt, pcZoneData(true), order.PlannedCapacityPublished},
			{2, pcTypeShort, "evt-short", pcEventAt, pcZoneData(false), order.PlannedCapacityPublished},
			{3, pcTypeNew, "evt-created-late", pcEventAt.Add(time.Hour), pcZoneData(false), order.PlannedCapacityPublished},
			{4, ceTypeCapacityPlanSuperseded, "evt-superseded", pcEventAt.Add(2 * time.Hour),
				pcSiteSupersededData(supersededCarriesSite), order.PlannedCapacitySuperseded},
		}
		for _, s := range steps {
			if err := f.handle(t, pcMsg(t, s.offset, s.typ, s.id, pcPlanID, s.at, s.data)); err != nil {
				t.Fatalf("offset %d: %v", s.offset, err)
			}
			w, ok := f.store.get(pcPlanID)
			if !ok {
				t.Fatalf("offset %d: the plan must be in the read model", s.offset)
			}
			if w.SiteID != pcSiteID || w.Location != pcZone || w.Status != s.status {
				t.Fatalf("superseded carries site %v, offset %d: window = %+v; want site_id %q, location %q, status %s",
					supersededCarriesSite, s.offset, w, pcSiteID, pcZone, s.status)
			}
		}
		if f.dlq.count() != 0 {
			t.Fatal("nothing may be dead-lettered")
		}
	}
}

// The shortage landing first and the Published second (out of order, the
// same event time) still ends up with the site_id, and a Shortage's
// quantities are kept.
func TestPlannedCapacityConsumer_SiteIDIsLearnedFromALaterWrite(t *testing.T) {
	f := newKPCFixture()
	if err := f.handle(t, pcMsg(t, 1, pcTypeShort, "evt-short", pcPlanID, pcEventAt, pcZoneData(false))); err != nil {
		t.Fatal(err)
	}
	if w, _ := f.store.get(pcPlanID); w.SiteID != "" {
		t.Fatalf("a plan never seen with a site_id must have none, got %q", w.SiteID)
	}
	if err := f.handle(t, pcMsg(t, 2, pcTypePub, "evt-pub", pcPlanID, pcEventAt, pcZoneData(true))); err != nil {
		t.Fatal(err)
	}
	if w, _ := f.store.get(pcPlanID); w.SiteID != pcSiteID || w.Shortage != 4000 {
		t.Fatalf("window = %+v; want site_id %q learned and shortage 4000", w, pcSiteID)
	}
}

// A plan that never carries a site_id stays a legacy row: no site_id, and
// the location is what matches.
func TestPlannedCapacityConsumer_LegacyPlanHasNoSiteID(t *testing.T) {
	f := newKPCFixture()
	if err := f.handle(t, pcMsg(t, 1, pcTypePub, "evt-pub", pcPlanID, pcEventAt, pcData())); err != nil {
		t.Fatal(err)
	}
	w, _ := f.store.get(pcPlanID)
	if w.SiteID != "" || !w.MatchesSite("SIM1") || w.MatchesSite(pcZone) {
		t.Fatalf("window = %+v; want no site_id and a match on its location SIM1 only", w)
	}
}
