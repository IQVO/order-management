package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0031 amendment: a plan is matched to the order's site by its
// site_id when it has one, else by its location; a write without a
// site_id never erases a stored one.

func TestPlannedCapacityWindow_MatchesSite(t *testing.T) {
	tests := []struct {
		name     string
		siteID   string
		location string
		site     string
		want     bool
	}{
		{"site_id equals the site, location is a zone", "SIM1", "PATH-ZONE-A", "SIM1", true},
		{"site_id equals the site and so does the location", "SIM1", "SIM1", "SIM1", true},
		{"site_id is another site even though location equals the site", "SIM2", "SIM1", "SIM1", false},
		{"site_id is another site and location is a zone", "SIM2", "PATH-ZONE-A", "SIM1", false},
		{"no site_id, location equals the site (legacy plan)", "", "SIM1", "SIM1", true},
		{"no site_id, location differs", "", "SIM2", "SIM1", false},
		{"no site_id, location is a zone", "", "PATH-ZONE-A", "SIM1", false},
		{"site_id is matched case-sensitively", "sim1", "PATH-ZONE-A", "SIM1", false},
		{"location is matched case-sensitively when there is no site_id", "", "sim1", "SIM1", false},
		{"an empty site never matches a plan that has a site_id", "SIM1", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := pcWindow()
			w.SiteID, w.Location = tt.siteID, tt.location
			if got := w.MatchesSite(tt.site); got != tt.want {
				t.Fatalf("MatchesSite(%q) with site_id %q, location %q = %v, want %v",
					tt.site, tt.siteID, tt.location, got, tt.want)
			}
		})
	}
}

// (stored site_id, incoming site_id) x (equal, differs, empty), for a
// write that replaces the row whole and for a SUPERSEDED write that keeps
// the stored facts. The expectations are written out, not derived.
func TestPlannedCapacityWindow_ReplacementOf_SiteID(t *testing.T) {
	tests := []struct {
		name           string
		stored         string
		incoming       string
		wantPublished  string // a Created / Published / ShortageDetected write
		wantSuperseded string // a CapacityPlanSuperseded write
	}{
		{"both empty", "", "", "", ""},
		{"stored empty, incoming set: the site id is learned", "", "SIM1", "SIM1", "SIM1"},
		{"stored set, incoming empty: never erased", "SIM1", "", "SIM1", "SIM1"},
		{"stored set, incoming equal", "SIM1", "SIM1", "SIM1", "SIM1"},
		{"stored set, incoming differs: a whole write replaces, a tombstone keeps", "SIM1", "SIM2", "SIM2", "SIM1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prev := pcWindow()
			prev.SiteID = tt.stored

			pub := pcWindow()
			pub.SiteID, pub.AsOf = tt.incoming, pcAsOf.Add(time.Minute)
			if got := pub.ReplacementOf(prev).SiteID; got != tt.wantPublished {
				t.Fatalf("published write over stored %q with incoming %q kept site_id %q, want %q",
					tt.stored, tt.incoming, got, tt.wantPublished)
			}

			sup := pcSupersededWindow()
			sup.SiteID, sup.AsOf = tt.incoming, pcAsOf.Add(time.Minute)
			if got := sup.ReplacementOf(prev).SiteID; got != tt.wantSuperseded {
				t.Fatalf("superseded write over stored %q with incoming %q kept site_id %q, want %q",
					tt.stored, tt.incoming, got, tt.wantSuperseded)
			}
		})
	}
}

// Merging the site id must not disturb what ReplacementOf already did.
func TestPlannedCapacityWindow_ReplacementOf_SiteIDKeepsTheRestOfTheMerge(t *testing.T) {
	prev := pcWindow()
	prev.SiteID = "SIM1"

	shortage := pcWindow() // a ShortageDetected: no site_id, new quantities
	shortage.Shortage, shortage.AsOf = 5000, pcAsOf.Add(time.Minute)
	got := shortage.ReplacementOf(prev)
	if got.SiteID != "SIM1" || got.Shortage != 5000 || got.AsOf != shortage.AsOf {
		t.Fatalf("shortage write = %+v; want the new quantities and the stored site_id", got)
	}

	tomb := pcSupersededWindow()
	tomb.Location, tomb.AsOf = "OTHER-LOCATION", pcAsOf.Add(time.Minute)
	got = tomb.ReplacementOf(prev)
	if got.SiteID != "SIM1" || got.Location != prev.Location || got.Shortage != prev.Shortage ||
		got.Status != order.PlannedCapacitySuperseded || got.SupersededBy != pcSuccessor {
		t.Fatalf("tombstone = %+v; want the stored facts, SUPERSEDED, and the stored site_id", got)
	}
}

func TestCapacityConstraints_MatchesThePlansSiteID(t *testing.T) {
	zonePlan := func(siteID, location string) order.PlannedCapacityWindow {
		w := pcWindow()
		w.SiteID, w.Location = siteID, location
		return w
	}
	tests := []struct {
		name string
		plan order.PlannedCapacityWindow
		want int
	}{
		{"annotated when location is a zone and site_id is the site", zonePlan("SIM1", "PATH-ZONE-A"), 1},
		{"NOT annotated when site_id is another site, even though location equals this site", zonePlan("SIM2", "SIM1"), 0},
		{"legacy: no site_id and location equals the site", zonePlan("", "SIM1"), 1},
		{"legacy: no site_id and location differs", zonePlan("", "PATH-ZONE-A"), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := promisedOrder(t, pcEnd.Add(time.Hour))
			got := order.CapacityConstraints(o, pcNow, "SIM1", []order.PlannedCapacityWindow{tt.plan})
			if len(got) != tt.want {
				t.Fatalf("constraints = %d (%v), want %d", len(got), got, tt.want)
			}
		})
	}
}
