package http_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/domain/order"
)

// ADR 0031 amendment: a plan whose location is a zone and whose site_id is
// the order's site annotates the order; an order is NOT annotated by a plan
// whose site_id is another site even though its location equals the site;
// GET /planned-capacity keeps listing by location and shows siteId.
func TestOrderResponse_CapacityConstraintMatchesBySiteID(t *testing.T) {
	zone := pcPlan("plan-zone", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	zone.Location, zone.SiteID = "PATH-ZONE-A", "SIM1"
	wrongSite := pcPlan("plan-wrong-site", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	wrongSite.Location, wrongSite.SiteID = "SIM1", "SIM2"

	tests := []struct {
		name      string
		plan      order.PlannedCapacityWindow
		annotated bool
	}{
		{"zone location with the order's site_id", zone, true},
		{"site_id of another site, location equal to this site", wrongSite, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.NewPlannedCapacityRepo()
			env := newPlannedCapacityEnv(t, repo)
			if _, err := repo.Upsert(context.Background(), tt.plan); err != nil {
				t.Fatal(err)
			}
			_, body := placeOrder(t, env)
			if got := strings.Contains(body, `"capacityConstraint"`); got != tt.annotated {
				t.Fatalf("annotated = %v, want %v: %s", got, tt.annotated, body)
			}
		})
	}
}

func TestGetPlannedCapacity_ListsByLocationAndShowsSiteID(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	env := newPlannedCapacityEnv(t, repo)
	zone := pcPlan("plan-zone", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	zone.Location, zone.SiteID = "PATH-ZONE-A", "SIM1"
	legacy := pcPlan("plan-legacy", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	for _, w := range []order.PlannedCapacityWindow{zone, legacy} {
		if _, err := repo.Upsert(context.Background(), w); err != nil {
			t.Fatal(err)
		}
	}

	byZone := env.do(t, http.MethodGet, "/planned-capacity?site=PATH-ZONE-A", "")
	if byZone.Code != http.StatusOK || !strings.Contains(byZone.Body.String(), `"planId":"plan-zone"`) ||
		!strings.Contains(byZone.Body.String(), `"siteId":"SIM1"`) {
		t.Fatalf("by the zone = %d %s; want the zone plan with its siteId", byZone.Code, byZone.Body.String())
	}
	bySite := env.do(t, http.MethodGet, "/planned-capacity?site=SIM1", "")
	body := bySite.Body.String()
	if bySite.Code != http.StatusOK || strings.Contains(body, "plan-zone") || !strings.Contains(body, `"planId":"plan-legacy"`) {
		t.Fatalf("by SIM1 = %d %s; the listing still filters on location only", bySite.Code, body)
	}
	if strings.Contains(body, "siteId") {
		t.Fatalf("a plan without a site id must omit siteId: %s", body)
	}
}
