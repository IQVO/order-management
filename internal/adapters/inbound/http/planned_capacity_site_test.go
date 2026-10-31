package http_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
)

// ADR 0047: the capacity annotation matches an order that carries a
// fulfillmentSiteId on THAT site, and names it in capacityConstraint.site.
// The env is wired with the default site SIM1 (newPlannedCapacityEnv).
func TestOrderResponse_CapacityConstraint_MatchesTheOrdersOwnSite(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	env := newPlannedCapacityEnv(t, repo)
	atWH2 := pcPlan("plan-wh2", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))
	atWH2.Location = "WH2"
	if _, err := repo.Upsert(context.Background(), atWH2); err != nil {
		t.Fatal(err)
	}

	rec := env.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":"WH2","lines":[{"sku":"SKU-9","quantity":3}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /orders = %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, `"capacityConstraint":{"constrained":true,"site":"WH2"`) || !strings.Contains(body, `"planId":"plan-wh2"`) {
		t.Fatalf("an order at WH2 must be annotated by WH2's shortage and name WH2: %s", body)
	}

	// The same shortage does not touch an order without a site (default SIM1).
	_, plain := placeOrder(t, env)
	if strings.Contains(plain, `"capacityConstraint"`) {
		t.Fatalf("an order without a site is matched on SIM1, not WH2: %s", plain)
	}
}

func TestOrderResponse_CapacityConstraint_DefaultSiteShortageDoesNotAnnotateAnotherSitesOrder(t *testing.T) {
	repo := memory.NewPlannedCapacityRepo()
	env := newPlannedCapacityEnv(t, repo)
	if _, err := repo.Upsert(context.Background(), pcPlan("plan-sim1", pcClockNow.Add(5*time.Hour), pcClockNow.Add(11*time.Hour))); err != nil {
		t.Fatal(err)
	}

	rec := env.do(t, http.MethodPost, "/orders", `{"fulfillmentSiteId":"WH2","lines":[{"sku":"SKU-9","quantity":3}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /orders = %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); strings.Contains(body, `"capacityConstraint"`) {
		t.Fatalf("SIM1's shortage must not annotate an order fulfilled from WH2: %s", body)
	}
}
