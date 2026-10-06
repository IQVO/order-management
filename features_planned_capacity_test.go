package main_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// Step definitions for features/planned_capacity.feature (ADR 0031). Like
// every other step in this suite they observe the service ONLY through its
// REST API; the one Given step feeds the read model through the real
// ApplyPlannedCapacity use case — the exact handler the Kafka consumer
// drives — because there is deliberately no write endpoint.

func registerPlannedCapacitySteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^warehouse-planning has published a shortage of (\d+) for site "([^"]*)" from "([^"]*)" to "([^"]*)"$`, w.publishedShortage)
	sc.Step(`^warehouse-planning has created a draft plan with a shortage of (\d+) for site "([^"]*)" from "([^"]*)" to "([^"]*)"$`, w.draftShortage)
	sc.Step(`^planned capacity is requested for site "([^"]*)"$`, w.requestPlannedCapacity)
	sc.Step(`^planned capacity is requested without a site$`, w.requestPlannedCapacityWithoutSite)
	sc.Step(`^the order is capacity-constrained by plan "([^"]*)"$`, w.orderConstrainedBy)
	sc.Step(`^the order is not capacity-constrained$`, w.orderNotConstrained)
	sc.Step(`^the planned capacity lists plan "([^"]*)" as "([^"]*)" with a shortage of (\d+)$`, w.plannedCapacityLists)
	sc.Step(`^the planned capacity lists no plan "([^"]*)"$`, w.plannedCapacityListsNo)
}

// applyPlan feeds one plan to the read model under a fresh event id.
func (w *world) applyPlan(status order.PlannedCapacityStatus, shortage int, site, from, to string) error {
	start, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return fmt.Errorf("window start %q: %w", from, err)
	}
	end, err := time.Parse(time.RFC3339, to)
	if err != nil {
		return fmt.Errorf("window end %q: %w", to, err)
	}
	w.planSeq++
	return w.plannedApply.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{
		EventID: fmt.Sprintf("evt-bdd-%d", w.planSeq),
		Window: order.PlannedCapacityWindow{
			PlanID: fmt.Sprintf("plan-bdd-%d", w.planSeq), WarehouseID: "WH-1", Location: site, PathID: "pick-rebin-pack",
			Start: start, End: end, AssignedDemand: 12000, CapacityOverWindow: float64(12000 - shortage), Shortage: float64(shortage),
			BottleneckStep: "REBIN", Status: status, AsOf: fixedNow.Add(-time.Hour),
		},
	})
}

func (w *world) publishedShortage(shortage int, site, from, to string) error {
	return w.applyPlan(order.PlannedCapacityPublished, shortage, site, from, to)
}

func (w *world) draftShortage(shortage int, site, from, to string) error {
	return w.applyPlan(order.PlannedCapacityDraft, shortage, site, from, to)
}

func (w *world) requestPlannedCapacity(site string) error {
	return w.do(http.MethodGet, "/planned-capacity?site="+url.QueryEscape(site), nil)
}

func (w *world) requestPlannedCapacityWithoutSite() error {
	return w.do(http.MethodGet, "/planned-capacity", nil)
}

func (w *world) constraint() (map[string]any, bool, error) {
	obj, err := w.decodeLast()
	if err != nil {
		return nil, false, err
	}
	c, ok := obj["capacityConstraint"].(map[string]any)
	return c, ok, nil
}

func (w *world) orderConstrainedBy(planID string) error {
	c, ok, err := w.constraint()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("response has no capacityConstraint: %s", w.lastBody)
	}
	if c["constrained"] != true || c["site"] != "SIM1" {
		return fmt.Errorf("capacityConstraint = %v, want constrained=true at site SIM1", c)
	}
	windows, _ := c["windows"].([]any)
	for _, raw := range windows {
		if m, _ := raw.(map[string]any); m != nil && m["planId"] == planID {
			return nil
		}
	}
	return fmt.Errorf("capacityConstraint.windows = %v, want plan %q", windows, planID)
}

func (w *world) orderNotConstrained() error {
	if _, ok, err := w.constraint(); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("response unexpectedly has a capacityConstraint: %s", w.lastBody)
	}
	return nil
}

func (w *world) listedWindow(planID string) (map[string]any, error) {
	obj, err := w.decodeLast()
	if err != nil {
		return nil, err
	}
	windows, ok := obj["windows"].([]any)
	if !ok {
		return nil, fmt.Errorf("response has no windows array: %s", w.lastBody)
	}
	for _, raw := range windows {
		if m, _ := raw.(map[string]any); m != nil && m["planId"] == planID {
			return m, nil
		}
	}
	return nil, nil
}

func (w *world) plannedCapacityLists(planID, status string, shortage int) error {
	m, err := w.listedWindow(planID)
	if err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("plan %q is not listed: %s", planID, w.lastBody)
	}
	if m["status"] != status || m["shortage"] != float64(shortage) {
		return fmt.Errorf("plan %q = %v, want status %s and shortage %d", planID, m, status, shortage)
	}
	return nil
}

func (w *world) plannedCapacityListsNo(planID string) error {
	m, err := w.listedWindow(planID)
	if err != nil {
		return err
	}
	if m != nil {
		return fmt.Errorf("plan %q is unexpectedly listed: %v", planID, m)
	}
	return nil
}
