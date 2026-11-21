package main_test

// Step definitions of the documentation-derived wave-3 features
// (features/missed_load_rules.feature, features/
// planned_capacity_site_rules.feature): the vocabulary they need beyond
// the existing steps — a warehouse-planning event carrying a site id
// (ADR 0053), the demand projection also wired into the re-promise use
// case, outbound-shipping's PackageMissedLoad (ADR 0051's order-wide
// re-promise, driven through the same use case the consumer calls), and
// an "any occurrence" OrderRepromised assertion (a missed load can raise
// one event per moved group, so "the last one" is not enough).

import (
	"context"
	"fmt"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// docwaveMissedLoad mirrors OutboundShippingConsumer's PackageMissedLoad
// leg: source_order_id is a bare order id, and the missed load's cutoff
// rides along for telemetry only.
func (w *world) docwaveMissedLoad(eventID, orderRef, cutoff string) error {
	orderID := docwaveResolveOrder(w, orderRef)
	when, err := time.Parse(time.RFC3339, cutoff)
	if err != nil {
		return fmt.Errorf("missed load cutoff %q: %w", cutoff, err)
	}
	return w.rules.repromise.ExecuteForMissedLoad(context.Background(), usecases.RepromiseMissedLoadRequest{
		SourceEventId: eventID, OrderId: shared.OrderId(orderID), Cutoff: when,
	})
}

func docwaveResolveOrder(w *world, ref string) string {
	if id, ok := w.rules.named[ref]; ok {
		return id
	}
	return ref
}

func (w *world) docwaveMissedLoadForOrder(eventID, cutoff string) error {
	if w.lastOrderID == "" {
		return fmt.Errorf("no order was placed in this scenario yet")
	}
	return w.docwaveMissedLoad(eventID, w.lastOrderID, cutoff)
}

func (w *world) docwaveMissedLoadForNamed(eventID, orderRef, cutoff string) error {
	return w.docwaveMissedLoad(eventID, orderRef, cutoff)
}

// docwavePlanEventWithSite feeds one warehouse-planning event carrying
// the plan's canonical site id (ADR 0053) through the real
// ApplyPlannedCapacity use case, like planEvent in features_wave2_test.go.
func (w *world) docwavePlanEventWithSite(eventID, planID, siteID string, table *godog.Table) error {
	data := rows(table)
	if len(data) != 1 {
		return fmt.Errorf("planning event needs exactly one data row, got %d", len(data))
	}
	win, err := windowFromRow(planID, data[0])
	if err != nil {
		return err
	}
	win.SiteID = siteID
	return w.plannedApply.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: eventID, Window: win})
}

// docwaveStaticDemandSiteWithRepromise is staticDemandSite plus the
// re-promise use case, so a moved group's fresh cutoff is projected too.
func (w *world) docwaveStaticDemandSiteWithRepromise(site string) error {
	w.staticDemandSite(site)
	w.rules.repromise.DemandProjection = usecases.DemandProjectionPolicy{
		SiteID: site, AssignmentVersion: usecases.StaticDemandAssignmentVersion,
	}
	return nil
}

// docwaveAnyRepromised asserts SOME OrderRepromised occurrence moved
// from one CPT to another for a reason, not just the latest one.
func (w *world) docwaveAnyRepromised(from, to, reason string) error {
	for _, ev := range w.eventsNamed("OrderRepromised") {
		e, ok := ev.(shared.OrderRepromised)
		if !ok {
			continue
		}
		if e.CptIdOld == from && e.CptIdNew == to && e.Reason == reason && e.OrderID.String() == w.lastOrderID {
			return nil
		}
	}
	return fmt.Errorf("no OrderRepromised %q -> %q (%s) for order %s (captured: %v)",
		from, to, reason, w.lastOrderID, w.eventCounts())
}

// docwaveAnyActiveDemand asserts SOME ACTIVE SiteSkuDemandChanged for a
// line carries the given due instant — a missed load can re-emit demand
// for a line that already projected one at intake, so "the last one" or
// "the first one" is not enough.
func (w *world) docwaveAnyActiveDemand(lineNo int, due string) error {
	for _, ev := range w.eventsNamed("SiteSkuDemandChanged") {
		d, ok := ev.(shared.SiteSkuDemandChanged)
		if !ok || d.LineNo != lineNo || d.State != shared.SiteSkuDemandActive {
			continue
		}
		if got := d.DueAt.UTC().Format(time.RFC3339); got == due {
			return nil
		}
	}
	return fmt.Errorf("no ACTIVE SiteSkuDemandChanged event for line %d is due %s (captured: %v)", lineNo, due, w.eventCounts())
}

// registerDocWaveSteps registers the wave-3 documentation vocabulary on sc.
func registerDocWaveSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^warehouse-planning event "([^"]*)" states plan "([^"]*)" carrying site id "([^"]*)":$`, w.docwavePlanEventWithSite)
	sc.Step(`^the deployment projects demand to the static site "([^"]*)" including re-promises$`, w.docwaveStaticDemandSiteWithRepromise)
	sc.Step(`^outbound-shipping reports a missed load "([^"]*)" for the order with cutoff "([^"]*)"$`, w.docwaveMissedLoadForOrder)
	sc.Step(`^outbound-shipping reports a missed load "([^"]*)" for order "([^"]*)" with cutoff "([^"]*)"$`, w.docwaveMissedLoadForNamed)
	sc.Step(`^an "OrderRepromised" event moved the promise from CPT "([^"]*)" to "([^"]*)" because of "([^"]*)"$`, w.docwaveAnyRepromised)
	sc.Step(`^an ACTIVE SiteSkuDemandChanged event for line (\d+) is due at "([^"]*)"$`, w.docwaveAnyActiveDemand)
}
