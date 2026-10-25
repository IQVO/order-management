package main_test

import (
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Step definitions for features/fulfillment_site.feature (ADR 0044). Like
// every other step in this suite they observe the service ONLY through its
// REST API and the recording event publisher.

func registerFulfillmentSiteSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^the deployment projects demand to the static site "([^"]*)"$`, w.staticDemandSite)
	sc.Step(`^an order is placed for SKUs "([^"]*)" at fulfillment site "([^"]*)"$`, w.placeAtSite)
	sc.Step(`^a network-originated hold is placed for SKUs "([^"]*)" at fulfillment site "([^"]*)" with required ship-by "([^"]*)"$`, w.placeHoldAtSite)
	sc.Step(`^the response has no field "([^"]*)"$`, w.responseHasNoField)
	sc.Step(`^the last "SiteSkuDemandChanged" event is for site "([^"]*)" with assignment version "([^"]*)" and state "([^"]*)"$`, w.lastDemandIs)
}

func (w *world) staticDemandSite(site string) error {
	policy := usecases.DemandProjectionPolicy{SiteID: site, AssignmentVersion: usecases.StaticDemandAssignmentVersion}
	w.receive.DemandProjection = policy
	w.retry.DemandProjection = policy
	w.cancel.DemandProjection = policy
	return nil
}

func (w *world) placeAtSite(skus, site string) error {
	return w.postAndCapture(map[string]any{"lines": simpleLines(skus), "fulfillmentSiteId": site})
}

func (w *world) placeHoldAtSite(skus, site, shipBy string) error {
	return w.postAndCapture(map[string]any{
		"lines": simpleLines(skus), "releaseOnAllocation": false, "requiredShipBy": shipBy, "fulfillmentSiteId": site,
	})
}

func (w *world) responseHasNoField(field string) error {
	obj, err := w.decodeLast()
	if err != nil {
		return err
	}
	if v, present := obj[field]; present {
		return fmt.Errorf("response unexpectedly carries %q = %v", field, v)
	}
	return nil
}

func (w *world) lastDemandIs(site, version, state string) error {
	ev, err := w.lastEvent("SiteSkuDemandChanged")
	if err != nil {
		return err
	}
	d, ok := ev.(shared.SiteSkuDemandChanged)
	if !ok {
		return fmt.Errorf("event is %T, not SiteSkuDemandChanged", ev)
	}
	if d.SiteID != site || d.AssignmentVersion != version || string(d.State) != state {
		return fmt.Errorf("SiteSkuDemandChanged = site %q, version %q, state %q; want %q, %q, %q",
			d.SiteID, d.AssignmentVersion, d.State, site, version, state)
	}
	return nil
}
