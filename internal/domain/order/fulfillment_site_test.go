package order_test

import (
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func TestOrder_HasNoFulfillmentSiteByDefault(t *testing.T) {
	o := newOrder(t, false, lineSpec{"SKU-1", 1, "pick", false})
	if got := o.FulfillmentSiteID(); got != nil {
		t.Fatalf("FulfillmentSiteID() = %q, want nil", *got)
	}
}

func TestOrder_AssignFulfillmentSite_SetsItOnce(t *testing.T) {
	o := newOrder(t, false, lineSpec{"SKU-1", 1, "pick", false})
	if err := o.AssignFulfillmentSite("WH2"); err != nil {
		t.Fatalf("AssignFulfillmentSite: %v", err)
	}
	got := o.FulfillmentSiteID()
	if got == nil || *got != "WH2" {
		t.Fatalf("FulfillmentSiteID() = %v, want WH2", got)
	}

	err := o.AssignFulfillmentSite("WH3")
	if !errors.Is(err, order.ErrFulfillmentSiteAlreadyAssigned) {
		t.Fatalf("second assign error = %v, want ErrFulfillmentSiteAlreadyAssigned", err)
	}
	if got := o.FulfillmentSiteID(); got == nil || *got != "WH2" {
		t.Fatalf("site changed to %v after a rejected reassignment", got)
	}
}

func TestOrder_FulfillmentSiteID_ReturnsACopy(t *testing.T) {
	o := newOrder(t, false, lineSpec{"SKU-1", 1, "pick", false})
	_ = o.AssignFulfillmentSite("WH2")
	*o.FulfillmentSiteID() = "EVIL"
	if got := o.FulfillmentSiteID(); *got != "WH2" {
		t.Fatalf("site mutated through the accessor: %q", *got)
	}
}

func TestRehydrate_CarriesTheFulfillmentSiteWithoutAliasing(t *testing.T) {
	site := shared.FulfillmentSiteID("WH2")
	o := order.Rehydrate(order.OrderSnapshot{ID: "ord-1", FulfillmentSiteID: &site})
	site = "CHANGED"
	if got := o.FulfillmentSiteID(); got == nil || *got != "WH2" {
		t.Fatalf("FulfillmentSiteID() = %v, want WH2 (snapshot must be copied)", got)
	}
	if got := order.Rehydrate(order.OrderSnapshot{ID: "ord-1"}).FulfillmentSiteID(); got != nil {
		t.Fatalf("FulfillmentSiteID() = %q, want nil for a snapshot without one", *got)
	}
}

func TestOrder_SiteSurvivesTheLifecycle(t *testing.T) {
	o := newOrder(t, false, lineSpec{"SKU-1", 1, "pick", false})
	_ = o.AssignFulfillmentSite("WH2")
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if got := o.FulfillmentSiteID(); got == nil || *got != "WH2" {
		t.Fatalf("site after allocate = %v", got)
	}
}
