package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR-0042: ReceiveOrder routes an order line only onto a CUSTOMER_ORDER
// path, through the real PathSelectionPolicy and the catalogue port.

func TestReceiveOrder_RoutesOnlyOntoCustomerOrderPaths(t *testing.T) {
	f := newFixture()
	permissive := shared.NewEligibility(nil, nil, nil, false)
	f.catalogue.cycleTimes = map[shared.PathId]time.Duration{}
	for _, id := range []shared.PathId{"PACK", "REBIN", "SLAM", shared.DefaultPathId, "RELOCATE"} {
		f.catalogue.eligibility[id] = permissive
		f.catalogue.cycleTimes[id] = 2 * time.Hour
	}
	f.catalogue.cycleTimes["RELOCATE"] = time.Minute // faster, but not an order path
	f.catalogue.intake = map[shared.PathId][]shared.DemandSource{
		"PACK": {}, "REBIN": {}, "SLAM": {},
		shared.DefaultPathId: {shared.DemandSourceCustomerOrder},
		"RELOCATE":           {"SLOT_RELOCATION"},
	}

	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{{SKU: "SKU-1", Quantity: 1}}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := o.Lines()[0].PathID(); got != shared.DefaultPathId {
		t.Fatalf("line PathID = %q, want %q (the one CUSTOMER_ORDER path)", got, shared.DefaultPathId)
	}
}

// TestReceiveOrder_NoPathDeclaresIntake_FallsBackToDefaultNotPack is the
// E5 regression at the use-case level: the live catalogue before seeding
// (four 2h paths, permissive eligibility, no intake) must not route a
// customer line to PACK.
func TestReceiveOrder_NoPathDeclaresIntake_FallsBackToDefaultNotPack(t *testing.T) {
	f := newFixture()
	permissive := shared.NewEligibility(nil, nil, nil, false)
	f.catalogue.cycleTimes = map[shared.PathId]time.Duration{}
	f.catalogue.intake = map[shared.PathId][]shared.DemandSource{}
	for _, id := range []shared.PathId{"PACK", "PICK", "REBIN", "SLAM"} {
		f.catalogue.eligibility[id] = permissive
		f.catalogue.cycleTimes[id] = 2 * time.Hour
		f.catalogue.intake[id] = nil
	}

	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{{SKU: "SKU-1", Quantity: 1}}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := o.Lines()[0].PathID(); got != shared.DefaultPathId {
		t.Fatalf("line PathID = %q, want %q (fail open to the default, never PACK)", got, shared.DefaultPathId)
	}
}
