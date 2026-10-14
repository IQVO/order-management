package order

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// ADR-0042 (process-path-management ADR 0034, decision E5): a customer
// order line is routed only onto paths whose intake contains
// CUSTOMER_ORDER. Paths with another or an empty intake (downstream stages
// PACK/REBIN/SLAM, RELOCATE, inventory-control families) are never
// candidates. When no active path declares CUSTOMER_ORDER (catalogue not
// seeded yet, or a producer older than ADR 0034), the policy fails open to
// shared.DefaultPathId exactly like the empty-catalogue rule.

func pathWithIntake(id shared.PathId, cycleTime time.Duration, intake ...shared.DemandSource) shared.ActivePathCandidate {
	return shared.ActivePathCandidate{PathId: id, CycleTimeP95: cycleTime, CycleTimeKnown: true,
		Eligibility: shared.NewEligibility(nil, nil, nil, false), Intake: intake}
}

func selectFor(t *testing.T, quantity int, catalogue EligibilitySource) (shared.PathId, bool) {
	t.Helper()
	var policy PathSelectionPolicy
	return policy.Select("SKU-1", quantity, false, nil, catalogue)
}

// TestPathSelection_LiveCatalogueWithoutIntake_FallsBackToDefaultNotPack is
// the regression E5 found on the live catalogue: PACK, PICK, REBIN and SLAM
// all at 2h with permissive eligibility and no intake. Before ADR-0042 the
// tie broke on the lowest PathId and a customer order line went to PACK.
func TestPathSelection_LiveCatalogueWithoutIntake_FallsBackToDefaultNotPack(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		pathWithIntake("PACK", 2*time.Hour),
		pathWithIntake("PICK", 2*time.Hour),
		pathWithIntake("REBIN", 2*time.Hour),
		pathWithIntake("SLAM", 2*time.Hour),
	}}
	gotID, gotOK := selectFor(t, 1, catalogue)
	if !gotOK || gotID != shared.DefaultPathId {
		t.Fatalf("Select = (%q, %v), want (%q, true): with no CUSTOMER_ORDER path the policy fails open to the default, never PACK", gotID, gotOK, shared.DefaultPathId)
	}
}

func TestPathSelection_OnlyCustomerOrderPathsAreCandidates(t *testing.T) {
	// The seeded catalogue: PICK is the one CUSTOMER_ORDER entry point;
	// PACK sorts first and RELOCATE is faster, but neither may take an
	// order line.
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		pathWithIntake("PACK", 2*time.Hour),
		pathWithIntake("PICK", 2*time.Hour, shared.DemandSourceCustomerOrder),
		pathWithIntake("REBIN", 2*time.Hour),
		pathWithIntake("RELOCATE", 10*time.Minute, shared.DemandSource("SLOT_RELOCATION")),
		pathWithIntake("AMNESTY", time.Minute, shared.DemandSource("INVENTORY_CONTROL")),
	}}
	gotID, gotOK := selectFor(t, 1, catalogue)
	if !gotOK || gotID != "PICK" {
		t.Fatalf("Select = (%q, %v), want (PICK, true)", gotID, gotOK)
	}
}

func TestPathSelection_RanksOnlyAmongCustomerOrderPaths(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		pathWithIntake("PICK", 2*time.Hour, shared.DemandSourceCustomerOrder),
		pathWithIntake("SINGLES", 30*time.Minute, shared.DemandSource("STOCK_TRANSFER"), shared.DemandSourceCustomerOrder),
		pathWithIntake("FAST", time.Minute, shared.DemandSource("STOCK_TRANSFER")),
	}}
	gotID, gotOK := selectFor(t, 1, catalogue)
	if !gotOK || gotID != "SINGLES" {
		t.Fatalf("Select = (%q, %v), want (SINGLES, true): the fastest CUSTOMER_ORDER path wins, a faster non-order path does not", gotID, gotOK)
	}
}

func TestPathSelection_CustomerOrderPathsDeclaredButNoneEligible_Rejects(t *testing.T) {
	// Intake narrows the candidates; it does not turn an eligibility
	// rejection into the fail-open default.
	maxOne := 1
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		{PathId: "PICK", CycleTimeP95: 2 * time.Hour, CycleTimeKnown: true,
			Eligibility: shared.NewEligibility(&maxOne, nil, nil, false),
			Intake:      []shared.DemandSource{shared.DemandSourceCustomerOrder}},
		pathWithIntake("PACK", 2*time.Hour),
	}}
	gotID, gotOK := selectFor(t, 5, catalogue)
	if gotOK || gotID != "" {
		t.Fatalf("Select = (%q, %v), want (\"\", false): the only CUSTOMER_ORDER path is ineligible and PACK is not a candidate", gotID, gotOK)
	}
}

func TestPathSelection_UnknownDemandSourcesAreIgnored(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		pathWithIntake("ALPHA", time.Minute, shared.DemandSource("SOMETHING_NEW")),
		pathWithIntake("PICK", 2*time.Hour, shared.DemandSource("SOMETHING_NEW"), shared.DemandSourceCustomerOrder),
	}}
	gotID, gotOK := selectFor(t, 1, catalogue)
	if !gotOK || gotID != "PICK" {
		t.Fatalf("Select = (%q, %v), want (PICK, true)", gotID, gotOK)
	}
}

func TestActivePathCandidate_AcceptsDemandSource(t *testing.T) {
	c := pathWithIntake("PICK", time.Hour, shared.DemandSource("STOCK_TRANSFER"), shared.DemandSourceCustomerOrder)
	if !c.AcceptsDemandSource(shared.DemandSourceCustomerOrder) {
		t.Fatal("AcceptsDemandSource(CUSTOMER_ORDER) = false for an intake that lists it")
	}
	if pathWithIntake("PACK", time.Hour).AcceptsDemandSource(shared.DemandSourceCustomerOrder) {
		t.Fatal("AcceptsDemandSource(CUSTOMER_ORDER) = true for the empty intake")
	}
}
