package order

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// fakeEligibilitySource is a scripted EligibilitySource for these unit
// tests -- mirrors fakeCatalogue's scripting style used in
// internal/application/usecases's own tests, kept local here since domain
// tests must not depend on the application package. ADR-0021 widens this
// to script ListActive directly (a plain slice of candidates) rather than
// a per-id map, since that is now the only method PathSelectionPolicy
// actually calls.
type fakeEligibilitySource struct {
	active []shared.ActivePathCandidate
}

func (f fakeEligibilitySource) Eligibility(pathID shared.PathId) (shared.Eligibility, bool) {
	for _, c := range f.active {
		if c.PathId == pathID {
			return c.Eligibility, true
		}
	}
	return shared.Eligibility{}, false
}

func (f fakeEligibilitySource) ListActive() []shared.ActivePathCandidate {
	return f.active
}

func candidate(id shared.PathId, cycleTime time.Duration, known bool, e shared.Eligibility) shared.ActivePathCandidate {
	return shared.ActivePathCandidate{PathId: id, CycleTimeP95: cycleTime, CycleTimeKnown: known, Eligibility: e}
}

func TestPathSelectionPolicySelect(t *testing.T) {
	permissiveEligibility := shared.NewEligibility(nil, nil, nil, false)
	maxOne := 1
	singlesEligibility := shared.NewEligibility(&maxOne, nil, nil, false)
	requiresHazmatEligibility := shared.NewEligibility(nil, []string{"hazmat"}, nil, false)
	excludesHazmatEligibility := shared.NewEligibility(nil, nil, []string{"hazmat"}, false)
	excludesGiftWrapEligibility := shared.NewEligibility(nil, nil, []string{"giftWrap"}, false)

	permissive := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, permissiveEligibility),
	}}
	singlesOnly := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, singlesEligibility),
	}}
	requiresHazmat := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, requiresHazmatEligibility),
	}}
	excludesHazmat := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, excludesHazmatEligibility),
	}}
	excludesGiftWrap := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, excludesGiftWrapEligibility),
	}}
	empty := fakeEligibilitySource{}

	tests := []struct {
		name       string
		sku        shared.SKU
		quantity   int
		giftWrap   bool
		attributes []string
		catalogue  EligibilitySource
		wantPathID shared.PathId
		wantOK     bool
	}{
		{
			name: "nil catalogue falls open to the default path (ADR-0013 floor)",
			sku:  "SKU-1", quantity: 1,
			catalogue: nil, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "empty ListActive falls open to the default path",
			sku:  "SKU-1", quantity: 1,
			catalogue: empty, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "permissive eligibility (zero value) admits any line",
			sku:  "SKU-1", quantity: 50, giftWrap: true,
			catalogue: permissive, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "quantity within MaxUnitsPerLine is eligible",
			sku:  "SKU-1", quantity: 1,
			catalogue: singlesOnly, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "quantity exceeding MaxUnitsPerLine is rejected when no other candidate exists",
			sku:  "SKU-1", quantity: 2,
			catalogue: singlesOnly, wantOK: false,
		},
		{
			name: "a required product attribute present is eligible",
			sku:  "SKU-1", quantity: 1, attributes: []string{"hazmat"},
			catalogue: requiresHazmat, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "a required product attribute missing is rejected when no other candidate exists",
			sku:  "SKU-1", quantity: 1,
			catalogue: requiresHazmat, wantOK: false,
		},
		{
			name: "an excluded product attribute present is rejected when no other candidate exists",
			sku:  "SKU-1", quantity: 1, attributes: []string{"hazmat"},
			catalogue: excludesHazmat, wantOK: false,
		},
		{
			name: "no excluded product attribute present is eligible",
			sku:  "SKU-1", quantity: 1,
			catalogue: excludesHazmat, wantPathID: shared.DefaultPathId, wantOK: true,
		},
		{
			name: "giftWrap=true is folded into the evaluated attributes and can be excluded",
			sku:  "SKU-1", quantity: 1, giftWrap: true,
			catalogue: excludesGiftWrap, wantOK: false,
		},
		{
			name: "giftWrap=false never contributes the giftWrap attribute",
			sku:  "SKU-1", quantity: 1, giftWrap: false,
			catalogue: excludesGiftWrap, wantPathID: shared.DefaultPathId, wantOK: true,
		},
	}

	var policy PathSelectionPolicy
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotOK := policy.Select(tt.sku, tt.quantity, tt.giftWrap, tt.attributes, tt.catalogue)
			if gotOK != tt.wantOK {
				t.Fatalf("Select(...) ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotOK && gotID != tt.wantPathID {
				t.Fatalf("Select(...) = %q, want %q", gotID, tt.wantPathID)
			}
			if !gotOK && gotID != "" {
				t.Fatalf("Select(...) on ok=false must return the zero PathId, got %q", gotID)
			}
		})
	}
}

// TestPathSelectionPolicySelect_MultiPath exercises ADR-0021's genuinely
// new behaviour: enumerating and ranking MORE THAN ONE active candidate,
// which ADR-0016 §4 documented as impossible before this change.
func TestPathSelectionPolicySelect_MultiPath(t *testing.T) {
	t.Run("a line ineligible for the default path routes to a different eligible active path", testMultiPathRoutesToEligibleAlternative)
	t.Run("among several eligible candidates the shortest known cycle time wins", testMultiPathShortestKnownCycleTimeWins)
	t.Run("a candidate with unknown cycle time never beats one with a known cycle time", testMultiPathKnownCycleTimeBeatsUnknown)
	t.Run("a cycle-time-unknown candidate is still picked when it is the only eligible one", testMultiPathUnknownCycleTimeAsOnlyCandidate)
	t.Run("a tie on known cycle time breaks on the lower PathId, deterministically", testMultiPathTieBreaksOnLowerPathID)
	t.Run("no active candidate is eligible for the line -- ok=false, zero PathId", testMultiPathNoEligibleCandidateRejects)
}

func testMultiPathRoutesToEligibleAlternative(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 30*time.Minute, true, shared.NewEligibility(nil, []string{"hazmat"}, nil, false)),
		candidate("HAZMAT", 90*time.Minute, true, shared.NewEligibility(nil, nil, nil, false)),
	}}
	var policy PathSelectionPolicy
	// No hazmat attribute -- PICK's RequiredProductAttributes rejects
	// it, but HAZMAT (permissive) still admits it. Before ADR-0021
	// this line would have been rejected outright (ADR-0016 §4).
	gotID, gotOK := policy.Select("SKU-1", 1, false, nil, catalogue)
	if !gotOK {
		t.Fatalf("Select(...) ok = false, want true (HAZMAT path should have admitted the line)")
	}
	if gotID != "HAZMAT" {
		t.Fatalf("Select(...) = %q, want %q", gotID, "HAZMAT")
	}
}

func testMultiPathShortestKnownCycleTimeWins(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 45*time.Minute, true, permissiveMultiPathEligibility()),
		candidate("SINGLES", 20*time.Minute, true, permissiveMultiPathEligibility()),
		candidate("SLOW", 90*time.Minute, true, permissiveMultiPathEligibility()),
	}}
	var policy PathSelectionPolicy
	gotID, gotOK := policy.Select("SKU-1", 1, false, nil, catalogue)
	if !gotOK || gotID != "SINGLES" {
		t.Fatalf("Select(...) = (%q, %v), want (%q, true)", gotID, gotOK, "SINGLES")
	}
}

func testMultiPathKnownCycleTimeBeatsUnknown(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, permissiveMultiPathEligibility()),
		candidate("SINGLES", 999*time.Hour, true, permissiveMultiPathEligibility()),
	}}
	var policy PathSelectionPolicy
	gotID, gotOK := policy.Select("SKU-1", 1, false, nil, catalogue)
	if !gotOK || gotID != "SINGLES" {
		t.Fatalf("Select(...) = (%q, %v), want (%q, true) -- known cycle time must beat unknown", gotID, gotOK, "SINGLES")
	}
}

func testMultiPathUnknownCycleTimeAsOnlyCandidate(t *testing.T) {
	maxOne := 1
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 0, false, shared.NewEligibility(&maxOne, nil, nil, false)),
	}}
	var policy PathSelectionPolicy
	// quantity 5 exceeds singles' MaxUnitsPerLine=1, so DefaultPathId
	// is the only candidate AND it's ineligible -- must reject.
	if _, ok := policy.Select("SKU-1", 5, false, nil, catalogue); ok {
		t.Fatalf("Select(...) ok = true, want false (no eligible candidate)")
	}
	// quantity 1 is within bound: the sole candidate, cycle-time
	// unknown, must still be pickable.
	gotID, gotOK := policy.Select("SKU-1", 1, false, nil, catalogue)
	if !gotOK || gotID != shared.DefaultPathId {
		t.Fatalf("Select(...) = (%q, %v), want (%q, true)", gotID, gotOK, shared.DefaultPathId)
	}
}

func testMultiPathTieBreaksOnLowerPathID(t *testing.T) {
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate("ZULU", 30*time.Minute, true, permissiveMultiPathEligibility()),
		candidate("ALPHA", 30*time.Minute, true, permissiveMultiPathEligibility()),
	}}
	var policy PathSelectionPolicy
	for i := 0; i < 5; i++ {
		gotID, gotOK := policy.Select("SKU-1", 1, false, nil, catalogue)
		if !gotOK || gotID != "ALPHA" {
			t.Fatalf("Select(...) run %d = (%q, %v), want (%q, true)", i, gotID, gotOK, "ALPHA")
		}
	}
}

func testMultiPathNoEligibleCandidateRejects(t *testing.T) {
	maxOne := 1
	catalogue := fakeEligibilitySource{active: []shared.ActivePathCandidate{
		candidate(shared.DefaultPathId, 30*time.Minute, true, shared.NewEligibility(nil, []string{"hazmat"}, nil, false)),
		candidate("SINGLES", 20*time.Minute, true, shared.NewEligibility(&maxOne, nil, nil, false)),
	}}
	var policy PathSelectionPolicy
	gotID, gotOK := policy.Select("SKU-1", 5, false, nil, catalogue)
	if gotOK {
		t.Fatalf("Select(...) ok = true, want false")
	}
	if gotID != "" {
		t.Fatalf("Select(...) on ok=false must return the zero PathId, got %q", gotID)
	}
}

// permissiveMultiPathEligibility is the zero-value Eligibility every
// multi-path candidate in TestPathSelectionPolicySelect_MultiPath uses
// unless a case specifically scripts a constraint.
func permissiveMultiPathEligibility() shared.Eligibility {
	return shared.NewEligibility(nil, nil, nil, false)
}
