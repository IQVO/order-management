package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// TestReceiveOrder_PerGroupPromising covers ADR 0014 §3 / ADR 0017 end to
// end through the real ReceiveOrder use case (not just the domain layer
// unit tests): a partial-shipment order whose two lines fall back to
// DIFFERENT LeadTime instants (the fixture's PromisePolicy has no
// Schedule/Capability wired, so every line hits the fallback -- see
// newFixture's own comment) lands in two separate PromiseGroups, and the
// published OrderAllocated event's Lines[] carry the correct per-line
// promise attribution for each.
func TestReceiveOrder_PerGroupPromising(t *testing.T) {
	t.Run("partial-shipment order splits into two groups by distinct fallback cutoff", testPerGroupPromisingSplitsByFallbackCutoff)
	t.Run("ship-complete order still gets exactly one group, byte-identical to the legacy single promise", testPerGroupPromisingShipCompleteSingleGroup)
}

// receiveTwoPathOrder places the per-group-promising scenario input: a
// two-line order whose "pick" line gets the fixture's 24h default lead
// time and whose "singles" line gets its 6h override — two genuinely
// different fallback instants, no schedule/capability involved.
func receiveTwoPathOrder(t *testing.T, f *fixture, allowPartialShipment bool) *order.Order {
	t.Helper()
	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"),
		line("SKU-2", 1, "singles"),
	}, allowPartialShipment)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return o
}

func testPerGroupPromisingSplitsByFallbackCutoff(t *testing.T) {
	f := newFixture()

	o := receiveTwoPathOrder(t, f, true)
	if o.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want %q (both lines allocate cleanly)", o.Status(), order.StatusReleased)
	}

	groups := o.PromiseGroups()
	if len(groups) != 2 {
		t.Fatalf("PromiseGroups() = %d groups, want 2: %+v", len(groups), groups)
	}

	var pickGroup, singlesGroup *order.PromiseGroup
	for i := range groups {
		switch groups[i].LineNos[0] {
		case 1:
			pickGroup = &groups[i]
		case 2:
			singlesGroup = &groups[i]
		}
	}
	if pickGroup == nil || singlesGroup == nil {
		t.Fatalf("expected one group per line, got %+v", groups)
	}
	wantPickCutoff := f.clock.Now().Add(24 * time.Hour)
	wantSinglesCutoff := f.clock.Now().Add(6 * time.Hour)
	if !pickGroup.Promise.CutoffAt.Equal(wantPickCutoff) {
		t.Errorf("pick group cutoff = %v, want %v", pickGroup.Promise.CutoffAt, wantPickCutoff)
	}
	if !singlesGroup.Promise.CutoffAt.Equal(wantSinglesCutoff) {
		t.Errorf("singles group cutoff = %v, want %v", singlesGroup.Promise.CutoffAt, wantSinglesCutoff)
	}

	// Backward compatibility (ADR 0014 §3): the legacy PromiseDate()
	// must be the LATEST of the two groups' cutoffs -- the "pick"
	// line's 24h, never the earlier 6h "singles" one.
	if got := o.PromiseDate(); got == nil || !got.Equal(wantPickCutoff) {
		t.Fatalf("PromiseDate() = %v, want the latest group's cutoff %v", got, wantPickCutoff)
	}

	assertPerLinePromiseAttribution(t, f, wantPickCutoff, wantSinglesCutoff)
}

// assertPerLinePromiseAttribution checks the published OrderAllocated
// event's Lines[] attribute each line to ITS OWN group's promise detail,
// not the order-level summary.
func assertPerLinePromiseAttribution(t *testing.T, f *fixture, wantPickCutoff, wantSinglesCutoff time.Time) {
	t.Helper()
	allocated := findOrderAllocated(t, f.events)
	if len(allocated.Lines) != 2 {
		t.Fatalf("OrderAllocated.Lines = %v, want 2 entries", allocated.Lines)
	}
	byLineNo := make(map[int]int) // lineNo -> index into allocated.Lines
	for i, l := range allocated.Lines {
		byLineNo[l.LineNo] = i
	}
	pickLine := allocated.Lines[byLineNo[1]]
	singlesLine := allocated.Lines[byLineNo[2]]

	if pickLine.PromiseCutoffAt == nil || !pickLine.PromiseCutoffAt.Equal(wantPickCutoff) {
		t.Errorf("line 1 PromiseCutoffAt = %v, want %v", pickLine.PromiseCutoffAt, wantPickCutoff)
	}
	if singlesLine.PromiseCutoffAt == nil || !singlesLine.PromiseCutoffAt.Equal(wantSinglesCutoff) {
		t.Errorf("line 2 PromiseCutoffAt = %v, want %v", singlesLine.PromiseCutoffAt, wantSinglesCutoff)
	}
	if pickLine.PromiseBasis == nil || *pickLine.PromiseBasis != order.BasisLeadTime.String() {
		t.Errorf("line 1 PromiseBasis = %v, want %q", pickLine.PromiseBasis, order.BasisLeadTime)
	}
	if singlesLine.PromiseBasis == nil || *singlesLine.PromiseBasis != order.BasisLeadTime.String() {
		t.Errorf("line 2 PromiseBasis = %v, want %q", singlesLine.PromiseBasis, order.BasisLeadTime)
	}
	// Neither line has a CPT identity (LeadTime-basis promises have
	// none) -- pointer must be nil, not a pointer to an empty string.
	if pickLine.PromiseCptId != nil {
		t.Errorf("line 1 PromiseCptId = %v, want nil (LeadTime basis has no CPT identity)", *pickLine.PromiseCptId)
	}
	if singlesLine.PromiseCptId != nil {
		t.Errorf("line 2 PromiseCptId = %v, want nil", *singlesLine.PromiseCptId)
	}
}

func testPerGroupPromisingShipCompleteSingleGroup(t *testing.T) {
	f := newFixture()

	o := receiveTwoPathOrder(t, f, false)
	if o.Status() != order.StatusReleased {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusReleased)
	}

	groups := o.PromiseGroups()
	if len(groups) != 1 {
		t.Fatalf("PromiseGroups() = %d groups, want exactly 1 for a ship-complete order: %+v", len(groups), groups)
	}
	if len(groups[0].LineNos) != 2 {
		t.Fatalf("group LineNos = %v, want both lines in the one group", groups[0].LineNos)
	}
	// The slowest line ("pick", 24h) governs -- exactly
	// LeadTimePolicy's existing "slowest allocated line governs"
	// rule, unchanged.
	want := f.clock.Now().Add(24 * time.Hour)
	if got := o.PromiseDate(); got == nil || !got.Equal(want) {
		t.Fatalf("PromiseDate() = %v, want %v", got, want)
	}
}
