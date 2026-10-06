package usecases_test

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Tests for ADR 0020 §2 on the re-promise path: an order carrying an
// externally-dictated requiredShipBy must be re-promised by FeasibleBy
// (BasisNetwork, never past the deadline), exactly like first-time
// promising — not by PromiseGroups, which would answer on the
// Capability/LeadTime basis and may land after the deadline.

func deadlinePolicy(windows ...order.CPTWindow) order.PromisePolicy {
	return order.PromisePolicy{
		Schedule:   &deadlineSchedule{windows: windows},
		Capability: &deadlineCapability{cycleTimes: map[shared.PathId]time.Duration{"singles": time.Hour}},
		Fallback:   order.NewLeadTimePolicy(24*time.Hour, nil),
		SiteId:     "site-1",
	}
}

func receiveDeadlineOrder(t *testing.T, rf *repromiseFixture, deadline time.Time, windows ...order.CPTWindow) *order.Order {
	t.Helper()
	rf.f.promise = deadlinePolicy(windows...)
	o, err := rf.f.receiveOrder().ExecuteWithDeadline(context.Background(),
		[]usecases.NewLine{deadlineLine()}, false, true, &deadline)
	if err != nil {
		t.Fatalf("ExecuteWithDeadline: %v", err)
	}
	if b := o.PromiseBasis(); b == nil || *b != order.BasisNetwork {
		t.Fatalf("intake basis = %v, want BasisNetwork", b)
	}
	rf.f.events.reset()
	return o
}

func TestRepromiseOrder_DeadlineOrder_StaysNetworkBasisAndWithinDeadline(t *testing.T) {
	rf := newRepromiseFixture()
	deadline := now().Add(8 * time.Hour)
	o := receiveDeadlineOrder(t, rf, deadline,
		order.CPTWindow{CptId: "sp1-early", CutoffAt: now().Add(2 * time.Hour), EligiblePathIds: []string{"singles"}},
		order.CPTWindow{CptId: "sp1-late", CutoffAt: now().Add(6 * time.Hour), EligiblePathIds: []string{"singles"}},
	)
	if o.PromiseCptId() == nil || *o.PromiseCptId() != "sp1-late" {
		t.Fatalf("intake promised CPT = %v, want sp1-late", o.PromiseCptId())
	}

	// A new, later window appears (still before the deadline). The
	// ordinary PromiseGroups search would answer with the EARLIEST
	// window on BasisCapability; FeasibleBy answers with the LATEST
	// qualifying one on BasisNetwork. Only the latter honours the deadline.
	newCutoff := now().Add(7 * time.Hour)
	refreshed := deadlinePolicy(
		order.CPTWindow{CptId: "sp1-early", CutoffAt: now().Add(2 * time.Hour), EligiblePathIds: []string{"singles"}},
		order.CPTWindow{CptId: "sp1-late", CutoffAt: now().Add(6 * time.Hour), EligiblePathIds: []string{"singles"}},
		order.CPTWindow{CptId: "sp1-1900", CutoffAt: newCutoff, EligiblePathIds: []string{"singles"}},
	)

	err := rf.repromiseOrder(refreshed).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	stored, err := rf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	if b := stored.PromiseBasis(); b == nil || *b != order.BasisNetwork {
		t.Fatalf("basis after repromise = %v, want BasisNetwork (the deadline must survive)", b)
	}
	if d := stored.PromiseDate(); d == nil || !d.Equal(newCutoff) || d.After(deadline) {
		t.Fatalf("promiseDate after repromise = %v, want %v and not after %v", d, newCutoff, deadline)
	}
	if got := stored.RequiredShipBy(); got == nil || !got.Equal(deadline) {
		t.Fatalf("requiredShipBy = %v, want %v", got, deadline)
	}

	repromised := findOrderRepromised(t, rf.f.events)
	if repromised.CptIdOld != "sp1-late" || repromised.CptIdNew != "sp1-1900" {
		t.Errorf("OrderRepromised CptIdOld/New = %q/%q, want sp1-late/sp1-1900", repromised.CptIdOld, repromised.CptIdNew)
	}
}

func TestRepromiseOrder_DeadlineOrder_InfeasibleKeepsPromiseAndPublishesNothing(t *testing.T) {
	rf := newRepromiseFixture()
	deadline := now().Add(8 * time.Hour)
	cutoff := now().Add(6 * time.Hour)
	o := receiveDeadlineOrder(t, rf, deadline,
		order.CPTWindow{CptId: "sp1-1800", CutoffAt: cutoff, EligiblePathIds: []string{"singles"}},
	)

	// The only window now left is AFTER the deadline. PromiseGroups
	// would happily re-promise onto it (BasisCapability, 20h out);
	// FeasibleBy refuses — same outcome as intake: no new promise.
	late := deadlinePolicy(
		order.CPTWindow{CptId: "sp1-late", CutoffAt: now().Add(20 * time.Hour), EligiblePathIds: []string{"singles"}},
	)

	err := rf.repromiseOrder(late).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	})
	if err != nil {
		t.Fatalf("Execute: %v, want nil (fail-soft)", err)
	}

	assertEventNames(t, rf.f.events) // nothing published
	stored, err := rf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	if d := stored.PromiseDate(); d == nil || !d.Equal(cutoff) {
		t.Fatalf("promiseDate = %v, want the unchanged intake cutoff %v", d, cutoff)
	}
	if b := stored.PromiseBasis(); b == nil || *b != order.BasisNetwork {
		t.Fatalf("basis = %v, want BasisNetwork unchanged", b)
	}
}

func TestRepromiseOrder_NoDeadlineOrder_StillUsesPromiseGroups(t *testing.T) {
	rf := newRepromiseFixture()
	o := rf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if o.RequiredShipBy() != nil {
		t.Fatal("fixture order must carry no deadline")
	}
	moved := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(6*time.Hour, nil)}
	if err := rf.repromiseOrder(moved).Execute(context.Background(), usecases.RepromiseOrderRequest{
		SourceEventId: "evt-1", OrderId: o.ID(), LineNo: 1, Reason: "TaskCPTMissed",
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	stored, _ := rf.f.orders.FindByID(context.Background(), o.ID())
	if b := stored.PromiseBasis(); b == nil || *b != order.BasisLeadTime {
		t.Fatalf("basis = %v, want BasisLeadTime for an order without a deadline", b)
	}
}
