package usecases_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

var (
	pcNow   = time.Date(2026, 10, 4, 21, 15, 30, 0, time.UTC)
	pcStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	pcEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	pcAsOf  = time.Date(2026, 10, 4, 21, 45, 10, 0, time.UTC)
)

func pcWindow() order.PlannedCapacityWindow {
	return order.PlannedCapacityWindow{
		PlanID: "plan-0b7a", WarehouseID: "WH-7", Location: "SIM1", PathID: "pick-rebin-pack",
		Start: pcStart, End: pcEnd, AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN", Status: order.PlannedCapacityPublished, AsOf: pcAsOf,
	}
}

// pcTxStore is ONE in-memory store that implements the read model, the
// idempotency gate AND a UnitOfWork with real rollback semantics: Execute
// snapshots both maps and restores them when fn fails, so "a failure leaves
// nothing written and the claim un-recorded" is observable without Postgres.
// (The Postgres equivalent is proven against a real database in the
// integration tests.)
type pcTxStore struct {
	windows map[string]order.PlannedCapacityWindow
	claimed map[string]bool

	upsertErr error // returned by Upsert AFTER the claim succeeded
	claimErr  error
	listErr   error
	listCalls int
	listed    struct {
		location    string
		endingAfter time.Time
	}
}

func newPCTxStore() *pcTxStore {
	return &pcTxStore{windows: map[string]order.PlannedCapacityWindow{}, claimed: map[string]bool{}}
}

func (s *pcTxStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	winSnap := map[string]order.PlannedCapacityWindow{}
	for k, v := range s.windows {
		winSnap[k] = v
	}
	claimSnap := map[string]bool{}
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	if err := fn(ctx); err != nil {
		s.windows, s.claimed = winSnap, claimSnap
		return err
	}
	return nil
}

func (s *pcTxStore) MarkProcessed(_ context.Context, id string) (bool, error) {
	if s.claimErr != nil {
		return false, s.claimErr
	}
	if s.claimed[id] {
		return false, nil
	}
	s.claimed[id] = true
	return true, nil
}

func (s *pcTxStore) Upsert(_ context.Context, w order.PlannedCapacityWindow) (bool, error) {
	if s.upsertErr != nil {
		return false, s.upsertErr
	}
	if prev, ok := s.windows[w.PlanID]; ok && !w.Supersedes(prev) {
		return false, nil
	}
	s.windows[w.PlanID] = w
	return true, nil
}

func (s *pcTxStore) ListByLocation(_ context.Context, location string, endingAfter time.Time) ([]order.PlannedCapacityWindow, error) {
	s.listCalls++
	s.listed.location, s.listed.endingAfter = location, endingAfter
	if s.listErr != nil {
		return nil, s.listErr
	}
	var out []order.PlannedCapacityWindow
	for _, w := range s.windows {
		if w.Location == location && w.End.After(endingAfter) {
			out = append(out, w)
		}
	}
	return out, nil
}

var (
	_ ports.PlannedCapacityRepo            = (*pcTxStore)(nil)
	_ ports.PlannedCapacityProcessedEvents = (*pcTxStore)(nil)
	_ ports.UnitOfWork                     = (*pcTxStore)(nil)
)

func applyUC(s *pcTxStore) *usecases.ApplyPlannedCapacity {
	return &usecases.ApplyPlannedCapacity{Windows: s, Processed: s, UnitOfWork: s}
}

func TestApplyPlannedCapacity_ValidEventUpdatesTheReadModel(t *testing.T) {
	s := newPCTxStore()
	if err := applyUC(s).Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: "evt-3b2a", Window: pcWindow()}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, ok := s.windows["plan-0b7a"]
	if !ok || got.Shortage != 4000 || got.BottleneckStep != "REBIN" || got.Status != order.PlannedCapacityPublished {
		t.Fatalf("read model = %+v (present=%v)", got, ok)
	}
	if !s.claimed["evt-3b2a"] {
		t.Fatal("the event id must be claimed")
	}
}

func TestApplyPlannedCapacity_ReplayedIDIsANoOp(t *testing.T) {
	s := newPCTxStore()
	uc := applyUC(s)
	req := usecases.ApplyPlannedCapacityRequest{EventID: "evt-3b2a", Window: pcWindow()}
	if err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Same id, different content: a replay must not be applied at all.
	changed := pcWindow()
	changed.Shortage, changed.AsOf = 9, pcAsOf.Add(time.Hour)
	if err := uc.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: "evt-3b2a", Window: changed}); err != nil {
		t.Fatalf("replay Execute: %v", err)
	}
	if got := s.windows["plan-0b7a"].Shortage; got != 4000 {
		t.Fatalf("shortage = %v after replay, want the original 4000", got)
	}
}

func TestApplyPlannedCapacity_LastWriterWinsByPlan(t *testing.T) {
	s := newPCTxStore()
	uc := applyUC(s)
	ctx := context.Background()

	newer := pcWindow()
	newer.Shortage, newer.AsOf = 5200, pcAsOf.Add(time.Minute)
	older := pcWindow()
	older.Shortage, older.AsOf = 1100, pcAsOf.Add(-time.Minute)

	for i, w := range []order.PlannedCapacityWindow{newer, older} {
		if err := uc.Execute(ctx, usecases.ApplyPlannedCapacityRequest{EventID: "evt-lww-" + string(rune('a'+i)), Window: w}); err != nil {
			t.Fatalf("Execute %d: %v", i, err)
		}
	}
	if got := s.windows["plan-0b7a"].Shortage; got != 5200 {
		t.Fatalf("shortage = %v, want 5200: the older event arrived later and must not win", got)
	}
	if !s.claimed["evt-lww-b"] {
		t.Fatal("a stale write is still a handled event: its id must be claimed so it is not redelivered forever")
	}
}

func TestApplyPlannedCapacity_InvalidWindowTouchesNothing(t *testing.T) {
	s := newPCTxStore()
	bad := pcWindow()
	bad.End = bad.Start
	err := applyUC(s).Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: "evt-bad", Window: bad})
	if !errors.Is(err, order.ErrInvalidPlannedCapacity) {
		t.Fatalf("err = %v, want ErrInvalidPlannedCapacity", err)
	}
	if len(s.windows) != 0 || len(s.claimed) != 0 {
		t.Fatalf("an invalid message must not touch the store: windows=%v claimed=%v", s.windows, s.claimed)
	}
}

// The transient-failure contract: the Upsert fails AFTER the claim
// succeeded; the whole scope rolls back, so nothing is written and the id is
// NOT recorded — the redelivery is therefore processed, not skipped.
func TestApplyPlannedCapacity_TransientUpsertFailureRollsBackTheClaim(t *testing.T) {
	s := newPCTxStore()
	uc := applyUC(s)
	req := usecases.ApplyPlannedCapacityRequest{EventID: "evt-3b2a", Window: pcWindow()}

	s.upsertErr = errBoom
	if err := uc.Execute(context.Background(), req); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the repository error", err)
	}
	if len(s.windows) != 0 {
		t.Fatalf("nothing may be written, got %v", s.windows)
	}
	if s.claimed["evt-3b2a"] {
		t.Fatal("the claim must be un-recorded, or the redelivery would be skipped as already handled")
	}

	s.upsertErr = nil
	if err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if _, ok := s.windows["plan-0b7a"]; !ok || !s.claimed["evt-3b2a"] {
		t.Fatal("the redelivery must be processed once the repository recovers")
	}
}

func TestApplyPlannedCapacity_ClaimFailureIsReturned(t *testing.T) {
	s := newPCTxStore()
	s.claimErr = errBoom
	err := applyUC(s).Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: "evt-1", Window: pcWindow()})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the claim error", err)
	}
	if len(s.windows) != 0 {
		t.Fatalf("nothing may be written, got %v", s.windows)
	}
}

func TestApplyPlannedCapacity_WorksWithoutAUnitOfWork(t *testing.T) {
	windows, processed := memory.NewPlannedCapacityRepo(), memory.NewPlannedCapacityProcessedEventsRepo()
	uc := &usecases.ApplyPlannedCapacity{Windows: windows, Processed: processed}
	if err := uc.Execute(context.Background(), usecases.ApplyPlannedCapacityRequest{EventID: "evt-1", Window: pcWindow()}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, err := windows.ListByLocation(context.Background(), "SIM1", pcNow)
	if err != nil || len(got) != 1 {
		t.Fatalf("ListByLocation = %v, %v", got, err)
	}
}

func pcPromisedOrder(t *testing.T, cutoff time.Time) *order.Order {
	t.Helper()
	line, err := order.NewOrderLine(1, "SKU-9", 3, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	o, err := order.New("ord-pc-1", []*order.OrderLine{line}, false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := o.Allocate(1, "res-41"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	o.SetPromiseDate(cutoff)
	return o
}

func TestOrderCapacityConstraints_For(t *testing.T) {
	s := newPCTxStore()
	s.windows["plan-0b7a"] = pcWindow()
	uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), SiteID: "SIM1"}

	got, err := uc.For(context.Background(), pcPromisedOrder(t, pcEnd.Add(time.Hour)))
	if err != nil || len(got) != 1 || got[0].PlanID != "plan-0b7a" {
		t.Fatalf("For = %v, %v; want the overlapping plan", got, err)
	}
	if s.listed.location != "SIM1" || !s.listed.endingAfter.Equal(pcNow) {
		t.Fatalf("lookup used (%q, %v), want (SIM1, now)", s.listed.location, s.listed.endingAfter)
	}

	// Cutoff before the window starts: not constrained.
	if got, err := uc.For(context.Background(), pcPromisedOrder(t, pcStart)); err != nil || len(got) != 0 {
		t.Fatalf("For = %v, %v; want no constraint", got, err)
	}
}

func TestOrderCapacityConstraints_OtherSiteIsNotConstrained(t *testing.T) {
	s := newPCTxStore()
	s.windows["plan-0b7a"] = pcWindow()
	uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), SiteID: "SIM2"}
	if got, err := uc.For(context.Background(), pcPromisedOrder(t, pcEnd.Add(time.Hour))); err != nil || len(got) != 0 {
		t.Fatalf("For = %v, %v; want none for another site", got, err)
	}
}

func TestOrderCapacityConstraints_NoPromiseSkipsTheReadModel(t *testing.T) {
	s := newPCTxStore()
	line, _ := order.NewOrderLine(1, "SKU-9", 3, "pick", false)
	o, _ := order.New("ord-pc-2", []*order.OrderLine{line}, false)
	uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), SiteID: "SIM1"}
	got, err := uc.For(context.Background(), o)
	if err != nil || got != nil {
		t.Fatalf("For = %v, %v; want nil, nil", got, err)
	}
	if s.listCalls != 0 {
		t.Fatalf("an order with no promise must not query the read model (calls=%d)", s.listCalls)
	}
}

func TestOrderCapacityConstraints_ReadModelErrorIsReturned(t *testing.T) {
	s := newPCTxStore()
	s.listErr = errBoom
	uc := &usecases.OrderCapacityConstraints{Windows: s, Clock: memory.NewFixedClock(pcNow), SiteID: "SIM1"}
	if _, err := uc.For(context.Background(), pcPromisedOrder(t, pcEnd)); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the read-model error", err)
	}
}

func TestGetPlannedCapacity_DefaultsToNowAndHonoursFrom(t *testing.T) {
	s := newPCTxStore()
	s.windows["plan-0b7a"] = pcWindow()
	uc := &usecases.GetPlannedCapacity{Windows: s, Clock: memory.NewFixedClock(pcNow)}

	if _, err := uc.Execute(context.Background(), "SIM1", nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !s.listed.endingAfter.Equal(pcNow) {
		t.Fatalf("default from = %v, want now %v", s.listed.endingAfter, pcNow)
	}

	from := pcStart.Add(-48 * time.Hour)
	got, err := uc.Execute(context.Background(), "SIM1", &from)
	if err != nil || len(got) != 1 {
		t.Fatalf("Execute = %v, %v", got, err)
	}
	if !s.listed.endingAfter.Equal(from) {
		t.Fatalf("explicit from = %v, want %v", s.listed.endingAfter, from)
	}
}
