package usecases_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// pcCopyStore is ONE in-memory store implementing the copy, the
// idempotency gate AND a UnitOfWork with real rollback: Execute snapshots
// both maps and restores them when fn fails, so "a failure leaves nothing
// written and the claim un-recorded" is observable without Postgres.
type pcCopyStore struct {
	rows    map[string]ports.ProductClassificationRecord
	claimed map[string]bool

	upsertErr error
	claimErr  error
	upserts   int
}

func newPCCopyStore() *pcCopyStore {
	return &pcCopyStore{rows: map[string]ports.ProductClassificationRecord{}, claimed: map[string]bool{}}
}

func (s *pcCopyStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	rowSnap := map[string]ports.ProductClassificationRecord{}
	for k, v := range s.rows {
		rowSnap[k] = v
	}
	claimSnap := map[string]bool{}
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	if err := fn(ctx); err != nil {
		s.rows, s.claimed = rowSnap, claimSnap
		return err
	}
	return nil
}

func (s *pcCopyStore) MarkProcessed(_ context.Context, id string) (bool, error) {
	if s.claimErr != nil {
		return false, s.claimErr
	}
	if s.claimed[id] {
		return false, nil
	}
	s.claimed[id] = true
	return true, nil
}

func (s *pcCopyStore) Upsert(_ context.Context, rec ports.ProductClassificationRecord) (bool, error) {
	s.upserts++
	if s.upsertErr != nil {
		return false, s.upsertErr
	}
	if prev, ok := s.rows[rec.SKU]; ok && prev.Version >= rec.Version {
		return false, nil
	}
	s.rows[rec.SKU] = rec
	return true, nil
}

var (
	_ ports.ProductClassificationCopy            = (*pcCopyStore)(nil)
	_ ports.ProductClassificationProcessedEvents = (*pcCopyStore)(nil)
	_ ports.UnitOfWork                           = (*pcCopyStore)(nil)
)

func classRecord(version int64, tags ...string) ports.ProductClassificationRecord {
	return ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: tags, TemperatureClass: "Frozen", DotHazardClass: 3, Version: version}
}

func applyClassUC(s *pcCopyStore) *usecases.ApplyProductClassification {
	return &usecases.ApplyProductClassification{
		Copy: s, Processed: s, UnitOfWork: s,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestApplyProductClassification_FirstEventInsertsTheRow(t *testing.T) {
	s := newPCCopyStore()
	rec := classRecord(3, "Hazmat", "TemperatureSensitive")
	if err := applyClassUC(s).Execute(context.Background(), usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: rec}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, ok := s.rows["SKU-1"]
	if !ok || got.Version != 3 || len(got.HandlingTags) != 2 || got.HandlingTags[0] != "Hazmat" || got.TemperatureClass != "Frozen" || got.DotHazardClass != 3 {
		t.Fatalf("copy = %+v (present=%v)", got, ok)
	}
	if !s.claimed["evt-1"] {
		t.Fatal("the event id must be claimed")
	}
}

func TestApplyProductClassification_HigherVersionReplacesLowerOrEqualIsIgnored(t *testing.T) {
	s := newPCCopyStore()
	uc := applyClassUC(s)
	ctx := context.Background()
	steps := []struct {
		id      string
		rec     ports.ProductClassificationRecord
		wantVer int64
		wantTag string
	}{
		{"evt-v2", classRecord(2, "Fragile"), 2, "Fragile"},
		{"evt-v5", classRecord(5, "Hazmat"), 5, "Hazmat"},
		{"evt-v4", classRecord(4, "HighValue"), 5, "Hazmat"},  // stale: arrived late
		{"evt-v5b", classRecord(5, "Oversized"), 5, "Hazmat"}, // equal: not newer
	}
	for _, st := range steps {
		if err := uc.Execute(ctx, usecases.ApplyProductClassificationRequest{EventID: st.id, Record: st.rec}); err != nil {
			t.Fatalf("%s: %v", st.id, err)
		}
		got := s.rows["SKU-1"]
		if got.Version != st.wantVer || got.HandlingTags[0] != st.wantTag {
			t.Fatalf("after %s copy = %+v, want version %d tag %s", st.id, got, st.wantVer, st.wantTag)
		}
		if !s.claimed[st.id] {
			t.Fatalf("%s: a stale event is still handled, its id must be claimed", st.id)
		}
	}
}

func TestApplyProductClassification_ReplayedIDIsANoOp(t *testing.T) {
	s := newPCCopyStore()
	uc := applyClassUC(s)
	if err := uc.Execute(context.Background(), usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: classRecord(1, "Hazmat")}); err != nil {
		t.Fatal(err)
	}
	if err := uc.Execute(context.Background(), usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: classRecord(9, "Fragile")}); err != nil {
		t.Fatal(err)
	}
	if got := s.rows["SKU-1"]; got.Version != 1 || s.upserts != 1 {
		t.Fatalf("copy = %+v upserts=%d: a replayed id must not reach the copy", got, s.upserts)
	}
}

func TestApplyProductClassification_InvalidRequestTouchesNothing(t *testing.T) {
	tests := map[string]usecases.ApplyProductClassificationRequest{
		"empty event id": {EventID: "", Record: classRecord(1, "Hazmat")},
		"empty sku":      {EventID: "evt-1", Record: ports.ProductClassificationRecord{Version: 1}},
		"version zero":   {EventID: "evt-1", Record: classRecord(0, "Hazmat")},
		"negative":       {EventID: "evt-1", Record: classRecord(-3, "Hazmat")},
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			s := newPCCopyStore()
			err := applyClassUC(s).Execute(context.Background(), req)
			if !errors.Is(err, usecases.ErrInvalidProductClassification) {
				t.Fatalf("err = %v, want ErrInvalidProductClassification", err)
			}
			if len(s.rows) != 0 || len(s.claimed) != 0 || s.upserts != 0 {
				t.Fatal("an invalid request must not touch the store")
			}
		})
	}
}

func TestApplyProductClassification_VersionOneIsValid(t *testing.T) {
	if err := (usecases.ApplyProductClassificationRequest{EventID: "e", Record: classRecord(1)}).Validate(); err != nil {
		t.Fatalf("version 1 is the first valid version: %v", err)
	}
}

func TestApplyProductClassification_TransientUpsertFailureRollsBackTheClaim(t *testing.T) {
	s := newPCCopyStore()
	uc := applyClassUC(s)
	req := usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: classRecord(1, "Hazmat")}

	s.upsertErr = errBoom
	if err := uc.Execute(context.Background(), req); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the repository error", err)
	}
	if len(s.rows) != 0 || s.claimed["evt-1"] {
		t.Fatal("a failed upsert must leave nothing written and the claim un-recorded")
	}

	s.upsertErr = nil
	if err := uc.Execute(context.Background(), req); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if _, ok := s.rows["SKU-1"]; !ok || !s.claimed["evt-1"] {
		t.Fatal("the redelivery must be applied once the repository recovers")
	}
}

func TestApplyProductClassification_ClaimFailureIsReturned(t *testing.T) {
	s := newPCCopyStore()
	s.claimErr = errBoom
	err := applyClassUC(s).Execute(context.Background(), usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: classRecord(1, "Hazmat")})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the claim error", err)
	}
	if s.upserts != 0 {
		t.Fatal("no upsert may run when the claim failed")
	}
}

func TestApplyProductClassification_WorksWithoutAUnitOfWorkOrLogger(t *testing.T) {
	s := newPCCopyStore()
	uc := &usecases.ApplyProductClassification{Copy: s, Processed: s}
	ctx := context.Background()
	if err := uc.Execute(ctx, usecases.ApplyProductClassificationRequest{EventID: "evt-2", Record: classRecord(2, "Hazmat")}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// A stale version and a duplicate id with no Logger must not panic.
	if err := uc.Execute(ctx, usecases.ApplyProductClassificationRequest{EventID: "evt-1", Record: classRecord(1, "Fragile")}); err != nil {
		t.Fatalf("stale: %v", err)
	}
	if err := uc.Execute(ctx, usecases.ApplyProductClassificationRequest{EventID: "evt-2", Record: classRecord(3, "Fragile")}); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if got := s.rows["SKU-1"]; got.Version != 2 {
		t.Fatalf("copy version = %d, want 2", got.Version)
	}
}
