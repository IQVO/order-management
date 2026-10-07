package productclassificationcopy_test

import (
	"context"
	"testing"

	"github.com/claudioed/order-management/internal/adapters/outbound/productclassificationcopy"
	"github.com/claudioed/order-management/internal/application/ports"
)

func TestMemoryStore_FoundNotFoundAndVersionGuard(t *testing.T) {
	ctx := context.Background()
	s := productclassificationcopy.NewMemoryStore()

	got, err := s.GetClassification(ctx, "SKU-1")
	if err != nil || got.Known || got.SKU != "SKU-1" || got.HandlingTags != nil {
		t.Fatalf("unknown SKU = %+v, %v; want Known=false, nil error (the HTTP client's 404 shape)", got, err)
	}

	steps := []struct {
		rec         ports.ProductClassificationRecord
		wantApplied bool
		wantTags    []string
	}{
		{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Fragile"}, Version: 2}, true, []string{"Fragile"}},
		{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Hazmat", "TemperatureSensitive"}, Version: 3}, true, []string{"Hazmat", "TemperatureSensitive"}},
		{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"HighValue"}, Version: 3}, false, []string{"Hazmat", "TemperatureSensitive"}},
		{ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: []string{"Oversized"}, Version: 1}, false, []string{"Hazmat", "TemperatureSensitive"}},
	}
	for i, st := range steps {
		applied, err := s.Upsert(ctx, st.rec)
		if err != nil || applied != st.wantApplied {
			t.Fatalf("step %d: Upsert = %v, %v; want applied=%v", i, applied, err, st.wantApplied)
		}
		got, err := s.GetClassification(ctx, "SKU-1")
		if err != nil || !got.Known || !equal(got.HandlingTags, st.wantTags) {
			t.Fatalf("step %d: lookup = %+v, %v; want Known=true %v", i, got, err, st.wantTags)
		}
	}

	if other, _ := s.GetClassification(ctx, "SKU-2"); other.Known {
		t.Fatal("rows are keyed by SKU")
	}
}

func TestMemoryStore_DoesNotAliasCallerSlices(t *testing.T) {
	ctx := context.Background()
	s := productclassificationcopy.NewMemoryStore()
	tags := []string{"Hazmat"}
	if _, err := s.Upsert(ctx, ports.ProductClassificationRecord{SKU: "SKU-1", HandlingTags: tags, Version: 1}); err != nil {
		t.Fatal(err)
	}
	tags[0] = "mutated"
	got, _ := s.GetClassification(ctx, "SKU-1")
	got.HandlingTags[0] = "mutated-too"
	again, _ := s.GetClassification(ctx, "SKU-1")
	if again.HandlingTags[0] != "Hazmat" {
		t.Fatalf("stored tags = %v, want the copy isolated from callers", again.HandlingTags)
	}
}

func TestMemoryProcessedEvents_ClaimsOnce(t *testing.T) {
	ctx := context.Background()
	p := productclassificationcopy.NewMemoryProcessedEvents()
	if first, err := p.MarkProcessed(ctx, "evt-1"); err != nil || !first {
		t.Fatalf("first claim = %v, %v", first, err)
	}
	if again, err := p.MarkProcessed(ctx, "evt-1"); err != nil || again {
		t.Fatalf("second claim = %v, %v; want false", again, err)
	}
	if other, _ := p.MarkProcessed(ctx, "evt-2"); !other {
		t.Fatal("another id is a new claim")
	}
}

func TestPermissiveLookup_AlwaysUnknown(t *testing.T) {
	got, err := productclassificationcopy.NewPermissiveLookup().GetClassification(context.Background(), "SKU-1")
	if err != nil || got.Known || got.SKU != "SKU-1" {
		t.Fatalf("permissive = %+v, %v; want Known=false", got, err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
