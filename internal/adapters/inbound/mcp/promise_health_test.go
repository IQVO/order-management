package mcp

import (
	"testing"
	"time"
)

func TestAggregatePromiseHealth(t *testing.T) {
	bucket1 := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	bucket2 := time.Date(2026, 9, 14, 11, 0, 0, 0, time.UTC)

	rows := []PromiseHealthRow{
		{
			PathID:                    "pick",
			HourBucket:                bucket1,
			PromiseBasisCapability:    3,
			PromiseBasisLeadTime:      1,
			OrdersSplitShipment:       1,
			PromiseToCutoffGapSeconds: 3600,
			PromiseToCutoffGapSamples: 2,
		},
		{
			PathID:                    "pick",
			HourBucket:                bucket2,
			PromiseBasisCapability:    1,
			PromiseBasisLeadTime:      0,
			PromiseBasisNetwork:       2,
			OrdersSplitShipment:       0,
			PromiseToCutoffGapSeconds: 7200,
			PromiseToCutoffGapSamples: 1,
		},
		{
			// Fleet-wide re-promise row: pathID="" per the documented
			// convention (report.Row.OrdersRepromised).
			PathID:           "",
			HourBucket:       bucket1,
			OrdersRepromised: 2,
		},
	}

	got := aggregatePromiseHealth(rows)

	if got.PromiseBasisCapability != 4 {
		t.Errorf("PromiseBasisCapability = %d, want 4", got.PromiseBasisCapability)
	}
	if got.PromiseBasisLeadTime != 1 {
		t.Errorf("PromiseBasisLeadTime = %d, want 1", got.PromiseBasisLeadTime)
	}
	// ADR 0020: promises dictated by an external deadline have their own
	// bucket and count toward the allocation total.
	if got.PromiseBasisNetwork != 2 {
		t.Errorf("PromiseBasisNetwork = %d, want 2", got.PromiseBasisNetwork)
	}
	if got.OrdersAllocatedTotal != 7 {
		t.Errorf("OrdersAllocatedTotal = %d, want 7 (4 + 1 + 2)", got.OrdersAllocatedTotal)
	}
	if got.OrdersSplitShipment != 1 {
		t.Errorf("OrdersSplitShipment = %d, want 1", got.OrdersSplitShipment)
	}
	wantSplitRate := 1.0 / 7.0
	if got.SplitShipmentRate != wantSplitRate {
		t.Errorf("SplitShipmentRate = %v, want %v", got.SplitShipmentRate, wantSplitRate)
	}
	if got.OrdersRepromised != 2 {
		t.Errorf("OrdersRepromised = %d, want 2", got.OrdersRepromised)
	}
	wantRepromiseRate := 2.0 / 7.0
	if got.RepromiseRate != wantRepromiseRate {
		t.Errorf("RepromiseRate = %v, want %v", got.RepromiseRate, wantRepromiseRate)
	}
	// Weighted mean: (3600*2 + 7200*1) / 3 = 4800
	wantGap := 4800.0
	if got.PromiseToCutoffGapSeconds != wantGap {
		t.Errorf("PromiseToCutoffGapSeconds = %v, want %v", got.PromiseToCutoffGapSeconds, wantGap)
	}
}

func TestAggregatePromiseHealth_Empty(t *testing.T) {
	got := aggregatePromiseHealth(nil)
	if got.OrdersAllocatedTotal != 0 || got.SplitShipmentRate != 0 || got.RepromiseRate != 0 || got.PromiseToCutoffGapSeconds != 0 {
		t.Errorf("empty aggregate = %+v, want all zero", got)
	}
}

func TestGetPromiseHealth_QueryAndAggregate(t *testing.T) {
	h := newHarness(t)
	ctx := h.ctx()
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	cutoff := base.Add(4 * time.Hour)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	must(h.reports.ApplyOrderAllocated(ctx, "e1", "pick", base, "Capability", &cutoff, true))
	must(h.reports.ApplyOrderAllocated(ctx, "e2", "pick", base, "LeadTime", nil, false))
	must(h.reports.ApplyOrderRepromised(ctx, "e3", base))

	out, err := h.deps.getPromiseHealth(ctx, promiseHealthInput{
		From: base.Add(-time.Hour).Format(time.RFC3339),
		To:   base.Add(time.Hour).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("getPromiseHealth: %v", err)
	}
	if out.PromiseBasisCapability != 1 || out.PromiseBasisLeadTime != 1 {
		t.Errorf("basis distribution = %+v, want 1/1", out)
	}
	if out.OrdersSplitShipment != 1 {
		t.Errorf("OrdersSplitShipment = %d, want 1", out.OrdersSplitShipment)
	}
	if out.OrdersRepromised != 1 {
		t.Errorf("OrdersRepromised = %d, want 1", out.OrdersRepromised)
	}
	if out.PromiseToCutoffGapSeconds != 4*3600 {
		t.Errorf("PromiseToCutoffGapSeconds = %v, want %v", out.PromiseToCutoffGapSeconds, 4*3600)
	}
}

func TestGetPromiseHealth_InvalidTimestamps(t *testing.T) {
	h := newHarness(t)
	tests := []struct {
		name string
		in   promiseHealthInput
	}{
		{"bad from", promiseHealthInput{From: "nope", To: "2026-09-14T10:00:00Z"}},
		{"bad to", promiseHealthInput{From: "2026-09-14T09:00:00Z", To: "nope"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := h.deps.getPromiseHealth(h.ctx(), tt.in); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}
