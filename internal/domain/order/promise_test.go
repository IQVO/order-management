package order_test

import (
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func testTime() time.Time {
	return time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
}

func TestNewLeadTimePolicyNormalisesConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		defaultLead time.Duration
		perPath     map[shared.PathId]time.Duration
		lookup      shared.PathId
		want        time.Duration
	}{
		{
			name:   "a zero default falls back to DefaultLeadTime",
			lookup: "pick", want: order.DefaultLeadTime,
		},
		{
			name: "a negative default falls back to DefaultLeadTime", defaultLead: -time.Hour,
			lookup: "pick", want: order.DefaultLeadTime,
		},
		{
			name: "an explicit default is honoured", defaultLead: 12 * time.Hour,
			lookup: "pick", want: 12 * time.Hour,
		},
		{
			name: "a per-path override wins over the default", defaultLead: 12 * time.Hour,
			perPath: map[shared.PathId]time.Duration{"singles": 6 * time.Hour},
			lookup:  "singles", want: 6 * time.Hour,
		},
		{
			name: "a non-positive override is discarded, not trusted", defaultLead: 12 * time.Hour,
			perPath: map[shared.PathId]time.Duration{"singles": -6 * time.Hour},
			lookup:  "singles", want: 12 * time.Hour,
		},
		{
			name: "an unlisted path gets the default", defaultLead: 12 * time.Hour,
			perPath: map[shared.PathId]time.Duration{"singles": 6 * time.Hour},
			lookup:  "multis", want: 12 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := order.NewLeadTimePolicy(tt.defaultLead, tt.perPath)
			if got := policy.LeadTimeFor(tt.lookup); got != tt.want {
				t.Fatalf("LeadTimeFor(%q) = %v, want %v", tt.lookup, got, tt.want)
			}
		})
	}
}

func TestLeadTimeForOnAZeroValuePolicy(t *testing.T) {
	var policy order.LeadTimePolicy
	if got := policy.LeadTimeFor("pick"); got != order.DefaultLeadTime {
		t.Fatalf("LeadTimeFor = %v, want %v", got, order.DefaultLeadTime)
	}
}

func TestPromiseDateUsesTheSlowestAllocatedLine(t *testing.T) {
	now := testTime()
	policy := order.NewLeadTimePolicy(12*time.Hour, map[shared.PathId]time.Duration{
		"singles": 6 * time.Hour,
		"multis":  36 * time.Hour,
	})

	tests := []struct {
		name         string
		paths        []shared.PathId
		lineStatuses []order.LineStatus
		wantOK       bool
		wantLead     time.Duration
	}{
		{
			name:         "the slowest allocated path governs",
			paths:        []shared.PathId{"singles", "multis"},
			lineStatuses: []order.LineStatus{order.LineAllocated, order.LineAllocated},
			wantOK:       true, wantLead: 36 * time.Hour,
		},
		{
			name:         "an unallocated slow line does not pull the promise out",
			paths:        []shared.PathId{"singles", "multis"},
			lineStatuses: []order.LineStatus{order.LineAllocated, order.LineBackordered},
			wantOK:       true, wantLead: 6 * time.Hour,
		},
		{
			name:         "an already-released line still counts",
			paths:        []shared.PathId{"multis", "singles"},
			lineStatuses: []order.LineStatus{order.LineReleased, order.LinePending},
			wantOK:       true, wantLead: 36 * time.Hour,
		},
		{
			name:         "an unconfigured path gets the default lead time",
			paths:        []shared.PathId{"pick"},
			lineStatuses: []order.LineStatus{order.LineAllocated},
			wantOK:       true, wantLead: 12 * time.Hour,
		},
		{
			name:         "nothing allocated means no promise date",
			paths:        []shared.PathId{"singles", "multis"},
			lineStatuses: []order.LineStatus{order.LineBackordered, order.LinePending},
			wantOK:       false,
		},
		{
			name:         "a cancelled order has no promise date",
			paths:        []shared.PathId{"singles"},
			lineStatuses: []order.LineStatus{order.LineCancelled},
			wantOK:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := make([]*order.OrderLine, 0, len(tt.paths))
			for i := range tt.paths {
				lines = append(lines, order.RehydrateOrderLine(
					i+1, "SKU-1", 1, tt.paths[i], false, tt.lineStatuses[i], nil,
				))
			}
			o := order.Rehydrate(order.OrderSnapshot{ID: "ord-1", Lines: lines, AllowPartialShipment: true})

			got, ok := policy.PromiseDate(now, o)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				if !got.IsZero() {
					t.Fatalf("expected the zero time when there is no promise date, got %v", got)
				}
				return
			}
			want := now.Add(tt.wantLead)
			if !got.Equal(want) {
				t.Fatalf("PromiseDate = %v, want %v", got, want)
			}
		})
	}
}
