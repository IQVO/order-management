package shared_test

import (
	"testing"

	"github.com/claudioed/order-management/internal/domain/shared"
)

func intPtr(n int) *int { return &n }

// TestEligibilityEqual pins the value object's equality contract: two
// Eligibility rules are the same rule only when every component — the
// nonSortable flag, the maxUnitsPerLine bound (nil meaning unbounded),
// and BOTH attribute lists, element-wise and in order — agrees. nil and
// empty slices compare equal (both mean "no constraint"), and nil and 0
// pointers do NOT (nil is unbounded, 0 is a bound of zero).
func TestEligibilityEqual(t *testing.T) {
	tests := []struct {
		name  string
		a     shared.Eligibility
		b     shared.Eligibility
		equal bool
	}{
		{
			name:  "zero values are equal",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(nil, nil, nil, false),
			equal: true,
		},
		{
			name:  "identical rules are equal",
			a:     shared.NewEligibility(intPtr(1), []string{"hazmat"}, []string{"giftWrap"}, true),
			b:     shared.NewEligibility(intPtr(1), []string{"hazmat"}, []string{"giftWrap"}, true),
			equal: true,
		},
		{
			name:  "nil max equals nil max",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(nil, nil, nil, false),
			equal: true,
		},
		{
			name:  "nil max differs from zero max (unbounded vs bounded-at-zero)",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(intPtr(0), nil, nil, false),
			equal: false,
		},
		{
			name:  "different max values differ",
			a:     shared.NewEligibility(intPtr(1), nil, nil, false),
			b:     shared.NewEligibility(intPtr(2), nil, nil, false),
			equal: false,
		},
		{
			name:  "nil max differs from set max on either side",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(intPtr(5), nil, nil, false),
			equal: false,
		},
		{
			name:  "nonSortable flag differs",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(nil, nil, nil, true),
			equal: false,
		},
		{
			name:  "required attributes nil equals empty",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(nil, []string{}, nil, false),
			equal: true,
		},
		{
			name:  "required attributes differing lengths differ",
			a:     shared.NewEligibility(nil, []string{"hazmat"}, nil, false),
			b:     shared.NewEligibility(nil, []string{"hazmat", "fragile"}, nil, false),
			equal: false,
		},
		{
			name:  "required attributes same length different element differ",
			a:     shared.NewEligibility(nil, []string{"hazmat"}, nil, false),
			b:     shared.NewEligibility(nil, []string{"fragile"}, nil, false),
			equal: false,
		},
		{
			name:  "required attributes are order sensitive",
			a:     shared.NewEligibility(nil, []string{"hazmat", "fragile"}, nil, false),
			b:     shared.NewEligibility(nil, []string{"fragile", "hazmat"}, nil, false),
			equal: false,
		},
		{
			name:  "excluded attributes nil equals empty",
			a:     shared.NewEligibility(nil, nil, nil, false),
			b:     shared.NewEligibility(nil, nil, []string{}, false),
			equal: true,
		},
		{
			name:  "excluded attributes differing lengths differ",
			a:     shared.NewEligibility(nil, nil, []string{"hazmat"}, false),
			b:     shared.NewEligibility(nil, nil, nil, false),
			equal: false,
		},
		{
			name:  "excluded attributes same length different element differ",
			a:     shared.NewEligibility(nil, nil, []string{"hazmat"}, false),
			b:     shared.NewEligibility(nil, nil, []string{"giftWrap"}, false),
			equal: false,
		},
		{
			name:  "excluded attributes are order sensitive",
			a:     shared.NewEligibility(nil, nil, []string{"hazmat", "fragile"}, false),
			b:     shared.NewEligibility(nil, nil, []string{"fragile", "hazmat"}, false),
			equal: false,
		},
		{
			name:  "first difference short-circuits later equal components",
			a:     shared.NewEligibility(intPtr(1), []string{"hazmat"}, []string{"giftWrap"}, false),
			b:     shared.NewEligibility(intPtr(2), []string{"hazmat"}, []string{"giftWrap"}, false),
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equal(tt.b); got != tt.equal {
				t.Fatalf("a.Equal(b) = %v, want %v", got, tt.equal)
			}
			if got := tt.b.Equal(tt.a); got != tt.equal {
				t.Fatalf("b.Equal(a) = %v, want %v (Equal must be symmetric)", got, tt.equal)
			}
		})
	}
}

// TestEligibilityNonSortable pins the nonSortable accessor's round trip
// through the constructor for both freight kinds — the accessor is the
// read side of the mirror contract even though no caller in this service
// evaluates it for routing yet (see Eligibility's and
// order.lineEligible's doc comments).
func TestEligibilityNonSortable(t *testing.T) {
	tests := []struct {
		name        string
		nonSortable bool
	}{
		{name: "sortable path", nonSortable: false},
		{name: "non-sortable freight path", nonSortable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := shared.NewEligibility(nil, nil, nil, tt.nonSortable)
			if got := e.NonSortable(); got != tt.nonSortable {
				t.Fatalf("NonSortable() = %v, want %v", got, tt.nonSortable)
			}
		})
	}
}
