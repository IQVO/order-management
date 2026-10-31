package shared_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// The rule mirrors facility-layout's site code: ^[A-Z0-9]+$ (non-empty,
// uppercase ASCII letters and digits). No upper length bound, because
// facility-layout has none.
func TestNewFulfillmentSiteID(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"letters", "WH", true},
		{"letters and digits", "WH1", true},
		{"digit-only is a valid code", "123", true},
		{"single letter (minimum length)", "A", true},
		{"single digit (minimum length)", "0", true},
		{"first letter of range", "A", true},
		{"last letter of range", "Z", true},
		{"first digit of range", "0", true},
		{"last digit of range", "9", true},
		{"no upper length bound: 256 chars", strings.Repeat("A", 256), true},
		{"no upper length bound: 1024 chars", strings.Repeat("Z9", 512), true},

		{"empty", "", false},
		{"lowercase", "wh1", false},
		{"mixed case", "Wh1", false},
		{"lowercase last char", "WHa", false},
		{"char just below A", "WH@", false},
		{"char just above Z", "WH[", false},
		{"char just below 0", "WH/", false},
		{"char just above 9", "WH:", false},
		{"hyphen", "WH-1", false},
		{"underscore", "WH_1", false},
		{"space", "WH 1", false},
		{"leading space", " WH1", false},
		{"trailing space", "WH1 ", false},
		{"trailing newline", "WH1\n", false},
		{"non-ASCII letter", "WH\u00c9", false},
		{"non-ASCII digit", "WH\u0661", false},
		{"only a space", " ", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shared.NewFulfillmentSiteID(tt.value)
			if tt.valid {
				if err != nil {
					t.Fatalf("NewFulfillmentSiteID(%q) error = %v, want nil", tt.value, err)
				}
				if got.String() != tt.value || string(got) != tt.value {
					t.Fatalf("got %q, want %q", got, tt.value)
				}
				return
			}
			if !errors.Is(err, shared.ErrInvalidFulfillmentSiteID) {
				t.Fatalf("NewFulfillmentSiteID(%q) error = %v, want ErrInvalidFulfillmentSiteID", tt.value, err)
			}
			if got != "" {
				t.Fatalf("got %q on error, want the zero value", got)
			}
		})
	}
}

func TestNewFulfillmentSiteID_ErrorsNameTheProblem(t *testing.T) {
	_, empty := shared.NewFulfillmentSiteID("")
	_, bad := shared.NewFulfillmentSiteID("wh1")
	if empty == nil || !strings.Contains(empty.Error(), "must not be empty") {
		t.Fatalf("empty error = %v", empty)
	}
	if bad == nil || !strings.Contains(bad.Error(), `"wh1"`) {
		t.Fatalf("malformed error = %v, want it to quote the value", bad)
	}
}
