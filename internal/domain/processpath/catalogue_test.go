package processpath

import (
	"errors"
	"testing"
)

func TestCatalogueLookup(t *testing.T) {
	c := New([]PathDefinition{
		{Id: "PICK", MatchPrefix: "pick"},
		{Id: "PICK_DUP", MatchPrefix: "pick"}, // same prefix, same length: first declared wins
		{Id: "PICK_ZONE_A", MatchPrefix: "pick-zone-a"},
		{Id: "PACK", MatchPrefix: "pack"},
	})

	tests := []struct {
		name    string
		id      string
		want    string
		wantErr error
	}{
		{name: "exact match", id: "pick", want: "PICK"},
		{name: "case-insensitive exact match", id: "PICK", want: "PICK"},
		{name: "prefix family match", id: "pick-station-3", want: "PICK"},
		{name: "longest prefix wins", id: "pick-zone-a-east", want: "PICK_ZONE_A"},
		{
			// Two definitions with the SAME prefix length: the FIRST
			// declared one wins (strict `>` keeps the incumbent). Lookup
			// is deterministic, which is all the contract promises — it
			// deliberately does not promise "last wins".
			name: "equal-length prefixes keep the first declared definition",
			id:   "pick-station-3", want: "PICK",
		},
		{name: "bare substring without separator does not match", id: "picking", wantErr: ErrUnknownPath},
		{name: "unknown id", id: "rebin-1", wantErr: ErrUnknownPath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := c.Lookup(tt.id)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Lookup(%q) err = %v, want %v", tt.id, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Lookup(%q): %v", tt.id, err)
			}
			if def.Id != tt.want {
				t.Fatalf("Lookup(%q).Id = %q, want %q", tt.id, def.Id, tt.want)
			}
		})
	}
}

func TestCatalogueLookupIgnoresDefinitionsWithEmptyMatchPrefix(t *testing.T) {
	c := New([]PathDefinition{{Id: "BROKEN", MatchPrefix: ""}})
	if _, err := c.Lookup("broken"); !errors.Is(err, ErrUnknownPath) {
		t.Fatalf("Lookup: err = %v, want %v", err, ErrUnknownPath)
	}
}

func TestNewCopiesInputSlice(t *testing.T) {
	defs := []PathDefinition{{Id: "PICK", MatchPrefix: "pick"}}
	c := New(defs)
	defs[0].MatchPrefix = "mutated"

	if _, err := c.Lookup("pick"); err != nil {
		t.Fatalf("Lookup: mutating the caller's slice after New must not affect the catalogue, got: %v", err)
	}
}
