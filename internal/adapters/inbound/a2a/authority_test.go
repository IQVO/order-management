package a2a

import (
	"fmt"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// authorityTagPrefix is the pinned skill-tag form of a declaration (contracts
// G2): "authority:L1" .. "authority:L4".
const authorityTagPrefix = "authority:"

// skillAuthorityViolation returns why a skill breaks the authority rule, or
// "": exactly one authority:<L> tag with L in L1..L4; a skill tagged "write"
// must be L3 (L4 is forbidden here: no ADR grants it) and a non-write skill
// must be L1 or L2. Every skill of this agent is a read, so only L1/L2 can
// ever pass -- this context exposes no write skill (ADR 0046's write gate).
func skillAuthorityViolation(s a2a.AgentSkill) string {
	var levels []string
	write := false
	for _, tag := range s.Tags {
		if lvl, ok := strings.CutPrefix(tag, authorityTagPrefix); ok {
			levels = append(levels, lvl)
		}
		if tag == "write" {
			write = true
		}
	}
	if len(levels) != 1 {
		return fmt.Sprintf("must carry exactly one %s<L> tag, got %v", authorityTagPrefix, levels)
	}
	level := levels[0]
	switch level {
	case "L1", "L2", "L3", "L4":
	default:
		return fmt.Sprintf("declares unknown authority level %q", level)
	}
	switch {
	case write && level != "L3":
		return fmt.Sprintf("is tagged write so it must be L3 (L4 is forbidden here), got %s", level)
	case !write && level != "L1" && level != "L2":
		return fmt.Sprintf("is a read skill so it must be L1 or L2, got %s", level)
	}
	return ""
}

// TestEveryAgentToolDeclaresAuthority (A2A half; the MCP half lives in the
// mcp package under the same name) asserts every skill on the Agent Card
// declares exactly one valid authority level, the same level as the tool it
// fronts (both skills are pure reads, so L1).
func TestEveryAgentToolDeclaresAuthority(t *testing.T) {
	card := Card(Config{AgentURL: testAgentURL, Version: "1.0.0"})
	if len(card.Skills) == 0 {
		t.Fatal("card has no skills; the sensor is looking at nothing")
	}
	want := map[string]string{skillOrderStatus: "L1", skillPromiseHealth: "L1"}
	for _, s := range card.Skills {
		if v := skillAuthorityViolation(s); v != "" {
			t.Errorf("skill %q %s", s.ID, v)
		}
		if !containsTag(s.Tags, authorityTagPrefix+want[s.ID]) {
			t.Errorf("skill %q must be tagged %s%s, tags = %v", s.ID, authorityTagPrefix, want[s.ID], s.Tags)
		}
	}
}

// TestSkillAuthoritySensorFailsOnBadSkills proves the sensor can fail.
func TestSkillAuthoritySensorFailsOnBadSkills(t *testing.T) {
	cases := []struct {
		name string
		tags []string
		bad  bool
	}{
		{"read L1", []string{"read", "authority:L1"}, false},
		{"read L2", []string{"read", "authority:L2"}, false},
		{"write L3", []string{"write", "authority:L3"}, false},
		{"missing", []string{"read"}, true},
		{"two levels", []string{"read", "authority:L1", "authority:L2"}, true},
		{"unknown level", []string{"read", "authority:L5"}, true},
		{"write L4", []string{"write", "authority:L4"}, true},
		{"write L1", []string{"write", "authority:L1"}, true},
		{"read L3", []string{"read", "authority:L3"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skillAuthorityViolation(a2a.AgentSkill{Tags: tc.tags}) != ""; got != tc.bad {
				t.Fatalf("violation=%v, want %v", got, tc.bad)
			}
		})
	}
}
