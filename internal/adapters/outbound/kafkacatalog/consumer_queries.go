package kafkacatalog

import (
	"time"

	"github.com/claudioed/order-management/internal/domain/processpath"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// IsActive satisfies ports.ProcessPathCatalogue against this consumer's
// current in-memory snapshot. Matching semantics are byte-for-byte
// identical to processpath.Catalogue.Lookup — this consumer builds a real
// processpath.Catalogue on every read rather than reimplementing the
// match rule, so the two can never drift.
func (c *Consumer) IsActive(pathID shared.PathId) bool {
	_, err := c.lookup(pathID)
	return err == nil
}

// CycleTimeP95 satisfies ports.ProcessPathCatalogue: it returns the
// looked-up path's cycle time and whether it is known. An unknown path,
// or a path whose wire event never carried a parseable cycle_time_p95,
// both report known=false — PromisePolicy treats them identically.
func (c *Consumer) CycleTimeP95(pathID shared.PathId) (time.Duration, bool) {
	def, err := c.lookup(pathID)
	if err != nil {
		return 0, false
	}
	return def.CycleTimeP95, def.CycleTimeKnown
}

// Eligibility satisfies ports.ProcessPathCatalogue: it returns the
// looked-up path's declared eligibility rule and whether it is known
// (false for an unknown/inactive path).
func (c *Consumer) Eligibility(pathID shared.PathId) (shared.Eligibility, bool) {
	def, err := c.lookup(pathID)
	if err != nil {
		return shared.Eligibility{}, false
	}
	return def.Eligibility, true
}

// ListActive satisfies ports.ProcessPathCatalogue (ADR-0021): it
// snapshots every path currently in this consumer's in-memory cache.
// Deactivated paths are already absent from c.paths (applyDeactivated
// deletes them), so every entry returned here is, by construction,
// active. An empty or not-yet-ready cache returns an empty slice, never
// an error — the same "missing data fails open" convention as
// IsActive/CycleTimeP95/Eligibility.
func (c *Consumer) ListActive() []shared.ActivePathCandidate {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]shared.ActivePathCandidate, 0, len(c.paths))
	for _, d := range c.paths {
		out = append(out, shared.ActivePathCandidate{
			PathId:         shared.PathId(d.Id),
			CycleTimeP95:   d.CycleTimeP95,
			CycleTimeKnown: d.CycleTimeKnown,
			Eligibility:    d.Eligibility,
			Intake:         append([]shared.DemandSource(nil), d.Intake...),
		})
	}
	return out
}

// lookup resolves pathID against a fresh processpath.Catalogue built
// from the current in-memory snapshot, so IsActive/CycleTimeP95/
// Eligibility all share exactly the same matching semantics and can
// never drift from one another.
func (c *Consumer) lookup(pathID shared.PathId) (processpath.PathDefinition, error) {
	c.mu.RLock()
	defs := make([]processpath.PathDefinition, 0, len(c.paths))
	for _, d := range c.paths {
		defs = append(defs, d)
	}
	c.mu.RUnlock()
	return processpath.New(defs).Lookup(pathID.String())
}
