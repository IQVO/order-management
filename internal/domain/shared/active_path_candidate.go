package shared

import "time"

// ActivePathCandidate is order.PathSelectionPolicy's own mirror of
// ports.ActivePath (ADR-0021) — one currently active process path a line
// could be routed to, with the capability facts the policy needs to rank
// it. Independently declared here rather than importing the application
// layer's port type, exactly the same reasoning as Eligibility's own doc
// comment (the domain layer never imports ports or adapter types).
//
// Intake (ADR-0042) is the set of demand sources the path's owner
// (process-path-management, its ADR 0034) allows to route work onto it.
// nil/empty means no demand source may select the path: it is reached
// only by an explicit path id from an upstream stage.
type ActivePathCandidate struct {
	PathId         PathId
	CycleTimeP95   time.Duration
	CycleTimeKnown bool
	Eligibility    Eligibility
	Intake         []DemandSource
}

// AcceptsDemandSource reports whether source is in the candidate's intake.
func (c ActivePathCandidate) AcceptsDemandSource(source DemandSource) bool {
	for _, s := range c.Intake {
		if s == source {
			return true
		}
	}
	return false
}
