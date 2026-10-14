// Package processpath models the fleet's process-path catalogue as a
// small, read-only lookup: which process-path families are currently
// active, matched by prefix. This package mirrors wes-work-planning's
// and fulfillment-execution's own pathcatalog package byte-for-byte in
// its matching semantics -- all three read the same published-language
// events from process-path-management, so they must agree. See
// wes-work-planning's ADR-0017 (and its addendum) for the full history
// of why an earlier exact-match version of this idea was wrong, and
// order-management's own ADR-0013 for why this context needs the
// catalogue at all: an OrderLine's resolved PathId is validated against
// it before the order is ever persisted, rather than being discovered
// wrong one saga step later in wes-work-planning.
//
// order-management deliberately keeps a SMALLER shape than WES/FE's
// copy for the fields it does not need: RequiredCapabilities and Direct
// are still not carried here. ADR-0014 step A DOES widen this struct
// with CycleTimeP95 and Eligibility, since order.PromisePolicy is a real
// consumer of both — see the kafkacatalog package doc comment for the
// wire-decoding side of that widening.
package processpath

import (
	"errors"
	"strings"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// ErrUnknownPath is returned by Lookup when id does not match any
// currently active path's prefix.
var ErrUnknownPath = errors.New("processpath: unknown or inactive process path id")

// PathDefinition is one currently active process path: its canonical id,
// the lower-cased prefix family of real path_id values it recognizes,
// and (ADR-0014) its declared cycle time and eligibility.
//
// CycleTimeKnown is false when the wire event carried no parseable
// cycle_time_p95 for this path — PromisePolicy treats "path known, cycle
// time unknown" the same as "path unknown": fall back to LeadTimePolicy.
//
// DestinationLocationRole mirrors process-path-management's own optional
// declaration (ADR 0006 there) of which facility-layout LocationRole this
// path's completed work is destined for (Drop | WorkCenter | Shipping),
// or "" when no destination role was declared. Made available on this
// read model (ADR-0014's "available but not yet acted upon" discipline,
// matching how Direct/RequiredCapabilities are carried in WES/FE's own
// copies of this catalogue) — nothing in this service currently makes a
// routing decision off it.
//
// Intake (ADR-0042) is the set of demand sources process-path-management
// declares may route work onto the path (its ADR 0034); empty when the
// path declares none or the producer predates the field.
type PathDefinition struct {
	Id                      string
	MatchPrefix             string
	CycleTimeP95            time.Duration
	CycleTimeKnown          bool
	Eligibility             shared.Eligibility
	DestinationLocationRole string
	Intake                  []shared.DemandSource
}

// Catalogue is the validated, in-memory set of currently active process
// paths.
type Catalogue struct {
	defs []PathDefinition
}

// New builds a Catalogue from defs.
func New(defs []PathDefinition) *Catalogue {
	out := make([]PathDefinition, len(defs))
	copy(out, defs)
	return &Catalogue{defs: out}
}

// Lookup returns the declared definition whose MatchPrefix matches id
// (case-insensitively: id equals the prefix, or id starts with
// prefix + "-"), or ErrUnknownPath if no active path recognizes id. When
// more than one declared prefix would match, the LONGEST matching prefix
// wins.
func (c *Catalogue) Lookup(id string) (PathDefinition, error) {
	lower := strings.ToLower(id)

	var best PathDefinition
	bestLen := -1
	for _, d := range c.defs {
		prefix := strings.ToLower(d.MatchPrefix)
		if prefix == "" {
			continue
		}
		if !matchesPrefix(lower, prefix) {
			continue
		}
		if len(prefix) > bestLen {
			best = d
			bestLen = len(prefix)
		}
	}
	if bestLen == -1 {
		return PathDefinition{}, ErrUnknownPath
	}
	return best, nil
}

// matchesPrefix reports whether id (already lower-cased) belongs to
// prefix's family: either id equals prefix exactly, or id starts with
// prefix followed by a "-" separator. Deliberately does NOT match a bare
// substring prefix without the separator.
func matchesPrefix(id, prefix string) bool {
	if id == prefix {
		return true
	}
	return strings.HasPrefix(id, prefix+"-")
}
