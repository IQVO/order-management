package order

import (
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// DefaultCPTHorizon bounds how many upcoming CPT windows PromisePolicy
// queries per Promise call when Horizon is unset (zero value).
const DefaultCPTHorizon = 10

// CPTWindow is one concrete, upcoming CPT (Critical Pull Time) this
// policy can promise a line against: a departure identity, the instant
// it cuts off, and the paths currently eligible to make it. It is the
// domain's own copy of the shape a ports.CPTScheduleCache adapter
// produces — kept here (rather than importing a ports type into the
// domain) because PromisePolicy is pure domain logic per ADR 0014 and
// must not depend on the application layer.
type CPTWindow struct {
	CptId           string
	CutoffAt        time.Time
	EligiblePathIds []string
}

// CapabilitySource is the minimal capability question PromisePolicy
// needs: a path's 95th-percentile cycle time. Any adapter satisfying
// ports.ProcessPathCatalogue's superset of methods satisfies this
// narrower domain-owned interface automatically (Go's structural
// interface-to-interface assignability) — no adapter code needs to know
// this type exists.
type CapabilitySource interface {
	CycleTimeP95(pathID shared.PathId) (time.Duration, bool)
}

// ScheduleSource is the minimal CPT-schedule question PromisePolicy
// needs. Satisfied by ports.CPTScheduleCache.
type ScheduleSource interface {
	NextCutoffs(siteId string, from time.Time, n int) ([]CPTWindow, bool)
}

// CapacitySource is the minimal remaining-capacity question PromisePolicy
// needs. Satisfied by ports.PathCapacity (including its current
// always-unknown UnknownPathCapacity implementation, and ADR-0015's
// Kafka-fed kafkapathcapacity.Consumer). cutoffAt is passed alongside
// cptId because PromisePolicy already has it in hand from the CPTWindow
// it is evaluating (see linesFitWindow below) and a capacity adapter
// keyed on wes-work-planning's native CutoffAt currency needs it to
// answer — see ADR-0015 for the full reasoning.
type CapacitySource interface {
	Remaining(pathID shared.PathId, cptId string, cutoffAt time.Time) (units int, known bool)
}

// PromisePolicy is the domain service ADR 0014 introduces to replace
// LeadTimePolicy as the PRIMARY promise policy, while keeping
// LeadTimePolicy itself unchanged as the fallback. It is pure domain
// logic: no Kafka, no HTTP. Schedule/Capability/Capacity are small
// interfaces the application layer wires to real outbound ports; nil is
// a legal, tested value for any of them (missing input is a fallback
// trigger, never a panic) except Fallback, which should always be a real
// LeadTimePolicy.
//
// SiteId is a known simplification for this phase: order-management does
// not yet model which site an order ships from (ADR 0014 leaves that to
// a future phase). Every promise in this phase is computed against one
// configured site — see cmd/order/main.go's DEFAULT_SITE_ID wiring.
type PromisePolicy struct {
	Schedule   ScheduleSource
	Capability CapabilitySource
	Capacity   CapacitySource
	Fallback   LeadTimePolicy
	SiteId     string
	// Horizon bounds how many upcoming CPT windows are queried per
	// Promise call. Zero means DefaultCPTHorizon.
	Horizon int
}

func (p PromisePolicy) horizon() int {
	if p.Horizon > 0 {
		return p.Horizon
	}
	return DefaultCPTHorizon
}

// Promise computes the order's promise at allocation time: the earliest
// CPT window every allocated (or already released) line can make,
// falling back to LeadTimePolicy when no capability/schedule input is
// available or no window in the queried horizon qualifies. Returns
// ok=false only when LeadTimePolicy itself would also return ok=false —
// no line allocated yet, meaning there is nothing to promise.
//
// The rule per line, for a given window w: (a) the line's path is in
// w.EligiblePathIds; (b) now + cycleTimeP95(path) <= w.CutoffAt; (c)
// capacity for (path, w.CptId) is unknown, or remaining >= the line's
// quantity. Every allocated line must fit the SAME window — the same
// "most-constrained line governs" spirit LeadTimePolicy already applies
// order-wide.
func (p PromisePolicy) Promise(now time.Time, o *Order) (Promise, bool) {
	lines := allocatedLines(o)
	if len(lines) == 0 {
		return Promise{}, false
	}

	if p.Schedule == nil || p.Capability == nil {
		return p.fallback(now, o)
	}

	windows, known := p.Schedule.NextCutoffs(p.SiteId, now, p.horizon())
	if !known || len(windows) == 0 {
		return p.fallback(now, o)
	}

	for _, w := range windows {
		if p.linesFitWindow(now, lines, w) {
			return Promise{CptId: w.CptId, CutoffAt: w.CutoffAt, Basis: BasisCapability}, true
		}
	}
	return p.fallback(now, o)
}

// FeasibleBy answers the DUAL of Promise: not "what is the earliest
// window we can make?" but "is there a window at or before a deadline
// someone else set, that every allocated line can make?".
//
// External retail networks send demand with the ship date already
// stamped on it (network-fulfillment ADR 0001) and require a whole-order
// commit or reject inside a fixed acknowledgement window. That inverts
// the question this policy was built to answer, but NOT the machinery
// that answers it: the eligibility / cycle-time / capacity rule per line
// is identical, so this reuses linesFitWindow rather than restating it.
// If the two ever diverge, the divergence is the bug.
//
// The returned Promise carries the window we actually found — its real
// CptId and CutoffAt, which are at or before deadline — tagged
// BasisNetwork because we did not choose it. Callers should treat
// ok=true as "safe to commit to the network", and the returned CutoffAt
// as the departure we are now on the hook for.
//
// Of the qualifying windows it returns the LATEST, not the earliest.
// See the loop below for why; it is the opposite of Promise's choice
// and deliberate.
//
// Deliberately NOT symmetric with Promise in one respect: there is no
// LeadTimePolicy fallback. Promise falls back because a building that
// cannot say WHEN is still a building that will ship — an approximate
// internal promise beats refusing to take the order. A feasibility
// answer cannot borrow that reasoning: ok=true here becomes a
// fill-or-kill commitment to an external party who measures us on it,
// and a guess dressed as a commitment is how a vendor loses its
// account. Missing schedule, missing capability, unknown cycle time or
// an empty horizon therefore all return false — "we cannot show this is
// feasible", never "probably fine".
//
// That means FeasibleBy returns false on a cold cache at startup, which
// will look like a bug the first time someone sees it. It is not. The
// caller's correct response is to retry once the caches are warm, never
// to substitute an estimate.
func (p PromisePolicy) FeasibleBy(now time.Time, o *Order, deadline time.Time) (Promise, bool) {
	lines := allocatedLines(o)
	if len(lines) == 0 {
		return Promise{}, false
	}

	// No fallback: absent inputs mean we cannot demonstrate feasibility.
	if p.Schedule == nil || p.Capability == nil {
		return Promise{}, false
	}

	windows, known := p.Schedule.NextCutoffs(p.SiteId, now, p.horizon())
	if !known || len(windows) == 0 {
		return Promise{}, false
	}

	var best Promise
	var found bool

	for _, w := range windows {
		// Skip windows the deadline excludes. Do NOT break: a window
		// past the deadline says nothing about later entries, because
		// ScheduleSource guarantees no ordering — ascending cutoffs are
		// a property of today's adapters, not of the domain interface.
		if w.CutoffAt.After(deadline) {
			continue
		}
		if !p.linesFitWindow(now, lines, w) {
			continue
		}
		// Keep the LATEST qualifying window, not the first. When the
		// date is fixed by someone else, shipping earlier than required
		// buys nothing — the network has already quoted the customer a
		// date — while consuming path capacity before a cutoff that
		// other demand may genuinely need. So we commit to the last
		// departure that still meets the deadline and leave the floor
		// the most slack. This is the one place this policy's goal
		// differs from Promise's: Promise races to the earliest cutoff
		// because sooner is better when WE choose; FeasibleBy has
		// nothing to gain from sooner.
		if !found || w.CutoffAt.After(best.CutoffAt) {
			best = Promise{CptId: w.CptId, CutoffAt: w.CutoffAt, Basis: BasisNetwork}
			found = true
		}
	}
	return best, found
}

// linesFitWindow reports whether every line in lines can make w, per the
// three conditions in Promise's doc comment.
func (p PromisePolicy) linesFitWindow(now time.Time, lines []*OrderLine, w CPTWindow) bool {
	eligible := make(map[shared.PathId]bool, len(w.EligiblePathIds))
	for _, id := range w.EligiblePathIds {
		eligible[shared.PathId(id)] = true
	}

	for _, l := range lines {
		if !eligible[l.PathID()] {
			return false
		}
		cycleTime, cycleKnown := p.Capability.CycleTimeP95(l.PathID())
		if !cycleKnown {
			return false
		}
		if now.Add(cycleTime).After(w.CutoffAt) {
			return false
		}
		if p.Capacity != nil {
			if remaining, capKnown := p.Capacity.Remaining(l.PathID(), w.CptId, w.CutoffAt); capKnown && remaining < l.Quantity() {
				return false
			}
		}
	}
	return true
}

func (p PromisePolicy) fallback(now time.Time, o *Order) (Promise, bool) {
	d, ok := p.Fallback.PromiseDate(now, o)
	if !ok {
		return Promise{}, false
	}
	return Promise{CutoffAt: d, Basis: BasisLeadTime}, true
}

// allocatedLines returns the order's lines that are Allocated or
// Released — the same eligibility LeadTimePolicy.PromiseDate already
// applies, kept identical here so both policies agree on "what counts".
func allocatedLines(o *Order) []*OrderLine {
	var out []*OrderLine
	for _, l := range o.Lines() {
		if l.Status() == LineAllocated || l.Status() == LineReleased {
			out = append(out, l)
		}
	}
	return out
}

// lineNos returns each line's 1-based LineNo, in the same order as lines.
func lineNos(lines []*OrderLine) []int {
	out := make([]int, len(lines))
	for i, l := range lines {
		out[i] = l.LineNo()
	}
	return out
}

// promiseForLines is Promise's search-and-fallback logic, parameterized
// on an explicit line slice instead of deriving it from o via
// allocatedLines internally. Promise(now, o) itself is unchanged and
// still the one public entry point for "the whole order is one group" —
// this unexported helper only exists so PromiseGroups can reuse the
// EXACT SAME window search / capacity / eligibility logic per line
// (ADR 0014 §3), rather than duplicating it. o is still needed because
// p.fallback ultimately delegates to p.Fallback.PromiseDate(now, o),
// which is LeadTimePolicy's own whole-order, slowest-line-governs
// signature — untouched by this ADR, per the task's explicit direction
// not to change LeadTimePolicy. Called with a single-line slice, that
// whole-order fallback degenerates to exactly what a single-line order's
// fallback would be, since only that one line is ever Allocated/Released
// on the temporary rehydrated order fallbackOrder builds.
func (p PromisePolicy) promiseForLines(now time.Time, o *Order, lines []*OrderLine) (Promise, bool) {
	if len(lines) == 0 {
		return Promise{}, false
	}

	if p.Schedule == nil || p.Capability == nil {
		return p.fallbackForLines(now, o, lines)
	}

	windows, known := p.Schedule.NextCutoffs(p.SiteId, now, p.horizon())
	if !known || len(windows) == 0 {
		return p.fallbackForLines(now, o, lines)
	}

	for _, w := range windows {
		if p.linesFitWindow(now, lines, w) {
			return Promise{CptId: w.CptId, CutoffAt: w.CutoffAt, Basis: BasisCapability}, true
		}
	}
	return p.fallbackForLines(now, o, lines)
}

// fallbackForLines delegates to p.Fallback.PromiseDate exactly as
// p.fallback(now, o) does for the whole-order case, but against a
// temporary aggregate rehydrated with ONLY lines Allocated — so
// LeadTimePolicy's own "slowest allocated line governs" rule applies to
// just this subset, never to the whole order's lines. LeadTimePolicy
// itself is not modified: this is the minimum parameterization needed to
// reuse it per group without changing its signature or behaviour.
func (p PromisePolicy) fallbackForLines(now time.Time, o *Order, lines []*OrderLine) (Promise, bool) {
	if len(lines) == len(allocatedLines(o)) {
		// The common single-group case (every allocated line, i.e. a
		// ship-complete order or a partial-shipment order where every
		// line shares one fallback instant): reuse p.fallback(now, o)
		// directly rather than building a throwaway aggregate, so the
		// ship-complete path is provably byte-identical to Promise's
		// own existing behaviour.
		return p.fallback(now, o)
	}
	subset := make([]*OrderLine, len(lines))
	for i, l := range lines {
		subset[i] = RehydrateOrderLine(l.LineNo(), l.SKU(), l.Quantity(), l.PathID(), l.GiftWrap(), l.Status(), l.ReservationID())
	}
	tmp := Rehydrate(OrderSnapshot{ID: o.ID(), Lines: subset, AllowPartialShipment: o.AllowPartialShipment()})
	d, ok := p.Fallback.PromiseDate(now, tmp)
	if !ok {
		return Promise{}, false
	}
	return Promise{CutoffAt: d, Basis: BasisLeadTime}, true
}

// PromiseGroups computes ADR 0014 §3's per-shipment-group promise: when
// o.AllowPartialShipment() is false, exactly one group covering every
// allocated line, computed via the EXISTING unchanged Promise(now, o)
// search — this is the "exactly as today" case the ADR requires to be
// byte-identical to Promise's own result. When true, each allocated line
// is evaluated INDEPENDENTLY (as if it were the sole line of a
// single-line order), and lines whose independently-computed Promise is
// identical (same Basis, same CptId, same CutoffAt) are grouped together
// — directly implementing "lines are grouped by the cutoff they can
// make". Returns ok=false in exactly the same case Promise does: no line
// allocated yet.
func (p PromisePolicy) PromiseGroups(now time.Time, o *Order) ([]PromiseGroup, bool) {
	lines := allocatedLines(o)
	if len(lines) == 0 {
		return nil, false
	}

	if !o.AllowPartialShipment() {
		promise, ok := p.promiseForLines(now, o, lines)
		if !ok {
			return nil, false
		}
		return []PromiseGroup{{LineNos: lineNos(lines), Promise: promise}}, true
	}

	// Partial shipment: compute each line's own promise, then partition
	// by identical result. A stable, deterministic group order is kept
	// by first-occurrence: the first line to produce a given Promise
	// value opens its group, and every later line with the same Promise
	// value is appended to it.
	type keyed struct {
		key     Promise
		promise Promise
		lineNo  int
	}
	perLine := make([]keyed, 0, len(lines))
	for _, l := range lines {
		promise, ok := p.promiseForLines(now, o, []*OrderLine{l})
		if !ok {
			// Promise's own contract only returns ok=false when NO
			// line is allocated at all; a single already-allocated
			// line always has a fallback answer (LeadTimePolicy never
			// refuses to promise an allocated line — see promise.go).
			// Unreachable in practice, but fail closed rather than
			// silently drop the line from every group.
			return nil, false
		}
		perLine = append(perLine, keyed{key: promise, promise: promise, lineNo: l.LineNo()})
	}

	var groups []PromiseGroup
	index := make(map[Promise]int, len(perLine))
	for _, k := range perLine {
		if i, ok := index[k.key]; ok {
			groups[i].LineNos = append(groups[i].LineNos, k.lineNo)
			continue
		}
		index[k.key] = len(groups)
		groups = append(groups, PromiseGroup{LineNos: []int{k.lineNo}, Promise: k.promise})
	}
	return groups, true
}
