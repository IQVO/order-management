package order

import (
	"time"
)

// PromiseDate is the date this order is promised for — the CPT's
// CutoffAt when the promise has a Capability basis, or the computed
// instant from LeadTimePolicy otherwise. Nil until at least one line is
// allocated. Kept as a bare time.Time on the wire and in the database for
// backward compatibility (ADR 0014 §1); see PromiseCptId/PromiseBasis
// for the rest of the Promise value.
func (o *Order) PromiseDate() *time.Time {
	if o.promiseDate == nil {
		return nil
	}
	d := *o.promiseDate
	return &d
}

// PromiseCptId is the CPT identity (e.g. "sp1-1800") the promise
// targets, when the promise has a Capability basis. Nil for a
// LeadTime-basis promise, which has no departure identity, or before any
// promise has been computed.
func (o *Order) PromiseCptId() *string {
	if o.promiseCptId == nil {
		return nil
	}
	id := *o.promiseCptId
	return &id
}

// PromiseBasis reports which policy produced the current promise
// (Capability or LeadTime). Nil before any promise has been computed.
func (o *Order) PromiseBasis() *PromiseBasis {
	if o.promiseBasis == nil {
		return nil
	}
	b := *o.promiseBasis
	return &b
}

// SetPromiseDate records a bare promise date without a CPT identity or
// basis. Kept for any caller that only has a computed instant (e.g.
// tests exercising LeadTimePolicy directly) — production allocation code
// should prefer SetPromise, which also records CptId/Basis.
func (o *Order) SetPromiseDate(d time.Time) { o.promiseDate = &d }

// SetPromise records the full Promise value ADR 0014 introduces: the
// cutoff instant (kept on promiseDate for backward compatibility), the
// CPT identity (nil for a LeadTime-basis promise), and which policy
// produced it.
//
// This method's body is deliberately left untouched by ADR 0017 (it
// predates per-group promising and every existing test exercises it
// directly): it sets ONLY the legacy summary fields, never
// promiseGroups. A caller that wants the full per-group breakdown
// recorded too should call SetPromiseGroups instead, which is real,
// additional method surface — not a replacement for this one.
func (o *Order) SetPromise(p Promise) {
	d := p.CutoffAt
	o.promiseDate = &d
	basis := p.Basis
	o.promiseBasis = &basis
	if p.CptId == "" {
		o.promiseCptId = nil
		return
	}
	cptId := p.CptId
	o.promiseCptId = &cptId
}

// PromiseGroups returns the full per-shipment-group promise breakdown
// ADR 0014 §3 / ADR 0017 introduces — the real, full-fidelity source of
// truth. A ship-complete order (or any order whose promise was set via
// SetPromiseGroups with a single group, which is what PromisePolicy.
// PromiseGroups always produces for AllowPartialShipment=false) has
// exactly one entry. Empty (nil) until SetPromiseGroups has been called
// at least once, or for an order rehydrated without a persisted group
// breakdown (a pre-ADR-0017 row, or one whose promise was set via the
// legacy SetPromise). The slice and its PromiseGroup values are copies:
// mutating the returned slice cannot corrupt the aggregate.
func (o *Order) PromiseGroups() []PromiseGroup {
	out := make([]PromiseGroup, len(o.promiseGroups))
	for i, g := range o.promiseGroups {
		lineNos := make([]int, len(g.LineNos))
		copy(lineNos, g.LineNos)
		out[i] = PromiseGroup{LineNos: lineNos, Promise: g.Promise}
	}
	return out
}

// SetPromiseGroups records the full per-shipment-group promise breakdown
// (ADR 0014 §3 / ADR 0017): groups is stored verbatim as the new,
// full-fidelity PromiseGroups() source of truth, AND the existing
// single-valued promiseDate/promiseCptId/promiseBasis fields are
// re-derived from it as a backward-compatible projection, so every
// existing reader of those three fields (the Postgres repo's write path,
// the wire event publisher, PromiseDate()/PromiseCptId()/PromiseBasis()
// themselves) keeps working unchanged.
//
// The projection rule, per ADR 0014 §3 ("the order's PromiseDate()
// becomes the latest of them"): PromiseDate() is set to the LATEST
// CutoffAt among all groups — so no existing reader ever sees an earlier
// date than the single-promise behaviour would have produced. ADR 0014
// does not specify an aggregation rule for CptId/Basis (a single string
// cannot represent N different departures), so this method makes the
// same honest choice ADR 0017 documents: PromiseCptId()/PromiseBasis()
// are projected from the SAME group whose CutoffAt is that latest one —
// i.e. all three legacy fields describe "the group with the latest
// cutoff", consistently, rather than three independently-chosen groups.
//
// Calling this with a single group covering every allocated line (what
// PromisePolicy.PromiseGroups always returns for
// AllowPartialShipment=false) reproduces SetPromise's own single-field
// assignment exactly, byte for byte — this is what makes the ship-
// complete path's behaviour provably unchanged rather than merely
// "should be the same".
func (o *Order) SetPromiseGroups(groups []PromiseGroup) {
	stored := make([]PromiseGroup, len(groups))
	for i, g := range groups {
		lineNos := make([]int, len(g.LineNos))
		copy(lineNos, g.LineNos)
		stored[i] = PromiseGroup{LineNos: lineNos, Promise: g.Promise}
	}
	o.promiseGroups = stored

	if len(groups) == 0 {
		return
	}

	latest := groups[0]
	for _, g := range groups[1:] {
		if g.Promise.CutoffAt.After(latest.Promise.CutoffAt) {
			latest = g
		}
	}
	o.SetPromise(latest.Promise)
}
