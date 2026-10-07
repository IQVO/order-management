package order

import (
	"sort"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// EligibilitySource is the catalogue question PathSelectionPolicy needs:
// a path's declared Eligibility rule (single-id, kept for callers that
// only need that), plus ListActive (ADR-0021) so this policy can
// enumerate every currently active path rather than only ever evaluating
// one hardcoded candidate. Any adapter satisfying
// ports.ProcessPathCatalogue's superset of methods satisfies this
// narrower domain-owned interface automatically (Go's structural
// interface-to-interface assignability), exactly the pattern
// PromisePolicy's CapabilitySource/ScheduleSource/CapacitySource already
// use — no adapter code needs to know this type exists. A nil
// EligibilitySource, or one whose ListActive returns nothing, is a
// legal, tested value: Select then falls back to today's behaviour,
// exactly ADR-0013's original floor.
type EligibilitySource interface {
	Eligibility(pathID shared.PathId) (shared.Eligibility, bool)
	ListActive() []shared.ActivePathCandidate
}

// PathSelectionPolicy decides which wes-work-planning process path an
// OrderLine's work should be enqueued onto, based on attributes known at
// intake (SKU, Quantity, GiftWrap) and the line's derived product
// attributes (e.g. "hazmat", "fragile", looked up by ReceiveOrder from
// the local product-classification copy, ADR 0036), evaluated
// against every currently active path's declared Eligibility. It is a
// pure domain policy: no I/O of its own. The application layer
// (ReceiveOrder) performs the classification lookup and passes plain
// values in, and supplies the EligibilitySource so this type never
// touches Kafka/HTTP.
//
// History: ADR-0013 shipped v1 with exactly one rule ("every line
// resolves to shared.DefaultPathId") and deliberately deferred
// attribute-driven routing until real eligibility data existed. ADR-0016
// (step B of ADR-0014) made this type evaluate Eligibility for real, but
// only against that one hardcoded candidate -- it had no way to discover
// a genuinely different, eligible alternative when DefaultPathId's own
// Eligibility rejected a line, a limitation ADR-0016 §4 documented
// explicitly. ADR-0021 closes that gap: EligibilitySource.ListActive
// gives this policy every currently active path, so it can filter to the
// eligible subset and pick the best one, rather than reject a line the
// fleet's real catalogue could actually carry on some other path.
type PathSelectionPolicy struct{}

// giftWrapAttribute is the product-attribute-vocabulary string this
// policy uses to represent "this line requested gift wrap" when
// evaluating it against a path's declared RequiredProductAttributes /
// ExcludedProductAttributes -- the same free-form vocabulary as
// "hazmat"/"fragile" (see shared.Eligibility's doc comment, which
// explicitly lists "giftWrap" alongside them).
const giftWrapAttribute = "giftWrap"

// Select resolves the PathId for a line described by sku, quantity,
// giftWrap and productAttributes (free-form strings from the fleet's
// existing product-classification vocabulary, e.g. "hazmat", "fragile",
// looked up via ReceiveOrder's classification lookup -- sku itself is
// accepted for symmetry with ADR-0013's original signature and so a
// future rule keyed directly on SKU needs no signature change again).
//
// catalogue is consulted via ListActive (ADR-0021) for every currently
// active path's declared Eligibility and CycleTimeP95. A nil catalogue,
// or one whose ListActive() returns no candidates, fails OPEN to
// shared.DefaultPathId (ok=true) -- missing catalogue data is a
// "not yet wired" state, never a rejection trigger, matching this
// fleet's established convention for a read-only membership/eligibility
// check.
//
// Among the candidates the line is eligible for, this policy picks the
// one with the shortest KNOWN CycleTimeP95 (ADR-0014 §4's original
// rule); a candidate whose cycle time is not yet known sorts after every
// candidate with a known one, but remains pickable if it is the only
// eligible candidate. Ties (identical known cycle time, or all
// candidates cycle-time-unknown) break on the lower PathId, so the same
// inputs against an unchanged catalogue always resolve the same way.
//
// ok=false means: no currently active path's declared Eligibility admits
// this line. The returned PathId is the zero value in that case; callers
// must not use it.
func (PathSelectionPolicy) Select(sku shared.SKU, quantity int, giftWrap bool, productAttributes []string, catalogue EligibilitySource) (shared.PathId, bool) {
	_ = sku // accepted for signature symmetry/future use; this rule does not key on it directly

	if catalogue == nil {
		return shared.DefaultPathId, true
	}

	candidates := catalogue.ListActive()
	if len(candidates) == 0 {
		return shared.DefaultPathId, true
	}

	attributes := productAttributes
	if giftWrap {
		attributes = append(append([]string(nil), productAttributes...), giftWrapAttribute)
	}

	eligible := make([]shared.ActivePathCandidate, 0, len(candidates))
	for _, c := range candidates {
		if lineEligible(c.Eligibility, quantity, attributes) {
			eligible = append(eligible, c)
		}
	}
	if len(eligible) == 0 {
		return "", false
	}

	sort.Slice(eligible, func(i, j int) bool {
		a, b := eligible[i], eligible[j]
		if a.CycleTimeKnown != b.CycleTimeKnown {
			// A known cycle time always sorts before an unknown one.
			return a.CycleTimeKnown
		}
		if a.CycleTimeKnown && a.CycleTimeP95 != b.CycleTimeP95 {
			return a.CycleTimeP95 < b.CycleTimeP95
		}
		return a.PathId < b.PathId
	})
	return eligible[0].PathId, true
}

// lineEligible reports whether a line with the given quantity and
// productAttributes satisfies eligibility's declared rule: quantity
// within MaxUnitsPerLine (when bounded), every RequiredProductAttribute
// present, and no ExcludedProductAttribute present. NonSortable is a
// property of the freight the path carries as a whole, not a per-line
// input this policy has a signal for, so it is deliberately not
// evaluated here.
func lineEligible(eligibility shared.Eligibility, quantity int, productAttributes []string) bool {
	if max := eligibility.MaxUnitsPerLine(); max != nil && quantity > *max {
		return false
	}

	for _, required := range eligibility.RequiredProductAttributes() {
		if !containsAttribute(productAttributes, required) {
			return false
		}
	}
	for _, excluded := range eligibility.ExcludedProductAttributes() {
		if containsAttribute(productAttributes, excluded) {
			return false
		}
	}
	return true
}

func containsAttribute(attributes []string, target string) bool {
	for _, a := range attributes {
		if a == target {
			return true
		}
	}
	return false
}
