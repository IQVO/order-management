package order

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// PlannedCapacityStatus is the lifecycle state of a warehouse-planning
// CapacityPlan as order-management sees it (ADR 0031). Only a PUBLISHED
// plan is a statement an operator has committed to; a DRAFT is kept in
// the read model for visibility but never influences an order.
type PlannedCapacityStatus string

const (
	// PlannedCapacityDraft: CapacityPlanCreated was observed, the plan has
	// not (yet) been published. Never constrains a promise.
	PlannedCapacityDraft PlannedCapacityStatus = "DRAFT"
	// PlannedCapacityPublished: CapacityPlanPublished (or the
	// CapacityShortageDetected raised with it) was observed.
	PlannedCapacityPublished PlannedCapacityStatus = "PUBLISHED"
)

// ErrInvalidPlannedCapacity is returned by PlannedCapacityWindow.Validate
// for a payload that can never be a sane planned-capacity statement
// (missing identity, inverted window, negative quantity, unknown
// status). It is deterministic: retrying the same event cannot help, so a
// consumer dead-letters it instead of retrying.
var ErrInvalidPlannedCapacity = errors.New("order: invalid planned capacity window")

// PlannedCapacityWindow is one row of order-management's LOCAL read model
// of warehouse-planning's CapacityPlans (ADR 0031): a PLANNED statement
// about a future [Start, End) window at one site, with the demand the
// planned capacity cannot process (Shortage, in orders). It is NOT live
// execution state, and order-management never calls warehouse-planning
// to refresh it — it only ever mirrors the events that service publishes.
//
// Identity is PlanID (last writer wins, see Supersedes); End is exclusive,
// as warehouse-planning publishes it.
type PlannedCapacityWindow struct {
	PlanID             string
	WarehouseID        string
	Location           string // site/building code, e.g. "SIM1"
	PathID             string // warehouse-planning's own path label, informational
	Start              time.Time
	End                time.Time
	AssignedDemand     float64
	CapacityOverWindow float64
	Shortage           float64
	BottleneckStep     string
	Status             PlannedCapacityStatus
	// AsOf is the CloudEvents `time` of the event that wrote this row.
	AsOf time.Time
}

// Validate reports ErrInvalidPlannedCapacity (wrapped with the reason)
// when w can never be a sane statement. A shortage of exactly zero is
// valid (a published plan with enough capacity).
func (w PlannedCapacityWindow) Validate() error {
	if w.PlanID == "" {
		return invalidPlanned("plan id is required")
	}
	if w.Location == "" {
		return invalidPlanned("location is required")
	}
	if w.Start.IsZero() || w.End.IsZero() {
		return invalidPlanned("window start and end are required")
	}
	if !w.End.After(w.Start) {
		return invalidPlanned("window end must be after window start")
	}
	if w.AssignedDemand < 0 || w.CapacityOverWindow < 0 || w.Shortage < 0 {
		return invalidPlanned("quantities must not be negative")
	}
	if w.Status != PlannedCapacityDraft && w.Status != PlannedCapacityPublished {
		return invalidPlanned("unknown status " + string(w.Status))
	}
	if w.AsOf.IsZero() {
		return invalidPlanned("event time is required")
	}
	return nil
}

func invalidPlanned(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidPlannedCapacity, reason)
}

// Overlaps reports whether the half-open interval [from, until) shares at
// least one instant with this window's [Start, End). Both intervals are
// half-open, so a promise that ends exactly when the window starts (or a
// window that ends exactly when the interval starts) does NOT overlap; one
// second of shared time does. An empty or inverted interval
// (until <= from) overlaps nothing.
func (w PlannedCapacityWindow) Overlaps(from, until time.Time) bool {
	if !from.Before(until) {
		return false
	}
	return w.Start.Before(until) && from.Before(w.End)
}

// Constraining reports whether this window is a published shortage — the
// only kind of row that may influence an order.
func (w PlannedCapacityWindow) Constraining() bool {
	return w.Status == PlannedCapacityPublished && w.Shortage > 0
}

// Supersedes reports whether w should replace prev for the same PlanID
// (last-writer-wins). A later (or equal) occurred-at wins, so an
// out-of-order older event never overwrites a newer one; and a DRAFT
// never replaces a PUBLISHED row, because a plan is published at most
// once and is never un-published.
func (w PlannedCapacityWindow) Supersedes(prev PlannedCapacityWindow) bool {
	if prev.Status == PlannedCapacityPublished && w.Status == PlannedCapacityDraft {
		return false
	}
	return !w.AsOf.Before(prev.AsOf)
}

// CapacityConstraints returns the published shortage windows at site that
// the order's promise is exposed to, ordered by window start then plan id.
//
// The order's promise is a statement that it leaves the building by a
// cutoff, so the work it needs happens in [now, cutoff). The order is
// capacity-constrained when that interval overlaps a PUBLISHED shortage
// window at the same site. With per-shipment-group promises (ADR 0017)
// every group's cutoff is checked; otherwise the legacy single promise
// date is. An order with no promise yet, or a cancelled one, is never
// constrained. This only ANNOTATES: it neither moves the promise nor
// touches allocation (ADR 0017's fill-or-kill is out of scope), and with
// no windows it returns nil, so behaviour with no planning events is
// exactly what it was.
func CapacityConstraints(o *Order, now time.Time, site string, windows []PlannedCapacityWindow) []PlannedCapacityWindow {
	if o.Status() == StatusCancelled {
		return nil
	}
	cutoffs := promiseCutoffs(o)
	var out []PlannedCapacityWindow
	for _, w := range windows {
		if w.Location != site || !w.Constraining() {
			continue
		}
		for _, cutoff := range cutoffs {
			if w.Overlaps(now, cutoff) {
				out = append(out, w)
				break
			}
		}
	}
	slices.SortFunc(out, func(a, b PlannedCapacityWindow) int {
		if c := a.Start.Compare(b.Start); c != 0 {
			return c
		}
		return strings.Compare(a.PlanID, b.PlanID)
	})
	return out
}

// promiseCutoffs lists every cutoff the order is promised for: one per
// promise group, else the legacy single promise date, else none.
func promiseCutoffs(o *Order) []time.Time {
	if groups := o.PromiseGroups(); len(groups) > 0 {
		out := make([]time.Time, len(groups))
		for i, g := range groups {
			out[i] = g.Promise.CutoffAt
		}
		return out
	}
	if d := o.PromiseDate(); d != nil {
		return []time.Time{*d}
	}
	return nil
}
