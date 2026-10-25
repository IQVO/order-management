package usecases

import (
	"context"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// StaticDemandAssignmentVersion identifies the Phase-1 static demand-site
// policy: every line projects to the one explicitly configured site, and
// the version travels on the wire so a consumer can tell projection
// regimes apart. Future dynamic assignment must introduce a NEW version
// rather than changing the semantics of this value.
const StaticDemandAssignmentVersion = "static-site-v1"

// OrderSiteDemandAssignmentVersion identifies the ADR-0044 regime: the
// line's site is the fulfillment site the CALLER assigned on the order
// (customer-ordering or network-fulfillment) — never inferred here. It
// is a new value alongside, and never a change to, StaticDemandAssignmentVersion.
const OrderSiteDemandAssignmentVersion = "order-site-v1"

// DemandProjectionPolicy selects the explicit static site for the additive
// site/SKU demand projection. It deliberately lives in the application
// layer as a versioned configuration seam: the Order aggregate owns no
// site assignment (a documented ADR-0031 deferral) and Phase-1 performs
// no dynamic assignment — the site is a property of THIS deployment's
// configuration, not of any order. The zero value (empty SiteID or empty
// AssignmentVersion) disables the STATIC regime, so every existing wiring
// and test is unchanged.
//
// ADR 0044 amends this seam without removing it: an order that carries a
// caller-assigned FulfillmentSiteID projects to THAT site under
// OrderSiteDemandAssignmentVersion, even when this policy is the zero
// value; an order without one falls back to this static site exactly as
// before; with neither, nothing is emitted.
type DemandProjectionPolicy struct {
	SiteID            string
	AssignmentVersion string
}

func (p DemandProjectionPolicy) enabled() bool {
	return p.SiteID != "" && p.AssignmentVersion != ""
}

// resolve picks the (site, assignment_version) an order's demand projects
// to: the order's own caller-assigned site wins (ADR 0044), otherwise the
// static policy site (ADR 0035), otherwise ok=false and nothing is emitted.
func (p DemandProjectionPolicy) resolve(o *order.Order) (site, version string, ok bool) {
	if s := o.FulfillmentSiteID(); s != nil {
		return s.String(), OrderSiteDemandAssignmentVersion, true
	}
	if p.enabled() {
		return p.SiteID, p.AssignmentVersion, true
	}
	return "", "", false
}

// publishLineDemand emits one SiteSkuDemandChanged fact for a single line at dueAt,
// to the already-resolved site and assignment version. Callers that owe
// each line its OWN cutoff (ADR 0017 per-shipment-group promising) use
// publishForGroups instead of computing cutoffs themselves.
func publishLineDemand(ctx context.Context, events ports.EventPublisher, occurredAt time.Time, sourceOrderID shared.OrderId, line *order.OrderLine, site, version string, dueAt time.Time, state shared.SiteSkuDemandState) error {
	return events.Publish(ctx, shared.NewSiteSkuDemandChanged(
		occurredAt, sourceOrderID, line.LineNo(), site, line.SKU(), line.Quantity(), dueAt, state, version,
	))
}

// publishForGroups emits one ACTIVE-or-REMOVED fact per line, with due_at
// taken from the line's OWN promise group (ADR 0014 §3 / ADR 0017): a
// split-shipment order's lines are promised to different cutoffs, and a
// site demand consumer reasons about the line's real due date, not the
// order-level latest cutoff. A line no group attributes falls back to the
// order-level PromiseDate (the latest-cutoff projection); an order with
// no promise at all (e.g. an infeasible ADR-0020 deadline, which
// deliberately writes no promise) emits nothing — there is no due date
// to report.
//
// Must run inside the same UnitOfWork as the order Save that produced the
// line state being projected, so the aggregate change and its outbox rows
// commit together (ADR 0022).
func (p DemandProjectionPolicy) publishForGroups(ctx context.Context, events ports.EventPublisher, occurredAt time.Time, o *order.Order, lines []*order.OrderLine, state shared.SiteSkuDemandState) error {
	site, version, ok := p.resolve(o)
	if !ok || len(lines) == 0 {
		return nil
	}
	byLine := promiseGroupByLine(o)
	for _, line := range lines {
		dueAt := o.PromiseDate()
		if g, ok := byLine[line.LineNo()]; ok {
			cutoff := g.Promise.CutoffAt
			dueAt = &cutoff
		}
		if dueAt == nil {
			continue
		}
		if err := publishLineDemand(ctx, events, occurredAt, o.ID(), line, site, version, *dueAt, state); err != nil {
			return err
		}
	}
	return nil
}
