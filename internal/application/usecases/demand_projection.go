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

// DemandProjectionPolicy selects the explicit static site for the additive
// site/SKU demand projection. It deliberately lives in the application
// layer as a versioned configuration seam: the Order aggregate owns no
// site assignment (a documented ADR-0031 deferral) and Phase-1 performs
// no dynamic assignment — the site is a property of THIS deployment's
// configuration, not of any order. The zero value (empty SiteID or empty
// AssignmentVersion) leaves the whole projection disabled, so every
// existing wiring and test is unchanged.
type DemandProjectionPolicy struct {
	SiteID            string
	AssignmentVersion string
}

func (p DemandProjectionPolicy) enabled() bool {
	return p.SiteID != "" && p.AssignmentVersion != ""
}

// publish emits one SiteSkuDemandChanged fact for a single line at dueAt.
// Callers that owe each line its OWN cutoff (ADR 0017 per-shipment-group
// promising) use publishForGroups instead of computing cutoffs themselves.
func (p DemandProjectionPolicy) publish(ctx context.Context, events ports.EventPublisher, occurredAt time.Time, sourceOrderID shared.OrderId, line *order.OrderLine, dueAt time.Time, state shared.SiteSkuDemandState) error {
	if !p.enabled() {
		return nil
	}
	return events.Publish(ctx, shared.NewSiteSkuDemandChanged(
		occurredAt, sourceOrderID, line.LineNo(), p.SiteID, line.SKU(), line.Quantity(), dueAt, state, p.AssignmentVersion,
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
	if !p.enabled() || len(lines) == 0 {
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
		if err := p.publish(ctx, events, occurredAt, o.ID(), line, *dueAt, state); err != nil {
			return err
		}
	}
	return nil
}
