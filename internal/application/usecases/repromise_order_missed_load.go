// repromise_order_missed_load.go is ADR 0051's second inbound half: the
// re-promise path for outbound-shipping's PackageMissedLoad — a package
// that was not loaded by its cutoff, so its order is NOT shipping on the
// promise it holds.
//
// It deliberately adds NO new machinery: this is the SAME RepromiseOrder
// use case, the SAME PromisePolicy recompute, the SAME OrderRepromised
// event and the SAME idempotency gate, exposed at order granularity
// because PackageMissedLoad.source_order_id is a bare OrderId (the
// producer strips any -line-N suffix; its orders[] on ShipmentConfirmed
// carries the same shape), not the WorkUnitId-shaped per-line reference
// TaskCPTMissed/PackageManifested carry. What the per-line Execute does
// for one line's group, ExecuteForMissedLoad does for EVERY group the
// order currently holds.
package usecases

import (
	"context"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// reasonPackageMissedLoad is the repromise reason recorded on
// OrderRepromised when the trigger was outbound-shipping's
// PackageMissedLoad — the upstream event name, the same convention
// reasonTaskCPTMissed/reasonPackageManifested follow.
const reasonPackageMissedLoad = "PackageMissedLoad"

// RepromiseMissedLoadRequest is one decoded PackageMissedLoad: which
// Kafka message (SourceEventId, the idempotency key — the SAME
// repromise_processed_events gate the per-line path uses, so one id can
// never be evaluated twice by either path), which order the missed
// package belonged to (OrderId, from data.source_order_id), and the
// missed load's cutoff (Cutoff — carried for logging/telemetry only; the
// re-promise itself recomputes from CURRENT capability inputs, the
// existing machinery's convention, never from the missed cutoff).
type RepromiseMissedLoadRequest struct {
	SourceEventId string
	OrderId       shared.OrderId
	Cutoff        time.Time
}

// ExecuteForMissedLoad re-promises every promise group the named order
// currently holds, publishing one OrderRepromised per group whose promise
// moved. Every "this signal does not map to a re-promisable order"
// condition is FAIL SOFT exactly like Execute: an unknown order, an order
// with no current groups (never allocated, or a pre-ADR-0014 row without
// a group breakdown), a fully-shipped or cancelled order (no
// Allocated/Released line left for the policy to reason about — see ADR
// 0051 §known gaps), or no feasible fresh promise (a requiredShipBy the
// order can no longer make). Only an infrastructure failure is returned.
//
// What the existing re-promise cannot yet do (ADR 0051 §known gaps,
// recorded rather than papered over): it re-promises groups holding
// Allocated/Released lines only. A missed load whose order is entirely
// Shipped is a late fact about a shipment that already left — nothing to
// re-promise — and a missed load for an order with NO recorded promise
// groups keeps its legacy single-valued promise untouched. Re-promising
// THOSE orders needs a promise-recompute for legacy rows, which is not
// this repo's current machinery and is deliberately not invented here.
func (uc *RepromiseOrder) ExecuteForMissedLoad(ctx context.Context, req RepromiseMissedLoadRequest) error {
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.SourceEventId)
		if err != nil {
			return err
		}
		if !isNew {
			uc.log("repromise: event already processed, skipping", "event_id", req.SourceEventId)
			return nil
		}

		o, freshGroups, moved, ok, err := uc.missedLoadMoves(ctx, req)
		if err != nil || !ok {
			return err
		}

		o.SetPromiseGroups(freshGroups)
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.publishMoved(ctx, o, moved)
	})
}

// missedLoadMoves loads the missed-load order and recomputes its promise
// groups, reporting whether ANY group's promise moved (ok) together with
// the fresh breakdown to persist and the pairs to publish. Every
// fail-soft skip (unknown order, no current groups, no feasible fresh
// promise, nothing moved) is logged here and reports ok=false, nil.
func (uc *RepromiseOrder) missedLoadMoves(ctx context.Context, req RepromiseMissedLoadRequest) (o *order.Order, fresh []order.PromiseGroup, moved []groupMove, ok bool, err error) {
	o, err = uc.Orders.FindByID(ctx, req.OrderId)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if o == nil {
		uc.log("repromise: missed-load order not found, skipping",
			"event_id", req.SourceEventId, "order_id", req.OrderId.String())
		return nil, nil, nil, false, nil
	}

	currentGroups := o.PromiseGroups()
	if len(currentGroups) == 0 {
		uc.log("repromise: missed-load order has no current promise groups, skipping",
			"event_id", req.SourceEventId, "order_id", req.OrderId.String())
		return nil, nil, nil, false, nil
	}

	freshGroups, feasible := uc.freshPromiseGroups(o)
	if !feasible {
		uc.log("repromise: no fresh promise available for this order, skipping",
			"event_id", req.SourceEventId, "order_id", req.OrderId.String())
		return nil, nil, nil, false, nil
	}

	moved = movedGroups(currentGroups, freshGroups)
	if len(moved) == 0 {
		uc.log("repromise: promise unchanged, no re-promise needed",
			"event_id", req.SourceEventId, "order_id", req.OrderId.String())
		return nil, nil, nil, false, nil
	}
	return o, freshGroups, moved, true, nil
}

// publishMoved raises one OrderRepromised per moved group (and the
// demand projection for each), still inside the caller's UnitOfWork.
func (uc *RepromiseOrder) publishMoved(ctx context.Context, o *order.Order, moved []groupMove) error {
	now := uc.Clock.Now()
	for _, m := range moved {
		if err := uc.publishMovedDemandFor(ctx, o, m.fresh); err != nil {
			return err
		}
		if err := uc.Events.Publish(ctx, shared.NewOrderRepromised(
			now, o.ID(), m.current.Promise.CptId, m.fresh.Promise.CptId, reasonPackageMissedLoad,
		)); err != nil {
			return err
		}
	}
	return nil
}

// groupMove is one promise group whose commitment moved: the group as it
// was, and the freshly recomputed group covering (a member line of) it.
type groupMove struct {
	current order.PromiseGroup
	fresh   order.PromiseGroup
}

// movedGroups pairs each current group with the fresh group covering its
// first member line and keeps the pairs whose promise actually moved
// (same comparison as the per-line path's promiseMoved). Lines can
// regroup between the two computations; pairing on membership keeps the
// comparison honest about WHICH commitment moved, which is what the
// published OrderRepromised names.
func movedGroups(current, fresh []order.PromiseGroup) []groupMove {
	var moved []groupMove
	for _, g := range current {
		if len(g.LineNos) == 0 {
			continue
		}
		f, found := groupContainingLine(fresh, g.LineNos[0])
		if !found {
			continue
		}
		if promiseMoved(g.Promise, f.Promise) {
			moved = append(moved, groupMove{current: g, fresh: f})
		}
	}
	return moved
}

// publishMovedDemandFor is publishMovedDemand over an explicit group
// instead of the per-line path's freshly-computed one — the same demand
// projection, the same fresh cutoff, still inside the caller's
// UnitOfWork. A no-op unless the projection is enabled.
func (uc *RepromiseOrder) publishMovedDemandFor(ctx context.Context, o *order.Order, freshGroup order.PromiseGroup) error {
	var movedLines []*order.OrderLine
	for _, l := range o.Lines() {
		if lineInGroup(freshGroup, l.LineNo()) {
			movedLines = append(movedLines, l)
		}
	}
	return uc.DemandProjection.publishForGroups(ctx, uc.Events, uc.Clock.Now(), o, movedLines, shared.SiteSkuDemandActive)
}
