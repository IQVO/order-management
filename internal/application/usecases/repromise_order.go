// Package usecases: RepromiseOrder — ADR 0014 §5 / ADR 0018.
//
// This is the final piece of ADR 0014's promise-derived-from-fulfillment-
// capability rollout: the feedback loop. fulfillment-execution's own ADR
// 0025 publishes TaskCPTMissed (a task still open past its CPT) and
// PackageManifested (a SLAM pass) onto warehouse.fulfillment.events, each
// carrying an order_ref shaped like wes-work-planning's WorkUnitId — this
// context's own frozen `{orderId}-line-{lineNo}` formula (see
// allocation.go's WorkUnitID/ParseWorkUnitID). RepromiseOrder is what
// reacts: given the (OrderId, LineNo) the inbound Kafka adapter already
// parsed out of that order_ref, it recomputes the promise for the
// affected shipment group from the CURRENT capability/capacity inputs
// (reusing PromisePolicy.PromiseGroups — ADR 0017's real, shipped
// recompute — not new promise math) and, if the group's promise moved,
// raises OrderRepromised and persists the new group breakdown.
package usecases

import (
	"context"
	"log/slog"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// RepromiseOrder is the Kafka-consumer-driven use case ADR 0014 §5
// names: it is called from the inbound repromise Kafka adapter, never
// from HTTP. It is idempotent on the Kafka message's event_id (see
// ports.RepromiseProcessedEvents' doc comment for the deliberate
// event_id-only simplification of ADR 0014's stated
// "(orderId, sourceEventId)" key), because fulfillment-execution's
// missed-CPT sweep re-emits TaskCPTMissed on every pass for as long as
// a task stays overdue (its own ADR 0025 §4).
type RepromiseOrder struct {
	Orders    ports.OrderRepo
	Promise   order.PromisePolicy
	Events    ports.EventPublisher
	Clock     ports.Clock
	Processed ports.RepromiseProcessedEvents
	// Logger receives structured, non-fatal records for every fail-soft
	// path (already processed, order not found, line not found in any
	// current group, no fresh promise available). Optional: nil
	// silences it, mirroring this repo's other optional-Logger use
	// cases.
	Logger *slog.Logger
	// UnitOfWork brackets the idempotency-gate MarkProcessed, the
	// aggregate Save, and the OrderRepromised Publish in one atomic
	// scope (transactional outbox). Optional: nil means "no
	// transactional backing".
	UnitOfWork ports.UnitOfWork
}

// RepromiseOrderRequest is one fulfillment-execution signal, already
// decoded and parsed by the inbound Kafka adapter: which Kafka message
// (SourceEventId, for idempotency), which order, and which specific
// line's task missed its CPT or whose package manifested.
type RepromiseOrderRequest struct {
	SourceEventId string
	OrderId       shared.OrderId
	LineNo        int
	// Reason names the fulfillment-execution event_type that triggered
	// this recompute ("TaskCPTMissed" or "PackageManifested",
	// verbatim) — carried straight onto OrderRepromised.Reason when the
	// promise moves.
	Reason string
}

// Execute recomputes the promise for the PromiseGroup containing
// req.LineNo and, if it moved, persists the new group breakdown and
// publishes OrderRepromised.
//
// Every "this signal doesn't map to a live, promotable order line"
// condition is FAIL SOFT — logged and treated as a no-op, never an
// error returned to the caller — mirroring this fleet's convention for
// a soft reconciliation input (the same permissive spirit as a
// SKU-not-found lookup elsewhere in the fleet, not a hard business-rule
// violation): the order was never found (stale/unknown order_ref), or
// req.LineNo is not present in ANY of the order's current
// PromiseGroups (a stale/already-cancelled line, or an order that was
// never allocated), or the fresh recompute has nothing to promise at
// all (PromisePolicy.PromiseGroups' own ok=false — no line allocated).
// Only a genuine infrastructure failure (Processed/Orders/Events
// erroring) is returned, so the Kafka consumer's own
// commit-and-skip-on-error convention applies uniformly to both layers.
func (uc *RepromiseOrder) Execute(ctx context.Context, req RepromiseOrderRequest) error {
	return atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.SourceEventId)
		if err != nil {
			return err
		}
		if !isNew {
			uc.log("repromise: event already processed, skipping", "event_id", req.SourceEventId)
			return nil
		}

		o, err := uc.Orders.FindByID(ctx, req.OrderId)
		if err != nil {
			return err
		}
		if o == nil {
			uc.log("repromise: order not found, skipping",
				"event_id", req.SourceEventId, "order_id", req.OrderId.String())
			return nil
		}

		currentGroup, found := groupContainingLine(o.PromiseGroups(), req.LineNo)
		if !found {
			uc.log("repromise: line not found in any current promise group, skipping",
				"event_id", req.SourceEventId, "order_id", req.OrderId.String(), "line_no", req.LineNo)
			return nil
		}

		freshGroups, ok := uc.Promise.PromiseGroups(uc.Clock.Now(), o)
		if !ok {
			uc.log("repromise: no fresh promise available for this order, skipping",
				"event_id", req.SourceEventId, "order_id", req.OrderId.String(), "line_no", req.LineNo)
			return nil
		}

		freshGroup, found := groupContainingLine(freshGroups, req.LineNo)
		if !found {
			uc.log("repromise: line not found in the freshly recomputed promise groups, skipping",
				"event_id", req.SourceEventId, "order_id", req.OrderId.String(), "line_no", req.LineNo)
			return nil
		}

		if !promiseMoved(currentGroup.Promise, freshGroup.Promise) {
			uc.log("repromise: promise unchanged, no re-promise needed",
				"event_id", req.SourceEventId, "order_id", req.OrderId.String(), "line_no", req.LineNo)
			return nil
		}

		o.SetPromiseGroups(freshGroups)
		if err := uc.Orders.Save(ctx, o); err != nil {
			return err
		}
		return uc.Events.Publish(ctx, shared.NewOrderRepromised(
			uc.Clock.Now(), o.ID(), currentGroup.Promise.CptId, freshGroup.Promise.CptId, req.Reason,
		))
	})
}

// groupContainingLine returns the PromiseGroup in groups whose LineNos
// contains lineNo, and whether one was found.
func groupContainingLine(groups []order.PromiseGroup, lineNo int) (order.PromiseGroup, bool) {
	for _, g := range groups {
		for _, n := range g.LineNos {
			if n == lineNo {
				return g, true
			}
		}
	}
	return order.PromiseGroup{}, false
}

// promiseMoved reports whether the fresh promise is a genuinely
// different commitment than the current one: a different CPT identity,
// a different basis, or a different cutoff instant. Any one of the
// three differing is "moved" — comparing CptId alone would miss a
// LeadTime-basis promise (empty CptId either side) whose CutoffAt
// shifted, and comparing CutoffAt alone would miss a
// same-instant-different-CPT-identity edge case that should never
// happen in practice but is not worth relying on not happening.
func promiseMoved(current, fresh order.Promise) bool {
	return current.CptId != fresh.CptId ||
		current.Basis != fresh.Basis ||
		!current.CutoffAt.Equal(fresh.CutoffAt)
}

func (uc *RepromiseOrder) log(msg string, args ...any) {
	if uc.Logger != nil {
		uc.Logger.Warn(msg, args...)
	}
}
