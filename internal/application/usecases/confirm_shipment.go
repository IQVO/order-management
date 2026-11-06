// Package usecases: ConfirmShipment — ADR 0051's inbound ship-confirm half.
//
// outbound-shipping publishes ShipmentConfirmed when a closed load has
// physically left the site on a trailer. This use case applies that fact
// to every order the event names: each order's Released lines become
// Shipped (the derived order status reaching Shipped or
// PartiallyShipped), the aggregate is saved, and the OrderLineShipped /
// OrderShipped / OrderPartiallyShipped domain events are raised — all in
// ONE UnitOfWork with the idempotency claim on the CloudEvents id, so a
// redelivered event changes nothing.
//
// Scope note (ADR 0051 §known gaps): ShipmentConfirmed.orders[] carries
// ORDER ids only — no per-package, per-line detail — so one confirmation
// ships the order's whole released remainder. The per-package refinement
// is additive later and changes nothing here.
package usecases

import (
	"context"
	"errors"
	"log/slog"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ErrInvalidShipmentConfirmation marks a ConfirmShipmentRequest that can
// never be applied (no CloudEvents id). The consumer dead-letters it
// rather than retrying, exactly like ErrInvalidReleaseConfirmation.
var ErrInvalidShipmentConfirmation = errors.New("invalid shipment confirmation request")

// ShipmentOrderOutcome is what handling one named order came to. The
// string values double as structured-log values; the consumer logs one
// record per non-shipped outcome (shipped needs no warning).
type ShipmentOrderOutcome string

const (
	// ShipmentOrderShipped: at least one line moved Released -> Shipped
	// (the order may now be Shipped or PartiallyShipped).
	ShipmentOrderShipped ShipmentOrderOutcome = "shipped"
	// ShipmentOrderNoChange: a FIRST-seen event whose order was already
	// fully shipped by an earlier event — nothing moved, nothing saved.
	ShipmentOrderNoChange ShipmentOrderOutcome = "no_change"
	// ShipmentOrderDuplicate: the CloudEvents id was already processed;
	// the whole event is the idempotent no-op (checked before any order
	// is even loaded).
	ShipmentOrderDuplicate ShipmentOrderOutcome = "duplicate"
	// ShipmentOrderIgnoredNoOrders: the event named no orders at all
	// (an empty orders[] list — a load whose packages all manifested
	// without an order id).
	ShipmentOrderIgnoredNoOrders ShipmentOrderOutcome = "ignored_no_orders"
	// ShipmentOrderIgnoredUnknownOrder: the event names an order this
	// context has no record of — skip-and-log (ADR 0051), never an
	// error: the consumer commits past it rather than crashing or
	// dead-lettering a fact this context simply cannot apply.
	ShipmentOrderIgnoredUnknownOrder ShipmentOrderOutcome = "ignored_unknown_order"
	// ShipmentOrderIgnoredNotShippable: the named order has no line in
	// Released or Shipped (still Received/Allocated/Backordered, or
	// cancelled) — order.ErrOrderNotShippable surfaced as a skip, for
	// the same never-crash reason.
	ShipmentOrderIgnoredNotShippable ShipmentOrderOutcome = "ignored_not_shippable"
)

// ConfirmShipmentRequest is one decoded ShipmentConfirmed: which Kafka
// message (EventID, the idempotency key), which load (LoadID, carried
// onto the raised events as the correlation back to the physical fact),
// and which orders the load carried (OrderIDs, from data.orders[].order_id).
type ConfirmShipmentRequest struct {
	EventID  string
	LoadID   string
	OrderIDs []shared.OrderId
}

// ConfirmShipment applies one ShipmentConfirmed. Kafka-consumer-driven,
// never HTTP: the inbound outbound-shipping consumer is its only caller.
type ConfirmShipment struct {
	Orders    ports.OrderRepo
	Events    ports.EventPublisher
	Clock     ports.Clock
	Processed ports.ShipmentProcessedEvents
	// Logger receives structured, non-fatal records for every fail-soft
	// path (duplicate id, no orders, unknown order, not shippable).
	// Optional: nil silences it, mirroring the other optional-Logger use
	// cases.
	Logger *slog.Logger
	// UnitOfWork brackets the idempotency-gate MarkProcessed, every
	// aggregate Save, and every event Publish in one atomic scope
	// (transactional outbox). Optional: nil means "no transactional
	// backing".
	UnitOfWork ports.UnitOfWork
}

// Execute applies req and reports, per order, what came of it.
//
// Fail-soft contract (ADR 0051): an event naming an UNKNOWN order, or an
// order in a non-shippable state, is logged and skipped — nil error, no
// state change for that order, the other named orders still ship. Only a
// genuine infrastructure failure (repo/publisher erroring) is returned,
// so the consumer's retry/DLQ machinery applies uniformly.
func (uc *ConfirmShipment) Execute(ctx context.Context, req ConfirmShipmentRequest) (map[shared.OrderId]ShipmentOrderOutcome, error) {
	if req.EventID == "" {
		return nil, ErrInvalidShipmentConfirmation
	}
	outcomes := make(map[shared.OrderId]ShipmentOrderOutcome, len(req.OrderIDs))
	if len(req.OrderIDs) == 0 {
		uc.log(ctx, "shipment confirmation names no orders, skipping",
			"event_id", req.EventID, "load_id", req.LoadID)
		outcomes[shared.OrderId("")] = ShipmentOrderIgnoredNoOrders
		return outcomes, nil
	}
	return outcomes, atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		isNew, err := uc.Processed.MarkProcessed(ctx, req.EventID)
		if err != nil {
			return err
		}
		if !isNew {
			uc.log(ctx, "shipment confirmation already processed, skipping",
				"event_id", req.EventID, "load_id", req.LoadID)
			for _, id := range req.OrderIDs {
				outcomes[id] = ShipmentOrderDuplicate
			}
			return nil
		}
		for _, id := range req.OrderIDs {
			outcome, err := uc.shipOne(ctx, req, id)
			if err != nil {
				return err
			}
			outcomes[id] = outcome
		}
		return nil
	})
}

// shipOne applies the confirmation to one named order inside the caller's
// UnitOfWork. Returned errors are infrastructure only; every
// "this event does not map to a shippable order" condition is an outcome.
func (uc *ConfirmShipment) shipOne(ctx context.Context, req ConfirmShipmentRequest, id shared.OrderId) (ShipmentOrderOutcome, error) {
	o, err := uc.Orders.FindByID(ctx, id)
	if err != nil {
		return "", err
	}
	if o == nil {
		uc.log(ctx, "shipment confirmation names an unknown order, skipping",
			"event_id", req.EventID, "load_id", req.LoadID, "order_id", id.String())
		return ShipmentOrderIgnoredUnknownOrder, nil
	}
	shipped, err := o.ShipAll()
	if err != nil {
		if errors.Is(err, order.ErrOrderNotShippable) {
			uc.log(ctx, "shipment confirmation names an order with no released lines, skipping",
				"event_id", req.EventID, "load_id", req.LoadID, "order_id", id.String())
			return ShipmentOrderIgnoredNotShippable, nil
		}
		return "", err
	}
	if len(shipped) == 0 {
		uc.log(ctx, "shipment confirmation re-delivered under a new id, order already shipped",
			"event_id", req.EventID, "load_id", req.LoadID, "order_id", id.String())
		return ShipmentOrderNoChange, nil
	}
	if err := uc.Orders.Save(ctx, o); err != nil {
		return "", err
	}
	for _, lineNo := range shipped {
		if err := uc.Events.Publish(ctx, shared.NewOrderLineShipped(uc.Clock.Now(), id, lineNo, req.LoadID)); err != nil {
			return "", err
		}
	}
	if err := uc.publishOrderLevel(ctx, o, req.LoadID); err != nil {
		return "", err
	}
	return ShipmentOrderShipped, nil
}

// publishOrderLevel raises the one order-level fact this transition
// reached: OrderShipped when every line has now shipped, or
// OrderPartiallyShipped when some non-cancelled line has not (see
// Order.Status, ADR 0051). An order still showing Released or earlier
// statuses (a split-shipment order whose other lines never released)
// raises OrderPartiallyShipped — something shipped, not everything.
func (uc *ConfirmShipment) publishOrderLevel(ctx context.Context, o *order.Order, loadID string) error {
	now := uc.Clock.Now()
	switch o.Status() {
	case order.StatusShipped:
		return uc.Events.Publish(ctx, shared.NewOrderShipped(now, o.ID(), loadID))
	default:
		return uc.Events.Publish(ctx, shared.NewOrderPartiallyShipped(now, o.ID(), loadID))
	}
}

func (uc *ConfirmShipment) log(_ context.Context, msg string, args ...any) {
	if uc.Logger != nil {
		uc.Logger.Warn(msg, args...)
	}
}
