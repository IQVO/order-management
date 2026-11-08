package order

// ship.go is the ship-confirm half of the order lifecycle (ADR 0051):
// the transitions an Order undergoes when outbound-shipping publishes
// ShipmentConfirmed — the fleet's "this actually shipped" fact. It is a
// separate file only to keep order.go under the revive file-length limit;
// it is the same Order aggregate, and every rule below is enforced exactly
// like the allocate/release rules beside it.
//
// The shipped half of the lifecycle:
//
//	Allocated -> Released (ReleaseOrder, wes-work-planning accepted work)
//	Released  -> Shipped  (Ship, outbound-shipping confirmed the load left)
//
// Shipped is terminal: no operation in this context moves a line out of
// it (the physical package has left the building; there is no fact that
// could undo that here). The LineShipped status constant itself lives in
// status.go with the rest of the lifecycle vocabulary.

import "errors"

var (
	// ErrLineNotReleased enforces "only a Released line can ship": a
	// ship-confirm for a line that never released (Pending, Allocated,
	// Backordered) or was Cancelled is a fact mismatch, not a shipping
	// event this aggregate can accept. Re-delivering a ShipmentConfirmed
	// does NOT hit this error: a line already Shipped is the idempotent
	// no-op success (see Ship).
	ErrLineNotReleased = errors.New("order line is not released")

	// ErrOrderNotShippable enforces "only an order in a shippable state
	// transitions": ShipmentConfirmed names an order, and an order with
	// no line in Released or Shipped (still Received/Allocated/
	// Backordered, or fully Cancelled) has nothing ship-confirm could
	// apply to. The consumer treats it as skip-and-log, never a crash
	// (ADR 0051): the physical fact and the order state disagree, and
	// this context records facts, never guesses which side is wrong.
	ErrOrderNotShippable = errors.New("order has no shippable lines")
)

// Ship marks one line Shipped once outbound-shipping confirmed the load
// carrying its package departed. Only a Released line may ship
// (ErrLineNotReleased); a line already Shipped is the idempotent no-op
// success, so re-delivering the same ShipmentConfirmed — or a later one
// for the same order — changes nothing (ADR 0051's idempotency rule at
// the aggregate level, below the CloudEvents-id gate).
//
// Ship reports whether THIS call moved the line, so the use case raises
// OrderLineShipped only for lines that actually transitioned.
func (o *Order) Ship(lineNo int) (shipped bool, err error) {
	line, err := o.line(lineNo)
	if err != nil {
		return false, err
	}
	switch line.status {
	case LineShipped:
		return false, nil
	case LineReleased:
		line.status = LineShipped
		return true, nil
	default:
		return false, ErrLineNotReleased
	}
}

// ShipAll ships every Released line of the order — ShipmentConfirmed is an
// order-level fact (its orders[] carries order ids, not per-package
// detail; the per-package refinement is additive later, ADR 0051's known
// gaps), so one confirmation ships the order's whole released remainder.
//
// It returns the line numbers THIS call moved. Rules:
//
//   - an order with no Released or Shipped line is not shippable
//     (ErrOrderNotShippable) — nothing transitions;
//   - lines in any other state (Allocated, Backordered, Pending) are
//     left exactly as they are — a partial order's un-released lines do
//     not block the released ones from shipping, and are precisely what
//     makes the derived status PartiallyShipped;
//   - an order whose lines are all already Shipped (a re-delivery, or a
//     second load) is the idempotent no-op: nil error, empty result;
//   - one aggregate-wide mutation: the loop cannot half-apply, because
//     every line it touches is Released (the only state Ship accepts)
//     and Ship on a Released line never fails.
func (o *Order) ShipAll() (shippedLines []int, err error) {
	shippable := false
	for _, l := range o.lines {
		if l.status == LineReleased || l.status == LineShipped {
			shippable = true
			break
		}
	}
	if !shippable {
		return nil, ErrOrderNotShippable
	}
	for _, l := range o.lines {
		if l.status != LineReleased {
			continue
		}
		moved, err := o.Ship(l.lineNo)
		if err != nil {
			return nil, err
		}
		if moved {
			shippedLines = append(shippedLines, l.lineNo)
		}
	}
	return shippedLines, nil
}
