package events

import (
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// The structs below are the log publisher's JSON wire shape for each domain
// event. They are adapter-owned: the domain events carry no serialisation
// tags (ADR: wire shape is an adapter concern), so the field names — and
// their order, which json.Marshal preserves — are pinned here and by
// testdata/log_payloads.golden.

type logHeader struct {
	EventName  string    `json:"eventName"`
	OccurredAt time.Time `json:"occurredAt"`
}

func headerOf(e shared.DomainEvent) logHeader {
	return logHeader{EventName: e.EventName(), OccurredAt: e.OccurredAt()}
}

type orderReceivedPayload struct {
	logHeader
	OrderID   string
	LineCount int
}

type orderLineAllocatedPayload struct {
	logHeader
	OrderID       string
	LineNo        int
	SKU           string
	Quantity      int
	ReservationID string
}

type orderLineBackorderedPayload struct {
	logHeader
	OrderID  string
	LineNo   int
	SKU      string
	Quantity int
}

type releasedLinePayload struct {
	LineNo           int
	SKU              string
	PathID           string
	GiftWrap         bool
	FulfillmentClass string
	PromiseCptId     *string
	PromiseBasis     *string
	PromiseCutoffAt  *time.Time
}

type orderAllocatedPayload struct {
	logHeader
	OrderID      string
	PromiseDate  time.Time
	PromiseCptId string
	PromiseBasis string
	Lines        []releasedLinePayload
}

type orderPartiallyAllocatedPayload struct {
	logHeader
	OrderID          string
	AllocatedLines   int
	BackorderedLines int
	PromiseDate      time.Time
	PromiseCptId     string
	PromiseBasis     string
	Lines            []releasedLinePayload
}

type orderLineReleasedPayload struct {
	logHeader
	OrderID    string
	LineNo     int
	PathID     string
	WorkUnitID string
}

type orderReleasedPayload struct {
	logHeader
	OrderID string
}

type orderCancelledPayload struct {
	logHeader
	OrderID             string
	RevokedReservations int
}

type orderAllocationPartiallyFailedPayload struct {
	logHeader
	OrderID        string
	AllocatedLines int
	RemainingLines int
	Cause          string
}

type orderRepromisedPayload struct {
	logHeader
	OrderID  string
	CptIdOld string
	CptIdNew string
	Reason   string
}

// toReleasedLinePayloads preserves nil-ness: a nil slice marshals to null,
// an empty one to [] — exactly as the domain slice used to.
func toReleasedLinePayloads(lines []shared.ReleasedLine) []releasedLinePayload {
	if lines == nil {
		return nil
	}
	out := make([]releasedLinePayload, 0, len(lines))
	for _, l := range lines {
		out = append(out, releasedLinePayload{
			LineNo: l.LineNo, SKU: l.SKU.String(), PathID: l.PathID.String(),
			GiftWrap: l.GiftWrap, FulfillmentClass: l.FulfillmentClass,
			PromiseCptId: l.PromiseCptId, PromiseBasis: l.PromiseBasis, PromiseCutoffAt: l.PromiseCutoffAt,
		})
	}
	return out
}

// logPayloadOf maps a domain event to its log JSON shape. An event type
// this adapter does not know yet degrades to the name/time header alone
// rather than failing the publish.
func logPayloadOf(event shared.DomainEvent) any {
	h := headerOf(event)
	switch e := event.(type) {
	case shared.OrderReceived:
		return orderReceivedPayload{h, e.OrderID.String(), e.LineCount}
	case shared.OrderLineAllocated:
		return orderLineAllocatedPayload{h, e.OrderID.String(), e.LineNo, e.SKU.String(), e.Quantity, e.ReservationID}
	case shared.OrderLineBackordered:
		return orderLineBackorderedPayload{h, e.OrderID.String(), e.LineNo, e.SKU.String(), e.Quantity}
	case shared.OrderAllocated:
		return orderAllocatedPayload{h, e.OrderID.String(), e.PromiseDate, e.PromiseCptId, e.PromiseBasis, toReleasedLinePayloads(e.Lines)}
	case shared.OrderPartiallyAllocated:
		return orderPartiallyAllocatedPayload{h, e.OrderID.String(), e.AllocatedLines, e.BackorderedLines, e.PromiseDate, e.PromiseCptId, e.PromiseBasis, toReleasedLinePayloads(e.Lines)}
	case shared.OrderLineReleased:
		return orderLineReleasedPayload{h, e.OrderID.String(), e.LineNo, e.PathID.String(), e.WorkUnitID}
	case shared.OrderReleased:
		return orderReleasedPayload{h, e.OrderID.String()}
	case shared.OrderCancelled:
		return orderCancelledPayload{h, e.OrderID.String(), e.RevokedReservations}
	case shared.OrderAllocationPartiallyFailed:
		return orderAllocationPartiallyFailedPayload{h, e.OrderID.String(), e.AllocatedLines, e.RemainingLines, e.Cause}
	case shared.OrderRepromised:
		return orderRepromisedPayload{h, e.OrderID.String(), e.CptIdOld, e.CptIdNew, e.Reason}
	default:
		return h
	}
}
