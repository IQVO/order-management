package http

import (
	"context"
	"log/slog"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/pkg/logsanitize"
)

// orderResponse builds the order DTO and, when a planned-capacity read model
// is wired, annotates it with the capacity constraint (ADR 0031). A failing
// read model is logged and omitted: an order read must never fail because an
// advisory annotation is unavailable.
func (s *Server) orderResponse(ctx context.Context, o *order.Order) orderResponse {
	resp := toOrderResponse(o)
	if s.CapacityConstraints == nil {
		return resp
	}
	windows, err := s.CapacityConstraints.For(ctx, o)
	if err != nil {
		slog.WarnContext(ctx, "capacity constraint lookup failed; omitting annotation",
			"order_id", logsanitize.Sanitize(o.ID().String()), "error", err)
		return resp
	}
	if len(windows) == 0 {
		return resp
	}
	brief := make([]plannedCapacityWindowBrief, len(windows))
	for i, w := range windows {
		brief[i] = plannedCapacityWindowBrief{
			PlanID: w.PlanID, WindowStart: w.Start.UTC().Format(timeFormat), WindowEnd: w.End.UTC().Format(timeFormat),
			Shortage: w.Shortage, BottleneckStep: w.BottleneckStep,
		}
	}
	resp.CapacityConstraint = &capacityConstraintResponse{Constrained: true, Site: s.CapacityConstraints.SiteFor(o), Windows: brief}
	return resp
}

const timeFormat = time.RFC3339

func siteResponse(o *order.Order) *string {
	s := o.FulfillmentSiteID()
	if s == nil {
		return nil
	}
	v := s.String()
	return &v
}

func toOrderResponse(o *order.Order) orderResponse {
	lines := make([]orderLineResponse, 0, len(o.Lines()))
	for _, l := range o.Lines() {
		lines = append(lines, orderLineResponse{
			LineNo:        l.LineNo(),
			SKU:           l.SKU().String(),
			Quantity:      l.Quantity(),
			PathID:        l.PathID().String(),
			GiftWrap:      l.GiftWrap(),
			Status:        string(l.Status()),
			ReservationID: l.ReservationID(),
		})
	}

	var promiseDate *string
	if d := o.PromiseDate(); d != nil {
		formatted := d.UTC().Format(timeFormat)
		promiseDate = &formatted
	}

	return orderResponse{
		ID:                   o.ID().String(),
		Status:               string(o.Status()),
		AllowPartialShipment: o.AllowPartialShipment(),
		ReleaseOnAllocation:  o.ReleaseOnAllocation(),
		RequiredShipBy:       o.RequiredShipBy(),
		FulfillmentSiteID:    siteResponse(o),
		PromiseDate:          promiseDate,
		Lines:                lines,
	}
}
