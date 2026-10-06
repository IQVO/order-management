package order_test

import (
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// TestStatusIsDerivedFromLineStatuses walks the whole derivation table.
// The order-level Status is never stored, so this is the single place the
// order lifecycle is asserted end to end.
func TestStatusIsDerivedFromLineStatuses(t *testing.T) {
	tests := []struct {
		name                 string
		allowPartialShipment bool
		lineStatuses         []order.LineStatus
		want                 order.Status
	}{
		{
			name:         "every line pending is Received",
			lineStatuses: []order.LineStatus{order.LinePending, order.LinePending},
			want:         order.StatusReceived,
		},
		{
			name:         "every line allocated is Allocated",
			lineStatuses: []order.LineStatus{order.LineAllocated, order.LineAllocated},
			want:         order.StatusAllocated,
		},
		{
			name:         "BR3: ship-complete with any backordered line is Backordered",
			lineStatuses: []order.LineStatus{order.LineAllocated, order.LineBackordered},
			want:         order.StatusBackordered,
		},
		{
			name:                 "BR3: partial shipment with a backordered and an allocated line is PartiallyAllocated",
			allowPartialShipment: true,
			lineStatuses:         []order.LineStatus{order.LineAllocated, order.LineBackordered},
			want:                 order.StatusPartiallyAllocated,
		},
		{
			name:                 "partial shipment with nothing allocated is Backordered",
			allowPartialShipment: true,
			lineStatuses:         []order.LineStatus{order.LineBackordered, order.LineBackordered},
			want:                 order.StatusBackordered,
		},
		{
			name:         "ship-complete with every line backordered is Backordered",
			lineStatuses: []order.LineStatus{order.LineBackordered, order.LineBackordered},
			want:         order.StatusBackordered,
		},
		{
			name:         "a partially allocated pass mid-flight is PartiallyAllocated",
			lineStatuses: []order.LineStatus{order.LineAllocated, order.LinePending},
			want:         order.StatusPartiallyAllocated,
		},
		{
			name:         "every line released is Released",
			lineStatuses: []order.LineStatus{order.LineReleased, order.LineReleased},
			want:         order.StatusReleased,
		},
		{
			name:         "some lines released is PartiallyReleased",
			lineStatuses: []order.LineStatus{order.LineReleased, order.LineAllocated},
			want:         order.StatusPartiallyReleased,
		},
		{
			name:                 "a released line outranks a backordered one",
			allowPartialShipment: true,
			lineStatuses:         []order.LineStatus{order.LineReleased, order.LineBackordered},
			want:                 order.StatusPartiallyReleased,
		},
		{
			name:         "every line cancelled is Cancelled",
			lineStatuses: []order.LineStatus{order.LineCancelled, order.LineCancelled},
			want:         order.StatusCancelled,
		},
		{
			name:         "single-line order allocated",
			lineStatuses: []order.LineStatus{order.LineAllocated},
			want:         order.StatusAllocated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := orderWithLineStatuses(tt.allowPartialShipment, tt.lineStatuses...)
			if got := o.Status(); got != tt.want {
				t.Fatalf("Status() = %q, want %q", got, tt.want)
			}
		})
	}
}

// orderWithLineStatuses builds an order whose lines sit in exactly the
// given statuses, via Rehydrate — the only route that can place a line in
// an arbitrary state without walking the legal transitions (which is
// precisely what the invariant tests cover instead).
func orderWithLineStatuses(allowPartialShipment bool, statuses ...order.LineStatus) *order.Order {
	lines := make([]*order.OrderLine, 0, len(statuses))
	for i, status := range statuses {
		var reservationID *string
		if status == order.LineAllocated || status == order.LineReleased {
			id := "res-x"
			reservationID = &id
		}
		lines = append(lines, order.RehydrateOrderLine(
			i+1, shared.SKU("SKU-1"), 1, shared.DefaultPathId, false, status, reservationID,
		))
	}
	return order.Rehydrate(order.OrderSnapshot{ID: "ord-1", Lines: lines, AllowPartialShipment: allowPartialShipment})
}
