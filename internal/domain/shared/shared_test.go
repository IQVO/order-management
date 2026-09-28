package shared_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

func TestNewOrderId(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    shared.OrderId
		wantErr error
	}{
		{name: "valid", value: "ord-1", want: "ord-1"},
		{name: "empty is rejected", value: "", wantErr: shared.ErrEmptyOrderID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shared.NewOrderId(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got = %q, want %q", got, tt.want)
			}
			if tt.wantErr == nil && got.String() != tt.value {
				t.Fatalf("String() = %q, want %q", got.String(), tt.value)
			}
		})
	}
}

func TestNewSKU(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr error
	}{
		{name: "valid", value: "SKU-1"},
		{name: "empty is rejected", value: "", wantErr: shared.ErrEmptySKU},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shared.NewSKU(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got.String() != tt.value {
				t.Fatalf("String() = %q, want %q", got.String(), tt.value)
			}
		})
	}
}

func TestNewPathId(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    shared.PathId
		wantErr error
	}{
		{name: "valid", value: "singles", want: "singles"},
		{name: "empty is rejected", value: "", wantErr: shared.ErrEmptyPathID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shared.NewPathId(tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewPathIdOrDefault(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  shared.PathId
	}{
		{name: "empty falls back to the default path", value: "", want: shared.DefaultPathId},
		{name: "explicit value is kept", value: "singles", want: "singles"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shared.NewPathIdOrDefault(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got = %q, want %q", got, tt.want)
			}
			if got.String() != string(tt.want) {
				t.Fatalf("String() = %q, want %q", got.String(), tt.want)
			}
		})
	}
}

func TestDomainEventsCarryNameAndTimestamp(t *testing.T) {
	at := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	promise := at.Add(48 * time.Hour)

	tests := []struct {
		name  string
		event shared.DomainEvent
		want  string
	}{
		{"OrderReceived", shared.NewOrderReceived(at, "ord-1", 2), "OrderReceived"},
		{"OrderLineAllocated", shared.NewOrderLineAllocated(at, "ord-1", 1, "SKU-1", 3, "res-1"), "OrderLineAllocated"},
		{"OrderLineBackordered", shared.NewOrderLineBackordered(at, "ord-1", 2, "SKU-2", 4), "OrderLineBackordered"},
		{"OrderAllocated", shared.NewOrderAllocated(at, "ord-1", promise, nil), "OrderAllocated"},
		{"OrderPartiallyAllocated", shared.NewOrderPartiallyAllocated(at, "ord-1", 1, 1, promise, nil), "OrderPartiallyAllocated"},
		{"OrderLineReleased", shared.NewOrderLineReleased(at, "ord-1", 1, "pick", "wu-1"), "OrderLineReleased"},
		{"OrderReleased", shared.NewOrderReleased(at, "ord-1"), "OrderReleased"},
		{"OrderCancelled", shared.NewOrderCancelled(at, "ord-1", 2), "OrderCancelled"},
		{"OrderRepromised", shared.NewOrderRepromised(at, "ord-1", "sp1-1200", "sp1-1800", "TaskCPTMissed"), "OrderRepromised"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.EventName(); got != tt.want {
				t.Fatalf("EventName() = %q, want %q", got, tt.want)
			}
			if got := tt.event.OccurredAt(); !got.Equal(at) {
				t.Fatalf("OccurredAt() = %v, want %v", got, at)
			}
		})
	}
}

func TestEventPayloadsCarryTheirDetail(t *testing.T) {
	at := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	promise := at.Add(24 * time.Hour)

	t.Run("OrderLineAllocated", func(t *testing.T) { assertOrderLineAllocatedDetail(t, at) })
	t.Run("OrderLineBackordered", func(t *testing.T) { assertOrderLineBackorderedDetail(t, at) })
	t.Run("OrderPartiallyAllocated", func(t *testing.T) { assertOrderPartiallyAllocatedDetail(t, at, promise) })
	t.Run("OrderLineReleased", func(t *testing.T) { assertOrderLineReleasedDetail(t, at) })
	t.Run("OrderCancelled", func(t *testing.T) { assertOrderCancelledDetail(t, at) })
	t.Run("OrderAllocated", func(t *testing.T) { assertOrderAllocatedDetail(t, at, promise) })
	t.Run("OrderReceived", func(t *testing.T) { assertOrderReceivedDetail(t, at) })
	t.Run("OrderRepromised", func(t *testing.T) { assertOrderRepromisedDetail(t, at) })
	t.Run("OrderRepromised with empty CptIdOld (LeadTime basis)", func(t *testing.T) { assertOrderRepromisedEmptyCptIdOldDetail(t, at) })
}

func assertOrderLineAllocatedDetail(t *testing.T, at time.Time) {
	t.Helper()
	allocated := shared.NewOrderLineAllocated(at, "ord-7", 2, "SKU-9", 5, "res-42")
	if allocated.OrderID != "ord-7" || allocated.LineNo != 2 || allocated.SKU != "SKU-9" ||
		allocated.Quantity != 5 || allocated.ReservationID != "res-42" {
		t.Fatalf("OrderLineAllocated lost detail: %+v", allocated)
	}
}

func assertOrderLineBackorderedDetail(t *testing.T, at time.Time) {
	t.Helper()
	backordered := shared.NewOrderLineBackordered(at, "ord-7", 3, "SKU-3", 1)
	if backordered.LineNo != 3 || backordered.SKU != "SKU-3" || backordered.Quantity != 1 {
		t.Fatalf("OrderLineBackordered lost detail: %+v", backordered)
	}
}

func assertOrderPartiallyAllocatedDetail(t *testing.T, at, promise time.Time) {
	t.Helper()
	partial := shared.NewOrderPartiallyAllocated(at, "ord-7", 2, 1, promise, nil)
	if partial.AllocatedLines != 2 || partial.BackorderedLines != 1 || !partial.PromiseDate.Equal(promise) {
		t.Fatalf("OrderPartiallyAllocated lost detail: %+v", partial)
	}
}

func assertOrderLineReleasedDetail(t *testing.T, at time.Time) {
	t.Helper()
	released := shared.NewOrderLineReleased(at, "ord-7", 1, "singles", "wu-1")
	if released.PathID != "singles" || released.WorkUnitID != "wu-1" {
		t.Fatalf("OrderLineReleased lost detail: %+v", released)
	}
}

func assertOrderCancelledDetail(t *testing.T, at time.Time) {
	t.Helper()
	cancelled := shared.NewOrderCancelled(at, "ord-7", 3)
	if cancelled.RevokedReservations != 3 {
		t.Fatalf("OrderCancelled lost detail: %+v", cancelled)
	}
}

func assertOrderAllocatedDetail(t *testing.T, at, promise time.Time) {
	t.Helper()
	full := shared.NewOrderAllocated(at, "ord-7", promise, []shared.ReleasedLine{
		{LineNo: 1, SKU: "SKU-1", PathID: "pick", GiftWrap: true},
	})
	if !full.PromiseDate.Equal(promise) {
		t.Fatalf("OrderAllocated lost promise date: %+v", full)
	}
	if len(full.Lines) != 1 || full.Lines[0].SKU != "SKU-1" || full.Lines[0].PathID != "pick" || !full.Lines[0].GiftWrap {
		t.Fatalf("OrderAllocated lost lines detail: %+v", full.Lines)
	}
}

func assertOrderReceivedDetail(t *testing.T, at time.Time) {
	t.Helper()
	received := shared.NewOrderReceived(at, "ord-7", 4)
	if received.LineCount != 4 {
		t.Fatalf("OrderReceived lost line count: %+v", received)
	}
}

func assertOrderRepromisedDetail(t *testing.T, at time.Time) {
	t.Helper()
	repromised := shared.NewOrderRepromised(at, "ord-7", "sp1-1200", "sp1-1800", "TaskCPTMissed")
	if repromised.OrderID != "ord-7" || repromised.CptIdOld != "sp1-1200" || repromised.CptIdNew != "sp1-1800" || repromised.Reason != "TaskCPTMissed" {
		t.Fatalf("OrderRepromised lost detail: %+v", repromised)
	}
}

func assertOrderRepromisedEmptyCptIdOldDetail(t *testing.T, at time.Time) {
	t.Helper()
	repromisedFromLeadTime := shared.NewOrderRepromised(at, "ord-7", "", "sp1-1800", "PackageManifested")
	if repromisedFromLeadTime.CptIdOld != "" {
		t.Fatalf("OrderRepromised should allow an empty CptIdOld for a LeadTime-basis previous promise: %+v", repromisedFromLeadTime)
	}
}
