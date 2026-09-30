package order_test

import (
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func TestNewOrderLineInvariants(t *testing.T) {
	tests := []struct {
		name       string
		sku        shared.SKU
		quantity   int
		pathID     shared.PathId
		giftWrap   bool
		wantErr    error
		wantPathID shared.PathId
	}{
		{
			name: "valid line", sku: "SKU-1", quantity: 3, pathID: "singles", giftWrap: true,
			wantPathID: "singles",
		},
		{
			name: "absent path id defaults to pick", sku: "SKU-1", quantity: 1, pathID: "",
			wantPathID: shared.DefaultPathId,
		},
		{
			name: "empty sku is rejected", sku: "", quantity: 1, pathID: "pick",
			wantErr: shared.ErrEmptySKU,
		},
		{
			name: "zero quantity is rejected", sku: "SKU-1", quantity: 0, pathID: "pick",
			wantErr: shared.ErrNonPositiveQuantity,
		},
		{
			name: "negative quantity is rejected", sku: "SKU-1", quantity: -5, pathID: "pick",
			wantErr: shared.ErrNonPositiveQuantity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertNewOrderLineOutcome(t, tt.sku, tt.quantity, tt.pathID, tt.wantPathID, tt.giftWrap, tt.wantErr)
		})
	}
}

// assertNewOrderLineOutcome pins one NewOrderLine case: the exact
// validation error (and no line) for rejected input, or the defaulted,
// Pending, detail-preserving shape for valid input.
func assertNewOrderLineOutcome(t *testing.T, sku shared.SKU, quantity int, pathID, wantPathID shared.PathId, giftWrap bool, wantErr error) {
	t.Helper()
	line, err := order.NewOrderLine(1, sku, quantity, pathID, giftWrap)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if wantErr != nil {
		if line != nil {
			t.Fatalf("expected no line on error, got %+v", line)
		}
		return
	}
	if line.PathID() != wantPathID {
		t.Fatalf("PathID() = %q, want %q", line.PathID(), wantPathID)
	}
	if line.Status() != order.LinePending {
		t.Fatalf("Status() = %q, want %q", line.Status(), order.LinePending)
	}
	if line.SKU() != sku || line.Quantity() != quantity || line.GiftWrap() != giftWrap {
		t.Fatalf("line lost detail: %+v", line)
	}
	if line.LineNo() != 1 {
		t.Fatalf("LineNo() = %d, want 1", line.LineNo())
	}
	if line.ReservationID() != nil {
		t.Fatalf("a fresh line must have no reservation id, got %v", *line.ReservationID())
	}
}

func TestOrderLineReservationIDIsCopied(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}

	got := o.Lines()[0].ReservationID()
	if got == nil || *got != "res-1" {
		t.Fatalf("ReservationID() = %v, want res-1", got)
	}

	// Mutating the returned pointer must not corrupt the aggregate.
	*got = "tampered"
	again := o.Lines()[0].ReservationID()
	if again == nil || *again != "res-1" {
		t.Fatalf("aggregate state was mutated through the returned pointer: %v", again)
	}
}

func TestRehydrateOrderLineSkipsConstructionInvariants(t *testing.T) {
	reservationID := "res-9"
	line := order.RehydrateOrderLine(2, "SKU-2", 4, "singles", true, order.LineAllocated, &reservationID)

	if line.LineNo() != 2 || line.SKU() != "SKU-2" || line.Quantity() != 4 ||
		line.PathID() != "singles" || !line.GiftWrap() || line.Status() != order.LineAllocated {
		t.Fatalf("rehydrated line lost detail: %+v", line)
	}
	if got := line.ReservationID(); got == nil || *got != "res-9" {
		t.Fatalf("ReservationID() = %v, want res-9", got)
	}
}
