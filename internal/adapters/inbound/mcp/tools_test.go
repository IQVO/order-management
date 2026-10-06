package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// harness wires the read use case the MCP adapter needs over an in-memory
// repo, seeding orders directly via the domain's own Rehydrate
// constructor (mirroring internal/domain/order's own test suite) rather
// than through ReceiveOrder -- ReceiveOrder needs a real
// InventoryReservationClient and LeadTimePolicy to exercise allocation,
// which is out of scope for this adapter's own tests: what matters here
// is that get_order correctly reads back whatever the repo holds, not
// how an order comes to hold a given state.
type harness struct {
	t *testing.T

	orders  *memory.OrderRepo
	reports *analyticsstore.MemoryStore

	deps Deps
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	orders := memory.NewOrderRepo()
	reports := analyticsstore.NewMemoryStore()

	h := &harness{
		t:       t,
		orders:  orders,
		reports: reports,
	}
	h.deps = Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: reportStoreTestAdapter{reports},
	}
	return h
}

// reportStoreTestAdapter adapts a report.ReportStore (the MemoryStore test
// double) into this package's own PromiseHealthStore port, mirroring
// cmd/mcp's real reportStoreAdapter -- kept here rather than exported from
// the production code so tests exercise the SAME translation shape a real
// deployment uses without the production package depending on
// internal/analytics/report itself (see PromiseHealthStore's doc comment).
type reportStoreTestAdapter struct {
	store *analyticsstore.MemoryStore
}

func (a reportStoreTestAdapter) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{
		From:        from,
		To:          to,
		PathId:      pathId,
		Granularity: report.GranularityHour,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, PromiseHealthRow{
			PathID:                    row.Key.PathId,
			HourBucket:                row.Key.HourBucket,
			PromiseBasisCapability:    row.PromiseBasisCapability,
			PromiseBasisLeadTime:      row.PromiseBasisLeadTime,
			PromiseBasisNetwork:       row.PromiseBasisNetwork,
			OrdersRepromised:          row.OrdersRepromised,
			OrdersSplitShipment:       row.OrdersSplitShipment,
			PromiseToCutoffGapSeconds: row.PromiseToCutoffGapSeconds,
			PromiseToCutoffGapSamples: row.PromiseToCutoffGapSamples,
		})
	}
	return rows, nil
}

func (h *harness) ctx() context.Context { return context.Background() }

// mustSaveOrder rehydrates and saves an Order directly into the repo.
func (h *harness) mustSaveOrder(id string, lines []*order.OrderLine, allowPartialShipment bool) {
	h.t.Helper()
	orderID, err := shared.NewOrderId(id)
	if err != nil {
		h.t.Fatalf("order id %q: %v", id, err)
	}
	o := order.Rehydrate(order.OrderSnapshot{ID: orderID, Lines: lines, AllowPartialShipment: allowPartialShipment})
	if err := h.orders.Save(h.ctx(), o); err != nil {
		h.t.Fatalf("saving order %q: %v", id, err)
	}
}

func TestGetOrder(t *testing.T) {
	t.Run("empty orderId rejected", testGetOrderEmptyOrderIDRejected)
	t.Run("unknown order rejected", testGetOrderUnknownOrderRejected)
	t.Run("order with an allocated and a backordered line", testGetOrderAllocatedAndBackorderedLines)
}

// newGetOrderHarness returns a harness whose repo already holds ORD-1 —
// one Allocated line carrying RES-1 and one Backordered line — the
// read-back fixture every TestGetOrder case starts from, mirroring the
// original table-driven form's setup.
func newGetOrderHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	resID := "RES-1"
	h.mustSaveOrder("ORD-1", []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 2, "pick", false, order.LineAllocated, &resID),
		order.RehydrateOrderLine(2, "SKU-2", 1, "pick", true, order.LineBackordered, nil),
	}, true)
	return h
}

func testGetOrderEmptyOrderIDRejected(t *testing.T) {
	h := newGetOrderHarness(t)

	if _, err := h.deps.getOrder(h.ctx(), getOrderInput{OrderId: ""}); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func testGetOrderUnknownOrderRejected(t *testing.T) {
	h := newGetOrderHarness(t)

	if _, err := h.deps.getOrder(h.ctx(), getOrderInput{OrderId: "ORD-NOPE"}); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func testGetOrderAllocatedAndBackorderedLines(t *testing.T) {
	h := newGetOrderHarness(t)

	out, err := h.deps.getOrder(h.ctx(), getOrderInput{OrderId: "ORD-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.ID != "ORD-1" {
		t.Fatalf("unexpected order id %q", out.ID)
	}
	if out.Status != "PartiallyAllocated" {
		t.Fatalf("unexpected status %q", out.Status)
	}
	if len(out.Lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(out.Lines))
	}
	allocated := out.Lines[0]
	if allocated.Status != "Allocated" || allocated.ReservationID == nil || *allocated.ReservationID != "RES-1" {
		t.Fatalf("unexpected allocated line %+v", allocated)
	}
	backordered := out.Lines[1]
	if backordered.Status != "Backordered" || backordered.ReservationID != nil {
		t.Fatalf("unexpected backordered line %+v", backordered)
	}
}
