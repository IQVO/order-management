package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// shipFixture bundles the fixture adapters ConfirmShipment needs.
type shipFixture struct {
	f         *fixture
	processed *fakeProcessedEvents
}

func newShipFixture() *shipFixture {
	return &shipFixture{f: newFixture(), processed: newFakeProcessedEvents()}
}

func (sf *shipFixture) confirmShipment() *usecases.ConfirmShipment {
	return &usecases.ConfirmShipment{
		Orders: sf.f.orders, Events: sf.f.events, Clock: sf.f.clock, Processed: sf.processed,
	}
}

func (sf *shipFixture) execute(t *testing.T, req usecases.ConfirmShipmentRequest) map[shared.OrderId]usecases.ShipmentOrderOutcome {
	t.Helper()
	outcomes, err := sf.confirmShipment().Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return outcomes
}

func TestConfirmShipment_ShipsReleasedOrderAndRaisesEvents(t *testing.T) {
	sf := newShipFixture()
	o := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick")) // reserves+releases implicitly
	if o.Status() != order.StatusReleased {
		t.Fatalf("precondition status = %q, want Released", o.Status())
	}
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-1", LoadID: "load-77", OrderIDs: []shared.OrderId{o.ID()},
	})
	if outcomes[o.ID()] != usecases.ShipmentOrderShipped {
		t.Fatalf("outcome = %q, want shipped", outcomes[o.ID()])
	}
	stored, err := sf.f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v %v", stored, err)
	}
	if stored.Status() != order.StatusShipped {
		t.Fatalf("status = %q, want Shipped", stored.Status())
	}
	names := sf.f.events.names()
	for _, want := range []string{"OrderLineShipped", "OrderShipped"} {
		if !containsEvent(names, want) {
			t.Fatalf("events = %v, want %s among them", names, want)
		}
	}
}

func TestConfirmShipment_PartialOrderEndsPartiallyShipped(t *testing.T) {
	sf := newShipFixture()
	// allowPartialShipment with SKU-2 backordered: line 1 allocates and
	// releases, line 2 stays Backordered (backorderedFixture's shape).
	f := sf.f
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o := f.mustReceive(t, true, line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"))
	if o.Status() != order.StatusPartiallyReleased {
		t.Fatalf("precondition status = %q, want PartiallyReleased", o.Status())
	}
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-2", LoadID: "load-78", OrderIDs: []shared.OrderId{o.ID()},
	})
	if outcomes[o.ID()] != usecases.ShipmentOrderShipped {
		t.Fatalf("outcome = %q, want shipped", outcomes[o.ID()])
	}
	stored, _ := f.orders.FindByID(context.Background(), o.ID())
	if stored.Status() != order.StatusPartiallyShipped {
		t.Fatalf("status = %q, want PartiallyShipped", stored.Status())
	}
	if !containsEvent(f.events.names(), "OrderPartiallyShipped") {
		t.Fatalf("events = %v, want OrderPartiallyShipped", f.events.names())
	}
}

func TestConfirmShipment_RedeliveryUnderSameIdChangesNothing(t *testing.T) {
	sf := newShipFixture()
	o := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	req := usecases.ConfirmShipmentRequest{EventID: "evt-ship-3", LoadID: "load-79", OrderIDs: []shared.OrderId{o.ID()}}
	first := sf.execute(t, req)
	if first[o.ID()] != usecases.ShipmentOrderShipped {
		t.Fatalf("first outcome = %q, want shipped", first[o.ID()])
	}
	eventsAfterFirst := len(sf.f.events.names())
	second := sf.execute(t, req)
	if second[o.ID()] != usecases.ShipmentOrderDuplicate {
		t.Fatalf("second outcome = %q, want duplicate", second[o.ID()])
	}
	if got := len(sf.f.events.names()); got != eventsAfterFirst {
		t.Fatalf("re-delivery published %d more events, want 0", got-eventsAfterFirst)
	}
}

func TestConfirmShipment_AlreadyShippedOrderUnderNewIdIsNoChange(t *testing.T) {
	sf := newShipFixture()
	o := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	sf.execute(t, usecases.ConfirmShipmentRequest{EventID: "evt-a", LoadID: "load-1", OrderIDs: []shared.OrderId{o.ID()}})
	eventsAfterFirst := len(sf.f.events.names())
	second := sf.execute(t, usecases.ConfirmShipmentRequest{EventID: "evt-b", LoadID: "load-2", OrderIDs: []shared.OrderId{o.ID()}})
	if second[o.ID()] != usecases.ShipmentOrderNoChange {
		t.Fatalf("outcome = %q, want no_change", second[o.ID()])
	}
	if got := len(sf.f.events.names()); got != eventsAfterFirst {
		t.Fatalf("second load's confirmation published %d more events, want 0", got-eventsAfterFirst)
	}
}

func TestConfirmShipment_UnknownOrderIsFailSoft(t *testing.T) {
	sf := newShipFixture()
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-4", LoadID: "load-80", OrderIDs: []shared.OrderId{"ord-never-heard-of"},
	})
	if outcomes["ord-never-heard-of"] != usecases.ShipmentOrderIgnoredUnknownOrder {
		t.Fatalf("outcome = %q, want ignored_unknown_order", outcomes["ord-never-heard-of"])
	}
	if len(sf.f.events.names()) != 0 {
		t.Fatalf("events = %v, want none", sf.f.events.names())
	}
}

func TestConfirmShipment_NotShippableOrderIsFailSoft(t *testing.T) {
	sf := newShipFixture()
	// Block allocation so the order stays Received/Pending.
	sf.f.inventory.reserveErr = errBoom
	o := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	if o.Status() != order.StatusReceived && o.Status() != order.StatusBackordered {
		t.Fatalf("precondition status = %q, want Received or Backordered", o.Status())
	}
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-5", LoadID: "load-81", OrderIDs: []shared.OrderId{o.ID()},
	})
	if outcomes[o.ID()] != usecases.ShipmentOrderIgnoredNotShippable {
		t.Fatalf("outcome = %q, want ignored_not_shippable", outcomes[o.ID()])
	}
	stored, _ := sf.f.orders.FindByID(context.Background(), o.ID())
	if stored.Status() == order.StatusShipped || stored.Status() == order.StatusPartiallyShipped {
		t.Fatalf("status = %q, a not-shippable order must not transition", stored.Status())
	}
}

func TestConfirmShipment_MixedOrdersShipTheKnownShippableOne(t *testing.T) {
	sf := newShipFixture()
	known := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-6", LoadID: "load-82",
		OrderIDs: []shared.OrderId{"ord-unknown", known.ID()},
	})
	if outcomes["ord-unknown"] != usecases.ShipmentOrderIgnoredUnknownOrder {
		t.Fatalf("unknown outcome = %q", outcomes["ord-unknown"])
	}
	if outcomes[known.ID()] != usecases.ShipmentOrderShipped {
		t.Fatalf("known outcome = %q, want shipped (one bad name must not block the rest)", outcomes[known.ID()])
	}
}

func TestConfirmShipment_EmptyOrderListIsFailSoft(t *testing.T) {
	sf := newShipFixture()
	outcomes := sf.execute(t, usecases.ConfirmShipmentRequest{EventID: "evt-ship-7", LoadID: "load-83"})
	if outcomes[""] != usecases.ShipmentOrderIgnoredNoOrders {
		t.Fatalf("outcome = %q, want ignored_no_orders", outcomes[""])
	}
	if len(sf.processed.calls) != 0 {
		t.Fatalf("gate calls = %v, want none (nothing to claim)", sf.processed.calls)
	}
}

func TestConfirmShipment_MissingEventIdIsInvalid(t *testing.T) {
	sf := newShipFixture()
	if _, err := sf.confirmShipment().Execute(context.Background(), usecases.ConfirmShipmentRequest{LoadID: "load-84"}); !errors.Is(err, usecases.ErrInvalidShipmentConfirmation) {
		t.Fatalf("err = %v, want ErrInvalidShipmentConfirmation", err)
	}
}

func TestConfirmShipment_InfrastructureErrorPropagates(t *testing.T) {
	sf := newShipFixture()
	o := sf.f.mustReceive(t, false, line("SKU-1", 1, "pick"))
	sf.processed.err = errBoom
	_, err := sf.confirmShipment().Execute(context.Background(), usecases.ConfirmShipmentRequest{
		EventID: "evt-ship-8", LoadID: "load-85", OrderIDs: []shared.OrderId{o.ID()},
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the infrastructure failure", err)
	}
}

func containsEvent(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}
