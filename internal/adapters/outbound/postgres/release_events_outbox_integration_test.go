//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
)

// okInventory reserves every line, so ReceiveOrder's folded allocate-then-release
// pass runs all the way to the release transition.
type okInventory struct{ n int }

func (i *okInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	i.n++
	return ports.ReservationResult{ReservationID: "res-" + string(rune('a'+i.n))}, nil
}
func (*okInventory) RevokeReservation(context.Context, string) error { return nil }

// TestOutbox_ReceiveOrder_ReleaseFactsCommitWithTheAggregate proves the release
// facts (OrderLineReleased x N, OrderReleased) are raised at the real release
// transition, land on the ANALYTICS topic only (the integration topic keeps its
// three-event contract), carry the CloudEvents type built from the event-name
// constants, and commit atomically with the aggregate that is persisted Released.
func TestOutbox_ReceiveOrder_ReleaseFactsCommitWithTheAggregate(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	orders := postgres.NewOrderRepo(pool)
	writer := outboundkafka.NewPublisher(nil)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: newSeqID()}
	outbox := postgres.NewOutboxPublisher(pool, writer, analytics)

	uc := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		Inventory: &okInventory{}, UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	o, err := uc.Execute(ctx, []usecases.NewLine{
		{SKU: "sku-1", Quantity: 1, PathID: "pick"},
		{SKU: "sku-2", Quantity: 2, PathID: "pick"},
	}, false)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	const lineType = "com.warehouse.wes.order-management.order.OrderLineReleased"
	const orderType = "com.warehouse.wes.order-management.order.OrderReleased"

	if got := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, lineType); got != 2 {
		t.Fatalf("analytics OrderLineReleased rows = %d, want 2", got)
	}
	if got := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, orderType); got != 1 {
		t.Fatalf("analytics OrderReleased rows = %d, want 1", got)
	}
	if got := countOutboxWhereTopic(t, pool, outboundkafka.Topic, lineType) +
		countOutboxWhereTopic(t, pool, outboundkafka.Topic, orderType); got != 0 {
		t.Fatalf("integration topic rows for release facts = %d, want 0 (outside its contract)", got)
	}

	persisted, err := orders.FindByID(ctx, o.ID())
	if err != nil || persisted == nil {
		t.Fatalf("find order: %v (order=%v)", err, persisted)
	}
	if persisted.Status() != order.StatusReleased {
		t.Fatalf("persisted status = %q, want %q", persisted.Status(), order.StatusReleased)
	}
}

func newSeqID() func() string {
	n := 0
	return func() string {
		n++
		return "evt-" + string(rune('0'+n/10)) + string(rune('0'+n%10))
	}
}
