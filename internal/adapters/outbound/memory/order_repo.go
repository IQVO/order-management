// Package memory provides in-memory implementations of the outbound ports,
// used by the unit/httptest suites and by `go run ./cmd/order` with no
// DATABASE_URL set, so the service is fully functional without Postgres.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// OrderRepo is an in-memory implementation of ports.OrderRepo.
type OrderRepo struct {
	mu     sync.RWMutex
	orders map[shared.OrderId]*order.Order
	nextID int
}

// NewOrderRepo constructs an empty OrderRepo.
func NewOrderRepo() *OrderRepo {
	return &OrderRepo{orders: make(map[shared.OrderId]*order.Order)}
}

func (r *OrderRepo) Save(_ context.Context, o *order.Order) error {
	stored := cloneOrder(o)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.orders[o.ID()] = stored
	return nil
}

// FindByID returns (nil, nil) when no order has this id. "Not found" is
// the application's concern (usecases.ErrOrderNotFound), not the
// repository's.
func (r *OrderRepo) FindByID(_ context.Context, id shared.OrderId) (*order.Order, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	o, ok := r.orders[id]
	if !ok {
		return nil, nil
	}
	return cloneOrder(o), nil
}

// cloneOrder deep-copies an aggregate through its public accessors and
// order.Rehydrate -- the same field set the Postgres adapter persists and
// reads back. The repository must never hand out the instance it stores:
// the mutex above guards the map, not the aggregate, so a shared *Order let
// one goroutine's use case (SetPromiseGroups in a Kafka consumer) write
// while another read it -- a data race `go test -race` caught -- and let a
// caller's unsaved mutation leak into every later FindByID. A fresh
// aggregate per Save/FindByID is what Postgres gives, so the in-memory
// service (`cmd/order` without DATABASE_URL) now has the same semantics.
func cloneOrder(o *order.Order) *order.Order {
	src := o.Lines()
	lines := make([]*order.OrderLine, len(src))
	for i, l := range src {
		lines[i] = order.RehydrateOrderLine(l.LineNo(), l.SKU(), l.Quantity(), l.PathID(), l.GiftWrap(), l.Status(), l.ReservationID())
	}
	return order.Rehydrate(order.OrderSnapshot{
		ID:                   o.ID(),
		Lines:                lines,
		AllowPartialShipment: o.AllowPartialShipment(),
		PromiseDate:          o.PromiseDate(),
		PromiseCptID:         o.PromiseCptId(),
		PromiseBasis:         o.PromiseBasis(),
		PromiseGroups:        o.PromiseGroups(),
		HeldAtIntake:         !o.ReleaseOnAllocation(),
		RequiredShipBy:       copyTime(o.RequiredShipBy()),
		FulfillmentSiteID:    o.FulfillmentSiteID(),
		Version:              o.Version(),
	})
}

func copyTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func (r *OrderRepo) NextID(_ context.Context) (shared.OrderId, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return shared.OrderId(fmt.Sprintf("ord-%d", r.nextID)), nil
}
