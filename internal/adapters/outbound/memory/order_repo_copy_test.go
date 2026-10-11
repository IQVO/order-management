package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// The in-memory OrderRepo used to store and return the caller's own *Order:
// the map lock guarded the map, not the aggregate, so a Kafka consumer's
// use case wrote an order (SetPromiseGroups) while another goroutine read
// the same instance -- a data race `go test -race` found in the repromise
// consumer integration suite. These tests pin the Postgres-equivalent
// contract: Save and FindByID exchange independent copies.

func savedOrder(t *testing.T, repo *memory.OrderRepo) *order.Order {
	t.Helper()
	res := "res-1"
	shipBy := time.Date(2026, 10, 12, 18, 0, 0, 0, time.UTC)
	o := order.Rehydrate(order.OrderSnapshot{
		ID:                   "ord-copy",
		Lines:                []*order.OrderLine{order.RehydrateOrderLine(1, "SKU-1", 2, shared.DefaultPathId, true, order.LineAllocated, &res)},
		AllowPartialShipment: true,
		HeldAtIntake:         true,
		RequiredShipBy:       &shipBy,
		Version:              3,
	})
	o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: order.Promise{CptId: "cpt-1", CutoffAt: shipBy, Basis: order.BasisCapability}}})
	if err := repo.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return o
}

func TestOrderRepoFindByIDReturnsAnIndependentCopy(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewOrderRepo()
	saved := savedOrder(t, repo)

	got, err := repo.FindByID(ctx, "ord-copy")
	if err != nil || got == nil {
		t.Fatalf("FindByID: %v, %v", got, err)
	}
	if got == saved {
		t.Fatal("FindByID returned the very instance passed to Save")
	}
	again, _ := repo.FindByID(ctx, "ord-copy")
	if again == got {
		t.Fatal("two FindByID calls returned the same instance")
	}

	// Every persisted field survives the copy.
	if got.Version() != 3 || !got.AllowPartialShipment() || got.ReleaseOnAllocation() {
		t.Fatalf("scalar fields lost: version=%d partial=%v releaseOnAllocation=%v", got.Version(), got.AllowPartialShipment(), got.ReleaseOnAllocation())
	}
	if rsb := got.RequiredShipBy(); rsb == nil || !rsb.Equal(*saved.RequiredShipBy()) {
		t.Fatalf("RequiredShipBy = %v", rsb)
	}
	if pd := got.PromiseDate(); pd == nil || got.PromiseBasis() == nil || got.PromiseCptId() == nil || *got.PromiseCptId() != "cpt-1" {
		t.Fatalf("promise fields lost: date=%v basis=%v cpt=%v", pd, got.PromiseBasis(), got.PromiseCptId())
	}
	if gs := got.PromiseGroups(); len(gs) != 1 || gs[0].Promise.CptId != "cpt-1" {
		t.Fatalf("PromiseGroups = %+v", gs)
	}
	l := got.Lines()[0]
	if l.SKU() != "SKU-1" || l.Quantity() != 2 || !l.GiftWrap() || l.Status() != order.LineAllocated || *l.ReservationID() != "res-1" {
		t.Fatalf("line fields lost: %+v", l)
	}

	// A mutation of a read copy is invisible until it is saved...
	if err := got.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if fresh, _ := repo.FindByID(ctx, "ord-copy"); fresh.Lines()[0].Status() != order.LineAllocated {
		t.Fatal("an unsaved mutation leaked into the repository")
	}
	// ...and a later mutation of the saved instance never reaches it either.
	if err := saved.Release(1); err != nil {
		t.Fatalf("Release saved: %v", err)
	}
	if fresh, _ := repo.FindByID(ctx, "ord-copy"); fresh.Lines()[0].Status() != order.LineAllocated {
		t.Fatal("mutating the instance passed to Save changed the stored order")
	}
	// Saving makes it durable.
	if err := repo.Save(ctx, got); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if fresh, _ := repo.FindByID(ctx, "ord-copy"); fresh.Lines()[0].Status() != order.LineReleased {
		t.Fatal("a saved mutation was not stored")
	}
}

// Run with -race: a writer that reads, mutates and saves while readers poll
// the same order must not share memory (it did before cloneOrder).
func TestOrderRepoConcurrentReadWriteIsRaceFree(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewOrderRepo()
	savedOrder(t, repo)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if o, _ := repo.FindByID(ctx, "ord-copy"); o != nil {
					_ = o.PromiseGroups()
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		o, _ := repo.FindByID(ctx, "ord-copy")
		o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: order.Promise{CutoffAt: time.Unix(int64(i), 0).UTC(), Basis: order.BasisCapability}}})
		if err := repo.Save(ctx, o); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
