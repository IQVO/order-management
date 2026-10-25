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
	site := shared.FulfillmentSiteID("WH2")
	o := order.Rehydrate(order.OrderSnapshot{
		ID:                   "ord-copy",
		Lines:                []*order.OrderLine{order.RehydrateOrderLine(1, "SKU-1", 2, shared.DefaultPathId, true, order.LineAllocated, &res)},
		AllowPartialShipment: true,
		HeldAtIntake:         true,
		RequiredShipBy:       &shipBy,
		FulfillmentSiteID:    &site,
		Version:              3,
	})
	o.SetPromiseGroups([]order.PromiseGroup{{LineNos: []int{1}, Promise: order.Promise{CptId: "cpt-1", CutoffAt: shipBy, Basis: order.BasisCapability}}})
	if err := repo.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	return o
}

func findOrder(t *testing.T, repo *memory.OrderRepo) *order.Order {
	t.Helper()
	got, err := repo.FindByID(context.Background(), "ord-copy")
	if err != nil || got == nil {
		t.Fatalf("FindByID: %v, %v", got, err)
	}
	return got
}

func TestOrderRepoFindByIDReturnsFreshInstances(t *testing.T) {
	repo := memory.NewOrderRepo()
	saved := savedOrder(t, repo)
	got := findOrder(t, repo)
	if got == saved {
		t.Fatal("FindByID returned the very instance passed to Save")
	}
	if findOrder(t, repo) == got {
		t.Fatal("two FindByID calls returned the same instance")
	}
}

// Every field the Postgres adapter persists survives the copy.
func TestOrderRepoCopyKeepsEveryPersistedField(t *testing.T) {
	repo := memory.NewOrderRepo()
	saved := savedOrder(t, repo)
	got := findOrder(t, repo)
	assertOrderScalars(t, got, saved)
	assertOrderPromise(t, got)
	assertOrderLine(t, got.Lines()[0])
}

func assertOrderScalars(t *testing.T, got, saved *order.Order) {
	t.Helper()
	if got.Version() != 3 || !got.AllowPartialShipment() || got.ReleaseOnAllocation() {
		t.Fatalf("scalar fields lost: version=%d partial=%v releaseOnAllocation=%v", got.Version(), got.AllowPartialShipment(), got.ReleaseOnAllocation())
	}
	if rsb := got.RequiredShipBy(); rsb == nil || !rsb.Equal(*saved.RequiredShipBy()) {
		t.Fatalf("RequiredShipBy = %v", rsb)
	}
	if site := got.FulfillmentSiteID(); site == nil || *site != "WH2" {
		t.Fatalf("FulfillmentSiteID = %v, want WH2", site)
	}
}

func assertOrderPromise(t *testing.T, got *order.Order) {
	t.Helper()
	if got.PromiseDate() == nil || got.PromiseBasis() == nil {
		t.Fatalf("promise fields lost: date=%v basis=%v", got.PromiseDate(), got.PromiseBasis())
	}
	if cpt := got.PromiseCptId(); cpt == nil || *cpt != "cpt-1" {
		t.Fatalf("PromiseCptId = %v", cpt)
	}
	if gs := got.PromiseGroups(); len(gs) != 1 || gs[0].Promise.CptId != "cpt-1" {
		t.Fatalf("PromiseGroups = %+v", gs)
	}
}

func assertOrderLine(t *testing.T, l *order.OrderLine) {
	t.Helper()
	if l.SKU() != "SKU-1" || l.Quantity() != 2 || !l.GiftWrap() || l.Status() != order.LineAllocated {
		t.Fatalf("line fields lost: %+v", l)
	}
	if res := l.ReservationID(); res == nil || *res != "res-1" {
		t.Fatalf("ReservationID = %v", res)
	}
}

func assertStoredLineStatus(t *testing.T, repo *memory.OrderRepo, want order.LineStatus, why string) {
	t.Helper()
	if got := findOrder(t, repo).Lines()[0].Status(); got != want {
		t.Fatalf("stored line status = %s, want %s: %s", got, want, why)
	}
}

// Only Save changes what the repository holds: neither a mutated read copy
// nor a later mutation of the instance that was saved leaks into it.
func TestOrderRepoUnsavedMutationsStayPrivate(t *testing.T) {
	repo := memory.NewOrderRepo()
	saved := savedOrder(t, repo)

	got := findOrder(t, repo)
	if err := got.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	assertStoredLineStatus(t, repo, order.LineAllocated, "an unsaved mutation of a read copy leaked")

	if err := saved.Release(1); err != nil {
		t.Fatalf("Release saved: %v", err)
	}
	assertStoredLineStatus(t, repo, order.LineAllocated, "mutating the instance passed to Save changed the stored order")

	if err := repo.Save(context.Background(), got); err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertStoredLineStatus(t, repo, order.LineReleased, "a saved mutation was not stored")
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
