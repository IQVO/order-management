//go:build integration

// Integration tests for OrderRepo's optimistic-concurrency-control
// version guard (see docs/docs/adr/0024-optimistic-concurrency-version-column.md)
// against a real Postgres 16, gated behind the `integration` build tag.
// Testcontainers-only: each test boots and owns its own disposable
// Postgres container, never reads DATABASE_URL or hardcodes localhost.
package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// versionDB boots a throwaway Postgres and runs every migration in this
// repo, including the orders.version column one (0009).
func versionDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_management"),
		tcpostgres.WithUsername("order_management"),
		tcpostgres.WithPassword("order_management"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := postgres.RunMigrations(url, "../../../../migrations"); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func mustSingleLineOrder(t *testing.T, id shared.OrderId) *order.Order {
	t.Helper()
	l, err := order.NewOrderLine(1, shared.SKU("SKU-VER-1"), 1, shared.DefaultPathId, false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	o, err := order.New(id, []*order.OrderLine{l}, true)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	return o
}

func readVersion(t *testing.T, pool *pgxpool.Pool, id shared.OrderId) int {
	t.Helper()
	var v int
	if err := pool.QueryRow(context.Background(), "SELECT version FROM orders WHERE id = $1", id.String()).Scan(&v); err != nil {
		t.Fatalf("read version: %v", err)
	}
	return v
}

// TestOrderRepo_Save_VersionGuard_FreshInsertStartsAtVersionOne covers
// the brand-new-row path: Save's version-guarded UPDATE affects 0 rows
// (nothing exists yet), the existence check confirms that, and the
// fallback INSERT stores o.Version() (1, from order.New) verbatim.
func TestOrderRepo_Save_VersionGuard_FreshInsertStartsAtVersionOne(t *testing.T) {
	pool := versionDB(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	id := shared.OrderId("ord-ver-fresh-" + time.Now().Format("150405.000000000"))
	o := mustSingleLineOrder(t, id)
	if o.Version() != 1 {
		t.Fatalf("New() order Version() = %d, want 1", o.Version())
	}

	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := readVersion(t, pool, id); got != 1 {
		t.Fatalf("stored version after fresh insert = %d, want 1", got)
	}
	if o.Version() != 1 {
		t.Fatalf("in-memory Version() after fresh-insert Save = %d, want unchanged 1", o.Version())
	}
}

// TestOrderRepo_Save_VersionGuard_CurrentVersionSucceedsAndIncrements
// covers the common case: a Save against the version the aggregate was
// actually loaded/last-saved at succeeds, bumps the stored version by
// exactly one, and the in-memory aggregate reflects the new version so
// a THIRD Save in the same process chains correctly.
func TestOrderRepo_Save_VersionGuard_CurrentVersionSucceedsAndIncrements(t *testing.T) {
	pool := versionDB(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	id := shared.OrderId("ord-ver-ok-" + time.Now().Format("150405.000000000"))
	o := mustSingleLineOrder(t, id)
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (insert): %v", err)
	}

	if err := o.Allocate(1, "res-ver-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (update at version 1): %v", err)
	}
	if got := readVersion(t, pool, id); got != 2 {
		t.Fatalf("stored version after first update = %d, want 2", got)
	}
	if o.Version() != 2 {
		t.Fatalf("in-memory Version() after first update = %d, want 2", o.Version())
	}

	if err := o.Release(1); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (update at version 2): %v", err)
	}
	if got := readVersion(t, pool, id); got != 3 {
		t.Fatalf("stored version after second update = %d, want 3", got)
	}
}

// TestOrderRepo_Save_VersionGuard_StaleVersionFailsAndLeavesLinesUntouched
// is the core correctness claim: a Save whose in-memory aggregate was
// loaded at a version another writer has already advanced past fails
// with ports.ErrConcurrentModification, and — because the version check
// runs BEFORE order_lines is touched at all — the line row a concurrent
// writer already wrote is completely unaffected by the failed Save's
// own (different) line mutation.
func TestOrderRepo_Save_VersionGuard_StaleVersionFailsAndLeavesLinesUntouched(t *testing.T) {
	pool := versionDB(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	id := shared.OrderId("ord-ver-stale-" + time.Now().Format("150405.000000000"))
	seed := mustSingleLineOrder(t, id)
	if err := repo.Save(ctx, seed); err != nil {
		t.Fatalf("Save (insert): %v", err)
	}

	// Two independent readers load the SAME row at version 1.
	winner, err := repo.FindByID(ctx, id)
	if err != nil || winner == nil {
		t.Fatalf("FindByID (winner): %v, %v", winner, err)
	}
	loser, err := repo.FindByID(ctx, id)
	if err != nil || loser == nil {
		t.Fatalf("FindByID (loser): %v, %v", loser, err)
	}
	if winner.Version() != 1 || loser.Version() != 1 {
		t.Fatalf("both readers must load version 1, got winner=%d loser=%d", winner.Version(), loser.Version())
	}

	// The winner allocates and saves first, advancing the row to
	// version 2.
	if err := winner.Allocate(1, "res-winner"); err != nil {
		t.Fatalf("winner Allocate: %v", err)
	}
	if err := repo.Save(ctx, winner); err != nil {
		t.Fatalf("winner Save: %v", err)
	}

	// The loser, still holding version 1, attempts its own (different)
	// mutation and Save — this must fail, and must NOT touch
	// order_lines, so the winner's reservation_id survives untouched.
	if err := loser.Allocate(1, "res-loser"); err != nil {
		t.Fatalf("loser Allocate (in-memory, legal — the domain has no version awareness): %v", err)
	}
	err = repo.Save(ctx, loser)
	if !errors.Is(err, ports.ErrConcurrentModification) {
		t.Fatalf("loser Save error = %v, want ports.ErrConcurrentModification", err)
	}

	reloaded, err := repo.FindByID(ctx, id)
	if err != nil || reloaded == nil {
		t.Fatalf("FindByID (reloaded): %v, %v", reloaded, err)
	}
	if got := reloaded.Version(); got != 2 {
		t.Fatalf("stored version after the failed Save = %d, want 2 (unchanged by the loser)", got)
	}
	line := reloaded.Lines()[0]
	if r := line.ReservationID(); r == nil || *r != "res-winner" {
		t.Fatalf("line reservation_id = %v, want \"res-winner\" (the loser's failed Save must never have touched order_lines)", r)
	}
}

// TestOrderRepo_Save_ConcurrentGoroutines_RaceOnSameOrder is the real
// concurrency proof this ADR requires: two goroutines load the SAME
// order via the real RetryAllocation and ReleaseHeldOrder use cases and
// race to Save it. Exactly one must succeed; the other must observe
// ports.ErrConcurrentModification — never both succeeding (a silent
// lost update) and never both failing.
func TestOrderRepo_Save_ConcurrentGoroutines_RaceOnSameOrder(t *testing.T) {
	pool := versionDB(t)
	orders := postgres.NewOrderRepo(pool)
	ctx := context.Background()
	clock := fixedClockAt(time.Now().UTC())
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}

	id := shared.OrderId("ord-ver-race-" + time.Now().Format("150405.000000000"))
	// Seed a HELD order with one Backordered line: RetryAllocation
	// (clears the backorder) and ReleaseHeldOrder (releases already-
	// allocated lines) are two DIFFERENT real use cases that can each
	// legitimately act on the same order without one being a
	// structural no-op against the other's starting state — mirroring
	// the task brief's "retry-allocation racing release" scenario.
	// Line 1 is pre-allocated by direct domain manipulation (mirroring
	// this package's existing allocatedFixture-style setup elsewhere
	// in this repo) and line 2 is left Pending so RetryAllocation has
	// legitimate work if the order were Backordered — but to keep both
	// use cases racing on the SAME row-level write (not two disjoint
	// lines), this test instead races two calls to the SAME use case
	// (ReleaseHeldOrder) from two goroutines, which is exactly the
	// task brief's alternative-acceptable shape ("or two calls to the
	// same use case").
	seed := mustSingleLineOrder(t, id)
	if err := seed.Allocate(1, "res-race-seed"); err != nil {
		t.Fatalf("seed Allocate: %v", err)
	}
	seed.Hold()
	if err := orders.Save(ctx, seed); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	// ReleaseHeldOrder is idempotent once the release has actually
	// landed (its own doc comment: "succeed without touching
	// anything"), so two calls fired back-to-back through the plain
	// repo race for only a few microseconds — usually not enough for
	// BOTH FindByID calls to land before either Save does, so in
	// practice one goroutine's Execute has already committed by the
	// time the other's FindByID runs, and the second sees the
	// already-released state and takes the no-op success branch
	// instead of ever reaching Save. That is a correct outcome for
	// ReleaseHeldOrder, but it does not exercise the version guard
	// itself.
	//
	// To force the version guard to actually fire, this test wraps the
	// real repo in a rendezvous barrier that holds BOTH goroutines'
	// FindByID calls until both have arrived, so both goroutines are
	// guaranteed to load the SAME row at the SAME version before
	// either one's Save runs — the precise "read, read, write, write"
	// interleaving that is a lost-update race in the absence of a
	// version guard. This barrier does not change what either
	// goroutine does with the data it reads or writes; it only removes
	// the timing luck from whether the race actually happens.
	barrier := &rendezvousOrderRepo{OrderRepo: orders, wg: &sync.WaitGroup{}}
	barrier.wg.Add(2)
	release := &usecases.ReleaseHeldOrder{
		Orders: barrier, Inventory: alwaysAllocatesVersionTest{}, Events: noopPublisher{}, Clock: clock, Promise: promise,
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := release.Execute(ctx, id)
			results[i] = err
		}(i)
	}
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ports.ErrConcurrentModification):
			conflicts++
		default:
			t.Fatalf("unexpected error from concurrent ReleaseHeldOrder: %v", err)
		}
	}

	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected exactly one success and one ErrConcurrentModification, got successes=%d conflicts=%d errs=%v", successes, conflicts, results)
	}

	final, err := orders.FindByID(ctx, id)
	if err != nil || final == nil {
		t.Fatalf("FindByID (final): %v, %v", final, err)
	}
	if final.Version() != 2 {
		t.Fatalf("final stored version = %d, want exactly 2 (exactly one Save committed the release)", final.Version())
	}
	t.Logf("concurrent ReleaseHeldOrder: successes=%d conflicts=%d finalVersion=%d", successes, conflicts, final.Version())
}

// rendezvousOrderRepo wraps a real ports.OrderRepo and forces every
// FindByID call to block until n callers have ALL completed their own
// underlying (real) read, then releases them all together. It exists
// purely to make a lost-update race deterministic in a test: a naive
// "block entry, then close a channel" barrier is NOT sufficient here,
// because closing a channel only makes the blocked goroutine runnable —
// it does not force it to actually run before the goroutine that closed
// the channel continues. In practice that let one goroutine's entire
// read-mutate-Save cycle (including its Save committing) complete
// before the other goroutine's own FindByID call ever ran, so the
// "loser" ended up reading the ALREADY-released state instead of racing
// against it, defeating the test. A sync.WaitGroup barrier — Done()
// after each caller's real read, Wait() before any caller returns —
// guarantees both real reads have actually happened before either
// caller's subsequent Save can start, which is the precise
// "read, read, write, write" interleaving a lost-update race requires.
type rendezvousOrderRepo struct {
	ports.OrderRepo
	wg *sync.WaitGroup
}

func (r *rendezvousOrderRepo) FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	o, err := r.OrderRepo.FindByID(ctx, id)
	r.wg.Done()
	r.wg.Wait()
	return o, err
}

type fixedClockAt time.Time

func (c fixedClockAt) Now() time.Time { return time.Time(c) }

type alwaysAllocatesVersionTest struct{}

func (alwaysAllocatesVersionTest) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	return ports.ReservationResult{ReservationID: "res-stub"}, nil
}
func (alwaysAllocatesVersionTest) RevokeReservation(context.Context, string) error { return nil }

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, shared.DomainEvent) error { return nil }
