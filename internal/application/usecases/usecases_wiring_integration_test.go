//go:build integration

// Package usecases_test proves order-management's main write use cases
// against a REAL Postgres (testcontainers): the real OrderRepo, the real
// UnitOfWork, and a buffering event publisher, wired exactly like the
// composition root in cmd/order. These are integration tests in the
// fleet's sense: they execute the real cross-component contracts (the
// receive -> allocate -> release lifecycle over the three-table Order
// aggregate, the Backordered -> RetryAllocate recovery, held-intake
// allocation followed by CancelOrder's reservation revocation, and the
// Save-inside-UoW optimistic-version bracket) against real
// infrastructure, with no in-memory repo fakes anywhere in the path.
//
// The package boots its own throwaway Postgres via testcontainers in
// TestMain: one container for the whole package, migrated once into a
// TEMPLATE database, one private database per test. Never an external
// DATABASE_URL, never t.Skip.
//
// The scripted InventoryReservationClient and the buffering publisher are
// the package's own fakes_test.go fixtures (same usecases_test package):
// inventory-storage is a cross-context Supplier whose own contract is
// exercised in ITS repo; what these tests prove is everything on THIS
// side of that boundary persisting and publishing correctly.
package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// One Postgres container serves the whole package (containers are slow to
// boot). TestMain starts it, applies every migration ONCE into a template
// database, and each test then gets its own database cloned from that
// template (CREATE DATABASE ... TEMPLATE, a file-level copy: milliseconds).
// Isolation is therefore total — no TRUNCATE bookkeeping, no dependence on
// test order, and tests that assert on global state still start pristine.
//
// Never an external DATABASE_URL, never t.Skip.
const wiringTemplateDB = "usecases_migrated_template"

var (
	wiringBaseURL string // connection URL of the container's default database
	wiringDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(wiringRunTests(m))
}

func wiringRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_usecases"),
		tcpostgres.WithUsername("order_management"),
		tcpostgres.WithPassword("order_management"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	wiringBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	// Migrate a template database once; every test clones it.
	if err := wiringCreateDatabase(ctx, wiringTemplateDB); err != nil {
		fmt.Fprintf(os.Stderr, "create template database: %v\n", err)
		return 1
	}
	migrations, err := filepath.Abs(filepath.Join("..", "..", "..", "migrations"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve migrations dir: %v\n", err)
		return 1
	}
	if err := postgres.RunMigrations(wiringWithDB(wiringBaseURL, wiringTemplateDB), migrations); err != nil {
		fmt.Fprintf(os.Stderr, "migrate template: %v\n", err)
		return 1
	}

	return m.Run()
}

// wiringWithDB rewrites the path of a connection URL to the named database.
func wiringWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// wiringCreateDatabase creates an empty database inside the shared container.
func wiringCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, wiringBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// wiringMigratedDB hands the test a connection URL to its own private
// database, cloned from the migrated template. Cloning is a file-level
// copy, so it costs milliseconds and the test's writes never leak into
// another test.
func wiringMigratedDB(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("usecases_it_%d", wiringDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), wiringBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, wiringTemplateDB)); err != nil {
		t.Fatalf("clone database: %v", err)
	}
	return wiringWithDB(wiringBaseURL, name)
}

// wiringClock is the ports.Clock the use cases already accept; a
// deterministic timestamp keeps the published events comparable.
type wiringClock struct{ now time.Time }

func (c wiringClock) Now() time.Time { return c.now }

// wiredStack is the real adapter stack over a private migrated database,
// wired exactly like cmd/order's composition root for the order lifecycle:
// the real Postgres OrderRepo, the real UnitOfWork, a lead-time promise
// policy, and the package fixtures' scripted inventory + buffering
// publisher the tests assert on. No in-memory repo fakes.
type wiredStack struct {
	orders    *postgres.OrderRepo
	receive   *usecases.ReceiveOrder
	retry     *usecases.RetryAllocation
	cancel    *usecases.CancelOrder
	get       *usecases.GetOrder
	inventory *fakeInventory
	publisher *recordingPublisher
}

func newWiredStack(t *testing.T) *wiredStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, wiringMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	orders := postgres.NewOrderRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	inventory := newFakeInventory()
	publisher := &recordingPublisher{}
	clock := wiringClock{time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}

	return &wiredStack{
		orders: orders,
		receive: &usecases.ReceiveOrder{
			Orders: orders, Events: publisher, Clock: clock,
			Inventory: inventory, Promise: promise, UnitOfWork: uow,
		},
		retry: &usecases.RetryAllocation{
			Orders: orders, Inventory: inventory, Events: publisher,
			Clock: clock, Promise: promise, UnitOfWork: uow,
		},
		cancel: &usecases.CancelOrder{
			Orders: orders, Inventory: inventory, Events: publisher,
			Clock: clock, UnitOfWork: uow,
		},
		get:       &usecases.GetOrder{Orders: orders},
		inventory: inventory,
		publisher: publisher,
	}
}

// mustReceive drives ReceiveOrder over the real stack and fails the test
// on the first hard error.
func (s *wiredStack) mustReceive(t *testing.T, lines ...usecases.NewLine) *order.Order {
	t.Helper()
	o, err := s.receive.Execute(context.Background(), lines, false)
	if err != nil {
		t.Fatalf("receive order: %v", err)
	}
	return o
}

// assertPersisted re-reads the aggregate through the real repo and checks
// the persisted shape: status, per-line statuses (keyed by line number),
// the promise, and the optimistic-concurrency version the use case's
// Saves advanced.
func assertPersisted(t *testing.T, s *wiredStack, id shared.OrderId, wantStatus order.Status, wantLines map[int]order.LineStatus, minVersion int) {
	t.Helper()
	o, err := s.get.Execute(context.Background(), id)
	if err != nil {
		t.Fatalf("get order %s: %v", id, err)
	}
	if o.Status() != wantStatus {
		t.Fatalf("persisted status = %q, want %q", o.Status(), wantStatus)
	}
	if len(o.Lines()) != len(wantLines) {
		t.Fatalf("persisted lines = %d, want %d", len(o.Lines()), len(wantLines))
	}
	for lineNo, want := range wantLines {
		lines := o.Lines()
		if lineNo < 1 || lineNo > len(lines) {
			t.Fatalf("line %d out of range (order has %d lines)", lineNo, len(lines))
		}
		if got := lines[lineNo-1].Status(); got != want {
			t.Fatalf("persisted line %d status = %q, want %q", lineNo, got, want)
		}
	}
	if o.PromiseDate() == nil {
		t.Fatal("promise date must survive the Postgres round trip")
	}
	if o.Version() < minVersion {
		t.Fatalf("persisted version = %d, want >= %d (Save must bump the stored version)", o.Version(), minVersion)
	}
}

// requireEvents fails unless every wanted event name appears at least the
// wanted number of times in names. A want of 0 documents an event that
// must NOT have fired any additional times beyond earlier assertions.
func requireEvents(t *testing.T, names []string, want map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, n := range names {
		got[n]++
	}
	for name, count := range want {
		if got[name] < count {
			t.Fatalf("expected at least %d %q event(s), got %d (all events: %v)", count, name, got[name], names)
		}
	}
}

// TestUsecases_ReceiveOrderReleasesShipCompleteEndToEnd is the aggregate's
// happy path over real Postgres: intake persists the order and immediately
// allocates and releases every line in the same call (the choreographed
// release of ADR 0005), the promise survives the round trip, and the
// buffering publisher saw the whole fact sequence. Cancelling an order
// that already released is BR6's domain rejection.
func TestUsecases_ReceiveOrderReleasesShipCompleteEndToEnd(t *testing.T) {
	s := newWiredStack(t)
	ctx := context.Background()

	o := s.mustReceive(t,
		usecases.NewLine{SKU: "ITCOV-SKU-1", Quantity: 2, PathID: "pick"},
		usecases.NewLine{SKU: "ITCOV-SKU-2", Quantity: 1, PathID: "pick", GiftWrap: true},
	)
	if o.Status() != order.StatusReleased {
		t.Fatalf("in-memory status = %q, want Released", o.Status())
	}

	assertPersisted(t, s, o.ID(), order.StatusReleased,
		map[int]order.LineStatus{1: order.LineReleased, 2: order.LineReleased}, 2)

	// The real reservation ids inventory-storage handed back are the ones
	// persisted on the lines — CancelOrder's revocation list depends on it.
	for _, l := range o.Lines() {
		if l.ReservationID() == nil || *l.ReservationID() == "" {
			t.Fatalf("line %d persisted without a reservation id", l.LineNo())
		}
	}

	requireEvents(t, s.publisher.names(), map[string]int{
		"OrderReceived":      1,
		"OrderLineAllocated": 2,
		"OrderAllocated":     1,
		"OrderLineReleased":  2,
		"OrderReleased":      1,
	})

	// BR6: a released order can no longer be cancelled.
	if _, err := s.cancel.Execute(ctx, o.ID()); !errors.Is(err, order.ErrOrderAlreadyReleased) {
		t.Fatalf("cancelling a released order must be rejected with ErrOrderAlreadyReleased, got %v", err)
	}

	// A second ReceiveOrder over the same stack is a brand-new aggregate
	// (NextID mints ids in Postgres-backed reality too): both orders
	// coexist in the private database.
	second := s.mustReceive(t, usecases.NewLine{SKU: "ITCOV-SKU-1", Quantity: 1, PathID: "pick"})
	if second.ID() == o.ID() {
		t.Fatal("NextID must mint distinct order ids")
	}
	if _, err := s.get.Execute(ctx, second.ID()); err != nil {
		t.Fatalf("get second order: %v", err)
	}
}

// TestUsecases_BackorderThenRetryAllocationReleases exercises the
// aggregate's recovery lifecycle: inventory-storage's 409 (a business
// fact) leaves a ship-complete order Backordered with nothing released,
// and a later RetryAllocation — the only route out of Backordered —
// allocates the line, clears BR3, and releases the whole order.
func TestUsecases_BackorderThenRetryAllocationReleases(t *testing.T) {
	s := newWiredStack(t)
	ctx := context.Background()
	s.inventory.mu.Lock()
	s.inventory.reserveErrBySKU["ITCOV-OUT"] = ports.ErrInsufficientStock
	s.inventory.mu.Unlock()

	o := s.mustReceive(t,
		usecases.NewLine{SKU: "ITCOV-SKU-1", Quantity: 1, PathID: "pick"},
		usecases.NewLine{SKU: "ITCOV-OUT", Quantity: 1, PathID: "pick"},
	)
	if o.Status() != order.StatusBackordered {
		t.Fatalf("in-memory status = %q, want Backordered", o.Status())
	}
	assertPersisted(t, s, o.ID(), order.StatusBackordered,
		map[int]order.LineStatus{1: order.LineAllocated, 2: order.LineBackordered}, 2)
	requireEvents(t, s.publisher.names(), map[string]int{
		"OrderReceived":        1,
		"OrderLineAllocated":   1,
		"OrderLineBackordered": 1,
	})

	// Stock arrives: the retry allocates the backordered line and the
	// whole ship-complete order releases in the same pass.
	s.inventory.mu.Lock()
	delete(s.inventory.reserveErrBySKU, "ITCOV-OUT")
	s.inventory.mu.Unlock()

	if _, err := s.retry.Execute(ctx, o.ID()); err != nil {
		t.Fatalf("retry allocation: %v", err)
	}
	assertPersisted(t, s, o.ID(), order.StatusReleased,
		map[int]order.LineStatus{1: order.LineReleased, 2: order.LineReleased}, 3)
	requireEvents(t, s.publisher.names(), map[string]int{
		"OrderLineAllocated": 2, // line 1 at intake + line 2 on retry
		"OrderAllocated":     1,
		"OrderLineReleased":  2,
		"OrderReleased":      1,
	})

	// Nothing left to retry: the recovery action has no more work.
	if _, err := s.retry.Execute(ctx, o.ID()); !errors.Is(err, usecases.ErrNoBackorderedLines) {
		t.Fatalf("retry with no backordered lines must be rejected with ErrNoBackorderedLines, got %v", err)
	}
}

// TestUsecases_HeldOrderAllocatesThenCancelRevokes covers held intake
// (ADR 0020: held orders allocate but do not release) and cancellation:
// CancelOrder revokes every allocated reservation on inventory-storage,
// persists Cancelled, and publishes OrderCancelled — the persisted read
// model agrees.
func TestUsecases_HeldOrderAllocatesThenCancelRevokes(t *testing.T) {
	s := newWiredStack(t)
	ctx := context.Background()

	o, err := s.receive.ExecuteHeld(ctx, []usecases.NewLine{
		{SKU: "ITCOV-SKU-1", Quantity: 2, PathID: "pick"},
	}, false, false)
	if err != nil {
		t.Fatalf("receive held order: %v", err)
	}
	if o.Status() != order.StatusAllocated {
		t.Fatalf("held order status = %q, want Allocated (allocated but not released)", o.Status())
	}
	assertPersisted(t, s, o.ID(), order.StatusAllocated,
		map[int]order.LineStatus{1: order.LineAllocated}, 2)
	requireEvents(t, s.publisher.names(), map[string]int{
		"OrderReceived":      1,
		"OrderLineAllocated": 1,
		"OrderReleased":      0,
	})

	cancelled, err := s.cancel.Execute(ctx, o.ID())
	if err != nil {
		t.Fatalf("cancel order: %v", err)
	}
	if cancelled.Status() != order.StatusCancelled {
		t.Fatalf("cancelled status = %q, want Cancelled", cancelled.Status())
	}
	assertPersisted(t, s, o.ID(), order.StatusCancelled,
		map[int]order.LineStatus{1: order.LineCancelled}, 3)

	// Every allocated reservation really went back to inventory-storage.
	s.inventory.mu.Lock()
	revokes := len(s.inventory.revokeCalls)
	s.inventory.mu.Unlock()
	if revokes != 1 {
		t.Fatalf("expected 1 reservation revoked on inventory-storage, got %d", revokes)
	}
	requireEvents(t, s.publisher.names(), map[string]int{
		"OrderCancelled": 1,
	})
}
