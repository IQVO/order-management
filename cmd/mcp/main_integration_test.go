//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	inboundmcp "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
	"github.com/claudioed/order-management/internal/pgtx"
	"github.com/claudioed/order-management/internal/testing/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// TestMCPAgainstPostgres_ReadsWhatTheAPIWroteAndNeverWrites proves the cmd/mcp
// composition root end to end against a real testcontainers Postgres (the
// testcontainers-go/modules/postgres container booted once per test binary by
// internal/testing/pgtest): buildAdapters migrates the database itself
// (idempotently — pgtest's databases are already migrated, a second run must
// be a no-op), the documented read tools (docs/docs/mcp/tools.md: get_order,
// get_promise_health) answer exactly what the documented write path
// (ReceiveOrder, the use case behind POST /orders, ADR-0005) persisted, and
// those reads add no outbox row: this context's MCP surface is read-only by
// design (ADR-0010; the governance charter's write gate), so there is no write
// tool to exercise and the outbox must be untouched.
func TestMCPAgainstPostgres_ReadsWhatTheAPIWroteAndNeverWrites(t *testing.T) {
	ctx := context.Background()
	oltpURL := pgtest.URL(t) // fresh, OLTP-migrated
	analyticsURL := seedAnalytics(t, analyticsFixture{
		hour:       time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC),
		capability: 3, leadTime: 5, network: 2,
		split: 4, repromised: 6, gapSum: 25200.0, gapSamples: 4,
	})

	pool, err := postgres.NewPool(ctx, oltpURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	countOutbox := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
			t.Fatalf("count outbox_events: %v", err)
		}
		return n
	}

	seededID := seedOrder(t, oltpURL)
	before := countOutbox()

	// cmd/mcp reads ANALYTICS_DATABASE_URL itself (buildPromiseHealthStore);
	// the test points it at the seeded analytical database. The unset case is
	// the documented all-zero fallback, not this test's subject.
	t.Setenv("ANALYTICS_DATABASE_URL", analyticsURL)

	orders, closeAdapters, err := buildAdapters(ctx, oltpURL, oltpURL, migrationsDirForTest(t), quietLogger())
	if err != nil {
		t.Fatalf("buildAdapters: %v", err)
	}
	t.Cleanup(closeAdapters)
	promiseHealth, closePromiseHealth := buildPromiseHealthStore(ctx, quietLogger())
	t.Cleanup(closePromiseHealth)

	deps := inboundmcp.Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: promiseHealth,
	}
	ct, st := sdk.NewInMemoryTransports()
	ss, err := inboundmcp.NewServer(deps).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "t", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: err=%v res=%+v", name, err, res)
		}
		return res.StructuredContent.(map[string]any)
	}

	// get_order answers the seeded order (docs/docs/mcp/tools.md §get_order).
	got := call("get_order", map[string]any{"orderId": seededID})
	if got["id"] != seededID || got["allowPartialShipment"] != true || got["status"] != "Released" {
		t.Fatalf("get_order = %v", got)
	}
	if pd, _ := got["promiseDate"].(string); pd == "" {
		t.Fatalf("get_order promiseDate = %v, want the LeadTimePolicy promise the write path set", got["promiseDate"])
	}
	lines, _ := got["lines"].([]any)
	if len(lines) != 2 {
		t.Fatalf("get_order lines = %v", lines)
	}
	line1, _ := lines[0].(map[string]any)
	line2, _ := lines[1].(map[string]any)
	if line1["sku"] != "SKU-100" || line1["quantity"] != 2.0 || line1["pathId"] != "pick" || line1["giftWrap"] != false {
		t.Fatalf("get_order line 1 = %v", line1)
	}
	if line2["sku"] != "SKU-200" || line2["quantity"] != 1.0 || line2["pathId"] != "pack" || line2["giftWrap"] != true {
		t.Fatalf("get_order line 2 = %v", line2)
	}
	for i, line := range []map[string]any{line1, line2} {
		if res, _ := line["reservationId"].(string); res == "" {
			t.Fatalf("get_order line %d reservationId = %v, want the id the write path's reservation kept", i+1, line["reservationId"])
		}
	}

	// An unknown id is a tool-level error ("order not found"), per the docs.
	res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "get_order", Arguments: map[string]any{"orderId": seededID + "ghost"}})
	if err != nil {
		t.Fatalf("get_order unknown id: transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("get_order unknown id: expected a tool-level error, got %+v", res)
	}

	// get_promise_health answers the seeded analytical window
	// (docs/docs/mcp/tools.md §get_promise_health, ADR-0019).
	window := map[string]any{"from": "2026-10-09T00:00:00Z", "to": "2026-10-10T00:00:00Z"}
	health := call("get_promise_health", window)
	assertPromiseHealth(t, health, 3, 5, 2, 10, 4, 0.4, 6, 0.6, 6300.0)

	// The optional exact-match pathId filter is omitempty-optional (docs) and,
	// documented on the tool itself, zeroes the path-less re-promise count.
	filtered := call("get_promise_health", map[string]any{
		"from": window["from"], "to": window["to"], "pathId": "pick",
	})
	assertPromiseHealth(t, filtered, 3, 5, 2, 10, 4, 0.4, 0, 0.0, 6300.0)

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the read-only MCP binary wrote an event", before, after)
	}
}

// assertPromiseHealth pins one get_promise_health answer: the promise-basis
// distribution, its total, the split-shipment and re-promise counts and rates,
// and the sample-weighted promise-to-cutoff gap. Rates use an epsilon: they
// are divisions of the seeded counts (e.g. 4/10), not exact binary fractions.
func assertPromiseHealth(t *testing.T, got map[string]any, capability, leadTime, network, total, split int, splitRate float64, repromised int, repromiseRate, gap float64) {
	t.Helper()
	if got["promiseBasisCapability"] != float64(capability) || got["promiseBasisLeadTime"] != float64(leadTime) || got["promiseBasisNetwork"] != float64(network) {
		t.Fatalf("promise basis = %v/%v/%v, want %d/%d/%d", got["promiseBasisCapability"], got["promiseBasisLeadTime"], got["promiseBasisNetwork"], capability, leadTime, network)
	}
	if got["ordersAllocatedTotal"] != float64(total) {
		t.Fatalf("ordersAllocatedTotal = %v, want %d", got["ordersAllocatedTotal"], total)
	}
	if got["ordersSplitShipment"] != float64(split) || got["ordersRepromised"] != float64(repromised) {
		t.Fatalf("split/repromised = %v/%v, want %d/%d", got["ordersSplitShipment"], got["ordersRepromised"], split, repromised)
	}
	if math.Abs(got["splitShipmentRate"].(float64)-splitRate) > 1e-9 || math.Abs(got["repromiseRate"].(float64)-repromiseRate) > 1e-9 {
		t.Fatalf("rates = %v/%v, want %v/%v", got["splitShipmentRate"], got["repromiseRate"], splitRate, repromiseRate)
	}
	if math.Abs(got["promiseToCutoffGapSeconds"].(float64)-gap) > 1e-9 {
		t.Fatalf("promiseToCutoffGapSeconds = %v, want %v", got["promiseToCutoffGapSeconds"], gap)
	}
}

// --- shared seeding: the documented write path, and the analytics read model ---

// seedOrder drives the REAL ReceiveOrder use case (the one POST /orders
// drives, ADR-0005) over the real Postgres OrderRepo and the real UnitOfWork,
// with the fleet's scripted inventory and a transactional outbox publisher
// (the same shapes internal/application/usecases'
// usecases_wiring_integration_test.go and the REST idempotency integration
// tests use): two lines across two process paths, one gift-wrapped, released
// on allocation. It returns the minted order id.
func seedOrder(t *testing.T, url string) string {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	orders := postgres.NewOrderRepo(pool)
	receive := &usecases.ReceiveOrder{
		Orders:     orders,
		Events:     &txOutbox{pool: pool},
		Clock:      fixedClockAt(time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)),
		Inventory:  &alwaysReservingInventory{},
		Promise:    order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)},
		UnitOfWork: postgres.NewUnitOfWork(pool),
	}
	o, err := receive.Execute(ctx, []usecases.NewLine{
		{SKU: shared.SKU("SKU-100"), Quantity: 2, PathID: shared.PathId("pick")},
		{SKU: shared.SKU("SKU-200"), Quantity: 1, PathID: shared.PathId("pack"), GiftWrap: true},
	}, true)
	if err != nil {
		t.Fatalf("receive order: %v", err)
	}
	return string(o.ID())
}

// analyticsFixture is one hour's worth of the Order Funnel & Allocation Health
// read model (migrations/analytics), with every counter distinct and non-zero
// so an aggregation bug cannot hide behind equal operands.
type analyticsFixture struct {
	hour                                 time.Time
	capability, leadTime, network, split int
	repromised                           int
	gapSum                               float64
	gapSamples                           int
}

// seedAnalytics migrates a fresh EMPTY database with the analytics migration
// set (its golang-migrate numbering is independent of the OLTP set) and writes
// one funnel_rollup row plus one repromise_rollup row, the exact shape
// cmd/order-projector UPSERTs as analytics events arrive. It returns the DSN.
func seedAnalytics(t *testing.T, f analyticsFixture) string {
	t.Helper()
	url := pgtest.EmptyURL(t)
	if err := postgres.RunMigrations(url, analyticsMigrationsDirForTest(t)); err != nil {
		t.Fatalf("analytics migrations: %v", err)
	}
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		INSERT INTO funnel_rollup (path_id, hour_bucket, promise_basis_capability, promise_basis_lead_time,
			promise_basis_network, orders_split_shipment, promise_to_cutoff_gap_seconds_sum, promise_to_cutoff_gap_samples)
		VALUES ('pick', $1, $2, $3, $4, $5, $6, $7)`,
		f.hour, f.capability, f.leadTime, f.network, f.split, f.gapSum, f.gapSamples); err != nil {
		t.Fatalf("seed funnel_rollup: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO repromise_rollup (hour_bucket, orders_repromised) VALUES ($1, $2)`,
		f.hour, f.repromised); err != nil {
		t.Fatalf("seed repromise_rollup: %v", err)
	}
	return url
}

// txOutbox enqueues one outbox_events row per event through the transaction
// bound to ctx, exactly as postgres.OutboxPublisher does for the real Kafka
// encoders — the seeding write path therefore leaves the same trail the OLTP
// binary's POST /orders leaves.
type txOutbox struct{ pool *pgxpool.Pool }

func (p *txOutbox) Publish(ctx context.Context, event shared.DomainEvent) error {
	exec := func(sql string, args ...any) (pgconn.CommandTag, error) { return p.pool.Exec(ctx, sql, args...) }
	if tx, ok := pgtx.TxFrom(ctx); ok {
		exec = func(sql string, args ...any) (pgconn.CommandTag, error) { return tx.Exec(ctx, sql, args...) }
	}
	_, err := exec(`INSERT INTO outbox_events (topic, event_type, value) VALUES ('t', $1, '{}')`, fmt.Sprintf("%T", event))
	return err
}

// alwaysReservingInventory is the scripted Supplier stand-in: every reserve
// succeeds with a distinct reservation id, the happy path the wiring
// integration tests also assume.
type alwaysReservingInventory struct{ next int }

func (f *alwaysReservingInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	f.next++
	return ports.ReservationResult{ReservationID: "res-" + strconv.Itoa(f.next)}, nil
}

func (f *alwaysReservingInventory) RevokeReservation(context.Context, string) error { return nil }

func fixedClockAt(now time.Time) ports.Clock { return fixedClock{now} }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func migrationsDirChecked(t *testing.T, dir string) string {
	t.Helper()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

func migrationsDirForTest(t *testing.T) string {
	t.Helper()
	return migrationsDirChecked(t, "../../migrations")
}

func analyticsMigrationsDirForTest(t *testing.T) string {
	t.Helper()
	return migrationsDirChecked(t, "../../migrations/analytics")
}
