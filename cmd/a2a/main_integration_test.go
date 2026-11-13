//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	inbounda2a "github.com/claudioed/order-management/internal/adapters/inbound/a2a"
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

// TestA2AAgainstPostgres_ReadsWhatTheAPIWroteAndNeverWrites proves the cmd/a2a
// composition root end to end against a real testcontainers Postgres (the
// testcontainers-go/modules/postgres container booted once per test binary by
// internal/testing/pgtest): buildDeps migrates the database itself
// (idempotently — pgtest's databases are already migrated, a second run must
// be a no-op), the documented skills (ADR-0046: order_status, promise_health)
// answer over the real JSON-RPC router exactly what the documented write path
// (ReceiveOrder, the use case behind POST /orders, ADR-0005) persisted, and
// those reads add no outbox row: the A2A write gate allows no write skill
// because the MCP surface offers none (ADR-0010), so there is nothing for a
// relay to publish from this binary.
func TestA2AAgainstPostgres_ReadsWhatTheAPIWroteAndNeverWrites(t *testing.T) {
	ctx := context.Background()
	oltpURL := pgtest.URL(t) // fresh, OLTP-migrated
	analyticsURL := seedAnalytics(t, time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC))

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

	// buildDeps needs the mandatory boot config (loadBootConfig's validated
	// shape) — here pointed at the two fresh databases directly. The
	// migrations path is the documented MIGRATIONS_PATH knob: the binary's
	// "migrations" default assumes the deployment's working directory, while
	// a test binary runs from cmd/a2a.
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	cfg := bootConfig{databaseURL: oltpURL, analyticsURL: analyticsURL, agentURL: "http://placeholder.invalid/"}
	deps, closeDeps, err := buildDeps(ctx, cfg, quietLogger())
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(closeDeps)

	// The card the SERVER serves advertises a placeholder URL (only the
	// client's own card below decides where it dials); the JSON-RPC endpoint
	// itself is the real newRouter the binary serves.
	srv := httptest.NewServer(newRouter(
		inbounda2a.Handler(inbounda2a.NewExecutor(deps)),
		inbounda2a.Card(inbounda2a.Config{AgentURL: "http://placeholder.invalid/", Version: "it"}),
		"order-management-a2a-it",
	))
	t.Cleanup(srv.Close)
	client, err := a2aclient.NewFromCard(ctx, inbounda2a.Card(inbounda2a.Config{AgentURL: srv.URL + "/", Version: "it"}))
	if err != nil {
		t.Fatal(err)
	}

	// order_status answers the seeded order (ADR-0046's skill table).
	env, raw := skillCall(t, client, "order_status", map[string]any{"orderId": seededID})
	data, _ := env["data"].(map[string]any)
	o, _ := data["orderStatus"].(map[string]any)
	if env["problem"] != nil || o["id"] != seededID || o["status"] != "Released" || o["allowPartialShipment"] != true {
		t.Fatalf("order_status = %s", raw)
	}
	if pd, _ := o["promiseDate"].(string); pd == "" {
		t.Fatalf("order_status promiseDate = %v, want the LeadTimePolicy promise the write path set", o["promiseDate"])
	}
	lines, _ := o["lines"].([]any)
	if len(lines) != 2 {
		t.Fatalf("order_status lines = %v", lines)
	}
	line1, _ := lines[0].(map[string]any)
	line2, _ := lines[1].(map[string]any)
	if line1["sku"] != "SKU-100" || line1["quantity"] != 2.0 || line1["pathId"] != "pick" || line1["giftWrap"] != false {
		t.Fatalf("order_status line 1 = %v", line1)
	}
	if line2["sku"] != "SKU-200" || line2["quantity"] != 1.0 || line2["pathId"] != "pack" || line2["giftWrap"] != true {
		t.Fatalf("order_status line 2 = %v", line2)
	}
	for i, line := range []map[string]any{line1, line2} {
		if res, _ := line["reservationId"].(string); res == "" {
			t.Fatalf("order_status line %d reservationId = %v, want the id the write path's reservation kept", i+1, line["reservationId"])
		}
	}

	// An unknown id is answered as the 404 order-not-found problem the REST
	// adapter's callers see (ADR-0046: same classification and problem types).
	env, raw = skillCall(t, client, "order_status", map[string]any{"orderId": seededID + "ghost"})
	assertProblem(t, env, raw, 404, "order-not-found")

	// The write gate: a guessed write verb is an unknown-skill problem, never
	// a mutation (ADR-0046).
	env, raw = skillCall(t, client, "place_order", map[string]any{"orderId": seededID})
	assertProblem(t, env, raw, 400, "unknown-skill")

	// promise_health answers the seeded analytical window (ADR-0046 /
	// ADR-0019): funnel_rollup 3 capability + 5 lead-time + 2 network, 4
	// split-shipment orders of 10 allocated, repromise_rollup 6 re-promises,
	// and a 25200s gap sum over 4 samples (mean 6300s).
	env, raw = skillCall(t, client, "promise_health", map[string]any{"from": "2026-10-09T00:00:00Z", "to": "2026-10-10T00:00:00Z"})
	health, _ := env["data"].(map[string]any)["promiseHealth"].(map[string]any)
	if env["problem"] != nil {
		t.Fatalf("promise_health = %s", raw)
	}
	if health["promiseBasisCapability"] != 3.0 || health["promiseBasisLeadTime"] != 5.0 || health["promiseBasisNetwork"] != 2.0 || health["ordersAllocatedTotal"] != 10.0 {
		t.Fatalf("promise_health basis = %v", health)
	}
	if health["ordersSplitShipment"] != 4.0 || health["ordersRepromised"] != 6.0 {
		t.Fatalf("promise_health counts = %v", health)
	}
	if math.Abs(health["splitShipmentRate"].(float64)-0.4) > 1e-9 || math.Abs(health["repromiseRate"].(float64)-0.6) > 1e-9 {
		t.Fatalf("promise_health rates = %v", health)
	}
	if math.Abs(health["promiseToCutoffGapSeconds"].(float64)-6300.0) > 1e-9 {
		t.Fatalf("promise_health gap = %v", health)
	}

	if after := countOutbox(); after != before {
		t.Fatalf("outbox rows %d -> %d: the read-only A2A binary wrote an event", before, after)
	}
}

// skillCall sends one skill request the way the Agent Card's examples document
// it (a Data part carrying {"skill","arguments"}) and returns the decoded
// envelope plus everything the agent put on the wire.
func skillCall(t *testing.T, client *a2aclient.Client, skill string, args map[string]any) (map[string]any, string) {
	t.Helper()
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"skill": skill, "arguments": args}))
	resp, err := client.SendMessage(context.Background(), &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("%s: %v", skill, err)
	}
	reply, ok := resp.(*a2a.Message)
	if !ok || len(reply.Parts) < 2 {
		t.Fatalf("%s: reply = %T", skill, resp)
	}
	data, ok := reply.Parts[0].Content.(a2a.Data)
	if !ok {
		t.Fatalf("%s: first part is %T", skill, reply.Parts[0].Content)
	}
	if _, ok := reply.Parts[1].Content.(a2a.Text); !ok {
		t.Fatalf("%s: second part is %T, want the one-line summary text", skill, reply.Parts[1].Content)
	}
	raw, _ := json.Marshal(data.Value)
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env, string(raw)
}

// assertProblem pins one rejected skill answer: an RFC 7807-shaped problem
// with the given status and type slug (namespaced under
// https://errors.order-management.warehouse-systems.dev/, the same base the
// REST adapter uses).
func assertProblem(t *testing.T, env map[string]any, raw string, status int, slug string) {
	t.Helper()
	problem, _ := env["problem"].(map[string]any)
	if problem == nil {
		t.Fatalf("expected a problem envelope, got %s", raw)
	}
	if problem["status"] != float64(status) || !strings.HasSuffix(problem["type"].(string), slug) {
		t.Fatalf("problem = %v, want status %d type *%s", problem, status, slug)
	}
	if env["data"] != nil {
		t.Fatalf("a rejected skill must carry no data: %s", raw)
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

// seedAnalytics migrates a fresh EMPTY database with the analytics migration
// set (its golang-migrate numbering is independent of the OLTP set) and writes
// one funnel_rollup row plus one repromise_rollup row — the exact shape
// cmd/order-projector UPSERTs as analytics events arrive, with every counter
// distinct and non-zero so an aggregation bug cannot hide behind equal
// operands. It returns the DSN.
func seedAnalytics(t *testing.T, hour time.Time) string {
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
		VALUES ('pick', $1, 3, 5, 2, 4, 25200.0, 4)`, hour); err != nil {
		t.Fatalf("seed funnel_rollup: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO repromise_rollup (hour_bucket, orders_repromised) VALUES ($1, 6)`, hour); err != nil {
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

func analyticsMigrationsDirForTest(t *testing.T) string {
	t.Helper()
	dir := "../../migrations/analytics"
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("analytics migrations directory not found at %s: %v", dir, err)
	}
	return dir
}
