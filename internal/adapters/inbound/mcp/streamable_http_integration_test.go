//go:build integration

// Integration tests for the MCP inbound adapter over the REAL Streamable
// HTTP transport: mcp.Handler(server) mounted on an httptest.Server,
// driven by the SDK's own client (mcp.NewClient +
// StreamableClientTransport), with the REAL Postgres-backed use cases
// behind it — exactly the deployment shape cmd/mcp serves. This proves
// the wire contract (initialize, tools/list, tools/call) end-to-end over
// both registered tools, not the tool handlers in isolation: get_order
// reads through the real OrderRepo against a seeded order that was
// created by the real ReceiveOrder use case, and get_promise_health
// aggregates KPIs the real analytics projection wrote to a real
// analytics database (cmd/mcp's own two-database shape, OLTP +
// ANALYTICS, mirrored here as two migrated templates on one container).
//
// Postgres comes from testcontainers: one container per package run,
// migrated once into TEMPLATE databases (OLTP + analytics), one private
// clone per test. Never an external DATABASE_URL, never t.Skip.
//
// order-management exposes no write tool over MCP (see tools.go's Deps
// doc comment: every write use case carries business invariants an agent
// must not drive), so this suite proves the read tools plus the two
// error shapes that matter on the wire: a domain rejection (unknown
// order) and invalid input, both of which MUST come back as tool errors
// (res.IsError), never transport errors.
package mcp_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	mcpadapter "github.com/claudioed/order-management/internal/adapters/inbound/mcp"
	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/analytics/report"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// One Postgres container serves the whole package, migrated once into a
// template database per schema (the OLTP orders schema and the analytics
// funnel schema are DIFFERENT migration sets and cannot share a database
// — golang-migrate's schema_migrations would collide); each test gets a
// private clone of each (milliseconds). See
// internal/application/usecases/usecases_wiring_integration_test.go for
// the same pattern's rationale. Never an external DATABASE_URL, never
// t.Skip.
const (
	mcpOLTPTemplateDB      = "mcp_oltp_migrated_template"
	mcpAnalyticsTemplateDB = "mcp_analytics_migrated_template"
)

var (
	mcpBaseURL string
	mcpDBSeq   atomic.Uint64
)

func TestMain(m *testing.M) {
	os.Exit(mcpRunTests(m))
}

func mcpRunTests(m *testing.M) int {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("order_mcp"),
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

	mcpBaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}

	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve repo root: %v\n", err)
		return 1
	}
	for _, tmpl := range []struct{ db, migrations string }{
		{mcpOLTPTemplateDB, filepath.Join(root, "migrations")},
		{mcpAnalyticsTemplateDB, filepath.Join(root, "migrations", "analytics")},
	} {
		if err := mcpCreateDatabase(ctx, tmpl.db); err != nil {
			fmt.Fprintf(os.Stderr, "create template database %s: %v\n", tmpl.db, err)
			return 1
		}
		if err := postgres.RunMigrations(mcpWithDB(mcpBaseURL, tmpl.db), tmpl.migrations); err != nil {
			fmt.Fprintf(os.Stderr, "migrate template %s: %v\n", tmpl.db, err)
			return 1
		}
	}
	return m.Run()
}

// mcpWithDB rewrites the path of a connection URL to the named database.
func mcpWithDB(baseURL, name string) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + name
	return u.String()
}

// mcpCreateDatabase creates an empty database inside the shared container.
func mcpCreateDatabase(ctx context.Context, name string) error {
	conn, err := pgx.Connect(ctx, mcpBaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", name)); err != nil {
		return fmt.Errorf("create database %s: %w", name, err)
	}
	return nil
}

// mcpMigratedDB hands the test a connection URL to its own private
// database cloned from the named migrated template.
func mcpMigratedDB(t *testing.T, template string) string {
	t.Helper()
	name := fmt.Sprintf("mcp_it_%d", mcpDBSeq.Add(1))
	conn, err := pgx.Connect(context.Background(), mcpBaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), fmt.Sprintf(
		"CREATE DATABASE %q WITH TEMPLATE %q", name, template)); err != nil {
		t.Fatalf("clone database from %s: %v", template, err)
	}
	return mcpWithDB(mcpBaseURL, name)
}

// mcpFixedClock is the ports.Clock the use cases accept.
type mcpFixedClock struct{ now time.Time }

func (c mcpFixedClock) Now() time.Time { return c.now }

// mcpScriptedInventory reserves every line cleanly so the seeded order
// reaches Released (the state get_order reports on the wire).
type mcpScriptedInventory struct{ mu sync.Mutex }

func (i *mcpScriptedInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return ports.ReservationResult{ReservationID: "res-mcp-1"}, nil
}

func (i *mcpScriptedInventory) RevokeReservation(context.Context, string) error { return nil }

// mcpBufferingPublisher records the name of every event the seed use
// case publishes.
type mcpBufferingPublisher struct {
	mu     sync.Mutex
	events []string
}

func (p *mcpBufferingPublisher) Publish(_ context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event.EventName())
	return nil
}

// mcpPromiseHealthAdapter adapts the real analytics report store into the
// MCP package's own PromiseHealthStore port — the same translation
// cmd/mcp's reportStoreAdapter performs at the wiring boundary (this
// composition is the one place allowed to see both shapes).
type mcpPromiseHealthAdapter struct{ store report.ReportStore }

func (a mcpPromiseHealthAdapter) QueryPromiseHealth(ctx context.Context, from, to time.Time, pathId string) ([]mcpadapter.PromiseHealthRow, error) {
	rep, err := a.store.Query(ctx, report.ReportQuery{
		From: from, To: to, PathId: pathId, Granularity: report.GranularityHour,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]mcpadapter.PromiseHealthRow, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		rows = append(rows, mcpadapter.PromiseHealthRow{
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

// mcpHarness wires the REAL production stack — Postgres OrderRepo,
// UnitOfWork, ReceiveOrder (to seed a genuinely-persisted order),
// GetOrder, the real analytics projection + report store behind the
// PromiseHealth port, mcp.NewServer, mcp.Handler — and serves it over
// HTTP. It returns a connected SDK client session plus the seeded order
// id; the test drives tools/list and tools/call exactly like a model
// host would.
type mcpHarness struct {
	session    *sdkmcp.ClientSession
	orderID    string
	publisher  *mcpBufferingPublisher
	seededPath string
}

// mcpSeedHour is the fixed business hour every analytics fact lands in.
var mcpSeedHour = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

func newMCPHarness(t *testing.T) *mcpHarness {
	t.Helper()
	ctx := context.Background()

	// OLTP side: real repo + UoW + ReceiveOrder seeding one released
	// two-line order, exactly the write path that produced whatever a
	// production get_order would read back.
	oltpPool, err := postgres.NewPool(ctx, mcpMigratedDB(t, mcpOLTPTemplateDB))
	if err != nil {
		t.Fatalf("open oltp pool: %v", err)
	}
	t.Cleanup(oltpPool.Close)

	orders := postgres.NewOrderRepo(oltpPool)
	uow := postgres.NewUnitOfWork(oltpPool)
	publisher := &mcpBufferingPublisher{}
	clock := mcpFixedClock{mcpSeedHour}
	receive := &usecases.ReceiveOrder{
		Orders: orders, Events: publisher, Clock: clock,
		Inventory:  &mcpScriptedInventory{},
		Promise:    order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)},
		UnitOfWork: uow,
	}
	seeded, err := receive.Execute(ctx, []usecases.NewLine{
		{SKU: "ITCOV-MCP-SKU-1", Quantity: 2, PathID: "pick"},
		{SKU: "ITCOV-MCP-SKU-2", Quantity: 1, PathID: "pick", GiftWrap: true},
	}, false)
	if err != nil {
		t.Fatalf("seed receive order: %v", err)
	}

	// Analytics side: the real projection writes the funnel + promise KPI
	// facts the real report store (behind get_promise_health) reads.
	analyticsPool, err := postgres.NewPool(ctx, mcpMigratedDB(t, mcpAnalyticsTemplateDB))
	if err != nil {
		t.Fatalf("open analytics pool: %v", err)
	}
	t.Cleanup(analyticsPool.Close)

	projection := analyticsstore.NewPostgresProjection(analyticsPool)
	cutoff := mcpSeedHour.Add(4 * time.Hour)
	seeds := []struct {
		name string
		run  func() error
	}{
		{"lead-time allocation", func() error {
			return projection.ApplyOrderAllocated(ctx, "itcov-mcp-evt-lead", "pick", mcpSeedHour, "LeadTime", nil, false)
		}},
		{"capability allocation", func() error {
			return projection.ApplyOrderAllocated(ctx, "itcov-mcp-evt-cap", "pick", mcpSeedHour.Add(30*time.Minute), "Capability", &cutoff, false)
		}},
		{"network partial allocation", func() error {
			return projection.ApplyOrderPartiallyAllocated(ctx, "itcov-mcp-evt-net", "pick", mcpSeedHour, "Network", nil, true)
		}},
		{"repromise", func() error {
			return projection.ApplyOrderRepromised(ctx, "itcov-mcp-evt-reprom", mcpSeedHour)
		}},
	}
	for _, s := range seeds {
		if err := s.run(); err != nil {
			t.Fatalf("seed analytics %s: %v", s.name, err)
		}
	}

	// The MCP surface, wired like cmd/mcp: the shared GetOrder use case
	// over the real repo, and the PromiseHealth port over the real
	// report store.
	server := mcpadapter.NewServer(mcpadapter.Deps{
		GetOrder:      &usecases.GetOrder{Orders: orders},
		PromiseHealth: mcpPromiseHealthAdapter{store: analyticsstore.NewPostgresReport(analyticsPool)},
	})

	hs := httptest.NewServer(mcpadapter.Handler(server))
	t.Cleanup(hs.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "itcov-test-host", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdkmcp.StreamableClientTransport{
		Endpoint: hs.URL, DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	return &mcpHarness{
		session:    session,
		orderID:    seeded.ID().String(),
		publisher:  publisher,
		seededPath: "pick",
	}
}

func TestMCP_ListToolsExposesTheContract(t *testing.T) {
	h := newMCPHarness(t)

	list, err := h.session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"get_order", "get_promise_health"} {
		if !names[want] {
			t.Fatalf("tools/list must expose %q, got %v", want, names)
		}
	}
	// This context exposes no write tool over MCP (see Deps' doc
	// comment), so every registered tool must be annotated read-only —
	// a host can safely gate the whole surface.
	for _, tool := range list.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("tool %q must carry ReadOnlyHint=true (no write tool is exposed)", tool.Name)
		}
	}
}

func TestMCP_GetOrderRoundTripThroughPostgres(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	res, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": h.orderID},
	})
	if err != nil {
		t.Fatalf("tools/call get_order: %v", err)
	}
	if res.IsError {
		t.Fatalf("get_order returned a tool error: %+v", res.Content)
	}
	out, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res.StructuredContent)
	}
	if out["id"] != h.orderID {
		t.Fatalf("id = %v, want %s", out["id"], h.orderID)
	}
	if out["status"] != string(order.StatusReleased) {
		t.Fatalf("status = %v, want Released (the seeded order released at intake)", out["status"])
	}
	lines, ok := out["lines"].([]any)
	if !ok || len(lines) != 2 {
		t.Fatalf("lines = %v, want the seeded 2 lines", out["lines"])
	}
	first, _ := lines[0].(map[string]any)
	if first["sku"] != "ITCOV-MCP-SKU-1" || first["status"] != string(order.LineReleased) {
		t.Fatalf("line 1 = %v, want SKU ITCOV-MCP-SKU-1 / Released", first)
	}
	if first["reservationId"] == nil || first["reservationId"] == "" {
		t.Fatalf("line 1 reservationId = %v, want the persisted reservation id", first["reservationId"])
	}

	// The seeded write path really published its facts — the read the
	// tool just served reflects a genuinely persisted aggregate, not a
	// hand-inserted row.
	h.publisher.mu.Lock()
	sawReceived := false
	for _, name := range h.publisher.events {
		if name == "OrderReceived" {
			sawReceived = true
		}
	}
	h.publisher.mu.Unlock()
	if !sawReceived {
		t.Fatal("seed use case must have published OrderReceived")
	}

	// Domain rejection: an unknown order id is a TOOL error, never a
	// transport error.
	missing, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name:      "get_order",
		Arguments: map[string]any{"orderId": "ORD-GHOST-ITCOV"},
	})
	if err != nil {
		t.Fatalf("tools/call get_order (unknown id) must not be a transport error: %v", err)
	}
	if !missing.IsError {
		t.Fatal("get_order on an unknown order must surface a tool error")
	}
}

func TestMCP_GetPromiseHealthAggregatesFromPostgres(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()
	window := map[string]any{
		"from": "2026-10-09T00:00:00Z",
		"to":   "2026-10-10T00:00:00Z",
	}

	res, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "get_promise_health", Arguments: window,
	})
	if err != nil {
		t.Fatalf("tools/call get_promise_health: %v", err)
	}
	if res.IsError {
		t.Fatalf("get_promise_health returned a tool error: %+v", res.Content)
	}
	out, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("no structured content: %+v", res.StructuredContent)
	}

	// One LeadTime + one Capability + one Network allocation were seeded.
	wantCounts := map[string]float64{
		"promiseBasisCapability": 1,
		"promiseBasisLeadTime":   1,
		"promiseBasisNetwork":    1,
		"ordersAllocatedTotal":   3,
		"ordersSplitShipment":    1,
		"ordersRepromised":       1,
	}
	for field, want := range wantCounts {
		if got := out[field].(float64); got != want {
			t.Fatalf("%s = %v, want %v (full payload: %v)", field, out[field], want, out)
		}
	}
	// Rates derive from the totals: 1 split and 1 repromise over 3 orders.
	if got := out["splitShipmentRate"].(float64); !nearly(got, 1.0/3.0) {
		t.Fatalf("splitShipmentRate = %v, want %v", got, 1.0/3.0)
	}
	if got := out["repromiseRate"].(float64); !nearly(got, 1.0/3.0) {
		t.Fatalf("repromiseRate = %v, want %v", got, 1.0/3.0)
	}
	// Only the Capability allocation carried a real cutoff: the mean gap
	// is exactly (4h - 30m) = 12600 seconds.
	if got := out["promiseToCutoffGapSeconds"].(float64); !nearly(got, 12600) {
		t.Fatalf("promiseToCutoffGapSeconds = %v, want 12600", got)
	}

	// The documented path-filter trade-off: OrdersRepromised is
	// fleet-wide, never path-scoped, so filtering to a real path zeroes
	// that one KPI while the basis distribution stays.
	filtered, err := h.session.CallTool(ctx, &sdkmcp.CallToolParams{
		Name: "get_promise_health",
		Arguments: map[string]any{
			"from": window["from"], "to": window["to"], "pathId": h.seededPath,
		},
	})
	if err != nil {
		t.Fatalf("tools/call get_promise_health (filtered): %v", err)
	}
	if filtered.IsError {
		t.Fatalf("get_promise_health (filtered) returned a tool error: %+v", filtered.Content)
	}
	fout, _ := filtered.StructuredContent.(map[string]any)
	if got := fout["ordersRepromised"].(float64); got != 0 {
		t.Fatalf("ordersRepromised under a path filter = %v, want 0 (fleet-wide KPI)", got)
	}
	if got := fout["promiseBasisCapability"].(float64); got != 1 {
		t.Fatalf("promiseBasisCapability under a path filter = %v, want 1", got)
	}
}

func TestMCP_CallToolRejectsInvalidInput(t *testing.T) {
	h := newMCPHarness(t)
	ctx := context.Background()

	cases := []struct {
		name string
		call *sdkmcp.CallToolParams
	}{
		{
			name: "empty order id",
			call: &sdkmcp.CallToolParams{
				Name: "get_order", Arguments: map[string]any{"orderId": ""},
			},
		},
		{
			name: "malformed from timestamp",
			call: &sdkmcp.CallToolParams{
				Name: "get_promise_health",
				Arguments: map[string]any{
					"from": "not-a-timestamp", "to": "2026-10-10T00:00:00Z",
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.session.CallTool(ctx, tc.call)
			if err != nil {
				t.Fatalf("invalid input must be a tool error, not a transport error: %v", err)
			}
			if !res.IsError {
				t.Fatalf("invalid input must surface a tool error, got %+v", res.Content)
			}
		})
	}
}

// nearly compares two floats within a small relative epsilon.
func nearly(got, want float64) bool {
	epsilon := 1e-9
	return got > want-epsilon && got < want+epsilon
}
