//go:build integration

// Integration tests for the OLTP Postgres adapters (OrderRepo and the
// Postgres EventPublisher) against a real Postgres 16, gated behind the
// `integration` build tag. They are driven by CI's integration job (two
// postgres:16 service containers — one per database this repo owns) or run
// locally against `docker compose up -d` with DATABASE_URL exported; when
// DATABASE_URL is not set they skip, so `make test` stays hermetic.
package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func requireDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set, skipping OLTP postgres integration test")
	}
	return url
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := requireDatabaseURL(t)
	if err := postgres.RunMigrations(url, "../../../../migrations"); err != nil {
		t.Fatalf("migrate OLTP: %v", err)
	}
	pool, err := postgres.NewPool(context.Background(), url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestOrderRepo_SaveAndFindByID_RoundTrip(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	occurredAt := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	id := shared.OrderId("ord-int-" + time.Now().Format("150405.000000000"))

	// Build the aggregate through the domain's public API, not by
	// hand-crafting rows: what is stored must be exactly what the
	// aggregate produces.
	lines := mustLines(t, id)
	o, err := order.New(id, lines, true)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}

	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (received): %v", err)
	}

	// Drive the same aggregate through its whole legal lifecycle,
	// persisting after each mutation — the repo must round-trip every
	// status transition the use cases can produce.
	if err := o.Allocate(1, "res-int-1"); err != nil {
		t.Fatalf("Allocate line 1: %v", err)
	}
	if err := o.Allocate(2, "res-int-2"); err != nil {
		t.Fatalf("Allocate line 2: %v", err)
	}
	o.SetPromise(order.Promise{CptId: "sp1-1800", CutoffAt: occurredAt.Add(48 * time.Hour), Basis: order.BasisCapability})
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (allocated): %v", err)
	}

	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		if reloaded.Status() != order.StatusAllocated {
			t.Errorf("status = %s, want Allocated", reloaded.Status())
		}
		if d := reloaded.PromiseDate(); d == nil || !d.Equal(occurredAt.Add(48*time.Hour)) {
			t.Errorf("promiseDate = %v, want %v", d, occurredAt.Add(48*time.Hour))
		}
		if cptId := reloaded.PromiseCptId(); cptId == nil || *cptId != "sp1-1800" {
			t.Errorf("promiseCptId = %v, want sp1-1800", cptId)
		}
		if basis := reloaded.PromiseBasis(); basis == nil || *basis != order.BasisCapability {
			t.Errorf("promiseBasis = %v, want Capability", basis)
		}
		if r := reloaded.Lines()[0].ReservationID(); r == nil || *r != "res-int-1" {
			t.Errorf("line 1 reservationId = %v, want res-int-1", r)
		}
	})

	if err := o.Release(1); err != nil {
		t.Fatalf("Release line 1: %v", err)
	}
	if err := o.Release(2); err != nil {
		t.Fatalf("Release line 2: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (released): %v", err)
	}

	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		if reloaded.Status() != order.StatusReleased {
			t.Errorf("status = %s, want Released", reloaded.Status())
		}
		for _, l := range reloaded.Lines() {
			if l.Status() != order.LineReleased {
				t.Errorf("line %d status = %s, want Released", l.LineNo(), l.Status())
			}
		}
	})
}

func TestOrderRepo_BackorderedAndCancelledRoundTrip(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	id := shared.OrderId("ord-int-bo-" + time.Now().Format("150405.000000000"))
	o, err := order.New(id, mustLines(t, id), false)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A Backordered line stores no reservation id; a cancelled order
	// stores the derived Cancelled status on every line.
	if err := o.MarkBackordered(2); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (backordered): %v", err)
	}

	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		if reloaded.Status() != order.StatusBackordered {
			t.Errorf("status = %s, want Backordered (ship-complete default)", reloaded.Status())
		}
		if r := reloaded.Lines()[1].ReservationID(); r != nil {
			t.Errorf("backordered line reservationId = %v, want nil", r)
		}
	})

	if err := o.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (cancelled): %v", err)
	}

	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		if reloaded.Status() != order.StatusCancelled {
			t.Errorf("status = %s, want Cancelled", reloaded.Status())
		}
	})
}

// TestOrderRepo_FindByID_Missing covers the repository's documented
// "not found is the application's concern" contract: (nil, nil), not an
// error.
func TestOrderRepo_FindByID_Missing(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)

	got, err := repo.FindByID(context.Background(), shared.OrderId("ord-int-does-not-exist"))
	if err != nil {
		t.Fatalf("FindByID missing: err = %v, want nil", err)
	}
	if got != nil {
		t.Fatalf("FindByID missing = %+v, want nil", got)
	}
}

// TestOrderRepo_NextID_Unique asserts every minted id is distinct — the
// `ord-<uuid>` convention's whole guarantee.
func TestOrderRepo_NextID_Unique(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)

	seen := make(map[shared.OrderId]struct{}, 100)
	for i := 0; i < 100; i++ {
		id, err := repo.NextID(context.Background())
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("NextID minted duplicate id %s", id)
		}
		seen[id] = struct{}{}
	}
}

// TestOrderRepo_PromiseGroups_RoundTrip covers ADR 0014 §3 / ADR 0017:
// a partial-shipment order whose promise is set as MULTIPLE groups round-
// trips its full breakdown (correct group count, correct line_nos per
// group, correct CptId/CutoffAt/Basis per group) through a real
// Postgres, via the widened Rehydrate/RehydrateWithGroups path — not
// just the legacy single-valued summary fields, which the older
// TestOrderRepo_SaveAndFindByID_RoundTrip test above already covers.
func TestOrderRepo_PromiseGroups_RoundTrip(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewOrderRepo(pool)
	ctx := context.Background()

	id := shared.OrderId("ord-int-groups-" + time.Now().Format("150405.000000000"))
	l1, err := order.NewOrderLine(1, shared.SKU("SKU-INT-G1"), 2, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine 1: %v", err)
	}
	l2, err := order.NewOrderLine(2, shared.SKU("SKU-INT-G2"), 1, "singles", false)
	if err != nil {
		t.Fatalf("NewOrderLine 2: %v", err)
	}
	l3, err := order.NewOrderLine(3, shared.SKU("SKU-INT-G3"), 1, "multis", false)
	if err != nil {
		t.Fatalf("NewOrderLine 3: %v", err)
	}
	o, err := order.New(id, []*order.OrderLine{l1, l2, l3}, true) // AllowPartialShipment=true
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (received): %v", err)
	}

	for i, res := range []string{"res-int-g1", "res-int-g2", "res-int-g3"} {
		if err := o.Allocate(i+1, res); err != nil {
			t.Fatalf("Allocate line %d: %v", i+1, err)
		}
	}

	baseTime := time.Date(2026, 9, 13, 8, 0, 0, 0, time.UTC)
	// Three groups: lines 1+2 share one real CPT cutoff (Capability
	// basis); line 3 falls back to a distinct, later LeadTime instant.
	groups := []order.PromiseGroup{
		{LineNos: []int{1, 2}, Promise: order.Promise{CptId: "sp1-1200", CutoffAt: baseTime.Add(2 * time.Hour), Basis: order.BasisCapability}},
		{LineNos: []int{3}, Promise: order.Promise{CutoffAt: baseTime.Add(48 * time.Hour), Basis: order.BasisLeadTime}},
	}
	o.SetPromiseGroups(groups)
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (allocated, grouped promise): %v", err)
	}

	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		got := reloaded.PromiseGroups()
		if len(got) != 2 {
			t.Fatalf("PromiseGroups() = %d groups, want 2: %+v", len(got), got)
		}

		byFirstLine := make(map[int]order.PromiseGroup, len(got))
		for _, g := range got {
			if len(g.LineNos) == 0 {
				t.Fatalf("group has empty LineNos: %+v", g)
			}
			byFirstLine[g.LineNos[0]] = g
		}

		sharedGroup, ok := byFirstLine[1]
		if !ok {
			t.Fatalf("no group starting with line 1: %+v", got)
		}
		if len(sharedGroup.LineNos) != 2 || sharedGroup.LineNos[0] != 1 || sharedGroup.LineNos[1] != 2 {
			t.Fatalf("shared group LineNos = %v, want [1 2]", sharedGroup.LineNos)
		}
		if sharedGroup.Promise.CptId != "sp1-1200" {
			t.Fatalf("shared group CptId = %q, want sp1-1200", sharedGroup.Promise.CptId)
		}
		if sharedGroup.Promise.Basis != order.BasisCapability {
			t.Fatalf("shared group Basis = %q, want Capability", sharedGroup.Promise.Basis)
		}
		if !sharedGroup.Promise.CutoffAt.Equal(baseTime.Add(2 * time.Hour)) {
			t.Fatalf("shared group CutoffAt = %v, want %v", sharedGroup.Promise.CutoffAt, baseTime.Add(2*time.Hour))
		}

		soloGroup, ok := byFirstLine[3]
		if !ok {
			t.Fatalf("no group starting with line 3: %+v", got)
		}
		if len(soloGroup.LineNos) != 1 {
			t.Fatalf("solo group LineNos = %v, want [3]", soloGroup.LineNos)
		}
		if soloGroup.Promise.CptId != "" {
			t.Fatalf("solo group CptId = %q, want empty (LeadTime basis has no CPT identity)", soloGroup.Promise.CptId)
		}
		if soloGroup.Promise.Basis != order.BasisLeadTime {
			t.Fatalf("solo group Basis = %q, want LeadTime", soloGroup.Promise.Basis)
		}
		if !soloGroup.Promise.CutoffAt.Equal(baseTime.Add(48 * time.Hour)) {
			t.Fatalf("solo group CutoffAt = %v, want %v", soloGroup.Promise.CutoffAt, baseTime.Add(48*time.Hour))
		}

		// The legacy summary fields must still project the LATEST
		// cutoff (ADR 0014 §3 / ADR 0017's documented projection rule)
		// even after a real round trip through Postgres, not just in
		// memory.
		want := baseTime.Add(48 * time.Hour)
		if d := reloaded.PromiseDate(); d == nil || !d.Equal(want) {
			t.Fatalf("PromiseDate() = %v, want %v (the latest group's cutoff)", d, want)
		}
		if b := reloaded.PromiseBasis(); b == nil || *b != order.BasisLeadTime {
			t.Fatalf("PromiseBasis() = %v, want LeadTime (from the latest group)", b)
		}
	})

	// Overwriting with a NEW, differently-shaped breakdown (a single
	// group now) must fully replace the old rows — delete-then-reinsert,
	// not accumulate stale groups from the previous save.
	o.SetPromiseGroups([]order.PromiseGroup{
		{LineNos: []int{1, 2, 3}, Promise: order.Promise{CptId: "sp1-1800", CutoffAt: baseTime.Add(6 * time.Hour), Basis: order.BasisCapability}},
	})
	if err := repo.Save(ctx, o); err != nil {
		t.Fatalf("Save (re-grouped): %v", err)
	}
	assertRoundTrip(t, repo, id, func(reloaded *order.Order) {
		got := reloaded.PromiseGroups()
		if len(got) != 1 {
			t.Fatalf("after re-grouping, PromiseGroups() = %d groups, want exactly 1 (stale groups must be gone): %+v", len(got), got)
		}
		if len(got[0].LineNos) != 3 {
			t.Fatalf("LineNos = %v, want all 3 lines in the one group", got[0].LineNos)
		}
	})
}

// TestOutboxPublisher_AppendsToOutboxEventsTable covers the Postgres
// OutboxPublisher: every published event lands in outbox_events, once
// per configured encoder that has a wire form for it, with its topic,
// type, and encoded payload — replacing the old EventPublisher (which
// wrote to `events` with no relay ever draining it).
func TestOutboxPublisher_AppendsToOutboxEventsTable(t *testing.T) {
	pool := newPool(t)
	orders := postgres.NewOrderRepo(pool)
	writer := kafka.NewPublisher(nil)
	analytics := &kafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-int" }}
	pub := postgres.NewOutboxPublisher(pool, writer, analytics)
	ctx := context.Background()

	occurredAt := time.Date(2026, 9, 7, 11, 30, 0, 0, time.UTC).UTC()
	event := shared.NewOrderReceived(occurredAt, shared.OrderId("ord-int-evt"), 2)
	if err := pub.Publish(ctx, event); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// OrderReceived is analytics-only (kafka.Publisher's own published
	// contract is OrderAllocated/OrderPartiallyAllocated/OrderRepromised
	// — see that package's doc comment), so exactly one row is expected.
	var topic, eventType string
	var value []byte
	err := pool.QueryRow(ctx,
		`SELECT topic, event_type, value FROM outbox_events
		 WHERE event_type = 'OrderReceived' AND published_at IS NULL
		 LIMIT 1`,
	).Scan(&topic, &eventType, &value)
	if err != nil {
		t.Fatalf("query outbox_events: %v", err)
	}
	if topic != kafka.AnalyticsTopic {
		t.Errorf("topic = %s, want %s", topic, kafka.AnalyticsTopic)
	}
	if eventType != "OrderReceived" {
		t.Errorf("event_type = %s, want OrderReceived", eventType)
	}
	if len(value) == 0 {
		t.Error("value = empty, want the marshalled event JSON")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE event_type = 'OrderReceived'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 outbox row for OrderReceived (analytics only), got %d", count)
	}
}

// mustLines builds two valid order lines on the default path.
func mustLines(t *testing.T, id shared.OrderId) []*order.OrderLine {
	t.Helper()
	_ = id // lines carry no order reference; the id is only for context
	l1, err := order.NewOrderLine(1, shared.SKU("SKU-INT-1"), 2, shared.DefaultPathId, false)
	if err != nil {
		t.Fatalf("NewOrderLine 1: %v", err)
	}
	l2, err := order.NewOrderLine(2, shared.SKU("SKU-INT-2"), 1, shared.DefaultPathId, true)
	if err != nil {
		t.Fatalf("NewOrderLine 2: %v", err)
	}
	return []*order.OrderLine{l1, l2}
}

// assertRoundTrip reloads id through the repo and hands the rehydrated
// aggregate to assert.
func assertRoundTrip(t *testing.T, repo *postgres.OrderRepo, id shared.OrderId, assert func(*order.Order)) {
	t.Helper()
	reloaded, err := repo.FindByID(context.Background(), id)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if reloaded == nil {
		t.Fatalf("FindByID %s = nil, want the saved order", id)
	}
	assert(reloaded)
}
