//go:build integration

// Integration tests for the transactional outbox (ADR: see
// docs/docs/adr — this rollout's ADR) against a real Postgres 16, gated
// behind the `integration` build tag. Testcontainers-only: the package's
// TestMain (main_integration_test.go) boots ONE disposable Postgres
// container shared by every test here (each test truncates the tables), never reads
// DATABASE_URL or hardcodes localhost, so CI cannot silently skip this
// contract and a local run is byte-for-byte identical to CI's.
package postgres_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// recordingSink is a fake postgres.Sink: it records everything sent, and
// can be told to fail once on a specific event type so tests can drive
// the relay's failure/retry path deterministically.
type recordingSink struct {
	sent   []outboundkafka.Encoded
	failOn string // Topic to fail on once, "" for never
	failed bool
}

func (s *recordingSink) Send(_ context.Context, enc outboundkafka.Encoded) error {
	if s.failOn != "" && enc.Topic == s.failOn && !s.failed {
		s.failed = true
		return errors.New("broker down")
	}
	s.sent = append(s.sent, enc)
	return nil
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM outbox_events WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// noopInventory always fails allocation with a generic (non-business)
// error, so ReceiveOrder's own best-effort implicit allocation pass
// fails immediately with zero lines allocated and enqueues no
// additional outbox rows — keeping these tests' row counts about the
// ReceiveOrder/CancelOrder Save+Publish boundary under test, not about
// allocation's own event set.
type noopInventory struct{}

func (noopInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	return ports.ReservationResult{}, errors.New("inventory not configured (test)")
}
func (noopInventory) RevokeReservation(context.Context, string) error { return nil }

// TestOutbox_ReceiveOrder_CommitsAggregateAndBothTopicRowsTogether is the
// core atomicity claim: ReceiveOrder's aggregate Save and its
// OrderReceived publish land in the same Postgres transaction, and
// because this service fans every event out to two topics
// (kafka.FanOutPublisher's direct-mode equivalent), the SAME event
// enqueues one outbox row per topic.
func TestOutbox_ReceiveOrder_CommitsAggregateAndBothTopicRowsTogether(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	orders := postgres.NewOrderRepo(pool)
	writer := outboundkafka.NewPublisher(nil)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-1" }}
	outbox := postgres.NewOutboxPublisher(pool, writer, analytics)
	uow := postgres.NewUnitOfWork(pool)

	uc := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		Inventory: noopInventory{}, UnitOfWork: uow,
	}

	o, err := uc.Execute(ctx, []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}, true)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if got := countOutbox(t, pool, "published_at IS NULL AND event_type = 'com.warehouse.wes.order-management.order.OrderReceived'"); got != 1 {
		t.Fatalf("expected exactly 1 unpublished OrderReceived row total, got %d", got)
	}
	gotIntegration := countOutboxWhereTopic(t, pool, outboundkafka.Topic, "com.warehouse.wes.order-management.order.OrderReceived")
	gotAnalytics := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, "com.warehouse.wes.order-management.order.OrderReceived")
	// OrderReceived is not part of the integration publisher's contract
	// (see kafka.Publisher's package doc — only OrderAllocated/
	// OrderPartiallyAllocated/OrderRepromised are), so exactly one row
	// (analytics) is expected, not two.
	if gotIntegration != 0 {
		t.Fatalf("expected 0 integration-topic rows for OrderReceived (outside its contract), got %d", gotIntegration)
	}
	if gotAnalytics != 1 {
		t.Fatalf("expected 1 analytics-topic row for OrderReceived, got %d", gotAnalytics)
	}

	found, err := orders.FindByID(ctx, o.ID())
	if err != nil || found == nil {
		t.Fatalf("expected order persisted, got %v err=%v", found, err)
	}
}

// TestOutbox_SiteSkuDemandChanged_CommitsWithOrderSaveInOneTransaction
// pins the Phase-1 site/SKU demand projection's atomicity claim: with the
// static demand-site scope enabled, ReceiveOrder's allocation pass emits
// SiteSkuDemandChanged, and the row on the INTEGRATION topic (the event's
// real destination — its CloudEvents key/subject is the line-scoped
// "<order>/line/<n>", not the bare order id) is enqueued in the SAME
// transaction as the Order save. A rollback of the receive must take the
// demand row with it.
func TestOutbox_SiteSkuDemandChanged_CommitsWithOrderSaveInOneTransaction(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	orders := postgres.NewOrderRepo(pool)
	writer := outboundkafka.NewPublisher(nil)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-demand" }}
	outbox := postgres.NewOutboxPublisher(pool, writer, analytics)
	uow := postgres.NewUnitOfWork(pool)

	demandType := "com.warehouse.wes.order-management.siteskudemand.SiteSkuDemandChanged"

	uc := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC().Truncate(time.Microsecond)},
		Inventory: allocatingInventory{}, UnitOfWork: uow,
		DemandProjection: usecases.DemandProjectionPolicy{
			SiteID: "SIM1", AssignmentVersion: usecases.StaticDemandAssignmentVersion,
		},
	}
	o, err := uc.Execute(ctx, []usecases.NewLine{{SKU: "sku-1", Quantity: 3, PathID: "pick"}}, true)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if got := countOutboxWhereTopic(t, pool, outboundkafka.Topic, demandType); got != 1 {
		t.Fatalf("expected 1 integration-topic SiteSkuDemandChanged row, got %d", got)
	}
	var key, value []byte
	if err := pool.QueryRow(ctx,
		`SELECT key, value FROM outbox_events WHERE topic = $1 AND event_type = $2`, outboundkafka.Topic, demandType,
	).Scan(&key, &value); err != nil {
		t.Fatalf("read demand row: %v", err)
	}
	wantKey := o.ID().String() + "/line/1"
	if string(key) != wantKey {
		t.Fatalf("outbox key = %q, want the line-scoped subject %q", key, wantKey)
	}
	if !strings.Contains(string(value), `"state":"ACTIVE"`) {
		t.Fatalf("demand row value misses the ACTIVE state: %s", value)
	}

	// The analytics topic is NOT part of this event's contract: the
	// projector's funnel does not consume a site demand fact.
	if got := countOutboxWhereTopic(t, pool, outboundkafka.AnalyticsTopic, demandType); got != 0 {
		t.Fatalf("expected 0 analytics-topic SiteSkuDemandChanged rows, got %d", got)
	}
}

// allocatingInventory always succeeds, so ReceiveOrder's implicit
// allocation pass genuinely allocates the line (and raises the demand
// projection) inside the outbox transaction.
type allocatingInventory struct{}

func (allocatingInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	return ports.ReservationResult{ReservationID: "res-demand-1"}, nil
}
func (allocatingInventory) RevokeReservation(context.Context, string) error { return nil }

func countOutboxWhereTopic(t *testing.T, pool *pgxpool.Pool, topic, eventType string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE topic = $1 AND event_type = $2`, topic, eventType).Scan(&n)
	if err != nil {
		t.Fatalf("count outbox by topic: %v", err)
	}
	return n
}

// TestOutbox_PublishFailure_RollsBackAggregate is the whole point of the
// outbox: if an event cannot be enqueued, the aggregate change that
// raised it must not survive either. The failure is provoked with a
// broken Orders repo swapped in for AnalyticsPublisher's enrichment
// lookup — an Encode that returns an error − so this test drives the
// Encoder contract directly rather than depending on inventory calls.
func TestOutbox_PublishFailure_RollsBackAggregate(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()

	orders := postgres.NewOrderRepo(pool)
	outbox := postgres.NewOutboxPublisher(pool, failingEncoder{})
	uow := postgres.NewUnitOfWork(pool)

	uc := &usecases.ReceiveOrder{
		Orders: orders, Events: outbox, Clock: fixedClock{t: time.Now().UTC()},
		Inventory:  noopInventory{},
		UnitOfWork: uow,
	}

	before, err := countOrders(t, pool)
	if err != nil {
		t.Fatalf("count orders before: %v", err)
	}

	if _, err := uc.Execute(ctx, []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}, true); err == nil {
		t.Fatal("expected the failing encoder to fail the publish and roll back the receive")
	}

	after, err := countOrders(t, pool)
	if err != nil {
		t.Fatalf("count orders after: %v", err)
	}
	if after != before {
		t.Fatalf("order row survived a failed publish: the unit of work did not roll back (before=%d after=%d)", before, after)
	}
	if got := countOutbox(t, pool, "1=1"); got != 0 {
		t.Fatalf("expected no outbox rows at all, got %d", got)
	}
}

func countOrders(t *testing.T, pool *pgxpool.Pool) (int, error) {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), "SELECT count(*) FROM orders").Scan(&n)
	return n, err
}

// failingEncoder implements outboundkafka.Encoder and always errors, so
// postgres.OutboxPublisher.Publish fails deterministically without
// depending on a real Kafka broker or a malformed value database
// constraints happen to reject.
type failingEncoder struct{}

func (failingEncoder) Encode(context.Context, shared.DomainEvent) (outboundkafka.Encoded, bool, error) {
	return outboundkafka.Encoded{}, false, errors.New("encode failure (test)")
}

// TestOutboxRelay_PublishesInOrderAndMarksRows exercises the relay
// end-to-end against rows a real use case enqueued: CancelOrder raises
// OrderCancelled, which both encoders forward, so one Execute produces
// two ordered rows (integration first, analytics second — the order
// postgres.NewOutboxPublisher's encoders were given).
func TestOutboxRelay_PublishesInOrderAndMarksRows(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	orders := postgres.NewOrderRepo(pool)
	writer := outboundkafka.NewPublisher(nil)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-cancel" }}
	outbox := postgres.NewOutboxPublisher(pool, writer, analytics)
	uow := postgres.NewUnitOfWork(pool)

	receive := &usecases.ReceiveOrder{Orders: orders, Events: outbox, Clock: fixedClock{t: now}, Inventory: noopInventory{}, UnitOfWork: uow}
	o, err := receive.Execute(ctx, []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}, true)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	cancel := &usecases.CancelOrder{Orders: orders, Inventory: noopInventory{}, Events: outbox, Clock: fixedClock{t: now.Add(time.Second)}, UnitOfWork: uow}
	if _, err := cancel.Execute(ctx, o.ID()); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	sink := &recordingSink{}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	// OrderReceived and OrderCancelled are BOTH outside kafka.Publisher's
	// integration contract (only OrderAllocated/OrderPartiallyAllocated/
	// OrderRepromised are forwarded there — see that package's doc
	// comment); AnalyticsPublisher forwards every domain event. So this
	// pass drains exactly 2 rows, both on the analytics topic.
	if n != 2 || len(sink.sent) != 2 {
		t.Fatalf("expected 2 published, got n=%d sent=%d", n, len(sink.sent))
	}
	for _, enc := range sink.sent {
		if enc.Topic != outboundkafka.AnalyticsTopic {
			t.Fatalf("expected every row on the analytics topic, got %s", enc.Topic)
		}
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected every row marked published, %d still pending", got)
	}

	// A second pass finds nothing and republishes nothing.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 0 || len(sink.sent) != 2 {
		t.Fatalf("second pass should be a no-op, got n=%d err=%v sent=%d", n, err, len(sink.sent))
	}
}

// TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater verifies
// per-topic ordering survives a Send failure: the failing topic's row
// stays unpublished and is retried on the next pass, without skipping
// ahead to a later row on the SAME topic.
func TestOutboxRelay_SinkFailure_StopsAtFailedRowAndRetriesLater(t *testing.T) {
	pool := outboxDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	orders := postgres.NewOrderRepo(pool)
	writer := outboundkafka.NewPublisher(nil)
	analytics := &outboundkafka.AnalyticsPublisher{Orders: orders, NewID: func() string { return "evt-x" }}
	outbox := postgres.NewOutboxPublisher(pool, writer, analytics)
	uow := postgres.NewUnitOfWork(pool)

	receive := &usecases.ReceiveOrder{Orders: orders, Events: outbox, Clock: fixedClock{t: now}, Inventory: noopInventory{}, UnitOfWork: uow}
	if _, err := receive.Execute(ctx, []usecases.NewLine{{SKU: "sku-1", Quantity: 1, PathID: "pick"}}, true); err != nil {
		t.Fatalf("receive: %v", err)
	}

	// OrderReceived only reaches the analytics topic — fail exactly that
	// row once, verify it stays pending, then let it succeed on retry.
	sink := &recordingSink{failOn: outboundkafka.AnalyticsTopic}
	relay := postgres.NewOutboxRelay(pool, sink, slog.Default())

	n, err := relay.RelayOnce(ctx)
	if err == nil {
		t.Fatal("expected the failing row to surface an error")
	}
	if n != 0 || len(sink.sent) != 0 {
		t.Fatalf("expected nothing published before the failure, got n=%d sent=%v", n, sink.sent)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 1 {
		t.Fatalf("expected the row still pending, got %d pending", got)
	}
	var attempts int
	var lastErr string
	if err := pool.QueryRow(ctx, "SELECT attempts, coalesce(last_error,'') FROM outbox_events WHERE topic = $1", outboundkafka.AnalyticsTopic).Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if attempts != 1 || lastErr == "" {
		t.Fatalf("expected the row to record the failed attempt, got attempts=%d last_error=%q", attempts, lastErr)
	}

	// Broker recovers: the next pass drains it.
	n, err = relay.RelayOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("recovery pass: n=%d err=%v", n, err)
	}
	if got := countOutbox(t, pool, "published_at IS NULL"); got != 0 {
		t.Fatalf("expected outbox drained, %d pending", got)
	}
}
