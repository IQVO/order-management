//go:build integration

package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
	"github.com/claudioed/order-management/internal/pgtx"
)

// Support for the per-route idempotency scenarios (REST API guideline 2.x):
// a scripted, call-counting inventory and an outbox publisher that writes
// through the request's transaction exactly as postgres.OutboxPublisher does.

// scriptedInventory is a ports.InventoryReservationClient whose answer the
// test changes between requests and whose Reserve calls it counts.
type scriptedInventory struct {
	mu         sync.Mutex
	reserveErr error
	reserves   int
}

func (s *scriptedInventory) Reserve(context.Context, ports.ReservationRequest) (ports.ReservationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserves++
	if s.reserveErr != nil {
		return ports.ReservationResult{}, s.reserveErr
	}
	return ports.ReservationResult{ReservationID: "res-stub"}, nil
}

func (s *scriptedInventory) RevokeReservation(context.Context, string) error { return nil }

func (s *scriptedInventory) answer(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveErr = err
}

func (s *scriptedInventory) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reserves
}

// txOutbox enqueues one outbox_events row per event through the transaction
// bound to ctx (the middleware's, via pgtx), so the outbox row commits or
// rolls back with the aggregate and the key row. failNext makes the next
// Publish fail AFTER the aggregate was saved, to prove the rollback.
type txOutbox struct {
	pool     *pgxpool.Pool
	failNext atomic.Bool
}

func (p *txOutbox) Publish(ctx context.Context, event shared.DomainEvent) error {
	if p.failNext.CompareAndSwap(true, false) {
		return fmt.Errorf("outbox: simulated failure after the aggregate write")
	}
	exec := func(sql string, args ...any) (pgconn.CommandTag, error) { return p.pool.Exec(ctx, sql, args...) }
	if tx, ok := pgtx.TxFrom(ctx); ok {
		exec = func(sql string, args ...any) (pgconn.CommandTag, error) { return tx.Exec(ctx, sql, args...) }
	}
	_, err := exec(`INSERT INTO outbox_events (topic, event_type, value) VALUES ('t', $1, '{}')`, fmt.Sprintf("%T", event))
	return err
}

// routesRouter wires the REAL router over Postgres with EVERY use case on the
// same scripted inventory and transactional outbox.
func routesRouter(pool *pgxpool.Pool, inv ports.InventoryReservationClient, outbox *txOutbox) http.Handler {
	orders := postgres.NewOrderRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	clock := fixedClockAt(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC))
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}
	server := &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: outbox, Clock: clock, Inventory: inv, Promise: promise, UnitOfWork: uow},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, Promise: promise, UnitOfWork: uow},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, Promise: promise, UnitOfWork: uow},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, UnitOfWork: uow},
		GetOrder:        &usecases.GetOrder{Orders: orders},
		IdempotencyPool: pool,
	}
	return inboundhttp.NewRouter(server, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
}

// postKeyed sends one POST with an Idempotency-Key (omitted when key == "").
func postKeyed(h http.Handler, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set(inboundhttp.IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// intake creates an order through the keyed POST /orders and returns its id.
func intake(t *testing.T, h http.Handler, key, body string) string {
	t.Helper()
	rec := postKeyed(h, "/orders", key, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("setup: POST /orders = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	var o struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &o); err != nil || o.ID == "" {
		t.Fatalf("setup: decode order: %v (body: %s)", err, rec.Body.String())
	}
	return o.ID
}

func rowCount(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func problemType(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem: %v (body: %s)", err, rec.Body.String())
	}
	return p.Type
}

// assertReplayed checks the replay contract: same status and body as the
// first answer, plus Idempotency-Replayed: true, and the first answer
// itself never carried the header.
func assertReplayed(t *testing.T, first, replay *httptest.ResponseRecorder) {
	t.Helper()
	if got := first.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "" {
		t.Fatalf("the first execution carries %s: %q", inboundhttp.IdempotencyReplayedHeader, got)
	}
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay differs from the first answer:\nfirst  %d %s\nreplay %d %s", first.Code, first.Body.String(), replay.Code, replay.Body.String())
	}
	if got := replay.Header().Get(inboundhttp.IdempotencyReplayedHeader); got != "true" {
		t.Fatalf("replay %s = %q, want \"true\"", inboundhttp.IdempotencyReplayedHeader, got)
	}
}
