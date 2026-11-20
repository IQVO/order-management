//go:build integration

package http_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// A RETRYABLE refusal is not stored (ADR 0054, the fleet rule
// .claude/rules/fleet/idempotency-and-outbox.md): nothing committed and the
// answer depends on state that can change, so the key row is rolled back with
// the rest of the request and a retry under the same key executes again. These
// tests drive the REAL router and a REAL Postgres (testcontainers) through two
// real RETRYABLE problem types: the optimistic-lock conflict a genuine race
// produces, and a refusal that depends on a fact another context delivers later.

// gatedClock is the use cases' clock. Armed, its next Now() parks the caller
// until release(): the test freezes one request between loading the order and
// saving it, which is where a version race is lost.
type gatedClock struct {
	at      time.Time
	armed   atomic.Bool
	reached chan struct{}
	freed   chan struct{}
	once    sync.Once
}

func newGatedClock(at time.Time) *gatedClock {
	return &gatedClock{at: at, reached: make(chan struct{}), freed: make(chan struct{})}
}

func (c *gatedClock) Now() time.Time {
	if c.armed.CompareAndSwap(true, false) {
		close(c.reached)
		<-c.freed
	}
	return c.at
}

func (c *gatedClock) arm()     { c.armed.Store(true) }
func (c *gatedClock) release() { c.once.Do(func() { close(c.freed) }) }

// learningCatalogue is a process-path catalogue that does not know the path
// until the test says so: the state another context's event delivers later.
type learningCatalogue struct{ known atomic.Bool }

func (c *learningCatalogue) IsActive(shared.PathId) bool { return c.known.Load() }
func (c *learningCatalogue) CycleTimeP95(shared.PathId) (time.Duration, bool) {
	return 0, false
}
func (c *learningCatalogue) Eligibility(shared.PathId) (shared.Eligibility, bool) {
	return shared.Eligibility{}, false
}
func (c *learningCatalogue) ListActive() []shared.ActivePathCandidate { return nil }

// retryableRouter wires the REAL router over Postgres with every use case on one
// clock and, for ReceiveOrder, an optional process-path catalogue.
func retryableRouter(pool *pgxpool.Pool, inv ports.InventoryReservationClient, outbox *txOutbox, clock ports.Clock, catalogue ports.ProcessPathCatalogue) http.Handler {
	orders := postgres.NewOrderRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	promise := order.PromisePolicy{Fallback: order.NewLeadTimePolicy(24*time.Hour, nil)}
	server := &inboundhttp.Server{
		ReceiveOrder:    &usecases.ReceiveOrder{Orders: orders, Events: outbox, Clock: clock, Inventory: inv, Promise: promise, UnitOfWork: uow, Catalogue: catalogue},
		RetryAllocation: &usecases.RetryAllocation{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, Promise: promise, UnitOfWork: uow},
		ReleaseHeld:     &usecases.ReleaseHeldOrder{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, Promise: promise, UnitOfWork: uow},
		CancelOrder:     &usecases.CancelOrder{Orders: orders, Inventory: inv, Events: outbox, Clock: clock, UnitOfWork: uow},
		GetOrder:        &usecases.GetOrder{Orders: orders},
		IdempotencyPool: pool,
	}
	return inboundhttp.NewRouter(server, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
}

func keyRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM idempotency_keys WHERE key = $1", key).Scan(&n); err != nil {
		t.Fatalf("count key rows of %q: %v", key, err)
	}
	return n
}

// A genuine lost race: request A loads the held order and is frozen before its
// Save; request B releases the same order under another key and commits; A then
// saves against a stale version and answers 409 concurrent-modification. That
// answer depends on B's commit, so it is RETRYABLE: A's key row is gone, and
// the retry under A's key executes, succeeds and is then stored and replayed.
func TestReplayClass_ConcurrentModificationLoserIsReleasedAndItsRetryExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	clock := newGatedClock(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC))
	outbox := &txOutbox{pool: pool}
	h := retryableRouter(pool, &scriptedInventory{}, outbox, clock, nil)
	id := intake(t, h, "k-intake", heldOrderBody)
	path := "/orders/" + id + "/releases"

	clock.arm()
	loser := make(chan *httptest.ResponseRecorder, 1)
	go func() { loser <- postKeyed(h, path, "k-loser", "") }()
	select {
	case <-clock.reached:
	case <-time.After(20 * time.Second):
		clock.release()
		t.Fatal("the first request never reached its save: the race cannot be staged")
	}

	winner := postKeyed(h, path, "k-winner", "")
	clock.release()
	if winner.Code != http.StatusOK {
		t.Fatalf("the winning release = %d, want 200 (body: %s)", winner.Code, winner.Body.String())
	}

	var refused *httptest.ResponseRecorder
	select {
	case refused = <-loser:
	case <-time.After(20 * time.Second):
		t.Fatal("the losing request never answered")
	}
	if refused.Code != http.StatusConflict || !strings.HasSuffix(problemType(t, refused), "concurrent-modification") {
		t.Fatalf("the losing release = %d %s, want 409 concurrent-modification", refused.Code, refused.Body.String())
	}
	if got := keyRows(t, pool, "k-loser"); got != 0 {
		t.Fatalf("key rows of the loser = %d, want 0: a RETRYABLE refusal releases its key", got)
	}
	if got := keyRows(t, pool, "k-winner"); got != 1 {
		t.Fatalf("key rows of the winner = %d, want 1: its 200 is stored", got)
	}

	retry := postKeyed(h, path, "k-loser", "")
	if retry.Code != http.StatusOK || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("the retry under the loser's key = %d replayed=%q, want a fresh 200 execution (body: %s)",
			retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader), retry.Body.String())
	}
	if got := keyRows(t, pool, "k-loser"); got != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1: a 2xx is stored", got)
	}
	outboxAfterRetry := rowCount(t, pool, "outbox_events")
	assertReplayed(t, retry, postKeyed(h, path, "k-loser", ""))
	if got := rowCount(t, pool, "outbox_events"); got != outboxAfterRetry {
		t.Fatalf("the replay ran the use case again (outbox rows %d -> %d)", outboxAfterRetry, got)
	}
}

// A refusal that depends on a fact another context delivers later: the
// process-path catalogue does not know the path yet. Nothing was committed, so
// the key is released: the same key with another body is a NEW request (never
// idempotency-key-reused), and once the catalogue knows the path the same key
// and body succeed, and only that success is stored.
func TestReplayClass_UnknownProcessPathIsReleasedSoTheRetryFindsItOnceKnown(t *testing.T) {
	pool := idempotencyDB(t)
	catalogue := &learningCatalogue{}
	h := retryableRouter(pool, &scriptedInventory{}, &txOutbox{pool: pool}, fixedClockAt(time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)), catalogue)

	first := postKeyed(h, "/orders", "k-path", releasedOrderBody)
	if first.Code != http.StatusBadRequest || !strings.HasSuffix(problemType(t, first), "unknown-process-path") {
		t.Fatalf("intake before the catalogue knows the path = %d %s, want 400 unknown-process-path", first.Code, first.Body.String())
	}
	if got := keyRows(t, pool, "k-path"); got != 0 {
		t.Fatalf("key rows after a RETRYABLE refusal = %d, want 0", got)
	}

	other := postKeyed(h, "/orders", "k-path", heldOrderBody)
	if strings.HasSuffix(problemType(t, other), "idempotency-key-reused") {
		t.Fatalf("another body under a released key answered idempotency-key-reused: the request is new, not a reuse (%s)", other.Body.String())
	}

	catalogue.known.Store(true)
	retry := postKeyed(h, "/orders", "k-path", releasedOrderBody)
	if retry.Code != http.StatusCreated || retry.Header().Get(inboundhttp.IdempotencyReplayedHeader) != "" {
		t.Fatalf("retry once the catalogue knows the path = %d replayed=%q, want a fresh 201 (body: %s)",
			retry.Code, retry.Header().Get(inboundhttp.IdempotencyReplayedHeader), retry.Body.String())
	}
	assertReplayed(t, retry, postKeyed(h, "/orders", "k-path", releasedOrderBody))
	if got := rowCount(t, pool, "orders"); got != 1 {
		t.Fatalf("orders = %d, want 1: the refusals created nothing and the replay created nothing", got)
	}
}
