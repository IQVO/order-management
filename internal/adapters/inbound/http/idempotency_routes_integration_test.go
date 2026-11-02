//go:build integration

package http_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
)

// Per-route idempotency scenarios against the REAL router and a REAL
// Postgres (testcontainers): all three POST routes are keyed, a replay is
// byte-identical with Idempotency-Replayed, reuse is 409, a 4xx is cached, a
// 5xx is not stored, and the key row / aggregate / outbox rows commit
// together.

const (
	heldOrderBody     = `{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":false,"releaseOnAllocation":false}`
	releasedOrderBody = `{"lines":[{"sku":"SKU-1","quantity":1}],"allowPartialShipment":false}`
)

func TestIdempotency_EveryPostRouteDemandsAValidKey(t *testing.T) {
	pool := idempotencyDB(t)
	h := routesRouter(pool, &scriptedInventory{}, &txOutbox{pool: pool})

	routes := []string{"/orders", "/orders/ord-1/releases", "/orders/ord-1/allocation-retries"}
	badKeys := map[string]string{
		"too long":     strings.Repeat("k", 129),
		"control char": "bad\tkey",
		"non-ASCII":    "clé",
	}
	for _, path := range routes {
		rec := postKeyed(h, path, "", releasedOrderBody)
		if rec.Code != http.StatusBadRequest || !strings.HasSuffix(problemType(t, rec), "idempotency-key-required") {
			t.Errorf("POST %s without a key = %d %s, want 400 idempotency-key-required", path, rec.Code, rec.Body.String())
		}
		for name, key := range badKeys {
			rec := postKeyed(h, path, key, releasedOrderBody)
			if rec.Code != http.StatusBadRequest || !strings.HasSuffix(problemType(t, rec), "idempotency-key-invalid") {
				t.Errorf("POST %s with a %s key = %d %s, want 400 idempotency-key-invalid", path, name, rec.Code, rec.Body.String())
			}
		}
	}
	if got := rowCount(t, pool, "idempotency_keys"); got != 0 {
		t.Fatalf("idempotency_keys rows = %d, want 0: a rejected key must store nothing", got)
	}
}

func TestIdempotency_Releases_ReplayThenReuseIs409(t *testing.T) {
	pool := idempotencyDB(t)
	inv := &scriptedInventory{}
	h := routesRouter(pool, inv, &txOutbox{pool: pool})
	id := intake(t, h, "k-intake", heldOrderBody)

	first := postKeyed(h, "/orders/"+id+"/releases", "k-release", "")
	if first.Code != http.StatusOK {
		t.Fatalf("release = %d, want 200 (body: %s)", first.Code, first.Body.String())
	}
	reservesAfterFirst, outboxAfterFirst := inv.calls(), rowCount(t, pool, "outbox_events")

	replay := postKeyed(h, "/orders/"+id+"/releases", "k-release", "")
	assertReplayed(t, first, replay)
	if inv.calls() != reservesAfterFirst || rowCount(t, pool, "outbox_events") != outboxAfterFirst {
		t.Fatalf("the replay ran the use case again (reserve calls %d -> %d, outbox rows %d -> %d)",
			reservesAfterFirst, inv.calls(), outboxAfterFirst, rowCount(t, pool, "outbox_events"))
	}

	reuse := map[string]string{
		"another route": "/orders/" + id + "/allocation-retries",
		"another order": "/orders/ord-other/releases",
		"POST /orders":  "/orders",
	}
	for name, path := range reuse {
		rec := postKeyed(h, path, "k-release", "")
		if rec.Code != http.StatusConflict || !strings.HasSuffix(problemType(t, rec), "idempotency-key-reused") {
			t.Errorf("reusing the key on %s (%s) = %d %s, want 409 idempotency-key-reused", name, path, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Idempotency-Replayed"); got != "" {
			t.Errorf("a 409 reuse answer carries Idempotency-Replayed: %q", got)
		}
	}
}

func TestIdempotency_AllocationRetries_ReplayRunsTheUseCaseOnce(t *testing.T) {
	pool := idempotencyDB(t)
	inv := &scriptedInventory{}
	inv.answer(ports.ErrInsufficientStock)
	h := routesRouter(pool, inv, &txOutbox{pool: pool})
	id := intake(t, h, "k-intake", releasedOrderBody)

	inv.answer(nil) // stock arrived: the retry allocates
	before := inv.calls()
	first := postKeyed(h, "/orders/"+id+"/allocation-retries", "k-retry", "")
	if first.Code != http.StatusOK {
		t.Fatalf("allocation retry = %d, want 200 (body: %s)", first.Code, first.Body.String())
	}
	if inv.calls() != before+1 {
		t.Fatalf("reserve calls = %d, want %d", inv.calls(), before+1)
	}
	assertReplayed(t, first, postKeyed(h, "/orders/"+id+"/allocation-retries", "k-retry", ""))
	if inv.calls() != before+1 {
		t.Fatalf("the replay called inventory-storage again (%d calls)", inv.calls())
	}
	if got := rowCount(t, pool, "idempotency_keys"); got != 2 {
		t.Fatalf("idempotency_keys rows = %d, want 2 (intake + retry)", got)
	}
}

func TestIdempotency_AllocationRetries_ConcurrentSameKeyOneEffect(t *testing.T) {
	pool := idempotencyDB(t)
	inv := &scriptedInventory{}
	inv.answer(ports.ErrInsufficientStock)
	h := routesRouter(pool, inv, &txOutbox{pool: pool})
	id := intake(t, h, "k-intake", releasedOrderBody)
	inv.answer(nil)
	before := inv.calls()

	const n = 5
	results := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = postKeyed(h, "/orders/"+id+"/allocation-retries", "k-concurrent", "")
		}(i)
	}
	wg.Wait()

	executed := 0
	for i, rec := range results {
		if rec.Code != http.StatusOK || rec.Body.String() != results[0].Body.String() {
			t.Fatalf("request %d = %d %s, want the same 200 body as request 0 (%s)", i, rec.Code, rec.Body.String(), results[0].Body.String())
		}
		if rec.Header().Get("Idempotency-Replayed") == "" {
			executed++
		}
	}
	if executed != 1 {
		t.Fatalf("%d of %d concurrent requests executed, want exactly 1 (the rest replay)", executed, n)
	}
	if inv.calls() != before+1 {
		t.Fatalf("reserve calls = %d, want %d: one effect for %d concurrent requests", inv.calls(), before+1, n)
	}
}

func TestIdempotency_ClientErrorIsCachedAndReplayed(t *testing.T) {
	pool := idempotencyDB(t)
	h := routesRouter(pool, &scriptedInventory{}, &txOutbox{pool: pool})
	id := intake(t, h, "k-intake", releasedOrderBody) // not held: nothing to release on demand
	outboxBefore := rowCount(t, pool, "outbox_events")

	first := postKeyed(h, "/orders/"+id+"/releases", "k-409", "")
	if first.Code != http.StatusConflict || !strings.HasSuffix(problemType(t, first), "order-not-held") {
		t.Fatalf("release of an unheld order = %d %s, want 409 order-not-held", first.Code, first.Body.String())
	}
	assertReplayed(t, first, postKeyed(h, "/orders/"+id+"/releases", "k-409", ""))
	if got := rowCount(t, pool, "outbox_events"); got != outboxBefore {
		t.Fatalf("a rejected release wrote outbox rows (%d -> %d)", outboxBefore, got)
	}
}

func TestIdempotency_ServerErrorIsNotStoredAndTheRetryReExecutes(t *testing.T) {
	pool := idempotencyDB(t)
	inv := &scriptedInventory{}
	inv.answer(ports.ErrInsufficientStock)
	h := routesRouter(pool, inv, &txOutbox{pool: pool})
	id := intake(t, h, "k-intake", releasedOrderBody)
	outboxBefore := rowCount(t, pool, "outbox_events")

	inv.answer(fmt.Errorf("%w: connection refused", ports.ErrDownstreamUnavailable))
	down := postKeyed(h, "/orders/"+id+"/allocation-retries", "k-503", "")
	if down.Code != http.StatusServiceUnavailable {
		t.Fatalf("retry while inventory-storage is down = %d, want 503 (body: %s)", down.Code, down.Body.String())
	}
	var stored int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM idempotency_keys WHERE key = 'k-503'").Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("a 5xx must not be stored: rows = %d, err = %v", stored, err)
	}
	if got := rowCount(t, pool, "outbox_events"); got != outboxBefore {
		t.Fatalf("a failed retry left outbox rows behind (%d -> %d)", outboxBefore, got)
	}

	inv.answer(nil)
	again := postKeyed(h, "/orders/"+id+"/allocation-retries", "k-503", "")
	if again.Code != http.StatusOK || again.Header().Get("Idempotency-Replayed") != "" {
		t.Fatalf("retry with the same key after recovery = %d (replayed=%q), want a fresh 200 execution",
			again.Code, again.Header().Get("Idempotency-Replayed"))
	}
}

func TestIdempotency_KeyRowAggregateAndOutboxCommitTogether(t *testing.T) {
	pool := idempotencyDB(t)
	outbox := &txOutbox{pool: pool}
	h := routesRouter(pool, &scriptedInventory{}, outbox)

	outbox.failNext.Store(true) // fails AFTER the order was saved
	failed := postKeyed(h, "/orders", "k-atomic", releasedOrderBody)
	if failed.Code < http.StatusInternalServerError {
		t.Fatalf("intake with a failing outbox = %d, want 5xx (body: %s)", failed.Code, failed.Body.String())
	}
	for _, table := range []string{"orders", "outbox_events", "idempotency_keys"} {
		if got := rowCount(t, pool, table); got != 0 {
			t.Fatalf("%s rows = %d after a rolled-back intake, want 0: the three writes are one transaction", table, got)
		}
	}

	ok := postKeyed(h, "/orders", "k-atomic", releasedOrderBody) // same key: the 5xx was never stored
	if ok.Code != http.StatusCreated {
		t.Fatalf("intake retry = %d, want 201 (body: %s)", ok.Code, ok.Body.String())
	}
	if rowCount(t, pool, "orders") != 1 || rowCount(t, pool, "idempotency_keys") != 1 || rowCount(t, pool, "outbox_events") < 1 {
		t.Fatalf("after a committed intake: orders=%d keys=%d outbox=%d, want 1, 1, >=1",
			rowCount(t, pool, "orders"), rowCount(t, pool, "idempotency_keys"), rowCount(t, pool, "outbox_events"))
	}
}
