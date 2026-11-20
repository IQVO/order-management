//go:build integration

package http_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/pgtx"
)

// No production problem type declares OUTCOME (ADR 0054 §3: no keyed use case
// commits and then refuses), but the class is part of the vocabulary and the
// middleware must honour it the day a refusal needs it: the command's writes
// commit WITH the stored answer, a replay records nothing new, and an OUTCOME
// whose writes were lost is never stored. A synthetic OUTCOME refusal drives the
// real middleware over a real Postgres.

func outcomeProbeWrite(t *testing.T, r *http.Request) {
	t.Helper()
	tx, ok := pgtx.TxFrom(r.Context())
	if !ok {
		t.Fatal("the middleware did not bind a transaction into the request context")
	}
	if _, err := tx.Exec(r.Context(), `INSERT INTO outcome_probe (n) VALUES (1)`); err != nil {
		t.Fatalf("probe write: %v", err)
	}
}

func TestIdempotency_OutcomeRefusalCommitsItsWritesWithTheStoredAnswer(t *testing.T) {
	pool := idempotencyDB(t)
	if _, err := pool.Exec(t.Context(), `CREATE TABLE IF NOT EXISTS outcome_probe (n INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `TRUNCATE outcome_probe`); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		outcomeProbeWrite(t, r)
		inboundhttp.RefuseAsOutcomeForTest(w, r)
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-outcome"))
	if first.Code != http.StatusConflict {
		t.Fatalf("first = %d %s, want the 409 OUTCOME refusal", first.Code, first.Body.String())
	}
	for i := 0; i < 3; i++ {
		replay := httptest.NewRecorder()
		mw.ServeHTTP(replay, poisonRequest("k-outcome"))
		assertReplayed(t, first, replay)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if got := rowCount(t, pool, "outcome_probe"); got != 1 {
		t.Fatalf("committed writes = %d, want 1: committed with the answer, and a replay records nothing new", got)
	}
	if got := keyRows(t, pool, "k-outcome"); got != 1 {
		t.Fatalf("key rows = %d, want 1", got)
	}
}

// An OUTCOME answer whose writes cannot be committed (the transaction was
// aborted inside the handler, so the savepoint cannot be released) is a 500 and
// stores nothing: the key retries, like a 2xx behind a failed release.
func TestIdempotency_AnOutcomeRefusalWhoseWritesAreLostIsNeverStored(t *testing.T) {
	pool := idempotencyDB(t)
	var calls atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		poisonTx(t, r)
		inboundhttp.RefuseAsOutcomeForTest(w, r)
	})
	mw := inboundhttp.RequireIdempotencyKey(pool)(handler)

	first := httptest.NewRecorder()
	mw.ServeHTTP(first, poisonRequest("k-outcome-lost"))
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first = %d %s, want 500: the writes the OUTCOME stands for never happened", first.Code, first.Body.String())
	}
	if got := keyRows(t, pool, "k-outcome-lost"); got != 0 {
		t.Fatalf("key rows = %d, want 0", got)
	}
	again := httptest.NewRecorder()
	mw.ServeHTTP(again, poisonRequest("k-outcome-lost"))
	if calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2: the key retries", calls.Load())
	}
}
