//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/order-management/internal/adapters/outbound/postgres"
	"github.com/claudioed/order-management/internal/testing/pgtest"
	// The analytical Postgres below is booted once per test binary by
	// internal/testing/pgtest through testcontainers-go/modules/postgres; the
	// blank import keeps that container dependency visible in this package
	// for the architecture sensor.
	_ "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Shutdown()
	os.Exit(code)
}

// freeAddr reserves an ephemeral loopback port for a server to listen on.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// waitUntil polls cond every 200ms until it holds or the 90s deadline passes.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// getJSON fetches url and decodes its JSON body, returning the status code
// and the response's content type alongside it.
func getJSON(t *testing.T, url string) (int, string, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", nil
	}
	defer func() { _ = resp.Body.Close() }()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, resp.Header.Get("Content-Type"), body
}

// sigtermDrain stops the binary the way the pod's orchestrator does. The
// test registers its OWN SIGTERM handler first, so a signal landing before
// run() reaches its signal.Notify can never fall back to the default action
// and kill the whole test process; os/signal broadcasts to every registered
// channel, so a signal only our handler saw means run() missed it and the
// signal is retried until run() returns.
func sigtermDrain(t *testing.T, done <-chan error, timeout time.Duration) {
	t.Helper()
	relay := make(chan os.Signal, 1)
	signal.Notify(relay, syscall.SIGTERM)
	defer signal.Stop(relay)
	deadline := time.Now().Add(timeout)
	for {
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Fatalf("send SIGTERM: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("a signalled reports server must drain cleanly, got %v", err)
			}
			return
		case <-relay:
			// run() had not armed its own handler yet; retry shortly.
			time.Sleep(100 * time.Millisecond)
		case <-time.After(2 * time.Second):
			if time.Now().After(deadline) {
				t.Fatal("the reports server did not stop")
			}
		}
	}
}

// analyticsMigrationsDir resolves the analytics migration set from this
// file's compile-time location — the schema cmd/order-projector owns and
// this test applies itself, since the reader never migrates.
func analyticsMigrationsDir(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(file), "..", "..", "migrations", "analytics")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("analytics migrations directory not found at %s: %v", dir, err)
	}
	return dir
}

// TestReportsServeTheSeededFactsAndStopCleanly proves the READER half of the
// analytical data product (ADR-0006) end to end: over an EMPTY
// testcontainers Postgres migrated with the analytics set the projector owns,
// facts seeded through the repo's own analytics writer (the exact
// PostgresProjection cmd/order-projector applies per consumed event) are
// served by the real run() exactly as documented
// (docs/docs/analytics/order-funnel-report.md §REST API, the contract of
// ADR-0039): /healthz answers, GET /reports/funnel returns one row per
// (pathId, hourBucket) with the promise KPIs of ADR-0019 on the allocation
// row and the path-less ordersRepromised on the pathId "" row, a missing
// from/to is an RFC 7807 application/problem+json 400, /reports/funnel/
// freshness reports the age of the most recently applied event, and a
// SIGTERM drains to a clean nil return.
func TestReportsServeTheSeededFactsAndStopCleanly(t *testing.T) {
	ctx := context.Background()

	// EMPTY analytical database, migrated with the projector's own set: the
	// reader must boot against a schema it never creates itself.
	url := pgtest.EmptyURL(t)
	if err := postgres.RunMigrations(url, analyticsMigrationsDir(t)); err != nil {
		t.Fatalf("run the analytics migrations: %v", err)
	}

	// The repo's own analytics writer applies the same facts the projector
	// would project from the consumed events.
	writer, err := analyticsstore.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	projection := analyticsstore.NewPostgresProjection(writer)
	hour := time.Date(2026, 11, 12, 14, 0, 0, 0, time.UTC)
	at := hour.Add(5 * time.Minute)
	cutoff := at.Add(90 * time.Minute) // gap operand distinct from every other number here
	freshnessAt := time.Now().UTC().Add(-90 * time.Second)
	seeds := []struct {
		apply func() error
		what  string
	}{
		{func() error {
			return projection.ApplyOrderAllocated(ctx, "reports-it-alloc-1", "pick", at, "Capability", &cutoff, false)
		}, "OrderAllocated on pick with a Capability promise"},
		{func() error {
			return projection.ApplyLineReleased(ctx, "reports-it-line-1", "pack", hour.Add(20*time.Minute))
		}, "OrderLineReleased on pack"},
		{func() error {
			return projection.ApplyOrderRepromised(ctx, "reports-it-reprom-1", hour.Add(40*time.Minute))
		}, "OrderRepromised on the path-less rollup"},
		{func() error {
			// Outside the funnel window below; its occurred_at is what the
			// freshness probe must report the age of.
			return projection.ApplyLineReleased(ctx, "reports-it-fresh-1", "pick", freshnessAt)
		}, "the freshness-driving event"},
	}
	for _, s := range seeds {
		if err := s.apply(); err != nil {
			t.Fatalf("seed %s: %v", s.what, err)
		}
	}
	writer.Close()

	// The documented env knobs of cmd/order-reports (main.go).
	addr := freeAddr(t)
	t.Setenv("ANALYTICS_DATABASE_URL", url)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("LOG_LEVEL", "error")

	done := make(chan error, 1)
	go func() { done <- run() }()

	// /healthz answers only after the read-only pool's ping succeeded — the
	// liveness probe is the boot proof.
	waitUntil(t, "/healthz 200", func() bool {
		code, _, _ := getJSON(t, "http://"+addr+"/healthz")
		return code == http.StatusOK
	})

	// The documented funnel report answers from the seeded facts: rows are
	// ordered by (hourBucket, pathId), so the path-less re-promise row
	// synthesized by the reader comes first (ADR-0019: ordersRepromised is
	// never path-dimensioned).
	code, _, body := getJSON(t, "http://"+addr+"/reports/funnel?from=2026-11-12T14:00:00Z&to=2026-11-12T15:00:00Z")
	rows, _ := body["rows"].([]any)
	if code != http.StatusOK || len(rows) != 3 {
		t.Fatalf("funnel = %d %v", code, body)
	}
	repromisedRow, _ := rows[0].(map[string]any)
	packRow, _ := rows[1].(map[string]any)
	pickRow, _ := rows[2].(map[string]any)
	if repromisedRow["pathId"] != "" || repromisedRow["ordersRepromised"] != float64(1) || repromisedRow["ordersAllocated"] != float64(0) {
		t.Fatalf("re-promise row = %v", repromisedRow)
	}
	if packRow["pathId"] != "pack" || packRow["linesReleased"] != float64(1) || packRow["ordersAllocated"] != float64(0) {
		t.Fatalf("pack row = %v", packRow)
	}
	if pickRow["pathId"] != "pick" || pickRow["hourBucket"] != "2026-11-12T14:00:00Z" ||
		pickRow["ordersAllocated"] != float64(1) || pickRow["promiseBasisCapability"] != float64(1) ||
		pickRow["promiseBasisLeadTime"] != float64(0) || pickRow["promiseToCutoffGapSeconds"] != float64(5400) {
		t.Fatalf("pick row = %v", pickRow)
	}

	// A missing from/to is the documented RFC 7807 400.
	code, contentType, _ := getJSON(t, "http://"+addr+"/reports/funnel")
	if code != http.StatusBadRequest || contentType != "application/problem+json" {
		t.Fatalf("funnel without from/to = %d %q, want a 400 application/problem+json", code, contentType)
	}

	// Freshness reports the age of the most recently applied event — the
	// seeded now-90s occurrence, not an empty read model's zero.
	code, _, body = getJSON(t, "http://"+addr+"/reports/funnel/freshness")
	lag, _ := body["lagSeconds"].(float64)
	if code != http.StatusOK || lag < 60 || lag >= 600 {
		t.Fatalf("freshness = %d %v", code, body)
	}

	// The process's own stop path: SIGTERM is what the pod receives; run()
	// must drain the server and return nil.
	sigtermDrain(t, done, 30*time.Second)
}
