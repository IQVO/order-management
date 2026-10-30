package main

import (
	"bytes"
	"context"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	inbounda2a "github.com/claudioed/order-management/internal/adapters/inbound/a2a"
	"github.com/claudioed/order-management/internal/adapters/outbound/analyticsstore"
)

// TestBuildDeps_ThreadsMigrationsDatabaseURLNotDatabaseURLForMigrations proves
// buildDeps hands MIGRATIONS_DATABASE_URL (not DATABASE_URL) to the migration
// step (ADR 0029, the PgBouncer / pg_advisory_lock incompatibility). The
// migrations DSN is a schemeless string golang-migrate rejects at once with a
// distinctive parse error; DATABASE_URL points at an address nothing listens
// on. If the wiring ever migrated against DATABASE_URL, the retry log would
// show a dial error instead. The context deadline cuts the boot retry short.
func TestBuildDeps_ThreadsMigrationsDatabaseURLNotDatabaseURLForMigrations(t *testing.T) {
	t.Setenv("MIGRATIONS_DATABASE_URL", "not-a-valid-connection-string")
	t.Setenv("MIGRATIONS_PATH", "../../migrations")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, closeDeps, err := buildDeps(ctx, bootConfig{
		databaseURL:  "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1",
		analyticsURL: "postgres://r:p@127.0.0.1:1/analytics?sslmode=disable",
		agentURL:     testAgentURL,
	}, logger)
	if err == nil {
		closeDeps()
		t.Fatal("a malformed MIGRATIONS_DATABASE_URL must fail boot")
	}
	if !strings.Contains(logs.String(), "parse scheme") {
		t.Fatalf("log = %q: expected the bogus-URL parse error from migrate.New; a dial error would mean migrations ran against DATABASE_URL", logs.String())
	}
	if strings.Contains(logs.String(), "refused") {
		t.Fatalf("migrations dialled DATABASE_URL instead of MIGRATIONS_DATABASE_URL: %q", logs.String())
	}
}

func TestBuildPromiseHealthStore(t *testing.T) {
	t.Run("a malformed DSN is a boot error, not an empty store", func(t *testing.T) {
		store, closeStore, err := buildPromiseHealthStore(context.Background(), "::not a dsn::")
		if err == nil || !strings.Contains(err.Error(), "analytics read-only pool") {
			t.Fatalf("err = %v, want the analytics pool error", err)
		}
		if store != nil {
			t.Fatalf("store = %v, want nil on error", store)
		}
		closeStore()
	})
	t.Run("a valid DSN builds a store without dialling", func(t *testing.T) {
		store, closeStore, err := buildPromiseHealthStore(context.Background(), "postgres://r:p@127.0.0.1:1/analytics?sslmode=disable")
		if err != nil || store == nil {
			t.Fatalf("store = %v, err = %v", store, err)
		}
		closeStore()
	})
}

// TestReportStoreAdapterTranslatesRowsOneForOne proves the wiring-boundary
// translation from report.Row to the A2A package's own PromiseHealthRow.
func TestReportStoreAdapterTranslatesRowsOneForOne(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	cutoff := base.Add(4 * time.Hour)
	mem := analyticsstore.NewMemoryStore()
	for _, err := range []error{
		mem.ApplyOrderAllocated(ctx, "e1", "pick", base, "Capability", &cutoff, true),
		mem.ApplyOrderAllocated(ctx, "e2", "pick", base, "Network", nil, false),
		mem.ApplyOrderRepromised(ctx, "e3", base),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}

	var store inbounda2a.PromiseHealthStore = reportStoreAdapter{mem}
	rows, err := store.QueryPromiseHealth(ctx, base.Add(-time.Hour), base.Add(time.Hour), "")
	if err != nil {
		t.Fatalf("QueryPromiseHealth: %v", err)
	}
	var capability, network, repromised, split, samples int
	var gap float64
	for _, r := range rows {
		capability += r.PromiseBasisCapability
		network += r.PromiseBasisNetwork
		repromised += r.OrdersRepromised
		split += r.OrdersSplitShipment
		samples += r.PromiseToCutoffGapSamples
		gap += r.PromiseToCutoffGapSeconds
	}
	if capability != 1 || network != 1 || repromised != 1 || split != 1 || samples != 1 || gap != 4*3600 {
		t.Fatalf("rows = %+v", rows)
	}

	// The path filter reaches the report query.
	filtered, err := store.QueryPromiseHealth(ctx, base.Add(-time.Hour), base.Add(time.Hour), "no-such-path")
	if err != nil || len(filtered) != 0 {
		t.Fatalf("filtered rows = %+v, err = %v, want none", filtered, err)
	}
}

// bannedImportPaths are import-path fragments cmd/a2a must never reach.
var bannedImportPaths = []string{
	"kafka", "outbox", "outbound/memory", "outbound/inventorystorage", "adapters/inbound/mcp", "adapters/inbound/http",
}

// forbiddenMentions are identifiers that would mean a write use case or a
// publisher crept into the composition root.
var forbiddenMentions = []string{
	"ReceiveOrder", "CancelOrder", "RetryAllocation", "ReleaseHeldOrder", "KAFKA_BROKERS", "EVENT_PUBLISHER",
}

// bannedImportsOf returns the banned import paths of one non-test source file.
func bannedImportsOf(t *testing.T, file string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, imp := range parsed.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		for _, b := range bannedImportPaths {
			if strings.Contains(strings.ToLower(path), b) {
				out = append(out, path+" (matched "+b+")")
			}
		}
	}
	return out
}

// forbiddenMentionsOf returns the forbidden identifiers one source file names.
func forbiddenMentionsOf(t *testing.T, file string) []string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range forbiddenMentions {
		if bytes.Contains(src, []byte(m)) {
			out = append(out, m)
		}
	}
	return out
}

// TestRuntimeIsReadOnlyAndPostgresOnly is the source-level sensor for the two
// boundaries of this binary: it is READ-ONLY (no Kafka, no outbox publisher, no
// write use case -- the MCP surface offers no write, so the A2A write gate
// allows none) and it has NO in-memory runtime path (a degraded store would
// answer plausible-looking empty facts).
func TestRuntimeIsReadOnlyAndPostgresOnly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v %v", files, err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		for _, v := range bannedImportsOf(t, f) {
			t.Errorf("%s imports %s: cmd/a2a is read-only and Postgres-only", f, v)
		}
		for _, v := range forbiddenMentionsOf(t, f) {
			t.Errorf("%s mentions %s: the A2A agent wires no write use case and no publisher", f, v)
		}
	}
}
