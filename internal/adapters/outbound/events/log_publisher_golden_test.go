package events_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/events"
	"github.com/claudioed/order-management/internal/domain/shared"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/log_payloads.golden")

// goldenEvents returns one fixture per domain event type (and the
// optional-field variants) with deterministic content.
func goldenEvents() []shared.DomainEvent {
	at := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	promise := at.Add(24 * time.Hour)
	cpt, basis := "sp1-1800", "Capability"
	cutoff := at.Add(6 * time.Hour)
	lines := []shared.ReleasedLine{
		{LineNo: 1, SKU: "SKU-1", PathID: "path-a", GiftWrap: true, FulfillmentClass: "SINGLE"},
		{LineNo: 2, SKU: "SKU-2", PathID: "path-b", FulfillmentClass: "MULTI_LINE_MULTI", PromiseCptId: &cpt, PromiseBasis: &basis, PromiseCutoffAt: &cutoff},
	}
	return []shared.DomainEvent{
		shared.NewOrderReceived(at, "ord-1", 2),
		shared.NewOrderLineAllocated(at, "ord-1", 1, "SKU-1", 3, "res-9"),
		shared.NewOrderLineBackordered(at, "ord-1", 2, "SKU-2", 4),
		shared.NewOrderAllocated(at, "ord-1", promise, lines),
		shared.NewOrderAllocated(at, "ord-2", promise, nil),
		shared.NewOrderAllocatedWithPromise(at, "ord-1", promise, "sp1-1800", "Capability", lines),
		shared.NewOrderPartiallyAllocated(at, "ord-1", 1, 1, promise, lines),
		shared.NewOrderPartiallyAllocatedWithPromise(at, "ord-1", 1, 1, promise, "sp1-1800", "Capability", lines),
		shared.NewOrderLineReleased(at, "ord-1", 1, "path-a", "wu-5"),
		shared.NewOrderReleased(at, "ord-1"),
		shared.NewOrderCancelled(at, "ord-1", 2),
		shared.NewOrderAllocationPartiallyFailed(at, "ord-1", 1, 2, "boom"),
		shared.NewOrderRepromised(at, "ord-1", "sp1-1200", "sp1-1800", "TaskCPTMissed"),
	}
}

// TestLogPublisherPayloadMatchesGolden pins the exact JSON the log
// publisher emits as `payload` for every domain event type, byte for
// byte, so moving the JSON shape out of the domain structs cannot drift.
func TestLogPublisherPayloadMatchesGolden(t *testing.T) {
	var got []string
	for _, ev := range goldenEvents() {
		var buf bytes.Buffer
		pub := events.NewLogPublisher(slog.New(slog.NewJSONHandler(&buf, nil)))
		if err := pub.Publish(context.Background(), ev); err != nil {
			t.Fatalf("Publish(%s): %v", ev.EventName(), err)
		}
		var record struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		got = append(got, string(record.Payload))
	}
	path := filepath.Join("testdata", "log_payloads.golden")
	joined := strings.Join(got, "\n") + "\n"
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(joined), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if string(want) != joined {
		t.Fatalf("log payload drifted from golden\nwant:\n%s\ngot:\n%s", want, joined)
	}
}
