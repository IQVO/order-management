package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func TestWorkUnitIDIsDeterministicAndLineScoped(t *testing.T) {
	if got, want := usecases.WorkUnitID("ord-77213", 1), "ord-77213-line-1"; got != want {
		t.Fatalf("WorkUnitID = %q, want %q", got, want)
	}
	if usecases.WorkUnitID("ord-1", 1) == usecases.WorkUnitID("ord-1", 2) {
		t.Fatal("two lines of the same order must not share a work unit id")
	}
	if usecases.WorkUnitID("ord-1", 1) == usecases.WorkUnitID("ord-2", 1) {
		t.Fatal("two orders must not share a work unit id")
	}
}

// TestParseWorkUnitID_RoundTripsWithWorkUnitID pins the exact contract
// ADR 0018 depends on: for every OrderId/lineNo WorkUnitID can produce,
// ParseWorkUnitID must recover them byte-for-byte. This is the
// round-trip a gremlins string-splitting mutant is most likely to break
// silently.
func TestParseWorkUnitID_RoundTripsWithWorkUnitID(t *testing.T) {
	tests := []struct {
		orderID shared.OrderId
		lineNo  int
	}{
		{"ord-7c9e6679-7d5a-4b37-b2f1-93b0c4a1d8f2", 1},
		{"ord-7c9e6679-7d5a-4b37-b2f1-93b0c4a1d8f2", 2},
		{"ord-1", 1},
		{"ord-1", 42},
	}
	for _, tt := range tests {
		wire := usecases.WorkUnitID(tt.orderID, tt.lineNo)
		gotOrderID, gotLineNo, ok := usecases.ParseWorkUnitID(wire)
		if !ok {
			t.Fatalf("ParseWorkUnitID(%q) ok = false, want true", wire)
		}
		if gotOrderID != tt.orderID {
			t.Errorf("ParseWorkUnitID(%q) orderID = %q, want %q", wire, gotOrderID, tt.orderID)
		}
		if gotLineNo != tt.lineNo {
			t.Errorf("ParseWorkUnitID(%q) lineNo = %d, want %d", wire, gotLineNo, tt.lineNo)
		}
	}
}

// TestParseWorkUnitID_LastOccurrenceIsSafeAgainstAnOrderIdContainingTheMarker
// pins the deliberate "split on the LAST -line- occurrence" choice: even
// an (unrealistic today, but not impossible) order id that itself
// contains the literal "-line-" substring must still resolve to the
// TRAILING numeric suffix as the line number, not an earlier one.
func TestParseWorkUnitID_LastOccurrenceIsSafeAgainstAnOrderIdContainingTheMarker(t *testing.T) {
	orderID, lineNo, ok := usecases.ParseWorkUnitID("ord-weird-line-thing-line-3")
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if orderID != "ord-weird-line-thing" {
		t.Errorf("orderID = %q, want %q", orderID, "ord-weird-line-thing")
	}
	if lineNo != 3 {
		t.Errorf("lineNo = %d, want 3", lineNo)
	}
}

// TestParseWorkUnitID_MalformedInputNeverPanicsAndReportsNotOK covers
// every shape of malformed order_ref this consumer must tolerate per ADR
// 0018: no marker at all, a marker with nothing before/after it, a
// non-numeric or non-positive line number. Every case must degrade to
// ok=false, never a panic.
func TestParseWorkUnitID_MalformedInputNeverPanicsAndReportsNotOK(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"no marker at all", "ord-7c9e6679"},
		{"marker with empty order id before it", "-line-1"},
		{"marker with nothing after it", "ord-1-line-"},
		{"non-numeric line number", "ord-1-line-abc"},
		{"zero line number", "ord-1-line-0"},
		{"negative line number", "ord-1-line--1"},
		{"float line number", "ord-1-line-1.5"},
		{"trailing whitespace on line number", "ord-1-line-1 "},
		{"marker only, no order id, no line number", "-line-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ParseWorkUnitID(%q) panicked: %v", tt.input, r)
				}
			}()
			orderID, lineNo, ok := usecases.ParseWorkUnitID(tt.input)
			if ok {
				t.Fatalf("ParseWorkUnitID(%q) = (%q, %d, true), want ok=false", tt.input, orderID, lineNo)
			}
			if orderID != "" || lineNo != 0 {
				t.Fatalf("ParseWorkUnitID(%q) on ok=false must zero its outputs, got (%q, %d)", tt.input, orderID, lineNo)
			}
		})
	}
}

// findOrderAllocationPartiallyFailed returns the first
// shared.OrderAllocationPartiallyFailed event published to p, failing
// the test if none was published.
func findOrderAllocationPartiallyFailed(t *testing.T, p *recordingPublisher) shared.OrderAllocationPartiallyFailed {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if f, ok := e.(shared.OrderAllocationPartiallyFailed); ok {
			return f
		}
	}
	t.Fatalf("no OrderAllocationPartiallyFailed event was published; events = %v", p.names())
	return shared.OrderAllocationPartiallyFailed{}
}

// TestReceiveOrder_BackorderEventPublishFailure_IntakeStillSucceeds
// covers allocateLines' OrderLineBackordered publish-error branch: when
// the 409 business fact cannot be published, allocation fails closed
// and ReceiveOrder's implicit attempt is best-effort — intake itself
// already succeeded, so the caller still gets an order with no error.
// (The in-memory repo stores the aggregate pointer, so the post-failure
// re-read observes the unpersisted in-memory Backordered mutation; the
// durable contract — nil error, OrderReceived as the only fact ever
// published, no panic — is what this pins.)
func TestReceiveOrder_BackorderEventPublishFailure_IntakeStillSucceeds(t *testing.T) {
	f := newFixture()
	f.inventory.reserveErrBySKU["SKU-1"] = ports.ErrInsufficientStock
	// OrderReceived (publish #1) must still fire; the backorder fact
	// (publish #2) fails.
	f.events.failAfter(1, errBoom)

	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
	if err != nil {
		t.Fatalf("Execute: %v, want nil — ReceiveOrder must not fail because its implicit allocation attempt did", err)
	}
	if o == nil || o.ID() == "" {
		t.Fatal("Execute must still return the received order")
	}
	assertEventNames(t, f.events, "OrderReceived")
}

// TestReceiveOrder_HardFailureCauseIsTruncatedToMaxCauseRunes proves
// truncateCause's bound end to end: a verbose upstream error surfacing
// through OrderAllocationPartiallyFailed.Cause is capped at
// maxCauseLen runes plus the ellipsis marker, so the visibility event
// can never carry an unbounded payload.
func TestReceiveOrder_HardFailureCauseIsTruncatedToMaxCauseRunes(t *testing.T) {
	f := newFixture()
	longCause := strings.Repeat("x", 600)
	f.inventory.reserveErrBySKU["SKU-2"] = errors.New(longCause)

	o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"),
		line("SKU-2", 1, "pick"),
	}, false)
	if err != nil {
		t.Fatalf("Execute: %v, want nil (best-effort allocation)", err)
	}

	failed := findOrderAllocationPartiallyFailed(t, f.events)
	if failed.AllocatedLines != 1 || failed.RemainingLines != 1 {
		t.Fatalf("AllocatedLines/RemainingLines = %d/%d, want 1/1", failed.AllocatedLines, failed.RemainingLines)
	}
	cause := []rune(failed.Cause)
	if len(cause) != 501 {
		t.Fatalf("Cause length = %d runes, want 501 (500 + ellipsis)", len(cause))
	}
	if string(cause[:500]) != longCause[:500] {
		t.Fatal("Cause prefix does not match the original error's first 500 runes")
	}
	if string(cause[500]) != "…" {
		t.Fatalf("Cause suffix = %q, want the truncation ellipsis", string(cause[500]))
	}

	// The genuinely reserved line is persisted, not stranded: the
	// re-read order the caller got shows the partial progress.
	stored, err := f.orders.FindByID(context.Background(), o.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v, %v", stored, err)
	}
	assertLineStatuses(t, stored, order.LineAllocated, order.LinePending)
}
