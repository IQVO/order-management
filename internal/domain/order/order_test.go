package order_test

import (
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// lineSpec is the compact description a test uses to build an order.
type lineSpec struct {
	sku      shared.SKU
	qty      int
	pathID   shared.PathId
	giftWrap bool
}

func newOrder(t *testing.T, allowPartialShipment bool, specs ...lineSpec) *order.Order {
	t.Helper()
	lines := make([]*order.OrderLine, 0, len(specs))
	for i, s := range specs {
		line, err := order.NewOrderLine(i+1, s.sku, s.qty, s.pathID, s.giftWrap)
		if err != nil {
			t.Fatalf("NewOrderLine(%d): %v", i+1, err)
		}
		lines = append(lines, line)
	}
	o, err := order.New("ord-1", lines, allowPartialShipment)
	if err != nil {
		t.Fatalf("order.New: %v", err)
	}
	return o
}

func TestNewOrderInvariants(t *testing.T) {
	validLine, err := order.NewOrderLine(1, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}

	tests := []struct {
		name    string
		id      shared.OrderId
		lines   []*order.OrderLine
		wantErr error
	}{
		{name: "valid", id: "ord-1", lines: []*order.OrderLine{validLine}},
		{name: "empty id is rejected", id: "", lines: []*order.OrderLine{validLine}, wantErr: shared.ErrEmptyOrderID},
		{name: "no lines is rejected", id: "ord-1", lines: nil, wantErr: order.ErrNoLines},
		{name: "empty line slice is rejected", id: "ord-1", lines: []*order.OrderLine{}, wantErr: order.ErrNoLines},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertNewOrderOutcome(t, tt.id, tt.lines, tt.wantErr)
		})
	}
}

// assertNewOrderOutcome pins one New() case's outcome: the exact
// validation error (and no aggregate) for a rejected intake, or the
// Received / correct-id / no-promise-yet shape for a valid one.
func assertNewOrderOutcome(t *testing.T, id shared.OrderId, lines []*order.OrderLine, wantErr error) {
	t.Helper()
	o, err := order.New(id, lines, false)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if wantErr != nil {
		if o != nil {
			t.Fatalf("expected no order on error, got %+v", o)
		}
		return
	}
	if o.ID() != id {
		t.Fatalf("ID() = %q, want %q", o.ID(), id)
	}
	if o.Status() != order.StatusReceived {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusReceived)
	}
	if o.PromiseDate() != nil {
		t.Fatalf("a received order must have no promise date")
	}
}

func TestNewOrderNumbersLinesFromOne(t *testing.T) {
	o := newOrder(t, false,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 2},
		lineSpec{sku: "SKU-3", qty: 3},
	)
	for i, line := range o.Lines() {
		if line.LineNo() != i+1 {
			t.Fatalf("line %d has LineNo() = %d, want %d", i, line.LineNo(), i+1)
		}
	}
}

// INVARIANT: cannot allocate the same line twice.
func TestAllocateRejectsAnAlreadyAllocatedLine(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})

	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("first Allocate: %v", err)
	}
	err := o.Allocate(1, "res-2")
	if !errors.Is(err, order.ErrLineAlreadyAllocated) {
		t.Fatalf("second Allocate err = %v, want %v", err, order.ErrLineAlreadyAllocated)
	}
	if got := o.Lines()[0].ReservationID(); got == nil || *got != "res-1" {
		t.Fatalf("the rejected second allocation must not overwrite the reservation id, got %v", got)
	}
}

// INVARIANT: a Backordered line may return to Allocated ONLY via RetryAllocate.
func TestBackorderedLineOnlyReturnsToAllocatedViaRetry(t *testing.T) {
	t.Run("Allocate refuses a backordered line", testAllocateRefusesBackorderedLine)
	t.Run("RetryAllocate refuses a line that is not backordered", testRetryAllocateRefusesNotBackordered)
	t.Run("RetryAllocate is the sanctioned route", testRetryAllocateIsSanctionedRoute)
}

func testAllocateRefusesBackorderedLine(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	if err := o.MarkBackordered(1); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}
	err := o.Allocate(1, "res-1")
	if !errors.Is(err, order.ErrLineNotPending) {
		t.Fatalf("Allocate err = %v, want %v", err, order.ErrLineNotPending)
	}
	if o.Lines()[0].Status() != order.LineBackordered {
		t.Fatalf("line status = %q, want %q", o.Lines()[0].Status(), order.LineBackordered)
	}
}

func testRetryAllocateRefusesNotBackordered(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	err := o.RetryAllocate(1, "res-1")
	if !errors.Is(err, order.ErrLineNotBackordered) {
		t.Fatalf("RetryAllocate on a Pending line err = %v, want %v", err, order.ErrLineNotBackordered)
	}

	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	err = o.RetryAllocate(1, "res-2")
	if !errors.Is(err, order.ErrLineNotBackordered) {
		t.Fatalf("RetryAllocate on an Allocated line err = %v, want %v", err, order.ErrLineNotBackordered)
	}
}

func testRetryAllocateIsSanctionedRoute(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	if err := o.MarkBackordered(1); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}
	if err := o.RetryAllocate(1, "res-7"); err != nil {
		t.Fatalf("RetryAllocate: %v", err)
	}
	if o.Lines()[0].Status() != order.LineAllocated {
		t.Fatalf("line status = %q, want %q", o.Lines()[0].Status(), order.LineAllocated)
	}
	if got := o.Lines()[0].ReservationID(); got == nil || *got != "res-7" {
		t.Fatalf("ReservationID() = %v, want res-7", got)
	}
}

func TestMarkBackordered(t *testing.T) {
	t.Run("a pending line becomes backordered", func(t *testing.T) {
		o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
		if err := o.MarkBackordered(1); err != nil {
			t.Fatalf("MarkBackordered: %v", err)
		}
		if o.Lines()[0].Status() != order.LineBackordered {
			t.Fatalf("status = %q", o.Lines()[0].Status())
		}
	})

	t.Run("a failed retry leaves the line backordered without error", func(t *testing.T) {
		o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
		if err := o.MarkBackordered(1); err != nil {
			t.Fatalf("first MarkBackordered: %v", err)
		}
		if err := o.MarkBackordered(1); err != nil {
			t.Fatalf("second MarkBackordered: %v", err)
		}
		if o.Lines()[0].Status() != order.LineBackordered {
			t.Fatalf("status = %q", o.Lines()[0].Status())
		}
	})

	t.Run("an allocated line cannot be backordered", func(t *testing.T) {
		o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
		if err := o.Allocate(1, "res-1"); err != nil {
			t.Fatalf("Allocate: %v", err)
		}
		err := o.MarkBackordered(1)
		if !errors.Is(err, order.ErrLineAlreadyAllocated) {
			t.Fatalf("err = %v, want %v", err, order.ErrLineAlreadyAllocated)
		}
	})

	t.Run("a released line cannot be backordered", func(t *testing.T) {
		o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
		mustAllocateAndRelease(t, o, 1)
		err := o.MarkBackordered(1)
		if !errors.Is(err, order.ErrLineNotPending) {
			t.Fatalf("err = %v, want %v", err, order.ErrLineNotPending)
		}
	})
}

// INVARIANT: cannot release a line that isn't Allocated.
func TestReleaseRejectsALineThatIsNotAllocated(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, o *order.Order)
	}{
		{name: "pending", setup: func(*testing.T, *order.Order) {}},
		{
			name: "backordered",
			setup: func(t *testing.T, o *order.Order) {
				if err := o.MarkBackordered(1); err != nil {
					t.Fatalf("MarkBackordered: %v", err)
				}
			},
		},
		{
			name: "already released",
			setup: func(t *testing.T, o *order.Order) {
				mustAllocateAndRelease(t, o, 1)
			},
		},
		{
			name: "cancelled",
			setup: func(t *testing.T, o *order.Order) {
				if err := o.Cancel(); err != nil {
					t.Fatalf("Cancel: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
			tt.setup(t, o)
			err := o.Release(1)
			if !errors.Is(err, order.ErrLineNotAllocated) {
				t.Fatalf("Release err = %v, want %v", err, order.ErrLineNotAllocated)
			}
		})
	}
}

// INVARIANT (BR6): cannot cancel once ANY line is Released.
func TestCancelIsRejectedOnceAnyLineIsReleased(t *testing.T) {
	o := newOrder(t, true,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 1},
	)
	mustAllocateAndRelease(t, o, 1)
	if err := o.Allocate(2, "res-2"); err != nil {
		t.Fatalf("Allocate line 2: %v", err)
	}

	if err := o.EnsureCancellable(); !errors.Is(err, order.ErrOrderAlreadyReleased) {
		t.Fatalf("EnsureCancellable err = %v, want %v", err, order.ErrOrderAlreadyReleased)
	}
	if err := o.Cancel(); !errors.Is(err, order.ErrOrderAlreadyReleased) {
		t.Fatalf("Cancel err = %v, want %v", err, order.ErrOrderAlreadyReleased)
	}
	if o.Lines()[1].Status() != order.LineAllocated {
		t.Fatalf("a rejected cancellation must not touch any line, line 2 = %q", o.Lines()[1].Status())
	}
	if o.Status() != order.StatusPartiallyReleased {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusPartiallyReleased)
	}
}

func TestCancelBeforeReleaseCancelsEveryLine(t *testing.T) {
	o := newOrder(t, false,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 1},
	)
	if err := o.Allocate(1, "res-1"); err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if err := o.MarkBackordered(2); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}

	if err := o.EnsureCancellable(); err != nil {
		t.Fatalf("EnsureCancellable: %v", err)
	}
	if err := o.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	for _, line := range o.Lines() {
		if line.Status() != order.LineCancelled {
			t.Fatalf("line %d = %q, want %q", line.LineNo(), line.Status(), order.LineCancelled)
		}
	}
	if o.Status() != order.StatusCancelled {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusCancelled)
	}
}

// INVARIANT (BR3): a ship-complete order releases nothing until every line
// is allocated.
func TestEnsureReleasableEnforcesShipComplete(t *testing.T) {
	tests := []struct {
		name                 string
		allowPartialShipment bool
		setup                func(t *testing.T, o *order.Order)
		wantErr              error
	}{
		{
			name: "ship-complete with a backordered line is blocked",
			setup: func(t *testing.T, o *order.Order) {
				mustAllocate(t, o, 1, "res-1")
				if err := o.MarkBackordered(2); err != nil {
					t.Fatalf("MarkBackordered: %v", err)
				}
			},
			wantErr: order.ErrShipCompleteBlocked,
		},
		{
			name: "ship-complete with a pending line is blocked",
			setup: func(t *testing.T, o *order.Order) {
				mustAllocate(t, o, 1, "res-1")
			},
			wantErr: order.ErrShipCompleteBlocked,
		},
		{
			name: "ship-complete with every line allocated is releasable",
			setup: func(t *testing.T, o *order.Order) {
				mustAllocate(t, o, 1, "res-1")
				mustAllocate(t, o, 2, "res-2")
			},
		},
		{
			name: "ship-complete with a mix of allocated and already-released lines is releasable",
			setup: func(t *testing.T, o *order.Order) {
				mustAllocateAndRelease(t, o, 1)
				mustAllocate(t, o, 2, "res-2")
			},
		},
		{
			name:                 "partial shipment releases allocated lines independently",
			allowPartialShipment: true,
			setup: func(t *testing.T, o *order.Order) {
				mustAllocate(t, o, 1, "res-1")
				if err := o.MarkBackordered(2); err != nil {
					t.Fatalf("MarkBackordered: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOrder(t, tt.allowPartialShipment,
				lineSpec{sku: "SKU-1", qty: 1},
				lineSpec{sku: "SKU-2", qty: 1},
			)
			tt.setup(t, o)
			err := o.EnsureReleasable()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("EnsureReleasable err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestLineAddressingRejectsAnUnknownLineNumber(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})

	tests := []struct {
		name string
		call func() error
	}{
		{name: "Allocate", call: func() error { return o.Allocate(99, "res-1") }},
		{name: "RetryAllocate", call: func() error { return o.RetryAllocate(99, "res-1") }},
		{name: "MarkBackordered", call: func() error { return o.MarkBackordered(99) }},
		{name: "Release", call: func() error { return o.Release(99) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, order.ErrLineNotFound) {
				t.Fatalf("err = %v, want %v", err, order.ErrLineNotFound)
			}
		})
	}
}

func TestAllocatedReservationIDs(t *testing.T) {
	o := newOrder(t, true,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 1},
		lineSpec{sku: "SKU-3", qty: 1},
	)
	mustAllocate(t, o, 1, "res-1")
	if err := o.MarkBackordered(2); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}
	mustAllocate(t, o, 3, "res-3")

	got := o.AllocatedReservationIDs()
	want := []string{"res-1", "res-3"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// A released line's reservation is no longer this context's to revoke.
	if err := o.Release(3); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := o.AllocatedReservationIDs(); len(got) != 1 || got[0] != "res-1" {
		t.Fatalf("after release got %v, want [res-1]", got)
	}
}

func TestLinesWithStatus(t *testing.T) {
	o := newOrder(t, true,
		lineSpec{sku: "SKU-1", qty: 1},
		lineSpec{sku: "SKU-2", qty: 1},
		lineSpec{sku: "SKU-3", qty: 1},
	)
	mustAllocate(t, o, 1, "res-1")
	if err := o.MarkBackordered(2); err != nil {
		t.Fatalf("MarkBackordered: %v", err)
	}

	if got := o.LinesWithStatus(order.LinePending); len(got) != 1 || got[0].LineNo() != 3 {
		t.Fatalf("pending lines = %v, want just line 3", got)
	}
	if got := o.LinesWithStatus(order.LineAllocated); len(got) != 1 || got[0].LineNo() != 1 {
		t.Fatalf("allocated lines = %v, want just line 1", got)
	}
	if got := o.LinesWithStatus(order.LineBackordered); len(got) != 1 || got[0].LineNo() != 2 {
		t.Fatalf("backordered lines = %v, want just line 2", got)
	}
	if got := o.LinesWithStatus(order.LineCancelled); len(got) != 0 {
		t.Fatalf("cancelled lines = %v, want none", got)
	}
}

func TestLinesReturnsACopyOfTheSlice(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1}, lineSpec{sku: "SKU-2", qty: 1})

	lines := o.Lines()
	lines[0] = nil

	if o.Lines()[0] == nil {
		t.Fatal("mutating the returned slice corrupted the aggregate")
	}
}

func TestPromiseDateIsCopiedOnRead(t *testing.T) {
	o := newOrder(t, false, lineSpec{sku: "SKU-1", qty: 1})
	if o.PromiseDate() != nil {
		t.Fatal("a received order must have no promise date")
	}

	set := testTime()
	o.SetPromiseDate(set)

	got := o.PromiseDate()
	if got == nil || !got.Equal(set) {
		t.Fatalf("PromiseDate() = %v, want %v", got, set)
	}
	*got = got.Add(1000)
	if again := o.PromiseDate(); !again.Equal(set) {
		t.Fatalf("aggregate state was mutated through the returned pointer: %v", again)
	}
}

func TestRehydrateRestoresPersistedState(t *testing.T) {
	promise := testTime()
	reservationID := "res-1"
	lines := []*order.OrderLine{
		order.RehydrateOrderLine(1, "SKU-1", 2, "pick", false, order.LineAllocated, &reservationID),
		order.RehydrateOrderLine(2, "SKU-2", 3, "singles", true, order.LineBackordered, nil),
	}

	cptId := "sp1-1800"
	basis := order.BasisCapability
	o := order.Rehydrate(order.OrderSnapshot{
		ID: "ord-9", Lines: lines, AllowPartialShipment: true,
		PromiseDate: &promise, PromiseCptID: &cptId, PromiseBasis: &basis,
	})

	if o.ID() != "ord-9" || !o.AllowPartialShipment() {
		t.Fatalf("rehydrated order lost detail: %+v", o)
	}
	if got := o.PromiseDate(); got == nil || !got.Equal(promise) {
		t.Fatalf("PromiseDate() = %v, want %v", got, promise)
	}
	if got := o.PromiseCptId(); got == nil || *got != cptId {
		t.Fatalf("PromiseCptId() = %v, want %v", got, cptId)
	}
	if got := o.PromiseBasis(); got == nil || *got != basis {
		t.Fatalf("PromiseBasis() = %v, want %v", got, basis)
	}
	if o.Status() != order.StatusPartiallyAllocated {
		t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusPartiallyAllocated)
	}
}

// New must not write through to the caller's lines: numbering happens on
// the aggregate's own copies, so building a second order from the same
// line values cannot renumber the first (audit 2026-10-05, F5).
func TestNewDoesNotMutateTheCallersLines(t *testing.T) {
	l, err := order.NewOrderLine(7, "SKU-1", 1, "pick", false)
	if err != nil {
		t.Fatalf("NewOrderLine: %v", err)
	}
	o, err := order.New("ord-1", []*order.OrderLine{l}, false)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if l.LineNo() != 7 {
		t.Fatalf("caller's line renumbered to %d by New, want its own 7", l.LineNo())
	}
	if got := o.Lines()[0].LineNo(); got != 1 {
		t.Fatalf("aggregate line numbered %d, want 1", got)
	}
	// And the aggregate's line is its own entity: allocating through the
	// order does not leak into the caller's pointer.
	mustAllocate(t, o, 1, "res-1")
	if l.Status() != order.LinePending || l.ReservationID() != nil {
		t.Fatalf("caller's line changed by an Order mutation: status=%s reservation=%v", l.Status(), l.ReservationID())
	}
}

func TestRehydrateSnapshotDefaultsAndPersistedFields(t *testing.T) {
	line := order.RehydrateOrderLine(1, "SKU-1", 1, "pick", false, order.LineAllocated, nil)
	lines := []*order.OrderLine{line}

	t.Run("zero-value fields are the safe defaults", func(t *testing.T) {
		o := order.Rehydrate(order.OrderSnapshot{ID: "ord-1", Lines: lines})
		if o.Version() != 1 {
			t.Errorf("Version() = %d, want 1 for a snapshot with no version", o.Version())
		}
		if !o.ReleaseOnAllocation() {
			t.Error("ReleaseOnAllocation() = false, want true (HeldAtIntake zero value)")
		}
		if o.RequiredShipBy() != nil {
			t.Errorf("RequiredShipBy() = %v, want nil", o.RequiredShipBy())
		}
		if o.AllowPartialShipment() {
			t.Error("AllowPartialShipment() = true, want false")
		}
	})

	t.Run("persisted version, hold and deadline round-trip", func(t *testing.T) {
		deadline := testTime()
		o := order.Rehydrate(order.OrderSnapshot{
			ID: "ord-2", Lines: lines, HeldAtIntake: true, Version: 7, RequiredShipBy: &deadline,
		})
		if o.Version() != 7 {
			t.Errorf("Version() = %d, want the persisted 7", o.Version())
		}
		if o.ReleaseOnAllocation() {
			t.Error("ReleaseOnAllocation() = true, want false for HeldAtIntake")
		}
		if got := o.RequiredShipBy(); got == nil || !got.Equal(deadline) {
			t.Errorf("RequiredShipBy() = %v, want %v", got, deadline)
		}
	})
}

func mustAllocate(t *testing.T, o *order.Order, lineNo int, reservationID string) {
	t.Helper()
	if err := o.Allocate(lineNo, reservationID); err != nil {
		t.Fatalf("Allocate(%d): %v", lineNo, err)
	}
}

func mustAllocateAndRelease(t *testing.T, o *order.Order, lineNo int) {
	t.Helper()
	mustAllocate(t, o, lineNo, "res-released")
	if err := o.Release(lineNo); err != nil {
		t.Fatalf("Release(%d): %v", lineNo, err)
	}
}
