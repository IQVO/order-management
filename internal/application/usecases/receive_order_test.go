package usecases_test

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

func TestReceiveOrder(t *testing.T) {
	t.Run("creates an order, publishes OrderReceived first, then attempts allocation-then-release", func(t *testing.T) {
		f := newFixture()

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 2, "pick"),
			{SKU: "SKU-2", Quantity: 1, PathID: "singles", GiftWrap: true},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}

		// Both lines allocate cleanly against the fake inventory by
		// default, so a ship-complete order proceeds straight through to
		// Released in this single call — the folded flow this redesign
		// introduces.
		if o.Status() != order.StatusReleased {
			t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusReleased)
		}
		if o.AllowPartialShipment() {
			t.Fatal("AllowPartialShipment must default to false (ship-complete, BR3)")
		}
		assertLineStatuses(t, o, order.LineReleased, order.LineReleased)

		// OrderReceived fires FIRST, unconditionally, before allocation is
		// even attempted — a caller creating an order always sees that
		// fact regardless of what allocation does next.
		names := f.events.names()
		if len(names) == 0 || names[0] != "OrderReceived" {
			t.Fatalf("events = %v, want OrderReceived first", names)
		}

		if !o.Lines()[1].GiftWrap() {
			t.Fatal("gift wrap was lost between the request and the aggregate")
		}
		// PathID is still modeled internally exactly as before — this
		// redesign only removed the caller's ability to SET it on public
		// intake, not the domain/application layers' PathID field itself.
		if o.Lines()[0].PathID() != "pick" || o.Lines()[1].PathID() != "singles" {
			t.Fatalf("path ids were lost: %q, %q", o.Lines()[0].PathID(), o.Lines()[1].PathID())
		}

		// The order must be readable straight back out of the repo.
		stored, err := f.getOrder().Execute(context.Background(), o.ID())
		if err != nil {
			t.Fatalf("GetOrder: %v", err)
		}
		if stored.ID() != o.ID() {
			t.Fatalf("stored id = %q, want %q", stored.ID(), o.ID())
		}
	})

	t.Run("stamps the order's computed FulfillmentClass onto every released line", func(t *testing.T) {
		f := newFixture()

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 2, "pick"),
			{SKU: "SKU-2", Quantity: 1, PathID: "singles", GiftWrap: true},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		wantClass := o.FulfillmentClass().String()
		if wantClass != string(order.ClassMultiLineMulti) {
			t.Fatalf("test setup: want a MultiLineMulti order, got %q", wantClass)
		}

		allocated := findOrderAllocated(t, f.events)
		if len(allocated.Lines) != 2 {
			t.Fatalf("OrderAllocated.Lines = %v, want 2 entries", allocated.Lines)
		}
		for _, l := range allocated.Lines {
			if l.FulfillmentClass != wantClass {
				t.Errorf("released line %+v has FulfillmentClass %q, want %q", l, l.FulfillmentClass, wantClass)
			}
		}
	})

	t.Run("a backordered line keeps a ship-complete order held back from release", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Status() != order.StatusBackordered {
			t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusBackordered)
		}
		assertLineStatuses(t, o, order.LineAllocated, order.LineBackordered)
		assertEventNames(t, f.events, "OrderReceived", "OrderLineAllocated", "OrderLineBackordered")
	})

	t.Run("partial shipment releases what allocates and leaves the rest backordered", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
		}, true)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Status() != order.StatusPartiallyReleased {
			t.Fatalf("Status() = %q, want %q", o.Status(), order.StatusPartiallyReleased)
		}
		assertLineStatuses(t, o, order.LineReleased, order.LineBackordered)
		assertEventNames(t, f.events, "OrderReceived", "OrderLineAllocated", "OrderLineBackordered", "OrderPartiallyAllocated")
	})

	// This is the DESIGN DECISION documented on ReceiveOrder: a hard
	// (non-business) failure during the implicit allocation pass must
	// never turn ReceiveOrder itself into a failure — the order was
	// genuinely created and persisted before allocation was ever
	// attempted.
	t.Run("a hard failure during the implicit allocation pass does not fail ReceiveOrder", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErrBySKU["SKU-2"] = errBoom

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
		}, true)
		if err != nil {
			t.Fatalf("Execute: want no error (the order WAS received), got %v", err)
		}
		if o == nil {
			t.Fatal("Execute: want the received order, got nil")
		}
		// Line 1 allocated before line 2's hard failure halted the pass.
		// allocateAndRelease only attempts release after allocateLines
		// completes CLEANLY (allocErr == nil) — a hard mid-pass failure
		// short-circuits before release ever runs, so line 1 stays
		// Allocated (not yet Released) and line 2, never past Reserve,
		// stays Pending.
		assertLineStatuses(t, o, order.LineAllocated, order.LinePending)

		// The visibility trail: OrderReceived unconditionally, then the
		// line-1 progress, then the fail-closed OrderAllocationPartiallyFailed
		// event — never silently swallowed.
		names := f.events.names()
		if len(names) != 3 || names[0] != "OrderReceived" || names[1] != "OrderLineAllocated" || names[2] != "OrderAllocationPartiallyFailed" {
			t.Fatalf("events = %v, want [OrderReceived OrderLineAllocated OrderAllocationPartiallyFailed]", names)
		}

		// What is returned matches what is actually persisted.
		stored, getErr := f.getOrder().Execute(context.Background(), o.ID())
		if getErr != nil {
			t.Fatalf("GetOrder: %v", getErr)
		}
		assertLineStatuses(t, stored, order.LineAllocated, order.LinePending)
	})

	// The zero-progress case: the very first line's reserve call hard-fails,
	// so nothing was ever allocated. ReceiveOrder still succeeds and every
	// line is left Pending — exactly the state a freshly-received order
	// would have had before this redesign, ready for RetryAllocation-style
	// recovery... except RetryAllocation only targets Backordered lines, so
	// a fully-Pending order after a zero-progress hard failure has no
	// automatic recovery path; it is a known, documented v1 gap noted in
	// the type comment (no dedicated "retry a still-Pending line" command
	// exists) — a human/operator would need direct intervention. This is
	// unchanged from the ORIGINAL AllocateOrder's failure semantics: it
	// never had that recovery path either.
	t.Run("a zero-progress hard failure leaves every line Pending with no partial-failure event", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErr = errBoom

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "pick"),
		}, false)
		if err != nil {
			t.Fatalf("Execute: want no error, got %v", err)
		}
		assertLineStatuses(t, o, order.LinePending)
		// No OrderAllocationPartiallyFailed: that event only fires when
		// something was genuinely allocated before the failure.
		assertEventNames(t, f.events, "OrderReceived")
	})

	t.Run("nothing is reserved or released for invalid lines", func(t *testing.T) {
		tests := []struct {
			name    string
			lines   []usecases.NewLine
			wantErr error
		}{
			{
				name:    "empty sku",
				lines:   []usecases.NewLine{{SKU: "", Quantity: 1, PathID: "pick"}},
				wantErr: shared.ErrEmptySKU,
			},
			{
				name:    "zero quantity",
				lines:   []usecases.NewLine{{SKU: "SKU-1", Quantity: 0, PathID: "pick"}},
				wantErr: shared.ErrNonPositiveQuantity,
			},
			{
				name:    "negative quantity on a later line",
				lines:   []usecases.NewLine{line("SKU-1", 1, "pick"), {SKU: "SKU-2", Quantity: -1}},
				wantErr: shared.ErrNonPositiveQuantity,
			},
			{
				name:    "no lines at all",
				lines:   nil,
				wantErr: order.ErrNoLines,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				f := newFixture()
				_, err := f.receiveOrder().Execute(context.Background(), tt.lines, false)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				assertEventNames(t, f.events)
				if len(f.inventory.reserveCalls) != 0 {
					t.Fatalf("an invalid intake must never reach inventory-storage, calls = %d", len(f.inventory.reserveCalls))
				}
			})
		}
	})

	t.Run("propagates repository failures during intake itself", func(t *testing.T) {
		tests := []struct {
			name string
			repo *failingRepo
		}{
			{name: "NextID fails", repo: &failingRepo{nextIDErr: errBoom}},
			{name: "Save fails", repo: &failingRepo{saveErr: errBoom}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				f := newFixture()
				tt.repo.inner = f.orders
				f.orders = tt.repo

				_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
				if !errors.Is(err, errBoom) {
					t.Fatalf("err = %v, want %v", err, errBoom)
				}
			})
		}
	})

	t.Run("propagates a publisher failure on OrderReceived itself", func(t *testing.T) {
		f := newFixture()
		f.events.err = errBoom

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})

	// PathSelectionPolicy + ports.ProcessPathCatalogue: order-management
	// ADR-0013. A caller never supplies pathId on public intake, so these
	// tests exercise NewLine.PathID left empty (the HTTP adapter's real
	// behavior) to prove the use case's PathPolicy actually resolves it,
	// and that an inactive/unknown resolved path is rejected BEFORE the
	// order is ever persisted — the whole point of this ADR.
	t.Run("an empty PathID is resolved by PathPolicy, not left blank", func(t *testing.T) {
		f := newFixture()

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Lines()[0].PathID() != shared.DefaultPathId {
			t.Fatalf("line PathID = %q, want %q (PathPolicy's v1 default)", o.Lines()[0].PathID(), shared.DefaultPathId)
		}
	})

	t.Run("a resolved path that is not active in the catalogue is rejected before persisting", func(t *testing.T) {
		f := newFixture()
		f.catalogue.inactive[shared.DefaultPathId] = true

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if !errors.Is(err, shared.ErrUnknownProcessPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrUnknownProcessPath)
		}
		assertEventNames(t, f.events) // OrderReceived must NOT have fired
		if len(f.inventory.reserveCalls) != 0 {
			t.Fatalf("an order rejected for an unknown path must never reach inventory-storage, calls = %d", len(f.inventory.reserveCalls))
		}
	})

	t.Run("a nil Catalogue skips validation entirely (not-yet-wired default)", func(t *testing.T) {
		f := newFixture()
		uc := f.receiveOrder()
		uc.Catalogue = nil

		o, err := uc.Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Lines()[0].PathID() != shared.DefaultPathId {
			t.Fatalf("line PathID = %q, want %q", o.Lines()[0].PathID(), shared.DefaultPathId)
		}
	})

	t.Run("an explicitly supplied PathID still goes through catalogue validation", func(t *testing.T) {
		f := newFixture()
		f.catalogue.inactive["singles"] = true

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "singles"),
		}, false)
		if !errors.Is(err, shared.ErrUnknownProcessPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrUnknownProcessPath)
		}
	})

	// ADR-0016 (ADR-0014 step B): PathPolicy.Select now actually
	// evaluates the default path's declared Eligibility against the
	// line, rather than ignoring every input. These tests exercise that
	// through ReceiveOrder end to end -- the resolved PathId, the
	// classification lookup, and the eligibility rejection.
	t.Run("a line's quantity within MaxUnitsPerLine resolves normally", func(t *testing.T) {
		f := newFixture()
		maxOne := 1
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(&maxOne, nil, nil, false)

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Lines()[0].PathID() != shared.DefaultPathId {
			t.Fatalf("line PathID = %q, want %q", o.Lines()[0].PathID(), shared.DefaultPathId)
		}
	})

	t.Run("a quantity exceeding MaxUnitsPerLine is rejected before persisting", func(t *testing.T) {
		f := newFixture()
		maxOne := 1
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(&maxOne, nil, nil, false)

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 5},
		}, false)
		if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrLineIneligibleForResolvedPath)
		}
		assertEventNames(t, f.events) // OrderReceived must NOT have fired
		if len(f.inventory.reserveCalls) != 0 {
			t.Fatalf("an order rejected for ineligibility must never reach inventory-storage, calls = %d", len(f.inventory.reserveCalls))
		}
	})

	t.Run("a line missing a required product attribute is rejected", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, []string{"hazmat"}, nil, false)

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrLineIneligibleForResolvedPath)
		}
	})

	t.Run("a classification lookup supplying the required attribute makes the line eligible", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, []string{"Hazmat"}, nil, false)
		f.classification.tags["SKU-1"] = []string{"Hazmat"}

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Lines()[0].PathID() != shared.DefaultPathId {
			t.Fatalf("line PathID = %q, want %q", o.Lines()[0].PathID(), shared.DefaultPathId)
		}
	})

	t.Run("an excluded product attribute rejects the line even when the path is active", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, nil, []string{"Hazmat"}, false)
		f.classification.tags["SKU-1"] = []string{"Hazmat"}

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrLineIneligibleForResolvedPath)
		}
	})

	t.Run("gift wrap is folded into the evaluated attributes and can be excluded", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, nil, []string{"giftWrap"}, false)

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1, GiftWrap: true},
		}, false)
		if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
			t.Fatalf("err = %v, want %v", err, shared.ErrLineIneligibleForResolvedPath)
		}
	})

	t.Run("a classification lookup failure fails open — the line is still routed", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, []string{"hazmat"}, nil, false)
		f.classification.err = errBoom

		// Classification is unreachable, so no attribute is derived —
		// the required "hazmat" attribute is therefore missing and the
		// line IS still rejected. This test's point is that the
		// rejection comes from the eligibility rule, not from
		// propagating the lookup's own transport error (which must
		// never surface to ReceiveOrder's caller as errBoom).
		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
			t.Fatalf("err = %v, want %v (never the raw lookup failure)", err, shared.ErrLineIneligibleForResolvedPath)
		}
		if errors.Is(err, errBoom) {
			t.Fatalf("a classification lookup failure must never surface as errBoom, got %v", err)
		}
	})

	t.Run("a nil Classification is treated exactly like an unclassified SKU", func(t *testing.T) {
		f := newFixture()
		f.catalogue.eligibility[shared.DefaultPathId] = shared.NewEligibility(nil, nil, nil, false)
		uc := f.receiveOrder()
		uc.Classification = nil

		o, err := uc.Execute(context.Background(), []usecases.NewLine{
			{SKU: "SKU-1", Quantity: 1},
		}, false)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if o.Lines()[0].PathID() != shared.DefaultPathId {
			t.Fatalf("line PathID = %q, want %q", o.Lines()[0].PathID(), shared.DefaultPathId)
		}
	})

	// A publisher failure LATER in the implicit allocation-then-release
	// pass (e.g. on the final OrderAllocated event, after OrderReceived
	// and OrderLineAllocated already succeeded) is a hard failure of
	// allocateAndRelease, which ReceiveOrder swallows exactly like any
	// other hard allocation-phase failure — the order was genuinely
	// received and that is what matters to ReceiveOrder's own caller.
	t.Run("a publisher failure later in the implicit allocation pass does not fail ReceiveOrder", func(t *testing.T) {
		f := newFixture()
		f.events.failAfter(2, errBoom) // let OrderReceived + OrderLineAllocated through, then fail

		o, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
		if err != nil {
			t.Fatalf("Execute: want no error (the order WAS received), got %v", err)
		}
		if o == nil {
			t.Fatal("Execute: want the received order, got nil")
		}
	})

	// After a hard allocation failure, ReceiveOrder re-reads the persisted
	// order so what it returns never diverges from what is actually
	// stored (see the type doc). If THAT re-read itself fails, that is a
	// genuine infrastructure problem and must surface as an error — this
	// is the one way ReceiveOrder's implicit allocation attempt CAN still
	// produce a ReceiveOrder-level error, and it is not about allocation
	// failing, it is about being unable to report reality afterward.
	t.Run("a FindByID failure while re-reading after a hard allocation failure surfaces as an error", func(t *testing.T) {
		f := newFixture()
		f.inventory.reserveErrBySKU["SKU-2"] = errBoom
		f.orders = &findFails{inner: f.orders, findErr: errBoom}

		_, err := f.receiveOrder().Execute(context.Background(), []usecases.NewLine{
			line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
		}, true)
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})
}

func TestGetOrder(t *testing.T) {
	t.Run("returns the stored order", func(t *testing.T) {
		f := newFixture()
		o := f.mustReceive(t, true, line("SKU-1", 1, "pick"))

		got, err := f.getOrder().Execute(context.Background(), o.ID())
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if got.ID() != o.ID() || !got.AllowPartialShipment() {
			t.Fatalf("got %+v, want the received order", got)
		}
	})

	t.Run("an unknown id is ErrOrderNotFound", func(t *testing.T) {
		f := newFixture()
		_, err := f.getOrder().Execute(context.Background(), "ord-missing")
		if !errors.Is(err, usecases.ErrOrderNotFound) {
			t.Fatalf("err = %v, want %v", err, usecases.ErrOrderNotFound)
		}
	})

	t.Run("propagates repository failures", func(t *testing.T) {
		f := newFixture()
		f.orders = &failingRepo{inner: f.orders, findErr: errBoom}

		_, err := f.getOrder().Execute(context.Background(), "ord-1")
		if !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want %v", err, errBoom)
		}
	})
}

// TestReceiveOrder_Metrics_AcceptedOnSuccess proves the accepted
// outcome is recorded exactly once when intake genuinely succeeds —
// even though the implicit best-effort allocation attempt runs inside
// the same call afterwards.
func TestReceiveOrder_Metrics_AcceptedOnSuccess(t *testing.T) {
	f := newFixture()
	metrics := &countingMetrics{}
	uc := f.receiveOrder()
	uc.Metrics = metrics

	o, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if o == nil {
		t.Fatal("Execute returned no order")
	}
	if metrics.accepted != 1 || metrics.rejected != 0 {
		t.Fatalf("accepted=%d rejected=%d, want accepted=1 rejected=0", metrics.accepted, metrics.rejected)
	}
}

// TestReceiveOrder_Metrics_RejectedOnEveryRejectionPath proves every
// caller-facing input rejection records the rejected outcome exactly
// once, and — per ADR 0020 §4 — that a rejected intake leaves no trace.
func TestReceiveOrder_Metrics_RejectedOnEveryRejectionPath(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, uc *usecases.ReceiveOrder)
	}{
		{
			name: "contradictory hold intent (held + partial shipment)",
			run: func(t *testing.T, uc *usecases.ReceiveOrder) {
				_, err := uc.ExecuteHeld(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, true, false)
				if !errors.Is(err, order.ErrHeldOrderMustBeShipComplete) {
					t.Fatalf("ExecuteHeld err = %v, want ErrHeldOrderMustBeShipComplete", err)
				}
			},
		},
		{
			name: "invalid line (zero quantity)",
			run: func(t *testing.T, uc *usecases.ReceiveOrder) {
				_, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 0, "pick")}, false)
				if err == nil {
					t.Fatal("Execute err = nil, want a line-validation error")
				}
			},
		},
		{
			name: "no active path admits the line",
			run: func(t *testing.T, uc *usecases.ReceiveOrder) {
				// Empty PathID forces PathSelectionPolicy.Select over
				// the catalogue's active candidates. Script a singles
				// path capped at one unit and ask for two: the only
				// candidate exists but its Eligibility rejects the
				// line, which is the true ok=false case (an empty
				// catalogue fails OPEN to DefaultPathId instead).
				maxOne := 1
				uc.Catalogue = &fakeCatalogue{
					inactive:    map[shared.PathId]bool{},
					eligibility: map[shared.PathId]shared.Eligibility{"singles": shared.NewEligibility(&maxOne, nil, nil, false)},
				}
				_, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 2, "")}, false)
				if !errors.Is(err, shared.ErrLineIneligibleForResolvedPath) {
					t.Fatalf("Execute err = %v, want ErrLineIneligibleForResolvedPath", err)
				}
			},
		},
		{
			name: "explicit path is not active in the catalogue",
			run: func(t *testing.T, uc *usecases.ReceiveOrder) {
				uc.Catalogue = &fakeCatalogue{inactive: map[shared.PathId]bool{"pick": true}}
				_, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false)
				if !errors.Is(err, shared.ErrUnknownProcessPath) {
					t.Fatalf("Execute err = %v, want ErrUnknownProcessPath", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture()
			metrics := &countingMetrics{}
			uc := f.receiveOrder()
			uc.Metrics = metrics

			tt.run(t, uc)

			if metrics.accepted != 0 || metrics.rejected != 1 {
				t.Fatalf("accepted=%d rejected=%d, want accepted=0 rejected=1", metrics.accepted, metrics.rejected)
			}
			// A rejected intake must leave no trace: no order persisted,
			// no event published.
			if n := len(f.events.names()); n != 0 {
				t.Fatalf("events = %v, want none for a rejected intake", f.events.names())
			}
		})
	}
}

// TestReceiveOrder_Metrics_NilIsNotInstrumented_NeverPanics pins the
// optional-port convention: a nil Metrics means "not instrumented", and
// both the accepted and rejected recording guards must tolerate it.
func TestReceiveOrder_Metrics_NilIsNotInstrumented_NeverPanics(t *testing.T) {
	f := newFixture()
	uc := f.receiveOrder() // Metrics deliberately left nil

	if _, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 1, "pick")}, false); err != nil {
		t.Fatalf("Execute (accepted path, nil Metrics): %v", err)
	}
	if _, err := uc.Execute(context.Background(), []usecases.NewLine{line("SKU-1", 0, "pick")}, false); err == nil {
		t.Fatal("Execute (rejected path): err = nil, want a validation error")
	}
}
