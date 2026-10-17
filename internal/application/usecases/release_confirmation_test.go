package usecases_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// confirmationKey identifies one process-manager row.
type confirmationKey struct {
	orderID shared.OrderId
	lineNo  int
}

// confirmationStore is ONE in-memory stand-in for the release-confirmation
// table, its idempotency gate and the UnitOfWork that brackets them, with
// real rollback (a failed Execute restores both) -- the same shape as the
// planned-capacity store used by that consumer's tests. It records every
// call made OUTSIDE an Execute scope, so a test can assert that a write
// joined the use case's unit of work.
type confirmationStore struct {
	mu      sync.Mutex
	rows    map[confirmationKey]releaseconfirmation.Confirmation
	claimed map[string]bool

	depth        int
	executes     int
	outsideScope []string
	openCalls    int
	claimCalls   int
	lastCutoff   time.Time

	openErr  error
	findErr  error
	saveErr  error
	claimErr error
}

func newConfirmationStore() *confirmationStore {
	return &confirmationStore{
		rows:    map[confirmationKey]releaseconfirmation.Confirmation{},
		claimed: map[string]bool{},
	}
}

func (s *confirmationStore) Execute(ctx context.Context, fn func(context.Context) error) error {
	s.mu.Lock()
	s.executes++
	rowSnap := make(map[confirmationKey]releaseconfirmation.Confirmation, len(s.rows))
	for k, v := range s.rows {
		rowSnap[k] = v
	}
	claimSnap := make(map[string]bool, len(s.claimed))
	for k, v := range s.claimed {
		claimSnap[k] = v
	}
	s.depth++
	s.mu.Unlock()

	err := fn(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.depth--
	if err != nil {
		s.rows, s.claimed = rowSnap, claimSnap
	}
	return err
}

// scope records a write issued outside any unit of work. Callers hold s.mu.
func (s *confirmationStore) scope(op string) {
	if s.depth == 0 {
		s.outsideScope = append(s.outsideScope, op)
	}
}

func (s *confirmationStore) Open(_ context.Context, c releaseconfirmation.Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scope("Open")
	s.openCalls++
	if s.openErr != nil {
		return s.openErr
	}
	k := confirmationKey{c.OrderID(), c.LineNo()}
	if _, exists := s.rows[k]; !exists {
		s.rows[k] = c
	}
	return nil
}

func (s *confirmationStore) Find(_ context.Context, orderID shared.OrderId, lineNo int) (releaseconfirmation.Confirmation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findErr != nil {
		return releaseconfirmation.Confirmation{}, false, s.findErr
	}
	c, ok := s.rows[confirmationKey{orderID, lineNo}]
	return c, ok, nil
}

func (s *confirmationStore) Save(_ context.Context, c releaseconfirmation.Confirmation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scope("Save")
	if s.saveErr != nil {
		return s.saveErr
	}
	s.rows[confirmationKey{c.OrderID(), c.LineNo()}] = c
	return nil
}

func (s *confirmationStore) ClaimOverdue(_ context.Context, releasedBefore time.Time, limit int) ([]releaseconfirmation.Confirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scope("ClaimOverdue")
	s.claimCalls++
	s.lastCutoff = releasedBefore
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	var out []releaseconfirmation.Confirmation
	for _, c := range s.rows {
		if c.Overdue(releasedBefore) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ReleasedAt().Equal(out[j].ReleasedAt()) {
			return out[i].ReleasedAt().Before(out[j].ReleasedAt())
		}
		if out[i].OrderID() != out[j].OrderID() {
			return out[i].OrderID() < out[j].OrderID()
		}
		return out[i].LineNo() < out[j].LineNo()
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *confirmationStore) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scope("MarkProcessed")
	if s.claimed[eventID] {
		return false, nil
	}
	s.claimed[eventID] = true
	return true, nil
}

func (s *confirmationStore) row(orderID shared.OrderId, lineNo int) (releaseconfirmation.Confirmation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.rows[confirmationKey{orderID, lineNo}]
	return c, ok
}

func (s *confirmationStore) put(c releaseconfirmation.Confirmation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[confirmationKey{c.OrderID(), c.LineNo()}] = c
}

func (s *confirmationStore) rowCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

func (s *confirmationStore) isClaimed(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimed[id]
}

var (
	_ ports.ReleaseConfirmations               = (*confirmationStore)(nil)
	_ ports.ReleaseConfirmationProcessedEvents = (*confirmationStore)(nil)
	_ ports.UnitOfWork                         = (*confirmationStore)(nil)
)

// confirmationMetrics counts what the use cases report.
type confirmationMetrics struct {
	mu          sync.Mutex
	outcomes    []ports.ReleaseConfirmationOutcome
	unconfirmed int
}

func (m *confirmationMetrics) Handled(_ context.Context, outcome ports.ReleaseConfirmationOutcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outcomes = append(m.outcomes, outcome)
}

func (m *confirmationMetrics) LineUnconfirmed(context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unconfirmed++
}

func (m *confirmationMetrics) recorded() []ports.ReleaseConfirmationOutcome {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ports.ReleaseConfirmationOutcome(nil), m.outcomes...)
}

var _ ports.ReleaseConfirmationMetrics = (*confirmationMetrics)(nil)

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var confirmT0 = time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)

const confirmTimeout = 15 * time.Minute

func pendingRow(t *testing.T, orderID shared.OrderId, lineNo int, releasedAt time.Time) releaseconfirmation.Confirmation {
	t.Helper()
	c, err := releaseconfirmation.NewPending(orderID, lineNo, usecases.WorkUnitID(orderID, lineNo), "pick", releasedAt)
	if err != nil {
		t.Fatalf("NewPending: %v", err)
	}
	return c
}

// ---------------------------------------------------------------------
// The PENDING row is opened in the release's own unit of work.
// ---------------------------------------------------------------------

func receiveWithConfirmations(f *fixture, store *confirmationStore) *usecases.ReceiveOrder {
	uc := f.receiveOrder()
	uc.UnitOfWork = store
	uc.ReleaseConfirmations = store
	return uc
}

func TestReceiveOrder_Release_OpensAPendingConfirmationPerReleasedLineInTheReleaseUnitOfWork(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	o, err := receiveWithConfirmations(f, store).Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 2, "singles"),
	}, false)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	assertLineStatuses(t, o, order.LineReleased, order.LineReleased)

	if got := store.rowCount(); got != 2 {
		t.Fatalf("confirmation rows = %d, want one per released line (2)", got)
	}
	for i, l := range o.Lines() {
		c, ok := store.row(o.ID(), l.LineNo())
		if !ok {
			t.Fatalf("no row for line %d", l.LineNo())
		}
		if c.State() != releaseconfirmation.StatePending {
			t.Fatalf("line %d State = %q, want PENDING", l.LineNo(), c.State())
		}
		if want := usecases.WorkUnitID(o.ID(), l.LineNo()); c.WorkUnitID() != want {
			t.Fatalf("line %d WorkUnitID = %q, want %q", l.LineNo(), c.WorkUnitID(), want)
		}
		if c.PathID() != o.Lines()[i].PathID() {
			t.Fatalf("line %d PathID = %q, want the line's own path %q", l.LineNo(), c.PathID(), o.Lines()[i].PathID())
		}
		if !c.ReleasedAt().Equal(now()) {
			t.Fatalf("line %d ReleasedAt = %v, want the release instant %v", l.LineNo(), c.ReleasedAt(), now())
		}
	}
	// Same unit of work: the rows were written inside Execute, never beside it.
	if len(store.outsideScope) != 0 {
		t.Fatalf("writes issued outside the unit of work: %v", store.outsideScope)
	}
	if store.executes == 0 {
		t.Fatal("the release ran without opening a unit of work")
	}
}

func TestReceiveOrder_Release_ARollbackAfterOpeningTakesThePendingRowsWithIt(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	// OrderReceived, LineAllocated x2, OrderAllocated, OrderLineReleased x2
	// publish; the 7th event (OrderReleased) fails and rolls the whole
	// allocate-and-release scope back.
	f.events.failAfter(6, errBoom)

	if _, err := receiveWithConfirmations(f, store).Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, false); err != nil {
		t.Fatalf("ReceiveOrder must still succeed (best-effort allocation leg): %v", err)
	}
	if store.openCalls != 2 {
		t.Fatalf("Open calls = %d, want 2: the rows must be opened before the pass's last publish for this test to prove rollback", store.openCalls)
	}
	if got := store.rowCount(); got != 0 {
		t.Fatalf("confirmation rows after a rolled-back release = %d, want 0 (they commit with the release or not at all)", got)
	}
}

func TestReceiveOrder_Release_AFailedOpenFailsTheReleaseScope(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	store.openErr = errBoom
	retry := &usecases.RetryAllocation{
		Orders: f.orders, Inventory: f.inventory, Events: f.events, Clock: f.clock, Promise: f.promise,
		UnitOfWork: store, ReleaseConfirmations: store,
	}
	f.inventory.reserveErrBySKU["SKU-1"] = ports.ErrInsufficientStock
	o := f.mustReceive(t, true, line("SKU-1", 1, "pick"))
	delete(f.inventory.reserveErrBySKU, "SKU-1")

	if _, err := retry.Execute(context.Background(), o.ID()); !errors.Is(err, errBoom) {
		t.Fatalf("RetryAllocation = %v, want the Open failure to surface (a release without its row would be silent)", err)
	}
}

func TestReceiveOrder_Blocked_OpensNoConfirmation(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o, err := receiveWithConfirmations(f, store).Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, false)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	assertLineStatuses(t, o, order.LineAllocated, order.LineBackordered)
	if store.openCalls != 0 || store.rowCount() != 0 {
		t.Fatalf("a BR3-blocked order released nothing, but %d Open calls / %d rows were made", store.openCalls, store.rowCount())
	}
}

func TestReleaseHeldOrder_OpensConfirmationsOnlyWhenTheHoldIsReleased(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	o, err := receiveWithConfirmations(f, store).ExecuteHeld(context.Background(), []usecases.NewLine{
		{SKU: "SKU-1", Quantity: 2, PathID: "pick"},
	}, false, false)
	if err != nil {
		t.Fatalf("ExecuteHeld: %v", err)
	}
	if store.rowCount() != 0 {
		t.Fatalf("a held order put nothing on the floor, but %d rows were opened", store.rowCount())
	}

	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock, Inventory: f.inventory, Promise: f.promise,
		UnitOfWork: store, ReleaseConfirmations: store,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("ReleaseHeldOrder: %v", err)
	}
	c, ok := store.row(o.ID(), 1)
	if !ok || c.State() != releaseconfirmation.StatePending {
		t.Fatalf("row after the hold is released = %+v ok=%v, want PENDING", c, ok)
	}
	if len(store.outsideScope) != 0 {
		t.Fatalf("writes issued outside the unit of work: %v", store.outsideScope)
	}

	// A repeated release is the existing silent no-op: no second row.
	opens := store.openCalls
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("second ReleaseHeldOrder: %v", err)
	}
	if store.openCalls != opens {
		t.Fatalf("a repeated release opened %d more rows, want 0", store.openCalls-opens)
	}
}

func TestReleaseHeldOrder_LostReservationAtReconfirmOpensNoConfirmation(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	o, err := receiveWithConfirmations(f, store).ExecuteHeld(context.Background(), []usecases.NewLine{
		{SKU: "SKU-1", Quantity: 2, PathID: "pick"},
	}, false, false)
	if err != nil {
		t.Fatalf("ExecuteHeld: %v", err)
	}
	f.inventory.reserveErr = ports.ErrInsufficientStock

	uc := &usecases.ReleaseHeldOrder{
		Orders: f.orders, Events: f.events, Clock: f.clock, Inventory: f.inventory, Promise: f.promise,
		UnitOfWork: store, ReleaseConfirmations: store,
	}
	if _, err := uc.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("ReleaseHeldOrder: %v", err)
	}
	if store.openCalls != 0 {
		t.Fatalf("nothing was released, but %d rows were opened", store.openCalls)
	}
}

func TestRetryAllocation_OpensConfirmationsOnlyForTheLinesReleasedInThatPass(t *testing.T) {
	f := newFixture()
	store := newConfirmationStore()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o, err := receiveWithConfirmations(f, store).Execute(context.Background(), []usecases.NewLine{
		line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"),
	}, true)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	assertLineStatuses(t, o, order.LineReleased, order.LineBackordered)
	if _, ok := store.row(o.ID(), 1); !ok || store.rowCount() != 1 {
		t.Fatalf("after the first pass want exactly line 1 opened, got %d rows", store.rowCount())
	}
	first, _ := store.row(o.ID(), 1)

	delete(f.inventory.reserveErrBySKU, "SKU-2")
	f.clock.Advance(time.Minute)
	retry := &usecases.RetryAllocation{
		Orders: f.orders, Inventory: f.inventory, Events: f.events, Clock: f.clock, Promise: f.promise,
		UnitOfWork: store, ReleaseConfirmations: store,
	}
	if _, err := retry.Execute(context.Background(), o.ID()); err != nil {
		t.Fatalf("RetryAllocation: %v", err)
	}
	if store.rowCount() != 2 || store.openCalls != 2 {
		t.Fatalf("rows=%d openCalls=%d, want 2 and 2: line 1 must not be opened again by line 2's pass", store.rowCount(), store.openCalls)
	}
	second, ok := store.row(o.ID(), 2)
	if !ok || !second.ReleasedAt().Equal(now().Add(time.Minute)) {
		t.Fatalf("line 2 row = %+v ok=%v, want released at the retry's instant", second, ok)
	}
	if again, _ := store.row(o.ID(), 1); !again.ReleasedAt().Equal(first.ReleasedAt()) {
		t.Fatal("line 1's row was rewritten by the retry")
	}
}

// ---------------------------------------------------------------------
// ConfirmRelease: WorkUnitCreated confirms a PENDING row.
// ---------------------------------------------------------------------

type confirmFixture struct {
	store   *confirmationStore
	metrics *confirmationMetrics
	clock   *memory.FixedClock
	uc      *usecases.ConfirmRelease
	logs    *lockedBuffer
}

func newConfirmFixture() *confirmFixture {
	store := newConfirmationStore()
	metrics := &confirmationMetrics{}
	clock := memory.NewFixedClock(confirmT0.Add(2 * time.Second))
	logs := &lockedBuffer{}
	return &confirmFixture{
		store: store, metrics: metrics, clock: clock, logs: logs,
		uc: &usecases.ConfirmRelease{
			Confirmations: store, Processed: store, Clock: clock, UnitOfWork: store, Metrics: metrics,
			Logger: slog.New(slog.NewTextHandler(logs, nil)),
		},
	}
}

func (f *confirmFixture) confirm(t *testing.T, eventID string, orderID shared.OrderId, lineNo int) ports.ReleaseConfirmationOutcome {
	t.Helper()
	outcome, err := f.uc.Execute(context.Background(), usecases.ConfirmReleaseRequest{
		EventID: eventID, Reference: orderID.String(), LineNo: lineNo,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return outcome
}

func TestConfirmRelease_PendingBecomesConfirmed(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 2, confirmT0))

	if got := f.confirm(t, "evt-1", "ord-1", 2); got != ports.ReleaseConfirmationConfirmed {
		t.Fatalf("outcome = %q, want confirmed", got)
	}
	c, _ := f.store.row("ord-1", 2)
	if c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
	if at := c.ConfirmedAt(); at == nil || !at.Equal(f.clock.Now()) {
		t.Fatalf("ConfirmedAt = %v, want the clock's now %v", at, f.clock.Now())
	}
	if !f.store.isClaimed("evt-1") {
		t.Fatal("the CloudEvents id must be claimed with the state change")
	}
	if got := f.metrics.recorded(); len(got) != 1 || got[0] != ports.ReleaseConfirmationConfirmed {
		t.Fatalf("metrics = %v, want [confirmed]", got)
	}
	if len(f.store.outsideScope) != 0 {
		t.Fatalf("writes issued outside the unit of work: %v", f.store.outsideScope)
	}
}

func TestConfirmRelease_OnlyTheMatchingLineIsConfirmed(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
	f.store.put(pendingRow(t, "ord-1", 2, confirmT0))
	f.store.put(pendingRow(t, "ord-2", 1, confirmT0))

	f.confirm(t, "evt-1", "ord-1", 2)

	for _, other := range []confirmationKey{{"ord-1", 1}, {"ord-2", 1}} {
		if c, _ := f.store.row(other.orderID, other.lineNo); c.State() != releaseconfirmation.StatePending {
			t.Fatalf("%s line %d State = %q, want it left PENDING", other.orderID, other.lineNo, c.State())
		}
	}
}

func TestConfirmRelease_ReplayedEventIDIsADuplicateNoOp(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
	f.confirm(t, "evt-1", "ord-1", 1)
	firstAt := *mustRow(t, f.store, "ord-1", 1).ConfirmedAt()

	f.clock.Advance(time.Hour)
	if got := f.confirm(t, "evt-1", "ord-1", 1); got != ports.ReleaseConfirmationDuplicate {
		t.Fatalf("replay outcome = %q, want duplicate", got)
	}
	if at := mustRow(t, f.store, "ord-1", 1).ConfirmedAt(); !at.Equal(firstAt) {
		t.Fatalf("ConfirmedAt moved to %v on a replay, want it to stay %v", at, firstAt)
	}
}

func TestConfirmRelease_AnAlreadyConfirmedLineWithANewEventIDIsAnIdempotentDuplicate(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
	f.confirm(t, "evt-1", "ord-1", 1)
	firstAt := *mustRow(t, f.store, "ord-1", 1).ConfirmedAt()

	f.clock.Advance(time.Hour)
	if got := f.confirm(t, "evt-2", "ord-1", 1); got != ports.ReleaseConfirmationDuplicate {
		t.Fatalf("outcome = %q, want duplicate", got)
	}
	c := mustRow(t, f.store, "ord-1", 1)
	if c.State() != releaseconfirmation.StateConfirmed || !c.ConfirmedAt().Equal(firstAt) {
		t.Fatalf("row = %s %v, want it untouched", c.State(), c.ConfirmedAt())
	}
}

func TestConfirmRelease_LateConfirmationClearsAnUnconfirmedLine(t *testing.T) {
	f := newConfirmFixture()
	c := pendingRow(t, "ord-1", 1, confirmT0)
	c.MarkUnconfirmed(confirmT0.Add(confirmTimeout + time.Minute))
	f.store.put(c)
	f.clock = memory.NewFixedClock(confirmT0.Add(time.Hour))
	f.uc.Clock = f.clock

	if got := f.confirm(t, "evt-late", "ord-1", 1); got != ports.ReleaseConfirmationLateConfirmed {
		t.Fatalf("outcome = %q, want late_confirmed", got)
	}
	got := mustRow(t, f.store, "ord-1", 1)
	if got.State() != releaseconfirmation.StateConfirmed || !got.ConfirmedAt().Equal(f.clock.Now()) {
		t.Fatalf("row = %s %v, want CONFIRMED at %v", got.State(), got.ConfirmedAt(), f.clock.Now())
	}
	if !strings.Contains(f.logs.String(), "late") {
		t.Fatalf("a late confirmation must be logged so the ERROR it clears can be matched; logs: %q", f.logs.String())
	}
}

func TestConfirmRelease_AnUnknownLineIsIgnoredAndNotClaimed(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))

	// A transfer / RELOCATE / legacy work unit: a reference this service
	// never opened a row for, and an order line past the last one.
	for name, req := range map[string]usecases.ConfirmReleaseRequest{
		"unknown order":   {EventID: "evt-t", Reference: "transfer-77", LineNo: 1},
		"unknown line":    {EventID: "evt-l", Reference: "ord-1", LineNo: 9},
		"another context": {EventID: "evt-x", Reference: "reloc-plan-3", LineNo: 4},
	} {
		got, err := f.uc.Execute(context.Background(), req)
		if err != nil {
			t.Fatalf("%s: Execute: %v", name, err)
		}
		if got != ports.ReleaseConfirmationIgnoredUnknownLine {
			t.Fatalf("%s: outcome = %q, want ignored_unknown_line", name, got)
		}
		if f.store.isClaimed(req.EventID) {
			t.Fatalf("%s: an ignored event must not be claimed (the claim table would grow with every transfer unit)", name)
		}
	}
	if c := mustRow(t, f.store, "ord-1", 1); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("the known row changed to %q", c.State())
	}
	if f.store.rowCount() != 1 {
		t.Fatalf("rows = %d, an ignored event must not create one", f.store.rowCount())
	}
}

func TestConfirmRelease_MissingReferenceOrLineNoIsIgnoredWithoutATransaction(t *testing.T) {
	for name, req := range map[string]usecases.ConfirmReleaseRequest{
		"no reference":     {EventID: "evt-1", Reference: "", LineNo: 1},
		"no line_no":       {EventID: "evt-2", Reference: "ord-1", LineNo: 0},
		"negative line_no": {EventID: "evt-3", Reference: "ord-1", LineNo: -4},
		"neither":          {EventID: "evt-4"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newConfirmFixture()
			f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
			got, err := f.uc.Execute(context.Background(), req)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if got != ports.ReleaseConfirmationIgnoredNoReference {
				t.Fatalf("outcome = %q, want ignored_no_reference", got)
			}
			if f.store.executes != 0 || f.store.isClaimed(req.EventID) {
				t.Fatalf("executes=%d claimed=%v: an event without the new fields must touch nothing", f.store.executes, f.store.isClaimed(req.EventID))
			}
			if got := f.metrics.recorded(); len(got) != 1 || got[0] != ports.ReleaseConfirmationIgnoredNoReference {
				t.Fatalf("metrics = %v, want [ignored_no_reference]", got)
			}
		})
	}
}

func TestConfirmRelease_AnEmptyEventIDIsInvalidBeforeAnyTransaction(t *testing.T) {
	f := newConfirmFixture()
	_, err := f.uc.Execute(context.Background(), usecases.ConfirmReleaseRequest{Reference: "ord-1", LineNo: 1})
	if !errors.Is(err, usecases.ErrInvalidReleaseConfirmation) {
		t.Fatalf("err = %v, want ErrInvalidReleaseConfirmation", err)
	}
	if f.store.executes != 0 {
		t.Fatal("validation must run before the transaction opens")
	}
}

func TestConfirmRelease_AFailureAfterTheClaimRollsTheClaimBack(t *testing.T) {
	f := newConfirmFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
	f.store.saveErr = errBoom

	_, err := f.uc.Execute(context.Background(), usecases.ConfirmReleaseRequest{EventID: "evt-1", Reference: "ord-1", LineNo: 1})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the transient failure returned for a retry", err)
	}
	if f.store.isClaimed("evt-1") {
		t.Fatal("the claim must roll back with the failed state change, or the redelivery would be skipped as already handled")
	}
	if c := mustRow(t, f.store, "ord-1", 1); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q, want PENDING", c.State())
	}
	if len(f.metrics.recorded()) != 0 {
		t.Fatalf("metrics = %v, a failed attempt must not be counted", f.metrics.recorded())
	}

	// The redelivery succeeds once the store recovers.
	f.store.saveErr = nil
	if got := f.confirm(t, "evt-1", "ord-1", 1); got != ports.ReleaseConfirmationConfirmed {
		t.Fatalf("redelivery outcome = %q, want confirmed", got)
	}
}

func TestConfirmRelease_ALookupFailureIsReturnedNotIgnored(t *testing.T) {
	f := newConfirmFixture()
	f.store.findErr = errBoom
	_, err := f.uc.Execute(context.Background(), usecases.ConfirmReleaseRequest{EventID: "evt-1", Reference: "ord-1", LineNo: 1})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, a database failure must not look like an unknown line", err)
	}
}

func TestConfirmRelease_WorksWithoutMetricsLoggerOrUnitOfWork(t *testing.T) {
	store := newConfirmationStore()
	store.put(pendingRow(t, "ord-1", 1, confirmT0))
	uc := &usecases.ConfirmRelease{Confirmations: store, Processed: store, Clock: memory.NewFixedClock(confirmT0)}
	got, err := uc.Execute(context.Background(), usecases.ConfirmReleaseRequest{EventID: "evt-1", Reference: "ord-1", LineNo: 1})
	if err != nil || got != ports.ReleaseConfirmationConfirmed {
		t.Fatalf("Execute = %q, %v; want confirmed", got, err)
	}
}

func mustRow(t *testing.T, s *confirmationStore, orderID shared.OrderId, lineNo int) releaseconfirmation.Confirmation {
	t.Helper()
	c, ok := s.row(orderID, lineNo)
	if !ok {
		t.Fatalf("no row for %s line %d", orderID, lineNo)
	}
	return c
}

// ---------------------------------------------------------------------
// FlagUnconfirmedReleases: the timeout sweep.
// ---------------------------------------------------------------------

type flagFixture struct {
	store   *confirmationStore
	events  *recordingPublisher
	metrics *confirmationMetrics
	clock   *memory.FixedClock
	logs    *lockedBuffer
	uc      *usecases.FlagUnconfirmedReleases
}

func newFlagFixture() *flagFixture {
	store := newConfirmationStore()
	events := &recordingPublisher{}
	metrics := &confirmationMetrics{}
	clock := memory.NewFixedClock(confirmT0.Add(confirmTimeout + time.Minute))
	logs := &lockedBuffer{}
	return &flagFixture{
		store: store, events: events, metrics: metrics, clock: clock, logs: logs,
		uc: &usecases.FlagUnconfirmedReleases{
			Confirmations: store, Events: events, Clock: clock, UnitOfWork: store, Metrics: metrics,
			Logger: slog.New(slog.NewTextHandler(logs, nil)), Timeout: confirmTimeout,
		},
	}
}

func unconfirmedEvents(p *recordingPublisher) []shared.OrderReleaseUnconfirmed {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []shared.OrderReleaseUnconfirmed
	for _, e := range p.events {
		if ev, ok := e.(shared.OrderReleaseUnconfirmed); ok {
			out = append(out, ev)
		}
	}
	return out
}

func TestFlagUnconfirmedReleases_FlagsAnOverdueLineAsAFactNotAnAction(t *testing.T) {
	f := newFlagFixture()
	f.store.put(pendingRow(t, "ord-1", 2, confirmT0))

	n, err := f.uc.Execute(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("Execute = %d, %v; want 1 flagged", n, err)
	}

	assertRowFlagged(t, f, "ord-1", 2)
	assertOneUnconfirmedFact(t, f, "ord-1", 2, "pick")
	assertFlagTelemetry(t, f, "ord-1", 2)
	if len(f.store.outsideScope) != 0 {
		t.Fatalf("writes issued outside the unit of work: %v", f.store.outsideScope)
	}
}

// assertRowFlagged checks the process-manager row moved to UNCONFIRMED at the
// sweep's clock.
func assertRowFlagged(t *testing.T, f *flagFixture, orderID shared.OrderId, lineNo int) {
	t.Helper()
	c := mustRow(t, f.store, orderID, lineNo)
	if c.State() != releaseconfirmation.StateUnconfirmed {
		t.Fatalf("State = %q, want UNCONFIRMED", c.State())
	}
	if at := c.UnconfirmedAt(); at == nil || !at.Equal(f.clock.Now()) {
		t.Fatalf("UnconfirmedAt = %v, want %v", at, f.clock.Now())
	}
}

// assertOneUnconfirmedFact checks the sweep raised exactly the one analytics
// fact and nothing else: a fact, never an action (no cancel, revoke or
// re-release).
func assertOneUnconfirmedFact(t *testing.T, f *flagFixture, orderID shared.OrderId, lineNo int, pathID shared.PathId) {
	t.Helper()
	evs := unconfirmedEvents(f.events)
	if len(evs) != 1 {
		t.Fatalf("OrderReleaseUnconfirmed events = %d, want exactly 1 (events: %v)", len(evs), f.events.names())
	}
	ev := evs[0]
	if ev.OrderID != orderID || ev.LineNo != lineNo || ev.PathID != pathID || ev.WorkUnitID != usecases.WorkUnitID(orderID, lineNo) {
		t.Fatalf("event = %+v", ev)
	}
	if !ev.ReleasedAt.Equal(confirmT0) || !ev.OccurredAt().Equal(f.clock.Now()) {
		t.Fatalf("ReleasedAt=%v OccurredAt=%v, want %v and %v", ev.ReleasedAt, ev.OccurredAt(), confirmT0, f.clock.Now())
	}
	if got := f.events.names(); len(got) != 1 || got[0] != "OrderReleaseUnconfirmed" {
		t.Fatalf("events = %v, want only OrderReleaseUnconfirmed", got)
	}
}

// assertFlagTelemetry checks the counter and the ERROR log line an operator
// greps for.
func assertFlagTelemetry(t *testing.T, f *flagFixture, orderID shared.OrderId, lineNo int) {
	t.Helper()
	if f.metrics.unconfirmed != 1 {
		t.Fatalf("unconfirmed counter = %d, want 1", f.metrics.unconfirmed)
	}
	logs := f.logs.String()
	for _, want := range []string{"level=ERROR", "release unconfirmed", string(orderID), usecases.WorkUnitID(orderID, lineNo)} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log lacks %q: %s", want, logs)
		}
	}
}

func TestFlagUnconfirmedReleases_ExactlyOnce(t *testing.T) {
	f := newFlagFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))

	if n, err := f.uc.Execute(context.Background()); err != nil || n != 1 {
		t.Fatalf("first pass = %d, %v; want 1", n, err)
	}
	f.clock.Advance(24 * time.Hour)
	if n, err := f.uc.Execute(context.Background()); err != nil || n != 0 {
		t.Fatalf("second pass = %d, %v; want 0: an UNCONFIRMED line is flagged once", n, err)
	}
	if got := len(unconfirmedEvents(f.events)); got != 1 {
		t.Fatalf("events after two passes = %d, want 1", got)
	}
	if f.metrics.unconfirmed != 1 {
		t.Fatalf("counter = %d, want 1", f.metrics.unconfirmed)
	}
}

func TestFlagUnconfirmedReleases_OnlyRowsOlderThanTheTimeout(t *testing.T) {
	f := newFlagFixture()
	cutoff := f.clock.Now().Add(-confirmTimeout)
	f.store.put(pendingRow(t, "ord-old", 1, cutoff.Add(-time.Nanosecond))) // just past the timeout
	f.store.put(pendingRow(t, "ord-edge", 1, cutoff))                      // exactly the timeout: not older
	f.store.put(pendingRow(t, "ord-new", 1, cutoff.Add(time.Second)))      // still inside it

	n, err := f.uc.Execute(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("Execute = %d, %v; want exactly the one older than the timeout", n, err)
	}
	if !f.store.lastCutoff.Equal(cutoff) {
		t.Fatalf("cutoff = %v, want now - timeout = %v", f.store.lastCutoff, cutoff)
	}
	for id, want := range map[shared.OrderId]releaseconfirmation.State{
		"ord-old": releaseconfirmation.StateUnconfirmed, "ord-edge": releaseconfirmation.StatePending, "ord-new": releaseconfirmation.StatePending,
	} {
		if got := mustRow(t, f.store, id, 1).State(); got != want {
			t.Fatalf("%s State = %q, want %q", id, got, want)
		}
	}
}

func TestFlagUnconfirmedReleases_NeverTouchesConfirmedOrAlreadyFlaggedRows(t *testing.T) {
	f := newFlagFixture()
	confirmed := pendingRow(t, "ord-c", 1, confirmT0)
	confirmed.Confirm(confirmT0.Add(time.Second))
	flagged := pendingRow(t, "ord-u", 1, confirmT0)
	flagged.MarkUnconfirmed(confirmT0.Add(confirmTimeout))
	f.store.put(confirmed)
	f.store.put(flagged)

	if n, err := f.uc.Execute(context.Background()); err != nil || n != 0 {
		t.Fatalf("Execute = %d, %v; want 0", n, err)
	}
	if len(f.events.names()) != 0 {
		t.Fatalf("events = %v, want none", f.events.names())
	}
}

func TestFlagUnconfirmedReleases_ZeroOrNegativeTimeoutDisablesTheSweep(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Minute} {
		f := newFlagFixture()
		f.uc.Timeout = timeout
		f.store.put(pendingRow(t, "ord-1", 1, confirmT0.Add(-1000*time.Hour)))

		n, err := f.uc.Execute(context.Background())
		if err != nil || n != 0 {
			t.Fatalf("timeout %v: Execute = %d, %v; want 0 and no error", timeout, n, err)
		}
		if f.store.claimCalls != 0 || f.store.executes != 0 {
			t.Fatalf("timeout %v: a disabled sweep must not even open a transaction (claims=%d executes=%d)", timeout, f.store.claimCalls, f.store.executes)
		}
		if c := mustRow(t, f.store, "ord-1", 1); c.State() != releaseconfirmation.StatePending {
			t.Fatalf("timeout %v: State = %q, want PENDING", timeout, c.State())
		}
	}
}

func TestFlagUnconfirmedReleases_DrainsABacklogInBatches(t *testing.T) {
	f := newFlagFixture()
	f.uc.BatchSize = 2
	for i := 1; i <= 5; i++ {
		f.store.put(pendingRow(t, "ord-1", i, confirmT0))
	}

	n, err := f.uc.Execute(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("Execute = %d, %v; want all 5", n, err)
	}
	if got := len(unconfirmedEvents(f.events)); got != 5 {
		t.Fatalf("events = %d, want 5", got)
	}
	if f.store.executes < 3 {
		t.Fatalf("transactions = %d, want at least 3 batches of at most 2", f.store.executes)
	}
}

func TestFlagUnconfirmedReleases_APublishFailureRollsTheStateChangeBackAndTheNextPassFlagsIt(t *testing.T) {
	f := newFlagFixture()
	f.store.put(pendingRow(t, "ord-1", 1, confirmT0))
	f.events.failAfter(0, errBoom)

	if _, err := f.uc.Execute(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Execute = %v, want the publish failure", err)
	}
	if c := mustRow(t, f.store, "ord-1", 1); c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q after a failed publish, want PENDING (state change and fact commit together)", c.State())
	}
	if f.metrics.unconfirmed != 0 || strings.Contains(f.logs.String(), "release unconfirmed") {
		t.Fatalf("a rolled-back flag was reported: counter=%d logs=%s", f.metrics.unconfirmed, f.logs.String())
	}

	f.events.failAfter(1<<30, errBoom)
	if n, err := f.uc.Execute(context.Background()); err != nil || n != 1 {
		t.Fatalf("next pass = %d, %v; want the line flagged now", n, err)
	}
	if got := len(unconfirmedEvents(f.events)); got != 1 {
		t.Fatalf("events = %d, want 1", got)
	}
}

func TestFlagUnconfirmedReleases_AClaimFailureIsReturned(t *testing.T) {
	f := newFlagFixture()
	f.store.claimErr = errBoom
	if _, err := f.uc.Execute(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Execute = %v, want the claim failure", err)
	}
}

func TestFlagUnconfirmedReleases_WorksWithoutMetricsOrLogger(t *testing.T) {
	store := newConfirmationStore()
	store.put(pendingRow(t, "ord-1", 1, confirmT0))
	uc := &usecases.FlagUnconfirmedReleases{
		Confirmations: store, Events: &recordingPublisher{}, Clock: memory.NewFixedClock(confirmT0.Add(time.Hour)),
		Timeout: confirmTimeout,
	}
	if n, err := uc.Execute(context.Background()); err != nil || n != 1 {
		t.Fatalf("Execute = %d, %v; want 1", n, err)
	}
}
