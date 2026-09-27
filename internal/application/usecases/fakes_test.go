package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// errBoom stands in for any infrastructure failure that is not a business
// fact — the exact case allocation must NOT turn into a backorder.
var errBoom = errors.New("boom")

// fakeInventory is a scripted ports.InventoryReservationClient. Per-SKU
// scripts let one test express "SKU-1 reserves, SKU-2 is out of stock".
type fakeInventory struct {
	mu sync.Mutex

	// reserveErrBySKU maps a SKU to the error Reserve should return for
	// it. ports.ErrInsufficientStock models inventory-storage's 409.
	reserveErrBySKU map[shared.SKU]error
	// reserveErr applies to every SKU with no specific script.
	reserveErr error
	// revokeErr is returned by every RevokeReservation call.
	revokeErr error

	nextID       int
	reserveCalls []ports.ReservationRequest
	revokeCalls  []string
}

func newFakeInventory() *fakeInventory {
	return &fakeInventory{reserveErrBySKU: map[shared.SKU]error{}}
}

func (f *fakeInventory) Reserve(_ context.Context, req ports.ReservationRequest) (ports.ReservationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserveCalls = append(f.reserveCalls, req)

	if err, ok := f.reserveErrBySKU[req.SKU]; ok {
		return ports.ReservationResult{}, err
	}
	if f.reserveErr != nil {
		return ports.ReservationResult{}, f.reserveErr
	}
	f.nextID++
	return ports.ReservationResult{ReservationID: reservationID(f.nextID)}, nil
}

func (f *fakeInventory) RevokeReservation(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokeCalls = append(f.revokeCalls, id)
	return f.revokeErr
}

func reservationID(n int) string {
	return fmt.Sprintf("res-%d", n)
}

// recordingPublisher captures published events so a test can assert the
// exact facts a use case emitted, in order.
type recordingPublisher struct {
	mu     sync.Mutex
	events []shared.DomainEvent
	err    error
	// errAfter, when >= 0, makes Publish fail once that many events have
	// already been accepted — for asserting a failure on a later event in
	// a sequence rather than the first.
	errAfter int
}

func (p *recordingPublisher) Publish(_ context.Context, event shared.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil && len(p.events) >= p.errAfter {
		return p.err
	}
	p.events = append(p.events, event)
	return nil
}

// failAfter schedules err for the (n+1)th and every later Publish call.
func (p *recordingPublisher) failAfter(n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errAfter = n
	p.err = err
}

func (p *recordingPublisher) names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.events))
	for _, e := range p.events {
		out = append(out, e.EventName())
	}
	return out
}

// failingRepo wraps a real repo and fails a chosen operation, so the
// error-propagation paths of every use case are exercised.
type failingRepo struct {
	inner      ports.OrderRepo
	saveErr    error
	findErr    error
	nextIDErr  error
	saveCalls  int
	failSaveOn int // when > 0, only the Nth Save fails
}

func (r *failingRepo) Save(ctx context.Context, o *order.Order) error {
	r.saveCalls++
	if r.saveErr != nil && (r.failSaveOn == 0 || r.failSaveOn == r.saveCalls) {
		return r.saveErr
	}
	return r.inner.Save(ctx, o)
}

func (r *failingRepo) FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	if r.findErr != nil {
		return nil, r.findErr
	}
	return r.inner.FindByID(ctx, id)
}

func (r *failingRepo) NextID(ctx context.Context) (shared.OrderId, error) {
	if r.nextIDErr != nil {
		return "", r.nextIDErr
	}
	return r.inner.NextID(ctx)
}

// findFails wraps a real repo whose FindByID call always fails. Used to
// model ReceiveOrder's post-hard-failure re-read hitting its own
// infrastructure problem (a separate concern from the allocation failure
// that triggered the re-read in the first place).
type findFails struct {
	inner   ports.OrderRepo
	findErr error
}

func (r *findFails) Save(ctx context.Context, o *order.Order) error {
	return r.inner.Save(ctx, o)
}

func (r *findFails) FindByID(context.Context, shared.OrderId) (*order.Order, error) {
	return nil, r.findErr
}

func (r *findFails) NextID(ctx context.Context) (shared.OrderId, error) {
	return r.inner.NextID(ctx)
}

// fakeCatalogue is a scripted ports.ProcessPathCatalogue. By default every
// path is active — tests that want a rejection populate inactive with the
// specific PathId to reject, mirroring fakeInventory's per-key scripting
// style. cycleTimes/eligibilities are optional per-path scripts for
// PromisePolicy tests; an unscripted path reports known=false for both,
// matching a real path the catalogue has never seen a cycle time for.
type fakeCatalogue struct {
	inactive    map[shared.PathId]bool
	cycleTimes  map[shared.PathId]time.Duration
	eligibility map[shared.PathId]shared.Eligibility
}

func (c *fakeCatalogue) IsActive(pathID shared.PathId) bool {
	return !c.inactive[pathID]
}

func (c *fakeCatalogue) CycleTimeP95(pathID shared.PathId) (time.Duration, bool) {
	if c.inactive[pathID] {
		return 0, false
	}
	d, ok := c.cycleTimes[pathID]
	return d, ok
}

func (c *fakeCatalogue) Eligibility(pathID shared.PathId) (shared.Eligibility, bool) {
	if c.inactive[pathID] {
		return shared.Eligibility{}, false
	}
	e, ok := c.eligibility[pathID]
	return e, ok
}

// ListActive satisfies ports.ProcessPathCatalogue (ADR-0021): every
// PathId this fake has been told about via eligibility or cycleTimes,
// minus anything marked inactive. Tests that want PathSelectionPolicy to
// see more than one active candidate populate eligibility/cycleTimes for
// more than one PathId.
func (c *fakeCatalogue) ListActive() []shared.ActivePathCandidate {
	seen := map[shared.PathId]bool{}
	for id := range c.eligibility {
		seen[id] = true
	}
	for id := range c.cycleTimes {
		seen[id] = true
	}

	out := make([]shared.ActivePathCandidate, 0, len(seen))
	for id := range seen {
		if c.inactive[id] {
			continue
		}
		cycleTime, cycleTimeKnown := c.cycleTimes[id]
		out = append(out, shared.ActivePathCandidate{
			PathId:         id,
			CycleTimeP95:   cycleTime,
			CycleTimeKnown: cycleTimeKnown,
			Eligibility:    c.eligibility[id],
		})
	}
	return out
}

// fakeClassificationLookup is a scripted ports.ProductClassificationLookup.
// By default every SKU reports Known=false (unclassified), matching
// PermissiveLookup's own behaviour -- tests that want a specific line to
// carry derived attributes populate tags for that SKU.
type fakeClassificationLookup struct {
	tags map[shared.SKU][]string
	err  error
}

func (f *fakeClassificationLookup) GetClassification(_ context.Context, sku string) (ports.ProductClassification, error) {
	if f.err != nil {
		return ports.ProductClassification{}, f.err
	}
	tags, ok := f.tags[shared.SKU(sku)]
	if !ok {
		return ports.ProductClassification{SKU: sku, Known: false}, nil
	}
	return ports.ProductClassification{SKU: sku, HandlingTags: tags, Known: true}, nil
}

// countingMetrics is a scripted ports.OrderMetrics: it counts the
// accepted/rejected outcomes ReceiveOrder records so a test can assert
// the business-fact signal fires exactly once per intake attempt.
type countingMetrics struct {
	accepted int
	rejected int
}

func (m *countingMetrics) OrderAccepted(context.Context) { m.accepted++ }

func (m *countingMetrics) OrderRejected(context.Context) { m.rejected++ }

var _ ports.OrderMetrics = (*countingMetrics)(nil)

// fixture bundles everything a use-case test needs, wired to in-memory and
// fake adapters. No test in this package touches a real network or DB.
type fixture struct {
	orders         ports.OrderRepo
	inventory      *fakeInventory
	events         *recordingPublisher
	clock          *memory.FixedClock
	promise        order.PromisePolicy
	catalogue      *fakeCatalogue
	classification *fakeClassificationLookup
}

func now() time.Time {
	return time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
}

func newFixture() *fixture {
	leadTime := order.NewLeadTimePolicy(24*time.Hour, map[shared.PathId]time.Duration{
		"singles": 6 * time.Hour,
	})
	return &fixture{
		orders:    memory.NewOrderRepo(),
		inventory: newFakeInventory(),
		events:    &recordingPublisher{},
		clock:     memory.NewFixedClock(now()),
		// No Schedule/Capability wired in this fixture: PromisePolicy
		// always falls back to Fallback (LeadTimePolicy), matching
		// every existing test's expectations unchanged. Tests that
		// specifically exercise PromisePolicy's capability path live
		// in internal/domain/order, not here.
		promise:        order.PromisePolicy{Fallback: leadTime},
		catalogue:      &fakeCatalogue{inactive: map[shared.PathId]bool{}, eligibility: map[shared.PathId]shared.Eligibility{}},
		classification: &fakeClassificationLookup{tags: map[shared.SKU][]string{}},
	}
}

func (f *fixture) receiveOrder() *usecases.ReceiveOrder {
	return &usecases.ReceiveOrder{Orders: f.orders, Events: f.events, Clock: f.clock, Inventory: f.inventory, Promise: f.promise, Catalogue: f.catalogue, Classification: f.classification}
}

func (f *fixture) retryAllocation() *usecases.RetryAllocation {
	return &usecases.RetryAllocation{Orders: f.orders, Inventory: f.inventory, Events: f.events, Clock: f.clock, Promise: f.promise}
}

func (f *fixture) cancelOrder() *usecases.CancelOrder {
	return &usecases.CancelOrder{Orders: f.orders, Inventory: f.inventory, Events: f.events, Clock: f.clock}
}

func (f *fixture) getOrder() *usecases.GetOrder {
	return &usecases.GetOrder{Orders: f.orders}
}

// mustReceive creates an order through the real ReceiveOrder use case.
// Since ReceiveOrder now ALSO attempts allocation-then-release implicitly
// (the choreographed-release redesign), callers that want a purely
// Received/Pending order must make sure inventory has nothing scripted to
// succeed with yet, or must accept whatever allocation outcome the current
// fixture state produces. Most existing tests build up fixture state
// (e.g. reserveErrBySKU) BEFORE calling mustReceive precisely so the
// resulting order lands in the state the test wants to assert against.
func (f *fixture) mustReceive(t *testing.T, allowPartialShipment bool, lines ...usecases.NewLine) *order.Order {
	t.Helper()
	o, err := f.receiveOrder().Execute(context.Background(), lines, allowPartialShipment)
	if err != nil {
		t.Fatalf("ReceiveOrder: %v", err)
	}
	f.events.reset()
	return o
}

func (p *recordingPublisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
}

// allocatedFixture returns a fixture whose order has every requested line
// Allocated but NOT Released, built by direct domain manipulation rather
// than through ReceiveOrder. Since the choreographed-release redesign, the
// "Allocated but not yet released" window is no longer reachable through
// any public use case for a fully-allocatable ship-complete order — a
// line that allocates successfully is released in the SAME call — so
// tests that need to exercise that window (CancelOrder's revoke path in
// particular) build it directly: block ReceiveOrder's implicit allocation
// entirely (so every line stays Pending), then allocate each line by hand
// via the pure domain transition and persist the result.
func allocatedFixture(t *testing.T, allowPartialShipment bool, lines ...usecases.NewLine) (*fixture, *order.Order) {
	t.Helper()
	f := newFixture()
	f.inventory.reserveErr = errBoom
	o := f.mustReceive(t, allowPartialShipment, lines...)
	f.inventory.reserveErr = nil

	for _, l := range o.Lines() {
		result, err := f.inventory.Reserve(context.Background(), ports.ReservationRequest{
			SKU: l.SKU(), Quantity: l.Quantity(), DemandRef: o.ID(),
		})
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		if err := o.Allocate(l.LineNo(), result.ReservationID); err != nil {
			t.Fatalf("Allocate(%d): %v", l.LineNo(), err)
		}
	}
	if err := f.orders.Save(context.Background(), o); err != nil {
		t.Fatalf("Save: %v", err)
	}
	f.events.reset()
	return f, o
}

// backorderedFixture returns a fixture whose order has line 1 allocated
// (and, when allowPartialShipment is true, immediately released too — see
// Order.EnsureReleasable, which always permits release on a
// partial-shipment order regardless of other lines' state) and line 2
// backordered, built through the real ReceiveOrder use case so its
// implicit allocation-then-release flow runs exactly as it would in
// production.
func backorderedFixture(t *testing.T, allowPartialShipment bool) (*fixture, *order.Order) {
	t.Helper()
	f := newFixture()
	f.inventory.reserveErrBySKU["SKU-2"] = ports.ErrInsufficientStock
	o := f.mustReceive(t, allowPartialShipment, line("SKU-1", 1, "pick"), line("SKU-2", 1, "pick"))
	return f, o
}

func line(sku shared.SKU, qty int, pathID shared.PathId) usecases.NewLine {
	return usecases.NewLine{SKU: sku, Quantity: qty, PathID: pathID}
}

// assertLineStatuses checks the aggregate's per-line states in line order.
func assertLineStatuses(t *testing.T, o *order.Order, want ...order.LineStatus) {
	t.Helper()
	lines := o.Lines()
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for i, l := range lines {
		if l.Status() != want[i] {
			t.Fatalf("line %d status = %q, want %q", l.LineNo(), l.Status(), want[i])
		}
	}
}

func assertEventNames(t *testing.T, p *recordingPublisher, want ...string) {
	t.Helper()
	got := p.names()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

// findOrderAllocated returns the first shared.OrderAllocated event
// published to p, failing the test if none was published. Use this
// instead of assertEventNames when a test needs to inspect an event's
// payload (e.g. Lines[].FulfillmentClass), not just confirm it fired.
func findOrderAllocated(t *testing.T, p *recordingPublisher) shared.OrderAllocated {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.events {
		if a, ok := e.(shared.OrderAllocated); ok {
			return a
		}
	}
	t.Fatalf("no OrderAllocated event was published; events = %v", p.names())
	return shared.OrderAllocated{}
}
