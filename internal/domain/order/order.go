package order

import (
	"errors"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

var (
	// ErrNoLines is returned when an Order is constructed with no lines.
	// An order that asks for nothing is not a business fact.
	ErrNoLines = errors.New("order must have at least one line")

	// ErrLineNotFound is returned when a line number does not address a
	// line on this order.
	ErrLineNotFound = errors.New("order line not found")

	// ErrLineAlreadyAllocated enforces "cannot allocate the same line
	// twice": Allocate only accepts a Pending line.
	ErrLineAlreadyAllocated = errors.New("order line is already allocated")

	// ErrLineNotPending is returned when Allocate is called on a line that
	// is neither Pending nor already Allocated — most importantly a
	// Backordered line, which may only return to Allocated via
	// RetryAllocate.
	ErrLineNotPending = errors.New("order line is not pending allocation")

	// ErrLineNotBackordered is returned when RetryAllocate is called on a
	// line that is not Backordered. Together with ErrLineNotPending this
	// is the whole of the "Backordered -> Allocated only via
	// RetryAllocation" invariant.
	ErrLineNotBackordered = errors.New("order line is not backordered")

	// ErrLineNotAllocated enforces "cannot release a line that isn't
	// Allocated".
	ErrLineNotAllocated = errors.New("order line is not allocated")

	// ErrOrderAlreadyReleased enforces BR6: cancellation is legal only
	// while no line has reached Released.
	ErrOrderAlreadyReleased = errors.New("order already has released lines and can no longer be cancelled")

	// ErrShipCompleteBlocked enforces BR3 at release time: an order with
	// AllowPartialShipment=false may not release any line until every
	// line is allocated.
	ErrShipCompleteBlocked = errors.New("ship-complete order cannot be released while any line is unallocated")

	// ErrHeldOrderMustBeShipComplete enforces ADR 0020 §4 at intake: an
	// order held before release (releaseOnAllocation=false) may not also
	// allow partial shipment. A caller that holds an order is making one
	// whole-order commit/reject decision; per-group promising (ADR 0017)
	// would give it several cutoffs for a single answer.
	ErrHeldOrderMustBeShipComplete = errors.New("a held order (releaseOnAllocation=false) must be ship-complete")
)

// Order is the aggregate root: the unit of consistency for intake,
// allocation, release and cancellation. Every line mutation goes through
// an Order method, so the invariants below cannot be bypassed.
//
// promiseGroups (ADR 0014 §3 / ADR 0017) is the full-fidelity promise
// breakdown: one PromiseGroup per set of lines sharing a cutoff. It lives
// ALONGSIDE, not instead of, the legacy single-valued promiseDate/
// promiseCptId/promiseBasis fields — those three remain a derived,
// backward-compatible SUMMARY of promiseGroups (see SetPromiseGroups),
// never a second source of truth that could drift from it.
type Order struct {
	id                   shared.OrderId
	lines                []*OrderLine
	allowPartialShipment bool
	promiseDate          *time.Time
	promiseCptId         *string
	promiseBasis         *PromiseBasis
	promiseGroups        []PromiseGroup
	// heldAtIntake records ADR 0020 §1's releaseOnAllocation=false as
	// its INVERSE, deliberately. The zero value of a bool is false, and
	// every existing constructor, test literal and Rehydrate call site
	// leaves this field unset — so the zero value must mean "an ordinary
	// order that releases on allocation". Storing releaseOnAllocation
	// directly would make every one of those sites silently produce a
	// HELD order, which fails closed in the worst possible direction:
	// work that never reaches the floor, on orders nobody asked to hold.
	heldAtIntake bool

	// requiredShipBy is an externally-dictated deadline attached at
	// intake (ADR 0020 §2). When set, the promise is not CHOSEN by this
	// service — it is constrained to a window at or before this instant,
	// and an order that cannot make it gets no promise at all rather
	// than an optimistic one.
	//
	// A pointer, not a zero time.Time: "no deadline" is the overwhelming
	// majority of orders and must be distinguishable from "a deadline
	// that happens to be the zero instant", which would otherwise make
	// every ordinary order look infeasible.
	requiredShipBy *time.Time

	// version is inert optimistic-concurrency-control metadata (see the
	// version-column ADR). It is carried by the aggregate purely so a
	// repository's read-modify-write Save can guard its write against a
	// lost update, exactly like id: the domain layer never reasons
	// about it in any business rule. A freshly-constructed order (New)
	// starts at version 1, by convention; Rehydrate takes the
	// persisted value through OrderSnapshot.Version.
	version int
}

// New constructs an Order in Received status. lines must be non-empty;
// each line is numbered by its 1-based position.
//
// The Order takes its OWN copy of every line: numbering never writes
// through to the caller's *OrderLine, and the caller's pointers do not
// alias the aggregate's entities afterwards (all further mutation goes
// through Order methods, per the aggregate-boundary rule). A nil line is
// a programming error, as it always was.
func New(id shared.OrderId, lines []*OrderLine, allowPartialShipment bool) (*Order, error) {
	if id == "" {
		return nil, shared.ErrEmptyOrderID
	}
	if len(lines) == 0 {
		return nil, ErrNoLines
	}
	numbered := make([]*OrderLine, 0, len(lines))
	for i, l := range lines {
		owned := *l
		owned.lineNo = i + 1
		numbered = append(numbered, &owned)
	}
	return &Order{id: id, lines: numbered, allowPartialShipment: allowPartialShipment, version: 1}, nil
}

// OrderSnapshot is the persisted state of an Order, the single input of
// Rehydrate. Field zero values are the safe defaults a caller that does
// not care about a field should get:
//
//   - PromiseDate / PromiseCptID / PromiseBasis are nil for orders
//     persisted before ADR 0014, or for a promise never given a CPT
//     identity (a LeadTime-basis promise leaves PromiseCptID nil;
//     PromiseBasis is still recorded).
//   - PromiseGroups (ADR 0014 §3 / ADR 0017) may be nil — an order
//     persisted before that ADR, or one whose summary fields were set via
//     the legacy SetPromise path. PromiseGroups() then returns an empty
//     slice and the summary fields are exactly what was passed in.
//   - HeldAtIntake is ADR 0020 §1's releaseOnAllocation=false as its
//     INVERSE, deliberately (see Order.heldAtIntake): the zero value is
//     an ordinary order that releases on allocation. A row written before
//     migration 0005 reads back releaseOnAllocation=TRUE (the column's
//     DEFAULT), i.e. HeldAtIntake=false — pre-ADR orders rehydrate as
//     un-held, which is what they are.
//   - RequiredShipBy (ADR 0020 §2) is nil for the overwhelming majority
//     of orders.
//   - Version is the optimistic-concurrency version the row was loaded at.
//     The zero value means "never persisted" and rehydrates as 1, the
//     same starting version New assigns; every persisted row is >= 1
//     (the version migration's DEFAULT), so a pre-existing order's first
//     post-migration Save is guarded against version 1, never a mismatch
//     it could never have satisfied.
type OrderSnapshot struct {
	ID                   shared.OrderId
	Lines                []*OrderLine
	AllowPartialShipment bool
	PromiseDate          *time.Time
	PromiseCptID         *string
	PromiseBasis         *PromiseBasis
	PromiseGroups        []PromiseGroup
	HeldAtIntake         bool
	RequiredShipBy       *time.Time
	Version              int
}

// Rehydrate rebuilds an Order from persisted state without re-running
// construction invariants. It is the ONE persistence entry point: the
// outbound repository adapter calls it with what it read; tests and
// PromisePolicy's in-memory scratch computation call it with a partial
// snapshot (nothing that does persists the result, so the defaulted
// Version is inert there).
func Rehydrate(s OrderSnapshot) *Order {
	version := s.Version
	if version == 0 {
		version = 1
	}
	return &Order{
		id: s.ID, lines: s.Lines, allowPartialShipment: s.AllowPartialShipment,
		promiseDate: s.PromiseDate, promiseCptId: s.PromiseCptID, promiseBasis: s.PromiseBasis,
		promiseGroups:  s.PromiseGroups,
		heldAtIntake:   s.HeldAtIntake,
		requiredShipBy: s.RequiredShipBy,
		version:        version,
	}
}

func (o *Order) ID() shared.OrderId         { return o.id }
func (o *Order) AllowPartialShipment() bool { return o.allowPartialShipment }

// Version returns the optimistic-concurrency-control version this
// aggregate was loaded at (or 1 for a freshly-constructed order never
// yet persisted). It is inert infrastructure metadata: no business rule
// in this package ever reads or branches on it — see the field's own
// doc comment. Only the repository's Save reads this, to guard its
// write against a lost update from a concurrent reader of the same row.
func (o *Order) Version() int { return o.version }

// SetVersion overwrites the in-memory version. It exists solely for a
// repository adapter to call immediately after a successful
// version-guarded Save, so the in-memory aggregate reflects the row it
// just persisted (version+1) rather than going stale — mirroring how
// SetRequiredShipBy lets a repository attach state after construction.
// No business logic anywhere in this service calls this; a use case
// never needs to.
func (o *Order) SetVersion(v int) { o.version = v }

// ReleaseOnAllocation reports whether this order releases its lines as
// soon as they are allocated (ADR 0020 §1). True for every order not
// explicitly held at intake, including every order persisted before
// migration 0005.
func (o *Order) ReleaseOnAllocation() bool { return !o.heldAtIntake }

// Hold marks the order as held at intake: allocate, but do not release
// until ReleaseHeldOrder says so. It is called only by the intake use
// case, on a freshly-constructed order, before any allocation — there is
// deliberately no way to hold an order that has already released work,
// because the floor cannot un-see a task it has been given.
func (o *Order) Hold() { o.heldAtIntake = true }

// RequiredShipBy returns the externally-dictated deadline, or nil when
// this order has none (the overwhelming majority).
func (o *Order) RequiredShipBy() *time.Time { return o.requiredShipBy }

// SetRequiredShipBy attaches an external deadline (ADR 0020 §2).
//
// A mutator for intake, which attaches the deadline to a freshly
// constructed order the same way it calls Hold(). A repository
// rehydrating a persisted order passes OrderSnapshot.RequiredShipBy
// instead.
func (o *Order) SetRequiredShipBy(t time.Time) { o.requiredShipBy = &t }

// Lines returns the order's lines. The slice is a copy, but the
// *OrderLine values are the aggregate's own entities: they are read-only
// from outside, since every mutating operation lives on Order.
func (o *Order) Lines() []*OrderLine {
	out := make([]*OrderLine, len(o.lines))
	copy(out, o.lines)
	return out
}

// PromiseDate is the date this order is promised for — the CPT's
// CutoffAt when the promise has a Capability basis, or the computed
// instant from LeadTimePolicy otherwise. Nil until at least one line is
// allocated. Kept as a bare time.Time on the wire and in the database for
// backward compatibility (ADR 0014 §1); see PromiseCptId/PromiseBasis
// for the rest of the Promise value.
func (o *Order) PromiseDate() *time.Time {
	if o.promiseDate == nil {
		return nil
	}
	d := *o.promiseDate
	return &d
}

// PromiseCptId is the CPT identity (e.g. "sp1-1800") the promise
// targets, when the promise has a Capability basis. Nil for a
// LeadTime-basis promise, which has no departure identity, or before any
// promise has been computed.
func (o *Order) PromiseCptId() *string {
	if o.promiseCptId == nil {
		return nil
	}
	id := *o.promiseCptId
	return &id
}

// PromiseBasis reports which policy produced the current promise
// (Capability or LeadTime). Nil before any promise has been computed.
func (o *Order) PromiseBasis() *PromiseBasis {
	if o.promiseBasis == nil {
		return nil
	}
	b := *o.promiseBasis
	return &b
}

// SetPromiseDate records a bare promise date without a CPT identity or
// basis. Kept for any caller that only has a computed instant (e.g.
// tests exercising LeadTimePolicy directly) — production allocation code
// should prefer SetPromise, which also records CptId/Basis.
func (o *Order) SetPromiseDate(d time.Time) { o.promiseDate = &d }

// SetPromise records the full Promise value ADR 0014 introduces: the
// cutoff instant (kept on promiseDate for backward compatibility), the
// CPT identity (nil for a LeadTime-basis promise), and which policy
// produced it.
//
// This method's body is deliberately left untouched by ADR 0017 (it
// predates per-group promising and every existing test exercises it
// directly): it sets ONLY the legacy summary fields, never
// promiseGroups. A caller that wants the full per-group breakdown
// recorded too should call SetPromiseGroups instead, which is real,
// additional method surface — not a replacement for this one.
func (o *Order) SetPromise(p Promise) {
	d := p.CutoffAt
	o.promiseDate = &d
	basis := p.Basis
	o.promiseBasis = &basis
	if p.CptId == "" {
		o.promiseCptId = nil
		return
	}
	cptId := p.CptId
	o.promiseCptId = &cptId
}

// PromiseGroups returns the full per-shipment-group promise breakdown
// ADR 0014 §3 / ADR 0017 introduces — the real, full-fidelity source of
// truth. A ship-complete order (or any order whose promise was set via
// SetPromiseGroups with a single group, which is what PromisePolicy.
// PromiseGroups always produces for AllowPartialShipment=false) has
// exactly one entry. Empty (nil) until SetPromiseGroups has been called
// at least once, or for an order rehydrated without a persisted group
// breakdown (a pre-ADR-0017 row, or one whose promise was set via the
// legacy SetPromise). The slice and its PromiseGroup values are copies:
// mutating the returned slice cannot corrupt the aggregate.
func (o *Order) PromiseGroups() []PromiseGroup {
	out := make([]PromiseGroup, len(o.promiseGroups))
	for i, g := range o.promiseGroups {
		lineNos := make([]int, len(g.LineNos))
		copy(lineNos, g.LineNos)
		out[i] = PromiseGroup{LineNos: lineNos, Promise: g.Promise}
	}
	return out
}

// SetPromiseGroups records the full per-shipment-group promise breakdown
// (ADR 0014 §3 / ADR 0017): groups is stored verbatim as the new,
// full-fidelity PromiseGroups() source of truth, AND the existing
// single-valued promiseDate/promiseCptId/promiseBasis fields are
// re-derived from it as a backward-compatible projection, so every
// existing reader of those three fields (the Postgres repo's write path,
// the wire event publisher, PromiseDate()/PromiseCptId()/PromiseBasis()
// themselves) keeps working unchanged.
//
// The projection rule, per ADR 0014 §3 ("the order's PromiseDate()
// becomes the latest of them"): PromiseDate() is set to the LATEST
// CutoffAt among all groups — so no existing reader ever sees an earlier
// date than the single-promise behaviour would have produced. ADR 0014
// does not specify an aggregation rule for CptId/Basis (a single string
// cannot represent N different departures), so this method makes the
// same honest choice ADR 0017 documents: PromiseCptId()/PromiseBasis()
// are projected from the SAME group whose CutoffAt is that latest one —
// i.e. all three legacy fields describe "the group with the latest
// cutoff", consistently, rather than three independently-chosen groups.
//
// Calling this with a single group covering every allocated line (what
// PromisePolicy.PromiseGroups always returns for
// AllowPartialShipment=false) reproduces SetPromise's own single-field
// assignment exactly, byte for byte — this is what makes the ship-
// complete path's behaviour provably unchanged rather than merely
// "should be the same".
func (o *Order) SetPromiseGroups(groups []PromiseGroup) {
	stored := make([]PromiseGroup, len(groups))
	for i, g := range groups {
		lineNos := make([]int, len(g.LineNos))
		copy(lineNos, g.LineNos)
		stored[i] = PromiseGroup{LineNos: lineNos, Promise: g.Promise}
	}
	o.promiseGroups = stored

	if len(groups) == 0 {
		return
	}

	latest := groups[0]
	for _, g := range groups[1:] {
		if g.Promise.CutoffAt.After(latest.Promise.CutoffAt) {
			latest = g
		}
	}
	o.SetPromise(latest.Promise)
}

// Status derives the order-level status from the line statuses. There is
// deliberately no stored Status field: a derived status cannot drift out
// of sync with the lines it summarises.
//
// The derivation, in precedence order:
//
//   - every line Cancelled                  -> Cancelled
//   - any line Released, all lines Released -> Released
//   - any line Released, some not           -> PartiallyReleased
//   - any line Backordered, ship-complete   -> Backordered  (BR3: no line
//     proceeds to release until RetryAllocation clears the backorder)
//   - any line Backordered, partial allowed, at least one line Allocated
//     -> PartiallyAllocated
//   - any line Backordered, partial allowed, nothing allocated
//     -> Backordered
//   - every line Allocated                  -> Allocated
//   - some lines Allocated, rest Pending    -> PartiallyAllocated
//   - otherwise                             -> Received
func (o *Order) Status() Status {
	var allocated, backordered, released, cancelled int
	for _, l := range o.lines {
		switch l.status {
		case LineAllocated:
			allocated++
		case LineBackordered:
			backordered++
		case LineReleased:
			released++
		case LineCancelled:
			cancelled++
		}
	}
	total := len(o.lines)

	switch {
	case cancelled == total:
		return StatusCancelled
	case released == total:
		return StatusReleased
	case released > 0:
		return StatusPartiallyReleased
	case backordered > 0:
		if o.allowPartialShipment && allocated > 0 {
			return StatusPartiallyAllocated
		}
		return StatusBackordered
	case allocated == total:
		return StatusAllocated
	case allocated > 0:
		return StatusPartiallyAllocated
	default:
		return StatusReceived
	}
}

// Allocate records inventory-storage's reservation against a Pending line.
//
// Invariants enforced here:
//   - a line cannot be allocated twice (ErrLineAlreadyAllocated)
//   - a Backordered line cannot come back this way; only RetryAllocate
//     may do that (ErrLineNotPending)
func (o *Order) Allocate(lineNo int, reservationID string) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	switch line.status {
	case LineAllocated:
		return ErrLineAlreadyAllocated
	case LinePending:
		line.status = LineAllocated
		id := reservationID
		line.reservationID = &id
		return nil
	default:
		return ErrLineNotPending
	}
}

// RetryAllocate is the ONLY transition from Backordered back to Allocated.
func (o *Order) RetryAllocate(lineNo int, reservationID string) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	if line.status != LineBackordered {
		return ErrLineNotBackordered
	}
	line.status = LineAllocated
	id := reservationID
	line.reservationID = &id
	return nil
}

// ReconfirmReservation records the reservation inventory-storage holds for
// an Allocated line at release time. Reservations expire upstream (TTL), so
// a line allocated long ago is re-reserved right before release; the id may
// be the same one (still active) or a new one (the old one expired). Only an
// Allocated line can be reconfirmed.
func (o *Order) ReconfirmReservation(lineNo int, reservationID string) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	if line.status != LineAllocated {
		return ErrLineNotAllocated
	}
	id := reservationID
	line.reservationID = &id
	return nil
}

// LoseReservation moves an Allocated line back to Backordered when its
// reservation lapsed upstream and the stock is no longer available. It
// is the only Allocated -> Backordered transition; RetryAllocate brings
// the line back once stock returns.
func (o *Order) LoseReservation(lineNo int) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	if line.status != LineAllocated {
		return ErrLineNotAllocated
	}
	line.status = LineBackordered
	line.reservationID = nil
	return nil
}

// MarkBackordered records the business fact that inventory-storage has no
// usable stock for this line (its 409). A line already Backordered stays
// Backordered — a failed retry is not an error.
func (o *Order) MarkBackordered(lineNo int) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	switch line.status {
	case LinePending, LineBackordered:
		line.status = LineBackordered
		return nil
	case LineAllocated:
		return ErrLineAlreadyAllocated
	default:
		return ErrLineNotPending
	}
}

// Release marks a line as released once wes-work-planning has accepted its
// work unit. Only an Allocated line may be released.
func (o *Order) Release(lineNo int) error {
	line, err := o.line(lineNo)
	if err != nil {
		return err
	}
	if line.status != LineAllocated {
		return ErrLineNotAllocated
	}
	line.status = LineReleased
	return nil
}

// EnsureReleasable enforces BR3 at the release boundary: an order that
// does NOT allow partial shipment may not release anything until every
// line is Allocated (or already Released). An order that allows partial
// shipment releases its allocated lines independently.
func (o *Order) EnsureReleasable() error {
	if o.allowPartialShipment {
		return nil
	}
	for _, l := range o.lines {
		if l.status != LineAllocated && l.status != LineReleased {
			return ErrShipCompleteBlocked
		}
	}
	return nil
}

// EnsureCancellable enforces BR6 without mutating anything, so a use case
// can check the boundary BEFORE revoking reservations upstream.
func (o *Order) EnsureCancellable() error {
	for _, l := range o.lines {
		if l.status == LineReleased {
			return ErrOrderAlreadyReleased
		}
	}
	return nil
}

// Cancel cancels every line. Legal only while no line has reached
// Released (BR6) — the check is repeated here so the invariant holds even
// if a caller skips EnsureCancellable.
//
// v1 deliberately does NOT claw back work already released to
// wes-work-planning; see ADR 0004's known-gap section.
func (o *Order) Cancel() error {
	if err := o.EnsureCancellable(); err != nil {
		return err
	}
	for _, l := range o.lines {
		l.status = LineCancelled
	}
	return nil
}

// AllocatedReservationIDs returns the reservation ids that CancelOrder
// must revoke on inventory-storage, in line order.
func (o *Order) AllocatedReservationIDs() []string {
	var ids []string
	for _, l := range o.lines {
		if l.status == LineAllocated && l.reservationID != nil {
			ids = append(ids, *l.reservationID)
		}
	}
	return ids
}

// LinesWithStatus returns the lines currently in the given status, in line
// order. Use cases iterate this rather than reaching into o.lines.
func (o *Order) LinesWithStatus(status LineStatus) []*OrderLine {
	var out []*OrderLine
	for _, l := range o.lines {
		if l.status == status {
			out = append(out, l)
		}
	}
	return out
}

func (o *Order) line(lineNo int) (*OrderLine, error) {
	for _, l := range o.lines {
		if l.lineNo == lineNo {
			return l, nil
		}
	}
	return nil, ErrLineNotFound
}
