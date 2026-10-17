// Package releaseconfirmation is the process manager behind ADR 0043: it
// tracks, per released order line, whether wes-work-planning confirmed the
// release by raising WorkUnitCreated.
//
// It is deliberately NOT part of the Order aggregate. Whether another
// bounded context admitted the work is knowledge about that context's
// behaviour, not an invariant of the order, and a late or missing
// confirmation must never change an order's status or block a transition.
// The package imports only the shared kernel; it knows nothing of Kafka,
// SQL or the clock (every time is passed in).
package releaseconfirmation

import (
	"errors"
	"fmt"
	"time"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// ErrInvalidConfirmation marks a Confirmation that cannot exist: an empty
// order id or work unit id, a non-positive line number, no release time or
// an unknown state.
var ErrInvalidConfirmation = errors.New("invalid release confirmation")

// State is where a released line is in the confirmation loop.
type State string

const (
	// StatePending: released, no WorkUnitCreated seen yet.
	StatePending State = "PENDING"
	// StateConfirmed: wes-work-planning confirmed the work was enqueued.
	StateConfirmed State = "CONFIRMED"
	// StateUnconfirmed: the confirmation timeout passed with no
	// confirmation. A late WorkUnitCreated still moves it to CONFIRMED.
	StateUnconfirmed State = "UNCONFIRMED"
)

// valid reports whether s is one of the three states.
func (s State) valid() bool {
	if s == StatePending {
		return true
	}
	if s == StateConfirmed {
		return true
	}
	return s == StateUnconfirmed
}

// ConfirmResult says what Confirm did.
type ConfirmResult int

const (
	// ConfirmedNow: a PENDING line became CONFIRMED.
	ConfirmedNow ConfirmResult = iota + 1
	// ConfirmedLate: an UNCONFIRMED line became CONFIRMED (a late
	// confirmation clears the problem).
	ConfirmedLate
	// AlreadyConfirmed: the line was already CONFIRMED; nothing changed.
	AlreadyConfirmed
)

// Confirmation is the process-manager row of one released order line.
// Mutate it only through Confirm and MarkUnconfirmed.
type Confirmation struct {
	orderID       shared.OrderId
	lineNo        int
	workUnitID    string
	pathID        shared.PathId
	releasedAt    time.Time
	state         State
	confirmedAt   time.Time
	hasConfirmed  bool
	unconfirmedAt time.Time
	hasUnconfirm  bool
}

// NewPending opens the confirmation of a line released at releasedAt.
func NewPending(orderID shared.OrderId, lineNo int, workUnitID string, pathID shared.PathId, releasedAt time.Time) (Confirmation, error) {
	if err := validateIdentity(orderID, lineNo, workUnitID, releasedAt); err != nil {
		return Confirmation{}, err
	}
	return Confirmation{
		orderID: orderID, lineNo: lineNo, workUnitID: workUnitID, pathID: pathID,
		releasedAt: releasedAt, state: StatePending,
	}, nil
}

func validateIdentity(orderID shared.OrderId, lineNo int, workUnitID string, releasedAt time.Time) error {
	if orderID == "" {
		return fmt.Errorf("%w: empty order id", ErrInvalidConfirmation)
	}
	if lineNo <= 0 {
		return fmt.Errorf("%w: line number %d must be positive", ErrInvalidConfirmation, lineNo)
	}
	if workUnitID == "" {
		return fmt.Errorf("%w: empty work unit id", ErrInvalidConfirmation)
	}
	if releasedAt.IsZero() {
		return fmt.Errorf("%w: no release time", ErrInvalidConfirmation)
	}
	return nil
}

// Snapshot is the persisted shape of a Confirmation, used to rebuild one.
type Snapshot struct {
	OrderID       shared.OrderId
	LineNo        int
	WorkUnitID    string
	PathID        shared.PathId
	ReleasedAt    time.Time
	State         State
	ConfirmedAt   *time.Time
	UnconfirmedAt *time.Time
}

// Rehydrate rebuilds a Confirmation from its persisted snapshot. The times
// are copied, so the caller's variables never alias the row.
func Rehydrate(s Snapshot) (Confirmation, error) {
	if err := validateIdentity(s.OrderID, s.LineNo, s.WorkUnitID, s.ReleasedAt); err != nil {
		return Confirmation{}, err
	}
	if !s.State.valid() {
		return Confirmation{}, fmt.Errorf("%w: unknown state %q", ErrInvalidConfirmation, s.State)
	}
	c := Confirmation{
		orderID: s.OrderID, lineNo: s.LineNo, workUnitID: s.WorkUnitID, pathID: s.PathID,
		releasedAt: s.ReleasedAt, state: s.State,
	}
	if s.ConfirmedAt != nil {
		c.confirmedAt, c.hasConfirmed = *s.ConfirmedAt, true
	}
	if s.UnconfirmedAt != nil {
		c.unconfirmedAt, c.hasUnconfirm = *s.UnconfirmedAt, true
	}
	return c, nil
}

func (c Confirmation) OrderID() shared.OrderId { return c.orderID }
func (c Confirmation) LineNo() int             { return c.lineNo }
func (c Confirmation) WorkUnitID() string      { return c.workUnitID }
func (c Confirmation) PathID() shared.PathId   { return c.pathID }
func (c Confirmation) ReleasedAt() time.Time   { return c.releasedAt }
func (c Confirmation) State() State            { return c.state }

// ConfirmedAt returns a copy of the confirmation time, nil if never
// confirmed.
func (c Confirmation) ConfirmedAt() *time.Time {
	if !c.hasConfirmed {
		return nil
	}
	t := c.confirmedAt
	return &t
}

// UnconfirmedAt returns a copy of the time the line was flagged, nil if it
// never was. It is kept after a late confirmation, as the audit trail.
func (c Confirmation) UnconfirmedAt() *time.Time {
	if !c.hasUnconfirm {
		return nil
	}
	t := c.unconfirmedAt
	return &t
}

// Confirm records that wes-work-planning admitted the work, at time at. It
// is idempotent: a second call changes nothing and keeps the first
// confirmation time. Confirming an UNCONFIRMED line is allowed (a late
// confirmation) and keeps the unconfirmed time.
func (c *Confirmation) Confirm(at time.Time) ConfirmResult {
	if c.state == StateConfirmed {
		return AlreadyConfirmed
	}
	late := c.state == StateUnconfirmed
	c.state = StateConfirmed
	c.confirmedAt, c.hasConfirmed = at, true
	if late {
		return ConfirmedLate
	}
	return ConfirmedNow
}

// MarkUnconfirmed flags a PENDING line whose confirmation timed out, at
// time at. It reports whether the line changed: only a PENDING line can
// become UNCONFIRMED, and it does so exactly once.
func (c *Confirmation) MarkUnconfirmed(at time.Time) bool {
	if c.state != StatePending {
		return false
	}
	c.state = StateUnconfirmed
	c.unconfirmedAt, c.hasUnconfirm = at, true
	return true
}

// Overdue reports whether the line is PENDING and was released strictly
// before cutoff. A line released exactly at the cutoff is not yet overdue.
func (c Confirmation) Overdue(cutoff time.Time) bool {
	if c.state != StatePending {
		return false
	}
	return c.releasedAt.Before(cutoff)
}
