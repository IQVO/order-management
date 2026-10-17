package releaseconfirmation_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

var (
	t0      = time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	timeout = 15 * time.Minute
)

func pending(t *testing.T) releaseconfirmation.Confirmation {
	t.Helper()
	c, err := releaseconfirmation.NewPending("ord-1", 2, "ord-1-line-2", "pick", t0)
	if err != nil {
		t.Fatalf("NewPending: %v", err)
	}
	return c
}

func TestNewPending_StartsPendingWithItsIdentity(t *testing.T) {
	c := pending(t)
	if c.State() != releaseconfirmation.StatePending {
		t.Fatalf("State = %q, want PENDING", c.State())
	}
	if c.OrderID() != "ord-1" || c.LineNo() != 2 || c.WorkUnitID() != "ord-1-line-2" || c.PathID() != "pick" {
		t.Fatalf("identity = %s/%d/%s/%s", c.OrderID(), c.LineNo(), c.WorkUnitID(), c.PathID())
	}
	if !c.ReleasedAt().Equal(t0) {
		t.Fatalf("ReleasedAt = %v, want %v", c.ReleasedAt(), t0)
	}
	if c.ConfirmedAt() != nil || c.UnconfirmedAt() != nil {
		t.Fatal("a new PENDING row has neither a confirmed nor an unconfirmed time")
	}
}

func TestNewPending_RejectsAnIncompleteIdentity(t *testing.T) {
	tests := []struct {
		name       string
		orderID    shared.OrderId
		lineNo     int
		workUnitID string
		releasedAt time.Time
	}{
		{"empty order id", "", 1, "wu", t0},
		{"line number zero", "ord-1", 0, "wu", t0},
		{"negative line number", "ord-1", -1, "wu", t0},
		{"empty work unit id", "ord-1", 1, "", t0},
		{"zero released-at", "ord-1", 1, "wu", time.Time{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := releaseconfirmation.NewPending(tt.orderID, tt.lineNo, tt.workUnitID, "pick", tt.releasedAt)
			if !errors.Is(err, releaseconfirmation.ErrInvalidConfirmation) {
				t.Fatalf("err = %v, want ErrInvalidConfirmation", err)
			}
		})
	}
}

func TestConfirm_PendingBecomesConfirmed(t *testing.T) {
	c := pending(t)
	at := t0.Add(3 * time.Second)

	if got := c.Confirm(at); got != releaseconfirmation.ConfirmedNow {
		t.Fatalf("Confirm = %v, want ConfirmedNow", got)
	}
	if c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
	if got := c.ConfirmedAt(); got == nil || !got.Equal(at) {
		t.Fatalf("ConfirmedAt = %v, want %v", got, at)
	}
	if c.UnconfirmedAt() != nil {
		t.Fatal("a line confirmed in time was never unconfirmed")
	}
}

func TestConfirm_DuplicateIsANoOpAndKeepsTheFirstConfirmationTime(t *testing.T) {
	c := pending(t)
	first := t0.Add(3 * time.Second)
	c.Confirm(first)

	if got := c.Confirm(first.Add(time.Hour)); got != releaseconfirmation.AlreadyConfirmed {
		t.Fatalf("second Confirm = %v, want AlreadyConfirmed", got)
	}
	if got := c.ConfirmedAt(); got == nil || !got.Equal(first) {
		t.Fatalf("ConfirmedAt = %v, want the first confirmation %v", got, first)
	}
	if c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
}

func TestMarkUnconfirmed_PendingBecomesUnconfirmedExactlyOnce(t *testing.T) {
	c := pending(t)
	at := t0.Add(timeout + time.Minute)

	if !c.MarkUnconfirmed(at) {
		t.Fatal("a PENDING row must become UNCONFIRMED")
	}
	if c.State() != releaseconfirmation.StateUnconfirmed {
		t.Fatalf("State = %q, want UNCONFIRMED", c.State())
	}
	if got := c.UnconfirmedAt(); got == nil || !got.Equal(at) {
		t.Fatalf("UnconfirmedAt = %v, want %v", got, at)
	}

	if c.MarkUnconfirmed(at.Add(time.Hour)) {
		t.Fatal("an UNCONFIRMED row must not be flagged a second time")
	}
	if got := c.UnconfirmedAt(); got == nil || !got.Equal(at) {
		t.Fatalf("UnconfirmedAt moved to %v on the second call, want it to stay %v", got, at)
	}
}

func TestMarkUnconfirmed_ConfirmedStaysConfirmed(t *testing.T) {
	c := pending(t)
	c.Confirm(t0.Add(time.Second))

	if c.MarkUnconfirmed(t0.Add(timeout * 2)) {
		t.Fatal("a CONFIRMED row must never be flagged unconfirmed")
	}
	if c.State() != releaseconfirmation.StateConfirmed || c.UnconfirmedAt() != nil {
		t.Fatalf("state=%q unconfirmedAt=%v, want CONFIRMED and none", c.State(), c.UnconfirmedAt())
	}
}

func TestConfirm_LateConfirmationAfterUnconfirmedClearsTheProblem(t *testing.T) {
	c := pending(t)
	flagged := t0.Add(timeout + time.Minute)
	late := flagged.Add(10 * time.Minute)
	c.MarkUnconfirmed(flagged)

	if got := c.Confirm(late); got != releaseconfirmation.ConfirmedLate {
		t.Fatalf("Confirm = %v, want ConfirmedLate", got)
	}
	if c.State() != releaseconfirmation.StateConfirmed {
		t.Fatalf("State = %q, want CONFIRMED", c.State())
	}
	if got := c.ConfirmedAt(); got == nil || !got.Equal(late) {
		t.Fatalf("ConfirmedAt = %v, want %v", got, late)
	}
	// The history of having been flagged is kept for the audit trail.
	if got := c.UnconfirmedAt(); got == nil || !got.Equal(flagged) {
		t.Fatalf("UnconfirmedAt = %v, want it kept as %v", got, flagged)
	}
	// And it is idempotent from here on.
	if got := c.Confirm(late.Add(time.Hour)); got != releaseconfirmation.AlreadyConfirmed {
		t.Fatalf("Confirm after the late one = %v, want AlreadyConfirmed", got)
	}
}

func TestOverdue_OnlyAPendingRowStrictlyOlderThanTheCutoff(t *testing.T) {
	cutoff := t0.Add(time.Minute)

	// Released at t0, cutoff t0+1m: older than the cutoff.
	if !pending(t).Overdue(cutoff) {
		t.Fatal("a PENDING row released before the cutoff is overdue")
	}
	// Exactly at the cutoff is NOT older than it.
	if pending(t).Overdue(t0) {
		t.Fatal("a row released exactly at the cutoff is not yet overdue")
	}
	// One nanosecond past the release instant is.
	if !pending(t).Overdue(t0.Add(time.Nanosecond)) {
		t.Fatal("a row released one nanosecond before the cutoff is overdue")
	}
	// Younger than the cutoff.
	if pending(t).Overdue(t0.Add(-time.Second)) {
		t.Fatal("a row released after the cutoff is not overdue")
	}

	confirmed := pending(t)
	confirmed.Confirm(t0.Add(time.Second))
	if confirmed.Overdue(cutoff) {
		t.Fatal("a CONFIRMED row is never overdue")
	}
	flagged := pending(t)
	flagged.MarkUnconfirmed(cutoff)
	if flagged.Overdue(cutoff.Add(time.Hour)) {
		t.Fatal("an UNCONFIRMED row is already flagged, so never overdue again")
	}
}

func TestRehydrate_RebuildsEveryStateAndRejectsAnUnknownOne(t *testing.T) {
	confirmedAt := t0.Add(time.Second)
	unconfirmedAt := t0.Add(timeout)
	for _, tt := range []struct {
		name  string
		state releaseconfirmation.State
	}{
		{"pending", releaseconfirmation.StatePending},
		{"confirmed", releaseconfirmation.StateConfirmed},
		{"unconfirmed", releaseconfirmation.StateUnconfirmed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := releaseconfirmation.Rehydrate(releaseconfirmation.Snapshot{
				OrderID: "ord-1", LineNo: 2, WorkUnitID: "ord-1-line-2", PathID: "pick", ReleasedAt: t0,
				State: tt.state, ConfirmedAt: &confirmedAt, UnconfirmedAt: &unconfirmedAt,
			})
			if err != nil {
				t.Fatalf("Rehydrate: %v", err)
			}
			if c.State() != tt.state || c.OrderID() != "ord-1" || c.LineNo() != 2 {
				t.Fatalf("rehydrated = %s %s/%d", c.State(), c.OrderID(), c.LineNo())
			}
			if got := c.ConfirmedAt(); got == nil || !got.Equal(confirmedAt) {
				t.Fatalf("ConfirmedAt = %v", got)
			}
			if got := c.UnconfirmedAt(); got == nil || !got.Equal(unconfirmedAt) {
				t.Fatalf("UnconfirmedAt = %v", got)
			}
		})
	}

	_, err := releaseconfirmation.Rehydrate(releaseconfirmation.Snapshot{
		OrderID: "ord-1", LineNo: 2, WorkUnitID: "wu", ReleasedAt: t0, State: "LOST",
	})
	if !errors.Is(err, releaseconfirmation.ErrInvalidConfirmation) {
		t.Fatalf("an unknown state: err = %v, want ErrInvalidConfirmation", err)
	}
	_, err = releaseconfirmation.Rehydrate(releaseconfirmation.Snapshot{State: releaseconfirmation.StatePending})
	if !errors.Is(err, releaseconfirmation.ErrInvalidConfirmation) {
		t.Fatalf("an empty identity: err = %v, want ErrInvalidConfirmation", err)
	}
}

func TestRehydrate_CopiesTheTimesSoACallerCannotMutateTheRow(t *testing.T) {
	confirmedAt := t0.Add(time.Second)
	c, err := releaseconfirmation.Rehydrate(releaseconfirmation.Snapshot{
		OrderID: "ord-1", LineNo: 1, WorkUnitID: "wu", ReleasedAt: t0,
		State: releaseconfirmation.StateConfirmed, ConfirmedAt: &confirmedAt,
	})
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	confirmedAt = confirmedAt.Add(time.Hour)
	if got := c.ConfirmedAt(); got == nil || got.Equal(confirmedAt) {
		t.Fatalf("ConfirmedAt = %v aliases the caller's variable", got)
	}
	got := c.ConfirmedAt()
	*got = got.Add(time.Hour)
	if again := c.ConfirmedAt(); again.Equal(*got) {
		t.Fatal("ConfirmedAt hands out a pointer into the aggregate")
	}
}
