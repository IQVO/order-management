package usecases

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ErrInvalidReleaseConfirmation marks a ConfirmReleaseRequest that can never
// be applied (no CloudEvents id). The consumer dead-letters it rather than
// retrying.
var ErrInvalidReleaseConfirmation = errors.New("invalid release confirmation request")

// defaultFlagBatch bounds how many overdue lines FlagUnconfirmedReleases
// claims per transaction, so a large backlog is flagged in short
// transactions instead of one long lock.
const defaultFlagBatch = 100

// openReleaseConfirmations opens a PENDING confirmation row for every line
// released in THIS pass. It runs inside allocateAndRelease's unit of work,
// right where the lines move Allocated -> Released, so the rows commit or
// roll back with the release itself (ADR 0043). It is a no-op when the loop
// is off (deps.ReleaseConfirmations nil) or the pass released nothing.
func openReleaseConfirmations(ctx context.Context, deps allocationDeps, o *order.Order, released []shared.ReleasedLine) error {
	if deps.ReleaseConfirmations == nil {
		return nil
	}
	now := deps.Clock.Now()
	for _, rl := range released {
		c, err := releaseconfirmation.NewPending(o.ID(), rl.LineNo, WorkUnitID(o.ID(), rl.LineNo), rl.PathID, now)
		if err != nil {
			return err
		}
		if err := deps.ReleaseConfirmations.Open(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// ConfirmReleaseRequest is one decoded WorkUnitCreated. Reference and LineNo
// are the two OPTIONAL fields wes-work-planning stamps on the event (ADR
// 0043 / its ADR 0041); either may be absent.
type ConfirmReleaseRequest struct {
	// EventID is the CloudEvents id, the idempotency key.
	EventID string
	// Reference is the work unit's originating reference; for order work,
	// the order id. Empty when the producer did not send it.
	Reference string
	// LineNo is the order line number; 0 when the producer omitted it.
	LineNo int
}

// ConfirmRelease handles one WorkUnitCreated from wes-work-planning: it sets
// the matching PENDING or UNCONFIRMED confirmation row to CONFIRMED.
//
// An event without a reference or a positive line number, or whose
// (reference, line) is not a line this context opened a confirmation for (a
// transfer, RELOCATE or legacy work unit, an order released before the
// feature), is IGNORED and counted, never an error. The CloudEvents id claim
// and the state change run in ONE UnitOfWork, so a rollback un-claims the id
// and the redelivery is processed. An ignored event claims nothing.
//
// Confirmation is idempotent: a redelivered id, or a second event for a line
// already CONFIRMED, changes nothing. A late confirmation of an UNCONFIRMED
// line is allowed and clears the problem.
type ConfirmRelease struct {
	Confirmations ports.ReleaseConfirmations
	Processed     ports.ReleaseConfirmationProcessedEvents
	Clock         ports.Clock
	// UnitOfWork brackets the claim and the state change. Optional: nil
	// means no transactional backing (in-memory dev), like every use case.
	UnitOfWork ports.UnitOfWork
	// Metrics counts each handled event by outcome. Optional.
	Metrics ports.ReleaseConfirmationMetrics
	// Logger receives the late-confirmation line. Optional.
	Logger *slog.Logger
}

// Execute handles req and reports what came of it.
func (uc *ConfirmRelease) Execute(ctx context.Context, req ConfirmReleaseRequest) (ports.ReleaseConfirmationOutcome, error) {
	if req.EventID == "" {
		return "", ErrInvalidReleaseConfirmation
	}
	if req.Reference == "" || req.LineNo <= 0 {
		uc.record(ctx, ports.ReleaseConfirmationIgnoredNoReference)
		return ports.ReleaseConfirmationIgnoredNoReference, nil
	}

	var outcome ports.ReleaseConfirmationOutcome
	var late releaseconfirmation.Confirmation
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		c, found, err := uc.Confirmations.Find(ctx, shared.OrderId(req.Reference), req.LineNo)
		if err != nil {
			return err
		}
		if !found {
			outcome = ports.ReleaseConfirmationIgnoredUnknownLine
			return nil
		}
		first, err := uc.Processed.MarkProcessed(ctx, req.EventID)
		if err != nil {
			return err
		}
		if !first {
			outcome = ports.ReleaseConfirmationDuplicate
			return nil
		}
		result := c.Confirm(uc.Clock.Now())
		if result == releaseconfirmation.AlreadyConfirmed {
			outcome = ports.ReleaseConfirmationDuplicate
			return nil
		}
		if err := uc.Confirmations.Save(ctx, c); err != nil {
			return err
		}
		outcome = ports.ReleaseConfirmationConfirmed
		if result == releaseconfirmation.ConfirmedLate {
			outcome = ports.ReleaseConfirmationLateConfirmed
			late = c
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	// Reported only after the transaction committed, so a rolled-back
	// attempt that is retried is counted once.
	if outcome == ports.ReleaseConfirmationLateConfirmed {
		uc.logLate(ctx, late)
	}
	uc.record(ctx, outcome)
	return outcome, nil
}

func (uc *ConfirmRelease) record(ctx context.Context, outcome ports.ReleaseConfirmationOutcome) {
	if uc.Metrics != nil {
		uc.Metrics.Handled(ctx, outcome)
	}
}

func (uc *ConfirmRelease) logLate(ctx context.Context, c releaseconfirmation.Confirmation) {
	logger := uc.Logger
	if logger == nil {
		logger = slog.Default()
	}
	flagged := ""
	if at := c.UnconfirmedAt(); at != nil {
		flagged = at.UTC().Format(time.RFC3339)
	}
	logger.WarnContext(ctx, "release confirmed late: the line flagged unconfirmed was admitted after all",
		"order_id", c.OrderID().String(), "line_no", c.LineNo(), "work_unit_id", c.WorkUnitID(),
		"released_at", c.ReleasedAt().UTC().Format(time.RFC3339), "unconfirmed_at", flagged)
}

// FlagUnconfirmedReleases is the timeout sweep of ADR 0043: PENDING lines
// released more than Timeout ago become UNCONFIRMED, exactly once, and each
// raises ONE analytics-only OrderReleaseUnconfirmed fact in the same unit of
// work as the state change.
//
// The outcome is a fact, never an action: nothing is cancelled, re-released
// or retried. The postgres Sweeper (ADR 0032) runs it each pass; there is no
// second scheduler.
type FlagUnconfirmedReleases struct {
	Confirmations ports.ReleaseConfirmations
	Events        ports.EventPublisher
	Clock         ports.Clock
	// UnitOfWork brackets each batch's claim, state change and outbox rows.
	UnitOfWork ports.UnitOfWork
	// Metrics counts each flagged line. Optional.
	Metrics ports.ReleaseConfirmationMetrics
	// Logger receives the ERROR line per flagged release. Optional.
	Logger *slog.Logger
	// Timeout is how long a line may stay PENDING. Zero or negative
	// disables the sweep.
	Timeout time.Duration
	// BatchSize bounds the lines claimed per transaction; zero means the
	// default.
	BatchSize int
}

// Execute flags every overdue line, batch by batch, and returns how many it
// flagged. With a non-positive Timeout it does nothing.
func (uc *FlagUnconfirmedReleases) Execute(ctx context.Context) (int, error) {
	if uc.Timeout <= 0 {
		return 0, nil
	}
	batch := uc.BatchSize
	if batch <= 0 {
		batch = defaultFlagBatch
	}
	total := 0
	for {
		n, err := uc.flagBatch(ctx, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < batch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

// flagBatch claims, flags and announces up to limit overdue lines in one
// transaction. The metric and the ERROR log are emitted only after the
// commit, so a rolled-back batch that the next pass re-flags is reported
// once.
func (uc *FlagUnconfirmedReleases) flagBatch(ctx context.Context, limit int) (int, error) {
	now := uc.Clock.Now()
	cutoff := now.Add(-uc.Timeout)

	var flagged []releaseconfirmation.Confirmation
	err := atomically(ctx, uc.UnitOfWork, func(ctx context.Context) error {
		flagged = flagged[:0]
		overdue, err := uc.Confirmations.ClaimOverdue(ctx, cutoff, limit)
		if err != nil {
			return err
		}
		for _, c := range overdue {
			if !c.MarkUnconfirmed(now) {
				continue
			}
			if err := uc.Confirmations.Save(ctx, c); err != nil {
				return err
			}
			if err := uc.Events.Publish(ctx, shared.NewOrderReleaseUnconfirmed(
				now, c.OrderID(), c.LineNo(), c.PathID(), c.WorkUnitID(), c.ReleasedAt(),
			)); err != nil {
				return err
			}
			flagged = append(flagged, c)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, c := range flagged {
		uc.report(ctx, c)
	}
	return len(flagged), nil
}

func (uc *FlagUnconfirmedReleases) report(ctx context.Context, c releaseconfirmation.Confirmation) {
	if uc.Metrics != nil {
		uc.Metrics.LineUnconfirmed(ctx)
	}
	logger := uc.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.ErrorContext(ctx, "release unconfirmed",
		"order_id", c.OrderID().String(), "line_no", c.LineNo(), "path_id", c.PathID().String(),
		"work_unit_id", c.WorkUnitID(), "released_at", c.ReleasedAt().UTC().Format(time.RFC3339),
		"timeout", uc.Timeout.String())
}
