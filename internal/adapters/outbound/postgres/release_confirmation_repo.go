package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/domain/releaseconfirmation"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// ReleaseConfirmationRepo is the pgxpool-backed ports.ReleaseConfirmations
// (ADR 0043), over release_confirmations (migration 0012). Every method joins
// the ctx transaction when there is one, so a row commits or rolls back with
// the release, the confirmation or the timeout batch that wrote it; outside a
// ports.UnitOfWork scope each statement runs on its own.
type ReleaseConfirmationRepo struct {
	pool *pgxpool.Pool
}

// NewReleaseConfirmationRepo constructs a ReleaseConfirmationRepo over pool.
func NewReleaseConfirmationRepo(pool *pgxpool.Pool) *ReleaseConfirmationRepo {
	return &ReleaseConfirmationRepo{pool: pool}
}

const releaseConfirmationColumns = `order_id, line_no, work_unit_id, path_id, released_at, state, confirmed_at, unconfirmed_at`

func scanReleaseConfirmation(row pgx.Row) (releaseconfirmation.Confirmation, error) {
	var (
		orderID, workUnitID, pathID, state string
		lineNo                             int
		releasedAt                         time.Time
		confirmedAt, unconfirmedAt         *time.Time
	)
	if err := row.Scan(&orderID, &lineNo, &workUnitID, &pathID, &releasedAt, &state, &confirmedAt, &unconfirmedAt); err != nil {
		return releaseconfirmation.Confirmation{}, err
	}
	return releaseconfirmation.Rehydrate(releaseconfirmation.Snapshot{
		OrderID: shared.OrderId(orderID), LineNo: lineNo, WorkUnitID: workUnitID, PathID: shared.PathId(pathID),
		ReleasedAt: releasedAt.UTC(), State: releaseconfirmation.State(state),
		ConfirmedAt: utcPtr(confirmedAt), UnconfirmedAt: utcPtr(unconfirmedAt),
	})
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// Open inserts c as a new row; an existing (order id, line no) is left
// untouched, so a repeated release never resets a line's history.
func (r *ReleaseConfirmationRepo) Open(ctx context.Context, c releaseconfirmation.Confirmation) error {
	_, err := querierFrom(ctx, r.pool).Exec(ctx, `
INSERT INTO release_confirmations (`+releaseConfirmationColumns+`)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (order_id, line_no) DO NOTHING`,
		c.OrderID().String(), c.LineNo(), c.WorkUnitID(), c.PathID().String(), c.ReleasedAt(), string(c.State()),
		c.ConfirmedAt(), c.UnconfirmedAt())
	return err
}

// Find returns the row of (orderID, lineNo). The row is read FOR UPDATE, so
// inside a transaction a confirmation and the timeout sweep serialise on it:
// whichever commits first wins, and a confirmation that loses is simply a
// late confirmation.
func (r *ReleaseConfirmationRepo) Find(ctx context.Context, orderID shared.OrderId, lineNo int) (releaseconfirmation.Confirmation, bool, error) {
	c, err := scanReleaseConfirmation(querierFrom(ctx, r.pool).QueryRow(ctx,
		`SELECT `+releaseConfirmationColumns+` FROM release_confirmations WHERE order_id = $1 AND line_no = $2 FOR UPDATE`,
		orderID.String(), lineNo))
	if errors.Is(err, pgx.ErrNoRows) {
		return releaseconfirmation.Confirmation{}, false, nil
	}
	if err != nil {
		return releaseconfirmation.Confirmation{}, false, err
	}
	return c, true, nil
}

// Save persists c's state and times over its existing row.
func (r *ReleaseConfirmationRepo) Save(ctx context.Context, c releaseconfirmation.Confirmation) error {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx, `
UPDATE release_confirmations
SET state = $3, confirmed_at = $4, unconfirmed_at = $5
WHERE order_id = $1 AND line_no = $2`,
		c.OrderID().String(), c.LineNo(), string(c.State()), c.ConfirmedAt(), c.UnconfirmedAt())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("release confirmation %s line %d: no row to save", c.OrderID(), c.LineNo())
	}
	return nil
}

// ClaimOverdue returns up to limit PENDING rows released strictly before
// releasedBefore, oldest first. FOR UPDATE SKIP LOCKED holds them for the rest
// of the transaction and skips rows another replica's sweep already holds, so
// a line is flagged exactly once however many replicas sweep.
func (r *ReleaseConfirmationRepo) ClaimOverdue(ctx context.Context, releasedBefore time.Time, limit int) ([]releaseconfirmation.Confirmation, error) {
	rows, err := querierFrom(ctx, r.pool).Query(ctx, `
SELECT `+releaseConfirmationColumns+` FROM release_confirmations
WHERE state = 'PENDING' AND released_at < $1
ORDER BY released_at, order_id, line_no
LIMIT $2
FOR UPDATE SKIP LOCKED`, releasedBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []releaseconfirmation.Confirmation
	for rows.Next() {
		c, err := scanReleaseConfirmation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ReleaseConfirmationProcessedEventsRepo is the pgxpool-backed
// ports.ReleaseConfirmationProcessedEvents, over
// release_confirmation_processed_events (migration 0012).
type ReleaseConfirmationProcessedEventsRepo struct {
	pool *pgxpool.Pool
}

// NewReleaseConfirmationProcessedEventsRepo constructs the repo over pool.
func NewReleaseConfirmationProcessedEventsRepo(pool *pgxpool.Pool) *ReleaseConfirmationProcessedEventsRepo {
	return &ReleaseConfirmationProcessedEventsRepo{pool: pool}
}

// MarkProcessed records eventId if absent, returning true iff this call newly
// recorded it. It joins the ctx transaction when there is one, which is what
// makes the claim atomic with the state change it guards; a concurrent
// duplicate blocks on the primary key until the other transaction ends.
func (r *ReleaseConfirmationProcessedEventsRepo) MarkProcessed(ctx context.Context, eventId string) (bool, error) {
	tag, err := querierFrom(ctx, r.pool).Exec(ctx,
		`INSERT INTO release_confirmation_processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`,
		eventId)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
