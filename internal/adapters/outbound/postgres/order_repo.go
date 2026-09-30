package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// OrderRepo is a pgxpool-backed implementation of ports.OrderRepo. The
// Order aggregate is stored across three tables (orders, order_lines,
// order_promise_groups) and always written in a single transaction,
// because a line's status and its order are one unit of consistency.
type OrderRepo struct {
	pool *pgxpool.Pool
}

// NewOrderRepo constructs an OrderRepo over pool.
func NewOrderRepo(pool *pgxpool.Pool) *OrderRepo {
	return &OrderRepo{pool: pool}
}

// Save opens its own transaction for the three-table write, UNLESS ctx
// already carries one (i.e. this call runs inside a
// ports.UnitOfWork.Execute scope) — in that case it joins the outer
// transaction via beginOrJoin, so the aggregate write and the outbox
// insert(s) made by the same use case commit together or not at all
// (the transactional outbox).
//
// The orders-table write is optimistic-concurrency-guarded (see the
// version-column ADR): an existing row is only updated when its
// current version matches the version this aggregate was loaded at
// (o.Version()), and the write bumps the stored version by exactly one.
// This check runs FIRST, before order_lines or order_promise_groups is
// touched at all — a stale-version Save returns
// ports.ErrConcurrentModification and rolls back before any line write
// is even attempted, so a lost writer's Save never partially applies.
//
// The write is an explicit UPDATE-then-INSERT pair, not a single
// `INSERT ... ON CONFLICT DO UPDATE ... WHERE version = $x` statement.
// Both forms were considered; the explicit pair is what this repo
// ships because it makes the three real outcomes (fresh insert,
// version-matched update, version-mismatched conflict) three distinct,
// individually-verified code paths rather than one statement whose
// RowsAffected()==0 case conflates "no row with this id yet" with "row
// exists but version didn't match" — see
// TestOrderRepo_Save_VersionGuard_* in order_repo_integration_test.go,
// which exercises all three against a real Postgres.
func (r *OrderRepo) Save(ctx context.Context, o *order.Order) error {
	tx, commit, rollback, err := beginOrJoin(ctx, r.pool)
	if err != nil {
		return err
	}
	defer func() { _ = rollback(ctx) }()

	var promiseDate *time.Time
	if d := o.PromiseDate(); d != nil {
		promiseDate = d
	}
	promiseCptId := o.PromiseCptId()
	var promiseBasis *string
	if b := o.PromiseBasis(); b != nil {
		s := b.String()
		promiseBasis = &s
	}

	// Version-guarded UPDATE first: this is the common case (an order
	// that already exists — every Save after the very first one for a
	// given id). A row matching id AND o.Version() gets its version
	// bumped by exactly one in the same statement.
	var insertedFresh bool
	tag, err := tx.Exec(ctx, `
		UPDATE orders SET
			promise_date = $3,
			promise_cpt_id = $4,
			promise_basis = $5,
			release_on_allocation = $6,
			required_ship_by = $7,
			version = version + 1
		WHERE id = $1 AND version = $2
	`, o.ID().String(), o.Version(), promiseDate, promiseCptId, promiseBasis, o.ReleaseOnAllocation(), o.RequiredShipBy())
	if err != nil {
		return err
	}

	switch tag.RowsAffected() {
	case 1:
		// Version matched and was bumped by the UPDATE itself.
	case 0:
		insertedFresh, err = r.upsertMissingOrderRow(ctx, tx, o, promiseDate, promiseCptId, promiseBasis)
		if err != nil {
			return err
		}
	default:
		// Cannot happen: id is the primary key, so at most one row can
		// ever match. Guarded defensively rather than silently ignored.
		return fmt.Errorf("postgres: orders version-guarded update affected %d rows for id %s, want 0 or 1", tag.RowsAffected(), o.ID().String())
	}

	for _, l := range o.Lines() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_lines (order_id, line_no, sku, quantity, path_id, gift_wrap, line_status, reservation_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (order_id, line_no) DO UPDATE
			  SET line_status = EXCLUDED.line_status,
			      reservation_id = EXCLUDED.reservation_id
		`, o.ID().String(), l.LineNo(), l.SKU().String(), l.Quantity(), l.PathID().String(), l.GiftWrap(), string(l.Status()), l.ReservationID()); err != nil {
			return err
		}
	}

	// order_promise_groups (ADR 0014 §3 / ADR 0017): delete-then-reinsert
	// this order's full group set on every save, rather than diff/upsert
	// a breakdown whose shape (how many groups, which lines are in each)
	// can change entirely between allocation passes as more lines
	// allocate or a saturated path pushes a line to a different cutoff.
	if err := persistPromiseGroups(ctx, tx, o); err != nil {
		return err
	}

	if err := commit(ctx); err != nil {
		return err
	}
	// Reflect the version this Save just persisted onto the in-memory
	// aggregate, so a caller that keeps using the same *order.Order
	// after Save (e.g. a use case that returns it to its own caller)
	// is not left holding a stale version that would spuriously
	// conflict with itself on a later Save within the same process. A
	// fresh insert stored o.Version() itself (typically 1, from New);
	// an existing row's update bumped the STORED version by one via
	// `version = version + 1`, so the in-memory value must move to
	// match.
	if !insertedFresh {
		o.SetVersion(o.Version() + 1)
	}
	return nil
}

// persistPromiseGroups delete-then-reinserts the order's full
// order_promise_groups set, in group order (ADR 0014 §3 / ADR 0017).
func persistPromiseGroups(ctx context.Context, tx pgx.Tx, o *order.Order) error {
	if _, err := tx.Exec(ctx, `DELETE FROM order_promise_groups WHERE order_id = $1`, o.ID().String()); err != nil {
		return err
	}
	for i, g := range o.PromiseGroups() {
		var cptId *string
		if g.Promise.CptId != "" {
			id := g.Promise.CptId
			cptId = &id
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_promise_groups (order_id, group_no, line_nos, cpt_id, cutoff_at, basis)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, o.ID().String(), i+1, g.LineNos, cptId, g.Promise.CutoffAt, g.Promise.Basis.String()); err != nil {
			return err
		}
	}
	return nil
}

// upsertMissingOrderRow handles the version-guarded UPDATE's 0-rows case,
// which is ambiguous by RowsAffected alone: either no row with this id
// exists yet (a brand-new order's first Save), or a row exists at some
// OTHER version (a lost race). It disambiguates with a direct existence
// check, returning ErrConcurrentModification on the lost race (before
// touching order_lines/order_promise_groups at all) or inserting the
// fresh row and reporting insertedFresh=true.
func (r *OrderRepo) upsertMissingOrderRow(ctx context.Context, tx pgx.Tx, o *order.Order, promiseDate *time.Time, promiseCptId *string, promiseBasis *string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE id = $1)`, o.ID().String()).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		// A row is there, just not at the version we loaded —
		// some other writer already advanced it. Return before
		// touching order_lines/order_promise_groups at all.
		return false, ports.ErrConcurrentModification
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orders (id, allow_partial_shipment, promise_date, promise_cpt_id, promise_basis, release_on_allocation, required_ship_by, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, o.ID().String(), o.AllowPartialShipment(), promiseDate, promiseCptId, promiseBasis, o.ReleaseOnAllocation(), o.RequiredShipBy(), o.Version()); err != nil {
		return false, err
	}
	return true, nil
}

// FindByID returns (nil, nil) when no order has this id — "not found" is
// the application's concern, not the repository's.
//
// It reads through querierFrom rather than r.pool directly so that a
// call made from INSIDE a UnitOfWork scope (e.g. the analytics
// publisher's path-enrichment lookup, called from Encode while the same
// use case's Save is still uncommitted) sees the just-saved row rather
// than racing its own not-yet-committed transaction under READ COMMITTED
// isolation.
func (r *OrderRepo) FindByID(ctx context.Context, id shared.OrderId) (*order.Order, error) {
	q := querierFrom(ctx, r.pool)

	header, err := fetchOrderHeader(ctx, q, id)
	if err != nil || header == nil {
		return nil, err
	}

	lines, err := fetchOrderLines(ctx, q, id)
	if err != nil {
		return nil, err
	}

	promiseGroups, err := fetchPromiseGroups(ctx, q, id)
	if err != nil {
		return nil, err
	}

	o := order.RehydrateHeld(id, lines, header.allowPartialShipment, header.promiseDate, header.promiseCptId, header.promiseBasis, promiseGroups, header.releaseOnAllocation, header.version)
	// Set after rehydration rather than as a fourth Rehydrate parameter:
	// the deadline is optional and most orders have none, so widening the
	// constructor chain again would cost every call site an argument it
	// does not care about.
	if header.requiredShipBy != nil {
		o.SetRequiredShipBy(*header.requiredShipBy)
	}
	return o, nil
}

// orderHeader holds the orders-table columns FindByID scans, before they
// are folded into the rehydrated aggregate — including the optimistic-
// concurrency version (ADR: version column on Order).
type orderHeader struct {
	allowPartialShipment bool
	promiseDate          *time.Time
	promiseCptId         *string
	promiseBasis         *order.PromiseBasis
	releaseOnAllocation  bool
	requiredShipBy       *time.Time
	version              int
}

// fetchOrderHeader reads the single orders row for id through q (which
// may be a UnitOfWork transaction — see FindByID's doc comment). It
// returns (nil, nil) when no such order exists — FindByID's not-found
// contract.
func fetchOrderHeader(ctx context.Context, q querier, id shared.OrderId) (*orderHeader, error) {
	var promiseBasisRaw *string
	h := &orderHeader{}
	err := q.QueryRow(ctx, `
		SELECT allow_partial_shipment, promise_date, promise_cpt_id, promise_basis, release_on_allocation, required_ship_by, version FROM orders WHERE id = $1
	`, id.String()).Scan(&h.allowPartialShipment, &h.promiseDate, &h.promiseCptId, &promiseBasisRaw, &h.releaseOnAllocation, &h.requiredShipBy, &h.version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if promiseBasisRaw != nil {
		b := order.PromiseBasis(*promiseBasisRaw)
		h.promiseBasis = &b
	}
	return h, nil
}

// fetchOrderLines reads the order_lines rows for id in line order,
// rehydrating each into a domain OrderLine.
func fetchOrderLines(ctx context.Context, q querier, id shared.OrderId) ([]*order.OrderLine, error) {
	rows, err := q.Query(ctx, `
		SELECT line_no, sku, quantity, path_id, gift_wrap, line_status, reservation_id
		FROM order_lines WHERE order_id = $1 ORDER BY line_no
	`, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lines []*order.OrderLine
	for rows.Next() {
		var (
			lineNo        int
			sku           string
			quantity      int
			pathID        string
			giftWrap      bool
			lineStatus    string
			reservationID *string
		)
		if err := rows.Scan(&lineNo, &sku, &quantity, &pathID, &giftWrap, &lineStatus, &reservationID); err != nil {
			return nil, err
		}
		lines = append(lines, order.RehydrateOrderLine(
			lineNo, shared.SKU(sku), quantity, shared.PathId(pathID), giftWrap,
			order.LineStatus(lineStatus), reservationID,
		))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

// fetchPromiseGroups reads the order_promise_groups rows for id in
// group order (ADR 0014 §3 / ADR 0017).
func fetchPromiseGroups(ctx context.Context, q querier, id shared.OrderId) ([]order.PromiseGroup, error) {
	groupRows, err := q.Query(ctx, `
		SELECT line_nos, cpt_id, cutoff_at, basis
		FROM order_promise_groups WHERE order_id = $1 ORDER BY group_no
	`, id.String())
	if err != nil {
		return nil, err
	}
	defer groupRows.Close()

	var promiseGroups []order.PromiseGroup
	for groupRows.Next() {
		var (
			lineNos  []int
			cptId    *string
			cutoffAt time.Time
			basisRaw string
		)
		if err := groupRows.Scan(&lineNos, &cptId, &cutoffAt, &basisRaw); err != nil {
			return nil, err
		}
		var cptIdStr string
		if cptId != nil {
			cptIdStr = *cptId
		}
		promiseGroups = append(promiseGroups, order.PromiseGroup{
			LineNos: lineNos,
			Promise: order.Promise{CptId: cptIdStr, CutoffAt: cutoffAt, Basis: order.PromiseBasis(basisRaw)},
		})
	}
	if err := groupRows.Err(); err != nil {
		return nil, err
	}
	return promiseGroups, nil
}

// NextID mints an order id. The `ord-<uuid>` shape mirrors the
// `res-<uuid>` / `wu-<uuid>` conventions used elsewhere in the fleet.
func (r *OrderRepo) NextID(_ context.Context) (shared.OrderId, error) {
	return shared.OrderId("ord-" + uuid.NewString()), nil
}
