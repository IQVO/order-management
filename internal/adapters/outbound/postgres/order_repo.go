package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

	if _, err := tx.Exec(ctx, `
		INSERT INTO orders (id, allow_partial_shipment, promise_date, promise_cpt_id, promise_basis, release_on_allocation, required_ship_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			promise_date = EXCLUDED.promise_date,
			promise_cpt_id = EXCLUDED.promise_cpt_id,
			promise_basis = EXCLUDED.promise_basis
	`, o.ID().String(), o.AllowPartialShipment(), promiseDate, promiseCptId, promiseBasis, o.ReleaseOnAllocation(), o.RequiredShipBy()); err != nil {
		return err
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

	return commit(ctx)
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

	var allowPartialShipment bool
	var promiseDate *time.Time
	var promiseCptId *string
	var promiseBasisRaw *string
	var releaseOnAllocation bool
	var requiredShipBy *time.Time

	err := q.QueryRow(ctx, `
		SELECT allow_partial_shipment, promise_date, promise_cpt_id, promise_basis, release_on_allocation, required_ship_by FROM orders WHERE id = $1
	`, id.String()).Scan(&allowPartialShipment, &promiseDate, &promiseCptId, &promiseBasisRaw, &releaseOnAllocation, &requiredShipBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

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

	var promiseBasis *order.PromiseBasis
	if promiseBasisRaw != nil {
		b := order.PromiseBasis(*promiseBasisRaw)
		promiseBasis = &b
	}

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

	o := order.RehydrateHeld(id, lines, allowPartialShipment, promiseDate, promiseCptId, promiseBasis, promiseGroups, releaseOnAllocation)
	// Set after rehydration rather than as a fourth Rehydrate parameter:
	// the deadline is optional and most orders have none, so widening the
	// constructor chain again would cost every call site an argument it
	// does not care about.
	if requiredShipBy != nil {
		o.SetRequiredShipBy(*requiredShipBy)
	}
	return o, nil
}

// NextID mints an order id. The `ord-<uuid>` shape mirrors the
// `res-<uuid>` / `wu-<uuid>` conventions used elsewhere in the fleet.
func (r *OrderRepo) NextID(_ context.Context) (shared.OrderId, error) {
	return shared.OrderId("ord-" + uuid.NewString()), nil
}
