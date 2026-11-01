// Package postgres provides pgxpool-backed implementations of the outbound
// ports, plus a golang-migrate runner for the SQL migrations in
// /migrations.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/order-management/internal/pgtx"
)

// querier is the subset of pgx that both *pgxpool.Pool and pgx.Tx satisfy.
// Every adapter in this package that does NOT already manage its own
// transaction issues its SQL through the querier it resolves from the
// context, so the same repo/publisher code runs either autonomously
// (pool) or inside a UnitOfWork transaction (tx) without knowing which.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// withTx and txFrom are thin aliases over internal/pgtx — the ONE
// transaction-in-context mechanism this service uses. It lives in its
// own package (rather than as an unexported type here) specifically so
// internal/adapters/inbound/http's idempotency middleware can bind a
// transaction it began itself into the same slot, and have it join here
// via UnitOfWork.Execute/querierFrom/beginOrJoin below, without either
// adapter package importing the other (see internal/pgtx's doc comment
// for the full reasoning and internal/architecture's fitness tests for
// the rule this sidesteps).
func withTx(ctx context.Context, tx pgx.Tx) context.Context { return pgtx.WithTx(ctx, tx) }
func txFrom(ctx context.Context) (pgx.Tx, bool)             { return pgtx.TxFrom(ctx) }

// querierFrom resolves the transaction bound to ctx, or falls back to the
// pool when the caller is not inside a UnitOfWork.
func querierFrom(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := txFrom(ctx); ok {
		return tx
	}
	return pool
}

// beginOrJoin returns the ctx-bound transaction (owned=false: commit/
// rollback below are no-ops, the outer UnitOfWork.Execute owns its
// lifecycle) or begins a brand-new one on pool (owned=true). It is for
// adapters that historically opened their OWN transaction internally
// (e.g. OrderRepo.Save, which writes orders + order_lines + promise
// groups as one unit) — inside a UnitOfWork scope that internal
// transaction must JOIN the outer one instead of opening a second,
// separate transaction that cannot see the outbox insert made in the
// same use case.
func beginOrJoin(ctx context.Context, pool *pgxpool.Pool) (tx pgx.Tx, commit func(context.Context) error, rollback func(context.Context) error, err error) {
	if joined, ok := txFrom(ctx); ok {
		noop := func(context.Context) error { return nil }
		return joined, noop, noop, nil
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return tx, tx.Commit, tx.Rollback, nil
}

// UnitOfWork implements ports.UnitOfWork over a single Postgres
// transaction. Everything the wrapped function does through this
// package's adapters — the aggregate upsert(s) AND the outbox insert(s)
// — commits together or not at all.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constructs a UnitOfWork over pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Execute runs fn inside one transaction. A nested call (fn invoked with
// a ctx that already carries a transaction) joins the outer scope rather
// than opening a second one, so composing use cases never deadlocks on
// itself, and a request-scoped Idempotency-Key transaction
// (internal/adapters/inbound/http) carries the key row, the aggregate
// writes and the outbox rows as one unit.
//
// A joined scope runs in a SAVEPOINT of the outer transaction: if fn fails,
// only fn's writes are rolled back (the outer transaction stays usable),
// exactly what a standalone Execute does by rolling back its own
// transaction. Without it a failure half-way through a use case — a version
// conflict, a unique violation that aborts the transaction — would leave the
// first writes in the outer transaction (or the transaction itself aborted),
// to be committed together with the cached 4xx answer.
func (u *UnitOfWork) Execute(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	if outer, ok := txFrom(ctx); ok {
		return executeInSavepoint(ctx, outer, fn)
	}

	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin unit of work: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("postgres: rollback unit of work: %w", rbErr))
			}
		}
	}()

	if err = fn(withTx(ctx, tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit unit of work: %w", err)
	}
	return nil
}

// executeInSavepoint runs fn in a SAVEPOINT of the outer transaction (pgx
// models a nested Begin as one): released when fn succeeds, rolled back to
// when it fails or panics. The panic is re-raised so the request's Recoverer
// and the idempotency middleware still roll the whole transaction back.
func executeInSavepoint(ctx context.Context, outer pgx.Tx, fn func(ctx context.Context) error) (err error) {
	sp, err := outer.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin savepoint: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = sp.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			if rbErr := sp.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("postgres: rollback savepoint: %w", rbErr))
			}
		}
	}()

	if err = fn(withTx(ctx, sp)); err != nil {
		return err
	}
	if err = sp.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: release savepoint: %w", err)
	}
	return nil
}
