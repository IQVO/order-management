// Package postgres provides pgxpool-backed implementations of the outbound
// ports, plus a golang-migrate runner for the SQL migrations in
// /migrations.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MaxConns is the OLTP pool's per-process connection ceiling, shared by
// both cmd/order (the api Deployment, HPA-scalable up to
// charts/order-management values.yaml's autoscaling.maxReplicas, 4) and
// cmd/mcp (the mcp Deployment, fixed at 1 replica -- see that chart
// value's own doc comment for why it does not get an HPA).
//
// Sized against this shared Postgres instance's REAL max_connections
// (100, the Bitnami chart's own default -- warehouse-infra's
// terraform/postgres.tf does not override it; verified live with `SHOW
// max_connections` against the deployed release): at the OLTP
// Deployment's HPA ceiling of 4 replicas, 4 * 10 = 40 connections, ~40%
// of the instance-wide ceiling for this ONE of 9 backend services'
// OLTP path alone, deliberately leaving headroom for the other 8
// services (and this service's own mcp/projector/reports processes)
// sharing the SAME Postgres instance (confirmed via `\l`: all 9
// services' OLTP and analytics databases live in one Postgres release,
// not one instance per service -- see the pgxpool/statement_timeout ADR
// for the full connection-budget accounting and the documented residual
// risk if every sibling service scales to its own ceiling at once).
const MaxConns = 10

// StatementTimeout bounds how long a single query may hold a connection
// on the OLTP database before Postgres cancels it. order-management's
// OLTP queries are all single-aggregate reads/writes (one Order plus its
// lines/outbox rows, keyed by id) that normally complete in low
// milliseconds; 5s is generous headroom for lock contention or a slow
// disk without letting one runaway or blocked query hold a pool slot --
// and therefore a bulkhead slot the HPA's replica math is sizing
// capacity around -- indefinitely. See the pgxpool/statement_timeout ADR.
const StatementTimeout = "5s"

// NewPool opens a connection pool against databaseURL, with MaxConns and
// StatementTimeout applied to every connection.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return NewPoolWithLimits(ctx, databaseURL, MaxConns, StatementTimeout)
}

// NewPoolWithLimits is NewPool's shared implementation, taking maxConns
// and statementTimeout explicitly so an integration test can drive a
// much shorter timeout directly -- proving the AfterConnect hook really
// applies the setting to every new connection, by triggering an actual
// cancellation -- without waiting out the real production value.
// Production callers should use NewPool.
func NewPoolWithLimits(ctx context.Context, databaseURL string, maxConns int32, statementTimeout string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%s'", statementTimeout))
		return err
	}
	return pgxpool.NewWithConfig(ctx, config)
}
