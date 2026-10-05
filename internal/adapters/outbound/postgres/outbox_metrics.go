package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// outboxMeterName is this adapter's OpenTelemetry instrumentation scope,
// separate from internal/adapters/outbound/telemetry's service-wide scope
// so the adapter does not import its sibling package for one constant.
const outboxMeterName = "github.com/claudioed/order-management/internal/adapters/outbound/postgres"

// outboxLagQuery finds the age, in seconds, of the OLDEST unpublished
// outbox_events row — the one the relay would drain next. created_at is
// set once at INSERT time and never touched again, so now() - created_at
// for the oldest row IS how long the relay has been behind for at least
// that long. It says nothing about attempts/last_error on that row (a
// separate, already-queryable signal): a row can be lagging because the
// relay simply hasn't run yet (attempts=0) or because it is genuinely
// stuck retrying (attempts>0, last_error set); both cases raise this
// gauge, which is the point — "how long has SOMETHING been waiting," not
// "why."
const outboxLagQuery = `
	SELECT EXTRACT(EPOCH FROM (now() - created_at))
	FROM outbox_events
	WHERE published_at IS NULL
	ORDER BY id
	LIMIT 1
`

// oldestUnpublishedLagSeconds reports the age in seconds of the oldest
// unpublished outbox_events row via q, or 0 when the outbox is fully
// drained (no unpublished rows at all — pgx.ErrNoRows is not a failure
// here, it is the "caught up" answer).
func oldestUnpublishedLagSeconds(ctx context.Context, q querier) (float64, error) {
	var lag float64
	if err := q.QueryRow(ctx, outboxLagQuery).Scan(&lag); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: query outbox lag: %w", err)
	}
	return lag, nil
}

// RegisterOutboxLagGauge installs an asynchronous gauge, sampled once per
// collection, reporting order.outbox.lag_seconds: the age of the oldest
// unpublished outbox_events row (0 when fully drained). It is created
// against the global MeterProvider, so package init ordering versus
// telemetry.Setup does not matter — before Setup installs a real provider
// the no-op global meter makes registration a cheap success and sampling
// goes nowhere.
//
// Callers must keep the returned metric.Registration and call Unregister
// on shutdown, BEFORE the pool closes, so the callback stops touching a
// closed pool; a nil Registration is returned alongside a non-nil error
// only when instrument/callback registration itself failed (the database
// is never queried at registration time).
func RegisterOutboxLagGauge(pool *pgxpool.Pool) (metric.Registration, error) {
	meter := otel.Meter(outboxMeterName)
	gauge, err := meter.Float64ObservableGauge(
		"order.outbox.lag_seconds",
		metric.WithDescription("Age in seconds of the oldest unpublished outbox_events row; 0 when the outbox is fully drained."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: create outbox lag gauge: %w", err)
	}
	reg, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		lag, err := oldestUnpublishedLagSeconds(ctx, pool)
		if err != nil {
			return err
		}
		o.ObserveFloat64(gauge, lag)
		return nil
	}, gauge)
	if err != nil {
		return nil, fmt.Errorf("postgres: register outbox lag callback: %w", err)
	}
	return reg, nil
}
