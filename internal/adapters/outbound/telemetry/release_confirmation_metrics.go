package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/claudioed/order-management/internal/application/ports"
)

// The release-confirmation loop's instruments (ADR 0043). Names follow the
// service's order.<area>.<thing> convention.
const (
	// releaseConfirmationEventsName counts every WorkUnitCreated the
	// release-confirmation consumer handled, by `outcome`: confirmed,
	// late_confirmed, duplicate, ignored_no_reference, ignored_unknown_line.
	// A climbing ignored_no_reference rate after wes-work-planning ships its
	// reference/line_no fields means the producer is not stamping them.
	releaseConfirmationEventsName = "order.release_confirmation.events"
	// releaseConfirmationUnconfirmedName counts released lines the timeout
	// sweep flagged UNCONFIRMED. Any increase is an order line whose work
	// wes-work-planning never admitted: alert on it.
	releaseConfirmationUnconfirmedName = "order.release_confirmation.unconfirmed"
)

// ReleaseConfirmationMetrics implements ports.ReleaseConfirmationMetrics
// against the global MeterProvider (a no-op until Setup installs a real one).
type ReleaseConfirmationMetrics struct {
	events      metric.Int64Counter
	unconfirmed metric.Int64Counter
}

var _ ports.ReleaseConfirmationMetrics = (*ReleaseConfirmationMetrics)(nil)

// NewReleaseConfirmationMetrics registers the two counters. It only fails if
// an instrument name is invalid, a programming error rather than a runtime
// condition.
func NewReleaseConfirmationMetrics() (*ReleaseConfirmationMetrics, error) {
	meter := otel.Meter(meterName)
	events, err := meter.Int64Counter(
		releaseConfirmationEventsName,
		metric.WithDescription("WorkUnitCreated events handled by the release-confirmation consumer, by outcome."),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}
	unconfirmed, err := meter.Int64Counter(
		releaseConfirmationUnconfirmedName,
		metric.WithDescription("Released order lines flagged UNCONFIRMED: wes-work-planning never confirmed the work within the timeout."),
		metric.WithUnit("{line}"),
	)
	if err != nil {
		return nil, err
	}
	return &ReleaseConfirmationMetrics{events: events, unconfirmed: unconfirmed}, nil
}

// Handled counts one handled WorkUnitCreated under its outcome.
func (m *ReleaseConfirmationMetrics) Handled(ctx context.Context, outcome ports.ReleaseConfirmationOutcome) {
	m.events.Add(ctx, 1, metric.WithAttributes(outcomeKey.String(string(outcome))))
}

// LineUnconfirmed counts one line flagged UNCONFIRMED.
func (m *ReleaseConfirmationMetrics) LineUnconfirmed(ctx context.Context) {
	m.unconfirmed.Add(ctx, 1)
}
