// Package events provides outbound EventPublisher implementations. v1
// ships only the log publisher: Kafka integration events are explicitly
// deferred (see the README's "Deferred (v1)" section). The interface is
// deliberately the shape a Kafka producer satisfies —
// Publish(ctx, event) error — so adding a broker later is additive and
// touches no use case.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/claudioed/order-management/internal/domain/shared"
)

// LogPublisher publishes domain events by logging them as JSON.
type LogPublisher struct {
	logger *slog.Logger
}

// NewLogPublisher constructs a LogPublisher. A nil logger defaults to
// slog.Default().
func NewLogPublisher(logger *slog.Logger) *LogPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	payload, err := json.Marshal(logPayloadOf(event))
	if err != nil {
		return err
	}
	p.logger.InfoContext(ctx, "domain event published",
		"event_name", event.EventName(),
		"payload", json.RawMessage(payload),
	)
	return nil
}
