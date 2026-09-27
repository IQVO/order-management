package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// OutboxPublisher implements ports.EventPublisher (the singular
// Publish(ctx, event) shape this repo's port already declares — unlike
// process-path-management's variadic EventPublisher, order-management's
// port is NOT changed by this rollout) by writing each configured
// encoder's Kafka wire form of event into outbox_events instead of the
// broker. When called inside a ports.UnitOfWork.Execute scope the insert
// joins the use case's transaction, so the aggregate write and every
// outbox row commit together or not at all. OutboxRelay later drains the
// table onto Kafka.
//
// One outbox row is written per (event x encoder): today that means the
// SAME event enqueues a row for the integration topic (via the
// kafka.Publisher's Encode) and a row for the analytics topic (via
// kafka.AnalyticsPublisher's Encode) in the SAME transaction, so the two
// streams this service already fans out to (FanOutPublisher, direct
// mode) can never diverge from what actually happened — exactly
// FanOutPublisher's fail-fast-in-order semantics, just deferred to the
// relay instead of the broker call happening synchronously in the
// request path.
type OutboxPublisher struct {
	pool     *pgxpool.Pool
	encoders []outboundkafka.Encoder
}

// NewOutboxPublisher constructs an OutboxPublisher over pool that fans
// each event through every encoder given, in order. An encoder whose
// Encode reports ok=false for a given event (i.e. that event is outside
// its own published contract — see kafka.Publisher's and
// kafka.AnalyticsPublisher's doc comments) contributes no row for that
// event; this is not an error.
func NewOutboxPublisher(pool *pgxpool.Pool, encoders ...outboundkafka.Encoder) *OutboxPublisher {
	return &OutboxPublisher{pool: pool, encoders: encoders}
}

// Publish stores event's encoded message for every configured encoder
// that has one, in the outbox. It never touches Kafka.
func (p *OutboxPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	q := querierFrom(ctx, p.pool)
	for _, enc := range p.encoders {
		encoded, ok, err := enc.Encode(ctx, event)
		if err != nil {
			return fmt.Errorf("postgres: encode outbox event: %w", err)
		}
		if !ok {
			continue
		}
		headers, err := marshalHeaders(encoded.Headers)
		if err != nil {
			return fmt.Errorf("postgres: marshal outbox headers: %w", err)
		}
		if _, err := q.Exec(ctx, `
			INSERT INTO outbox_events (topic, event_type, key, value, headers)
			VALUES ($1, $2, $3, $4, $5)
		`, encoded.Topic, encoded.EventType, encoded.Key, encoded.Value, headers); err != nil {
			return fmt.Errorf("postgres: enqueue outbox event %s for %s: %w", encoded.EventType, encoded.Topic, err)
		}
	}
	return nil
}

// wireHeader is the JSON wire shape stored in outbox_events.headers —
// one entry per W3C trace-context header captured at encode time.
type wireHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// marshalHeaders converts kafka-go headers to their JSONB storage shape.
// A nil/empty slice marshals to "[]", never SQL NULL, matching the
// column's NOT NULL DEFAULT '[]'.
func marshalHeaders(headers []kafkago.Header) ([]byte, error) {
	out := make([]wireHeader, 0, len(headers))
	for _, h := range headers {
		out = append(out, wireHeader{Key: h.Key, Value: string(h.Value)})
	}
	return json.Marshal(out)
}

// unmarshalHeaders is marshalHeaders' inverse, used by the relay to
// rebuild the kafka-go headers it hands to a Sink.
func unmarshalHeaders(raw []byte) ([]kafkago.Header, error) {
	var wire []wireHeader
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	out := make([]kafkago.Header, 0, len(wire))
	for _, w := range wire {
		out = append(out, kafkago.Header{Key: w.Key, Value: []byte(w.Value)})
	}
	return out, nil
}
