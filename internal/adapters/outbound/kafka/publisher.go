// Package kafka publishes cross-service integration events to the shared
// warehouse-systems Kafka broker. It implements ports.EventPublisher, so it
// drops in wherever the log or Postgres outbox publisher is used today.
//
// OrderAllocated, OrderPartiallyAllocated, and — since ADR 0014 §5 / ADR
// 0018 — OrderRepromised are the published integration contract (see
// CLAUDE.md's Kafka integration section); every other domain event is a
// local concern and is not forwarded here — mirroring inventory-storage's
// own precedent of forwarding only a subset of its several domain events.
//
// Since the transactional outbox (see the ADR registered alongside
// postgres.OutboxPublisher), Publish's work is split into Encode (build
// the wire-ready message, no broker call) and the actual WriteMessages —
// Encode is what postgres.OutboxPublisher calls to build an outbox row
// inside the use case's transaction, and Publish itself is just
// Encode + write for the direct (no-outbox) dev/test path.
package kafka

import (
	"context"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Topic is the integration events topic this service publishes to.
const Topic = "warehouse.order-management.events"

// entityOrder is the `<entity>` segment of every CloudEvents `type` this
// service publishes: every Order* event is raised by the Order aggregate.
const entityOrder = "order"

// dataSchemaVersion is the `dataschema` version of every payload this
// package publishes today (both streams start at v1).
const dataSchemaVersion = 1

// tracerName scopes the publish spans this adapter emits.
const tracerName = "github.com/claudioed/order-management/internal/adapters/outbound/kafka"

// spanName follows the fleet-wide convention for messaging spans:
// "kafka.publish <topic>" on the producer side, "kafka.consume <topic>" on
// the consumer side. This service only publishes.
const spanName = "kafka.publish " + Topic

// Writer is the subset of *kafkago.Writer the Publisher depends on, so unit
// tests can substitute a fake without a real broker.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Encoded is one already-encoded, wire-ready Kafka message: the topic it
// belongs on (a multi-topic outbox/relay routes purely off this field —
// the underlying relay writer carries no fixed topic of its own), the
// partition key, the CloudEvents 1.0 structured-mode JSON event, and the
// headers captured at encode time (W3C trace context plus the CloudEvents
// `content-type` header). It is the unit postgres.OutboxPublisher stores
// and postgres.OutboxRelay later hands to a Sink, so the direct-publish
// and outbox paths can never disagree about what a message looks like.
//
// EventType is the full CloudEvents `type` attribute; it is persisted in
// the outbox row's event_type column purely for logging/inspection.
type Encoded struct {
	Topic     string
	EventType string
	Key       []byte
	Value     []byte
	Headers   []kafkago.Header
}

// Encoder turns one domain event into its Kafka wire form for one topic,
// without sending it. Both Publisher (this file) and AnalyticsPublisher
// (analytics_publisher.go) implement it, so postgres.NewOutboxPublisher
// can fan a single event out to both topics inside one transaction. ok is
// false when this encoder does not forward event's type at all — the
// same "not part of my published contract" skip each encoder's own
// Publish already performs, now exposed as data instead of a silent no-op
// write.
type Encoder interface {
	Encode(ctx context.Context, event shared.DomainEvent) (enc Encoded, ok bool, err error)
}

// releasedLineData is the `data.lines[]` entry shape — frozen, and shared
// verbatim with wes-work-planning's Kafka consumer (see CLAUDE.md's Kafka
// integration section). Field names are snake_case per the fleet's
// existing wire convention (e.g. inventory-storage's demand_ref).
//
// fulfillment_class is additive: it carries Order.FulfillmentClass()
// (SINGLE / SAME_SKU_MULTI / MULTI_LINE_MULTI), the same value for every
// line released in one pass since it classifies the whole order, not the
// individual line. Consumers that predate this field can safely ignore
// it — see ADR-0008.
//
// promise_cpt_id/promise_basis/promise_cutoff_at are ADR-0017's additive
// per-line promise fields (ADR 0014 §3's per-shipment-group promising):
// which group THIS line belongs to. omitempty for an order whose promise
// has no group breakdown to attribute a line to (the legacy SetPromise
// path). wes-work-planning's current consumer does not read these
// fields — see the top-level allocationData doc comment.
type releasedLineData struct {
	LineNo           int    `json:"line_no"`
	SKU              string `json:"sku"`
	PathID           string `json:"path_id"`
	GiftWrap         bool   `json:"gift_wrap"`
	FulfillmentClass string `json:"fulfillment_class"`
	PromiseCptId     string `json:"promise_cpt_id,omitempty"`
	PromiseBasis     string `json:"promise_basis,omitempty"`
	PromiseCutoffAt  string `json:"promise_cutoff_at,omitempty"`
}

// allocationData is the `data` payload shape for both OrderAllocated and
// OrderPartiallyAllocated, per CLAUDE.md. promise_date is frozen: field
// name and shape must match wes-work-planning's consumer expectations
// exactly. promise_cpt_id/promise_basis are ADR 0014's additive fields —
// omitempty so a LeadTime-basis promise's empty CptId does not add noise
// to the wire, and so wes-work-planning's existing consumer (which does
// not read these fields yet) is unaffected either way.
type allocationData struct {
	OrderID      string             `json:"order_id"`
	PromiseDate  string             `json:"promise_date"`
	PromiseCptId string             `json:"promise_cpt_id,omitempty"`
	PromiseBasis string             `json:"promise_basis,omitempty"`
	Lines        []releasedLineData `json:"lines"`
}

// repromisedData is the `data` payload shape for OrderRepromised (ADR
// 0014 §5 / ADR 0018): the fleet's "your delivery is delayed" trigger.
// cpt_id_old/cpt_id_new are omitempty for the same reason
// allocationData's promise_cpt_id is — a LeadTime-basis promise (either
// side) has no CPT departure identity, only a computed cutoff instant.
type repromisedData struct {
	OrderID  string `json:"order_id"`
	CptIdOld string `json:"cpt_id_old,omitempty"`
	CptIdNew string `json:"cpt_id_new,omitempty"`
	Reason   string `json:"reason"`
}

// Publisher publishes OrderAllocated and OrderPartiallyAllocated domain
// events as integration events on Topic.
type Publisher struct {
	writer Writer
}

// NewPublisher builds a Publisher over writer.
func NewPublisher(writer Writer) *Publisher {
	return &Publisher{writer: writer}
}

// NewWriter builds a *kafkago.Writer addressed at Topic on the given broker
// addresses.
func NewWriter(brokers ...string) *kafkago.Writer {
	return NewWriterForTopic(Topic, brokers...)
}

// NewWriterForTopic builds a Kafka writer for topic on the given brokers.
// Production code should use NewWriter, which pins the published integration
// topic. This variant lets integration tests exercise the identical writer
// configuration against an isolated, per-test topic.
//
// Balancer is kafkago.Hash (FNV-1a over Message.Key), not LeastBytes: this
// package's kafka-go dependency does NOT replicate Kafka's own key-hashing
// default partitioner automatically just because a Message carries a
// non-nil Key — the Balancer alone decides partition placement, and
// LeastBytes routes purely by cumulative byte volume, ignoring Key
// entirely. Hash is the balancer that actually gives "same Key always maps
// to the same partition", which is the whole point of stamping OrderId
// onto every message (see encodeEvent) after the Phase 3 1->8
// partition scaleup exposed the missing per-order ordering guarantee.
func NewWriterForTopic(topic string, brokers ...string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
	}
}

func toReleasedLineData(lines []shared.ReleasedLine) []releasedLineData {
	out := make([]releasedLineData, 0, len(lines))
	for _, l := range lines {
		rl := releasedLineData{
			LineNo: l.LineNo, SKU: l.SKU.String(), PathID: l.PathID.String(), GiftWrap: l.GiftWrap,
			FulfillmentClass: l.FulfillmentClass,
		}
		if l.PromiseCptId != nil {
			rl.PromiseCptId = *l.PromiseCptId
		}
		if l.PromiseBasis != nil {
			rl.PromiseBasis = *l.PromiseBasis
		}
		if l.PromiseCutoffAt != nil {
			rl.PromiseCutoffAt = l.PromiseCutoffAt.UTC().Format(time.RFC3339)
		}
		out = append(out, rl)
	}
	return out
}

// encodedEvent is one integration event already rendered as a CloudEvent.
type encodedEvent struct {
	id    string
	ceTyp string
	key   []byte
	value []byte
}

// encodeEvent maps event onto its CloudEvents 1.0 integration event and
// marshals it, without touching tracing or the broker. The CloudEvents
// `id` is minted here exactly once; the outbox persists the resulting
// bytes, so a relay redelivery carries the same id. key is the Kafka
// partition key — always the event's Order aggregate id (shared.OrderId),
// matching the analytics publisher's own key choice and the CloudEvents
// `subject` — so every integration event for the SAME order lands on the
// SAME partition (kafkago.Hash), preserving per-aggregate ordering
// (OrderAllocated -> OrderPartiallyAllocated -> OrderRepromised). ok is
// false for a domain event outside this publisher's published contract
// (see the package doc comment) — the caller (Publish, Encode) treats
// that as "nothing to do".
func encodeEvent(event shared.DomainEvent) (out encodedEvent, ok bool, err error) {
	var data any
	var orderID shared.OrderId

	switch e := event.(type) {
	case shared.OrderAllocated:
		orderID = e.OrderID
		data = allocationData{
			OrderID:      e.OrderID.String(),
			PromiseDate:  e.PromiseDate.UTC().Format(time.RFC3339),
			PromiseCptId: e.PromiseCptId,
			PromiseBasis: e.PromiseBasis,
			Lines:        toReleasedLineData(e.Lines),
		}
	case shared.OrderPartiallyAllocated:
		orderID = e.OrderID
		data = allocationData{
			OrderID:      e.OrderID.String(),
			PromiseDate:  e.PromiseDate.UTC().Format(time.RFC3339),
			PromiseCptId: e.PromiseCptId,
			PromiseBasis: e.PromiseBasis,
			Lines:        toReleasedLineData(e.Lines),
		}
	case shared.OrderRepromised:
		orderID = e.OrderID
		data = repromisedData{
			OrderID:  e.OrderID.String(),
			CptIdOld: e.CptIdOld,
			CptIdNew: e.CptIdNew,
			Reason:   e.Reason,
		}
	default:
		return encodedEvent{}, false, nil
	}

	id := uuid.NewString()
	value, err := cloudevents.New(cloudevents.Spec{
		ID:        id,
		Entity:    entityOrder,
		EventName: event.EventName(),
		Subject:   orderID.String(),
		Time:      event.OccurredAt(),
		Stream:    cloudevents.StreamEvents,
		Version:   dataSchemaVersion,
		Data:      data,
	})
	if err != nil {
		return encodedEvent{}, false, err
	}
	return encodedEvent{
		id:    id,
		ceTyp: cloudevents.Type(entityOrder, event.EventName()),
		key:   []byte(orderID.String()),
		value: value,
	}, true, nil
}

// Encode implements Encoder: it builds the wire-ready integration message
// for event (no span of its own — "or none", per the outbox design doc —
// so the trace headers it injects carry whatever span is already active
// on ctx, typically the HTTP request that caused this event, letting a
// consumer parent onto the REQUEST that enqueued the row rather than a
// later, unrelated relay pass).
func (p *Publisher) Encode(ctx context.Context, event shared.DomainEvent) (Encoded, bool, error) {
	enc, ok, err := encodeEvent(event)
	if err != nil || !ok {
		return Encoded{}, ok, err
	}
	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})
	return Encoded{Topic: Topic, EventType: enc.ceTyp, Key: enc.key, Value: enc.value, Headers: headers}, true, nil
}

// Publish forwards event onto Kafka directly (no outbox) — used when the
// service runs without Postgres (EVENT_PUBLISHER=kafka with no
// DATABASE_URL). It is Encode's logic plus the actual broker write,
// unchanged in observable behaviour from before the outbox refactor: one
// producer span per call, header injection from that span's context, and
// the call's error recorded on the span before it is returned.
func (p *Publisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	enc, ok, err := encodeEvent(event)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(Topic),
			semconv.MessagingOperationName("publish"),
			semconv.MessagingMessageID(enc.id),
			semconv.CloudEventsEventType(enc.ceTyp),
		),
	)
	defer span.End()

	// Inject after starting the span so the headers carry *this* span as the
	// parent: that is what stitches the downstream consumer's trace onto
	// this one.
	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	if err := p.writer.WriteMessages(ctx, kafkago.Message{Key: enc.key, Value: enc.value, Headers: headers}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
