package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/domain/order"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// AnalyticsTopic is the dedicated topic the analytics data product consumes.
// It is separate from the integration topic (Topic) so the OLTP integration
// contract and the analytical read-model stream evolve independently
// (ADR-0006). Note the hyphen in the context segment — it matches the
// integration topic (warehouse.order-management.events) exactly.
const AnalyticsTopic = "warehouse.order-management.analytics"

// analyticsSpanName is the producer span name for an analytics publish.
const analyticsSpanName = "kafka.publish " + AnalyticsTopic

// AnalyticsPublisher publishes each order-management domain event onto
// AnalyticsTopic as a CloudEvents 1.0 event (same `type` as the
// integration occurrence, `dataschema`
// urn:warehouse:order-management:analytics:<EventName>:v1). It satisfies ports.EventPublisher
// and is a SEPARATE adapter from Publisher: the integration publisher
// (publisher.go) forwards only OrderAllocated/OrderPartiallyAllocated and is
// left untouched.
//
// The report is keyed by PathId, but the order-level domain events do not
// carry a path. Order-scoped events are enriched with their path via an
// OrderRepo lookup (the same repo-lookup-enrichment posture the pilot used
// for task_type): an order's path is taken from its first line, a v1
// simplification that is exact because intake puts every line of an order on
// the same (default) path. Line-scoped events use the line's own path —
// OrderLineReleased carries it directly; the others are looked up by line
// number.
type AnalyticsPublisher struct {
	Writer Writer
	Orders ports.OrderRepo
	NewID  func() string
}

// NewAnalyticsPublisher constructs an AnalyticsPublisher writing to
// AnalyticsTopic on brokers. orders enriches events with their process path;
// newID mints the CloudEvents `id`.
//
// Balancer is kafkago.Hash, matching Publisher's NewWriterForTopic choice
// (see its doc comment): this publisher already keys every message by
// OrderId (marshalData), but LeastBytes would have silently discarded that
// key for partition routing — Hash is what actually turns the key into a
// same-order-same-partition guarantee once AnalyticsTopic gains more than
// one partition.
func NewAnalyticsPublisher(brokers []string, orders ports.OrderRepo, newID func() string) *AnalyticsPublisher {
	return &AnalyticsPublisher{
		Writer: &kafkago.Writer{
			BatchTimeout:           syncWriterBatchTimeout,
			Addr:                   kafkago.TCP(brokers...),
			Topic:                  AnalyticsTopic,
			Balancer:               &kafkago.Hash{},
			AllowAutoTopicCreation: true,
		},
		Orders: orders,
		NewID:  newID,
	}
}

// Publish emits event onto AnalyticsTopic. An event with no analytics payload
// (an unrecognised type) is skipped rather than erroring, so the caller can
// hand it the full event stream indiscriminately.
func (p *AnalyticsPublisher) Publish(ctx context.Context, event shared.DomainEvent) error {
	eventType, key, payload, ok, err := p.buildEvent(ctx, event)
	if err != nil || !ok {
		return err
	}
	return p.write(ctx, eventType, key, payload)
}

// Encode implements kafka.Encoder for the analytics stream: it builds the
// wire-ready analytics message for event without touching the broker, so
// postgres.OutboxPublisher can enqueue it in the same transaction as the
// aggregate write. Like Publisher.Encode, it opens no span of its own —
// the injected trace headers carry whatever span is already active on
// ctx.
func (p *AnalyticsPublisher) Encode(ctx context.Context, event shared.DomainEvent) (Encoded, bool, error) {
	eventType, key, payload, ok, err := p.buildEvent(ctx, event)
	if err != nil || !ok {
		return Encoded{}, ok, err
	}
	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})
	return Encoded{Topic: AnalyticsTopic, EventType: eventType, Key: []byte(key), Value: payload, Headers: headers}, true, nil
}

// buildEvent maps event onto its analytics CloudEvent and marshals it,
// without touching tracing or the broker. eventType is the full CloudEvents
// `type`; key (= `subject`) is the OrderId. ok is false for an event type
// outside the analytics contract (see marshalData), matching this
// publisher's existing "skip, don't error" convention.
func (p *AnalyticsPublisher) buildEvent(ctx context.Context, event shared.DomainEvent) (eventType, key string, payload []byte, ok bool, err error) {
	eventName, key, data, ok := p.marshalData(ctx, event)
	if !ok {
		return "", "", nil, false, nil
	}
	payload, err = cloudevents.New(cloudevents.Spec{
		ID:        p.newID(),
		Entity:    entityOrder,
		EventName: eventName,
		Subject:   key,
		Time:      event.OccurredAt(),
		Stream:    cloudevents.StreamAnalytics,
		Version:   dataSchemaVersion,
		Data:      data,
	})
	if err != nil {
		return "", "", nil, false, fmt.Errorf("kafka: build analytics event: %w", err)
	}
	return cloudevents.Type(entityOrder, eventName), key, payload, true, nil
}

// newID mints the CloudEvents id, falling back to a random UUID when no
// generator was injected (wiring always injects uuid.NewString; the
// fallback keeps the zero value usable in a test).
func (p *AnalyticsPublisher) newID() string {
	if p.NewID == nil {
		return uuid.NewString()
	}
	return p.NewID()
}

// orderPath returns the process path of orderID's first line, or "" when the
// order cannot be found. Best-effort: a missing path leaves the report's path
// dimension unspecified rather than failing the publish.
func (p *AnalyticsPublisher) orderPath(ctx context.Context, orderID shared.OrderId) string {
	o := p.findOrder(ctx, orderID)
	if o == nil {
		return ""
	}
	lines := o.Lines()
	if len(lines) == 0 {
		return ""
	}
	return lines[0].PathID().String()
}

// linePath returns the process path of a specific line, or "" when the order
// or line cannot be found.
func (p *AnalyticsPublisher) linePath(ctx context.Context, orderID shared.OrderId, lineNo int) string {
	o := p.findOrder(ctx, orderID)
	if o == nil {
		return ""
	}
	for _, l := range o.Lines() {
		if l.LineNo() == lineNo {
			return l.PathID().String()
		}
	}
	return ""
}

// findOrder looks orderID up in the repo, returning nil on any miss or error.
func (p *AnalyticsPublisher) findOrder(ctx context.Context, orderID shared.OrderId) *order.Order {
	if p.Orders == nil {
		return nil
	}
	o, err := p.Orders.FindByID(ctx, orderID)
	if err != nil {
		return nil
	}
	return o
}

// firstReleasedLinePath returns the path of the first released line carried on
// an allocation event, falling back to an order lookup when the slice is
// empty.
func (p *AnalyticsPublisher) firstReleasedLinePath(ctx context.Context, orderID shared.OrderId, lines []shared.ReleasedLine) string {
	if len(lines) > 0 {
		return lines[0].PathID.String()
	}
	return p.orderPath(ctx, orderID)
}

// marshalData maps a domain event to its event name, aggregate-id
// message key (the OrderId), and snake_case JSON payload (which always carries
// the enriched path_id). The bool return is false for an event type outside
// the analytics contract, so Publish can skip it.
func (p *AnalyticsPublisher) marshalData(ctx context.Context, e shared.DomainEvent) (eventType, key string, data json.RawMessage, ok bool) {
	switch ev := e.(type) {
	case shared.OrderReceived:
		return "OrderReceived", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id":   ev.OrderID.String(),
			"path_id":    p.orderPath(ctx, ev.OrderID),
			"line_count": ev.LineCount,
		}), true
	case shared.OrderAllocated:
		return "OrderAllocated", ev.OrderID.String(), mustMarshal(p.promiseAllocationData(ctx, map[string]any{
			"order_id": ev.OrderID.String(),
			"path_id":  p.firstReleasedLinePath(ctx, ev.OrderID, ev.Lines),
		}, ev.OrderID, ev.PromiseBasis, ev.PromiseDate)), true
	case shared.OrderPartiallyAllocated:
		return "OrderPartiallyAllocated", ev.OrderID.String(), mustMarshal(p.promiseAllocationData(ctx, map[string]any{
			"order_id":          ev.OrderID.String(),
			"path_id":           p.firstReleasedLinePath(ctx, ev.OrderID, ev.Lines),
			"allocated_lines":   ev.AllocatedLines,
			"backordered_lines": ev.BackorderedLines,
		}, ev.OrderID, ev.PromiseBasis, ev.PromiseDate)), true
	case shared.OrderAllocationPartiallyFailed:
		return "OrderAllocationPartiallyFailed", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id":        ev.OrderID.String(),
			"path_id":         p.orderPath(ctx, ev.OrderID),
			"allocated_lines": ev.AllocatedLines,
			"remaining_lines": ev.RemainingLines,
		}), true
	case shared.OrderReleased:
		return "OrderReleased", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id": ev.OrderID.String(),
			"path_id":  p.orderPath(ctx, ev.OrderID),
		}), true
	case shared.OrderCancelled:
		return "OrderCancelled", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id":             ev.OrderID.String(),
			"path_id":              p.orderPath(ctx, ev.OrderID),
			"revoked_reservations": ev.RevokedReservations,
		}), true
	case shared.OrderLineAllocated:
		return "OrderLineAllocated", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id": ev.OrderID.String(),
			"line_no":  ev.LineNo,
			"path_id":  p.linePath(ctx, ev.OrderID, ev.LineNo),
			"sku":      ev.SKU.String(),
		}), true
	case shared.OrderLineBackordered:
		return "OrderLineBackordered", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id": ev.OrderID.String(),
			"line_no":  ev.LineNo,
			"path_id":  p.linePath(ctx, ev.OrderID, ev.LineNo),
			"sku":      ev.SKU.String(),
		}), true
	case shared.OrderLineReleased:
		return "OrderLineReleased", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id":     ev.OrderID.String(),
			"line_no":      ev.LineNo,
			"path_id":      ev.PathID.String(),
			"work_unit_id": ev.WorkUnitID,
		}), true
	case shared.OrderRepromised:
		return "OrderRepromised", ev.OrderID.String(), mustMarshal(map[string]any{
			"order_id":   ev.OrderID.String(),
			"cpt_id_old": ev.CptIdOld,
			"cpt_id_new": ev.CptIdNew,
			"reason":     ev.Reason,
		}), true
	default:
		return "", "", nil, false
	}
}

// promiseAllocationData enriches an OrderAllocated/OrderPartiallyAllocated
// analytics payload with ADR 0014 §6's promise KPI fields: promise_basis
// (mirrored from the event, unchanged), promise_cutoff_at (the promise's
// CutoffAt -- the same instant as promiseDate on the event -- present only
// for a Capability-basis promise, since a LeadTime-basis "cutoff" is just
// now-plus-a-configured-duration and not a real departure), and
// split_shipment (true when the order had more than one order.PromiseGroup
// at allocation time, ADR 0014 §3 / ADR 0017). split_shipment needs an
// OrderRepo lookup -- the SAME repo-lookup-enrichment posture this file's
// orderPath/linePath/firstReleasedLinePath helpers already use for path_id,
// extended here to inspect PromiseGroups() rather than Lines(). Best-effort,
// like the rest of this file's enrichment: an order the repo cannot find
// (or which was rehydrated with no group breakdown at all) leaves
// split_shipment false rather than failing the publish.
func (p *AnalyticsPublisher) promiseAllocationData(ctx context.Context, data map[string]any, orderID shared.OrderId, promiseBasis string, promiseDate time.Time) map[string]any {
	if promiseBasis != "" {
		data["promise_basis"] = promiseBasis
	}
	if promiseBasis == string(order.BasisCapability) {
		data["promise_cutoff_at"] = promiseDate
	}
	data["split_shipment"] = p.hasSplitShipment(ctx, orderID)
	return data
}

// hasSplitShipment reports whether orderID's current PromiseGroups()
// breakdown has more than one group, i.e. its lines were promised to
// different cutoffs (ADR 0014 §3 / ADR 0017). false for a missing order or
// one with no recorded group breakdown -- never a failed publish.
func (p *AnalyticsPublisher) hasSplitShipment(ctx context.Context, orderID shared.OrderId) bool {
	o := p.findOrder(ctx, orderID)
	if o == nil {
		return false
	}
	return len(o.PromiseGroups()) > 1
}

// mustMarshal marshals a map whose shape is fully controlled by marshalData,
// so an error here is a programming mistake rather than a runtime condition.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kafka: marshal analytics data: %v", err))
	}
	return b
}

// write publishes one already-marshalled CloudEvent inside a producer span,
// injecting that span's context into the message headers so the projector's
// consume span becomes its child.
func (p *AnalyticsPublisher) write(ctx context.Context, eventType, key string, payload []byte) error {
	ctx, span := otel.Tracer(tracerName).Start(ctx, analyticsSpanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(AnalyticsTopic),
			semconv.MessagingOperationName("publish"),
			semconv.CloudEventsEventType(eventType),
		),
	)
	defer span.End()

	headers := []kafkago.Header{cloudevents.ContentTypeHeader()}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	msg := kafkago.Message{Key: []byte(key), Value: payload, Headers: headers}
	if err := p.Writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kafka: publish %s analytics event: %w", eventType, err)
	}
	return nil
}

// Close releases the underlying Kafka writer.
func (p *AnalyticsPublisher) Close() error {
	if w, ok := p.Writer.(*kafkago.Writer); ok {
		return w.Close()
	}
	return nil
}

// Compile-time assertion that AnalyticsPublisher satisfies the outbound
// event-publishing port.
var _ ports.EventPublisher = (*AnalyticsPublisher)(nil)
