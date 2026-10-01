// Package kafka holds the inbound Kafka adapters. This file implements the
// analytics projector's consumer: it reads the order-management analytics
// topic and applies each report-moving event to the funnel ProjectionStore,
// exactly once per CloudEvents id despite Kafka's at-least-once delivery (ADR-0006).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/analytics/report"
)

// AnalyticsConsumerGroup is the Kafka consumer group the analytics projector
// reads under. Distinct from any OLTP consumer group so the pipelines track
// their offsets independently.
const AnalyticsConsumerGroup = "order-management-analytics"

// tracerName scopes the consume spans this adapter emits.
const tracerName = "github.com/claudioed/order-management/internal/adapters/inbound/kafka"

// ProcessedEvents is the consumer's idempotency gate: it records which
// CloudEvents ids have been admitted, so an at-least-once redelivery is a no-op. It
// is declared here — where it is consumed — so the analytics side owns its own
// port and the OLTP application layer stays untouched (ADR-0006). The Postgres
// implementation lives in the analyticsstore outbound adapter.
type ProcessedEvents interface {
	// MarkProcessed records eventId if absent, returning true iff this call
	// newly recorded it.
	MarkProcessed(ctx context.Context, eventId string) (bool, error)
}

// Full CloudEvents `type` strings of the analytics events this projector
// consumes (its own service's events, entity `order`).
const (
	ceTypeOrderReceived                  = "com.warehouse.wes.order-management.order.OrderReceived"
	ceTypeOrderAllocated                 = "com.warehouse.wes.order-management.order.OrderAllocated"
	ceTypeOrderPartiallyAllocated        = "com.warehouse.wes.order-management.order.OrderPartiallyAllocated"
	ceTypeOrderAllocationPartiallyFailed = "com.warehouse.wes.order-management.order.OrderAllocationPartiallyFailed"
	ceTypeOrderReleased                  = "com.warehouse.wes.order-management.order.OrderReleased"
	ceTypeOrderCancelled                 = "com.warehouse.wes.order-management.order.OrderCancelled"
	ceTypeOrderLineAllocated             = "com.warehouse.wes.order-management.order.OrderLineAllocated"
	ceTypeOrderLineBackordered           = "com.warehouse.wes.order-management.order.OrderLineBackordered"
	ceTypeOrderLineReleased              = "com.warehouse.wes.order-management.order.OrderLineReleased"
	ceTypeOrderRepromised                = "com.warehouse.wes.order-management.order.OrderRepromised"
)

// analyticsData is the union of fields the projecting event payloads carry.
// Every report-moving event carries path_id (enriched by the publisher); the
// rest of the fields are unused by the projection and present only so the
// decode is total.
//
// PromiseBasis/PromiseCutoffAt/SplitShipment are ADR 0014 §6's additive
// promise KPI fields, present only on OrderAllocated/
// OrderPartiallyAllocated (see the analytics publisher's
// promiseAllocationData). PromiseCutoffAt is a pointer so "absent" (a
// LeadTime-basis promise, or an event published before this ADR) is
// distinguishable from the zero time.
type analyticsData struct {
	OrderID         string     `json:"order_id"`
	PathID          string     `json:"path_id"`
	LineNo          int        `json:"line_no"`
	PromiseBasis    string     `json:"promise_basis"`
	PromiseCutoffAt *time.Time `json:"promise_cutoff_at"`
	SplitShipment   bool       `json:"split_shipment"`
}

// AnalyticsConsumer reads analytics events off the analytics topic and applies
// each to the funnel ProjectionStore, exactly once per CloudEvents id.
type AnalyticsConsumer struct {
	Reader     *kafkago.Reader
	Projection report.ProjectionStore
	Processed  ProcessedEvents
	Logger     *slog.Logger
}

// NewAnalyticsConsumer constructs an AnalyticsConsumer reading topic from
// brokers under AnalyticsConsumerGroup.
func NewAnalyticsConsumer(brokers []string, topic string, projection report.ProjectionStore, processed ProcessedEvents, logger *slog.Logger) *AnalyticsConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: AnalyticsConsumerGroup,
		// Start a brand-new consumer group at the EARLIEST offset. The
		// analytics projection must see the full history of the topic (it is a
		// replayable read model, not a live integration reaction), so a fresh
		// projector — or a backfill into a new group — reads from the
		// beginning rather than kafka-go's default of the latest offset, which
		// would silently drop every event produced before the group first
		// committed an offset. Once the group has committed offsets, those take
		// precedence and this only affects the first join.
		StartOffset: kafkago.FirstOffset,
	})
	return &AnalyticsConsumer{Reader: reader, Projection: projection, Processed: processed, Logger: logger}
}

// Run reads and handles messages until ctx is cancelled or the reader returns
// a fatal error. A handling error is logged and the loop continues so one bad
// message cannot wedge the projector.
func (c *AnalyticsConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, msg); err != nil {
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				c.Logger.WarnContext(ctx, "skipping non-CloudEvents analytics message",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
				continue
			}
			c.Logger.ErrorContext(ctx, "analytics message handling failed", "error", err)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *AnalyticsConsumer) Close() error {
	return c.Reader.Close()
}

// Handle processes one consumed message inside a "kafka.consume <topic>" span
// whose parent is the producer's span, read from the message headers. It is
// exported separately from Run so the propagation can be tested without a live
// broker.
func (c *AnalyticsConsumer) Handle(ctx context.Context, msg kafkago.Message) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})

	ctx, span := otel.Tracer(tracerName).Start(ctx,
		"kafka.consume "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationName("process"),
		),
	)
	defer span.End()

	if err := c.HandleMessage(ctx, msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// HandleMessage decodes raw as a CloudEvents 1.0 event and applies the
// matching projection method for its full `type`. A message that is not a
// valid CloudEvent (including the retired flat envelope) returns an error
// wrapping cloudevents.ErrNotCloudEvent — Run logs it at WARN and moves on;
// it is never parsed any other way. Types outside the projection contract
// are ignored (and not marked processed). For a projecting event it dedupes
// on the CloudEvents `id` via ProcessedEvents before applying, so a
// redelivery is a no-op. `time` is the event's occurred-at. It is exported
// separately from Run so tests can feed raw events without a live broker.
func (c *AnalyticsConsumer) HandleMessage(ctx context.Context, raw []byte) error {
	e, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("analytics: %w", err)
	}

	// Only the funnel-moving events project. Every other analytics event type
	// is acknowledged without touching the read model or the processed set, so
	// a later contract change could reprocess it.
	if !isProjecting(e.Type()) {
		return nil
	}

	isNew, err := c.Processed.MarkProcessed(ctx, e.ID())
	if err != nil {
		return fmt.Errorf("analytics: mark processed: %w", err)
	}
	if !isNew {
		return nil
	}

	var data analyticsData
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("analytics: decode data: %w", err)
	}

	return c.apply(ctx, e.Type(), e.ID(), data, e.Time())
}

// isProjecting reports whether ceType moves a funnel counter.
func isProjecting(ceType string) bool {
	switch ceType {
	case ceTypeOrderReceived, ceTypeOrderAllocated, ceTypeOrderPartiallyAllocated,
		ceTypeOrderAllocationPartiallyFailed, ceTypeOrderReleased, ceTypeOrderCancelled,
		ceTypeOrderLineAllocated, ceTypeOrderLineBackordered, ceTypeOrderLineReleased,
		ceTypeOrderRepromised:
		return true
	default:
		return false
	}
}

// apply routes a decoded event to the projection method for its type.
// OrderAllocated/OrderPartiallyAllocated additionally carry ADR 0014 §6's
// promise KPI fields (data.PromiseBasis/PromiseCutoffAt/SplitShipment),
// applied in the SAME call as the funnel counter — see
// report.ProjectionStore.ApplyOrderAllocated's doc comment for why.
// OrderRepromised (ADR 0018) routes to the path_id-free counter.
func (c *AnalyticsConsumer) apply(ctx context.Context, ceType, eventID string, data analyticsData, at time.Time) error {
	switch ceType {
	case ceTypeOrderReceived:
		return c.Projection.ApplyOrderReceived(ctx, eventID, data.PathID, at)
	case ceTypeOrderAllocated:
		return c.Projection.ApplyOrderAllocated(ctx, eventID, data.PathID, at, data.PromiseBasis, data.PromiseCutoffAt, data.SplitShipment)
	case ceTypeOrderPartiallyAllocated:
		return c.Projection.ApplyOrderPartiallyAllocated(ctx, eventID, data.PathID, at, data.PromiseBasis, data.PromiseCutoffAt, data.SplitShipment)
	case ceTypeOrderAllocationPartiallyFailed:
		return c.Projection.ApplyOrderAllocationFailed(ctx, eventID, data.PathID, at)
	case ceTypeOrderReleased:
		return c.Projection.ApplyOrderReleased(ctx, eventID, data.PathID, at)
	case ceTypeOrderCancelled:
		return c.Projection.ApplyOrderCancelled(ctx, eventID, data.PathID, at)
	case ceTypeOrderLineAllocated:
		return c.Projection.ApplyLineAllocated(ctx, eventID, data.PathID, at)
	case ceTypeOrderLineBackordered:
		return c.Projection.ApplyLineBackordered(ctx, eventID, data.PathID, at)
	case ceTypeOrderLineReleased:
		return c.Projection.ApplyLineReleased(ctx, eventID, data.PathID, at)
	case ceTypeOrderRepromised:
		return c.Projection.ApplyOrderRepromised(ctx, eventID, at)
	default:
		return nil
	}
}

// headerCarrier adapts a kafka-go header slice to propagation.TextMapCarrier,
// so the W3C traceparent the publisher injected can be extracted here, making
// this consume span a child of the publish span. It mirrors the outbound
// adapter's carrier; it is duplicated rather than shared so the inbound adapter
// does not import the outbound one.
type headerCarrier struct {
	headers *[]kafkago.Header
}

var _ propagation.TextMapCarrier = headerCarrier{}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i := range *c.headers {
		if (*c.headers)[i].Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}
