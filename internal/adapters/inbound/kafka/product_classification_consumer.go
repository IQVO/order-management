// Package kafka's product_classification_consumer.go is the inbound half of
// ADR 0036: a Kafka consumer of product-master's integration topic that
// keeps order-management's LOCAL copy of product classifications
// (usecases.ApplyProductClassification). ports.ProductClassificationLookup
// is answered from that copy; this service never calls product-master.
//
// Shape and guarantees (PlannedCapacityConsumer's, ADR 0031, minus the DLQ):
//
//   - CloudEvents 1.0 only, decoded through internal/adapters/kafka/
//     cloudevents.Decode; dispatch on the FULL `type`
//     com.warehouse.wms.product-master.product.ProductClassified; every other
//     type on the topic is committed past untouched; the CloudEvents `id` is
//     the idempotency key.
//   - At-least-once with an atomic effect: the use case runs the id claim
//     and the version-guarded upsert in ONE UnitOfWork, and this consumer
//     commits the offset only after the use case returned nil.
//   - A message that can never be applied (not a CloudEvent, undecodable or
//     invalid payload) is logged at WARN and committed past: the payload is a
//     full-state replacement, so the next ProductClassified for that SKU
//     repairs the copy, and there is nothing a dead-letter replay would add.
//   - A transient failure (database down, deadlock) is retried on the SAME
//     message with backoff until it succeeds or the process stops. Nothing
//     is skipped on a transient error, and Run never returns one.
//   - STABLE shared consumer group, supplied by the composition root from
//     PRODUCT_CLASSIFICATION_CONSUMER_GROUP — never a literal here, never
//     per-process-unique: the copy is durable, so a restart resumes from the
//     committed offset instead of replaying the topic.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// ProductMasterEventsTopic is product-master's integration topic (key and
// subject = the SKU).
const ProductMasterEventsTopic = "warehouse.product-master.events"

// ceTypeProductClassified is the one type this consumer applies,
// byte-for-byte as product-master publishes it (its apis/asyncapi.yaml).
const ceTypeProductClassified = "com.warehouse.wms.product-master.product.ProductClassified"

const productClassificationConsumeSpanNameFmt = "kafka.consume %s"

// productClassifiedData is product-master's ProductClassified payload.
// temperature_class and dot_hazard_class are omitted when unset;
// classification_source is decoded but not stored (a migration artefact of
// product-master's ADR 0003).
type productClassifiedData struct {
	SKU                  string   `json:"sku"`
	HandlingTags         []string `json:"handling_tags"`
	TemperatureClass     string   `json:"temperature_class,omitempty"`
	DotHazardClass       int      `json:"dot_hazard_class,omitempty"`
	ClassificationSource string   `json:"classification_source,omitempty"`
	Version              int64    `json:"version"`
}

// ProductClassificationConsumer consumes ProductMasterEventsTopic and drives
// usecases.ApplyProductClassification.
type ProductClassificationConsumer struct {
	reader messageReader
	apply  *usecases.ApplyProductClassification
	logger *slog.Logger
}

// NewProductClassificationConsumer constructs a consumer of
// ProductMasterEventsTopic on brokers under groupID. groupID comes from
// configuration (the composition root reads
// PRODUCT_CLASSIFICATION_CONSUMER_GROUP); there is no default here on
// purpose, so a locally run process can never join a live cluster's group
// by accident.
func NewProductClassificationConsumer(brokers []string, groupID string, apply *usecases.ApplyProductClassification, logger *slog.Logger) *ProductClassificationConsumer {
	return NewProductClassificationConsumerForTopic(brokers, groupID, ProductMasterEventsTopic, apply, logger)
}

// NewProductClassificationConsumerForTopic is NewProductClassificationConsumer
// on an explicit topic (isolated integration tests).
func NewProductClassificationConsumerForTopic(brokers []string, groupID, topic string, apply *usecases.ApplyProductClassification, logger *slog.Logger) *ProductClassificationConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ProductClassificationConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		apply:  apply,
		logger: logger,
	}
}

// Close releases the Kafka reader.
func (c *ProductClassificationConsumer) Close() error {
	return c.reader.Close()
}

// Run consumes until ctx is cancelled and only returns then: a broker
// outage or a failing database is logged and retried with jittered
// backoff, never returned (see RepromiseConsumer.Run for the incident that
// rule comes from).
func (c *ProductClassificationConsumer) Run(ctx context.Context) error {
	policy := newRecoverBackoff()
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !c.waitToRecover(ctx, policy, "fetch", err) {
				return nil
			}
			continue
		}
		if !c.handleUntilDone(ctx, policy, msg) {
			return nil
		}
		policy.Reset()
	}
}

// handleUntilDone retries ONE message until it is handled and committed. It
// reports false only when ctx is done.
func (c *ProductClassificationConsumer) handleUntilDone(ctx context.Context, policy backoff.BackOff, msg kafkago.Message) bool {
	for {
		err := c.handleMessage(ctx, msg)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if !c.waitToRecover(ctx, policy, "handle", err, "partition", msg.Partition, "offset", msg.Offset) {
			return false
		}
	}
}

func (c *ProductClassificationConsumer) waitToRecover(ctx context.Context, policy backoff.BackOff, stage string, err error, args ...any) bool {
	wait := policy.NextBackOff()
	c.log(ctx, "product classification: handling failed, retrying the same message",
		append([]any{"stage", stage, "retry_in", wait.String(), "error", err}, args...)...)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}

// handleMessage processes one fetched message and commits its offset once
// it is applied, ignored or deterministically skipped. It returns an error
// (and commits nothing) when the use case or the commit failed, so the
// caller retries the same message.
func (c *ProductClassificationConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	topic := c.reader.Config().Topic
	msgCtx, span := c.startConsumeSpan(ctx, topic, msg)
	defer span.End()

	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		c.skip(msgCtx, msg, "non-CloudEvents message", err, nil)
		return c.commit(ctx, msg)
	}
	span.SetAttributes(
		semconv.MessagingMessageID(e.ID()),
		semconv.CloudEventsEventType(e.Type()),
		semconv.CloudEventsEventSource(e.Source()),
	)
	if e.Type() != ceTypeProductClassified {
		// Every other product-master type, or anything new: forward
		// compatibility, not poison.
		return c.commit(ctx, msg)
	}

	req, err := productClassifiedRequestFrom(e)
	if err == nil {
		err = c.apply.Execute(msgCtx, req)
	}
	switch {
	case errors.Is(err, usecases.ErrInvalidProductClassification) || errors.Is(err, errUndecodableClassification):
		recordSpanError(span, err)
		c.skip(msgCtx, msg, "invalid ProductClassified payload", err, &e)
		return c.commit(ctx, msg)
	case err != nil:
		recordSpanError(span, err)
		return fmt.Errorf("product classification: apply %s: %w", e.ID(), err)
	}
	return c.commit(ctx, msg)
}

// errUndecodableClassification marks a ProductClassified whose data cannot
// be decoded or contradicts its subject: deterministic, never retried.
var errUndecodableClassification = errors.New("undecodable ProductClassified")

// productClassifiedRequestFrom maps a decoded ProductClassified to an apply
// request. The use case validates sku and version itself.
func productClassifiedRequestFrom(e ce.Event) (usecases.ApplyProductClassificationRequest, error) {
	var data productClassifiedData
	if err := e.DataAs(&data); err != nil {
		return usecases.ApplyProductClassificationRequest{}, fmt.Errorf("%w: decode data: %v", errUndecodableClassification, err)
	}
	if e.Subject() != "" && data.SKU != "" && e.Subject() != data.SKU {
		return usecases.ApplyProductClassificationRequest{},
			fmt.Errorf("%w: subject %q does not match data.sku %q", errUndecodableClassification, e.Subject(), data.SKU)
	}
	return usecases.ApplyProductClassificationRequest{
		EventID: e.ID(),
		Record: ports.ProductClassificationRecord{
			SKU:              data.SKU,
			HandlingTags:     data.HandlingTags,
			TemperatureClass: data.TemperatureClass,
			DotHazardClass:   data.DotHazardClass,
			Version:          data.Version,
		},
	}, nil
}

// skip logs a message that will never be applied; the caller commits it.
func (c *ProductClassificationConsumer) skip(ctx context.Context, msg kafkago.Message, why string, cause error, e *ce.Event) {
	args := []any{"topic", c.reader.Config().Topic, "partition", msg.Partition, "offset", msg.Offset, "error", cause}
	if e != nil {
		args = append(args, "ce_id", e.ID(), "ce_type", e.Type())
	}
	c.log(ctx, "product classification: "+why+", skipping", args...)
}

// commit acknowledges msg so it is never redelivered.
func (c *ProductClassificationConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *ProductClassificationConsumer) startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})
	return otel.Tracer(repromiseTracerName).Start(ctx,
		fmt.Sprintf(productClassificationConsumeSpanNameFmt, topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("process"),
		),
	)
}

func (c *ProductClassificationConsumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}
