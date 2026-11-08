// Package kafka's outbound_shipping_consumer.go is the inbound half of
// ADR 0051: a Kafka consumer of outbound-shipping's integration topic
// that turns its ShipmentConfirmed into shipped order lines
// (usecases.ConfirmShipment) and its PackageMissedLoad into an
// order-wide re-promise (usecases.RepromiseOrder.ExecuteForMissedLoad).
// It never calls outbound-shipping; it only reads what that service
// published — the consumed wire shapes below are this context's own
// independently-declared copies of that service's real contract, decoded
// through the shared cloudevents adapter, never imported.
//
// Shape and guarantees (the same as ReleaseConfirmationConsumer, ADR
// 0043, which itself mirrors PlannedCapacityConsumer, ADR 0025/0031):
//
//   - CloudEvents 1.0 only, decoded through internal/adapters/kafka/
//     cloudevents.Decode; dispatch on the FULL `type`; every other type
//     on the topic is committed past (never dead-lettered); the
//     CloudEvents `id` is the idempotency key.
//   - At-least-once with an atomic effect: each use case runs its id
//     claim and the state change in ONE UnitOfWork, and this consumer
//     commits the offset only after the use case returned nil (or the
//     message was dead-lettered).
//   - A ShipmentConfirmed naming an order this context does not know, or
//     an order in a non-shippable state, is NOT poison: the use case
//     skips-and-logs it (ADR 0051) and the offset is committed — never a
//     crash, never a dead letter. Only a message that can never be
//     applied (not a CloudEvent, `data` that cannot be decoded) goes to
//     "<topic>.dlq"; a transient failure is retried in-process (bounded,
//     ADR 0025) and only then dead-lettered. Shutdown is never poison: a
//     message interrupted by cancellation stays uncommitted for
//     redelivery.
//   - STABLE shared consumer group, supplied by the composition root
//     from OUTBOUND_SHIPPING_CONSUMER_GROUP — never a literal here,
//     never per-process-unique.
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

	"github.com/claudioed/order-management/internal/application/usecases"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// OutboundShippingEventsTopic is outbound-shipping's integration topic.
const OutboundShippingEventsTopic = "warehouse.outbound-shipping.events"

// Full CloudEvents `type` strings this consumer dispatches on,
// byte-for-byte as outbound-shipping publishes them (its merged
// apis/asyncapi.yaml, the producer contract ADR 0051 pins). Dispatch is
// on the FULL string — never a short name or suffix.
const (
	ceTypeShipmentConfirmed = "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed"
	ceTypePackageMissedLoad = "com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad"
)

const outboundShippingConsumeSpanNameFmt = "kafka.consume %s"

// shipmentConfirmedOrderData is one entry of ShipmentConfirmed's
// data.orders[] — the only member this context reads.
type shipmentConfirmedOrderData struct {
	// OrderID is the DISTINCT source order id of the shipped packages;
	// a package manifested without one was excluded by the producer.
	OrderID string `json:"order_id"`
}

// shipmentConfirmedData is the part of outbound-shipping's real
// ShipmentConfirmed payload this consumer reads: the load id (carried
// onto the raised domain events as the correlation back to the physical
// fact) and the orders[] list. Every other field (site_id, cpt_id,
// carrier, trailer_id, seal_number, departed_at, version) is ignored —
// a fixed struct with no DisallowUnknownFields, the fleet's additive
// forward-compatibility discipline (see the wave-1 spike, Q7).
type shipmentConfirmedData struct {
	LoadId string                       `json:"load_id"`
	Orders []shipmentConfirmedOrderData `json:"orders"`
}

// packageMissedLoadData is the part of outbound-shipping's real
// PackageMissedLoad payload this consumer acts on. source_order_id is a
// BARE OrderId (the producer strips any -line-N suffix its upstream
// PackageManifested.source_order_id carried), which is why this event
// drives the ORDER-WIDE re-promise, not the per-line one. cutoff is
// decoded for logging/telemetry; the re-promise recomputes from current
// inputs. Every other field (package_id, load_id, site_id,
// detected_at, version) is ignored.
type packageMissedLoadData struct {
	SourceOrderId string    `json:"source_order_id"`
	Cutoff        time.Time `json:"cutoff"`
}

// outboundShippingRequest is one decoded event mapped onto the use case
// call it drives: exactly one of confirm / missedLoad is set.
type outboundShippingRequest struct {
	confirm    *usecases.ConfirmShipmentRequest
	missedLoad *usecases.RepromiseMissedLoadRequest
}

// OutboundShippingConsumer consumes OutboundShippingEventsTopic.
type OutboundShippingConsumer struct {
	reader    messageReader
	confirm   *usecases.ConfirmShipment
	repromise *usecases.RepromiseOrder
	logger    *slog.Logger
	dlqWriter dlqSink
}

// NewOutboundShippingConsumer constructs a consumer of
// OutboundShippingEventsTopic on brokers under groupID. groupID comes
// from configuration (the composition root reads
// OUTBOUND_SHIPPING_CONSUMER_GROUP); there is no default here on
// purpose, so a locally run process can never join a live cluster's
// group by accident.
func NewOutboundShippingConsumer(brokers []string, groupID string, confirm *usecases.ConfirmShipment, repromise *usecases.RepromiseOrder, logger *slog.Logger) *OutboundShippingConsumer {
	return NewOutboundShippingConsumerForTopic(brokers, groupID, OutboundShippingEventsTopic, confirm, repromise, logger)
}

// NewOutboundShippingConsumerForTopic is NewOutboundShippingConsumer on
// an explicit topic (isolated integration tests). The dead-letter topic
// is always topic+".dlq".
func NewOutboundShippingConsumerForTopic(brokers []string, groupID, topic string, confirm *usecases.ConfirmShipment, repromise *usecases.RepromiseOrder, logger *slog.Logger) *OutboundShippingConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &OutboundShippingConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		confirm:   confirm,
		repromise: repromise,
		logger:    logger,
		dlqWriter: newDLQWriter(brokers, topic),
	}
}

// Close releases the Kafka reader and the DLQ writer.
func (c *OutboundShippingConsumer) Close() error {
	readerErr := c.reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// Run consumes until ctx is cancelled and only returns then: a broker
// outage is logged and retried with jittered backoff, never returned
// (see RepromiseConsumer.Run for the incident that rule comes from).
func (c *OutboundShippingConsumer) Run(ctx context.Context) error {
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
		if !c.handleUntilDoneOutbound(ctx, policy, msg) {
			return nil
		}
		policy.Reset()
	}
}

// handleUntilDoneOutbound retries one message's handling across
// commit/DLQ-publish failures. It reports false only when ctx is done.
func (c *OutboundShippingConsumer) handleUntilDoneOutbound(ctx context.Context, policy backoff.BackOff, msg kafkago.Message) bool {
	for {
		err := c.handleMessage(ctx, msg)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if !c.waitToRecover(ctx, policy, "handle", err, logKeyPartition, msg.Partition, logKeyOffset, msg.Offset) {
			return false
		}
	}
}

func (c *OutboundShippingConsumer) waitToRecover(ctx context.Context, policy backoff.BackOff, stage string, err error, args ...any) bool {
	wait := policy.NextBackOff()
	c.log(ctx, "outbound shipping: kafka unavailable, retrying",
		append([]any{logKeyStage, stage, logKeyRetryIn, wait.String(), logKeyError, err}, args...)...)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}

// handleMessage processes one fetched message and commits its offset
// only once it is fully handled: applied (or deterministically
// skipped), or dead-lettered.
func (c *OutboundShippingConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	return handleEventMessage(ctx, c, msg, c.reader.Config().Topic,
		outboundShippingRequestFrom, "invalid outbound-shipping event", c.applyWithRetry)
}

// outboundShippingRequestFrom maps a decoded CloudEvent onto the use
// case call it drives. relevant=false means "neither consumed type,
// nothing to do" (forward compatibility: commit past it); a non-nil
// error means the payload can never be applied (deterministic:
// dead-letter it).
func outboundShippingRequestFrom(e ce.Event) (outboundShippingRequest, bool, error) {
	switch e.Type() {
	case ceTypeShipmentConfirmed:
		var data shipmentConfirmedData
		if err := e.DataAs(&data); err != nil {
			return outboundShippingRequest{}, false, fmt.Errorf("decode %s data: %w", e.Type(), err)
		}
		ids := make([]shared.OrderId, 0, len(data.Orders))
		for _, o := range data.Orders {
			if o.OrderID == "" {
				continue
			}
			ids = append(ids, shared.OrderId(o.OrderID))
		}
		return outboundShippingRequest{confirm: &usecases.ConfirmShipmentRequest{
			EventID: e.ID(), LoadID: data.LoadId, OrderIDs: ids,
		}}, true, nil
	case ceTypePackageMissedLoad:
		var data packageMissedLoadData
		if err := e.DataAs(&data); err != nil {
			return outboundShippingRequest{}, false, fmt.Errorf("decode %s data: %w", e.Type(), err)
		}
		if data.SourceOrderId == "" {
			// A package manifested without an order id is excluded from
			// ShipmentConfirmed by the producer too; there is nothing
			// this context can join on. Skip-and-log, commit past.
			return outboundShippingRequest{}, false, nil
		}
		return outboundShippingRequest{missedLoad: &usecases.RepromiseMissedLoadRequest{
			SourceEventId: e.ID(), OrderId: shared.OrderId(data.SourceOrderId), Cutoff: data.Cutoff,
		}}, true, nil
	default:
		return outboundShippingRequest{}, false, nil
	}
}

// applyWithRetry runs the mapped use case up to maxHandlerAttempts
// times with jittered backoff (ADR-0025). Both use cases are
// transactional, so every failed attempt leaves nothing written and the
// claim un-recorded. usecases.ErrInvalidShipmentConfirmation (no
// CloudEvents id — a programming error in the mapping above) is
// permanent, never retried.
func (c *OutboundShippingConsumer) applyWithRetry(ctx context.Context, req outboundShippingRequest) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	return backoff.Retry(func() error {
		var err error
		switch {
		case req.confirm != nil:
			_, err = c.confirm.Execute(ctx, *req.confirm)
			if errors.Is(err, usecases.ErrInvalidShipmentConfirmation) {
				return backoff.Permanent(err)
			}
		case req.missedLoad != nil:
			err = c.repromise.ExecuteForMissedLoad(ctx, *req.missedLoad)
		}
		return err
	}, bounded)
}

// deadLetter publishes the raw message to topic+".dlq" (error context
// in headers) and commits it. logCtx carries the span; ctx is the
// consumer's.
func (c *OutboundShippingConsumer) deadLetter(logCtx, ctx context.Context, msg kafkago.Message, why string, cause error, e *ce.Event) error {
	topic := c.reader.Config().Topic
	args := []any{logKeyTopic, topic, logKeyPartition, msg.Partition, logKeyOffset, msg.Offset,
		logKeyDlqTopic, topic + dlqTopicSuffix, logKeyError, cause}
	if e != nil {
		args = append(args, logKeyCeID, e.ID(), logKeyCeType, e.Type())
	}
	c.log(logCtx, "outbound shipping: "+why+", sending to dead-letter topic", args...)
	if err := c.dlqPublish(ctx, msg, cause); err != nil {
		return fmt.Errorf("outbound shipping: publish to dead-letter topic: %w", err)
	}
	return c.commit(ctx, msg)
}

// dlqPublish writes the raw, unmodified payload plus error context (as
// headers) to the dead-letter topic.
func (c *OutboundShippingConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: dlqHeaderSourceTopic, Value: []byte(c.reader.Config().Topic)},
		kafkago.Header{Key: dlqHeaderError, Value: []byte(cause.Error())},
		kafkago.Header{Key: dlqHeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, c.dlqWriter, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}

// commit acknowledges msg so it is never redelivered.
func (c *OutboundShippingConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *OutboundShippingConsumer) startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})
	return otel.Tracer(repromiseTracerName).Start(ctx,
		fmt.Sprintf(outboundShippingConsumeSpanNameFmt, topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("process"),
		),
	)
}

func (c *OutboundShippingConsumer) log(_ context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.Warn(msg, args...)
	}
}
