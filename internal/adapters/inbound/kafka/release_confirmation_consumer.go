// Package kafka's release_confirmation_consumer.go is the inbound half of
// ADR 0043: a Kafka consumer of wes-work-planning's integration topic that
// turns its WorkUnitCreated events into release confirmations
// (usecases.ConfirmRelease). It never calls wes-work-planning; it only reads
// what that service published.
//
// Shape and guarantees (the same as PlannedCapacityConsumer, ADR 0025/0031):
//
//   - CloudEvents 1.0 only, decoded through internal/adapters/kafka/
//     cloudevents.Decode; dispatch on the FULL `type`; every other type on
//     the topic is committed past (never dead-lettered); the CloudEvents `id`
//     is the idempotency key.
//   - At-least-once with an atomic effect: the use case runs the id claim and
//     the state change in ONE UnitOfWork, and this consumer commits the offset
//     only after the use case returned nil (or the message was dead-lettered).
//   - A WorkUnitCreated without the optional `reference`/`line_no` fields, or
//     for a line this service never released, is not poison: the use case
//     counts it and the offset is committed. Only a message that can never be
//     applied (not a CloudEvent, `data` that cannot be decoded) goes to
//     "<topic>.dlq"; a transient failure is retried in-process (bounded,
//     ADR 0025) and only then dead-lettered. Shutdown is never poison: a
//     message interrupted by cancellation stays uncommitted for redelivery.
//   - STABLE shared consumer group, supplied by the composition root from
//     RELEASE_CONFIRMATION_CONSUMER_GROUP — never a literal here, never
//     per-process-unique.
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
	"github.com/claudioed/order-management/internal/application/usecases"
)

// WorkPlanningEventsTopic is wes-work-planning's integration topic. It
// carries every work-unit event, order work and transfer/RELOCATE/legacy
// work alike.
const WorkPlanningEventsTopic = "warehouse.work-planning.events"

// ceTypeWorkUnitCreated is the full CloudEvents `type`, byte-for-byte as
// wes-work-planning publishes it. Dispatch is on the whole string.
const ceTypeWorkUnitCreated = "com.warehouse.wes.work-planning.workunit.WorkUnitCreated"

const releaseConfirmationConsumeSpanNameFmt = "kafka.consume %s"

// workUnitCreatedData is the part of WorkUnitCreated.data this consumer
// reads: the two OPTIONAL fields wes-work-planning stamps on the integration
// stream (its ADR 0041). Both are absent for transfer, RELOCATE and legacy
// work, and for a producer that predates them. Every other field is ignored.
type workUnitCreatedData struct {
	// Reference is the work unit's originating reference; for order work,
	// the order id.
	Reference string `json:"reference"`
	// LineNo is the order line number (> 0); omitted when unknown.
	LineNo int `json:"line_no"`
}

// ReleaseConfirmationConsumer consumes WorkPlanningEventsTopic and drives
// usecases.ConfirmRelease.
type ReleaseConfirmationConsumer struct {
	reader    messageReader
	confirm   *usecases.ConfirmRelease
	logger    *slog.Logger
	dlqWriter dlqSink
}

// NewReleaseConfirmationConsumer constructs a consumer of
// WorkPlanningEventsTopic on brokers under groupID. groupID comes from
// configuration (the composition root reads
// RELEASE_CONFIRMATION_CONSUMER_GROUP); there is no default here on purpose,
// so a locally run process can never join a live cluster's group by
// accident.
func NewReleaseConfirmationConsumer(brokers []string, groupID string, confirm *usecases.ConfirmRelease, logger *slog.Logger) *ReleaseConfirmationConsumer {
	return NewReleaseConfirmationConsumerForTopic(brokers, groupID, WorkPlanningEventsTopic, confirm, logger)
}

// NewReleaseConfirmationConsumerForTopic is NewReleaseConfirmationConsumer on
// an explicit topic (isolated integration tests). The dead-letter topic is
// always topic+".dlq".
func NewReleaseConfirmationConsumerForTopic(brokers []string, groupID, topic string, confirm *usecases.ConfirmRelease, logger *slog.Logger) *ReleaseConfirmationConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReleaseConfirmationConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		confirm:   confirm,
		logger:    logger,
		dlqWriter: newDLQWriter(brokers, topic),
	}
}

// Close releases the Kafka reader and the DLQ writer.
func (c *ReleaseConfirmationConsumer) Close() error {
	readerErr := c.reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// Run consumes until ctx is cancelled and only returns then: a broker outage
// is logged and retried with jittered backoff, never returned (see
// RepromiseConsumer.Run for the incident that rule comes from).
func (c *ReleaseConfirmationConsumer) Run(ctx context.Context) error {
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

// handleUntilDone retries one message's handling across commit/DLQ-publish
// failures. It reports false only when ctx is done.
func (c *ReleaseConfirmationConsumer) handleUntilDone(ctx context.Context, policy backoff.BackOff, msg kafkago.Message) bool {
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

func (c *ReleaseConfirmationConsumer) waitToRecover(ctx context.Context, policy backoff.BackOff, stage string, err error, args ...any) bool {
	wait := policy.NextBackOff()
	c.log(ctx, "release confirmation: kafka unavailable, retrying",
		append([]any{"stage", stage, "retry_in", wait.String(), "error", err}, args...)...)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}

// handleMessage processes one fetched message and commits its offset only
// once it is fully handled: applied (or deterministically ignored), or
// dead-lettered. Only a commit or DLQ-publish failure is returned (the
// caller retries the same message).
func (c *ReleaseConfirmationConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	topic := c.reader.Config().Topic
	msgCtx, span := c.startConsumeSpan(ctx, topic, msg)
	defer span.End()

	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "non-CloudEvents message", err, nil)
	}
	span.SetAttributes(
		semconv.MessagingMessageID(e.ID()),
		semconv.CloudEventsEventType(e.Type()),
		semconv.CloudEventsEventSource(e.Source()),
	)

	req, relevant, err := releaseConfirmationRequestFrom(e)
	if err != nil {
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "invalid work unit event", err, &e)
	}
	if !relevant {
		// Any other type on the topic (forward compatibility): commit past.
		return c.commit(ctx, msg)
	}

	if err := c.confirmWithRetry(msgCtx, req); err != nil {
		if ctx.Err() != nil {
			// Shutting down: never dead-letter a message only cancellation
			// interrupted. The offset stays uncommitted, so it is redelivered
			// on the next start and the use case's claim absorbs the replay.
			return ctx.Err()
		}
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "exhausted retries", err, &e)
	}
	return c.commit(ctx, msg)
}

// releaseConfirmationRequestFrom maps a decoded CloudEvent to a confirm
// request. relevant=false means "not a WorkUnitCreated, nothing to do"; a
// non-nil error means the payload can never be applied (deterministic:
// dead-letter it).
//
// A WorkUnitCreated whose optional fields are absent is still relevant: the
// use case counts it as ignored, which is how a transfer or legacy work unit
// shows up in the metrics.
func releaseConfirmationRequestFrom(e ce.Event) (usecases.ConfirmReleaseRequest, bool, error) {
	if e.Type() != ceTypeWorkUnitCreated {
		return usecases.ConfirmReleaseRequest{}, false, nil
	}
	var data workUnitCreatedData
	if err := e.DataAs(&data); err != nil {
		return usecases.ConfirmReleaseRequest{}, false, fmt.Errorf("decode %s data: %w", e.Type(), err)
	}
	return usecases.ConfirmReleaseRequest{EventID: e.ID(), Reference: data.Reference, LineNo: data.LineNo}, true, nil
}

// confirmWithRetry runs the use case up to maxHandlerAttempts times with
// jittered backoff (ADR-0025). The use case is transactional, so every
// failed attempt leaves nothing written and the claim un-recorded.
func (c *ReleaseConfirmationConsumer) confirmWithRetry(ctx context.Context, req usecases.ConfirmReleaseRequest) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	return backoff.Retry(func() error {
		_, err := c.confirm.Execute(ctx, req)
		if errors.Is(err, usecases.ErrInvalidReleaseConfirmation) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
}

// deadLetter publishes the raw message to topic+".dlq" (error context in
// headers) and commits it. logCtx carries the span; ctx is the consumer's.
func (c *ReleaseConfirmationConsumer) deadLetter(logCtx, ctx context.Context, msg kafkago.Message, why string, cause error, e *ce.Event) error {
	topic := c.reader.Config().Topic
	args := []any{"topic", topic, "partition", msg.Partition, "offset", msg.Offset,
		"dlq_topic", topic + dlqTopicSuffix, "error", cause}
	if e != nil {
		args = append(args, "ce_id", e.ID(), "ce_type", e.Type())
	}
	c.log(logCtx, "release confirmation: "+why+", sending to dead-letter topic", args...)
	if err := c.dlqPublish(ctx, msg, cause); err != nil {
		return fmt.Errorf("release confirmation: publish to dead-letter topic: %w", err)
	}
	return c.commit(ctx, msg)
}

// dlqPublish writes the raw, unmodified payload plus error context (as
// headers) to the dead-letter topic.
func (c *ReleaseConfirmationConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return writeDLQ(ctx, c.dlqWriter, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}

// commit acknowledges msg so it is never redelivered.
func (c *ReleaseConfirmationConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *ReleaseConfirmationConsumer) startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})
	return otel.Tracer(repromiseTracerName).Start(ctx,
		fmt.Sprintf(releaseConfirmationConsumeSpanNameFmt, topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("process"),
		),
	)
}

func (c *ReleaseConfirmationConsumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}
