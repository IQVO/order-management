// consumer_handle.go is the shared consume body of the CloudEvents
// consumers whose handling is "decode the envelope, map it to one use-case
// request, run that request with bounded retry" (planned capacity, release
// confirmation). Extracted from those two consumers' identical
// handleMessage bodies; the per-consumer plumbing (span, dead-letter,
// commit) stays on each consumer.
package kafka

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
)

// eventMessageConsumer is the per-topic machinery handleEventMessage
// drives: each consumer supplies its own span, dead-letter and commit
// plumbing around one reader.
type eventMessageConsumer interface {
	startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span)
	deadLetter(logCtx, ctx context.Context, msg kafkago.Message, why string, cause error, e *ce.Event) error
	commit(ctx context.Context, msg kafkago.Message) error
}

// handleEventMessage processes one message and commits its offset only
// once it is fully handled: applied (or deterministically ignored), or
// dead-lettered. requestFrom maps the decoded event to the use-case
// request (relevant=false means an unknown type: commit past it, forward
// compatibility; an error means the payload can never be applied:
// dead-letter it with invalidWhy). Only a commit or DLQ-publish failure is
// returned, so the caller retries the same message. A context
// cancellation during the apply is returned as-is, never dead-lettered:
// the offset stays uncommitted, so the message is redelivered on the next
// start and the use case's idempotency claim absorbs the replay.
func handleEventMessage[T any](
	ctx context.Context,
	c eventMessageConsumer,
	msg kafkago.Message,
	topic string,
	requestFrom func(ce.Event) (T, bool, error),
	invalidWhy string,
	apply func(context.Context, T) error,
) error {
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

	req, relevant, err := requestFrom(e)
	if err != nil {
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, invalidWhy, err, &e)
	}
	if !relevant {
		return c.commit(ctx, msg)
	}

	if err := apply(msgCtx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "exhausted retries", err, &e)
	}
	return c.commit(ctx, msg)
}
