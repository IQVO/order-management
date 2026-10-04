// Package kafka's planned_capacity_consumer.go is the inbound half of
// ADR 0031: a Kafka consumer of warehouse-planning's integration topic that
// mirrors its CapacityPlan events into order-management's local planned
// capacity read model (usecases.ApplyPlannedCapacity). It never calls
// warehouse-planning; it only reads what that service published.
//
// Shape and guarantees (the same as RepromiseConsumer, ADR 0018/0025):
//
//   - CloudEvents 1.0 only, decoded through internal/adapters/kafka/
//     cloudevents.Decode; dispatch on the FULL `type`; unknown types are
//     ignored (committed, never dead-lettered); the CloudEvents `id` is the
//     idempotency key.
//   - At-least-once with an atomic effect: the use case runs the id claim
//     and the read-model upsert in ONE UnitOfWork, and this consumer commits
//     the offset only after the use case returned nil (or the message was
//     dead-lettered).
//   - A message that can never be applied (not a CloudEvent, undecodable or
//     invalid payload) goes straight to "<topic>.dlq" and is committed; a
//     transient failure is retried in-process (bounded, ADR 0025) and only
//     then dead-lettered. Nothing here ever crashes the process or blocks
//     the partition.
//   - STABLE shared consumer group, supplied by the composition root from
//     PLANNED_CAPACITY_CONSUMER_GROUP — never a literal here, never
//     per-process-unique (this is a normal process-and-commit consumer, not
//     a full-replay cache).
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
	"github.com/claudioed/order-management/internal/domain/order"
)

// PlanningEventsTopic is warehouse-planning's integration topic
// (key and subject = the capacity plan id).
const PlanningEventsTopic = "warehouse.warehouse-planning.events"

// Full CloudEvents `type` strings, byte-for-byte as warehouse-planning
// publishes them (its apis/asyncapi.yaml). Dispatch is on the whole string.
const (
	ceTypeCapacityPlanCreated      = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated"
	ceTypeCapacityPlanPublished    = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished"
	ceTypeCapacityShortageDetected = "com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected"
	ceTypeBottleneckDetected       = "com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected"
)

const plannedCapacityConsumeSpanNameFmt = "kafka.consume %s"

// capacityPlanData is the superset of the CapacityPlanCreated /
// CapacityPlanPublished / CapacityShortageDetected payloads that this
// consumer reads. Fields a given type does not carry stay zero.
type capacityPlanData struct {
	PlanID             string    `json:"plan_id"`
	WarehouseID        string    `json:"warehouse_id"`
	Location           string    `json:"location"`
	PathID             string    `json:"path_id"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	AssignedDemand     float64   `json:"assigned_demand"`
	CapacityOverWindow float64   `json:"capacity_over_window"`
	Shortage           float64   `json:"shortage"`
	BottleneckStep     string    `json:"bottleneck_step"`
}

// PlannedCapacityConsumer consumes PlanningEventsTopic and drives
// usecases.ApplyPlannedCapacity.
type PlannedCapacityConsumer struct {
	reader    messageReader
	apply     *usecases.ApplyPlannedCapacity
	logger    *slog.Logger
	dlqWriter dlqSink
}

// dlqSink is the slice of *kafkago.Writer the consumer needs; an interface
// so dead-lettering can be unit-tested without a broker.
type dlqSink interface {
	dlqMessageWriter
	Close() error
}

// NewPlannedCapacityConsumer constructs a consumer of PlanningEventsTopic
// on brokers under groupID. groupID comes from configuration (the
// composition root reads PLANNED_CAPACITY_CONSUMER_GROUP); there is no
// default here on purpose, so a locally run process can never join a live
// cluster's group by accident.
func NewPlannedCapacityConsumer(brokers []string, groupID string, apply *usecases.ApplyPlannedCapacity, logger *slog.Logger) *PlannedCapacityConsumer {
	return NewPlannedCapacityConsumerForTopic(brokers, groupID, PlanningEventsTopic, apply, logger)
}

// NewPlannedCapacityConsumerForTopic is NewPlannedCapacityConsumer on an
// explicit topic (isolated integration tests). The dead-letter topic is
// always topic+".dlq".
func NewPlannedCapacityConsumerForTopic(brokers []string, groupID, topic string, apply *usecases.ApplyPlannedCapacity, logger *slog.Logger) *PlannedCapacityConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &PlannedCapacityConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		apply:     apply,
		logger:    logger,
		dlqWriter: newDLQWriter(brokers, topic),
	}
}

// Close releases the Kafka reader and the DLQ writer.
func (c *PlannedCapacityConsumer) Close() error {
	readerErr := c.reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// Run consumes until ctx is cancelled and only returns then: a broker
// outage is logged and retried with jittered backoff, never returned (see
// RepromiseConsumer.Run for the incident that rule comes from).
func (c *PlannedCapacityConsumer) Run(ctx context.Context) error {
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
func (c *PlannedCapacityConsumer) handleUntilDone(ctx context.Context, policy backoff.BackOff, msg kafkago.Message) bool {
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

func (c *PlannedCapacityConsumer) waitToRecover(ctx context.Context, policy backoff.BackOff, stage string, err error, args ...any) bool {
	wait := policy.NextBackOff()
	c.log(ctx, "planned capacity: kafka unavailable, retrying",
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
func (c *PlannedCapacityConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
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

	req, relevant, err := planningRequestFrom(e)
	if err != nil {
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "invalid planning event", err, &e)
	}
	if !relevant {
		// Unknown type (forward compatibility) or a known type that carries
		// nothing the read model lacks (BottleneckDetected): commit past it.
		return c.commit(ctx, msg)
	}

	if err := c.applyWithRetry(msgCtx, req); err != nil {
		recordSpanError(span, err)
		return c.deadLetter(msgCtx, ctx, msg, "exhausted retries", err, &e)
	}
	return c.commit(ctx, msg)
}

// planningRequestFrom maps a decoded CloudEvent to an apply request.
// relevant=false means "nothing to do" (ignored type); a non-nil error
// means the payload can never be applied (deterministic: dead-letter it).
func planningRequestFrom(e ce.Event) (usecases.ApplyPlannedCapacityRequest, bool, error) {
	var status order.PlannedCapacityStatus
	switch e.Type() {
	case ceTypeCapacityPlanCreated:
		status = order.PlannedCapacityDraft
	case ceTypeCapacityPlanPublished, ceTypeCapacityShortageDetected:
		status = order.PlannedCapacityPublished
	default:
		// Includes ceTypeBottleneckDetected: its payload (bottleneck_step,
		// path_capacity) is already carried by CapacityShortageDetected.
		return usecases.ApplyPlannedCapacityRequest{}, false, nil
	}

	var data capacityPlanData
	if err := e.DataAs(&data); err != nil {
		return usecases.ApplyPlannedCapacityRequest{}, false, fmt.Errorf("decode %s data: %w", e.Type(), err)
	}
	if e.Subject() != "" && data.PlanID != "" && e.Subject() != data.PlanID {
		return usecases.ApplyPlannedCapacityRequest{}, false,
			fmt.Errorf("%s: subject %q does not match data.plan_id %q", e.Type(), e.Subject(), data.PlanID)
	}
	w := order.PlannedCapacityWindow{
		PlanID: data.PlanID, WarehouseID: data.WarehouseID, Location: data.Location, PathID: data.PathID,
		Start: data.WindowStart.UTC(), End: data.WindowEnd.UTC(),
		AssignedDemand: data.AssignedDemand, CapacityOverWindow: data.CapacityOverWindow,
		Shortage: data.Shortage, BottleneckStep: data.BottleneckStep,
		Status: status, AsOf: e.Time().UTC(),
	}
	if err := w.Validate(); err != nil {
		return usecases.ApplyPlannedCapacityRequest{}, false, err
	}
	return usecases.ApplyPlannedCapacityRequest{EventID: e.ID(), Window: w}, true, nil
}

// applyWithRetry runs the use case up to maxHandlerAttempts times with
// jittered backoff (ADR-0025). The use case is transactional, so every
// failed attempt leaves nothing written and the claim un-recorded.
func (c *PlannedCapacityConsumer) applyWithRetry(ctx context.Context, req usecases.ApplyPlannedCapacityRequest) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	return backoff.Retry(func() error {
		err := c.apply.Execute(ctx, req)
		if errors.Is(err, order.ErrInvalidPlannedCapacity) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
}

// deadLetter publishes the raw message to topic+".dlq" (error context in
// headers) and commits it. logCtx carries the span; ctx is the consumer's.
func (c *PlannedCapacityConsumer) deadLetter(logCtx, ctx context.Context, msg kafkago.Message, why string, cause error, e *ce.Event) error {
	topic := c.reader.Config().Topic
	args := []any{"topic", topic, "partition", msg.Partition, "offset", msg.Offset,
		"dlq_topic", topic + dlqTopicSuffix, "error", cause}
	if e != nil {
		args = append(args, "ce_id", e.ID(), "ce_type", e.Type())
	}
	c.log(logCtx, "planned capacity: "+why+", sending to dead-letter topic", args...)
	if err := c.dlqPublish(ctx, msg, cause); err != nil {
		return fmt.Errorf("planned capacity: publish to dead-letter topic: %w", err)
	}
	return c.commit(ctx, msg)
}

// dlqPublish writes the raw, unmodified payload plus error context (as
// headers) to the dead-letter topic.
func (c *PlannedCapacityConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
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
func (c *PlannedCapacityConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *PlannedCapacityConsumer) startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})
	return otel.Tracer(repromiseTracerName).Start(ctx,
		fmt.Sprintf(plannedCapacityConsumeSpanNameFmt, topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("process"),
		),
	)
}

func (c *PlannedCapacityConsumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}
