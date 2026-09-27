// Package kafka's repromise_consumer.go implements ADR 0014 §5 / ADR
// 0018's inbound half: a Kafka consumer of fulfillment-execution's real
// shipped warehouse.fulfillment.events topic — the SAME shared/fan-out
// topic labor-performance already consumes for TaskCompleted — reacting
// only to TaskCPTMissed and PackageManifested (see fulfillment-
// execution's own ADR 0025, the companion decision that publishes
// these). Every other event type on that topic is silently skipped,
// mirroring labor-performance's own consumer's skip-unrecognized-
// event-type convention exactly (see that repo's
// internal/adapters/inbound/kafka/consumer.go, read as ground truth for
// this file's shape).
//
// Unlike the full-replay local-cache consumers in this same repo
// (kafkacatalog, kafkacptschedule, kafkapathcapacity — which build an
// in-memory read model by replaying a topic from FirstOffset under a
// PER-PROCESS-UNIQUE consumer group), this is a normal at-least-once
// "process each new message once, commit as you go" consumer: it drives
// a real use case (RepromiseOrder) exactly once per event_id via that
// use case's own idempotency gate, and commits its own offset as it
// goes. That is a DIFFERENT correctness shape than the local-cache
// pattern — see the fleet skill's explicit distinction — so this
// consumer uses a STABLE, meaningful, SHARED consumer group
// (RepromiseConsumerGroup), never a per-process-unique one. Using a
// per-process-unique group here would be wrong: it would make every
// process replay the ENTIRE topic history from the start on every
// restart, driving RepromiseOrder for years of already-handled
// TaskCPTMissed/PackageManifested messages (a real, previously-hit bug
// class in this fleet when the two patterns are conflated).
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/application/ports"
	"github.com/claudioed/order-management/internal/application/usecases"
)

// FulfillmentEventsTopic is fulfillment-execution's shared/fan-out
// integration topic, the same one labor-performance already consumes
// for TaskCompleted.
const FulfillmentEventsTopic = "warehouse.fulfillment.events"

// RepromiseConsumerGroup is this consumer's stable, shared Kafka
// consumer group id — see the package doc comment for why this MUST be
// a fixed shared name, not a per-process-unique one.
const RepromiseConsumerGroup = "order-management-repromise"

// dlqTopicSuffix names the dead-letter topic this consumer publishes a
// poison message to, relative to its OWN source topic (never a fixed
// constant): NewRepromiseConsumerForTopic's isolated test topics each
// get their own matching "<topic>.dlq", exactly mirroring how
// NewRepromiseConsumerForTopic already lets tests isolate the source
// topic/group without touching production names.
const dlqTopicSuffix = ".dlq"

// maxHandlerAttempts bounds RepromiseOrder.Execute's in-process retry
// (ADR-0025 §DLQ) before a message is dead-lettered: 1 initial attempt
// plus up to 2 retries, matching the plan's "up to 3" bound.
const maxHandlerAttempts = 3

const (
	retryInitialInterval = 100 * time.Millisecond
	retryMaxInterval     = 2 * time.Second
)

// repromiseTracerName scopes the consume spans this adapter emits.
const repromiseTracerName = "github.com/claudioed/order-management/internal/adapters/inbound/kafka"

const (
	eventTypeTaskCPTMissed      = "TaskCPTMissed"
	eventTypePackageManifested  = "PackageManifested"
	repromiseConsumeSpanNameFmt = "kafka.consume %s"
)

// fulfillmentEnvelope is the inbound decode shape of the fleet envelope
// on warehouse.fulfillment.events, as verified against
// fulfillment-execution's real, merged, shipped publisher (ADR 0025).
// Declared independently here — order-management NEVER imports another
// service's Go packages (see .claude/rules/bounded-context-boundary.md)
// — this is this repo's own copy of the same wire shape, not a shared
// type.
type fulfillmentEnvelope struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

// fulfillmentSpecversionProbe is a minimal decode used purely to
// discriminate today's flat envelope from a CloudEvents 1.0 structured
// envelope (ADR-0027 / ADR-0021 dual-read migration, Phase 2 Task 2e):
// a CloudEvents message carries a non-empty `specversion` key the flat
// envelope never has. This is a DIFFERENT probe/type from
// kafkapathcapacity's own — see this file's package doc comment for why
// the two consumers are kept independently updated.
type fulfillmentSpecversionProbe struct {
	Specversion string `json:"specversion"`
}

// fulfillmentCloudEventsEnvelope is the CloudEvents 1.0 structured
// envelope shape fulfillment-execution's asyncapi.yaml documents for
// this topic. `type` is the reverse-DNS event type string; `data` is
// byte-identical to the flat envelope's `data`.
type fulfillmentCloudEventsEnvelope struct {
	Specversion string          `json:"specversion"`
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Time        time.Time       `json:"time"`
	Data        json.RawMessage `json:"data"`
}

// cloudEventsTypeTaskCPTMissed and cloudEventsTypePackageManifested are
// the exact reverse-DNS `type` strings fulfillment-execution's
// asyncapi.yaml specifies for these two events (ADR-0025, ADR-0027 dual-
// read migration). Read directly from apis/asyncapi.yaml — do not
// re-derive the middle segments.
const (
	cloudEventsTypeTaskCPTMissed     = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	cloudEventsTypePackageManifested = "com.warehouse.wes.fulfillment-execution.package.PackageManifested"
)

// fulfillmentBareEventType strips a CloudEvents reverse-DNS type string
// back to the bare event name the existing switch/case logic in
// handleFulfillmentEvent keys on (e.g.
// "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" ->
// "TaskCPTMissed"). Returns the input unchanged if it contains no dot,
// so a malformed/unexpected type string fails soft downstream (falls
// into handleFulfillmentEvent's "unrecognized event type" default
// branch) rather than panicking here.
func fulfillmentBareEventType(ceType string) string {
	if idx := strings.LastIndex(ceType, "."); idx >= 0 && idx+1 < len(ceType) {
		return ceType[idx+1:]
	}
	return ceType
}

// decodeFulfillmentEnvelope normalizes either wire shape (today's flat
// envelope, or a CloudEvents 1.0 structured envelope, per ADR-0027 /
// ADR-0021 Phase 2 Task 2e) into this consumer's existing internal
// fulfillmentEnvelope representation, so handleMessage/
// handleFulfillmentEvent never need to know which shape arrived on the
// wire. specversion's presence is the sole discriminator (see the ADR's
// decision section) — never inferred from any other field.
func decodeFulfillmentEnvelope(raw []byte) (fulfillmentEnvelope, error) {
	var probe fulfillmentSpecversionProbe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return fulfillmentEnvelope{}, fmt.Errorf("repromise: probe specversion: %w", err)
	}
	if probe.Specversion == "" {
		// Flat envelope path (today's shape, unchanged).
		var flat fulfillmentEnvelope
		if err := json.Unmarshal(raw, &flat); err != nil {
			return fulfillmentEnvelope{}, fmt.Errorf("repromise: unmarshal envelope: %w", err)
		}
		return flat, nil
	}
	if probe.Specversion != "1.0" {
		// Malformed/unrecognized specversion: fail soft, mirroring
		// this consumer's existing malformed-message handling
		// posture (handleMessage logs and commits/skips rather than
		// wedging the Run loop).
		return fulfillmentEnvelope{}, fmt.Errorf("repromise: unrecognized CloudEvents specversion %q", probe.Specversion)
	}
	var ce fulfillmentCloudEventsEnvelope
	if err := json.Unmarshal(raw, &ce); err != nil {
		return fulfillmentEnvelope{}, fmt.Errorf("repromise: unmarshal CloudEvents envelope: %w", err)
	}
	return fulfillmentEnvelope{
		EventID:    ce.ID,
		EventType:  fulfillmentBareEventType(ce.Type),
		OccurredAt: ce.Time,
		Source:     ce.Source,
		Data:       ce.Data,
	}, nil
}

// taskCPTMissedData is fulfillment-execution's real TaskCPTMissed
// payload (its own internal/adapters/outbound/kafka/publisher.go,
// TaskCPTMissedData struct, ADR 0025 §7). TaskType/Cpt are decoded for
// completeness/future use even though RepromiseOrder itself does not
// need them today — order_ref is the one field this consumer actually
// acts on.
type taskCPTMissedData struct {
	TaskId   string    `json:"task_id"`
	OrderRef string    `json:"order_ref"`
	TaskType string    `json:"task_type,omitempty"`
	Cpt      time.Time `json:"cpt"`
}

// packageManifestedData is fulfillment-execution's real
// PackageManifested payload (ADR 0025 §7).
type packageManifestedData struct {
	PackageId string `json:"package_id"`
	OrderRef  string `json:"order_ref"`
}

// RepromiseConsumer consumes FulfillmentEventsTopic, driving
// RepromiseOrder for every TaskCPTMissed/PackageManifested message whose
// order_ref parses as a valid WorkUnitId-shaped
// "{orderId}-line-{lineNo}" reference.
type RepromiseConsumer struct {
	reader         *kafkago.Reader
	repromiseOrder *usecases.RepromiseOrder
	logger         *slog.Logger
	// dlqWriter publishes a poison message (ADR-0025 §DLQ) to
	// topic+dlqTopicSuffix after maxHandlerAttempts in-process retries
	// of handleFulfillmentEvent all fail with a genuine infrastructure
	// error. nil in the zero-value struct some existing unit tests
	// build directly (they never reach handleMessage's DLQ path, only
	// handleFulfillmentEvent) — dlqPublish itself guards against a nil
	// writer so those tests keep compiling unchanged.
	dlqWriter *kafkago.Writer
}

// NewRepromiseConsumer constructs a RepromiseConsumer reading
// FulfillmentEventsTopic on brokers under RepromiseConsumerGroup.
func NewRepromiseConsumer(brokers []string, repromiseOrder *usecases.RepromiseOrder, logger *slog.Logger) *RepromiseConsumer {
	return NewRepromiseConsumerForTopic(brokers, RepromiseConsumerGroup, FulfillmentEventsTopic, repromiseOrder, logger)
}

// NewRepromiseConsumerForTopic constructs a RepromiseConsumer reading
// topic on brokers under groupID. It supports isolated integration
// topics/groups while NewRepromiseConsumer retains the production
// topic/group. The dead-letter topic is always derived as
// topic+dlqTopicSuffix, so an isolated test topic gets its own isolated
// DLQ topic for free.
func NewRepromiseConsumerForTopic(brokers []string, groupID, topic string, repromiseOrder *usecases.RepromiseOrder, logger *slog.Logger) *RepromiseConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &RepromiseConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Topic:   topic,
		}),
		repromiseOrder: repromiseOrder,
		logger:         logger,
		dlqWriter: &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: topic + dlqTopicSuffix,
		},
	}
}

// Close releases the underlying Kafka reader and, if configured, the DLQ
// writer.
func (c *RepromiseConsumer) Close() error {
	readerErr := c.reader.Close()
	if c.dlqWriter == nil {
		return readerErr
	}
	return errors.Join(readerErr, c.dlqWriter.Close())
}

// Run consumes the topic until ctx is cancelled.
func (c *RepromiseConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			return err
		}
	}
}

// handleMessage processes one fetched message inside a
// "kafka.consume <topic>" span whose parent is the producing service's
// publish span, recovered from the message's W3C trace-context headers.
// A malformed or unhandleable message (bad JSON, an order_ref that does
// not parse as a WorkUnitId, or a RepromiseOrder fail-soft outcome) is
// logged and committed rather than redelivered forever — mirroring
// labor-performance's own consumer and this fleet's other Kafka
// consumers' commit-and-skip-on-error convention.
//
// ports.ErrConcurrentModification (the version-column optimistic-
// concurrency sentinel — see the version-column ADR) gets its OWN
// branch, deliberately distinct from every other error
// handleFulfillmentEvent can return: the message is logged but NOT
// committed, so THIS message is safely redelivered on the next
// rebalance/restart rather than silently dropped — a version conflict
// means some OTHER writer (an HTTP retry-allocation/release call, or a
// second repromise message for the same order) already advanced this
// order past the version RepromiseOrder read it at, and reprocessing
// this same message against the order's now-current state is exactly
// the at-least-once semantics this consumer already relies on for
// redelivered messages generally. The consume loop itself continues
// (this is NOT treated as a fatal, abort-the-whole-consumer condition:
// one order's transient conflict must not stop repromising every other
// order), so an in-memory reader keeps advancing past it for the
// remainder of THIS process's run — the message becomes due for
// redelivery only once the process restarts or the partition
// rebalances, which is an accepted, documented trade-off (see the ADR)
// rather than an immediate in-process retry loop.
//
// Every OTHER genuine infrastructure error (Processed/Orders/Events
// erroring, decode/lookup failures returned from
// handleFulfillmentEvent) is retried in-process, with jittered backoff,
// up to maxHandlerAttempts total attempts (ADR-0025 §DLQ) — a
// transient blip (a momentary Postgres hiccup, a lost connection) heals
// itself without ever reaching the DLQ. Only once ALL attempts are
// exhausted does the message go to the dead-letter topic
// (topic+dlqTopicSuffix) with the raw payload and the last error's
// context, and the offset is committed anyway — one poison message must
// never block every order behind it on this partition. A commit
// failure, or a DLQ publish failure, is the only thing that still
// aborts the consume loop (a genuine infrastructure problem this
// process cannot route around by itself).
func (c *RepromiseConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	topic := c.reader.Config().Topic

	msgCtx, span := c.startConsumeSpan(ctx, topic, msg)
	defer span.End()

	env, decodeErr := decodeFulfillmentEnvelope(msg.Value)
	if decodeErr != nil {
		recordSpanError(span, decodeErr)
		c.log(msgCtx, "skipping unparseable kafka message", "topic", topic, "error", decodeErr)
		return c.commit(ctx, msg)
	}

	span.SetAttributes(
		attribute.String("messaging.message.event_id", env.EventID),
		attribute.String("messaging.message.event_type", env.EventType),
		attribute.String("messaging.message.source", env.Source),
	)

	err := c.handleWithRetry(msgCtx, env)
	if err == nil {
		return c.commit(ctx, msg)
	}

	recordSpanError(span, err)
	if errors.Is(err, ports.ErrConcurrentModification) {
		c.log(msgCtx, "repromise: version conflict, leaving message uncommitted for safe redelivery",
			"topic", topic, "event_id", env.EventID, "event_type", env.EventType, "error", err)
		return nil
	}

	c.log(msgCtx, "repromise: exhausted retries, sending to dead-letter topic",
		"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
		"event_id", env.EventID, "event_type", env.EventType, "attempts", maxHandlerAttempts, "error", err)
	if dlqErr := c.dlqPublish(ctx, msg, err); dlqErr != nil {
		return fmt.Errorf("repromise: publish to dead-letter topic: %w", dlqErr)
	}
	return c.commit(ctx, msg)
}

// handleWithRetry retries handleFulfillmentEvent up to maxHandlerAttempts
// times with jittered exponential backoff (ADR-0025 §DLQ), bounded by
// ctx's own deadline/cancellation. ports.ErrConcurrentModification is
// NEVER retried here — handleMessage's own dedicated branch is what
// handles it (leaving the message uncommitted for redelivery), so
// retrying it in this loop too would just waste the retry budget on an
// outcome this loop cannot fix.
func (c *RepromiseConsumer) handleWithRetry(ctx context.Context, env fulfillmentEnvelope) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		err := c.handleFulfillmentEvent(ctx, env)
		if err == nil || errors.Is(err, ports.ErrConcurrentModification) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
}

// dlqPublish writes the raw, unmodified message payload plus error
// context (as headers, so the raw body stays byte-identical for a
// manual replay tool per the plan's ask) to the dead-letter topic. A nil
// dlqWriter (the zero-value RepromiseConsumer some unit tests construct
// directly, which never exercises this path) is a documented no-op
// rather than a nil-pointer panic.
func (c *RepromiseConsumer) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.dlqWriter == nil {
		return nil
	}
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// handleFulfillmentEvent filters for TaskCPTMissed/PackageManifested and
// drives RepromiseOrder. Every other event type on this shared topic is
// silently skipped, not an error — the same shared-topic convention
// labor-performance's own consumer already established for this exact
// topic. A returned error here means a genuine infrastructure failure
// from RepromiseOrder.Execute (Processed/Orders/Events erroring); every
// other condition (unparseable order_ref, no matching line, no promise
// movement) is RepromiseOrder's own fail-soft path and returns nil.
func (c *RepromiseConsumer) handleFulfillmentEvent(ctx context.Context, env fulfillmentEnvelope) error {
	var orderRef, reason string

	switch env.EventType {
	case eventTypeTaskCPTMissed:
		var data taskCPTMissedData
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return fmt.Errorf("repromise: decode %s data: %w", env.EventType, err)
		}
		orderRef, reason = data.OrderRef, eventTypeTaskCPTMissed
	case eventTypePackageManifested:
		var data packageManifestedData
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return fmt.Errorf("repromise: decode %s data: %w", env.EventType, err)
		}
		orderRef, reason = data.OrderRef, eventTypePackageManifested
	default:
		return nil
	}

	orderID, lineNo, ok := usecases.ParseWorkUnitID(orderRef)
	if !ok {
		c.log(ctx, "repromise: order_ref does not parse as a WorkUnitId, skipping",
			"event_id", env.EventID, "event_type", env.EventType, "order_ref", orderRef)
		return nil
	}

	return c.repromiseOrder.Execute(ctx, usecases.RepromiseOrderRequest{
		SourceEventId: env.EventID,
		OrderId:       orderID,
		LineNo:        lineNo,
		Reason:        reason,
	})
}

// commit acknowledges msg so it is never redelivered. Only a commit
// failure itself aborts the consume loop.
func (c *RepromiseConsumer) commit(ctx context.Context, msg kafkago.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return err
	}
	return nil
}

// startConsumeSpan mirrors AnalyticsConsumer.Handle's span setup for
// this consumer's own topic/group.
func (c *RepromiseConsumer) startConsumeSpan(ctx context.Context, topic string, msg kafkago.Message) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier{headers: &msg.Headers})
	return otel.Tracer(repromiseTracerName).Start(ctx,
		fmt.Sprintf(repromiseConsumeSpanNameFmt, topic),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("process"),
		),
	)
}

// recordSpanError marks span as failed without changing any control flow.
func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (c *RepromiseConsumer) log(ctx context.Context, msg string, args ...any) {
	if c.logger != nil {
		c.logger.WarnContext(ctx, msg, args...)
	}
}
