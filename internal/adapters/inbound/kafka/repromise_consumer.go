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
// a real use case (RepromiseOrder) exactly once per CloudEvents id via that
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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
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

// newDLQWriter builds the dead-letter writer for topic. It MUST set
// AllowAutoTopicCreation: the fleet creates every business topic on
// first write (warehouse-infra kafka.tf, num.partitions=8) and a fresh
// broker has no "<topic>.dlq" yet. Without the flag the first poison
// message fails its DLQ publish with "[3] Unknown Topic Or Partition",
// the offset is (correctly) not committed, and the consumer stops for
// good -- observed live in wes-work-planning, where it halted the
// whole order-to-work-unit flow.
func newDLQWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic + dlqTopicSuffix,
		AllowAutoTopicCreation: true,
		// BatchTimeout: a DLQ write is a synchronous single message; with
		// kafka-go's 1s default the writer holds every write for a full second
		// waiting to fill a batch, capping dead-lettering at ~1 msg/s/partition
		// (observed live: a backlog of legacy messages took hours to drain while
		// the consumer processed nothing else).
		BatchTimeout: dlqBatchTimeout,
	}
}

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

const repromiseConsumeSpanNameFmt = "kafka.consume %s"

// Full CloudEvents `type` strings this consumer dispatches on, byte-for-byte
// as fulfillment-execution publishes them (fleet standard §4 cross-service
// table). Dispatch is on the FULL string — never a short name or suffix.
const (
	ceTypeTaskCPTMissed     = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"
	ceTypePackageManifested = "com.warehouse.wes.fulfillment-execution.package.PackageManifested"
)

// Repromise reasons recorded on OrderRepromised — the upstream domain event
// name that triggered the repromise (the OrderRepromised payload's `reason`
// field is unchanged by the CloudEvents migration).
const (
	reasonTaskCPTMissed     = "TaskCPTMissed"
	reasonPackageManifested = "PackageManifested"
)

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
	reader         messageReader
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

// messageReader is the slice of *kafkago.Reader the consumer uses; an
// interface so the broker-outage recovery in Run can be unit-tested with
// a scripted reader.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Config() kafkago.ReaderConfig
	Close() error
}

// Broker-outage recovery bounds for Run (see its doc comment).
const (
	recoverInitialInterval = 250 * time.Millisecond
	recoverMaxInterval     = 30 * time.Second
)

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
		dlqWriter:      newDLQWriter(brokers, topic),
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

// Run consumes the topic until ctx is cancelled. It only returns when ctx
// is done: a broker outage must never take the service down with it.
//
// Observed live (warehouse-day simulation): the shared broker was
// OOM-killed and restarted; the in-flight offset commit failed with "use of
// closed network connection", Run returned it, and main exited the whole
// order-management process -- every later POST /orders failed until a
// manual restart.
//
// Now a fetch error, or a handleMessage error (which is only ever an
// offset-commit or DLQ-publish failure -- business errors are retried and
// dead-lettered inside handleMessage), is logged and retried with jittered
// exponential backoff (250ms doubling to 30s). A message whose handling
// failed is retried itself until it succeeds, so nothing is skipped: the
// Kafka reader has already moved past it in memory, and only a rebalance
// would otherwise redeliver it. Handling is idempotent on the CloudEvents
// id, so a retried message that partially succeeded is safe.
func (c *RepromiseConsumer) Run(ctx context.Context) error {
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

// handleUntilDone retries one message's handling across infrastructure
// failures. It reports false only when ctx is done.
func (c *RepromiseConsumer) handleUntilDone(ctx context.Context, policy backoff.BackOff, msg kafkago.Message) bool {
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

// waitToRecover logs a recoverable broker-side failure and sleeps the next
// backoff interval. It reports false if ctx ended while waiting.
func (c *RepromiseConsumer) waitToRecover(ctx context.Context, policy backoff.BackOff, stage string, err error, args ...any) bool {
	wait := policy.NextBackOff()
	c.log(ctx, "repromise: kafka unavailable, retrying",
		append([]any{"stage", stage, "retry_in", wait.String(), "error", err}, args...)...)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(wait):
		return true
	}
}

// newRecoverBackoff never gives up (MaxElapsedTime 0): Run is meant to
// outlive any broker outage.
func newRecoverBackoff() backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(recoverInitialInterval),
		backoff.WithMaxInterval(recoverMaxInterval),
		backoff.WithMaxElapsedTime(0),
	)
}

// handleMessage processes one fetched message inside a
// "kafka.consume <topic>" span whose parent is the producing service's
// publish span, recovered from the message's W3C trace-context headers.
// A message that is not a valid CloudEvents 1.0 event (bad JSON, the
// retired flat envelope) is a deterministic poison message: it goes
// straight to the dead-letter topic and is committed. An unhandleable but
// valid event (an order_ref that does not parse as a WorkUnitId, or a
// RepromiseOrder fail-soft outcome) is logged and committed rather than
// redelivered forever — mirroring
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

	e, decodeErr := cloudevents.Decode(msg.Value)
	if decodeErr != nil {
		// Not a valid CloudEvents 1.0 event (bad JSON, the retired flat
		// envelope, ...): a deterministic poison message. Dead-letter it
		// unmodified and commit past it — never retry, never parse any
		// other shape, never block the partition.
		recordSpanError(span, decodeErr)
		c.log(msgCtx, "repromise: non-CloudEvents message, sending to dead-letter topic",
			"topic", topic, "partition", msg.Partition, "offset", msg.Offset,
			"dlq_topic", topic+dlqTopicSuffix, "error", decodeErr)
		if dlqErr := c.dlqPublish(ctx, msg, decodeErr); dlqErr != nil {
			return fmt.Errorf("repromise: publish to dead-letter topic: %w", dlqErr)
		}
		return c.commit(ctx, msg)
	}

	span.SetAttributes(
		semconv.MessagingMessageID(e.ID()),
		semconv.CloudEventsEventType(e.Type()),
		semconv.CloudEventsEventSource(e.Source()),
	)

	err := c.handleWithRetry(msgCtx, e)
	if err == nil {
		return c.commit(ctx, msg)
	}

	recordSpanError(span, err)
	if errors.Is(err, ports.ErrConcurrentModification) {
		c.log(msgCtx, "repromise: version conflict, leaving message uncommitted for safe redelivery",
			"topic", topic, "ce_id", e.ID(), "ce_type", e.Type(), "error", err)
		return nil
	}

	c.log(msgCtx, "repromise: exhausted retries, sending to dead-letter topic",
		"topic", topic, "dlq_topic", topic+dlqTopicSuffix,
		"ce_id", e.ID(), "ce_type", e.Type(), "attempts", maxHandlerAttempts, "error", err)
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
func (c *RepromiseConsumer) handleWithRetry(ctx context.Context, e ce.Event) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		err := c.handleFulfillmentEvent(ctx, e)
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
	return writeDLQ(ctx, c.dlqWriter, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

// handleFulfillmentEvent dispatches on the event's FULL CloudEvents type,
// acting only on TaskCPTMissed/PackageManifested and driving
// RepromiseOrder. Every other type on this shared topic is silently
// skipped, not an error (forward compatibility). The CloudEvents `id` is
// the idempotency key handed to RepromiseOrder. A returned error here
// means a genuine infrastructure failure from RepromiseOrder.Execute (or
// an undecodable `data` payload); every other condition (unparseable
// order_ref, no matching line, no promise movement) is RepromiseOrder's
// own fail-soft path and returns nil.
func (c *RepromiseConsumer) handleFulfillmentEvent(ctx context.Context, e ce.Event) error {
	var orderRef, reason string

	switch e.Type() {
	case ceTypeTaskCPTMissed:
		var data taskCPTMissedData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("repromise: decode %s data: %w", e.Type(), err)
		}
		orderRef, reason = data.OrderRef, reasonTaskCPTMissed
	case ceTypePackageManifested:
		var data packageManifestedData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("repromise: decode %s data: %w", e.Type(), err)
		}
		orderRef, reason = data.OrderRef, reasonPackageManifested
	default:
		return nil
	}

	orderID, lineNo, ok := usecases.ParseWorkUnitID(orderRef)
	if !ok {
		c.log(ctx, "repromise: order_ref does not parse as a WorkUnitId, skipping",
			"ce_id", e.ID(), "ce_type", e.Type(), "order_ref", orderRef)
		return nil
	}

	return c.repromiseOrder.Execute(ctx, usecases.RepromiseOrderRequest{
		SourceEventId: e.ID(),
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

// dlqTopicReadyAttempts / dlqTopicReadyBackoff bound how long a DLQ publish
// waits for an auto-created "<topic>.dlq" to become writable.
const (
	dlqTopicReadyAttempts = 40
	dlqTopicReadyBackoff  = 250 * time.Millisecond
)

// dlqMessageWriter is the slice of *kafkago.Writer writeDLQ needs.
type dlqMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// writeDLQ publishes msg to the dead-letter topic, retrying (bounded) while
// the topic is still being auto-created. AllowAutoTopicCreation alone is not
// enough: the first write races partition leader election and the broker
// answers UnknownTopicOrPartition / LeaderNotAvailable for a few hundred
// milliseconds. Any other error -- or exhausting the budget -- is returned,
// so the caller still refuses to commit the offset (no message loss).
func writeDLQ(ctx context.Context, w dlqMessageWriter, msg kafkago.Message) error {
	var err error
	for attempt := 0; attempt < dlqTopicReadyAttempts; attempt++ {
		if err = w.WriteMessages(ctx, msg); err == nil || !isTopicNotReady(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(dlqTopicReadyBackoff):
		}
	}
	return err
}

// isTopicNotReady reports whether err only means the (auto-created) topic
// has no leader yet.
func isTopicNotReady(err error) bool {
	var werrs kafkago.WriteErrors
	if errors.As(err, &werrs) {
		for _, e := range werrs {
			if e != nil && !isTopicNotReady(e) {
				return false
			}
		}
		return werrs.Count() > 0
	}
	return errors.Is(err, kafkago.UnknownTopicOrPartition) || errors.Is(err, kafkago.LeaderNotAvailable)
}

// dlqBatchTimeout flushes a dead-letter write almost immediately.
const dlqBatchTimeout = 10 * time.Millisecond
