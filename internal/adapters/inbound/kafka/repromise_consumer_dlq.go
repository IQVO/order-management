// Package kafka's repromise_consumer_dlq.go holds RepromiseConsumer's retry and dead-letter path.
package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"
)

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
		// RequiredAcks: kafka-go defaults to RequireNone (acks=0), so the
		// broker never confirms the write and a broker blip silently drops the
		// poison message while the consumer commits past it (ADR 0033).
		RequiredAcks: dlqRequiredAcks,
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

// handleWithRetry retries handleFulfillmentEvent up to maxHandlerAttempts
// times with jittered exponential backoff (ADR-0025 §DLQ), bounded by
// ctx's own deadline/cancellation. ports.ErrConcurrentModification is
// retried like every other failure: RepromiseOrder.Execute runs in one
// atomic scope that re-reads the order, so each attempt reasons about
// the writer's now-current state and the conflict normally heals on the
// first retry (ADR-0024). Only when every attempt conflicts does the
// message go to the DLQ — a version conflict is never swallowed.
func (c *RepromiseConsumer) handleWithRetry(ctx context.Context, e ce.Event) error {
	policy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(retryInitialInterval),
		backoff.WithMaxInterval(retryMaxInterval),
	)
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)

	return backoff.Retry(func() error {
		return c.handleFulfillmentEvent(ctx, e)
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

// dlqRequiredAcks makes a dead-letter write wait for every in-sync replica.
const dlqRequiredAcks = kafkago.RequireAll
