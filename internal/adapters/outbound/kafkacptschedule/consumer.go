// Package kafkacptschedule is the outbound adapter that keeps a local,
// in-memory CPT (Critical Pull Time) schedule cache up to date by
// consuming process-path-management's own
// warehouse.process-path-management.events topic — the SAME topic
// internal/adapters/outbound/kafkacatalog already consumes, but this is
// a SEPARATE consumer instance with its own per-process-unique consumer
// group, filtering for a DIFFERENT event type (CPTScheduleChanged rather
// than ProcessPathCreated/Updated/Deactivated). Two independent
// consumers on one topic, each filtering for the event types they care
// about, is exactly kafkacatalog's own established pattern for ignoring
// event types outside its contract — this package is the fourth
// consumer of that shape, just pointed at a different event type.
//
// This package mirrors kafkacatalog's concurrency/readiness design
// byte-for-byte (do not re-derive it): see that package's doc comment
// for the two real bugs its readiness gate and per-process-unique
// consumer group fix, both of which apply here unchanged.
//
// CPTScheduleChanged carries the FULL schedule snapshot for a site every
// time (not a diff) — a fresh consumer replaying from FirstOffset needs
// no prior state, and applying a later CPTScheduleChanged for a site
// simply replaces that site's entire schedule.
package kafkacptschedule

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/domain/order"
)

// Topic is process-path-management's publish topic — the same topic
// kafkacatalog consumes. See that package's doc comment for why this
// service has no business knowing anything else about
// process-path-management beyond this topic name and the envelope/
// payload shapes it decodes.
const Topic = "warehouse.process-path-management.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS
// consumer group on Topic. Deliberately NOT a fixed, shared name, and
// deliberately DIFFERENT from kafkacatalog's own prefix — these are two
// independent consumer group families on the same topic, each replaying
// its own full history for its own event type.
const consumerGroupPrefix = "order-management-cpt-schedule"

// eventTypeChanged is the one event type this consumer acts on.
const eventTypeChanged = "CPTScheduleChanged"

// cptSearchHorizonDays bounds how far into the future NextCutoffs walks
// looking for concrete occurrences of a recurring cutoff rule, so a
// misconfigured or genuinely empty schedule cannot spin forever.
const cptSearchHorizonDays = 14

// envelope is the CloudEvents-like wrapper shared across every
// warehouse-systems publisher.
type envelope struct {
	EventType string          `json:"event_type"`
	Data      json.RawMessage `json:"data"`
}

// scheduleData is the wire payload shape for CPTScheduleChanged, decoded
// verbatim from process-path-management's publisher.
type scheduleData struct {
	SiteId   string       `json:"site_id"`
	Timezone string       `json:"timezone"`
	Cutoffs  []cutoffData `json:"cutoffs"`
}

type cutoffData struct {
	CptId           string   `json:"cpt_id"`
	LocalTime       string   `json:"local_time"`
	DaysOfWeek      []string `json:"days_of_week"`
	ShipMethod      string   `json:"ship_method"`
	EligiblePathIds []string `json:"eligible_path_ids"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// Consumer maintains a local per-site CPT schedule cache by replaying
// Topic from its earliest offset (a fresh, per-process consumer group)
// and applying every CPTScheduleChanged event as a full-snapshot replace
// of that site's schedule. It satisfies ports.CPTScheduleCache via
// NextCutoffs.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu        sync.RWMutex
	schedules map[string]scheduleData
	ready     bool
	readyCh   chan struct{}
	target    targetOffsets
}

// targetOffsets is the per-partition "caught up" watermark captured once
// at startup — see kafkacatalog's identical mechanism for the full
// rationale.
type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, PROCESS-UNIQUE consumer group, starting at the earliest offset.
func NewConsumer(ctx context.Context, brokers []string, logger *slog.Logger) (*Consumer, error) {
	return NewConsumerForTopic(ctx, brokers, Topic, logger)
}

// NewConsumerForTopic constructs the same full-replay consumer as
// NewConsumer, but against an explicit topic, so integration tests can
// exercise production replay/readiness behavior against an isolated
// throwaway topic.
func NewConsumerForTopic(ctx context.Context, brokers []string, topic string, logger *slog.Logger) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	target, err := newTargetOffsets(ctx, brokers, topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacptschedule: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(readerConfig(brokers, topic, uniqueConsumerGroup()))

	c := &Consumer{
		Reader:    reader,
		Logger:    logger,
		schedules: make(map[string]scheduleData),
		readyCh:   make(chan struct{}),
		target:    target,
	}
	if len(target) == 0 {
		c.markReady()
	}
	return c, nil
}

// replayCommitInterval makes the replay reader commit offsets
// periodically and asynchronously instead of after every message.
//
// With a GroupID set and CommitInterval left at zero, kafka-go's
// Reader.ReadMessage performs a SYNCHRONOUS CommitMessages broker round
// trip after EVERY message, which turns boot replay into
// O(history) x RTT. Against the real cluster broker that capped the replay
// at ~126 msg/s (2,000 messages in 15.8s) versus ~3,200 msg/s (10,000
// messages in 3.1s) with a 1s interval — slow enough that order-management
// could not replay warehouse.work-planning.events (~10,600 messages)
// inside WaitReadyTimeout and was killed in a CrashLoopBackOff.
//
// Async commits are safe here, and only here: the consumer group is unique
// to this process instance (uniqueConsumerGroup), its committed offsets are
// never resumed by any other process, and a restart replays from
// FirstOffset under a NEW group. Commit durability is therefore irrelevant
// to correctness — losing the last interval's commits on a crash changes
// nothing — so the per-message synchronous commit is pure overhead. Do NOT
// copy this to a consumer on a fixed, shared group that commits after
// handling (at-least-once): there the synchronous commit is deliberate.
const replayCommitInterval = time.Second

// readerConfig builds the kafka-go ReaderConfig for the full-replay cache
// reader: a process-unique group starting at the earliest offset, with
// periodic asynchronous commits (see replayCommitInterval).
func readerConfig(brokers []string, topic, group string) kafkago.ReaderConfig {
	return kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: replayCommitInterval,
	}
}

// uniqueConsumerGroup builds a group id unique to this process instance
// (hostname + PID + a nanosecond timestamp) — see kafkacatalog's package
// doc comment for why per-process uniqueness is a correctness
// requirement, not a cosmetic choice.
func uniqueConsumerGroup() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", consumerGroupPrefix, host, os.Getpid(), time.Now().UnixNano())
}

// newTargetOffsets dials Topic directly (no consumer group) and reads
// each partition's current last offset, returning only the partitions
// that actually have at least one message. Identical to kafkacatalog's
// own implementation.
func newTargetOffsets(ctx context.Context, brokers []string, topic string) (targetOffsets, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafkacptschedule: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafkacptschedule: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("kafkacptschedule: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("kafkacptschedule: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("kafkacptschedule: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("kafkacptschedule: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			out[p.ID] = last
		}
	}
	return out, nil
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// Ready reports whether this consumer has processed every message that
// existed in Topic at the moment it started. Safe to call concurrently.
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitReady blocks until Ready() would return true or ctx is done,
// whichever comes first.
func (c *Consumer) WaitReady(ctx context.Context) error {
	if c.Ready() {
		return nil
	}
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) markReady() {
	c.mu.Lock()
	alreadyReady := c.ready
	c.ready = true
	c.mu.Unlock()
	if !alreadyReady {
		close(c.readyCh)
	}
}

// Run consumes Topic until ctx is cancelled or the reader returns a
// fatal error. A handling error is logged and the loop continues, so one
// malformed message cannot wedge this consumer.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handle(msg); err != nil {
			c.Logger.ErrorContext(ctx, "CPT schedule message handling failed",
				"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
		}
		c.checkReady(msg)
	}
}

func (c *Consumer) checkReady(msg kafkago.Message) {
	c.mu.RLock()
	target, tracked := c.target[msg.Partition]
	alreadyReady := c.ready
	c.mu.RUnlock()
	if alreadyReady || !tracked {
		return
	}
	if msg.Offset+1 < target {
		return
	}

	c.mu.Lock()
	delete(c.target, msg.Partition)
	allCaughtUp := len(c.target) == 0
	c.mu.Unlock()

	if allCaughtUp {
		c.markReady()
	}
}

func (c *Consumer) handle(msg kafkago.Message) error {
	var env envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		return fmt.Errorf("kafkacptschedule: unmarshal envelope: %w", err)
	}

	switch env.EventType {
	case eventTypeChanged:
		var data scheduleData
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return fmt.Errorf("kafkacptschedule: unmarshal %s data: %w", env.EventType, err)
		}
		if data.SiteId == "" {
			return fmt.Errorf("kafkacptschedule: %s missing site_id", env.EventType)
		}
		c.mu.Lock()
		c.schedules[data.SiteId] = data
		c.mu.Unlock()
	default:
		// Every event type outside CPTScheduleChanged — including
		// ProcessPathCreated/Updated/Deactivated, which
		// kafkacatalog's own independent consumer instance handles
		// — is ignored here. Same fan-out-topic convention as every
		// other consumer in this fleet.
	}
	return nil
}

// NextCutoffs satisfies ports.CPTScheduleCache (and order.ScheduleSource)
// against this consumer's current in-memory snapshot for siteId.
func (c *Consumer) NextCutoffs(siteId string, from time.Time, n int) ([]order.CPTWindow, bool) {
	c.mu.RLock()
	sched, ok := c.schedules[siteId]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	windows, err := computeNextCutoffs(sched, from, n)
	if err != nil {
		if c.Logger != nil {
			c.Logger.Error("failed computing next CPT cutoffs", "site_id", siteId, "error", err)
		}
		return nil, false
	}
	return windows, true
}

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and failing startup outright.
const WaitReadyTimeout = 60 * time.Second
