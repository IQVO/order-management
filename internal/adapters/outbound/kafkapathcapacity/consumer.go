// Package kafkapathcapacity is the outbound adapter that keeps a local,
// in-memory remaining-capacity cache up to date by consuming
// wes-work-planning's warehouse.work-planning.events topic — a
// BRAND-NEW topic and event-source for this service, distinct from
// process-path-management's warehouse.process-path-management.events
// that kafkacatalog and kafkacptschedule already consume. It satisfies
// ports.PathCapacity (Remaining) and a Ready()/WaitReady() gate so the
// composition root can block starting real work until the initial
// replay of this topic's full history completes.
//
// This package mirrors kafkacatalog's and kafkacptschedule's own
// concurrency/readiness design byte-for-byte (do not re-derive it): see
// kafkacatalog's package doc comment for the two real bugs its readiness
// gate and per-process-unique consumer group fix, both of which apply
// here unchanged:
//
//  1. A readiness gate that only re-evaluates "am I caught up" on a NEW
//     message deadlocks forever on an ordinary restart where nothing new
//     has been published since the last run.
//  2. A FIXED, shared consumer group name lets a new process resume from
//     a PRIOR process's committed offset, reporting itself ready with an
//     empty local cache having never actually replayed anything. Fixed
//     by making every NewConsumer call use a unique, process-scoped
//     group id — never a shared name.
//
// This consumer filters strictly for the full CloudEvents type
// com.warehouse.wes.work-planning.workpool.PathCapacityChanged on this topic — wes-work-planning publishes other event types on the
// same topic (ShiftPlanCommitted, WorkUnitCreated, etc.) that this
// service has no interest in and ignores, the same fan-out-topic
// convention kafkacatalog/kafkacptschedule already established for
// process-path-management's topic.
//
// Correlation design (ADR-0015): wes-work-planning's PathCapacityChanged
// carries its own native CutoffAt (a time.Time), never
// process-path-management's cptId string. This cache is therefore keyed
// by (PathId, CutoffAt) — an EXACT match on the cutoff instant — not by
// cptId at all. cptId is accepted by Remaining purely for
// logging/observability; it plays no role in the lookup. See ADR-0015
// for why exact-match was chosen over a nearest-window-within-tolerance
// fallback.
package kafkapathcapacity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/order-management/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/order-management/internal/domain/shared"
)

// Topic is wes-work-planning's publish topic for its integration events
// — a NEW topic for this service, distinct from
// warehouse.process-path-management.events.
const Topic = "warehouse.work-planning.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS
// consumer group on Topic. Deliberately NOT a fixed, shared name — see
// the package doc comment for why per-process uniqueness is a
// correctness requirement here, not a cosmetic choice.
const consumerGroupPrefix = "order-management-path-capacity"

// ceTypeChanged is the one full CloudEvents `type` this consumer acts on,
// byte-for-byte as wes-work-planning publishes it (fleet standard §4
// cross-service table). Dispatch is on the FULL string, never a short name.
const ceTypeChanged = "com.warehouse.wes.work-planning.workpool.PathCapacityChanged"

// capacityData is the wire payload shape for PathCapacityChanged,
// decoded verbatim from wes-work-planning's publisher (its own
// internal/adapters/outbound/kafka/publisher.go, PR #69 / ADR-0018).
type capacityData struct {
	PathId         string    `json:"path_id"`
	CutoffAt       time.Time `json:"cutoff_at"`
	RemainingUnits int       `json:"remaining_units"`
	Known          bool      `json:"known"`
}

// capacityKey identifies one cached capacity entry: a path at an exact
// cutoff instant. CutoffAt is normalized to UTC before use so the same
// instant expressed with a different offset/location still hits the
// same cache entry.
type capacityKey struct {
	pathID   string
	cutoffAt int64 // UnixNano, UTC — a comparable map key for time.Time
}

func newCapacityKey(pathID string, cutoffAt time.Time) capacityKey {
	return capacityKey{pathID: pathID, cutoffAt: cutoffAt.UTC().UnixNano()}
}

// capacityEntry is one cached PathCapacityChanged observation.
type capacityEntry struct {
	remainingUnits int
	known          bool
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// Consumer maintains a local remaining-capacity cache by replaying Topic
// from its earliest offset (a fresh, per-process consumer group) and
// applying every PathCapacityChanged event as it arrives. It satisfies
// ports.PathCapacity via Remaining, delegating to the current in-memory
// snapshot.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu      sync.RWMutex
	entries map[capacityKey]capacityEntry
	ready   bool
	readyCh chan struct{}
	target  targetOffsets
}

// targetOffsets is the per-partition "caught up" watermark captured once
// at startup — see kafkacatalog's identical mechanism for the full
// rationale.
type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, PROCESS-UNIQUE consumer group, starting at the earliest offset.
// See the package doc comment for why the group must never be a fixed
// shared name.
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
		return nil, fmt.Errorf("kafkapathcapacity: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(readerConfig(brokers, topic, uniqueConsumerGroup()))

	c := &Consumer{
		Reader:  reader,
		Logger:  logger,
		entries: make(map[capacityKey]capacityEntry),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		// The topic has no partitions with any messages yet — there is
		// nothing to catch up to, so this consumer is trivially ready
		// from the start.
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
		return nil, fmt.Errorf("kafkapathcapacity: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafkapathcapacity: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("kafkapathcapacity: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("kafkapathcapacity: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("kafkapathcapacity: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("kafkapathcapacity: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			// last is exclusive (the offset of the NEXT message to be
			// written) — a consumer has caught up once it has
			// processed the message at offset last-1.
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
// whichever comes first. Used by the composition root to gate starting
// real work on cache completeness.
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

// Remaining satisfies ports.PathCapacity against this consumer's current
// in-memory snapshot. cptId is accepted for interface compatibility and
// observability only — correlation is on (pathId, cutoffAt) exactly, per
// the package doc comment. A path/cutoff this consumer has never
// observed a PathCapacityChanged for reports known=false: this is the
// correct, honest "no capacity signal yet" answer, not a bug — see
// ADR-0015's "what's still NOT solved" section.
func (c *Consumer) Remaining(pathID shared.PathId, _ string, cutoffAt time.Time) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[newCapacityKey(pathID.String(), cutoffAt)]
	if !ok || !entry.known {
		return 0, false
	}
	return entry.remainingUnits, true
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
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				// Not a CloudEvents 1.0 event (e.g. the retired flat
				// envelope): skip past it, never parse another shape.
				c.Logger.WarnContext(ctx, "skipping non-CloudEvents message",
					"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
			} else {
				c.Logger.ErrorContext(ctx, "path capacity message handling failed",
					"topic", msg.Topic, "offset", msg.Offset, "partition", msg.Partition, "error", err)
			}
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

// handle decodes msg as a CloudEvents 1.0 event and applies it. A
// non-CloudEvents message returns an error wrapping
// cloudevents.ErrNotCloudEvent (Run logs it at WARN and moves on). Each
// PathCapacityChanged is a last-write-wins observation for its
// (path, cutoff) key, so re-applying the same event is idempotent.
func (c *Consumer) handle(msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		return fmt.Errorf("kafkapathcapacity: %w", err)
	}

	switch e.Type() {
	case ceTypeChanged:
		var data capacityData
		if err := e.DataAs(&data); err != nil {
			return fmt.Errorf("kafkapathcapacity: unmarshal %s data: %w", e.Type(), err)
		}
		if data.PathId == "" {
			return fmt.Errorf("kafkapathcapacity: %s missing path_id", e.Type())
		}
		if data.CutoffAt.IsZero() {
			return fmt.Errorf("kafkapathcapacity: %s missing cutoff_at", e.Type())
		}
		c.mu.Lock()
		c.entries[newCapacityKey(data.PathId, data.CutoffAt)] = capacityEntry{
			remainingUnits: data.RemainingUnits,
			known:          data.Known,
		}
		c.mu.Unlock()
	default:
		// Every event type outside PathCapacityChanged — including
		// ShiftPlanCommitted, WorkUnitCreated, etc. — is ignored here.
		// Same fan-out-topic convention kafkacatalog/kafkacptschedule
		// already established for process-path-management's topic.
	}
	return nil
}

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and failing startup outright.
const WaitReadyTimeout = 60 * time.Second
