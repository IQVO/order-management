package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/order-management/internal/adapters/outbound/kafka"
)

// Sink is where OutboxRelay forwards drained events — in production
// kafka.RelaySink; in tests a recorder.
type Sink interface {
	Send(ctx context.Context, enc outboundkafka.Encoded) error
}

// OutboxRelay drains unpublished outbox_events rows onto a Sink, oldest
// first, marking each row published as it goes (see docs/docs/adr/
// 0026-transactional-outbox.md).
//
// Delivery is at-least-once: a crash between a successful Send and the
// row's UPDATE republishes that event on the next pass. Both downstream
// topics this service fans out to are already documented as tolerating
// this (see CLAUDE.md's Kafka integration section / the fan-out
// publisher's own doc comment) — a duplicate integration or analytics
// message is a safe, idempotent re-delivery for their consumers.
// Ordering within one topic is preserved because rows are drained in id
// order within one relay pass and FOR UPDATE SKIP LOCKED keeps two
// relays (a rolling deploy's overlapping old and new pod) from claiming
// the same row.
//
// A row that keeps failing is capped by WithMaxAttempts (ADR 0040): once
// it has failed that many times it is excluded from future claims, so one
// poison row cannot stop the whole relay forever. Such a row stays in
// outbox_events with its attempts and last_error for inspection; later
// rows with the same key overtake it (the documented price of the cap).
type OutboxRelay struct {
	pool        *pgxpool.Pool
	sink        Sink
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// RelayOption customises an OutboxRelay.
type RelayOption func(*OutboxRelay)

// WithInterval sets how long the relay sleeps between passes when the
// last pass found nothing to publish. Default 1s.
func WithInterval(d time.Duration) RelayOption {
	return func(r *OutboxRelay) { r.interval = d }
}

// WithBatchSize caps how many rows one pass claims. Default 100.
func WithBatchSize(n int) RelayOption {
	return func(r *OutboxRelay) { r.batchSize = n }
}

// WithMaxAttempts caps how many failed sends a row may accumulate before
// the relay stops claiming it. The default 0 means unlimited retries (the
// pre-cap behaviour). A row that reaches the cap is left unpublished in
// outbox_events with attempts and last_error, logged once at ERROR with
// its id, topic, event type and key, and re-armed by an operator with
// `UPDATE outbox_events SET attempts = 0 WHERE id = ...` (ADR 0040).
func WithMaxAttempts(n int) RelayOption {
	return func(r *OutboxRelay) { r.maxAttempts = n }
}

// NewOutboxRelay constructs a relay draining pool into sink.
func NewOutboxRelay(pool *pgxpool.Pool, sink Sink, logger *slog.Logger, opts ...RelayOption) *OutboxRelay {
	if logger == nil {
		logger = slog.Default()
	}
	r := &OutboxRelay{pool: pool, sink: sink, logger: logger, interval: time.Second, batchSize: 100}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Run drains the outbox until ctx is cancelled. A pass that publishes a
// full batch is followed immediately by another pass (there is probably
// more waiting); an empty pass sleeps for the configured interval. A
// failing pass is logged and retried after the interval — the rows stay
// unpublished, so nothing is lost.
func (r *OutboxRelay) Run(ctx context.Context) error {
	for {
		n, err := r.RelayOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay pass failed", "error", err)
		}
		if n == r.batchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
}

// pendingRow is one claimed, not-yet-sent outbox row.
type pendingRow struct {
	id  int64
	enc outboundkafka.Encoded
}

// RelayOnce performs a single pass: claim up to batchSize unpublished
// rows under a row lock, send each in order, and mark it published. It
// returns how many rows were published. On the first Send failure the
// pass stops (preserving per-topic ordering — a later event for the same
// topic must not overtake a failed earlier one), records the error on
// that row, and returns it; rows already sent in this pass stay marked
// published.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: begin relay pass: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := claimOutboxRows(ctx, tx, r.batchSize, r.maxAttempts)
	if err != nil {
		return 0, err
	}
	return r.publishBatch(ctx, tx, batch)
}

// claimOutboxRows selects up to batchSize unpublished rows under a row
// lock, in id order, and decodes them for sending. With maxAttempts > 0 a
// row that already failed maxAttempts times is no longer claimed.
func claimOutboxRows(ctx context.Context, tx pgx.Tx, batchSize, maxAttempts int) ([]pendingRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, topic, event_type, key, value, headers
		FROM outbox_events
		WHERE published_at IS NULL
		  AND ($2::int <= 0 OR attempts < $2::int)
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox rows: %w", err)
	}
	var batch []pendingRow
	for rows.Next() {
		var p pendingRow
		var headersRaw []byte
		if err := rows.Scan(&p.id, &p.enc.Topic, &p.enc.EventType, &p.enc.Key, &p.enc.Value, &headersRaw); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: scan outbox row: %w", err)
		}
		headers, err := unmarshalHeaders(headersRaw)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: unmarshal outbox row %d headers: %w", p.id, err)
		}
		p.enc.Headers = headers
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read outbox rows: %w", err)
	}
	return batch, nil
}

// publishBatch sends each claimed row and marks it published. On the
// first Send failure the pass stops (preserving per-topic ordering — a
// later event for the same topic must not overtake a failed earlier
// one), records the error on that row, and returns it; rows already sent
// in this pass stay marked published.
func (r *OutboxRelay) publishBatch(ctx context.Context, tx pgx.Tx, batch []pendingRow) (int, error) {
	published := 0
	for _, p := range batch {
		if err := r.sink.Send(ctx, p.enc); err != nil {
			return published, r.recordSendFailure(ctx, tx, p, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = $1
		`, p.id); err != nil {
			return published, fmt.Errorf("postgres: mark outbox row %d published: %w", p.id, err)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return published, fmt.Errorf("postgres: commit relay pass: %w", err)
	}
	if published > 0 {
		r.logger.DebugContext(ctx, "outbox relay published events", "count", published)
	}
	return published, nil
}

// recordSendFailure logs the send error on the failed row and commits the
// pass so earlier rows keep their published marks, wrapping every
// bookkeeping failure around the original send error. When this failure
// brings the row to the attempt cap, the row is reported once at ERROR:
// from the next pass on it is no longer claimed.
func (r *OutboxRelay) recordSendFailure(ctx context.Context, tx pgx.Tx, p pendingRow, err error) error {
	var attempts int
	if uerr := tx.QueryRow(ctx, `
		UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts
	`, p.id, err.Error()).Scan(&attempts); uerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: record outbox failure: %w", uerr))
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		err = errors.Join(err, fmt.Errorf("postgres: commit relay pass: %w", cerr))
	} else if r.maxAttempts > 0 && attempts >= r.maxAttempts {
		r.logger.ErrorContext(ctx, "outbox relay: row reached the attempt cap and is no longer claimed",
			"outbox_id", p.id, "topic", p.enc.Topic, "event_type", p.enc.EventType, "key", string(p.enc.Key),
			"attempts", attempts, "max_attempts", r.maxAttempts, "error", err,
			"rearm", "UPDATE outbox_events SET attempts = 0 WHERE id = <outbox_id>")
	}
	return fmt.Errorf("outbox relay: send %s (row %d) to %s: %w", p.enc.EventType, p.id, p.enc.Topic, err)
}
