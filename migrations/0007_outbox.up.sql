-- Transactional outbox (see docs/docs/adr/0026-transactional-outbox.md).
-- Every domain event a use case raises is written here in the SAME
-- transaction as the aggregate change that caused it, once per (event x
-- topic) pair — this service fans every event out to two topics
-- (integration + analytics, see kafka.FanOutPublisher), so the same
-- event enqueues up to two rows, one per topic/encoder. A background
-- relay (postgres.OutboxRelay) drains unpublished rows onto Kafka
-- afterwards, so the DB write and the previous direct-to-Kafka publish
-- can never diverge the way the old, undrained postgres.EventPublisher
-- (writing to `events`, with no relay reading it) risked.
--
-- This retires `events` (migrations/0001_init.up.sql): it was written to
-- but never drained by anything ("a future outbox-style Kafka relay,
-- deferred in v1" — see the old postgres.EventPublisher's doc comment),
-- so there is no undelivered history worth preserving; it is dropped
-- outright rather than left as a second, competing table.
DROP TABLE IF EXISTS events;

CREATE TABLE outbox_events (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    key          BYTEA,
    value        BYTEA       NOT NULL,
    -- JSON array of {"key","value"} pairs — the W3C trace-context headers
    -- captured at Encode time (see kafka.Encoded), so the relay's Sink
    -- can hand Kafka the exact same headers the direct-publish path
    -- would have sent, keeping consumer-side tracing unaffected by
    -- whether an event took the outbox path or not.
    headers      JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT
);

-- The relay only ever asks "what is still unpublished, oldest first"; a
-- partial index keeps that scan tiny no matter how much published
-- history accumulates.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (id) WHERE published_at IS NULL;
