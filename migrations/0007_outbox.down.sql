DROP TABLE IF EXISTS outbox_events;

CREATE TABLE events (
    id          BIGSERIAL PRIMARY KEY,
    event_name  TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL
);
