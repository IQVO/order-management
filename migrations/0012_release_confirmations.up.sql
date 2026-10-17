-- ADR 0043: the release-confirmation process manager. One row per released
-- order line, tracking whether wes-work-planning confirmed the release by
-- raising WorkUnitCreated. Purely additive: no existing table or column
-- changes, and with RELEASE_CONFIRMATION_MODE=off (the default) both tables
-- stay empty.
--
-- A PENDING row is inserted in the same transaction as the release
-- transition. (order_id, line_no) is the key (work_unit_id is derived from it,
-- `{order_id}-line-{line_no}`, and stored for operators' SQL). There is
-- deliberately no foreign key to orders: this is knowledge about another
-- context's behaviour, never an invariant of the order aggregate.
--
-- state: PENDING (released, awaiting WorkUnitCreated) -> CONFIRMED, or
-- PENDING -> UNCONFIRMED (the timeout passed; flagged once) -> CONFIRMED (a
-- late confirmation). unconfirmed_at is kept after a late confirmation as the
-- audit trail.
CREATE TABLE release_confirmations (
    order_id       TEXT        NOT NULL,
    line_no        INTEGER     NOT NULL,
    work_unit_id   TEXT        NOT NULL,
    path_id        TEXT        NOT NULL DEFAULT '',
    released_at    TIMESTAMPTZ NOT NULL,
    state          TEXT        NOT NULL DEFAULT 'PENDING',
    confirmed_at   TIMESTAMPTZ,
    unconfirmed_at TIMESTAMPTZ,
    PRIMARY KEY (order_id, line_no),
    CHECK (line_no > 0),
    CHECK (state IN ('PENDING', 'CONFIRMED', 'UNCONFIRMED'))
);

-- The timeout sweep scans PENDING rows by age; partial, so the index holds
-- only the open loop and stays tiny.
CREATE INDEX release_confirmations_pending_released_at
    ON release_confirmations (released_at)
    WHERE state = 'PENDING';

-- The retention prune scans CONFIRMED rows by confirmation time.
CREATE INDEX release_confirmations_confirmed_at
    ON release_confirmations (confirmed_at)
    WHERE state = 'CONFIRMED';

-- Operators look an UNCONFIRMED line up by state (runbook: "an order line
-- is UNCONFIRMED").
CREATE INDEX release_confirmations_unconfirmed_at
    ON release_confirmations (unconfirmed_at)
    WHERE state = 'UNCONFIRMED';

-- The idempotency gate of the release-confirmation consumer, keyed on the
-- CloudEvents id. A separate table from the other consumers' gates so they
-- never collide.
CREATE TABLE release_confirmation_processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
