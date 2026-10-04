-- ADR 0031: order-management's local read model of warehouse-planning's
-- CapacityPlans (planned capacity windows), fed ONLY by the CloudEvents
-- that service publishes on warehouse.warehouse-planning.events. Purely
-- additive: no existing table or column changes, and with no planning
-- events consumed both tables stay empty.
--
-- plan_id is the CapacityPlan id (the CloudEvents subject): last writer
-- wins per plan (see order.PlannedCapacityWindow.Supersedes). window_end is
-- exclusive. event_time is the occurred-at of the event that last wrote the
-- row, used to ignore a stale (older) write.
CREATE TABLE planned_capacity_windows (
    plan_id              TEXT PRIMARY KEY,
    warehouse_id         TEXT NOT NULL,
    location             TEXT NOT NULL,
    path_id              TEXT NOT NULL DEFAULT '',
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    assigned_demand      DOUBLE PRECISION NOT NULL DEFAULT 0,
    capacity_over_window DOUBLE PRECISION NOT NULL DEFAULT 0,
    shortage             DOUBLE PRECISION NOT NULL DEFAULT 0,
    bottleneck_step      TEXT NOT NULL DEFAULT '',
    status               TEXT NOT NULL,
    event_time           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (window_end > window_start),
    CHECK (shortage >= 0),
    CHECK (status IN ('DRAFT', 'PUBLISHED'))
);

CREATE INDEX planned_capacity_windows_location_end
    ON planned_capacity_windows (location, window_end);

-- The idempotency gate of the planned-capacity consumer, keyed on the
-- CloudEvents id. A separate table from repromise_processed_events so the
-- two OLTP consumers never collide.
CREATE TABLE planned_capacity_processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
