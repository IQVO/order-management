-- ADR 0052: CapacityPlanSuperseded moves a plan's row to a terminal
-- SUPERSEDED status that never constrains an order. Additive: a nullable
-- superseded_by (the newer plan's id), the status CHECK widened, and a
-- SUPERSEDED row must name its successor. Existing rows are untouched.
ALTER TABLE planned_capacity_windows
    ADD COLUMN superseded_by TEXT NULL;

ALTER TABLE planned_capacity_windows
    DROP CONSTRAINT planned_capacity_windows_status_check;

ALTER TABLE planned_capacity_windows
    ADD CONSTRAINT planned_capacity_windows_status_check
    CHECK (status IN ('DRAFT', 'PUBLISHED', 'SUPERSEDED'));

ALTER TABLE planned_capacity_windows
    ADD CONSTRAINT planned_capacity_windows_superseded_by_check
    CHECK (status <> 'SUPERSEDED' OR superseded_by IS NOT NULL);
