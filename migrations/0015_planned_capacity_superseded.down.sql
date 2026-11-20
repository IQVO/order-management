-- Reverts 0015. The old status CHECK cannot hold a SUPERSEDED row, so the
-- superseded plans are deleted first (ADR 0052, Consequences: a rolled-back
-- binary would again accept a redelivered Published for such a plan).
DELETE FROM planned_capacity_windows WHERE status = 'SUPERSEDED';

ALTER TABLE planned_capacity_windows
    DROP CONSTRAINT IF EXISTS planned_capacity_windows_superseded_by_check;

ALTER TABLE planned_capacity_windows
    DROP CONSTRAINT IF EXISTS planned_capacity_windows_status_check;

ALTER TABLE planned_capacity_windows
    ADD CONSTRAINT planned_capacity_windows_status_check
    CHECK (status IN ('DRAFT', 'PUBLISHED'));

ALTER TABLE planned_capacity_windows
    DROP COLUMN IF EXISTS superseded_by;
