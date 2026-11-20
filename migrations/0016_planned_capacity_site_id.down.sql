DROP INDEX IF EXISTS planned_capacity_windows_site_idx;

ALTER TABLE planned_capacity_windows
    DROP COLUMN IF EXISTS site_id;
