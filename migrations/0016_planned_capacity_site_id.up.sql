-- ADR 0031 (amended): a plan's canonical site_id (warehouse-planning ADR
-- 0012) is kept next to its location, so an order is matched to a plan by
-- site when the plan has one. Additive: a NOT NULL column whose default
-- is the empty string, which is exactly "this plan never carried a
-- site_id" (the legacy match by location). Existing rows are untouched.
ALTER TABLE planned_capacity_windows
    ADD COLUMN site_id TEXT NOT NULL DEFAULT '';

CREATE INDEX planned_capacity_windows_site_idx
    ON planned_capacity_windows (site_id, window_end)
    WHERE site_id <> '';
