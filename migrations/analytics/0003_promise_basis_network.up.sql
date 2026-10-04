-- ADR 0019 / ADR 0020: the promise-basis distribution gains its third
-- bucket. A promise DICTATED by an external party's deadline (basis
-- "Network", order.BasisNetwork) was previously dropped by the projector
-- (no case for it), so Capability + LeadTime silently under-counted every
-- network-originated allocation. Additive only: one new counter column at
-- the SAME (path_id, hour_bucket) grain. Existing rows default to 0, which
-- is correct — Network-basis events seen before this migration were never
-- counted anywhere and cannot be recovered from the rollup.

ALTER TABLE funnel_rollup
    ADD COLUMN promise_basis_network BIGINT NOT NULL DEFAULT 0;
