-- ADR 0044: an order may carry an OPTIONAL fulfillment site, assigned by the
-- caller at intake (customer-ordering or network-fulfillment) and never
-- inferred or looked up here. NULLABLE with no default is what makes this
-- additive: every existing row, and every caller that never sends the
-- field, reads back NULL = "no site named", and the SiteSkuDemandChanged
-- projection keeps using the static DEMAND_PROJECTION_SITE_ID (ADR 0035).
-- The column is written only by the INSERT of a new order, never updated.
-- The CHECK mirrors the domain value object (facility-layout's site-code
-- shape: uppercase letters and digits, non-empty) as a last line of defence.
ALTER TABLE orders
    ADD COLUMN fulfillment_site_id TEXT NULL,
    ADD CONSTRAINT orders_fulfillment_site_id_shape CHECK (fulfillment_site_id ~ '^[A-Z0-9]+$');
