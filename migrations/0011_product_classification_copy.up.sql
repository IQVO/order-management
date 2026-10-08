-- ADR 0036: order-management's LOCAL copy of product-master's product
-- classifications, fed ONLY by the ProductClassified CloudEvents that
-- service publishes on warehouse.product-master.events. Replaces the live
-- GET /products/{sku}/classification lookup against inventory-storage.
-- Purely additive: no existing table or column changes.
--
-- sku is the CloudEvents subject / Kafka key. version is product-master's
-- aggregate version after the change: a message is applied only when its
-- version is greater than the stored one (see
-- productclassificationcopy.PostgresStore.Upsert). temperature_class and
-- dot_hazard_class are NULL when product-master left them unset.
CREATE TABLE product_classification_copy (
    sku               TEXT PRIMARY KEY,
    handling_tags     TEXT[] NOT NULL DEFAULT '{}',
    temperature_class TEXT,
    dot_hazard_class  SMALLINT,
    version           BIGINT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (version >= 1),
    CHECK (dot_hazard_class IS NULL OR dot_hazard_class BETWEEN 1 AND 9)
);

-- The idempotency gate of the product-classification consumer, keyed on
-- the CloudEvents id. Its own table, like every other consumer's in this
-- repo, so consumers never share a key space.
CREATE TABLE product_classification_processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
