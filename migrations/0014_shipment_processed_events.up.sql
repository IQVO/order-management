-- ADR 0051: ConfirmShipment's idempotency gate. One row per applied
-- outbound-shipping ShipmentConfirmed CloudEvents id, so the at-least-once
-- redelivery the producer's outbox relay can produce never re-ships an
-- order. Purely additive: no existing table or column changes, and with
-- OUTBOUND_SHIPPING_CONSUMER_MODE=off (the default) the table stays empty.
--
-- Shaped exactly like repromise_processed_events (migration 0004) and
-- release_confirmation_processed_events (migration 0012): one idempotency
-- table per consumer, so ids never collide across consumers. The key is
-- the raw CloudEvents id with no per-order scoping: ONE ShipmentConfirmed
-- names MANY orders and ConfirmShipment applies the whole event in ONE
-- transaction, claiming the id once for all of them.
CREATE TABLE shipment_processed_events (
    event_id   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id)
);
