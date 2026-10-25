ALTER TABLE orders
    DROP CONSTRAINT orders_fulfillment_site_id_shape,
    DROP COLUMN fulfillment_site_id;
