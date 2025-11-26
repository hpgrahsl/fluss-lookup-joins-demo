SET 'parallelism.default' = '2';
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
SET 'state.backend.rocksdb.predefined-options' = 'FLASH_SSD_OPTIMIZED';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.timeout' = '5min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.unaligned.enabled' = 'true';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

USE CATALOG default_catalog;

-- job1: order lines enrichment using regular joins
INSERT INTO order_lines_enriched
SELECT 
    o.id AS order_id,
    c.id AS customer_id,
    c.name AS customer_name,
    c.membership_level,
    c.shipping_address,
    o.notes,
    o.create_ts AS order_datetime,
    o.creditcard AS creditcard,
    o.discount_percent,
    ol.product_id,
    ol.items AS quantity,
    p.name AS product_name,
    p.brand,
    p.vendor,
    p.price AS unit_price,
    ROUND((p.price * ol.items),2) AS line_subtotal,
    ROUND((p.price * ol.items * (100 - o.discount_percent) / 100),2) AS line_total_discounted    
FROM orders AS o
CROSS JOIN UNNEST (order_lines) AS ol(product_id, items)
LEFT JOIN customers AS c
    ON o.customer_id = c.id
LEFT JOIN products AS p
    ON ol.product_id = p.id;
