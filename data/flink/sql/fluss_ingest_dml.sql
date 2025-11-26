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

USE CATALOG fluss_catalog;

-- job1: stream kafka source tables into fluss tables
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO fluss_customers SELECT * FROM `default_catalog`.`default_database`.customers;
    INSERT INTO fluss_products SELECT * FROM `default_catalog`.`default_database`.products;
    INSERT INTO fluss_order_lines
        SELECT id,customer_id,notes,create_ts,creditcard,discount_percent,product_id,items 
            FROM `default_catalog`.`default_database`.orders 
                CROSS JOIN UNNEST (order_lines) AS ol(product_id, items);
END;

-- job2: order lines enrichment using lookup joins with fluss tables
INSERT INTO fluss_order_lines_enriched
SELECT 
    ol.id AS order_id,
    c.id AS customer_id,
    c.name AS customer_name,
    c.membership_level,
    c.shipping_address,
    ol.notes,
    ol.create_ts AS order_datetime,
    ol.creditcard AS creditcard,
    ol.discount_percent,
    p.id AS product_id,
    ol.items AS quantity,
    p.name AS product_name,
    p.brand,
    p.vendor,
    p.price AS unit_price,
    ROUND((p.price * ol.items),2) AS line_subtotal,
    ROUND((p.price * ol.items * (100 - ol.discount_percent) / 100),2) AS line_total_discounted    
FROM fluss_order_lines ol
LEFT JOIN fluss_customers FOR SYSTEM_TIME AS OF ol.proc_time AS c
    ON ol.customer_id = c.id
LEFT JOIN fluss_products FOR SYSTEM_TIME AS OF ol.proc_time AS p
    ON ol.product_id = p.id;
