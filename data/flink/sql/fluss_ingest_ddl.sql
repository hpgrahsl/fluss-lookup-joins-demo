USE CATALOG default_catalog;

CREATE TABLE customers (
    id STRING,
    name STRING,
    direct_subscription BOOLEAN,
    membership_level STRING,
    shipping_address STRING,
    activation_date STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecom_customers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-customers-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE TABLE products (
    id STRING,
    name STRING,
    brand STRING,
    vendor STRING,
    price DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecom_products',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-products-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE TABLE orders (
    id STRING,
    customer_id STRING,
    notes STRING,
    create_ts BIGINT,
    creditcard STRING,
    discount_percent INT,
    order_lines MAP<STRING,INT>
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecom_orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;

CREATE TABLE fluss_customers (
    id STRING,
    name STRING,
    direct_subscription BOOLEAN,
    membership_level STRING,
    shipping_address STRING,
    activation_date STRING,
    PRIMARY KEY (id) NOT ENFORCED
);

CREATE TABLE fluss_products (
    id STRING,
    name STRING,
    brand STRING,
    vendor STRING,
    price DOUBLE,
    PRIMARY KEY (id) NOT ENFORCED
);

CREATE TABLE fluss_order_lines (
    id STRING,
    customer_id STRING,
    notes STRING,
    create_ts BIGINT,
    creditcard STRING,
    discount_percent INT,
    product_id STRING,
    items INT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id,product_id) NOT ENFORCED
) WITH (
    'bucket.key' = 'id'
);

CREATE TABLE fluss_order_lines_enriched (
    order_id STRING,
    customer_id STRING,
    customer_name STRING,
    membership_level STRING,
    shipping_address STRING,
    notes STRING,
    create_ts BIGINT,
    creditcard STRING,
    discount_percent INT,
    product_id STRING,
    quantity INT,
    product_name STRING,
    brand STRING,
    vendor STRING,
    unit_price DOUBLE,
    line_subtotal DOUBLE,
    line_total_discounted DOUBLE,
    PRIMARY KEY (order_id,product_id) NOT ENFORCED
) WITH (
    'bucket.key' = 'order_id'
);
