CREATE TABLE orders_captured (
    order_id   STRING,
    amount     BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders.captured',
    'properties.group.id' = 'capture-settle',
    'properties.isolation.level' = 'read_committed',
    'upstream.transactional' = 'true',
    'scan.watermark.max-lateness' = '5 s',
    'format' = 'json'
);

CREATE TABLE ledger_out (
    order_id STRING,
    amount   BIGINT,
    status   STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ledger.entries',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.dedup-key' = 'order_id',
    'format' = 'json'
);

INSERT INTO ledger_out
SELECT order_id, amount, 'captured'
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time) AS rn
    FROM orders_captured
) WHERE rn = 1;
