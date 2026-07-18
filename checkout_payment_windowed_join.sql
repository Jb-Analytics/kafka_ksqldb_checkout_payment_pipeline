-- ============================================================================
-- REAL-TIME CHECKOUT + PAYMENT CORRELATION WITH ksqlDB
-- ============================================================================
-- Business problem
--   An e-commerce platform receives checkout events and payment events from
--   two independent services. An order may be fulfilled only when its payment
--   is correlated within 10 minutes and passes the business checks below.
--
-- Source topics (normally written by independent producers)
--   ecommerce_checkout_events
--   ecommerce_payment_events
--
-- Main sink topic
--   ecommerce_fulfillment_ready
--
-- This lab demonstrates source streams, event time, validation, automatic
-- repartitioning by ksqlDB, a windowed stream-stream join, CASE transformations,
-- branching, and a windowed materialized aggregation.
--
-- Run the numbered sections in order in the Confluent Cloud ksqlDB editor.
-- Persistent CREATE ... AS SELECT queries keep running in the background.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 0. READ EVENTS THAT MAY HAVE BEEN PRODUCED BEFORE THE QUERIES STARTED
-- ----------------------------------------------------------------------------
SET 'auto.offset.reset' = 'earliest';


-- ----------------------------------------------------------------------------
-- 1. REGISTER THE TWO RAW TOPICS
-- ----------------------------------------------------------------------------
-- These statements create the topics with three partitions when the topics do
-- not already exist. If they exist, ksqlDB registers their schemas.
--
-- TIMESTAMP makes event_ts the record's ROWTIME. The join is therefore based
-- on when the business event happened, not when Kafka happened to receive it.
-- JSON is used so this lab does not require Schema Registry.
CREATE STREAM checkout_events_raw
(
    event_id VARCHAR,
    event_ts BIGINT,
    order_id VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    customer_tier VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    quantity INTEGER,
    unit_price DOUBLE,
    discount_amount DOUBLE,
    currency VARCHAR,
    shipping_city VARCHAR,
    sales_channel VARCHAR
)
WITH
(
    KAFKA_TOPIC = 'ecommerce_checkout_events',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3,
    TIMESTAMP = 'event_ts'
);


CREATE STREAM payment_events_raw
(
    event_id VARCHAR,
    event_ts BIGINT,
    order_id VARCHAR,
    payment_id VARCHAR,
    payment_method VARCHAR,
    payment_status VARCHAR,
    amount DOUBLE,
    currency VARCHAR,
    gateway VARCHAR,
    fraud_score DOUBLE,
    failure_reason VARCHAR
)
WITH
(
    KAFKA_TOPIC = 'ecommerce_payment_events',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3,
    TIMESTAMP = 'event_ts'
);

DESCRIBE checkout_events_raw EXTENDED;
DESCRIBE payment_events_raw EXTENDED;


-- ----------------------------------------------------------------------------
-- 2. VALIDATE AND STANDARDIZE EACH INPUT STREAM
-- ----------------------------------------------------------------------------
-- A persistent query cleans the checkout records and derives the amount that
-- the payment service is expected to authorize.
CREATE STREAM valid_checkout_events
WITH
(
    KAFKA_TOPIC = 'ecommerce_valid_checkout_events',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    event_id,
    event_ts,
    order_id,
    customer_id,
    customer_name,
    customer_email,
    UCASE(customer_tier) AS customer_tier,
    product_id,
    product_name,
    UCASE(category) AS category,
    quantity,
    unit_price,
    discount_amount,
    ROUND((quantity * unit_price) - discount_amount, 2) AS expected_amount,
    UCASE(currency) AS currency,
    shipping_city,
    UCASE(sales_channel) AS sales_channel,
    CASE
        WHEN ((quantity * unit_price) - discount_amount) >= 20000 THEN 'HIGH'
        WHEN ((quantity * unit_price) - discount_amount) >= 5000 THEN 'MEDIUM'
        ELSE 'STANDARD'
    END AS order_value_band
FROM checkout_events_raw
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0
  AND discount_amount >= 0
  AND discount_amount < (quantity * unit_price)
  AND currency IS NOT NULL
EMIT CHANGES;


-- Invalid records are retained in a separate topic instead of disappearing.
-- This is a simple dead-letter/audit path for the classroom discussion.
CREATE STREAM rejected_checkout_events
WITH
(
    KAFKA_TOPIC = 'ecommerce_rejected_checkout_events',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    *,
    CASE
        WHEN order_id IS NULL THEN 'MISSING_ORDER_ID'
        WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER_ID'
        WHEN quantity IS NULL OR quantity <= 0 THEN 'INVALID_QUANTITY'
        WHEN unit_price IS NULL OR unit_price <= 0 THEN 'INVALID_UNIT_PRICE'
        WHEN discount_amount IS NULL OR discount_amount < 0 THEN 'INVALID_DISCOUNT'
        WHEN discount_amount >= (quantity * unit_price) THEN 'DISCOUNT_EXCEEDS_SUBTOTAL'
        WHEN currency IS NULL THEN 'MISSING_CURRENCY'
        ELSE 'UNKNOWN_VALIDATION_ERROR'
    END AS rejection_reason
FROM checkout_events_raw
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR quantity IS NULL OR quantity <= 0
   OR unit_price IS NULL OR unit_price <= 0
   OR discount_amount IS NULL OR discount_amount < 0
   OR discount_amount >= (quantity * unit_price)
   OR currency IS NULL
EMIT CHANGES;


CREATE STREAM valid_payment_events
WITH
(
    KAFKA_TOPIC = 'ecommerce_valid_payment_events',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    event_id,
    event_ts,
    order_id,
    payment_id,
    UCASE(payment_method) AS payment_method,
    UCASE(payment_status) AS payment_status,
    amount,
    UCASE(currency) AS currency,
    UCASE(gateway) AS gateway,
    fraud_score,
    failure_reason
FROM payment_events_raw
WHERE order_id IS NOT NULL
  AND payment_id IS NOT NULL
  AND amount > 0
  AND currency IS NOT NULL
  AND payment_status IS NOT NULL
  AND fraud_score BETWEEN 0.0 AND 1.0
EMIT CHANGES;


-- ----------------------------------------------------------------------------
-- 3. CORRELATE CHECKOUTS AND PAYMENTS IN EVENT TIME
-- ----------------------------------------------------------------------------
-- The Python producer deliberately publishes plain JSON values without Kafka
-- keys. Consequently, order_id starts as a value column and Kafka may place a
-- checkout and its matching payment in unrelated source partitions.
--
-- A stateful join can work only when rows with the same join value reach the
-- same processing task. Because this stream-stream join uses order_id and the
-- input streams are not already keyed by order_id, ksqlDB automatically:
--
--   1. reads order_id from each JSON value;
--   2. creates internal repartition topics for the join when required;
--   3. writes both inputs to those topics using order_id as the internal key;
--   4. brings equal order_id values to the same task and performs the join.
--
-- Therefore, an explicit PARTITION BY query is not necessary in this example.
-- Repartitioning still occurs; it is simply planned and managed internally by
-- ksqlDB instead of being exposed as two additional user-defined streams.
-- Both source topics use three partitions and order_id has the same VARCHAR
-- type on both sides, satisfying the remaining co-partitioning requirements.
-- The internal repartition topics are implementation details: they are not
-- application streams that learners need to create, query, or maintain.
--
-- INNER JOIN emits only orders that have a matching payment event.
-- WITHIN 10 MINUTES accepts either arrival order: a payment may arrive shortly
-- before or after its checkout event. GRACE PERIOD permits limited late and
-- out-of-order processing before the window state is retired.
--
-- Business rules:
--   * payment must be AUTHORIZED
--   * currencies must agree
--   * payment amount may differ by no more than INR 1 (rounding tolerance)
--   * fraud_score must be below 0.80
CREATE STREAM order_payment_decisions
WITH
(
    KAFKA_TOPIC = 'ecommerce_order_payment_decisions',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    o.order_id AS order_id,
    o.event_id AS checkout_event_id,
    p.event_id AS payment_event_id,
    o.customer_id AS customer_id,
    o.customer_name AS customer_name,
    o.customer_email AS customer_email,
    o.customer_tier AS customer_tier,
    o.product_id AS product_id,
    o.product_name AS product_name,
    o.category AS category,
    o.quantity AS quantity,
    o.expected_amount AS expected_amount,
    p.amount AS paid_amount,
    o.currency AS order_currency,
    p.currency AS payment_currency,
    p.payment_id AS payment_id,
    p.payment_method AS payment_method,
    p.payment_status AS payment_status,
    p.gateway AS payment_gateway,
    p.fraud_score AS fraud_score,
    p.failure_reason AS failure_reason,
    o.shipping_city AS shipping_city,
    o.sales_channel AS sales_channel,
    o.order_value_band AS order_value_band,
    o.event_ts AS checkout_ts,
    p.event_ts AS payment_ts,
    (p.event_ts - o.event_ts) AS payment_latency_ms,
    TIMESTAMPTOSTRING(o.event_ts, 'yyyy-MM-dd HH:mm:ss', 'Asia/Kolkata')
        AS checkout_time_ist,
    TIMESTAMPTOSTRING(p.event_ts, 'yyyy-MM-dd HH:mm:ss', 'Asia/Kolkata')
        AS payment_time_ist,
    CASE
        WHEN p.payment_status <> 'AUTHORIZED' THEN 'PAYMENT_NOT_AUTHORIZED'
        WHEN o.currency <> p.currency THEN 'CURRENCY_MISMATCH'
        WHEN ABS(o.expected_amount - p.amount) > 1.0 THEN 'AMOUNT_MISMATCH'
        WHEN p.fraud_score >= 0.80 THEN 'HIGH_FRAUD_RISK'
        ELSE 'READY_FOR_FULFILLMENT'
    END AS decision,
    CASE
        WHEN p.payment_status = 'AUTHORIZED'
         AND o.currency = p.currency
         AND ABS(o.expected_amount - p.amount) <= 1.0
         AND p.fraud_score < 0.80
        THEN TRUE
        ELSE FALSE
    END AS is_fulfillment_ready
FROM valid_checkout_events o
INNER JOIN valid_payment_events p
    WITHIN 10 MINUTES
    GRACE PERIOD 2 MINUTES
    ON o.order_id = p.order_id
EMIT CHANGES;


-- ----------------------------------------------------------------------------
-- 4. BRANCH THE JOIN RESULT INTO ACTIONABLE OUTPUT TOPICS
-- ----------------------------------------------------------------------------
-- This is the main output consumed by the warehouse/fulfillment service.
CREATE STREAM fulfillment_ready_orders
WITH
(
    KAFKA_TOPIC = 'ecommerce_fulfillment_ready',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    order_id,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    product_name,
    quantity,
    paid_amount,
    payment_id,
    payment_method,
    shipping_city,
    order_value_band,
    checkout_time_ist,
    payment_time_ist,
    payment_latency_ms,
    'ALLOCATE_INVENTORY' AS next_action
FROM order_payment_decisions
WHERE is_fulfillment_ready = TRUE
EMIT CHANGES;


-- Failed authorizations, mismatches, and high-risk orders go to manual review.
CREATE STREAM payment_review_orders
WITH
(
    KAFKA_TOPIC = 'ecommerce_payment_review',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    order_id,
    customer_id,
    customer_name,
    customer_email,
    payment_id,
    payment_status,
    expected_amount,
    paid_amount,
    fraud_score,
    failure_reason,
    decision,
    CASE
        WHEN decision = 'HIGH_FRAUD_RISK' THEN 'SEND_TO_FRAUD_TEAM'
        WHEN decision = 'AMOUNT_MISMATCH' THEN 'VERIFY_ORDER_TOTAL'
        WHEN decision = 'CURRENCY_MISMATCH' THEN 'VERIFY_CURRENCY'
        ELSE 'RETRY_OR_CONTACT_CUSTOMER'
    END AS next_action
FROM order_payment_decisions
WHERE is_fulfillment_ready = FALSE
EMIT CHANGES;


-- ----------------------------------------------------------------------------
-- 5. BUILD A LIVE, WINDOWED OPERATIONS DASHBOARD
-- ----------------------------------------------------------------------------
-- This materialized table tracks decisions per city in fixed five-minute
-- event-time buckets. JSON key format supports the composite GROUP BY key.
CREATE TABLE payment_outcomes_5_minute
WITH
(
    KAFKA_TOPIC = 'ecommerce_payment_outcomes_5_minute',
    KEY_FORMAT = 'JSON',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
)
AS
SELECT
    shipping_city,
    decision,
    COUNT(*) AS order_count,
    ROUND(SUM(paid_amount), 2) AS total_payment_amount,
    ROUND(AVG(fraud_score), 3) AS average_fraud_score
FROM order_payment_decisions
WINDOW TUMBLING
(
    SIZE 5 MINUTES,
    GRACE PERIOD 2 MINUTES
)
GROUP BY shipping_city, decision
EMIT CHANGES;

-- Run one at a time. A push query stays open until it is cancelled.
SELECT * FROM order_payment_decisions EMIT CHANGES;

SELECT * FROM fulfillment_ready_orders EMIT CHANGES;

SELECT * FROM payment_review_orders EMIT CHANGES;

SELECT
    WINDOWSTART,
    WINDOWEND,
    shipping_city,
    decision,
    order_count,
    total_payment_amount,
    average_fraud_score
FROM payment_outcomes_5_minute
EMIT CHANGES;

-- A pull query returns the current materialized snapshot and then finishes.
SELECT * FROM payment_outcomes_5_minute;

SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;


-- ----------------------------------------------------------------------------
-- 6. OPTIONAL CLEANUP (RUN ONLY WHEN YOU WANT TO RESET THE LAB)
-- ----------------------------------------------------------------------------
-- Terminate persistent queries before dropping their streams/tables. Query IDs
-- are deployment-specific, so get them from SHOW QUERIES and run, for example:
-- TERMINATE QUERY CSAS_FULFILLMENT_READY_ORDERS_5;
--
-- For a full reset, terminate all queries created by this file, then drop the
-- derived objects in reverse dependency order. Add DELETE TOPIC if you also
-- want ksqlDB to delete each backing Kafka topic.
--
-- DROP TABLE payment_outcomes_5_minute DELETE TOPIC;
-- DROP STREAM payment_review_orders DELETE TOPIC;
-- DROP STREAM fulfillment_ready_orders DELETE TOPIC;
-- DROP STREAM order_payment_decisions DELETE TOPIC;
-- DROP STREAM valid_payment_events DELETE TOPIC;
-- DROP STREAM rejected_checkout_events DELETE TOPIC;
-- DROP STREAM valid_checkout_events DELETE TOPIC;
-- DROP STREAM payment_events_raw;
-- DROP STREAM checkout_events_raw;
