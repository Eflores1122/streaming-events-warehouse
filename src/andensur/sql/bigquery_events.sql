-- Andén Sur BigQuery target shape.
--
-- This file is documentation. The local demo and CI run the DuckDB merge in
-- warehouse.py. They do not create a BigQuery client, they do not execute
-- these statements, and they do not need credentials.
--
-- Transport target: a Pub/Sub subscription or a Kafka consumer group feeds a
-- writer. Locally that role is Redis Streams (docker compose) or the
-- pure-Python consumer-group stand-in (default smoke test).
--
-- Write target: Storage Write API, committed mode, appending rows into
-- `andensur.events_staging`. Committed mode is at-least-once. A default
-- stream can pin offsets for exactly-once delivery inside that stream; this
-- demo does not emulate offset tokens. Legacy tabledata.insertAll insertId
-- deduplicates for a short best-effort window only. Do not treat insertId as
-- the warehouse guarantee. The guarantee is the MERGE on event_id below,
-- which is the same rule as the DuckDB primary key.
--
-- Replace PROJECT before anyone runs this by hand. This repository never does.

CREATE SCHEMA IF NOT EXISTS `PROJECT.andensur`
OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `PROJECT.andensur.events` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_time TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  late BOOL NOT NULL,
  broker STRING NOT NULL,
  stream_offset STRING,
  order_id STRING,
  amount_centavos INT64,
  city STRING,
  state STRING,
  currency STRING,
  customer_ref STRING,
  channel STRING,
  fulfillment STRING,
  sku STRING,
  bin_code STRING,
  qty INT64,
  shipment_id STRING,
  scan_type STRING,
  hub_code STRING,
  sku_count INT64,
  payload JSON
)
PARTITION BY DATE(event_time)
CLUSTER BY event_type
OPTIONS (
  description = 'Synthetic Andén Sur events. Idempotent on event_id. Target shape only.'
);

CREATE TABLE IF NOT EXISTS `PROJECT.andensur.events_staging` (
  event_id STRING NOT NULL,
  event_type STRING NOT NULL,
  event_time TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  late BOOL NOT NULL,
  broker STRING NOT NULL,
  stream_offset STRING,
  order_id STRING,
  amount_centavos INT64,
  city STRING,
  state STRING,
  currency STRING,
  customer_ref STRING,
  channel STRING,
  fulfillment STRING,
  sku STRING,
  bin_code STRING,
  qty INT64,
  shipment_id STRING,
  scan_type STRING,
  hub_code STRING,
  sku_count INT64,
  payload JSON
);

-- Insert event_ids the table does not already have. A retried micro-batch
-- does not create a second row. The source is deduped first so one staging
-- load cannot contain two copies of event_id.
MERGE `PROJECT.andensur.events` AS t
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, stream_offset) AS rn
    FROM `PROJECT.andensur.events_staging`
  )
  WHERE rn = 1
) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN
  INSERT (
    event_id,
    event_type,
    event_time,
    ingested_at,
    late,
    broker,
    stream_offset,
    order_id,
    amount_centavos,
    city,
    state,
    currency,
    customer_ref,
    channel,
    fulfillment,
    sku,
    bin_code,
    qty,
    shipment_id,
    scan_type,
    hub_code,
    sku_count,
    payload
  )
  VALUES (
    s.event_id,
    s.event_type,
    s.event_time,
    s.ingested_at,
    s.late,
    s.broker,
    s.stream_offset,
    s.order_id,
    s.amount_centavos,
    s.city,
    s.state,
    s.currency,
    s.customer_ref,
    s.channel,
    s.fulfillment,
    s.sku,
    s.bin_code,
    s.qty,
    s.shipment_id,
    s.scan_type,
    s.hub_code,
    s.sku_count,
    s.payload
  );

-- Derived mart. Rebuild touched hours from warehouse.events (the local name)
-- or from `events` here. Partition pruning wants a predicate on event_time
-- when the compaction window is known.
CREATE TABLE IF NOT EXISTS `PROJECT.andensur.hourly_corridor` (
  hour_start TIMESTAMP NOT NULL,
  event_type STRING NOT NULL,
  city STRING NOT NULL,
  event_count INT64 NOT NULL,
  late_count INT64 NOT NULL,
  amount_centavos INT64 NOT NULL
)
PARTITION BY DATE(hour_start)
CLUSTER BY event_type;
