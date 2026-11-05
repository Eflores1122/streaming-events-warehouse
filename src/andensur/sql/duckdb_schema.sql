-- Local warehouse for Andén Sur. Applied by warehouse.connect.
-- Timestamps are naive UTC. Mexico City civil time is event_time minus 6 hours
-- (the country is on UTC-6 without daylight saving for this calendar).

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS warehouse.events (
  event_id VARCHAR PRIMARY KEY,
  event_type VARCHAR NOT NULL,
  event_time TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  late BOOLEAN NOT NULL,
  broker VARCHAR NOT NULL,
  stream_offset VARCHAR,
  order_id VARCHAR,
  amount_centavos BIGINT,
  city VARCHAR,
  state VARCHAR,
  currency VARCHAR,
  customer_ref VARCHAR,
  channel VARCHAR,
  fulfillment VARCHAR,
  sku VARCHAR,
  bin_code VARCHAR,
  qty INTEGER,
  shipment_id VARCHAR,
  scan_type VARCHAR,
  hub_code VARCHAR,
  sku_count INTEGER,
  payload VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.rejects (
  reject_id VARCHAR PRIMARY KEY,
  stream_offset VARCHAR,
  reason VARCHAR NOT NULL,
  payload VARCHAR,
  rejected_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.ingest_batches (
  batch_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP NOT NULL,
  broker VARCHAR NOT NULL,
  received INTEGER NOT NULL,
  inserted INTEGER NOT NULL,
  duplicates INTEGER NOT NULL,
  late_rows INTEGER NOT NULL,
  rejected INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.stream_state (
  key VARCHAR PRIMARY KEY,
  watermark TIMESTAMP,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS mart.hourly_corridor (
  hour_start TIMESTAMP NOT NULL,
  event_type VARCHAR NOT NULL,
  city VARCHAR NOT NULL,
  event_count BIGINT NOT NULL,
  late_count BIGINT NOT NULL,
  amount_centavos BIGINT NOT NULL,
  PRIMARY KEY (hour_start, event_type, city)
);

CREATE OR REPLACE VIEW warehouse.orders AS
SELECT
  event_id,
  event_time,
  event_time - INTERVAL 6 HOUR AS event_time_mx,
  CAST(event_time - INTERVAL 6 HOUR AS DATE) AS business_date,
  ingested_at,
  late,
  order_id,
  city,
  state,
  amount_centavos,
  customer_ref,
  channel,
  fulfillment,
  currency,
  sku_count
FROM warehouse.events
WHERE event_type = 'order.created';

CREATE OR REPLACE VIEW warehouse.inventory_scans AS
SELECT
  event_id,
  event_time,
  event_time - INTERVAL 6 HOUR AS event_time_mx,
  CAST(event_time - INTERVAL 6 HOUR AS DATE) AS business_date,
  ingested_at,
  late,
  order_id,
  sku,
  bin_code,
  qty,
  scan_type,
  hub_code,
  city,
  state
FROM warehouse.events
WHERE event_type = 'inventory.scanned';

CREATE OR REPLACE VIEW warehouse.shipment_scans AS
SELECT
  event_id,
  event_time,
  event_time - INTERVAL 6 HOUR AS event_time_mx,
  CAST(event_time - INTERVAL 6 HOUR AS DATE) AS business_date,
  ingested_at,
  late,
  order_id,
  shipment_id,
  scan_type,
  hub_code,
  city,
  state
FROM warehouse.events
WHERE event_type = 'shipment.scanned';
