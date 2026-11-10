export const PRESETS = [
  {
    id: "byType",
    sql: `SELECT event_type, count(*) AS rows
FROM warehouse.events
GROUP BY 1
ORDER BY 1`,
  },
  {
    id: "mart",
    sql: `SELECT hour_start, event_type, city, event_count, late_count, amount_centavos
FROM mart.hourly_corridor
ORDER BY hour_start, event_type, city`,
  },
  {
    id: "orders",
    sql: `SELECT business_date, channel, fulfillment,
       count(*) AS orders,
       sum(amount_centavos) AS amount_centavos
FROM warehouse.orders
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3`,
  },
  {
    id: "late",
    sql: `SELECT event_id, event_time, event_time_mx, business_date, city, scan_type, hub_code
FROM warehouse.shipment_scans
WHERE late
ORDER BY event_id`,
  },
  {
    id: "batches",
    sql: `SELECT started_at, received, inserted, duplicates, late_rows, rejected
FROM warehouse.ingest_batches
ORDER BY started_at`,
  },
  {
    id: "lineage",
    sql: `SELECT stream_offset, event_id, event_type, scan_type, event_time, late
FROM warehouse.events
WHERE order_id = 'ORD-20261102-00001'
ORDER BY try_cast(stream_offset AS INTEGER)`,
  },
  {
    id: "rejects",
    sql: `SELECT reject_id, stream_offset, reason
FROM warehouse.rejects
ORDER BY rejected_at`,
  },
];
