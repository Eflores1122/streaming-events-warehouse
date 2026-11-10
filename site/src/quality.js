// Structural checks copied from src/andensur/quality.py.
// Freshness uses the browser clock and the 15 minute SLA (FRESHNESS_SLA_SECONDS).

export const FRESHNESS_SLA_SECONDS = 15 * 60;

const CHECKS = [
  ["events_present", "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM warehouse.events"],
  ["event_id_unique", "SELECT count(*) - count(DISTINCT event_id) FROM warehouse.events"],
  [
    "required_not_null",
    `SELECT count(*) FROM warehouse.events
     WHERE event_id IS NULL OR event_type IS NULL OR event_time IS NULL
        OR ingested_at IS NULL OR order_id IS NULL`,
  ],
  [
    "event_type_accepted",
    `SELECT count(*) FROM warehouse.events
     WHERE event_type NOT IN ('order.created', 'inventory.scanned', 'shipment.scanned')`,
  ],
  [
    "amount_non_negative",
    `SELECT count(*) FROM warehouse.events
     WHERE amount_centavos IS NOT NULL AND amount_centavos < 0`,
  ],
  [
    "currency_mxn",
    `SELECT count(*) FROM warehouse.events
     WHERE event_type = 'order.created' AND coalesce(currency, '') <> 'MXN'`,
  ],
  [
    "order_channel",
    `SELECT count(*) FROM warehouse.events
     WHERE event_type = 'order.created'
       AND (
         channel NOT IN ('online', 'store')
         OR fulfillment NOT IN ('released', 'cancelled')
         OR customer_ref IS NULL
         OR sku_count IS NULL
         OR sku_count < 1
       )`,
  ],
  [
    "order_id_unique",
    `SELECT count(*) FROM (
       SELECT order_id FROM warehouse.events
       WHERE event_type = 'order.created'
       GROUP BY order_id
       HAVING count(*) > 1
     )`,
  ],
  [
    "inventory_shape",
    `SELECT count(*) FROM warehouse.events
     WHERE event_type = 'inventory.scanned'
       AND (
         scan_type NOT IN ('pick', 'return_to_stock')
         OR sku IS NULL OR bin_code IS NULL OR hub_code IS NULL OR city IS NULL
         OR qty IS NULL OR qty < 1
       )`,
  ],
  [
    "inventory_has_order",
    `SELECT count(*)
     FROM warehouse.events AS inventory
     LEFT JOIN warehouse.events AS orders
       ON orders.event_type = 'order.created' AND orders.order_id = inventory.order_id
     WHERE inventory.event_type = 'inventory.scanned' AND orders.event_id IS NULL`,
  ],
  [
    "order_has_pick",
    `SELECT count(*)
     FROM warehouse.events AS orders
     LEFT JOIN warehouse.events AS inventory
       ON inventory.event_type = 'inventory.scanned'
      AND inventory.scan_type = 'pick'
      AND inventory.order_id = orders.order_id
     WHERE orders.event_type = 'order.created' AND inventory.event_id IS NULL`,
  ],
  [
    "scan_shape",
    `SELECT count(*) FROM warehouse.events
     WHERE event_type = 'shipment.scanned'
       AND (
         scan_type NOT IN ('picked_up', 'hub_in', 'out_for_delivery', 'delivered')
         OR shipment_id IS NULL OR hub_code IS NULL OR city IS NULL
       )`,
  ],
  [
    "scan_has_order",
    `SELECT count(*)
     FROM warehouse.events AS scan
     LEFT JOIN warehouse.events AS orders
       ON orders.event_type = 'order.created' AND orders.order_id = scan.order_id
     WHERE scan.event_type = 'shipment.scanned' AND orders.event_id IS NULL`,
  ],
  [
    "released_has_shipment",
    `SELECT count(*)
     FROM warehouse.events AS orders
     LEFT JOIN warehouse.events AS scan
       ON scan.event_type = 'shipment.scanned' AND scan.order_id = orders.order_id
     WHERE orders.event_type = 'order.created'
       AND orders.fulfillment = 'released'
       AND scan.event_id IS NULL`,
  ],
  [
    "cancelled_has_return",
    `SELECT count(*)
     FROM warehouse.events AS orders
     LEFT JOIN warehouse.events AS inventory
       ON inventory.event_type = 'inventory.scanned'
      AND inventory.scan_type = 'return_to_stock'
      AND inventory.order_id = orders.order_id
     WHERE orders.event_type = 'order.created'
       AND orders.fulfillment = 'cancelled'
       AND inventory.event_id IS NULL`,
  ],
  [
    "cancelled_has_no_shipment",
    `SELECT count(*)
     FROM warehouse.events AS orders
     JOIN warehouse.events AS scan
       ON scan.event_type = 'shipment.scanned' AND scan.order_id = orders.order_id
     WHERE orders.event_type = 'order.created' AND orders.fulfillment = 'cancelled'`,
  ],
  [
    "mart_matches_events",
    `SELECT CASE
       WHEN (SELECT count(*) FROM warehouse.events)
            = coalesce((SELECT sum(event_count) FROM mart.hourly_corridor), 0)
        AND (SELECT count(*) FROM warehouse.events WHERE late)
            = coalesce((SELECT sum(late_count) FROM mart.hourly_corridor), 0)
       THEN 0 ELSE 1
     END`,
  ],
];

export async function runQuality(scalar, asOfMs = Date.now()) {
  const results = [];
  for (const [name, sql] of CHECKS) {
    const violations = Number((await scalar(sql)) ?? 0);
    results.push({
      name,
      passed: violations === 0,
      detail: `violations=${violations}`,
      violations,
    });
  }
  results.push(await freshness(scalar, asOfMs));
  return results;
}

async function freshness(scalar, asOfMs) {
  const text = await scalar(
    "SELECT strftime(max(ingested_at), '%Y-%m-%dT%H:%M:%S') FROM warehouse.events",
  );
  if (!text) {
    return {
      name: "ingest_freshness",
      passed: false,
      detail: "no ingested_at",
      violations: 1,
    };
  }
  const latest = Date.parse(`${text}Z`);
  const age = (asOfMs - latest) / 1000;
  const passed = age <= FRESHNESS_SLA_SECONDS;
  return {
    name: "ingest_freshness",
    passed,
    detail: `age_seconds=${age.toFixed(3)} sla_seconds=${FRESHNESS_SLA_SECONDS}`,
    violations: passed ? 0 : 1,
  };
}
