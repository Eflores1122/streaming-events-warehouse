// Port of src/andensur/generate.py.
// CI diffs JSONL from this file against `python -m andensur generate`.

import { canonicalJson } from "./json.js";

export const SOURCE = "andensur-synthetic";
export const REPLAY_COPIES = 4;
export const LATE_SCANS = 3;
const LATE_SHIFT_MS = 12 * 60 * 60 * 1000;
const SPREAD_MS = 2 * 60 * 60 * 1000;
const SPAN_SECONDS = 2 * 60 * 60;

// Naive UTC instant of the generator clock: 2026-11-02 16:00:00.
export const STREAM_EPOCH_MS = Date.UTC(2026, 10, 2, 16, 0, 0);

export const CITIES = [
  ["Ciudad de Mexico", "CDMX", "HUB-MEX"],
  ["Guadalajara", "Jalisco", "HUB-GDL"],
  ["Monterrey", "Nuevo Leon", "HUB-MTY"],
  ["Puebla", "Puebla", "HUB-PBC"],
  ["Merida", "Yucatan", "HUB-MID"],
  ["Queretaro", "Queretaro", "HUB-QRO"],
];

const CROSS_DOCK = CITIES[0];

function pymod(value, divisor) {
  return ((value % divisor) + divisor) % divisor;
}

export function formatTime(ms) {
  const date = new Date(ms);
  const pad = (value) => String(value).padStart(2, "0");
  return (
    `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}` +
    `T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}Z`
  );
}

export function formatStamp(ms) {
  return formatTime(ms).replace("T", " ").replace("Z", "");
}

export function cancelledOrderCount(orders) {
  if (orders <= 0) throw new Error("orders must be positive");
  return Math.floor((orders - 1) / 10) + 1;
}

export function expectedCounts(orders) {
  const cancelled = cancelledOrderCount(orders);
  const released = orders - cancelled;
  const inventory = orders + cancelled;
  const scans = 4 * released;
  const unique = orders + inventory + scans;
  const replays = Math.min(REPLAY_COPIES, unique);
  return {
    orders,
    cancelled_orders: cancelled,
    released_orders: released,
    inventory_scans: inventory,
    scans,
    unique_events: unique,
    replays,
    published: unique + replays,
    late_events: Math.min(LATE_SCANS, released),
  };
}

export function amountCentavos(seed, index) {
  return 15_000 + pymod(seed + index * 97, 4_000) * 100;
}

export function customerRef(seed, index) {
  return `Sintetico${String(pymod(seed * 1000 + index, 1_000_000)).padStart(6, "0")}`;
}

function makeEvent(eventId, eventType, eventTimeMs, payload) {
  return {
    event_id: eventId,
    event_type: eventType,
    event_time: formatTime(eventTimeMs),
    source: SOURCE,
    payload,
  };
}

function inventory({ index, orderId, whenMs, scanType, place, skuCount }) {
  const [city, state, hub] = place;
  const suffix = scanType === "pick" ? "pick" : "return";
  return makeEvent(`evt-inv-${pad5(index)}-${suffix}`, "inventory.scanned", whenMs, {
    order_id: orderId,
    sku: `SKU-${String(index % 50).padStart(4, "0")}`,
    bin_code: `BIN-${hub}-${String(index % 20).padStart(2, "0")}`,
    qty: skuCount,
    scan_type: scanType,
    hub_code: hub,
    city,
    state,
  });
}

function scans(index, openedMs, orderId, shipmentId, origin, dest, lateDelivery) {
  const hubIn = origin[2] === CROSS_DOCK[2] ? origin : CROSS_DOCK;
  const plan = [
    ["picked_up", origin, openedMs + 10 * 60 * 1000],
    ["hub_in", hubIn, openedMs + 40 * 60 * 1000],
    ["out_for_delivery", dest, openedMs + 70 * 60 * 1000],
    [
      "delivered",
      dest,
      lateDelivery ? STREAM_EPOCH_MS - LATE_SHIFT_MS : openedMs + 100 * 60 * 1000,
    ],
  ];
  return plan.map(([scanType, place, whenMs], step) => {
    const [city, state, hub] = place;
    return makeEvent(`evt-scn-${pad5(index)}-${step}`, "shipment.scanned", whenMs, {
      shipment_id: shipmentId,
      order_id: orderId,
      scan_type: scanType,
      hub_code: hub,
      city,
      state,
    });
  });
}

function pad5(index) {
  return String(index).padStart(5, "0");
}

export function buildEvents({ orders, seed }) {
  if (!Number.isInteger(orders) || orders <= 0) throw new Error("orders must be positive");
  if (!Number.isInteger(seed)) throw new Error("seed must be an integer");
  const events = [];
  let lateLeft = LATE_SCANS;
  for (let index = 0; index < orders; index += 1) {
    const origin = CITIES[index % CITIES.length];
    const dest = CITIES[(index + 2) % CITIES.length];
    const opened =
      STREAM_EPOCH_MS - SPREAD_MS + Math.trunc((index * SPAN_SECONDS) / orders) * 1000;
    const amount = amountCentavos(seed, index);
    const cancelled = index % 10 === 0;
    const orderId = `ORD-20261102-${pad5(index)}`;
    const shipmentId = `SHP-20261102-${pad5(index)}`;
    const [city, state] = origin;
    const skuCount = 1 + (index % 4);
    events.push(
      makeEvent(`evt-ord-${pad5(index)}`, "order.created", opened, {
        order_id: orderId,
        customer_ref: customerRef(seed, index),
        channel: index % 10 < 7 ? "online" : "store",
        fulfillment: cancelled ? "cancelled" : "released",
        city,
        state,
        amount_centavos: amount,
        currency: "MXN",
        sku_count: skuCount,
      }),
    );
    events.push(
      inventory({
        index,
        orderId,
        whenMs: opened + 2 * 60 * 1000,
        scanType: "pick",
        place: origin,
        skuCount,
      }),
    );
    if (cancelled) {
      events.push(
        inventory({
          index,
          orderId,
          whenMs: opened + 4 * 60 * 1000,
          scanType: "return_to_stock",
          place: origin,
          skuCount,
        }),
      );
      continue;
    }
    const markLate = lateLeft > 0;
    if (markLate) lateLeft -= 1;
    events.push(...scans(index, opened, orderId, shipmentId, origin, dest, markLate));
  }
  const replayN = Math.min(REPLAY_COPIES, events.length);
  for (let index = 0; index < replayN; index += 1) {
    events.push(JSON.parse(JSON.stringify(events[index])));
  }
  return events;
}

export function eventsToJsonl(events) {
  return events.map((event) => canonicalJson(event)).join("\n") + "\n";
}
