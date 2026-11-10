// Port of the checks in src/andensur/models.py. A rejected delivery is acked.

import { canonicalJson } from "./json.js";
import { SOURCE } from "./generate.js";

export const EVENT_TYPES = ["order.created", "inventory.scanned", "shipment.scanned"];
export const SCAN_TYPES = ["picked_up", "hub_in", "out_for_delivery", "delivered"];
export const INVENTORY_SCAN_TYPES = ["pick", "return_to_stock"];
export const CHANNELS = ["online", "store"];
export const FULFILLMENTS = ["released", "cancelled"];

export class EventRejected extends Error {
  constructor(reason) {
    super(reason);
    this.reason = reason;
  }
}

export function parseEventTime(value) {
  if (typeof value !== "string" || !value) {
    throw new EventRejected("event_time must be a UTC timestamp");
  }
  const text = value.endsWith("Z") ? `${value.slice(0, -1)}+00:00` : value;
  const parsed = Date.parse(text);
  if (Number.isNaN(parsed)) throw new EventRejected("event_time must be a UTC timestamp");
  return parsed;
}

export function validateEvent(event) {
  if (!event || typeof event !== "object" || Array.isArray(event)) {
    throw new EventRejected("event is not an object");
  }
  for (const key of ["event_id", "event_type", "event_time", "source", "payload"]) {
    if (event[key] === null || event[key] === undefined || event[key] === "") {
      throw new EventRejected(`missing ${key}`);
    }
  }
  if (event.source !== SOURCE) throw new EventRejected(`source must be ${SOURCE}`);
  if (!EVENT_TYPES.includes(event.event_type)) {
    throw new EventRejected(`unknown event_type ${event.event_type}`);
  }
  if (typeof event.event_id !== "string") throw new EventRejected("event_id must be a string");
  parseEventTime(event.event_time);
  const payload = event.payload;
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new EventRejected("payload is not an object");
  }
  if (!payload.order_id) throw new EventRejected("order_id is required");
  const eventType = event.event_type;
  if (eventType === "order.created") requireAmount(payload);
  else if ("amount_centavos" in payload) requireAmount(payload);
  if (eventType === "order.created") {
    requireCustomer(payload);
    if (!CHANNELS.includes(payload.channel)) {
      throw new EventRejected("channel must be online or store");
    }
    if (!FULFILLMENTS.includes(payload.fulfillment)) {
      throw new EventRejected("fulfillment must be released or cancelled");
    }
    if (typeof payload.sku_count !== "number" || !Number.isInteger(payload.sku_count)) {
      throw new EventRejected("sku_count must be an integer");
    }
    if (payload.sku_count < 1) throw new EventRejected("sku_count must be positive");
  }
  if (eventType === "inventory.scanned") {
    if (!INVENTORY_SCAN_TYPES.includes(payload.scan_type)) {
      throw new EventRejected("inventory scan_type must be pick or return_to_stock");
    }
    if (!payload.sku || !payload.bin_code || !payload.hub_code) {
      throw new EventRejected("sku, bin_code, and hub_code are required");
    }
    const qty = payload.qty;
    if (typeof qty !== "number" || !Number.isInteger(qty) || qty < 1) {
      throw new EventRejected("qty must be a positive integer");
    }
    if (!payload.city) throw new EventRejected("city is required");
  }
  if (eventType === "shipment.scanned") {
    if (!SCAN_TYPES.includes(payload.scan_type)) {
      throw new EventRejected("scan_type is not recognized");
    }
    if (!payload.shipment_id || !payload.hub_code) {
      throw new EventRejected("shipment_id and hub_code are required");
    }
    if (!payload.city) throw new EventRejected("city is required");
  }
}

export function projectEvent(event) {
  const payload = event.payload;
  return {
    event_id: event.event_id,
    event_type: event.event_type,
    event_time_ms: parseEventTime(event.event_time),
    order_id: payload.order_id ?? null,
    amount_centavos: payload.amount_centavos ?? null,
    city: payload.city ?? null,
    state: payload.state ?? null,
    currency: payload.currency ?? null,
    customer_ref: payload.customer_ref ?? null,
    channel: payload.channel ?? null,
    fulfillment: payload.fulfillment ?? null,
    sku: payload.sku ?? null,
    bin_code: payload.bin_code ?? null,
    qty: payload.qty ?? null,
    shipment_id: payload.shipment_id ?? null,
    scan_type: payload.scan_type ?? null,
    hub_code: payload.hub_code ?? null,
    sku_count: payload.sku_count ?? null,
    payload: canonicalJson(payload),
  };
}

export function invalidEnvelope() {
  return {
    event_id: "evt-bad-manual",
    event_type: "order.created",
    event_time: "2026-11-02T14:05:00Z",
    source: SOURCE,
    payload: {
      order_id: "ORD-BAD",
      customer_ref: "nope",
      channel: "online",
      fulfillment: "released",
      city: "Puebla",
      state: "Puebla",
      amount_centavos: 15000,
      currency: "MXN",
      sku_count: 1,
    },
  };
}

function requireAmount(payload) {
  const amount = payload.amount_centavos;
  if (typeof amount !== "number" || !Number.isInteger(amount) || amount < 0) {
    throw new EventRejected("amount_centavos must be a non-negative integer");
  }
}

function requireCustomer(payload) {
  const ref = payload.customer_ref;
  if (typeof ref !== "string" || ref.length !== 15 || !ref.startsWith("Sintetico")) {
    throw new EventRejected("customer_ref must look like Sintetico######");
  }
  if (!/^\d{6}$/.test(ref.slice(9))) {
    throw new EventRejected("customer_ref must look like Sintetico######");
  }
}
