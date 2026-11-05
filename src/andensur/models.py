"""Event envelope shared by the generator, the brokers, and the warehouse."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

SOURCE = "andensur-synthetic"

EVENT_TYPES = (
    "order.created",
    "inventory.scanned",
    "shipment.scanned",
)

SCAN_TYPES = ("picked_up", "hub_in", "out_for_delivery", "delivered")
INVENTORY_SCAN_TYPES = ("pick", "return_to_stock")
CHANNELS = ("online", "store")
FULFILLMENTS = ("released", "cancelled")

# Flat columns stored on warehouse.events. Payload JSON is kept beside them.
EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "ingested_at",
    "late",
    "broker",
    "stream_offset",
    "order_id",
    "amount_centavos",
    "city",
    "state",
    "currency",
    "customer_ref",
    "channel",
    "fulfillment",
    "sku",
    "bin_code",
    "qty",
    "shipment_id",
    "scan_type",
    "hub_code",
    "sku_count",
    "payload",
)


class EventRejected(ValueError):
    """A delivery that cannot be merged. It is recorded and acked."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def format_time(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def parse_event_time(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise EventRejected("event_time must be a UTC timestamp")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EventRejected("event_time must be a UTC timestamp") from exc
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


def make_event(
    *,
    event_id: str,
    event_type: str,
    event_time: datetime | str,
    payload: dict,
) -> dict:
    when = event_time if isinstance(event_time, str) else format_time(event_time)
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": when,
        "source": SOURCE,
        "payload": payload,
    }


def validate_event(event: object) -> None:
    if not isinstance(event, dict):
        raise EventRejected("event is not an object")
    for key in ("event_id", "event_type", "event_time", "source", "payload"):
        if event.get(key) in (None, ""):
            raise EventRejected(f"missing {key}")
    if event["source"] != SOURCE:
        raise EventRejected(f"source must be {SOURCE}")
    if event["event_type"] not in EVENT_TYPES:
        raise EventRejected(f"unknown event_type {event['event_type']}")
    if not isinstance(event["event_id"], str):
        raise EventRejected("event_id must be a string")
    parse_event_time(event["event_time"])
    payload = event["payload"]
    if not isinstance(payload, dict):
        raise EventRejected("payload is not an object")
    if not payload.get("order_id"):
        raise EventRejected("order_id is required")
    event_type = event["event_type"]
    if event_type == "order.created":
        _require_amount(payload)
        if payload.get("currency") != "MXN":
            raise EventRejected("currency must be MXN")
    elif "amount_centavos" in payload:
        _require_amount(payload)
    if event_type == "order.created":
        _require_customer(payload)
        if payload.get("channel") not in CHANNELS:
            raise EventRejected("channel must be online or store")
        if payload.get("fulfillment") not in FULFILLMENTS:
            raise EventRejected("fulfillment must be released or cancelled")
        if not isinstance(payload.get("sku_count"), int) or isinstance(
            payload.get("sku_count"), bool
        ):
            raise EventRejected("sku_count must be an integer")
        if payload["sku_count"] < 1:
            raise EventRejected("sku_count must be positive")
    if event_type == "inventory.scanned":
        if payload.get("scan_type") not in INVENTORY_SCAN_TYPES:
            raise EventRejected("inventory scan_type must be pick or return_to_stock")
        if not payload.get("sku") or not payload.get("bin_code") or not payload.get("hub_code"):
            raise EventRejected("sku, bin_code, and hub_code are required")
        qty = payload.get("qty")
        if isinstance(qty, bool) or not isinstance(qty, int) or qty < 1:
            raise EventRejected("qty must be a positive integer")
        if not payload.get("city"):
            raise EventRejected("city is required")
    if event_type == "shipment.scanned":
        if payload.get("scan_type") not in SCAN_TYPES:
            raise EventRejected("scan_type is not recognized")
        if not payload.get("shipment_id") or not payload.get("hub_code"):
            raise EventRejected("shipment_id and hub_code are required")
        if not payload.get("city"):
            raise EventRejected("city is required")


def project_event(event: dict) -> dict:
    """Flatten one validated event. `late`, `ingested_at`, and broker fields are added later."""
    payload = event["payload"]
    amount = payload.get("amount_centavos")
    sku = payload.get("sku_count")
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_time": parse_event_time(event["event_time"]),
        "order_id": payload.get("order_id"),
        "amount_centavos": amount,
        "city": payload.get("city"),
        "state": payload.get("state"),
        "currency": payload.get("currency"),
        "customer_ref": payload.get("customer_ref"),
        "channel": payload.get("channel"),
        "fulfillment": payload.get("fulfillment"),
        "sku": payload.get("sku"),
        "bin_code": payload.get("bin_code"),
        "qty": payload.get("qty"),
        "shipment_id": payload.get("shipment_id"),
        "scan_type": payload.get("scan_type"),
        "hub_code": payload.get("hub_code"),
        "sku_count": sku,
        "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def _require_amount(payload: dict) -> None:
    amount = payload.get("amount_centavos")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        raise EventRejected("amount_centavos must be a non-negative integer")


def _require_customer(payload: dict) -> None:
    ref = payload.get("customer_ref")
    if not isinstance(ref, str) or len(ref) != 15 or not ref.startswith("Sintetico"):
        raise EventRejected("customer_ref must look like Sintetico######")
    if not ref[9:].isdigit():
        raise EventRejected("customer_ref must look like Sintetico######")


@dataclass(frozen=True)
class Delivery:
    offset: str
    event: dict


@dataclass(frozen=True)
class BatchStats:
    received: int
    inserted: int
    duplicates: int
    late_rows: int
    rejected: int

    @property
    def balanced(self) -> bool:
        return self.received == self.inserted + self.duplicates + self.rejected
