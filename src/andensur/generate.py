"""Deterministic synthetic retail orders, inventory scans, and shipment scans.

Andén Sur is a fulfillment floor for a fictitious Mexican online retailer.
Counts do not depend on the seed. The seed changes merchandise amounts and
customer references only. Every customer reference is `Sintetico######`.
Nothing in this generator is a real shopper, carton, or warehouse.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from andensur.models import make_event

STREAM_EPOCH = datetime(2026, 11, 2, 16, 0, 0)  # naive UTC
REPLAY_COPIES = 4
LATE_SCANS = 3
LATE_SHIFT = timedelta(hours=12)
SPREAD = timedelta(hours=2)

# ASCII names on purpose. They are corridor keys, not a gazetteer.
CITIES = (
    ("Ciudad de Mexico", "CDMX", "HUB-MEX"),
    ("Guadalajara", "Jalisco", "HUB-GDL"),
    ("Monterrey", "Nuevo Leon", "HUB-MTY"),
    ("Puebla", "Puebla", "HUB-PBC"),
    ("Merida", "Yucatan", "HUB-MID"),
    ("Queretaro", "Queretaro", "HUB-QRO"),
)

CROSS_DOCK = CITIES[0]


def cancelled_order_count(orders: int) -> int:
    if orders <= 0:
        raise ValueError("orders must be positive")
    # i % 10 == 0, with i starting at 0, so order 0 is always cancelled.
    return (orders - 1) // 10 + 1


def expected_counts(orders: int) -> dict[str, int]:
    cancelled = cancelled_order_count(orders)
    released = orders - cancelled
    inventory = orders + cancelled
    scans = 4 * released
    unique = orders + inventory + scans
    replays = min(REPLAY_COPIES, unique)
    return {
        "orders": orders,
        "cancelled_orders": cancelled,
        "released_orders": released,
        "inventory_scans": inventory,
        "scans": scans,
        "unique_events": unique,
        "replays": replays,
        "published": unique + replays,
        "late_events": min(LATE_SCANS, released),
    }


def amount_centavos(seed: int, index: int) -> int:
    """Whole pesos, between 150.00 and 414.99 MXN, from the seed and the index."""
    return 15_000 + ((seed + index * 97) % 4_000) * 100


def customer_ref(seed: int, index: int) -> str:
    return f"Sintetico{(seed * 1000 + index) % 1_000_000:06d}"


def build_events(*, orders: int, seed: int) -> list[dict]:
    if orders <= 0:
        raise ValueError("orders must be positive")
    events: list[dict] = []
    late_left = LATE_SCANS
    span_seconds = int(SPREAD.total_seconds())
    for index in range(orders):
        origin = CITIES[index % len(CITIES)]
        dest = CITIES[(index + 2) % len(CITIES)]
        opened = STREAM_EPOCH - SPREAD + timedelta(seconds=int(index * span_seconds / orders))
        amount = amount_centavos(seed, index)
        cancelled = index % 10 == 0
        order_id = f"ORD-20261102-{index:05d}"
        shipment_id = f"SHP-20261102-{index:05d}"
        city, state, _hub = origin
        sku_count = 1 + (index % 4)
        events.append(
            make_event(
                event_id=f"evt-ord-{index:05d}",
                event_type="order.created",
                event_time=opened,
                payload={
                    "order_id": order_id,
                    "customer_ref": customer_ref(seed, index),
                    "channel": "online" if index % 10 < 7 else "store",
                    "fulfillment": "cancelled" if cancelled else "released",
                    "city": city,
                    "state": state,
                    "amount_centavos": amount,
                    "currency": "MXN",
                    "sku_count": sku_count,
                },
            )
        )
        events.append(
            _inventory(
                index=index,
                order_id=order_id,
                when=opened + timedelta(minutes=2),
                scan_type="pick",
                place=origin,
                sku_count=sku_count,
            )
        )
        if cancelled:
            events.append(
                _inventory(
                    index=index,
                    order_id=order_id,
                    when=opened + timedelta(minutes=4),
                    scan_type="return_to_stock",
                    place=origin,
                    sku_count=sku_count,
                )
            )
            continue
        mark_late = late_left > 0
        if mark_late:
            late_left -= 1
        events.extend(_scans(index, opened, order_id, shipment_id, origin, dest, mark_late))
    replay_n = min(REPLAY_COPIES, len(events))
    events.extend(json.loads(json.dumps(event)) for event in events[:replay_n])
    return events


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


def read_jsonl(path: Path) -> list[dict]:
    events: list[dict] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                events.append(json.loads(text))
    return events


def _inventory(
    *,
    index: int,
    order_id: str,
    when: datetime,
    scan_type: str,
    place: tuple[str, str, str],
    sku_count: int,
) -> dict:
    city, state, hub = place
    suffix = "pick" if scan_type == "pick" else "return"
    return make_event(
        event_id=f"evt-inv-{index:05d}-{suffix}",
        event_type="inventory.scanned",
        event_time=when,
        payload={
            "order_id": order_id,
            "sku": f"SKU-{(index % 50):04d}",
            "bin_code": f"BIN-{hub}-{(index % 20):02d}",
            "qty": sku_count,
            "scan_type": scan_type,
            "hub_code": hub,
            "city": city,
            "state": state,
        },
    )


def _scans(
    index: int,
    opened: datetime,
    order_id: str,
    shipment_id: str,
    origin: tuple[str, str, str],
    dest: tuple[str, str, str],
    late_delivery: bool,
) -> list[dict]:
    hub_in = origin if origin[2] == CROSS_DOCK[2] else CROSS_DOCK
    plan = (
        ("picked_up", origin, opened + timedelta(minutes=10)),
        ("hub_in", hub_in, opened + timedelta(minutes=40)),
        ("out_for_delivery", dest, opened + timedelta(minutes=70)),
        (
            "delivered",
            dest,
            STREAM_EPOCH - LATE_SHIFT if late_delivery else opened + timedelta(minutes=100),
        ),
    )
    scans = []
    for step, (scan_type, place, when) in enumerate(plan):
        city, state, hub = place
        scans.append(
            make_event(
                event_id=f"evt-scn-{index:05d}-{step}",
                event_type="shipment.scanned",
                event_time=when,
                payload={
                    "shipment_id": shipment_id,
                    "order_id": order_id,
                    "scan_type": scan_type,
                    "hub_code": hub,
                    "city": city,
                    "state": state,
                },
            )
        )
    return scans
