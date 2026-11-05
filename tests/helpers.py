"""Small valid events for warehouse tests. Still synthetic."""

from __future__ import annotations

from andensur.models import make_event


def order_event(
    event_id: str = "evt-ord-00001",
    order_id: str = "ORD-1",
    amount: int = 19200,
    when: str = "2026-11-02T14:00:00Z",
    city: str = "Puebla",
) -> dict:
    return make_event(
        event_id=event_id,
        event_type="order.created",
        event_time=when,
        payload={
            "order_id": order_id,
            "customer_ref": "Sintetico000001",
            "channel": "online",
            "fulfillment": "released",
            "city": city,
            "state": "Puebla",
            "amount_centavos": amount,
            "currency": "MXN",
            "sku_count": 1,
        },
    )


def inventory_event(
    event_id: str = "evt-inv-00001-pick",
    order_id: str = "ORD-1",
    scan_type: str = "pick",
    qty: int = 1,
    when: str = "2026-11-02T14:02:00Z",
) -> dict:
    return make_event(
        event_id=event_id,
        event_type="inventory.scanned",
        event_time=when,
        payload={
            "order_id": order_id,
            "sku": "SKU-0001",
            "bin_code": "BIN-HUB-PBC-01",
            "qty": qty,
            "scan_type": scan_type,
            "hub_code": "HUB-PBC",
            "city": "Puebla",
            "state": "Puebla",
        },
    )


def scan_event(
    event_id: str = "evt-scn-00001-0",
    order_id: str = "ORD-1",
    when: str = "2026-11-02T14:10:00Z",
    scan_type: str = "picked_up",
) -> dict:
    return make_event(
        event_id=event_id,
        event_type="shipment.scanned",
        event_time=when,
        payload={
            "shipment_id": "SHP-1",
            "order_id": order_id,
            "scan_type": scan_type,
            "hub_code": "HUB-PBC",
            "city": "Puebla",
            "state": "Puebla",
        },
    )
