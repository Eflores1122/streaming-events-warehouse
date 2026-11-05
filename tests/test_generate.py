import json

from andensur.generate import (
    STREAM_EPOCH,
    amount_centavos,
    build_events,
    cancelled_order_count,
    customer_ref,
    expected_counts,
)


def test_counts_follow_the_published_rule():
    assert cancelled_order_count(120) == 12
    assert cancelled_order_count(1) == 1
    counts = expected_counts(120)
    assert counts == {
        "orders": 120,
        "cancelled_orders": 12,
        "released_orders": 108,
        "inventory_scans": 132,
        "scans": 432,
        "unique_events": 684,
        "replays": 4,
        "published": 688,
        "late_events": 3,
    }


def test_generator_is_deterministic_and_synthetic():
    first = build_events(orders=120, seed=42)
    second = build_events(orders=120, seed=42)
    assert first == second
    counts = expected_counts(120)
    assert len(first) == counts["published"]
    assert len({event["event_id"] for event in first}) == counts["unique_events"]
    assert first[:4] == first[-4:]
    blob = json.dumps(first)
    assert "@" not in blob
    assert "Sintetico" in blob
    for event in first:
        assert event["source"] == "andensur-synthetic"
        amount = event["payload"].get("amount_centavos")
        if amount is not None:
            assert isinstance(amount, int)
            assert amount % 100 == 0


def test_seed_42_first_order_and_first_late_scan():
    events = build_events(orders=120, seed=42)
    first = events[0]
    assert first["event_id"] == "evt-ord-00000"
    assert first["event_time"] == "2026-11-02T14:00:00Z"
    assert first["payload"]["customer_ref"] == customer_ref(42, 0) == "Sintetico042000"
    assert first["payload"]["amount_centavos"] == amount_centavos(42, 0) == 19200
    assert first["payload"]["city"] == "Ciudad de Mexico"
    assert first["payload"]["channel"] == "online"
    assert first["payload"]["fulfillment"] == "cancelled"
    late = next(event for event in events if event["event_id"] == "evt-scn-00001-3")
    assert late["event_type"] == "shipment.scanned"
    assert late["payload"]["scan_type"] == "delivered"
    assert late["event_time"] == "2026-11-02T04:00:00Z"
    assert late["event_time"] < "2026-11-02T10:00:00Z"
    opened = STREAM_EPOCH
    assert opened.isoformat(sep="T") == "2026-11-02T16:00:00"


def test_small_populations_match_the_formula():
    for orders in (1, 10, 40):
        events = build_events(orders=orders, seed=7)
        counts = expected_counts(orders)
        assert len(events) == counts["published"]
        assert len({event["event_id"] for event in events}) == counts["unique_events"]
        scans = [event for event in events if event["event_type"] == "shipment.scanned"]
        assert len({event["event_id"] for event in scans}) == counts["scans"]
