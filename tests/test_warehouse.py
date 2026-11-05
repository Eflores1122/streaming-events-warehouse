from datetime import datetime, timedelta

import pytest
from tests.helpers import inventory_event, order_event

from andensur.consumer import apply_events
from andensur.models import project_event
from andensur.warehouse import connect, merge_events


def _row(event: dict, offset: str = "1") -> dict:
    row = project_event(event)
    row["broker"] = "stand-in"
    row["stream_offset"] = offset
    return row


def test_same_event_id_inserts_once(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    event = order_event()
    first = apply_events(
        connection,
        [("1", event), ("2", event)],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 11, 5, 12, 0, 0),
    )
    assert first.inserted == 1
    assert first.duplicates == 1
    assert first.balanced
    second = apply_events(
        connection,
        [("3", event)],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 11, 5, 12, 5, 0),
    )
    assert second.inserted == 0
    assert second.duplicates == 1
    assert connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0] == 1
    stored = connection.execute(
        "SELECT stream_offset, ingested_at FROM warehouse.events"
    ).fetchone()
    assert stored[0] == "1"
    assert stored[1] == datetime(2026, 11, 5, 12, 0, 0)


def test_late_flag_uses_the_batch_watermark_not_the_wall_clock(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    on_time = order_event(when="2026-11-02T15:00:00Z")
    late = order_event(
        event_id="evt-ord-00002",
        order_id="ORD-2",
        when="2026-11-02T04:00:00Z",
    )
    stats = apply_events(
        connection,
        [("1", on_time), ("2", late)],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 11, 20, 0, 0, 0),
    )
    assert stats.late_rows == 1
    flags = dict(
        connection.execute(
            "SELECT event_id, late FROM warehouse.events ORDER BY event_id"
        ).fetchall()
    )
    assert flags["evt-ord-00001"] is False
    assert flags["evt-ord-00002"] is True
    # A replay days later must not rewrite the stored flag.
    apply_events(
        connection,
        [("9", on_time)],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 12, 1, 0, 0, 0),
    )
    still = connection.execute(
        "SELECT late FROM warehouse.events WHERE event_id = 'evt-ord-00001'"
    ).fetchone()[0]
    assert still is False


def test_failed_batch_rolls_back(tmp_path, monkeypatch):
    connection = connect(tmp_path / "wh.duckdb")

    def boom(*_args, **_kwargs):
        raise RuntimeError("disk")

    monkeypatch.setattr("andensur.consumer.record_batch", boom)
    with pytest.raises(RuntimeError, match="disk"):
        apply_events(
            connection,
            [("1", order_event())],
            broker_name="stand-in",
            late_window=timedelta(hours=6),
        )
    assert connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0] == 0
    assert connection.execute("SELECT count(*) FROM warehouse.ingest_batches").fetchone()[0] == 0


def test_inventory_scan_lands_on_the_order(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    apply_events(
        connection,
        [
            ("1", order_event(amount=25000)),
            ("2", inventory_event(qty=2)),
        ],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
    )
    row = connection.execute(
        """
        SELECT inventory.qty, inventory.sku
        FROM warehouse.inventory_scans AS inventory
        JOIN warehouse.orders AS orders ON orders.order_id = inventory.order_id
        """
    ).fetchone()
    assert row == (2, "SKU-0001")


def test_merge_rejects_a_lower_watermark(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    window = timedelta(hours=6)
    merge_events(
        connection,
        [_row(order_event(when="2026-11-02T16:00:00Z"), "1")],
        late_window=window,
        ingested_at=datetime(2026, 11, 2, 16, 0, 0),
    )
    merge_events(
        connection,
        [
            _row(
                order_event(
                    event_id="evt-ord-00009", order_id="ORD-9", when="2026-11-02T12:00:00Z"
                ),
                "2",
            )
        ],
        late_window=window,
        ingested_at=datetime(2026, 11, 2, 16, 1, 0),
    )
    watermark = connection.execute(
        "SELECT watermark FROM warehouse.stream_state WHERE key = 'events'"
    ).fetchone()[0]
    assert watermark == datetime(2026, 11, 2, 16, 0, 0)
