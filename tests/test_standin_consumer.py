from datetime import timedelta

from tests.helpers import order_event, scan_event

from andensur.config import GROUP, STREAM
from andensur.consumer import apply_events, consume
from andensur.standin import StandInBroker
from andensur.warehouse import connect


def test_pending_redelivery_and_ack(tmp_path):
    broker = StandInBroker(tmp_path / "broker", claim_after_s=30)
    broker.ensure_group(STREAM, GROUP)
    event = order_event()
    broker.publish(STREAM, event)
    broker.publish(STREAM, order_event(event_id="evt-ord-00002", order_id="ORD-2"))
    first = broker.read(STREAM, GROUP, "c1", count=10, block_ms=0)
    assert [item.offset for item in first] == ["1", "2"]
    again = broker.read(STREAM, GROUP, "c1", count=10, block_ms=0)
    assert [item.offset for item in again] == ["1", "2"]
    broker.ack(STREAM, GROUP, ["1", "2"])
    assert broker.read(STREAM, GROUP, "c1", count=10, block_ms=0) == []


def test_two_consumers_split_new_offsets(tmp_path):
    broker = StandInBroker(tmp_path / "broker", claim_after_s=30)
    broker.ensure_group(STREAM, GROUP)
    for index in range(4):
        broker.publish(STREAM, order_event(event_id=f"evt-ord-{index}", order_id=f"ORD-{index}"))
    left = broker.read(STREAM, GROUP, "c1", count=2, block_ms=0)
    right = broker.read(STREAM, GROUP, "c2", count=2, block_ms=0)
    assert {item.offset for item in left}.isdisjoint({item.offset for item in right})
    assert {item.offset for item in left + right} == {"1", "2", "3", "4"}


def test_claim_redelivers_unacked_work_without_a_second_warehouse_row(tmp_path):
    broker = StandInBroker(tmp_path / "broker", claim_after_s=0)
    connection = connect(tmp_path / "wh.duckdb")
    broker.ensure_group(STREAM, GROUP)
    event = order_event()
    broker.publish(STREAM, event)
    broker.publish(STREAM, order_event(event_id="evt-ord-00002", order_id="ORD-2"))
    first = broker.read(STREAM, GROUP, "c1", count=2, block_ms=0)
    apply_events(
        connection,
        [(item.offset, item.event) for item in first],
        broker_name=broker.kind,
        late_window=timedelta(hours=6),
    )
    claimed = broker.read(STREAM, GROUP, "c2", count=2, block_ms=0)
    assert [item.offset for item in claimed] == [item.offset for item in first]
    second = apply_events(
        connection,
        [(item.offset, item.event) for item in claimed],
        broker_name=broker.kind,
        late_window=timedelta(hours=6),
    )
    assert second.inserted == 0
    assert second.duplicates == 2
    broker.ack(STREAM, GROUP, [item.offset for item in claimed])
    assert connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0] == 2


def test_invalid_event_is_rejected_and_acked(tmp_path):
    broker = StandInBroker(tmp_path / "broker")
    connection = connect(tmp_path / "wh.duckdb")
    broker.ensure_group(STREAM, GROUP)
    good = order_event()
    bad = {"event_id": "evt-bad", "event_type": "order.created"}
    broker.publish(STREAM, good)
    broker.publish(STREAM, bad)
    batches = consume(
        broker,
        connection,
        stream=STREAM,
        group=GROUP,
        consumer="warehouse-1",
        batch_size=10,
        block_ms=0,
        max_idle_polls=1,
        late_window=timedelta(hours=6),
    )
    assert sum(batch.inserted for batch in batches) == 1
    assert sum(batch.rejected for batch in batches) == 1
    assert connection.execute("SELECT count(*) FROM warehouse.rejects").fetchone()[0] == 1
    again = consume(
        broker,
        connection,
        stream=STREAM,
        group=GROUP,
        consumer="warehouse-1",
        batch_size=10,
        block_ms=0,
        max_idle_polls=1,
        late_window=timedelta(hours=6),
    )
    assert again == []
    assert connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0] == 1
    assert connection.execute("SELECT count(*) FROM warehouse.rejects").fetchone()[0] == 1


def test_commit_failure_leaves_the_offset_pending(tmp_path, monkeypatch):
    broker = StandInBroker(tmp_path / "broker")
    connection = connect(tmp_path / "wh.duckdb")
    broker.ensure_group(STREAM, GROUP)
    broker.publish(STREAM, scan_event())

    def boom(*_args, **_kwargs):
        raise RuntimeError("disk")

    monkeypatch.setattr("andensur.consumer.record_batch", boom)
    try:
        consume(
            broker,
            connection,
            stream=STREAM,
            group=GROUP,
            consumer="warehouse-1",
            batch_size=10,
            block_ms=0,
            max_idle_polls=1,
            late_window=timedelta(hours=6),
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("consume should have failed")
    pending = broker.read(STREAM, GROUP, "warehouse-1", count=10, block_ms=0)
    assert [item.offset for item in pending] == ["1"]
    assert connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0] == 0
