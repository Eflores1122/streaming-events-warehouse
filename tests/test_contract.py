import os
import re
import tempfile
from datetime import timedelta
from importlib.resources import files
from pathlib import Path
from uuid import uuid4

import pytest

from andensur.models import EVENT_COLUMNS


def test_story_is_retail_fulfillment_without_a_tender_rail():
    root = Path(__file__).resolve().parents[1]
    paths = [root / "README.md", root / "pyproject.toml"]
    for folder in ("src", "tests", "dags"):
        paths.extend(path for path in (root / folder).rglob("*") if path.suffix in {".py", ".sql"})
    blob = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in paths
        if path.name != "test_contract.py"
    )
    for word in ("pay" + "ment", "sp" + "ei", "fin" + "tech", "bn" + "pl"):
        assert word not in blob
    assert re.search(r"\bbank\b", blob) is None
    assert re.search(r"\bcredit\b", blob) is None


def test_both_brokers_expose_the_same_read_ack_surface():
    from andensur.redis_streams import RedisStreamsBroker
    from andensur.standin import StandInBroker

    required = ("publish", "ensure_group", "read", "ack", "close")
    for cls in (StandInBroker, RedisStreamsBroker):
        for name in required:
            assert callable(getattr(cls, name))
        assert isinstance(cls.kind, str) and cls.kind
        assert isinstance(cls.label, str) and cls.label
    assert StandInBroker.kind == "stand-in"
    assert RedisStreamsBroker.kind == "redis"
    assert "not Redis Streams and not Kafka" in StandInBroker.label


def test_bigquery_file_is_a_target_shape_and_lists_the_same_columns():
    schema = files("andensur").joinpath("sql/duckdb_schema.sql").read_text(encoding="utf-8")
    target = files("andensur").joinpath("sql/bigquery_events.sql").read_text(encoding="utf-8")
    for column in EVENT_COLUMNS:
        assert column in schema
        assert column in target
    assert "MERGE `PROJECT.andensur.events`" in target
    assert "ON t.event_id = s.event_id" in target
    assert "WHEN NOT MATCHED THEN" in target
    assert "PARTITION BY DATE(event_time)" in target
    assert "CLUSTER BY event_type" in target
    assert "Storage Write API" in target
    assert "insertId" in target
    assert "at-least-once" in target
    assert "do not execute" in target.lower()
    assert "credentials" in target.lower()
    assert "google-cloud" not in target


def test_dag_is_the_batch_safety_net_and_does_not_embed_credentials():
    text = (
        Path(__file__)
        .resolve()
        .parents[1]
        .joinpath("dags/andensur_catchup_compact.py")
        .read_text(encoding="utf-8")
    )
    assert 'dag_id="andensur_catchup_compact"' in text
    assert "catchup=False" in text
    assert "max_active_runs=1" in text
    assert "replay_archive" in text
    assert "compact_mart" in text
    assert "freshness" in text
    lowered = text.lower()
    assert "credential" not in lowered
    assert "googleapis" not in lowered
    assert "secret" not in lowered


def test_redis_streams_round_trip_and_redelivery():
    url = os.environ.get("ANDENSUR_REDIS_URL", "redis://127.0.0.1:6379/0")
    try:
        from andensur.redis_streams import RedisStreamsBroker

        broker = RedisStreamsBroker(url)
        broker.ping()
    except Exception as exc:
        if os.environ.get("ANDENSUR_REQUIRE_REDIS") == "1":
            pytest.fail(f"Redis is required but unreachable: {exc}")
        pytest.skip(f"Redis is not running: {exc}")

    from andensur.consumer import apply_events
    from andensur.warehouse import connect

    stream = f"andensur.test.{uuid4().hex}"
    early = f"{stream}.early"
    group = "warehouse-test"
    event = {
        "event_id": "evt-ord-00007",
        "event_type": "order.created",
        "event_time": "2026-11-02T14:00:00Z",
        "source": "andensur-synthetic",
        "payload": {
            "order_id": "ORD-7",
            "customer_ref": "Sintetico000007",
            "channel": "online",
            "fulfillment": "released",
            "city": "Merida",
            "state": "Yucatan",
            "amount_centavos": 20000,
            "currency": "MXN",
            "sku_count": 1,
        },
    }
    try:
        broker.publish(early, event)
        broker.ensure_group(early, group)
        delivered = broker.read(early, group, "c1", count=5, block_ms=0)
        assert len(delivered) == 1
        assert delivered[0].event["event_id"] == "evt-ord-00007"
        broker.ack(early, group, [delivered[0].offset])

        broker.ensure_group(stream, group)
        broker.publish(stream, event)
        broker.publish(stream, event)
        first = broker.read(stream, group, "c1", count=10, block_ms=0)
        assert len(first) == 2
        with tempfile.TemporaryDirectory() as directory:
            connection = connect(Path(directory) / "wh.duckdb")
            stats = apply_events(
                connection,
                [(item.offset, item.event) for item in first],
                broker_name=broker.kind,
                late_window=timedelta(hours=6),
            )
            assert stats.inserted == 1
            assert stats.duplicates == 1
            pending = broker.read(stream, group, "c1", count=10, block_ms=0)
            assert [item.offset for item in pending] == [item.offset for item in first]
            again = apply_events(
                connection,
                [(item.offset, item.event) for item in pending],
                broker_name=broker.kind,
                late_window=timedelta(hours=6),
            )
            assert again.inserted == 0
            broker.ack(stream, group, [item.offset for item in pending])
            assert broker.read(stream, group, "c1", count=5, block_ms=0) == []
            stored = connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0]
            assert stored == 1
            connection.close()
    finally:
        broker.delete_stream(stream)
        broker.delete_stream(early)
        broker.close()
