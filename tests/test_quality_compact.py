from datetime import datetime, timedelta

from tests.helpers import inventory_event, order_event, scan_event

from andensur.compact import compact
from andensur.consumer import apply_events
from andensur.quality import run_quality
from andensur.warehouse import connect


def _load_released(connection):
    apply_events(
        connection,
        [
            ("1", order_event()),
            ("2", inventory_event()),
            ("3", scan_event()),
        ],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 11, 5, 16, 0, 0),
    )


def test_freshness_fails_when_ingest_is_older_than_the_sla(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    _load_released(connection)
    fresh = run_quality(
        connection,
        as_of=datetime(2026, 11, 5, 16, 5, 0),
        sla_seconds=900,
        require_mart=False,
    )
    assert all(row["passed"] for row in fresh)
    stale = run_quality(
        connection,
        as_of=datetime(2026, 11, 5, 18, 0, 0),
        sla_seconds=900,
        require_mart=False,
    )
    by_name = {row["name"]: row for row in stale}
    assert by_name["ingest_freshness"]["passed"] is False
    assert by_name["event_id_unique"]["passed"] is True


def test_bad_inventory_qty_fails_the_shape_check(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    _load_released(connection)
    connection.execute(
        """
        UPDATE warehouse.events
        SET qty = -1
        WHERE event_id = 'evt-inv-00001-pick'
        """
    )
    results = run_quality(
        connection,
        as_of=datetime(2026, 11, 5, 16, 1, 0),
        sla_seconds=900,
        require_mart=False,
    )
    failed = {row["name"] for row in results if not row["passed"]}
    assert "inventory_shape" in failed


def test_compaction_is_idempotent_and_matches_the_log(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    _load_released(connection)
    apply_events(
        connection,
        [
            (
                "5",
                order_event(
                    event_id="evt-ord-00008", order_id="ORD-8", when="2026-11-02T15:30:00Z"
                ),
            )
        ],
        broker_name="stand-in",
        late_window=timedelta(hours=6),
        ingested_at=datetime(2026, 11, 5, 16, 0, 0),
    )
    first = compact(connection)
    second = compact(connection)
    assert first == second
    assert first["mart_event_count"] == first["event_rows"] == 4
    hour = connection.execute("SELECT min(hour_start) FROM mart.hourly_corridor").fetchone()[0]
    touched = compact(connection, hour_start=hour)
    assert touched["mart_event_count"] == 4
    third = compact(connection, hour_start=hour)
    assert third == touched


def test_empty_warehouse_is_not_fresh(tmp_path):
    connection = connect(tmp_path / "wh.duckdb")
    results = run_quality(connection, require_rows=True, require_mart=False)
    failed = {row["name"] for row in results if not row["passed"]}
    assert "events_present" in failed
    assert "ingest_freshness" in failed
