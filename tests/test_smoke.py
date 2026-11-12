import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from andensur.catchup import replay_archive_path
from andensur.config import GROUP, LATE_WINDOW, STREAM
from andensur.consumer import consume
from andensur.generate import build_events, expected_counts, write_jsonl
from andensur.pipeline import run_demo
from andensur.standin import StandInBroker
from andensur.warehouse import connect


def test_reference_demo_dedups_and_flags_late_scans(tmp_path):
    summary = run_demo(
        orders=120,
        seed=42,
        broker_kind="stand-in",
        warehouse_path=tmp_path / "wh.duckdb",
        archive_path=tmp_path / "events.jsonl",
        standin_root=tmp_path / "broker",
        fresh=True,
        block_ms=0,
        max_idle_polls=1,
    )
    counts = expected_counts(120)
    assert summary["published"] == counts["published"] == 688
    assert summary["warehouse_events"] == counts["unique_events"] == 684
    assert summary["inserted"] == 684
    assert summary["duplicates"] == 4
    assert summary["rejected"] == 0
    assert summary["late"] == 3
    assert summary["mart_rows"] == 53
    assert summary["mart_event_count"] == 684
    assert summary["quality_passed"] is True
    assert summary["freshness_passed"] is True
    assert summary["broker_label"].startswith("stand-in")
    assert "not Redis Streams and not Kafka" in summary["broker_label"]
    assert summary["by_type"]["order.created"] == 120
    assert summary["by_type"]["inventory.scanned"] == 132
    assert summary["by_type"]["shipment.scanned"] == 432

    connection = connect(tmp_path / "wh.duckdb")
    business_date = connection.execute(
        """
        SELECT business_date
        FROM warehouse.shipment_scans
        WHERE event_id = 'evt-scn-00001-3'
        """
    ).fetchone()[0]
    assert str(business_date) == "2026-11-01"
    order_date = connection.execute(
        """
        SELECT business_date
        FROM warehouse.orders
        WHERE event_id = 'evt-ord-00000'
        """
    ).fetchone()[0]
    assert str(order_date) == "2026-11-02"
    connection.close()

    replay = replay_archive_path(tmp_path / "wh.duckdb", tmp_path / "events.jsonl")
    assert replay["inserted"] == 0
    assert replay["duplicates"] == 688

    second = run_demo(
        orders=120,
        seed=42,
        broker_kind="stand-in",
        warehouse_path=tmp_path / "wh.duckdb",
        archive_path=tmp_path / "events.jsonl",
        standin_root=tmp_path / "broker",
        fresh=False,
        block_ms=0,
        max_idle_polls=1,
    )
    assert second["inserted"] == 0
    assert second["duplicates"] == 688
    assert second["warehouse_events"] == 684
    assert second["quality_passed"] is True


def test_archive_catchup_then_stream_does_not_double_count(tmp_path):
    orders = 20
    events = build_events(orders=orders, seed=42)
    archive = tmp_path / "events.jsonl"
    write_jsonl(archive, events)
    broker = StandInBroker(tmp_path / "broker")
    broker.ensure_group(STREAM, GROUP)
    for event in events:
        broker.publish(STREAM, event)
    warehouse = tmp_path / "wh.duckdb"
    stats = replay_archive_path(warehouse, archive)
    counts = expected_counts(orders)
    assert stats["inserted"] == counts["unique_events"]
    assert stats["duplicates"] == counts["replays"]
    connection = connect(warehouse)
    late = connection.execute("SELECT count(*) FROM warehouse.events WHERE late").fetchone()[0]
    assert late == counts["late_events"]
    batches = consume(
        broker,
        connection,
        stream=STREAM,
        group=GROUP,
        consumer="warehouse-1",
        batch_size=50,
        block_ms=0,
        max_idle_polls=1,
        late_window=LATE_WINDOW,
    )
    assert sum(batch.inserted for batch in batches) == 0
    assert (
        connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0]
        == counts["unique_events"]
    )
    connection.close()
    broker.close()


def test_cli_smoke(tmp_path):
    warehouse = tmp_path / "wh.duckdb"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "andensur",
            "demo",
            "--orders",
            "8",
            "--seed",
            "42",
            "--fresh",
            "--broker",
            "stand-in",
            "--warehouse",
            str(warehouse),
            "--archive",
            str(tmp_path / "events.jsonl"),
            "--standin-root",
            str(tmp_path / "broker"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert "quality: pass" in completed.stdout
    assert "stand-in (local consumer-group stand-in" in completed.stdout
    assert warehouse.exists()


def test_generate_command_writes_the_archive(tmp_path):
    archive = tmp_path / "events.jsonl"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "andensur",
            "generate",
            "--orders",
            "5",
            "--seed",
            "42",
            "--archive",
            str(archive),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["published"] == expected_counts(5)["published"]
    lines = archive.read_text(encoding="utf-8").splitlines()
    assert len(lines) == payload["published"]
    assert json.loads(lines[0])["source"] == "andensur-synthetic"


def test_readme_locks_the_reference_counts_and_the_disclaimer():
    readme = Path(__file__).resolve().parents[1].joinpath("README.md").read_text(encoding="utf-8")
    assert "Andén Sur is not a real company." in readme
    assert "Nothing here describes an employer, a client, or a production system." in readme
    assert "synthetic" in readme.lower()
    assert "```mermaid" in readme
    assert "stand-in" in readme
    assert "not Redis Streams and not Kafka" in readme
    assert "event_id" in readme
    assert "BigQuery" in readme
    assert "Storage Write API" in readme
    assert "684" in readme
    assert "688" in readme
    assert "Sintetico042000" in readme
    assert "MIT" in readme
    assert "five generated types" not in readme
    assert "order.created" in readme
    assert "inventory.scanned" in readme
    assert "shipment.scanned" in readme
    assert "DuckDB-WASM" in readme
    assert "ack after commit" in readme
    for phrase in ("production GCP streaming", "runs in production on BigQuery"):
        assert phrase not in readme
    # The sample clock is the generator epoch, not the wall clock of a test run.
    assert datetime(2026, 11, 2, 14, 0, 0).strftime("%Y-%m-%dT%H:%M:%SZ") in readme
