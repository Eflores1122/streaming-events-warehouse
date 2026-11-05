"""One-shot path: generate, publish, consume, compact, check."""

from __future__ import annotations

import shutil
from pathlib import Path

from andensur.broker import open_broker
from andensur.catchup import replay_archive
from andensur.compact import compact
from andensur.config import (
    BATCH_SIZE,
    BLOCK_MS,
    CONSUMER,
    DEFAULT_ARCHIVE,
    DEFAULT_STANDIN,
    DEFAULT_WAREHOUSE,
    FRESHNESS_SLA_SECONDS,
    GROUP,
    LATE_WINDOW,
    MAX_IDLE_POLLS,
    STREAM,
)
from andensur.consumer import consume
from andensur.generate import build_events, write_jsonl
from andensur.quality import run_quality
from andensur.warehouse import connect


def run_demo(
    *,
    orders: int = 120,
    seed: int = 42,
    broker_kind: str = "stand-in",
    warehouse_path: Path = DEFAULT_WAREHOUSE,
    archive_path: Path = DEFAULT_ARCHIVE,
    standin_root: Path = DEFAULT_STANDIN,
    redis_url: str = "redis://127.0.0.1:6379/0",
    fresh: bool = False,
    batch_size: int = BATCH_SIZE,
    block_ms: int = BLOCK_MS,
    max_idle_polls: int = MAX_IDLE_POLLS,
    claim_after_s: float = 30.0,
) -> dict:
    warehouse_path = Path(warehouse_path)
    archive_path = Path(archive_path)
    standin_root = Path(standin_root)
    if fresh:
        _reset_local(warehouse_path, standin_root if broker_kind == "stand-in" else None)
    broker = open_broker(
        broker_kind,
        standin_root=standin_root,
        redis_url=redis_url,
        claim_after_s=claim_after_s,
    )
    connection = None
    try:
        if fresh and broker_kind == "redis":
            broker.delete_stream(STREAM)
        events = build_events(orders=orders, seed=seed)
        write_jsonl(archive_path, events)
        broker.ensure_group(STREAM, GROUP)
        for event in events:
            broker.publish(STREAM, event)
        connection = connect(warehouse_path)
        batches = consume(
            broker,
            connection,
            stream=STREAM,
            group=GROUP,
            consumer=CONSUMER,
            batch_size=batch_size,
            block_ms=block_ms,
            max_idle_polls=max_idle_polls,
            late_window=LATE_WINDOW,
        )
        mart = compact(connection)
        checks = run_quality(
            connection,
            sla_seconds=FRESHNESS_SLA_SECONDS,
            require_rows=True,
            require_mart=True,
        )
        return _summary(
            connection,
            broker_kind=broker.kind,
            broker_label=broker.label,
            orders=orders,
            seed=seed,
            published=len(events),
            batches=batches,
            mart=mart,
            checks=checks,
            warehouse_path=warehouse_path,
            archive_path=archive_path,
        )
    finally:
        if connection is not None:
            connection.close()
        broker.close()


def replay_into(
    warehouse_path: Path,
    archive_path: Path,
    *,
    batch_size: int = BATCH_SIZE,
) -> dict:
    connection = connect(warehouse_path)
    try:
        stats = replay_archive(connection, archive_path, batch_size=batch_size)
        mart = compact(connection)
        checks = run_quality(
            connection,
            sla_seconds=FRESHNESS_SLA_SECONDS,
            require_rows=True,
            require_mart=True,
        )
        stats["mart_rows"] = mart["mart_rows"]
        stats["quality_passed"] = all(row["passed"] for row in checks)
        stats["checks"] = checks
        return stats
    finally:
        connection.close()


def format_report(summary: dict) -> str:
    by_type = summary.get("by_type") or {}
    type_lines = [f"  {name}: {count}" for name, count in by_type.items()]
    failed = [row["name"] for row in summary.get("checks", []) if not row["passed"]]
    lines = [
        "Anden Sur streaming demo",
        f"broker: {summary['broker_label']}",
        f"orders: {summary['orders']}",
        f"seed: {summary['seed']}",
        f"published: {summary['published']}",
        f"inserted: {summary['inserted']}",
        f"duplicates: {summary['duplicates']}",
        f"rejected: {summary['rejected']}",
        f"warehouse_events: {summary['warehouse_events']}",
        f"late: {summary['late']}",
        f"mart_rows: {summary['mart_rows']}",
        f"mart_event_count: {summary['mart_event_count']}",
        "events_by_type:",
        *type_lines,
        f"freshness: {'pass' if summary['freshness_passed'] else 'fail'}",
        f"quality: {'pass' if summary['quality_passed'] else 'fail'}",
    ]
    if failed:
        lines.append("failed_checks: " + ", ".join(failed))
    lines.append(f"warehouse: {summary['warehouse']}")
    lines.append(f"archive: {summary['archive']}")
    return "\n".join(lines)


def _summary(
    connection,
    *,
    broker_kind: str,
    broker_label: str,
    orders: int,
    seed: int,
    published: int,
    batches,
    mart: dict,
    checks: list[dict],
    warehouse_path: Path,
    archive_path: Path,
) -> dict:
    by_type = {
        name: count
        for name, count in connection.execute(
            """
            SELECT event_type, count(*)
            FROM warehouse.events
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    }
    late = connection.execute("SELECT count(*) FROM warehouse.events WHERE late").fetchone()[0]
    warehouse_events = connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0]
    freshness = next(row for row in checks if row["name"] == "ingest_freshness")
    return {
        "broker": broker_kind,
        "broker_label": broker_label,
        "orders": orders,
        "seed": seed,
        "published": published,
        "inserted": sum(batch.inserted for batch in batches),
        "duplicates": sum(batch.duplicates for batch in batches),
        "rejected": sum(batch.rejected for batch in batches),
        "late_inserted": sum(batch.late_rows for batch in batches),
        "warehouse_events": int(warehouse_events),
        "late": int(late),
        "mart_rows": mart["mart_rows"],
        "mart_event_count": mart["mart_event_count"],
        "by_type": by_type,
        "quality_passed": all(row["passed"] for row in checks),
        "freshness_passed": freshness["passed"],
        "checks": checks,
        "warehouse": str(warehouse_path),
        "archive": str(archive_path),
    }


def _reset_local(warehouse_path: Path, standin_root: Path | None) -> None:
    if warehouse_path.exists():
        warehouse_path.unlink()
    wal = Path(str(warehouse_path) + ".wal")
    if wal.exists():
        wal.unlink()
    if standin_root is not None and standin_root.exists():
        shutil.rmtree(standin_root)
