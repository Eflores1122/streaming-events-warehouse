"""Micro-batch consumer. Ack runs only after the warehouse transaction commits."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import duckdb

from andensur.models import BatchStats, EventRejected, project_event, utcnow, validate_event
from andensur.warehouse import insert_rejects, merge_events, record_batch


def apply_events(
    connection: duckdb.DuckDBPyConnection,
    deliveries: list[tuple[str, dict]],
    *,
    broker_name: str,
    late_window: timedelta,
    ingested_at: datetime | None = None,
) -> BatchStats:
    """Validate, merge, and record one batch. Does not ack."""
    if not deliveries:
        return BatchStats(0, 0, 0, 0, 0)
    started = utcnow()
    ingested = ingested_at or started
    valid: list[dict] = []
    rejects: list[tuple] = []
    for offset, event in deliveries:
        try:
            validate_event(event)
        except EventRejected as exc:
            raw = event if isinstance(event, dict) else {"raw": str(event)}
            rejects.append(
                (
                    f"{broker_name}:{offset}",
                    str(offset),
                    exc.reason,
                    json.dumps(raw, sort_keys=True, default=str),
                    ingested,
                )
            )
            continue
        row = project_event(event)
        row["broker"] = broker_name
        row["stream_offset"] = str(offset)
        valid.append(row)
    connection.execute("BEGIN")
    try:
        inserted, duplicates, late_rows = merge_events(
            connection,
            valid,
            late_window=late_window,
            ingested_at=ingested,
        )
        insert_rejects(connection, rejects)
        stats = BatchStats(
            received=len(deliveries),
            inserted=inserted,
            duplicates=duplicates,
            late_rows=late_rows,
            rejected=len(rejects),
        )
        if not stats.balanced:
            raise RuntimeError(
                "batch accounting broke: "
                f"received={stats.received} inserted={stats.inserted} "
                f"duplicates={stats.duplicates} rejected={stats.rejected}"
            )
        finished = utcnow()
        record_batch(
            connection,
            (
                str(uuid.uuid4()),
                started,
                finished,
                broker_name,
                stats.received,
                stats.inserted,
                stats.duplicates,
                stats.late_rows,
                stats.rejected,
            ),
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return stats


def consume(
    broker,
    connection: duckdb.DuckDBPyConnection,
    *,
    stream: str,
    group: str,
    consumer: str,
    batch_size: int,
    block_ms: int,
    max_idle_polls: int,
    late_window: timedelta,
) -> list[BatchStats]:
    """Read until `max_idle_polls` empty reads. Ack each batch after commit."""
    broker.ensure_group(stream, group)
    batches: list[BatchStats] = []
    idle = 0
    while idle < max_idle_polls:
        deliveries = broker.read(
            stream,
            group,
            consumer,
            count=batch_size,
            block_ms=block_ms if idle else 0,
        )
        if not deliveries:
            idle += 1
            continue
        idle = 0
        stats = apply_events(
            connection,
            [(delivery.offset, delivery.event) for delivery in deliveries],
            broker_name=broker.kind,
            late_window=late_window,
        )
        broker.ack(stream, group, [delivery.offset for delivery in deliveries])
        batches.append(stats)
    return batches
