"""Replay the JSONL archive through the same event_id merge as the consumer.

The archive is the batch safety net when a consumer ack was lost or the
broker was empty. A second replay inserts nothing.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb

from andensur.config import BATCH_SIZE, LATE_WINDOW
from andensur.consumer import apply_events
from andensur.generate import read_jsonl
from andensur.models import BatchStats
from andensur.warehouse import connect


def replay_archive(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    batch_size: int = BATCH_SIZE,
    late_window: timedelta = LATE_WINDOW,
) -> dict:
    events = read_jsonl(path)
    totals = BatchStats(0, 0, 0, 0, 0)
    batches = 0
    for start in range(0, len(events), batch_size):
        chunk = events[start : start + batch_size]
        deliveries = [(f"archive-{start + index:06d}", event) for index, event in enumerate(chunk)]
        stats = apply_events(
            connection,
            deliveries,
            broker_name="archive",
            late_window=late_window,
        )
        totals = _add(totals, stats)
        batches += 1
    return {
        "archive": str(path),
        "batches": batches,
        "received": totals.received,
        "inserted": totals.inserted,
        "duplicates": totals.duplicates,
        "late_rows": totals.late_rows,
        "rejected": totals.rejected,
    }


def replay_archive_path(
    warehouse: Path,
    archive: Path,
    *,
    batch_size: int = BATCH_SIZE,
    late_window: timedelta = LATE_WINDOW,
) -> dict:
    if not Path(archive).is_file():
        raise FileNotFoundError(f"archive not found: {archive}")
    connection = connect(warehouse)
    try:
        return replay_archive(
            connection,
            Path(archive),
            batch_size=batch_size,
            late_window=late_window,
        )
    finally:
        connection.close()


def _add(left: BatchStats, right: BatchStats) -> BatchStats:
    return BatchStats(
        received=left.received + right.received,
        inserted=left.inserted + right.inserted,
        duplicates=left.duplicates + right.duplicates,
        late_rows=left.late_rows + right.late_rows,
        rejected=left.rejected + right.rejected,
    )
