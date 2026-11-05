"""DuckDB warehouse. Idempotency is the primary key on event_id.

A micro-batch is deduped, then inserted only when `event_id` is new. Replaying
a delivery does not update the stored row and does not move `late` after the
fact. The watermark is the max `event_time` seen. A row is late when its
`event_time` is strictly older than that watermark minus the allowed window.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from andensur.models import utcnow

_SCHEMA = Path(__file__).resolve().parent / "sql" / "duckdb_schema.sql"

_INSERT_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "ingested_at",
    "late",
    "broker",
    "stream_offset",
    "order_id",
    "amount_centavos",
    "city",
    "state",
    "currency",
    "customer_ref",
    "channel",
    "fulfillment",
    "sku",
    "bin_code",
    "qty",
    "shipment_id",
    "scan_type",
    "hub_code",
    "sku_count",
    "payload",
)

_INSERT_SQL = f"""
INSERT INTO warehouse.events ({", ".join(_INSERT_COLUMNS)})
VALUES ({", ".join("?" for _ in _INSERT_COLUMNS)})
"""


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute(_SCHEMA.read_text(encoding="utf-8"))
    return connection


def merge_events(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict],
    *,
    late_window: timedelta,
    ingested_at: datetime | None = None,
) -> tuple[int, int, int]:
    """Insert unseen event_ids. Returns inserted, duplicates, late_rows.

    `rows` must already carry broker and stream_offset. Caller holds the
    transaction. The first copy of an event_id in the batch wins. Duplicates
    are `len(rows) - inserted`, counting extra copies inside the batch and
    event_ids already stored. Existing rows are not updated.
    """
    received = len(rows)
    if received == 0:
        return 0, 0, 0
    ingested = ingested_at or utcnow()
    deduped: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        if row["event_id"] in seen:
            continue
        seen.add(row["event_id"])
        deduped.append(dict(row))
    watermark = _watermark_for(connection, rows)
    threshold = watermark - late_window
    existing = _existing_ids(connection, list(seen))
    to_insert = []
    late_rows = 0
    for row in deduped:
        row["ingested_at"] = ingested
        row["late"] = row["event_time"] < threshold
        if row["event_id"] in existing:
            continue
        to_insert.append(row)
        if row["late"]:
            late_rows += 1
    if to_insert:
        connection.executemany(
            _INSERT_SQL,
            [tuple(row[column] for column in _INSERT_COLUMNS) for row in to_insert],
        )
    _store_watermark(connection, watermark, ingested)
    inserted = len(to_insert)
    return inserted, received - inserted, late_rows


def insert_rejects(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> None:
    if not rows:
        return
    connection.executemany(
        """
        INSERT INTO warehouse.rejects (
          reject_id, stream_offset, reason, payload, rejected_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (reject_id) DO NOTHING
        """,
        rows,
    )


def record_batch(connection: duckdb.DuckDBPyConnection, row: tuple) -> None:
    connection.execute(
        """
        INSERT INTO warehouse.ingest_batches (
          batch_id, started_at, finished_at, broker, received, inserted,
          duplicates, late_rows, rejected
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(row),
    )


def event_count(connection: duckdb.DuckDBPyConnection) -> int:
    return _event_count(connection)


def _event_count(connection: duckdb.DuckDBPyConnection) -> int:
    return int(connection.execute("SELECT count(*) FROM warehouse.events").fetchone()[0])


def _watermark_for(connection: duckdb.DuckDBPyConnection, rows: list[dict]) -> datetime:
    batch_max = max(row["event_time"] for row in rows)
    stored = connection.execute(
        "SELECT watermark FROM warehouse.stream_state WHERE key = 'events'"
    ).fetchone()
    if stored is None or stored[0] is None:
        return batch_max
    current = stored[0]
    if isinstance(current, datetime) and current.tzinfo is not None:
        current = current.replace(tzinfo=None)
    return max(current, batch_max)


def _store_watermark(
    connection: duckdb.DuckDBPyConnection,
    watermark: datetime,
    updated_at: datetime,
) -> None:
    stored = connection.execute(
        "SELECT watermark FROM warehouse.stream_state WHERE key = 'events'"
    ).fetchone()
    if stored is None:
        connection.execute(
            """
            INSERT INTO warehouse.stream_state (key, watermark, updated_at)
            VALUES ('events', ?, ?)
            """,
            [watermark, updated_at],
        )
        return
    current = stored[0]
    if isinstance(current, datetime) and current.tzinfo is not None:
        current = current.replace(tzinfo=None)
    kept = watermark if current is None or watermark > current else current
    connection.execute(
        """
        UPDATE warehouse.stream_state
        SET watermark = ?, updated_at = ?
        WHERE key = 'events'
        """,
        [kept, updated_at],
    )


def _existing_ids(connection: duckdb.DuckDBPyConnection, event_ids: list[str]) -> set[str]:
    placeholders = ", ".join("?" for _ in event_ids)
    found = connection.execute(
        f"SELECT event_id FROM warehouse.events WHERE event_id IN ({placeholders})",
        event_ids,
    ).fetchall()
    return {row[0] for row in found}
