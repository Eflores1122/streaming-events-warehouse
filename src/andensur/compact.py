"""Rebuild mart.hourly_corridor from the deduped event log.

The event log is the system of record. Compaction replaces mart rows for
every hour, or for one hour. Running it twice does not change the totals.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from andensur.warehouse import connect, event_count


def compact(
    connection: duckdb.DuckDBPyConnection,
    *,
    hour_start: datetime | None = None,
) -> dict:
    connection.execute("BEGIN")
    try:
        if hour_start is None:
            connection.execute("DELETE FROM mart.hourly_corridor")
            predicate = ""
            params: list = []
        else:
            connection.execute(
                "DELETE FROM mart.hourly_corridor WHERE hour_start = ?",
                [hour_start],
            )
            predicate = "WHERE date_trunc('hour', event_time) = ?"
            params = [hour_start]
        connection.execute(
            f"""
            INSERT INTO mart.hourly_corridor (
              hour_start, event_type, city, event_count, late_count, amount_centavos
            )
            SELECT
              date_trunc('hour', event_time) AS hour_start,
              event_type,
              coalesce(city, 'UNKNOWN') AS city,
              count(*)::BIGINT AS event_count,
              sum(CASE WHEN late THEN 1 ELSE 0 END)::BIGINT AS late_count,
              coalesce(sum(amount_centavos), 0)::BIGINT AS amount_centavos
            FROM warehouse.events
            {predicate}
            GROUP BY 1, 2, 3
            """,
            params,
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    mart_rows, mart_events, mart_late = connection.execute(
        """
        SELECT
          count(*),
          coalesce(sum(event_count), 0),
          coalesce(sum(late_count), 0)
        FROM mart.hourly_corridor
        """
    ).fetchone()
    return {
        "mart_rows": int(mart_rows),
        "mart_event_count": int(mart_events),
        "mart_late_count": int(mart_late),
        "event_rows": event_count(connection),
        "hour_start": None if hour_start is None else hour_start.isoformat(sep=" "),
    }


def compact_path(path: Path, *, hour_start: datetime | None = None) -> dict:
    connection = connect(path)
    try:
        return compact(connection, hour_start=hour_start)
    finally:
        connection.close()
