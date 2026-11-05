"""Hourly catch-up and compaction for Andén Sur.

The streaming consumer is the primary path. This DAG is the batch safety net:
replay the JSONL archive through the same event_id merge, rebuild the hourly
corridor mart, and fail the run when freshness or a structural check fails.

catchup is off, so enabling the DAG does not replay from the start date.
max_active_runs is 1 because a DuckDB file has one writer. CI does not install
Airflow and does not import this module.

Schedule `15 * * * *` UTC is 09:15 in America/Mexico_City (UTC-6, no daylight
saving). There is no Airflow SLA on the tasks. Freshness is the quality check.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "edgar-flores",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _warehouse() -> Path:
    return Path(os.environ.get("ANDENSUR_WAREHOUSE", "data/warehouse/andensur.duckdb"))


def _archive() -> Path:
    return Path(os.environ.get("ANDENSUR_ARCHIVE", "data/archive/events.jsonl"))


@dag(
    dag_id="andensur_catchup_compact",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["andensur", "synthetic"],
)
def andensur_catchup_compact():
    @task
    def replay_archive():
        from andensur.catchup import replay_archive_path

        stats = replay_archive_path(_warehouse(), _archive())
        return {
            "inserted": stats["inserted"],
            "duplicates": stats["duplicates"],
            "received": stats["received"],
        }

    @task
    def compact_mart():
        from andensur.compact import compact_path

        stats = compact_path(_warehouse())
        return {
            "mart_rows": stats["mart_rows"],
            "mart_event_count": stats["mart_event_count"],
        }

    @task
    def freshness():
        from andensur.quality import assert_quality
        from andensur.warehouse import connect

        connection = connect(_warehouse())
        try:
            assert_quality(
                connection,
                require_rows=True,
                require_mart=True,
            )
        finally:
            connection.close()

    replay_archive() >> compact_mart() >> freshness()


andensur_catchup_compact()
