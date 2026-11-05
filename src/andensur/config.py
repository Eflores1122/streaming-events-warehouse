"""Paths and contracts shared by the CLI, the consumer, and the optional DAG."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

STREAM = "andensur.events"
GROUP = "andensur-warehouse"
CONSUMER = "warehouse-1"

DEFAULT_WAREHOUSE = Path("data/warehouse/andensur.duckdb")
DEFAULT_ARCHIVE = Path("data/archive/events.jsonl")
DEFAULT_STANDIN = Path("data/broker/stand-in")

LATE_WINDOW = timedelta(hours=6)
FRESHNESS_SLA_SECONDS = 15 * 60
BATCH_SIZE = 50

# Redis may block once on an empty read. The stand-in returns immediately.
BLOCK_MS = 200
MAX_IDLE_POLLS = 2
