"""Command line for the Andén Sur streaming reference."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from andensur.broker import open_broker
from andensur.catchup import replay_archive_path
from andensur.compact import compact, compact_path
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
from andensur.generate import build_events, expected_counts, write_jsonl
from andensur.pipeline import format_report, run_demo
from andensur.quality import run_quality
from andensur.warehouse import connect


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="andensur",
        description="Synthetic streaming demo for Anden Sur. No cloud credentials.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="generate, publish, consume, compact, and check")
    _add_common(demo)
    demo.add_argument("--orders", type=int, default=120)
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--fresh", action=argparse.BooleanOptionalAction, default=True)
    demo.set_defaults(func=_cmd_demo)

    generate = sub.add_parser("generate", help="write the JSONL archive and stop")
    generate.add_argument("--orders", type=int, default=120)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    generate.set_defaults(func=_cmd_generate)

    produce = sub.add_parser("produce", help="generate, archive, and publish")
    _add_common(produce)
    produce.add_argument("--orders", type=int, default=120)
    produce.add_argument("--seed", type=int, default=42)
    produce.set_defaults(func=_cmd_produce)

    consume_cmd = sub.add_parser("consume", help="micro-batch from the broker into DuckDB")
    _add_common(consume_cmd)
    consume_cmd.set_defaults(func=_cmd_consume)

    catchup = sub.add_parser("catchup", help="MERGE the JSONL archive into DuckDB")
    catchup.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    catchup.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    catchup.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    catchup.set_defaults(func=_cmd_catchup)

    compact_cmd = sub.add_parser("compact", help="rebuild mart.hourly_corridor")
    compact_cmd.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    compact_cmd.set_defaults(func=_cmd_compact)

    quality = sub.add_parser("dq", help="run freshness and structural checks")
    quality.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    quality.add_argument("--sla-seconds", type=int, default=FRESHNESS_SLA_SECONDS)
    quality.set_defaults(func=_cmd_quality)

    args = parser.parse_args(argv)
    return int(args.func(args))


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--broker", choices=("stand-in", "redis"), default="stand-in")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--standin-root", type=Path, default=DEFAULT_STANDIN)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)


def _cmd_demo(args: argparse.Namespace) -> int:
    summary = run_demo(
        orders=args.orders,
        seed=args.seed,
        broker_kind=args.broker,
        warehouse_path=args.warehouse,
        archive_path=args.archive,
        standin_root=args.standin_root,
        redis_url=args.redis_url,
        fresh=args.fresh,
        batch_size=args.batch_size,
    )
    print(format_report(summary))
    return 0 if summary["quality_passed"] else 1


def _cmd_generate(args: argparse.Namespace) -> int:
    events = build_events(orders=args.orders, seed=args.seed)
    write_jsonl(args.archive, events)
    counts = expected_counts(args.orders)
    print(
        json.dumps(
            {"archive": str(args.archive), "published": len(events), **counts},
            sort_keys=True,
        )
    )
    return 0


def _cmd_produce(args: argparse.Namespace) -> int:
    events = build_events(orders=args.orders, seed=args.seed)
    write_jsonl(args.archive, events)
    broker = open_broker(
        args.broker,
        standin_root=args.standin_root,
        redis_url=args.redis_url,
    )
    try:
        broker.ensure_group(STREAM, GROUP)
        for event in events:
            broker.publish(STREAM, event)
    finally:
        broker.close()
    print(f"published: {len(events)}")
    print(f"broker: {args.broker}")
    print(f"archive: {args.archive}")
    return 0


def _cmd_consume(args: argparse.Namespace) -> int:
    broker = open_broker(
        args.broker,
        standin_root=args.standin_root,
        redis_url=args.redis_url,
    )
    connection = connect(args.warehouse)
    try:
        batches = consume(
            broker,
            connection,
            stream=STREAM,
            group=GROUP,
            consumer=CONSUMER,
            batch_size=args.batch_size,
            block_ms=BLOCK_MS,
            max_idle_polls=MAX_IDLE_POLLS,
            late_window=LATE_WINDOW,
        )
        compact(connection)
    finally:
        connection.close()
        broker.close()
    inserted = sum(batch.inserted for batch in batches)
    duplicates = sum(batch.duplicates for batch in batches)
    print(f"inserted: {inserted}")
    print(f"duplicates: {duplicates}")
    print(f"warehouse: {args.warehouse}")
    return 0


def _cmd_catchup(args: argparse.Namespace) -> int:
    stats = replay_archive_path(args.warehouse, args.archive, batch_size=args.batch_size)
    print(
        json.dumps(
            {key: stats[key] for key in ("received", "inserted", "duplicates", "rejected")},
            sort_keys=True,
        )
    )
    return 0


def _cmd_compact(args: argparse.Namespace) -> int:
    stats = compact_path(args.warehouse)
    print(json.dumps(stats, sort_keys=True))
    return 0


def _cmd_quality(args: argparse.Namespace) -> int:
    connection = connect(args.warehouse)
    try:
        results = run_quality(
            connection,
            sla_seconds=args.sla_seconds,
            require_rows=True,
            require_mart=True,
        )
    finally:
        connection.close()
    for row in results:
        state = "pass" if row["passed"] else "fail"
        print(f"{state} {row['name']} {row['detail']}")
    return 0 if all(row["passed"] for row in results) else 1
