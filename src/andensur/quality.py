"""Freshness and structural checks. A failed check is a data error, not a retry."""

from __future__ import annotations

from datetime import datetime

import duckdb

from andensur.models import (
    CHANNELS,
    EVENT_TYPES,
    FULFILLMENTS,
    INVENTORY_SCAN_TYPES,
    SCAN_TYPES,
    utcnow,
)


class DataQualityError(RuntimeError):
    def __init__(self, results: list[dict]) -> None:
        self.results = results
        failed = [row for row in results if not row["passed"]]
        detail = "; ".join(f"{row['name']}: {row['detail']}" for row in failed)
        super().__init__(detail or "data quality failed")


def run_quality(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of: datetime | None = None,
    sla_seconds: int = 900,
    require_rows: bool = True,
    require_mart: bool = False,
) -> list[dict]:
    results = [_check_sql(connection, name, sql) for name, sql in _structural_checks(require_rows)]
    if require_mart:
        results.append(_check_sql(connection, "mart_matches_events", _MART_SQL))
    results.append(_freshness(connection, as_of=as_of or utcnow(), sla_seconds=sla_seconds))
    return results


def assert_quality(connection: duckdb.DuckDBPyConnection, **kwargs) -> list[dict]:
    results = run_quality(connection, **kwargs)
    if any(not row["passed"] for row in results):
        raise DataQualityError(results)
    return results


def _structural_checks(require_rows: bool) -> list[tuple[str, str]]:
    types = _sql_list(EVENT_TYPES)
    channels = _sql_list(CHANNELS)
    fulfillments = _sql_list(FULFILLMENTS)
    inventory_scans = _sql_list(INVENTORY_SCAN_TYPES)
    scans = _sql_list(SCAN_TYPES)
    checks = [
        (
            "event_id_unique",
            "SELECT count(*) - count(DISTINCT event_id) FROM warehouse.events",
        ),
        (
            "required_not_null",
            """
            SELECT count(*) FROM warehouse.events
            WHERE event_id IS NULL
               OR event_type IS NULL
               OR event_time IS NULL
               OR ingested_at IS NULL
               OR order_id IS NULL
            """,
        ),
        (
            "event_type_accepted",
            f"SELECT count(*) FROM warehouse.events WHERE event_type NOT IN ({types})",
        ),
        (
            "amount_non_negative",
            """
            SELECT count(*) FROM warehouse.events
            WHERE amount_centavos IS NOT NULL AND amount_centavos < 0
            """,
        ),
        (
            "currency_mxn",
            """
            SELECT count(*) FROM warehouse.events
            WHERE event_type = 'order.created'
              AND coalesce(currency, '') <> 'MXN'
            """,
        ),
        (
            "order_channel",
            f"""
            SELECT count(*) FROM warehouse.events
            WHERE event_type = 'order.created'
              AND (
                channel NOT IN ({channels})
                OR fulfillment NOT IN ({fulfillments})
                OR customer_ref IS NULL
                OR sku_count IS NULL
                OR sku_count < 1
              )
            """,
        ),
        (
            "order_id_unique",
            """
            SELECT count(*) FROM (
              SELECT order_id
              FROM warehouse.events
              WHERE event_type = 'order.created'
              GROUP BY order_id
              HAVING count(*) > 1
            )
            """,
        ),
        (
            "inventory_shape",
            f"""
            SELECT count(*) FROM warehouse.events
            WHERE event_type = 'inventory.scanned'
              AND (
                scan_type NOT IN ({inventory_scans})
                OR sku IS NULL
                OR bin_code IS NULL
                OR hub_code IS NULL
                OR city IS NULL
                OR qty IS NULL
                OR qty < 1
              )
            """,
        ),
        (
            "inventory_has_order",
            """
            SELECT count(*)
            FROM warehouse.events AS inventory
            LEFT JOIN warehouse.events AS orders
              ON orders.event_type = 'order.created'
             AND orders.order_id = inventory.order_id
            WHERE inventory.event_type = 'inventory.scanned'
              AND orders.event_id IS NULL
            """,
        ),
        (
            "order_has_pick",
            """
            SELECT count(*)
            FROM warehouse.events AS orders
            LEFT JOIN warehouse.events AS inventory
              ON inventory.event_type = 'inventory.scanned'
             AND inventory.scan_type = 'pick'
             AND inventory.order_id = orders.order_id
            WHERE orders.event_type = 'order.created'
              AND inventory.event_id IS NULL
            """,
        ),
        (
            "scan_shape",
            f"""
            SELECT count(*) FROM warehouse.events
            WHERE event_type = 'shipment.scanned'
              AND (
                scan_type NOT IN ({scans})
                OR shipment_id IS NULL
                OR hub_code IS NULL
                OR city IS NULL
              )
            """,
        ),
        (
            "scan_has_order",
            """
            SELECT count(*)
            FROM warehouse.events AS scan
            LEFT JOIN warehouse.events AS orders
              ON orders.event_type = 'order.created'
             AND orders.order_id = scan.order_id
            WHERE scan.event_type = 'shipment.scanned'
              AND orders.event_id IS NULL
            """,
        ),
        (
            "released_has_shipment",
            """
            SELECT count(*)
            FROM warehouse.events AS orders
            LEFT JOIN warehouse.events AS scan
              ON scan.event_type = 'shipment.scanned'
             AND scan.order_id = orders.order_id
            WHERE orders.event_type = 'order.created'
              AND orders.fulfillment = 'released'
              AND scan.event_id IS NULL
            """,
        ),
        (
            "cancelled_has_return",
            """
            SELECT count(*)
            FROM warehouse.events AS orders
            LEFT JOIN warehouse.events AS inventory
              ON inventory.event_type = 'inventory.scanned'
             AND inventory.scan_type = 'return_to_stock'
             AND inventory.order_id = orders.order_id
            WHERE orders.event_type = 'order.created'
              AND orders.fulfillment = 'cancelled'
              AND inventory.event_id IS NULL
            """,
        ),
        (
            "cancelled_has_no_shipment",
            """
            SELECT count(*)
            FROM warehouse.events AS orders
            JOIN warehouse.events AS scan
              ON scan.event_type = 'shipment.scanned'
             AND scan.order_id = orders.order_id
            WHERE orders.event_type = 'order.created'
              AND orders.fulfillment = 'cancelled'
            """,
        ),
    ]
    if require_rows:
        checks.insert(
            0,
            (
                "events_present",
                "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM warehouse.events",
            ),
        )
    return checks


_MART_SQL = """
SELECT CASE
  WHEN (
    SELECT count(*) FROM warehouse.events
  ) = coalesce((SELECT sum(event_count) FROM mart.hourly_corridor), 0)
  AND (
    SELECT count(*) FROM warehouse.events WHERE late
  ) = coalesce((SELECT sum(late_count) FROM mart.hourly_corridor), 0)
  THEN 0 ELSE 1
END
"""


def _freshness(
    connection: duckdb.DuckDBPyConnection,
    *,
    as_of: datetime,
    sla_seconds: int,
) -> dict:
    row = connection.execute("SELECT max(ingested_at) FROM warehouse.events").fetchone()
    if row is None or row[0] is None:
        return {
            "name": "ingest_freshness",
            "passed": False,
            "detail": "no ingested_at",
            "violations": 1,
        }
    latest = row[0]
    if isinstance(latest, datetime) and latest.tzinfo is not None:
        latest = latest.replace(tzinfo=None)
    age = (as_of - latest).total_seconds()
    passed = age <= sla_seconds
    return {
        "name": "ingest_freshness",
        "passed": passed,
        "detail": f"age_seconds={age:.3f} sla_seconds={sla_seconds}",
        "violations": 0 if passed else 1,
    }


def _check_sql(connection: duckdb.DuckDBPyConnection, name: str, sql: str) -> dict:
    violations = int(connection.execute(sql).fetchone()[0] or 0)
    return {
        "name": name,
        "passed": violations == 0,
        "detail": f"violations={violations}",
        "violations": violations,
    }


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value + "'" for value in values)
