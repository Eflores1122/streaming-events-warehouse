"""Broker factory.

Redis Streams is the docker-compose broker. The stand-in is the default for
the smoke test. Both expose publish, a consumer group, read, and ack.
Neither deduplicates event_id. That rule belongs to the warehouse.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from andensur.models import Delivery
from andensur.standin import StandInBroker


class Broker(Protocol):
    """Read, process, ack. The stream keeps every published copy."""

    kind: str
    label: str

    def publish(self, stream: str, event: dict) -> str: ...

    def ensure_group(self, stream: str, group: str) -> None: ...

    def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int,
        block_ms: int,
    ) -> list[Delivery]: ...

    def ack(self, stream: str, group: str, offsets: list[str]) -> None: ...

    def close(self) -> None: ...


def open_broker(
    kind: str,
    *,
    standin_root: Path,
    redis_url: str,
    claim_after_s: float = 30.0,
) -> Broker:
    if kind == "stand-in":
        return StandInBroker(standin_root, claim_after_s=claim_after_s)
    if kind == "redis":
        from andensur.redis_streams import RedisStreamsBroker

        try:
            broker = RedisStreamsBroker(redis_url, claim_after_s=claim_after_s)
            broker.ping()
        except Exception as exc:
            from andensur.redis_streams import RedisUnavailable

            if isinstance(exc, RedisUnavailable):
                raise SystemExit(str(exc)) from exc
            raise SystemExit(f"Redis is not reachable at {redis_url}: {exc}") from exc
        return broker
    raise SystemExit(f"unknown broker {kind!r}; choose stand-in or redis")
