"""Broker factory.

Redis Streams is the docker-compose broker. The stand-in is the default for
the smoke test. Both expose publish, a consumer group, read, and ack.
"""

from __future__ import annotations

from pathlib import Path

from andensur.standin import StandInBroker


def open_broker(
    kind: str,
    *,
    standin_root: Path,
    redis_url: str,
    claim_after_s: float = 30.0,
):
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
