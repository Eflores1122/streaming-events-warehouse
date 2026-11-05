"""Redis Streams broker.

Consumer groups, pending entries, and ack are Redis server features
(`XGROUP`, `XREADGROUP`, `XAUTOCLAIM`, `XACK`). The stream stores the JSON
envelope in one field, `event`. It does not deduplicate `event_id`.
"""

from __future__ import annotations

import json

from andensur.models import Delivery


class RedisUnavailable(RuntimeError):
    pass


class RedisStreamsBroker:
    kind = "redis"
    label = "redis-streams (Redis Streams consumer group, pending entries, and ack)"

    def __init__(self, url: str, *, claim_after_s: float = 30.0, client=None) -> None:
        self.url = url
        self.claim_after_s = claim_after_s
        if client is not None:
            self._redis = client
            return
        try:
            import redis
        except ImportError as exc:
            raise RedisUnavailable(
                "Redis backend needs the redis extra: pip install -e '.[redis]'"
            ) from exc
        self._redis = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=5,
        )

    def ping(self) -> bool:
        try:
            return bool(self._redis.ping())
        except Exception as exc:
            raise RedisUnavailable(f"Redis is not reachable at {self.url}: {exc}") from exc

    def delete_stream(self, stream: str) -> None:
        self._redis.delete(stream)

    def publish(self, stream: str, event: dict) -> str:
        body = json.dumps(event, sort_keys=True, separators=(",", ":"))
        return str(self._redis.xadd(stream, {"event": body}))

    def ensure_group(self, stream: str, group: str) -> None:
        import redis

        try:
            # id 0: a group created after the first XADD still sees those entries.
            self._redis.xgroup_create(stream, group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int,
        block_ms: int,
    ) -> list[Delivery]:
        own = self._read_id(stream, group, consumer, stream_id="0", count=count, block_ms=None)
        if own:
            return own
        claimed = self._claim(stream, group, consumer, count)
        if claimed:
            return claimed
        block = block_ms if block_ms and block_ms > 0 else None
        return self._read_id(stream, group, consumer, stream_id=">", count=count, block_ms=block)

    def ack(self, stream: str, group: str, offsets: list[str]) -> None:
        if offsets:
            self._redis.xack(stream, group, *offsets)

    def close(self) -> None:
        self._redis.close()

    def _claim(self, stream: str, group: str, consumer: str, count: int) -> list[Delivery]:
        idle_ms = max(0, int(self.claim_after_s * 1000))
        result = self._redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=idle_ms,
            start_id="0-0",
            count=count,
        )
        messages = result[1] if result and len(result) > 1 else []
        return self._to_deliveries(messages)

    def _read_id(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        stream_id: str,
        count: int,
        block_ms: int | None,
    ) -> list[Delivery]:
        kwargs = {
            "groupname": group,
            "consumername": consumer,
            "streams": {stream: stream_id},
            "count": count,
        }
        if block_ms is not None:
            kwargs["block"] = block_ms
        response = self._redis.xreadgroup(**kwargs)
        if not response:
            return []
        deliveries: list[Delivery] = []
        for _name, entries in response:
            deliveries.extend(self._to_deliveries(entries))
        return deliveries

    def _to_deliveries(self, entries) -> list[Delivery]:
        deliveries = []
        for entry in entries or []:
            offset, fields = entry
            event = _decode_event(fields)
            deliveries.append(Delivery(offset=str(offset), event=event))
        return deliveries


def _decode_event(fields) -> dict:
    if not isinstance(fields, dict):
        return {"_malformed": True}
    raw = fields.get("event")
    if not isinstance(raw, str):
        return {"_malformed": True}
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return {"_malformed": True, "raw": raw}
    if not isinstance(event, dict):
        return {"_malformed": True}
    return event
