"""Local consumer-group stand-in.

This is not Redis Streams and not Kafka. It exists so the generator, the
micro-batch consumer, and the idempotent warehouse can run with no broker
process. A directory holds an append-only log and one JSON file of group
state. `fcntl` locks that directory so a producer process and a consumer
process can share it on Linux or macOS.

The contract matches the Redis path closely enough for the tests:

- `read` returns this consumer's pending entries first (at-least-once)
- otherwise it claims pending entries older than `claim_after_s`
- otherwise it assigns new offsets and advances the group cursor
- `ack` drops offsets from the pending set
- the log is not compacted and does not deduplicate `event_id`
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

from andensur.models import Delivery


class StandInBroker:
    kind = "stand-in"
    label = "stand-in (local consumer-group stand-in; not Redis Streams and not Kafka)"

    def __init__(self, root: Path, *, claim_after_s: float = 30.0) -> None:
        self.root = Path(root)
        self.claim_after_s = claim_after_s
        self.root.mkdir(parents=True, exist_ok=True)

    def publish(self, stream: str, event: dict) -> str:
        with self._locked():
            state = self._load_state()
            offset = str(state["next_offset"])
            state["next_offset"] += 1
            with self._messages_path.open("a", encoding="utf-8") as handle:
                line = {"offset": offset, "stream": stream, "event": event}
                handle.write(json.dumps(line, sort_keys=True, separators=(",", ":")))
                handle.write("\n")
            self._save_state(state)
            return offset

    def ensure_group(self, stream: str, group: str) -> None:
        with self._locked():
            state = self._load_state()
            groups = state["groups"].setdefault(stream, {})
            groups.setdefault(group, {"cursor": 1, "pending": {}})
            self._save_state(state)

    def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int,
        block_ms: int,
    ) -> list[Delivery]:
        del block_ms  # the stand-in does not block; the caller treats [] as an idle poll
        with self._locked():
            state = self._load_state()
            group_state = self._require_group(state, stream, group)
            pending: dict = group_state["pending"]
            own = sorted(
                (offset for offset, meta in pending.items() if meta["consumer"] == consumer),
                key=int,
            )
            if own:
                return self._deliver(stream, own[:count])
            now = time.time()
            expired = sorted(
                (
                    offset
                    for offset, meta in pending.items()
                    if now - float(meta["since"]) >= self.claim_after_s
                ),
                key=int,
            )
            if expired:
                chosen = expired[:count]
                for offset in chosen:
                    pending[offset] = {"consumer": consumer, "since": now}
                self._save_state(state)
                return self._deliver(stream, chosen)
            messages = self._message_index()
            chosen = []
            scan = int(group_state["cursor"])
            last = int(state["next_offset"])
            while scan < last and len(chosen) < count:
                key = str(scan)
                message = messages.get(key)
                scan += 1
                if message is None or message["stream"] != stream:
                    continue
                if key in pending:
                    continue
                chosen.append(key)
            group_state["cursor"] = scan
            for offset in chosen:
                pending[offset] = {"consumer": consumer, "since": now}
            self._save_state(state)
            return self._deliver(stream, chosen)

    def ack(self, stream: str, group: str, offsets: list[str]) -> None:
        with self._locked():
            state = self._load_state()
            pending = self._require_group(state, stream, group)["pending"]
            for offset in offsets:
                pending.pop(str(offset), None)
            self._save_state(state)

    def close(self) -> None:
        return None

    @property
    def _messages_path(self) -> Path:
        return self.root / "messages.jsonl"

    @property
    def _state_path(self) -> Path:
        return self.root / "state.json"

    @contextmanager
    def _locked(self):
        self.root.mkdir(parents=True, exist_ok=True)
        handle = (self.root / ".lock").open("a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def _load_state(self) -> dict:
        if not self._state_path.exists():
            return {"next_offset": 1, "groups": {}}
        return json.loads(self._state_path.read_text(encoding="utf-8"))

    def _save_state(self, state: dict) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state), encoding="utf-8")
        os.replace(temporary, self._state_path)

    def _require_group(self, state: dict, stream: str, group: str) -> dict:
        try:
            return state["groups"][stream][group]
        except KeyError as exc:
            raise RuntimeError(
                f"consumer group {group!r} does not exist on {stream!r}; call ensure_group"
            ) from exc

    def _message_index(self) -> dict[str, dict]:
        index: dict[str, dict] = {}
        if not self._messages_path.exists():
            return index
        with self._messages_path.open(encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                message = json.loads(text)
                index.setdefault(str(message["offset"]), message)
        return index

    def _deliver(self, stream: str, offsets: list[str]) -> list[Delivery]:
        index = self._message_index()
        deliveries = []
        for offset in offsets:
            message = index.get(offset)
            if message is None or message.get("stream") != stream:
                continue
            event = message.get("event")
            if not isinstance(event, dict):
                event = {"_malformed": True}
            deliveries.append(Delivery(offset=offset, event=event))
        return deliveries
