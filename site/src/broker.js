// In-memory stand-in. Offsets start at 1, matching src/andensur/standin.py.
// One consumer. Pending entries are delivered again until ack.

export const STREAM = "andensur.events";
export const GROUP = "andensur-warehouse";
export const CONSUMER = "warehouse-1";
export const BROKER_LABEL =
  "stand-in (local consumer-group stand-in; not Redis Streams and not Kafka)";

export class StandIn {
  constructor() {
    this.nextOffset = 1;
    this.messages = new Map();
    this.cursor = 1;
    this.pending = new Map();
  }

  get logSize() {
    return this.messages.size;
  }

  get pendingCount() {
    return this.pending.size;
  }

  publish(event) {
    const offset = String(this.nextOffset);
    this.nextOffset += 1;
    this.messages.set(offset, event);
    return offset;
  }

  read(count) {
    const own = [...this.pending.keys()].sort((left, right) => Number(left) - Number(right));
    if (own.length) return this.deliver(own.slice(0, count));
    const chosen = [];
    let scan = this.cursor;
    while (scan < this.nextOffset && chosen.length < count) {
      const key = String(scan);
      scan += 1;
      if (!this.messages.has(key) || this.pending.has(key)) continue;
      chosen.push(key);
    }
    this.cursor = scan;
    for (const offset of chosen) this.pending.set(offset, Date.now());
    return this.deliver(chosen);
  }

  ack(offsets) {
    for (const offset of offsets) this.pending.delete(String(offset));
  }

  deliver(offsets) {
    const deliveries = [];
    for (const offset of offsets) {
      const event = this.messages.get(String(offset));
      if (event) deliveries.push({ offset: String(offset), event });
    }
    return deliveries;
  }
}
