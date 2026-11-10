import { writeFileSync } from "node:fs";
import { buildEvents, eventsToJsonl } from "../src/generate.js";

const orders = Number(process.argv[2]);
const seed = Number(process.argv[3]);
const out = process.argv[4];
if (!out || !Number.isInteger(orders) || !Number.isInteger(seed)) {
  console.error("usage: dump-events.mjs <orders> <seed> <out.jsonl>");
  process.exit(2);
}
const text = eventsToJsonl(buildEvents({ orders, seed }));
if (/[^\n\t\r\x20-\x7e]/.test(text)) {
  console.error("generator emitted a non-ASCII byte; Python json.dumps would escape it");
  process.exit(1);
}
writeFileSync(out, text);
