// DuckDB-WASM session. Insert rule matches src/andensur/warehouse.py:
// first event_id wins, late is watermark minus 6 hours, ack is the caller's job.

import * as duckdb from "@duckdb/duckdb-wasm";
import wasmUrl from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import workerUrl from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import schemaSql from "../../src/andensur/sql/duckdb_schema.sql?raw";
import { BROKER_LABEL } from "./broker.js";
import { formatStamp } from "./generate.js";
import { canonicalJson } from "./json.js";
import { EventRejected, projectEvent, validateEvent } from "./validate.js";

const LATE_WINDOW_MS = 6 * 60 * 60 * 1000;
const BROKER_NAME = "stand-in";

const INSERT_COLUMNS = [
  "event_id",
  "event_type",
  "event_time",
  "ingested_at",
  "late",
  "broker",
  "stream_offset",
  "order_id",
  "amount_centavos",
  "city",
  "state",
  "currency",
  "customer_ref",
  "channel",
  "fulfillment",
  "sku",
  "bin_code",
  "qty",
  "shipment_id",
  "scan_type",
  "hub_code",
  "sku_count",
  "payload",
];

export class WarehouseSession {
  constructor() {
    this.db = null;
    this.conn = null;
    this.watermarkMs = null;
    this.version = null;
  }

  async open(onProgress) {
    const worker = new Worker(new URL(workerUrl, import.meta.url));
    this.db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
    const wasmHref = new URL(wasmUrl, import.meta.url).href;
    await this.db.instantiate(wasmHref, null, (progress) => {
      if (!onProgress) return;
      onProgress({
        pct: progress.bytesTotal
          ? Math.min(100, Math.round((100 * progress.bytesLoaded) / progress.bytesTotal))
          : 0,
        bytesTotal: progress.bytesTotal || 0,
      });
    });
    this.version = await this.db.getVersion();
    this.conn = await this.db.connect();
    await this.conn.query("SET TimeZone='UTC'");
    await this.applySchema();
  }

  async reset() {
    this.watermarkMs = null;
    await this.conn.query("DROP SCHEMA IF EXISTS mart CASCADE");
    await this.conn.query("DROP SCHEMA IF EXISTS warehouse CASCADE");
    await this.applySchema();
  }

  async applySchema() {
    for (const statement of splitSql(schemaSql)) {
      await this.conn.query(statement);
    }
  }

  async applyBatch(deliveries) {
    const started = new Date();
    const valid = [];
    const rejects = [];
    for (const delivery of deliveries) {
      try {
        validateEvent(delivery.event);
      } catch (error) {
        if (!(error instanceof EventRejected)) throw error;
        const raw =
          delivery.event && typeof delivery.event === "object"
            ? delivery.event
            : { raw: String(delivery.event) };
        rejects.push({
          reject_id: `${BROKER_NAME}:${delivery.offset}`,
          stream_offset: String(delivery.offset),
          reason: error.reason,
          payload: canonicalJson(raw),
          rejected_at: started,
        });
        continue;
      }
      const row = projectEvent(delivery.event);
      row.broker = BROKER_NAME;
      row.stream_offset = String(delivery.offset);
      row.offset = String(delivery.offset);
      valid.push(row);
    }

    await this.conn.query("BEGIN");
    try {
      const merged = await this.mergeEvents(valid, started);
      for (const reject of rejects) merged.outcomes.set(reject.stream_offset, "rejected");
      await this.insertRejects(rejects);
      const stats = {
        received: deliveries.length,
        inserted: merged.inserted,
        duplicates: merged.duplicates,
        lateRows: merged.lateRows,
        rejected: rejects.length,
      };
      if (stats.received !== stats.inserted + stats.duplicates + stats.rejected) {
        throw new Error(
          `batch accounting broke: received=${stats.received} inserted=${stats.inserted} duplicates=${stats.duplicates} rejected=${stats.rejected}`,
        );
      }
      const finished = new Date();
      await this.conn.query(`
        INSERT INTO warehouse.ingest_batches (
          batch_id, started_at, finished_at, broker, received, inserted,
          duplicates, late_rows, rejected
        ) VALUES (
          ${sqlString(crypto.randomUUID())},
          ${sqlTimestamp(started.getTime())},
          ${sqlTimestamp(finished.getTime())},
          ${sqlString(BROKER_NAME)},
          ${stats.received},
          ${stats.inserted},
          ${stats.duplicates},
          ${stats.lateRows},
          ${stats.rejected}
        )
      `);
      await this.conn.query("COMMIT");
      this.watermarkMs = merged.watermarkMs;
      return { stats, outcomes: merged.outcomes, broker: BROKER_LABEL };
    } catch (error) {
      try {
        await this.conn.query("ROLLBACK");
      } catch {
        // The transaction may already be closed.
      }
      throw error;
    }
  }

  async mergeEvents(rows, ingested) {
    const outcomes = new Map();
    if (rows.length === 0) {
      return { inserted: 0, duplicates: 0, lateRows: 0, outcomes, watermarkMs: this.watermarkMs };
    }
    const deduped = [];
    const seen = new Set();
    for (const row of rows) {
      if (seen.has(row.event_id)) continue;
      seen.add(row.event_id);
      deduped.push({ ...row });
    }
    const batchMax = Math.max(...rows.map((row) => row.event_time_ms));
    const watermarkMs = this.watermarkMs == null ? batchMax : Math.max(this.watermarkMs, batchMax);
    const threshold = watermarkMs - LATE_WINDOW_MS;
    const existing = await this.existingIds([...seen]);
    const toInsert = [];
    let lateRows = 0;
    const insertedIds = new Set();
    for (const row of deduped) {
      row.ingested_at_ms = ingested.getTime();
      row.late = row.event_time_ms < threshold;
      if (existing.has(row.event_id)) continue;
      toInsert.push(row);
      insertedIds.add(row.event_id);
      if (row.late) lateRows += 1;
    }
    if (toInsert.length) await this.insertEvents(toInsert);
    await this.storeWatermark(watermarkMs, ingested);
    const firstWins = new Set();
    for (const row of rows) {
      if (!firstWins.has(row.event_id) && insertedIds.has(row.event_id)) {
        firstWins.add(row.event_id);
        outcomes.set(row.offset, "inserted");
      } else {
        outcomes.set(row.offset, "duplicate");
      }
    }
    return {
      inserted: toInsert.length,
      duplicates: rows.length - toInsert.length,
      lateRows,
      outcomes,
      watermarkMs,
    };
  }

  async existingIds(eventIds) {
    if (eventIds.length === 0) return new Set();
    const list = eventIds.map((id) => sqlString(id)).join(", ");
    const { rows } = await this.query(
      `SELECT event_id FROM warehouse.events WHERE event_id IN (${list})`,
    );
    return new Set(rows.map((row) => row.event_id));
  }

  async insertEvents(rows) {
    const values = rows
      .map(
        (row) => `(
          ${sqlString(row.event_id)},
          ${sqlString(row.event_type)},
          ${sqlTimestamp(row.event_time_ms)},
          ${sqlTimestamp(row.ingested_at_ms)},
          ${row.late ? "TRUE" : "FALSE"},
          ${sqlString(row.broker)},
          ${sqlString(row.stream_offset)},
          ${sqlString(row.order_id)},
          ${sqlNumber(row.amount_centavos)},
          ${sqlString(row.city)},
          ${sqlString(row.state)},
          ${sqlString(row.currency)},
          ${sqlString(row.customer_ref)},
          ${sqlString(row.channel)},
          ${sqlString(row.fulfillment)},
          ${sqlString(row.sku)},
          ${sqlString(row.bin_code)},
          ${sqlNumber(row.qty)},
          ${sqlString(row.shipment_id)},
          ${sqlString(row.scan_type)},
          ${sqlString(row.hub_code)},
          ${sqlNumber(row.sku_count)},
          ${sqlString(row.payload)}
        )`,
      )
      .join(",\n");
    await this.conn.query(
      `INSERT INTO warehouse.events (${INSERT_COLUMNS.join(", ")}) VALUES ${values}`,
    );
  }

  async insertRejects(rows) {
    if (!rows.length) return;
    const values = rows
      .map(
        (row) => `(
          ${sqlString(row.reject_id)},
          ${sqlString(row.stream_offset)},
          ${sqlString(row.reason)},
          ${sqlString(row.payload)},
          ${sqlTimestamp(row.rejected_at.getTime())}
        )`,
      )
      .join(",\n");
    await this.conn.query(`
      INSERT INTO warehouse.rejects (reject_id, stream_offset, reason, payload, rejected_at)
      VALUES ${values}
      ON CONFLICT (reject_id) DO NOTHING
    `);
  }

  async storeWatermark(watermarkMs, updatedAt) {
    await this.conn.query("DELETE FROM warehouse.stream_state WHERE key = 'events'");
    await this.conn.query(`
      INSERT INTO warehouse.stream_state (key, watermark, updated_at)
      VALUES ('events', ${sqlTimestamp(watermarkMs)}, ${sqlTimestamp(updatedAt.getTime())})
    `);
  }

  async compact() {
    await this.conn.query("BEGIN");
    try {
      await this.conn.query("DELETE FROM mart.hourly_corridor");
      await this.conn.query(`
        INSERT INTO mart.hourly_corridor (
          hour_start, event_type, city, event_count, late_count, amount_centavos
        )
        SELECT
          date_trunc('hour', event_time) AS hour_start,
          event_type,
          coalesce(city, 'UNKNOWN') AS city,
          count(*)::BIGINT AS event_count,
          sum(CASE WHEN late THEN 1 ELSE 0 END)::BIGINT AS late_count,
          coalesce(sum(amount_centavos), 0)::BIGINT AS amount_centavos
        FROM warehouse.events
        GROUP BY 1, 2, 3
      `);
      await this.conn.query("COMMIT");
    } catch (error) {
      await this.conn.query("ROLLBACK");
      throw error;
    }
    const { rows } = await this.query(`
      SELECT
        count(*)::BIGINT AS mart_rows,
        coalesce(sum(event_count), 0)::BIGINT AS mart_event_count,
        coalesce(sum(late_count), 0)::BIGINT AS mart_late_count
      FROM mart.hourly_corridor
    `);
    return rows[0];
  }

  async query(sql) {
    const table = await this.conn.query(sql);
    return tableToRows(table);
  }

  async scalar(sql) {
    const { rows } = await this.query(sql);
    if (!rows.length) return null;
    const first = rows[0];
    return first[Object.keys(first)[0]];
  }
}

export function splitSql(text) {
  const parts = [];
  let current = [];
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (trimmed.startsWith("--")) continue;
    current.push(line);
    if (trimmed.endsWith(";")) {
      const statement = current.join("\n").trim().replace(/;\s*$/, "");
      if (statement) parts.push(statement);
      current = [];
    }
  }
  const tail = current.join("\n").trim().replace(/;\s*$/, "");
  if (tail) parts.push(tail);
  return parts;
}

export function sqlLiteral(value) {
  return sqlString(value);
}

export function prepareUserSql(sql) {
  const withoutComments = sql.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--[^\n]*/g, " ").trim();
  if (!withoutComments) return { error: "empty" };
  const stripped = withoutComments.replace(/;+\s*$/g, "").trim();
  if (!stripped) return { error: "empty" };
  if (stripped.includes(";")) return { error: "multi" };
  if (!/^(select|with|explain|describe|summarize)\b/i.test(stripped)) return { error: "kind" };
  if (
    /\b(insert|update|delete|drop|attach|detach|copy|create|alter|install|load|pragma|call|export|import)\b/i.test(
      stripped,
    )
  ) {
    return { error: "write" };
  }
  return { sql: stripped };
}

function sqlString(value) {
  if (value === null || value === undefined) return "NULL";
  return `'${String(value).replaceAll("'", "''")}'`;
}

function sqlNumber(value) {
  if (value === null || value === undefined) return "NULL";
  if (typeof value !== "number" || !Number.isFinite(value)) return "NULL";
  return String(value);
}

function sqlTimestamp(ms) {
  return `TIMESTAMP ${sqlString(formatStamp(ms))}`;
}

function tableToRows(table) {
  const fields = table.schema.fields.map((field) => field.name);
  const rows = [];
  const count = Number(table.numRows);
  for (let index = 0; index < count; index += 1) {
    const row = {};
    for (const name of fields) {
      const column = table.getChild(name);
      row[name] = normalizeCell(column ? column.get(index) : null);
    }
    rows.push(row);
  }
  return { fields, rows };
}

function normalizeCell(value) {
  if (value == null) return null;
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    if (Math.abs(asNumber) >= 1e14) {
      const ms = asNumber > 1e16 ? Math.trunc(asNumber / 1000) : asNumber;
      return new Date(ms).toISOString().replace(".000Z", "Z");
    }
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  if (value instanceof Date) return value.toISOString().replace(/\.\d{3}Z$/, "Z");
  if (typeof value === "object") {
    if (typeof value.valueOf === "function") {
      const primitive = value.valueOf();
      if (primitive !== value && (typeof primitive !== "object" || primitive instanceof Date)) {
        return normalizeCell(primitive);
      }
    }
  }
  return value;
}
