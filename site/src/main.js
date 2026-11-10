import { CONSUMER, GROUP, StandIn } from "./broker.js";
import { buildEvents, eventsToJsonl, expectedCounts } from "./generate.js";
import { applyI18n, getLang, t, toggleLang } from "./i18n.js";
import { PRESETS } from "./queries.js";
import { runQuality } from "./quality.js";
import { invalidEnvelope } from "./validate.js";
import { prepareUserSql, sqlLiteral } from "./warehouse.js";

const RELATIONS = [
  ["warehouse.events", "grainEvents"],
  ["warehouse.orders", "grainOrders"],
  ["warehouse.inventory_scans", "grainInventory"],
  ["warehouse.shipment_scans", "grainShipments"],
  ["warehouse.rejects", "grainRejects"],
  ["warehouse.ingest_batches", "grainBatches"],
  ["warehouse.stream_state", "grainState"],
  ["mart.hourly_corridor", "grainMart"],
];

const state = {
  session: null,
  standin: null,
  orders: 120,
  seed: 42,
  batchSize: 50,
  running: false,
  paused: false,
  pauseWaiters: [],
  lastDeliveries: [],
  tape: [],
  census: null,
  watermark: null,
  batches: [],
  events: [],
  mart: [],
  hours: [],
  checks: null,
  lineage: [],
  result: null,
  statusKey: "statusIdle",
  statusVars: {},
  passInserted: 0,
  passDuplicates: 0,
  bytesTotal: 0,
};

const $ = (id) => document.getElementById(id);

function readInputs() {
  const orders = Number($("orders").value);
  const seed = Number($("seed").value);
  const batchSize = Number($("batch").value);
  if (!Number.isInteger(orders) || orders < 1 || orders > 200) return { error: "inputOrders" };
  if (!Number.isInteger(seed) || seed < 0 || seed > 999999) return { error: "inputSeed" };
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 200) return { error: "inputBatch" };
  return { orders, seed, batchSize };
}

function setStatus(key, vars = {}) {
  state.statusKey = key;
  state.statusVars = vars;
  $("status").textContent = t(key, vars);
}

function setProgress(value, max) {
  const bar = $("progress");
  bar.max = max || 1;
  bar.value = value;
}

function updateButtons() {
  const ready = Boolean(state.session) && !state.running;
  $("run").disabled = state.running;
  $("pause").disabled = !state.running;
  $("pause").dataset.i18n = state.paused ? "resume" : "pause";
  $("pause").textContent = t($("pause").dataset.i18n);
  $("replay").disabled = !ready || !state.standin;
  $("recommit").disabled = !ready;
  $("redeliver").disabled = !ready || state.lastDeliveries.length === 0;
  $("poison").disabled = !ready;
  $("query").disabled = state.running || !state.session;
  $("lineage-btn").disabled = state.running || !state.session;
  for (const input of document.querySelectorAll("#controls input")) input.disabled = state.running;
}

async function gate(ms) {
  while (state.paused) {
    setStatus("paused");
    await new Promise((resolve) => state.pauseWaiters.push(resolve));
  }
  await new Promise((resolve) => requestAnimationFrame(() => resolve()));
  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (!reduced && ms) await new Promise((resolve) => setTimeout(resolve, ms));
}

function renderPreview() {
  const inputs = readInputs();
  if (inputs.error) {
    $("formula").textContent = t(inputs.error);
    return;
  }
  const counts = expectedCounts(inputs.orders);
  const events = buildEvents(inputs);
  const first = events[0];
  const amount = first.payload.amount_centavos;
  $("formula").textContent = t("formula", {
    orders: inputs.orders,
    seed: inputs.seed,
    published: counts.published,
    unique: counts.unique_events,
    replays: counts.replays,
    late: counts.late_events,
  });
  $("sample").textContent = t("sample", {
    id: first.event_id,
    amount,
    mxn: (amount / 100).toFixed(2),
    customer: first.payload.customer_ref,
    fulfillment: first.payload.fulfillment,
    city: first.payload.city,
  });
  if (!state.running && !state.session) {
    state.tape = events.slice(0, 8).map((event) => ({
      offset: "-",
      event_id: event.event_id,
      event_type: event.event_type,
      event_time: event.event_time,
      status: "generated",
    }));
    renderTape();
    $("broker-meta").textContent = t("brokerMeta", {
      log: 0,
      pending: 0,
      group: GROUP,
      consumer: CONSUMER,
    });
  }
}

function renderTape() {
  const body = $("tape").querySelector("tbody");
  body.replaceChildren();
  if (!state.tape.length) {
    body.append(emptyRow(5));
    return;
  }
  for (const row of state.tape) {
    body.append(tr([row.offset, row.event_id, row.event_type, row.event_time, t(row.status)], [0, 1, 3]));
  }
}

function renderCensus() {
  const body = $("census").querySelector("tbody");
  body.replaceChildren();
  const counts = state.census || {};
  for (const [name, grain] of RELATIONS) {
    const value = counts[name];
    body.append(tr([name, t(grain), value == null ? "-" : String(value)], [0], [1]));
  }
  const mark = state.watermark ? state.watermark : t("none");
  $("watermark").textContent = `${t("watermark")}: ${mark}`;
}

function renderEvents() {
  fillBody($("events"), state.events, (row) =>
    tr(
      [
        row.stream_offset,
        row.event_id,
        row.event_type,
        row.event_time,
        row.city,
        Number(row.late) ? t("lateYes") : t("lateNo"),
      ],
      [0, 1, 3],
    ),
  );
}

function renderBatches() {
  fillBody($("batches"), state.batches, (row, index) =>
    tr(
      [
        String(index + 1),
        row.started_at,
        row.received,
        row.inserted,
        row.duplicates,
        row.late_rows,
        row.rejected,
      ],
      [1],
    ),
  );
}

function renderMart() {
  const bars = $("bars");
  bars.replaceChildren();
  if (!state.hours.length) {
    const note = document.createElement("p");
    note.className = "note";
    note.textContent = t("martEmpty");
    bars.append(note);
  } else {
    const max = Math.max(...state.hours.map((row) => Number(row.event_count) || 0), 1);
    for (const row of state.hours) {
      const line = document.createElement("div");
      line.className = "bar-row";
      const label = document.createElement("span");
      label.className = "mono";
      label.textContent = displayHour(row.hour_start);
      const track = document.createElement("span");
      track.className = "bar-track";
      const fill = document.createElement("span");
      fill.className = "bar-fill";
      const count = Number(row.event_count) || 0;
      fill.style.width = `${Math.max(2, Math.round((100 * count) / max))}%`;
      track.append(fill);
      const value = document.createElement("span");
      const late = Number(row.late_count) || 0;
      value.textContent = late ? `${count} · ${t("lateSuffix", { n: late })}` : String(count);
      line.append(label, track, value);
      bars.append(line);
    }
  }
  fillBody($("mart"), state.mart, (row) =>
    tr(
      [row.hour_utc, row.event_type, row.city, row.event_count, row.late_count, row.amount_centavos],
      [0, 1],
    ),
  );
}

function renderChecks() {
  const summary = $("quality-summary");
  const body = $("checks").querySelector("tbody");
  body.replaceChildren();
  if (!state.checks) {
    summary.textContent = t("qualityEmpty");
    body.append(emptyRow(3));
    return;
  }
  const failed = state.checks.filter((row) => !row.passed).length;
  summary.textContent = failed
    ? t("qualitySome", { n: failed })
    : t("qualityAll", { n: state.checks.length });
  for (const row of state.checks) {
    const label = document.createElement("td");
    label.textContent = `${t(`check_${row.name}`)} `;
    const code = document.createElement("code");
    code.textContent = row.name;
    label.append(code);
    const result = document.createElement("td");
    result.textContent = row.passed ? t("pass") : t("fail");
    const detail = document.createElement("td");
    detail.className = "mono-cell";
    detail.textContent = row.detail;
    const trElement = document.createElement("tr");
    trElement.append(label, result, detail);
    body.append(trElement);
  }
}

function renderLineage() {
  fillBody($("lineage"), state.lineage, (row) =>
    tr(
      [
        row.stream_offset,
        row.event_id,
        row.event_type,
        row.scan_type,
        row.event_time,
        row.event_time_mx,
        row.business_date,
        Number(row.late) ? t("lateYes") : t("lateNo"),
      ],
      [0, 1, 4, 5],
    ),
  );
}

function renderResult() {
  const head = $("result").querySelector("thead");
  const body = $("result").querySelector("tbody");
  head.replaceChildren();
  body.replaceChildren();
  if (!state.result) return;
  const header = document.createElement("tr");
  for (const field of state.result.fields) {
    const cell = document.createElement("th");
    cell.textContent = field;
    header.append(cell);
  }
  head.append(header);
  if (!state.result.rows.length) {
    body.append(emptyRow(Math.max(1, state.result.fields.length), t("sqlZero")));
    return;
  }
  for (const row of state.result.rows) {
    body.append(tr(state.result.fields.map((field) => formatCell(row[field]))));
  }
}

function renderPresets() {
  const host = $("presets");
  host.replaceChildren();
  for (const preset of PRESETS) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "secondary";
    button.textContent = t(`preset_${preset.id}`);
    button.addEventListener("click", () => {
      $("sql").value = preset.sql;
      if (state.session && !state.running) runQuery();
    });
    host.append(button);
  }
}

function refreshStatic() {
  renderPreview();
  renderTape();
  renderCensus();
  renderEvents();
  renderBatches();
  renderMart();
  renderChecks();
  renderLineage();
  renderResult();
  renderPresets();
  $("status").textContent = t(state.statusKey, state.statusVars);
  if (state.session) {
    $("broker-meta").textContent = t("brokerMeta", {
      log: state.standin ? state.standin.logSize : 0,
      pending: state.standin ? state.standin.pendingCount : 0,
      group: GROUP,
      consumer: CONSUMER,
    });
  }
  updateButtons();
}

function tr(values, monoIndexes = [], optionalIndexes = []) {
  const row = document.createElement("tr");
  values.forEach((value, index) => {
    const cell = document.createElement("td");
    if (monoIndexes.includes(index)) cell.className = "mono-cell";
    if (optionalIndexes.includes(index)) cell.classList.add("optional");
    cell.textContent = value == null ? "" : String(value);
    row.append(cell);
  });
  return row;
}

function emptyRow(span, text = t("empty")) {
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = span;
  cell.textContent = text;
  row.append(cell);
  return row;
}

function fillBody(table, rows, build) {
  const body = table.querySelector("tbody");
  body.replaceChildren();
  if (!rows.length) {
    body.append(emptyRow(table.querySelectorAll("th").length));
    return;
  }
  rows.forEach((row, index) => body.append(build(row, index)));
}

function formatCell(value) {
  if (value == null) return "";
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function displayHour(value) {
  if (value == null) return "";
  const text = String(value).replace("T", " ").replace("Z", "");
  return text.slice(0, 16);
}

function showChunk(deliveries, status) {
  state.tape = deliveries.slice(-8).map((delivery) => ({
    offset: delivery.offset,
    event_id: delivery.event.event_id,
    event_type: delivery.event.event_type,
    event_time: delivery.event.event_time,
    status,
  }));
  renderTape();
  $("broker-meta").textContent = t("brokerMeta", {
    log: state.standin.logSize,
    pending: state.standin.pendingCount,
    group: GROUP,
    consumer: CONSUMER,
  });
}

async function ensureSession() {
  if (state.session) return state.session;
  setStatus("loading", { pct: 0 });
  const { WarehouseSession } = await import("./warehouse.js");
  const session = new WarehouseSession();
  await session.open((progress) => {
    state.bytesTotal = progress.bytesTotal || state.bytesTotal;
    setStatus("loading", { pct: progress.pct });
    setProgress(progress.pct, 100);
  });
  state.session = session;
  $("engine").textContent = t("engine", {
    version: session.version || "",
    bytes: state.bytesTotal || "",
  });
  return session;
}

async function refreshWarehouse() {
  const session = state.session;
  const census = await session.query(`
    SELECT * FROM (
      SELECT 1 AS ord, 'warehouse.events' AS relation, count(*)::BIGINT AS rows FROM warehouse.events
      UNION ALL
      SELECT 2, 'warehouse.orders', count(*)::BIGINT FROM warehouse.orders
      UNION ALL
      SELECT 3, 'warehouse.inventory_scans', count(*)::BIGINT FROM warehouse.inventory_scans
      UNION ALL
      SELECT 4, 'warehouse.shipment_scans', count(*)::BIGINT FROM warehouse.shipment_scans
      UNION ALL
      SELECT 5, 'warehouse.rejects', count(*)::BIGINT FROM warehouse.rejects
      UNION ALL
      SELECT 6, 'warehouse.ingest_batches', count(*)::BIGINT FROM warehouse.ingest_batches
      UNION ALL
      SELECT 7, 'warehouse.stream_state', count(*)::BIGINT FROM warehouse.stream_state
      UNION ALL
      SELECT 8, 'mart.hourly_corridor', count(*)::BIGINT FROM mart.hourly_corridor
    ) ORDER BY ord
  `);
  state.census = {};
  for (const row of census.rows) state.census[row.relation] = Number(row.rows);
  state.watermark = await session.scalar(
    "SELECT strftime(watermark, '%Y-%m-%dT%H:%M:%SZ') FROM warehouse.stream_state WHERE key = 'events'",
  );
  const events = await session.query(`
    SELECT stream_offset, event_id, event_type,
           strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
           coalesce(city, '') AS city,
           CASE WHEN late THEN 1 ELSE 0 END AS late
    FROM warehouse.events
    ORDER BY try_cast(stream_offset AS INTEGER) DESC
    LIMIT 8
  `);
  state.events = events.rows.reverse();
  const batches = await session.query(`
    SELECT strftime(started_at, '%H:%M:%S') AS started_at,
           received, inserted, duplicates, late_rows, rejected
    FROM warehouse.ingest_batches
    ORDER BY rowid
  `);
  state.batches = batches.rows;
  const mart = await session.query(`
    SELECT strftime(hour_start, '%Y-%m-%d %H:%M') AS hour_utc,
           event_type, city, event_count, late_count, amount_centavos
    FROM mart.hourly_corridor
    ORDER BY hour_start, event_type, city
  `);
  state.mart = mart.rows;
  const hours = await session.query(`
    SELECT hour_start,
           sum(event_count)::BIGINT AS event_count,
           sum(late_count)::BIGINT AS late_count
    FROM mart.hourly_corridor
    GROUP BY 1
    ORDER BY 1
  `);
  state.hours = hours.rows;
  renderCensus();
  renderEvents();
  renderBatches();
  renderMart();
}

async function publishAll(events) {
  const chunk = Math.max(state.batchSize, 1);
  for (let index = 0; index < events.length; index += chunk) {
    const slice = events.slice(index, index + chunk);
    const deliveries = slice.map((event) => ({ offset: state.standin.publish(event), event }));
    showChunk(deliveries, "published");
    setStatus("publishing", { log: state.standin.logSize });
    setProgress(Math.min(events.length, index + slice.length), events.length + events.length);
    await gate(40);
  }
}

async function consumeAll(totalPublished) {
  let batch = 0;
  const batches = Math.ceil(totalPublished / state.batchSize) || 1;
  state.passInserted = 0;
  state.passDuplicates = 0;
  for (;;) {
    const deliveries = state.standin.read(state.batchSize);
    if (!deliveries.length) break;
    batch += 1;
    state.lastDeliveries = deliveries;
    showChunk(deliveries, "pending");
    await gate(80);
    const result = await state.session.applyBatch(deliveries);
    state.standin.ack(deliveries.map((delivery) => delivery.offset));
    state.passInserted += result.stats.inserted;
    state.passDuplicates += result.stats.duplicates;
    state.tape = deliveries.slice(-8).map((delivery) => ({
      offset: delivery.offset,
      event_id: delivery.event.event_id,
      event_type: delivery.event.event_type,
      event_time: delivery.event.event_time,
      status: result.outcomes.get(delivery.offset) || "duplicate",
    }));
    renderTape();
    $("broker-meta").textContent = t("brokerMeta", {
      log: state.standin.logSize,
      pending: state.standin.pendingCount,
      group: GROUP,
      consumer: CONSUMER,
    });
    setStatus("consuming", {
      batch,
      total: batches,
      received: result.stats.received,
      inserted: result.stats.inserted,
      duplicates: result.stats.duplicates,
      late: result.stats.lateRows,
      rejected: result.stats.rejected,
    });
    await refreshWarehouse();
    setProgress(totalPublished + Math.min(totalPublished, batch * state.batchSize), totalPublished * 2);
    await gate(180);
  }
}

async function finishPass(replay) {
  setStatus("compacting");
  const mart = await state.session.compact();
  await refreshWarehouse();
  setStatus("checking");
  state.checks = await runQuality((sql) => state.session.scalar(sql));
  renderChecks();
  await loadLineage();
  const rows = state.census["warehouse.events"];
  const late = Number(
    await state.session.scalar("SELECT count(*)::BIGINT FROM warehouse.events WHERE late"),
  );
  const summary = {
    rows,
    late,
    mart: Number(mart.mart_rows),
    martEvents: Number(mart.mart_event_count),
    duplicates: state.passDuplicates,
    inserted: state.passInserted,
  };
  if (replay) {
    setStatus("replayDone", summary);
  } else {
    setStatus("done", summary);
  }
  const kept = await state.session.scalar(
    "SELECT stream_offset || ' @ ' || strftime(ingested_at, '%Y-%m-%dT%H:%M:%SZ') FROM warehouse.events WHERE event_id = 'evt-ord-00000'",
  );
  if (state.orders === 120 && state.seed === 42) {
    const same = rows === 684 && late === 3 && Number(mart.mart_rows) === 53;
    $("match").textContent = same
      ? `${t("match")} ${kept ? t("kept", keptParts(kept)) : ""}`
      : t("differ", { rows, late, mart: mart.mart_rows });
  } else {
    $("match").textContent = t("otherSize", { orders: state.orders, seed: state.seed });
  }
  setProgress(1, 1);
}

function keptParts(text) {
  const [offset, ingested] = String(text).split(" @ ");
  return { offset: offset || "", ingested: ingested || "" };
}

async function runPipeline(fresh) {
  const inputs = readInputs();
  if (inputs.error) {
    setStatus(inputs.error);
    return;
  }
  if (state.running) return;
  state.running = true;
  state.paused = false;
  updateButtons();
  try {
    const session = await ensureSession();
    if (fresh) {
      await session.reset();
      state.standin = new StandIn();
      state.checks = null;
      state.census = null;
      state.events = [];
      state.batches = [];
      state.mart = [];
      state.hours = [];
      state.lineage = [];
      state.lastDeliveries = [];
      renderChecks();
      renderLineage();
      renderCensus();
      renderEvents();
      renderBatches();
      renderMart();
      state.orders = inputs.orders;
      state.seed = inputs.seed;
      state.batchSize = inputs.batchSize;
    } else {
      $("orders").value = String(state.orders);
      $("seed").value = String(state.seed);
      $("batch").value = String(state.batchSize);
    }
    const events = buildEvents({ orders: state.orders, seed: state.seed });
    await publishAll(events);
    await consumeAll(events.length);
    await finishPass(!fresh);
  } catch (error) {
    setStatus("failed", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    state.running = false;
    state.paused = false;
    const waiters = state.pauseWaiters;
    state.pauseWaiters = [];
    for (const resolve of waiters) resolve();
    updateButtons();
  }
}

async function redeliver() {
  if (!state.session || state.running || !state.lastDeliveries.length) return;
  state.running = true;
  updateButtons();
  try {
    showChunk(state.lastDeliveries, "pending");
    const result = await state.session.applyBatch(state.lastDeliveries);
    state.tape = state.lastDeliveries.slice(-8).map((delivery) => ({
      offset: delivery.offset,
      event_id: delivery.event.event_id,
      event_type: delivery.event.event_type,
      event_time: delivery.event.event_time,
      status: result.outcomes.get(delivery.offset) || "duplicate",
    }));
    renderTape();
    await refreshWarehouse();
    state.checks = await runQuality((sql) => state.session.scalar(sql));
    renderChecks();
    const kept = await state.session.scalar(
      "SELECT stream_offset || ' @ ' || strftime(ingested_at, '%Y-%m-%dT%H:%M:%SZ') FROM warehouse.events WHERE event_id = 'evt-ord-00000'",
    );
    setStatus("redeliverDone", {
      inserted: result.stats.inserted,
      duplicates: result.stats.duplicates,
    });
    if (kept) $("match").textContent = t("kept", keptParts(kept));
  } catch (error) {
    setStatus("failed", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    state.running = false;
    updateButtons();
  }
}

async function poison() {
  if (!state.session || state.running) return;
  state.running = true;
  updateButtons();
  try {
    const event = invalidEnvelope();
    const offset = state.standin.publish(event);
    const deliveries = state.standin.read(1);
    const result = await state.session.applyBatch(deliveries.length ? deliveries : [{ offset, event }]);
    state.standin.ack((deliveries.length ? deliveries : [{ offset }]).map((item) => item.offset));
    state.lastDeliveries = deliveries.length ? deliveries : [{ offset, event }];
    showChunk(state.lastDeliveries, result.outcomes.get(offset) || "rejected");
    await refreshWarehouse();
    const reason = await state.session.scalar(
      `SELECT reason FROM warehouse.rejects WHERE stream_offset = ${sqlLiteral(offset)}`,
    );
    setStatus("poisonDone", {
      reason: reason || "",
      rejects: state.census["warehouse.rejects"],
    });
    state.checks = await runQuality((sql) => state.session.scalar(sql));
    renderChecks();
  } catch (error) {
    setStatus("failed", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    state.running = false;
    updateButtons();
  }
}

async function compactAgain() {
  if (!state.session || state.running) return;
  state.running = true;
  updateButtons();
  try {
    setStatus("compacting");
    const mart = await state.session.compact();
    await refreshWarehouse();
    state.checks = await runQuality((sql) => state.session.scalar(sql));
    renderChecks();
    setStatus("compactAgain", {
      mart: Number(mart.mart_rows),
      martEvents: Number(mart.mart_event_count),
      late: Number(mart.mart_late_count),
    });
  } catch (error) {
    setStatus("failed", { message: error instanceof Error ? error.message : String(error) });
  } finally {
    state.running = false;
    updateButtons();
  }
}

async function runQuery() {
  const prepared = prepareUserSql($("sql").value);
  if (prepared.error) {
    $("query-meta").textContent = t(`sql_${prepared.error}`);
    return;
  }
  if (!state.session) {
    $("query-meta").textContent = t("sqlNeedRun");
    return;
  }
  try {
    const { fields, rows } = await state.session.query(prepared.sql);
    const shown = rows.slice(0, 500);
    state.result = { fields, rows: shown, total: rows.length };
    renderResult();
    $("query-meta").textContent = t("sqlMeta", { shown: shown.length, total: rows.length });
  } catch (error) {
    $("query-meta").textContent = error instanceof Error ? error.message : String(error);
  }
}

async function loadLineage() {
  if (!state.session) return;
  const orderId = $("order-id").value.trim();
  if (!/^[A-Za-z0-9-]{1,64}$/.test(orderId)) {
    $("lineage-note").textContent = t("lineageBad");
    return;
  }
  const { rows } = await state.session.query(`
    SELECT stream_offset, event_id, event_type, coalesce(scan_type, '') AS scan_type,
           strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
           strftime(event_time - INTERVAL 6 HOUR, '%Y-%m-%dT%H:%M:%SZ') AS event_time_mx,
           strftime(event_time - INTERVAL 6 HOUR, '%Y-%m-%d') AS business_date,
           CASE WHEN late THEN 1 ELSE 0 END AS late
    FROM warehouse.events
    WHERE order_id = ${sqlLiteral(orderId)}
    ORDER BY try_cast(stream_offset AS INTEGER)
  `);
  state.lineage = rows;
  renderLineage();
  const late = rows.find((row) => Number(row.late));
  $("lineage-note").textContent = rows.length
    ? late
      ? t("lineageLate", { id: late.event_id, date: late.business_date })
      : ""
    : t("lineageEmpty");
}

function downloadJsonl() {
  const inputs = readInputs();
  if (inputs.error) {
    setStatus(inputs.error);
    return;
  }
  const text = eventsToJsonl(buildEvents(inputs));
  const blob = new Blob([text], { type: "application/x-ndjson" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `andensur-orders${inputs.orders}-seed${inputs.seed}.jsonl`;
  link.click();
  URL.revokeObjectURL(url);
}

let mermaidModule = null;
let mermaidSeq = 0;

async function drawDiagrams() {
  const flow = $("diagram-flow");
  const ack = $("diagram-ack");
  try {
    if (!mermaidModule) {
      mermaidModule = (await import("mermaid")).default;
      mermaidModule.initialize({
        startOnLoad: false,
        securityLevel: "strict",
        theme: "base",
        fontFamily: '"Libre Franklin", sans-serif',
        flowchart: { curve: "linear", htmlLabels: false, padding: 8 },
        themeVariables: {
          fontFamily: '"Libre Franklin", sans-serif',
          background: "#f6f7f4",
          primaryColor: "#f6f7f4",
          primaryTextColor: "#14211b",
          primaryBorderColor: "#0B5C38",
          lineColor: "#14211b",
          secondaryColor: "#f6f7f4",
          tertiaryColor: "#f6f7f4",
          mainBkg: "#f6f7f4",
          textColor: "#14211b",
          nodeTextColor: "#14211b",
        },
      });
    }
    const lang = getLang();
    const spec = (await import("./i18n.js")).STR[lang];
    flow.innerHTML = (await mermaidModule.render(`flow-${mermaidSeq++}`, spec.flow)).svg;
    ack.innerHTML = (await mermaidModule.render(`ack-${mermaidSeq++}`, spec.ack)).svg;
    flow.setAttribute("aria-label", t("archCaption"));
    ack.setAttribute("aria-label", t("ackCaption"));
  } catch (error) {
    flow.textContent = error instanceof Error ? error.message : String(error);
  }
}

function bind() {
  applyI18n();
  $("sql").value = PRESETS[0].sql;
  renderPreview();
  renderCensus();
  renderEvents();
  renderBatches();
  renderMart();
  renderChecks();
  renderPresets();
  setStatus("statusIdle");
  updateButtons();
  drawDiagrams();

  $("lang").addEventListener("click", () => {
    toggleLang();
    refreshStatic();
    drawDiagrams();
  });
  for (const id of ["orders", "seed", "batch"]) {
    $(id).addEventListener("input", () => {
      if (!state.running) renderPreview();
    });
  }
  $("controls").addEventListener("submit", (event) => {
    event.preventDefault();
    runPipeline(true);
  });
  $("pause").addEventListener("click", () => {
    state.paused = !state.paused;
    if (!state.paused) {
      const waiters = state.pauseWaiters;
      state.pauseWaiters = [];
      for (const resolve of waiters) resolve();
    }
    updateButtons();
  });
  $("replay").addEventListener("click", () => runPipeline(false));
  $("recommit").addEventListener("click", compactAgain);
  $("redeliver").addEventListener("click", redeliver);
  $("poison").addEventListener("click", poison);
  $("download").addEventListener("click", downloadJsonl);
  $("query").addEventListener("click", runQuery);
  $("sql").addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      runQuery();
    }
  });
  $("lineage-form").addEventListener("submit", (event) => {
    event.preventDefault();
    loadLineage().catch((error) => {
      $("lineage-note").textContent = error instanceof Error ? error.message : String(error);
    });
  });
}

bind();
