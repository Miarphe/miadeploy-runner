import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { pathToFileURL } from "node:url";

const HUBS = (process.env.HUB_URL || "").split(/[\s,]+/).map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean);
let HUB = HUBS[0] || "";
const TOKEN = process.env.RUNNER_TOKEN || "";
const TAGS = (process.env.RUNNER_TAGS || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
const SLOTS = Math.max(1, Math.min(32, Number(process.env.RUNNER_SLOTS) || 3));
const SLOT = Math.max(0, Math.min(99, Number(process.env.RUNNER_SLOT) || 0));
const FLEET = SLOT > 0;
const KIND = process.env.RUNNER_KIND || "vps";
const GHA = KIND === "gha";
const MAX_MS = (Number(process.env.RUNNER_MAX_MINUTES) || (GHA ? 330 : 0)) * 60000;
const EVENT = process.env.GITHUB_EVENT_NAME || "";
const ID = KIND + (FLEET ? "-s" + SLOT : "") + "-" + Math.random().toString(36).slice(2, 8);
const STARTED_FILE = "/tmp/mia_started";
const NORESPAWN_FILE = "/tmp/mia_norespawn";
const flag = (f) => { try { writeFileSync(f, String(Date.now())); } catch {} };
let SHUTDOWN = false;
let staleSeen = false;
const active = new Map();
function abortAll() { SHUTDOWN = true; for (const a of active.values()) a.abort(); }

if (!HUB || !TOKEN) { console.error("HUB_URL and RUNNER_TOKEN are required."); process.exit(2); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString(), ...a);

async function hub(op, body = {}, ms = 30000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    const r = await fetch(HUB + "/api/runner/" + op, { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + TOKEN }, body: JSON.stringify({ id: ID, ...body }), signal: ac.signal });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(d.error || "HTTP " + r.status); e.status = r.status; throw e; }
    return d;
  } finally { clearTimeout(t); }
}

function makeMia(base, key) {
  const call = async (cap, op, args = {}) => {
    const r = await fetch(base + "/api/platform/call", { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + key }, body: JSON.stringify({ cap, op, args }) });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || "Mia call failed (" + r.status + ")");
    return d.result;
  };
  return {
    call,
    db: { query: (sql, args = []) => call("db", "query", { sql, args }), exec: (sql, args = []) => call("db", "exec", { sql, args }), batch: (stmts) => call("db", "batch", { stmts }) },
    files: { put: (path, data, type = "") => call("files", "put", { path, data, type }), get: (path) => call("files", "get", { path }), list: (prefix = "") => call("files", "list", { prefix }), del: (path) => call("files", "del", { path }) },
    jobs: {
      run: (name, payload = {}, opts = {}) => call("jobs", "run", { name, payload, ...opts }),
      status: (id) => call("jobs", "status", { id }),
      cancel: (id) => call("jobs", "cancel", { id }),
      list: (opts = {}) => call("jobs", "list", opts),
    },
    secrets: { get: (name) => call("secrets", "get", { name }) },
    notify: (text, title = "") => call("notify", "send", { text, title }),
    llm: { chat: (messages, opts = {}) => call("llm", "chat", { messages, ...opts }), ask: (prompt, opts = {}) => call("llm", "ask", { prompt, ...opts }) },
  };
}

const caches = new Map();

function prepare(c) {
  const dir = join(tmpdir(), "mia-bundles", c.version || "none");
  if (caches.has(dir)) return caches.get(dir);
  const p = (async () => {
    if (existsSync(join(dir, ".ok"))) return dir;
    const b = await hub("bundle", { job: c.job.id });
    mkdirSync(dir, { recursive: true });
    for (const [path, text] of Object.entries(b.files || {})) {
      if (path.includes("..")) continue;
      const f = join(dir, path);
      mkdirSync(dirname(f), { recursive: true });
      writeFileSync(f, text);
    }
    if (existsSync(join(dir, "jobs", "package.json"))) {
      await new Promise((res, rej) => execFile("npm", ["install", "--omit=dev", "--no-audit", "--no-fund", "--prefix", join(dir, "jobs")], { timeout: 300000 }, (e, so, se) => (e ? rej(new Error("npm install failed: " + String(se || e.message).slice(-400))) : res())));
    }
    writeFileSync(join(dir, ".ok"), "1");
    return dir;
  })();
  caches.set(dir, p);
  p.catch(() => caches.delete(dir));
  return p;
}

async function runJob(c) {
  const job = c.job;
  const ac = new AbortController();
  active.set(job.id, ac);
  let buf = "";
  const out = (...a) => {
    const line = a.map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join(" ");
    buf = (buf + line + "\n").slice(-7000);
    console.log("[" + job.name + "]", line);
  };
  const beat = setInterval(async () => {
    try { const r = await hub("beat", { job: job.id, log: buf }, 15000); if (r.stale) staleSeen = true; if (r.cancel) ac.abort(); } catch (e) { if (e.status === 401) ac.abort(); }
  }, 20000);
  let res;
  try {
    const dir = await prepare(c);
    const file = join(dir, "jobs", job.name + ".mjs");
    if (!existsSync(file)) throw new Error("jobs/" + job.name + ".mjs was not found in the deployed zip.");
    const mod = await import(pathToFileURL(file).href);
    const fn = mod.default || mod.run;
    if (typeof fn !== "function") throw new Error("jobs/" + job.name + ".mjs must export a default function.");
    const mia = makeMia(HUB, c.key);
    const work = Promise.resolve(fn(job.payload, { mia, log: out, signal: ac.signal, job: { id: job.id, name: job.name, attempt: job.attempts, project: c.project } }));
    const timer = new Promise((_, rej) => setTimeout(() => { rej(new Error("Timed out after " + job.timeout_s + "s.")); ac.abort(); }, job.timeout_s * 1000));
    const stop = new Promise((_, rej) => ac.signal.addEventListener("abort", () => rej(new Error("Canceled.")), { once: true }));
    const result = await Promise.race([work, timer, stop]);
    res = { ok: true, result: result === undefined ? null : result };
  } catch (e) {
    res = { ok: false, error: String(e && e.stack ? e.stack : e).slice(0, 4000) };
  } finally { clearInterval(beat); active.delete(job.id); }
  if (SHUTDOWN && !res.ok) {
    for (let i = 0; i < 3; i++) { try { await hub("release", { job: job.id }, 8000); break; } catch { await sleep(800); } }
    return;
  }
  for (let i = 0; i < 4; i++) {
    try { await hub("done", { job: job.id, log: buf, ...res }); break; } catch (e) { if (i === 3) log("could not report", job.id, e.message); else await sleep(3000 * (i + 1)); }
  }
}

let cfg = null;
let lastErr = "";
const bootEnd = Date.now() + 20 * 60000;
for (let round = 0; !cfg && (FLEET ? Date.now() < bootEnd : round < 4); round++) {
  let rejected = 0;
  for (const h of HUBS) {
    HUB = h;
    try { cfg = await hub("hello", { tags: TAGS, kind: KIND, slots: SLOTS, event: EVENT, slot: SLOT, epoch: Number(process.env.RUNNER_EPOCH) || 0 }, 25000); break; } catch (e) {
      lastErr = h + " " + e.message;
      if (e.status === 401) rejected++; else log("hub", h, "is not answering:", e.message);
    }
  }
  if (!cfg && rejected === HUBS.length) { log("The runner token was rejected."); if (FLEET) flag(NORESPAWN_FILE); process.exit(3); }
  if (!cfg && (FLEET || round < 3)) await sleep(FLEET ? Math.min(60000, 5000 * (round + 1)) : 4000 * (round + 1));
}
if (!cfg) { log("Hub unreachable:", lastErr); process.exit(1); }
if (cfg.stale) { log("This start was replaced by a newer one, exiting."); flag(NORESPAWN_FILE); process.exit(0); }
if (cfg.reconcile) { log("Reconcile run, the hub restarted any missing slot."); process.exit(0); }
if (HUBS.length > 1) log("using hub", HUB);
const startedAt = Date.now();
const DEADLINE = FLEET ? Number(cfg.deadline) || startedAt + 300 * 60000 : 0;
const HARD = FLEET ? Math.min(DEADLINE + 15 * 60000, startedAt + 345 * 60000) : 0;
flag(STARTED_FILE);
log("runner", ID, "online", KIND, "slots", SLOTS, "tags", TAGS.join(",") || "none", "keepAlive", cfg.keepAlive, FLEET ? "slot " + SLOT + " until " + new Date(DEADLINE).toISOString() : "");

let running = 0;
let stopping = false;
let lastBusy = Date.now();
let lastTick = 0;

async function shutdown(why) {
  if (SHUTDOWN) return;
  stopping = true;
  log(why);
  abortAll();
  const t = Date.now();
  while (running > 0 && Date.now() - t < 6000) await sleep(200);
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM received, releasing jobs"));
process.on("SIGINT", () => shutdown("SIGINT received, releasing jobs"));

while (true) {
  const now = Date.now();
  if (staleSeen && !SHUTDOWN) { flag(NORESPAWN_FILE); await shutdown("This start was replaced by a newer one, releasing jobs."); }
  if (!stopping && FLEET && now >= DEADLINE) { stopping = true; log("lifetime reached, draining"); }
  else if (!stopping && !FLEET && MAX_MS && now - startedAt > MAX_MS) { stopping = true; log("lifetime reached, draining"); }
  if (stopping && running === 0) break;
  if (stopping && FLEET && !SHUTDOWN && now >= HARD) { log("drain limit reached, releasing the rest"); abortAll(); }
  if (!FLEET && GHA && !cfg.keepAlive && running === 0 && now - lastBusy > cfg.idleSeconds * 1000) { log("idle, exiting"); break; }
  if (now - lastTick > (FLEET ? 60000 : 240000)) { lastTick = now; hub("tick", {}, 320000).then((r) => { if (r && r.stale) staleSeen = true; }).catch(() => {}); }
  if (stopping || running >= SLOTS) { await sleep(1000); continue; }
  let c;
  try { c = await hub("claim", { tags: TAGS, wait: 20000, busy: running }, 40000); } catch (e) {
    if (e.status === 401) { if (FLEET) { log("The runner token was rejected, ending this start."); stopping = true; continue; } log("The runner token was rejected."); process.exit(3); }
    log("claim error:", e.message);
    await sleep(5000);
    continue;
  }
  if (c.stale) { staleSeen = true; continue; }
  if (!c.job) continue;
  lastBusy = Date.now();
  running++;
  runJob(c).finally(() => { running--; lastBusy = Date.now(); });
}
process.exit(0);
