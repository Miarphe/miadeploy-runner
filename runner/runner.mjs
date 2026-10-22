import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { pathToFileURL } from "node:url";

const HUB = (process.env.HUB_URL || "").replace(/\/$/, "");
const TOKEN = process.env.RUNNER_TOKEN || "";
const TAGS = (process.env.RUNNER_TAGS || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
const SLOTS = Math.max(1, Math.min(8, Number(process.env.RUNNER_SLOTS) || 3));
const KIND = process.env.RUNNER_KIND || "vps";
const GHA = KIND === "gha";
const MAX_MS = (Number(process.env.RUNNER_MAX_MINUTES) || (GHA ? 330 : 0)) * 60000;
const EVENT = process.env.GITHUB_EVENT_NAME || "";
const ID = KIND + "-" + Math.random().toString(36).slice(2, 8);

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
  let buf = "";
  const out = (...a) => {
    const line = a.map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join(" ");
    buf = (buf + line + "\n").slice(-7000);
    console.log("[" + job.name + "]", line);
  };
  const beat = setInterval(async () => {
    try { const r = await hub("beat", { job: job.id, log: buf }, 15000); if (r.cancel) ac.abort(); } catch (e) { if (e.status === 401) ac.abort(); }
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
  } finally { clearInterval(beat); }
  for (let i = 0; i < 4; i++) {
    try { await hub("done", { job: job.id, log: buf, ...res }); break; } catch (e) { if (i === 3) log("could not report", job.id, e.message); else await sleep(3000 * (i + 1)); }
  }
}

let cfg = { keepAlive: false, idleSeconds: 240 };
for (let i = 0; ; i++) {
  try { cfg = await hub("hello", { tags: TAGS, kind: KIND, slots: SLOTS, event: EVENT }); break; } catch (e) {
    if (e.status === 401) { log("The runner token was rejected."); process.exit(3); }
    if (i >= 5) { log("Hub unreachable:", e.message); process.exit(1); }
    await sleep(4000 * (i + 1));
  }
}
log("runner", ID, "online", KIND, "slots", SLOTS, "tags", TAGS.join(",") || "none", "keepAlive", cfg.keepAlive);

let running = 0;
let stopping = false;
let lastBusy = Date.now();
let lastTick = 0;
const startedAt = Date.now();

while (true) {
  if (!stopping && MAX_MS && Date.now() - startedAt > MAX_MS) { stopping = true; log("lifetime reached, draining"); }
  if (stopping && running === 0) break;
  if (GHA && !cfg.keepAlive && running === 0 && Date.now() - lastBusy > cfg.idleSeconds * 1000) { log("idle, exiting"); break; }
  if (Date.now() - lastTick > 240000) { lastTick = Date.now(); hub("tick", {}, 320000).catch(() => {}); }
  if (stopping || running >= SLOTS) { await sleep(1000); continue; }
  let c;
  try { c = await hub("claim", { tags: TAGS, wait: 20000, busy: running }, 40000); } catch (e) {
    if (e.status === 401) { log("The runner token was rejected."); process.exit(3); }
    log("claim error:", e.message);
    await sleep(5000);
    continue;
  }
  if (!c.job) continue;
  lastBusy = Date.now();
  running++;
  runJob(c).finally(() => { running--; lastBusy = Date.now(); });
}
process.exit(0);
