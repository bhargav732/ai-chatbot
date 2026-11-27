// SyncBid AI - conversational bid-management assistant.
// Plain Node.js HTTP server: no external packages, so it deploys anywhere (Render, Railway, local).
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

// Load .env for local development (Render sets real environment variables).
const envFile = path.join(__dirname, ".env");
if (fs.existsSync(envFile)) {
  for (const line of fs.readFileSync(envFile, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

const T = require("./lib/tenders");
const P = require("./lib/prompts");
const gemini = require("./lib/gemini");
const demo = require("./lib/demo");

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MAX_UPLOAD = 10 * 1024 * 1024;          // 10 MB PDF
const DOC_TTL = 2 * 60 * 60 * 1000;           // uploaded docs kept 2 hours in memory
const MIME = { ".html": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".json": "application/json", ".webmanifest": "application/manifest+json" };

T.sync();                                      // initial "sync" of sample data
const documents = new Map();                   // docId -> { name, mimeType, data, size, at }
const hits = new Map();                        // ip -> [timestamps] for rate limiting

// ------------------------------------------------------------------ helpers
function send(res, status, body, headers = {}) {
  const isObj = typeof body === "object" && !Buffer.isBuffer(body);
  res.writeHead(status, {
    "Content-Type": isObj ? "application/json; charset=utf-8" : headers["Content-Type"] || "text/plain; charset=utf-8",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
    ...headers,
  });
  res.end(isObj ? JSON.stringify(body) : body);
}

function readJson(req, limit = 200 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > limit) { reject(Object.assign(new Error("Request too large"), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on("end", () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {}); }
      catch { reject(Object.assign(new Error("Invalid JSON"), { status: 400 })); }
    });
    req.on("error", reject);
  });
}

function rateLimited(req, max = 20, windowMs = 60_000) {
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
  const now = Date.now();
  const list = (hits.get(ip) || []).filter((t) => now - t < windowMs);
  list.push(now); hits.set(ip, list);
  return list.length > max;
}

function cleanProfile(p = {}) {
  const s = (v, n = 200) => String(v || "").slice(0, n);
  return {
    companyName: s(p.companyName, 120), avgTurnover: Number(p.avgTurnover) || 0,
    experienceYears: Number(p.experienceYears) || 0, similarWork: s(p.similarWork, 400),
    certifications: Array.isArray(p.certifications) ? p.certifications.slice(0, 20).map((c) => s(c, 80)) : [],
    isMSE: Boolean(p.isMSE), isStartup: Boolean(p.isStartup), hasOEMAuth: Boolean(p.hasOEMAuth),
  };
}

function serveStatic(req, res, urlPath) {
  const rel = urlPath === "/" ? "index.html" : decodeURIComponent(urlPath).replace(/^\/+/, "");
  const file = path.normalize(path.join(PUBLIC, rel));
  if (!file.startsWith(PUBLIC)) return send(res, 403, "Forbidden");
  fs.readFile(file, (err, data) => {
    if (err) return send(res, 404, "Not found");
    send(res, 200, data, {
      "Content-Type": MIME[path.extname(file)] || "application/octet-stream",
      "Cache-Control": "no-cache",            // always revalidate, so a redeploy shows up immediately
    });
  });
}

// ------------------------------------------------------------------ chat (Server-Sent Events)
async function handleChat(req, res) {
  if (rateLimited(req)) return send(res, 429, { error: "Too many messages. Please wait a minute." });
  const body = await readJson(req);
  const message = String(body.message || "").trim().slice(0, 4000);
  if (!message) return send(res, 400, { error: "Message is empty." });

  const tender = body.tenderId ? T.get(body.tenderId) : null;
  const profile = cleanProfile(body.profile);
  const history = Array.isArray(body.history) ? body.history.filter((h) => h && h.text).slice(-12) : [];
  const doc = body.docId ? documents.get(body.docId) : null;

  res.writeHead(200, { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache", Connection: "keep-alive", "X-Accel-Buffering": "no" });
  const event = (name, data) => res.write(`event: ${name}\ndata: ${JSON.stringify(data)}\n\n`);
  const controller = new AbortController();
  res.on("close", () => controller.abort());
  const started = Date.now();

  if (!gemini.isConfigured()) {
    const { text, intent } = demo.respond({ message, tender, profile, hasDocument: Boolean(doc) });
    event("meta", { mode: "demo", intent });
    for (const piece of text.match(/[\s\S]{1,24}/g) || []) {       // stream in small chunks like the real API
      event("delta", { text: piece });
      await new Promise((r) => setTimeout(r, 8));
    }
    event("done", { ms: Date.now() - started });
    return res.end();
  }

  try {
    const system = P.buildSystemInstruction({ tender, profile, hasDocument: Boolean(doc) });
    event("meta", { mode: "gemini", model: gemini.getModel() });
    const out = await gemini.streamChat({
      system, history, message, signal: controller.signal,
      attachment: doc ? { mimeType: doc.mimeType, data: doc.data } : null,
      onText: (t) => event("delta", { text: t }),
    });
    event("done", { ms: Date.now() - started, model: out.model, usage: out.usage });
  } catch (err) {
    if (controller.signal.aborted) return;
    console.error("[chat]", err.message);
    const hint = err.status === 400 || err.status === 403 ? " Check that GEMINI_API_KEY is valid." :
      err.status === 429 ? " The free-tier limit was reached; wait a minute and try again." : "";
    event("error", { error: err.message + hint });
  }
  res.end();
}

// ------------------------------------------------------------------ router
async function route(req, res) {
  const url = new URL(req.url, "http://localhost");
  const p = url.pathname;
  const m = req.method;

  if (m === "GET" && p === "/api/health")
    return send(res, 200, { ok: true, mode: gemini.isConfigured() ? "gemini" : "demo", model: gemini.isConfigured() ? gemini.getModel() : null, uptime: Math.round(process.uptime()) });

  if (m === "GET" && p === "/api/config")
    return send(res, 200, { mode: gemini.isConfigured() ? "gemini" : "demo", portals: T.PORTALS, quickActions: P.QUICK_ACTIONS });

  if (m === "GET" && p === "/api/tenders")
    return send(res, 200, { lastSync: T.getLastSync(), tenders: T.list({ portal: url.searchParams.get("portal") || "", q: url.searchParams.get("q") || "", status: url.searchParams.get("status") || "" }) });

  let match = p.match(/^\/api\/tenders\/([A-Za-z0-9-]+)$/);
  if (m === "GET" && match) {
    const t = T.get(match[1]);
    if (!t) return send(res, 404, { error: "Tender not found" });
    return send(res, 200, { tender: t, keyDates: T.keyDates(t), pricing: T.comparePricing(t) });
  }

  match = p.match(/^\/api\/tenders\/([A-Za-z0-9-]+)\/eligibility$/);
  if (m === "POST" && match) {
    const t = T.get(match[1]);
    if (!t) return send(res, 404, { error: "Tender not found" });
    const body = await readJson(req);
    return send(res, 200, T.checkEligibility(t, cleanProfile(body.profile)));
  }

  if (m === "POST" && p === "/api/sync") {
    const body = await readJson(req);
    const portals = (Array.isArray(body.portals) ? body.portals : T.PORTALS).filter((x) => T.PORTALS.includes(x));
    await new Promise((r) => setTimeout(r, 900));   // simulate network fetch
    return send(res, 200, T.sync(portals.length ? portals : T.PORTALS));
  }

  if (m === "POST" && p === "/api/upload") {
    const body = await readJson(req, Math.ceil(MAX_UPLOAD * 1.4));
    const mimeType = String(body.mimeType || "");
    if (!["application/pdf", "text/plain"].includes(mimeType)) return send(res, 415, { error: "Only PDF or TXT tender documents are supported." });
    const data = String(body.data || "");
    const size = Math.floor(data.length * 0.75);
    if (!size || size > MAX_UPLOAD) return send(res, 413, { error: "File must be under 10 MB." });
    for (const [id, d] of documents) if (Date.now() - d.at > DOC_TTL) documents.delete(id);
    const docId = crypto.randomUUID();
    documents.set(docId, { name: String(body.name || "document.pdf").slice(0, 120), mimeType, data, size, at: Date.now() });
    return send(res, 200, { docId, name: documents.get(docId).name, sizeKB: Math.round(size / 1024), aiReadable: gemini.isConfigured() });
  }

  if (m === "POST" && p === "/api/chat") return handleChat(req, res);

  if (m === "GET" && !p.startsWith("/api/")) return serveStatic(req, res, p);
  return send(res, 404, { error: "Not found" });
}

const server = http.createServer((req, res) => {
  route(req, res).catch((err) => {
    if (!err.status) console.error(err);          // 4xx client errors are expected, don't log stacks
    if (!res.headersSent) send(res, err.status || 500, { error: err.status ? err.message : "Server error" });
    else res.end();
  });
});

if (require.main === module) {
  server.listen(PORT, () => {
    console.log(`SyncBid AI running on http://localhost:${PORT}`);
    console.log(gemini.isConfigured() ? `AI mode: Gemini (${gemini.getModel()})` : "AI mode: DEMO (set GEMINI_API_KEY for real AI answers)");
  });
}

module.exports = server;
