import { writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { request } from "node:https";
import { pathToFileURL } from "node:url";
import SCHEDULE from "../crates/edgezero-test-harness/acceptance/response-egress-schedule.json" with { type: "json" };

const TOKEN = /^[a-z0-9-]{1,64}$/;
const MAX_INPUT = 4 * 1024 * 1024;
const MAX_TRANSFER = 64 * 1024 * 1024;

function token(value) {
  return typeof value === "string" && TOKEN.test(value);
}

function object(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hex(value, length) {
  return typeof value === "string" && value.length === length && /^[a-f0-9]+$/.test(value);
}

function clientAction(name) {
  return ["nonread", "disconnect", "cancel", "slow"].includes(name) ? name : "read";
}

function validIdentity(spec) {
  return token(spec.run) && hex(spec.revision, 40) &&
    hex(spec.artifact, 64) && hex(spec.binary_sha256, 64) &&
    ["axum", "cloudflare", "fastly", "spin"].includes(spec.adapter) &&
    typeof spec.sdk === "string" && spec.sdk.length > 0 && spec.sdk.length < 80 &&
    object(spec.runtime) &&
    ((typeof spec.runtime.revision === "string" && spec.runtime.revision.length > 0 && spec.runtime.revision.length <= 100) ||
      ["provider-unpublished", "operator-unavailable"].includes(spec.runtime.unknown)) &&
    object(spec.compatibility) && Array.isArray(spec.compatibility.flags) && spec.compatibility.flags.length <= 32 &&
    spec.compatibility.flags.every(flag => typeof flag === "string" && /^[a-zA-Z0-9_-]{1,80}$/.test(flag)) &&
    (spec.adapter !== "cloudflare" || (typeof spec.compatibility.date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(spec.compatibility.date))) &&
    typeof spec.deployment === "string" &&
    spec.deployment.length > 0 && spec.deployment.length <= 200;
}

function requireFact(condition, reason) {
  if (!condition) throw new Error(reason);
}

export function validateTarget(spec) {
  const fields = new Set(["origin", "allowed_origin", "isolated_nonproduction", "run", "adapter", "revision",
    "artifact", "binary_sha256", "sdk", "runtime", "compatibility", "deployment", "max_probe_ms", "execution_ceiling_ms"]);
  requireFact(Object.keys(spec).every(key => fields.has(key)), "Unexpected target field");
  const url = new URL(spec.origin);
  requireFact(url.protocol === "https:" && !url.username && !url.password &&
    url.pathname === "/" && !url.search && !url.hash && url.origin === spec.allowed_origin,
  "An explicitly allowlisted HTTPS origin without credentials or a path is required");
  requireFact(validIdentity(spec),
  "SDK, runtime, compatibility and independently recorded deployment identity are required");
  requireFact(Number.isInteger(spec.max_probe_ms) && spec.max_probe_ms >= 1000 &&
    spec.max_probe_ms <= 10_000, "Probe time budget must be 1000..10000 ms");
  requireFact(spec.isolated_nonproduction === true, "A designated isolated nonproduction target is required");
  requireFact(spec.adapter !== "fastly" || (Number.isInteger(spec.execution_ceiling_ms) &&
    spec.execution_ceiling_ms > 0 && spec.execution_ceiling_ms <= 60_000),
  "Fastly pending-source probes require an independently enforced finite execution ceiling");
  return url;
}

async function readJson(path) {
  const chunks = [];
  let length = 0;
  for await (const chunk of createReadStream(path, { highWaterMark: 16 * 1024 })) {
    length += chunk.length;
    requireFact(length <= MAX_INPUT, "Evidence input exceeds 4 MiB");
    chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks, length);
  return JSON.parse(bytes.toString("utf8"));
}

function runClient(origin, spec, probe, remaining) {
  return new Promise(resolve => {
    let bytes = 0;
    let status = null;
    let settled = false;
    let pauseTimer;
    let pauses = 0;
    const action = clientAction(probe.case);
    const started = performance.now();
    const finish = (ended, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      clearTimeout(pauseTimer);
      req.destroy();
      resolve({ ...probe, action, ended, result, status, bytes, pauses, elapsed_ms: Math.ceil(performance.now() - started) });
    };
    const req = request(new URL(`/qualification/${probe.case}`, origin), {
      method: probe.case === "head" ? "HEAD" : "GET", agent: false,
      headers: { "x-qualification-run": spec.run, "x-qualification-probe": probe.probe, "accept-encoding": "identity" },
    }, response => {
      status = response.statusCode;
      response.on("error", () => finish(false, "response-error"));
      response.on("end", () => finish(true, "end"));
      if (action === "nonread") { response.pause(); return; }
      if (action === "disconnect") { finish(false, "client-close"); return; }
      response.on("data", chunk => {
        bytes += chunk.length;
        if (bytes > remaining) { finish(false, "transfer-cap"); return; }
        if (action === "slow") {
          pauses++;
          response.pause();
          pauseTimer = setTimeout(() => response.resume(), 50);
        }
      });
    });
    const timer = setTimeout(() => finish(false, "client-deadline"), action === "cancel" ? 100 : spec.max_probe_ms);
    req.on("error", () => finish(false, "request-error"));
    req.end();
  });
}

async function main(args) {
  requireFact(args[0] === "--execute-hosted" && args.length === 3,
    "Use --execute-hosted TARGET.json CLIENTS.json; validate with scripts/test_integration.sh response-egress --validate CLIENTS.json EVENTS.json");
  const spec = await readJson(args[1]);
  const origin = validateTarget(spec);
  const expected = SCHEDULE;
  const clients = [];
  let remaining = MAX_TRANSFER;
  for (const probe of expected) {
    requireFact(remaining > 0, "Run transfer budget exhausted");
    const observed = await runClient(origin, spec, probe, remaining);
    clients.push(observed);
    remaining -= observed.bytes;
    if (spec.adapter === "fastly" && ["pending", "cancel", "nonread", "disconnect"].includes(probe.case)) {
      const teardown = Math.max(0, spec.execution_ceiling_ms - observed.elapsed_ms) + 100;
      await new Promise(resolve => setTimeout(resolve, teardown));
    }
  }
  await writeFile(args[2], JSON.stringify({ manifest: { ...spec, schema: 1, expected, telemetry_complete: false }, clients }, null, 2), { flag: "wx", mode: 0o600 });
  process.stdout.write("Client observations recorded; hosted qualification requires a complete correlated telemetry export.\n");
  return 2;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).then(code => { process.exitCode = code; }).catch(() => {
    process.stderr.write("Hosted lifecycle qualification failed; check the bounded target/evidence inputs.\n");
    process.exitCode = 1;
  });
}
