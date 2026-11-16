#!/usr/bin/env node
// Explicit two-node fleet proof for the EdgeZero store service on official celld.
//
// It runs a disposable S3-compatible bucket container, deploys
// services/celld-store with `celld deploy`, and starts two loopback celld nodes.
// Node B advertises its internal address through a fault proxy, so a call from
// node A to the object on node B can lose its reply after B ran it.
//
// Checks:
//   1. stock celld resends the lost call (the proxy sees the mutation twice);
//   2. the client still gets the first outcome and the value is applied once;
//   3. acknowledged writes survive kill -9 of the owner and takeover by A.
//
// Requirements: CELLD_BIN (official release), docker, curl. Missing
// requirements fail; nothing is skipped. This is local evidence only: it does
// not qualify a production bucket, network or fleet size.
import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, connect } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { EdgeZeroStore } from "../services/celld-store/src/index.js";

const root = fileURLToPath(new URL("../", import.meta.url));
const DEFAULT_IMAGE = "rustfs/rustfs:1.0.1";
const ACCESS_KEY = "fleetproofaccess";
const SECRET_KEY = "fleetproofsecret0123";
const BUCKET = "edgezero-fleet-proof";

export const trustedPolicy = JSON.stringify({
  version: 1,
  stores: [{ alias: "state", namespace: "fleet-proof-state" }],
  access: { mode: "trusted-network", grants: [{ store: "state", operations: ["read", "write", "delete", "list"] }] },
});

export function serviceConfig(policy = trustedPolicy) {
  return {
    name: "edgezero-store",
    main: "src/index.js",
    no_bundle: true,
    compatibility_date: "2026-10-01",
    durable_objects: { bindings: [{ name: "EDGEZERO_STORE", class_name: "EdgeZeroStore" }] },
    migrations: [{ tag: "v1", new_sqlite_classes: ["EdgeZeroStore"] }],
    vars: { EDGEZERO_STORE_POLICY: policy },
  };
}

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], ...options });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => (code === 0 ? resolve(stdout) : reject(new Error(`${command} ${args.join(" ")} failed (${code}): ${stderr}`))));
  });
}

async function freePort() {
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

async function waitFor(check, label, timeoutMs = 60_000) {
  const end = Date.now() + timeoutMs;
  for (;;) {
    try {
      if (await check()) return;
    } catch {
      // Not ready yet.
    }
    if (Date.now() > end) throw new Error(`timeout waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

/** Forwards to the real internal listener; can drop one reply after forwarding a matching request. */
export function faultProxy(listenPort, targetPort) {
  const state = { dropNext: null, forwarded: new Map(), holdUntil: new Map(), server: null };
  state.server = createServer((client) => {
    const upstream = connect(targetPort, "127.0.0.1");
    let dropping = false;
    let pending = [];
    let releaseAt = 0;
    let release;
    client.on("data", (chunk) => {
      const text = chunk.toString("latin1");
      for (const [marker, count] of state.forwarded) {
        if (text.includes(marker)) state.forwarded.set(marker, count + 1);
      }
      for (const [marker, until] of state.holdUntil) {
        if (text.includes(marker)) releaseAt = Math.max(releaseAt, until);
      }
      if (releaseAt > Date.now() || pending.length > 0) {
        pending.push(chunk);
        release ??= setTimeout(() => {
          if (!upstream.destroyed) upstream.write(Buffer.concat(pending));
          pending = [];
        }, Math.max(0, releaseAt - Date.now()));
        return;
      }
      if (state.dropNext !== null && text.includes(state.dropNext)) {
        state.dropNext = null;
        dropping = true;
        upstream.write(chunk);
        setTimeout(() => client.destroy(), 300);
        return;
      }
      upstream.write(chunk);
    });
    upstream.on("data", (chunk) => { if (!dropping) client.write(chunk); });
    const close = () => { clearTimeout(release); client.destroy(); upstream.destroy(); };
    client.on("close", close);
    upstream.on("close", close);
    client.on("error", close);
    upstream.on("error", close);
  });
  state.server.listen(listenPort, "127.0.0.1");
  return state;
}

function mutationId() {
  return randomBytes(16).toString("base64url");
}

async function put(base, key, value, id = mutationId()) {
  const response = await fetch(`${base}/v1/stores/state/put?key=${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { "x-edgezero-mutation-id": id },
    body: value,
  });
  return { status: response.status, id };
}

async function read(base, key) {
  const response = await fetch(`${base}/v1/stores/state/value?key=${encodeURIComponent(key)}`);
  return response.status === 200 ? response.text() : null;
}

async function recordRequest(base, instance, operation, key, payload, id = mutationId()) {
  const response = await fetch(`${base}/v1/stores/state/${operation}?key=${encodeURIComponent(key)}`, {
    method: operation === "record" ? "GET" : "POST",
    headers: {
      "x-edgezero-record-version": "1",
      "x-edgezero-expected-instance": instance,
      "x-edgezero-mutation-id": id,
      "content-type": "application/json",
    },
    ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
  });
  const reply = response.status === 204 ? null : await response.json();
  return { status: response.status, row: response.status === 200 ? reply : null, error: reply?.error ?? null,
    instance: response.headers.get("x-edgezero-store-instance"), id };
}

function recordValue(value, mode = "overwrite", generation) {
  return { body: Buffer.from(value).toString("base64"), metadata: Buffer.from("fleet-proof").toString("base64"), ttl_ms: null, mode,
    ...(generation === undefined ? {} : { generation }) };
}

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

export async function runFleetProof({ env = process.env, log = console.log } = {}) {
  const celld = env.CELLD_BIN;
  if (!celld || !isAbsolute(celld)) throw new Error("CELLD_BIN must select an absolute official celld executable; no passing skip");
  const version = (await run(celld, ["--version"])).trim();
  if (version !== "celld 0.6.3") throw new Error(`expected official celld 0.6.3, got ${version}`);
  const binaryDigest = sha256(celld);
  log(`celld version: ${version}; executable SHA-256 ${binaryDigest}`);
  log(`service src/index.js SHA-256 ${sha256(join(root, "services/celld-store/src/index.js"))}`);
  const help = await run(celld, ["--help"]);
  if (help.includes("--store-api-config")) throw new Error("CELLD_BIN is a modified celld; this proof requires an official release");
  const image = env.CELLD_PROOF_BUCKET_IMAGE ?? DEFAULT_IMAGE;
  const work = mkdtempSync(join(tmpdir(), "edgezero-fleet-proof-"));
  const container = `edgezero-fleet-proof-${process.pid}`;
  const children = [];
  let proxy;
  const cleanup = async () => {
    for (const child of children) {
      try { process.kill(-child.pid, "SIGKILL"); } catch { /* already gone */ }
    }
    proxy?.server.close();
    await run("docker", ["rm", "-f", container]).catch(() => {});
    rmSync(work, { recursive: true, force: true });
  };
  try {
    const bucketPort = await freePort();
    await run("docker", ["run", "-d", "--name", container, "-p", `127.0.0.1:${bucketPort}:9000`,
      "-e", `RUSTFS_ACCESS_KEY=${ACCESS_KEY}`, "-e", `RUSTFS_SECRET_KEY=${SECRET_KEY}`, image]);
    const endpoint = `http://127.0.0.1:${bucketPort}`;
    await waitFor(async () => {
      await run("curl", ["--silent", "--fail", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", `${ACCESS_KEY}:${SECRET_KEY}`, "-X", "PUT", `${endpoint}/${BUCKET}`]);
      return true;
    }, "bucket creation");
    const cellEnv = { PATH: env.PATH, HOME: join(work, "home"), AWS_ACCESS_KEY_ID: ACCESS_KEY, AWS_SECRET_ACCESS_KEY: SECRET_KEY, AWS_REGION: "us-east-1", NO_COLOR: "1" };
    const bucketArgs = ["--bucket", `s3://${BUCKET}`, "--endpoint", endpoint];
    log((await run(celld, ["diagnose", ...bucketArgs], { env: cellEnv })).trim());
    const service = join(work, "service");
    cpSync(join(root, "services/celld-store/src"), join(service, "src"), { recursive: true });
    writeFileSync(join(service, "wrangler.json"), JSON.stringify(serviceConfig(), null, 2));
    await run(celld, ["deploy", service, ...bucketArgs], { env: cellEnv });

    const ports = { a: await freePort(), aInternal: await freePort(), b: await freePort(), bInternal: await freePort(), bAdvertised: await freePort() };
    proxy = faultProxy(ports.bAdvertised, ports.bInternal);
    const startNode = (name, listen, internal, advertise) => {
      const child = spawn(celld, [...bucketArgs, "--listen", `127.0.0.1:${listen}`, "--internal-listen", `127.0.0.1:${internal}`, "--advertise", `127.0.0.1:${advertise}`], {
        env: { ...cellEnv, CELLD_NODE: `fleet-proof-${name}`, CELLD_WATCH: join(work, name) },
        detached: true,
        stdio: "ignore",
      });
      children.push(child);
      return child;
    };
    const healthy = (port) => async () => (await fetch(`http://127.0.0.1:${port}/.well-known/celld/health`)).ok;
    startNode("a", ports.a, ports.aInternal, ports.aInternal);
    const nodeB = startNode("b", ports.b, ports.bInternal, ports.bAdvertised);
    await waitFor(healthy(ports.a), "node A");
    await waitFor(healthy(ports.b), "node B");
    const viaA = `http://127.0.0.1:${ports.a}`;
    const viaB = `http://127.0.0.1:${ports.b}`;

    // The first call through B activates the object on B.
    if ((await put(viaB, "owner-probe", "b")).status !== 204) throw new Error("activation through B failed");
    if ((await put(viaA, "forwarded", "a")).status !== 204) throw new Error("forwarded write failed");
    // celld returns a peer tunnel to its idle pool once a reply body has been
    // streamed, so a read through A leaves a pooled tunnel for the next call.
    if (await read(viaA, "forwarded") !== "a") throw new Error("forwarded read failed");
    await new Promise((resolve) => setTimeout(resolve, 300));

    // 1 and 2: lose the reply after B ran the mutation.
    const id = mutationId();
    proxy.forwarded.set(id, 0);
    proxy.dropNext = id;
    const lost = await put(viaA, "lost-reply", "once", id);
    const deliveries = proxy.forwarded.get(id);
    log(`lost-reply mutation: client status ${lost.status}, proxy forwarded it ${deliveries} time(s)`);
    if (deliveries < 2) throw new Error("stock celld did not resend the lost call; the fault was not exercised");
    if (lost.status !== 204) throw new Error(`a resent mutation must return its first outcome, got ${lost.status}`);
    if (await read(viaA, "lost-reply") !== "once") throw new Error("lost-reply value is wrong");

    // Record Add and CAS must also answer their first success after an actual
    // internal resend. Without the ledger, each resend would reject its own write.
    const capability = await fetch(`${viaA}/v1/stores/state/capabilities`);
    if (!capability.ok) throw new Error("record preparation failed");
    const instance = capability.headers.get("x-edgezero-store-instance");
    if (!instance) throw new Error("missing prepared instance");
    const recordCall = (operation, key, payload, id) => recordRequest(viaA, instance, operation, key, payload, id);
    const baseline = (await put(viaA, "record-baseline", "baseline"));
    if (baseline.status !== 204) throw new Error("record baseline failed");
    const baselineRow = (await recordCall("record", "record-baseline")).row;
    let expectedGeneration = BigInt(baselineRow.generation);
    for (const mode of ["add", "if-generation-match"]) {
      const recordId = mutationId();
      proxy.forwarded.set(recordId, 0);
      proxy.dropNext = recordId;
      const payload = recordValue(mode, mode, mode === "add" ? undefined : String(expectedGeneration));
      const outcome = await recordCall("record-put", "record-resend", payload, recordId);
      const count = proxy.forwarded.get(recordId);
      if (count < 2 || outcome.status !== 204) throw new Error(`record ${mode} fault not neutralized: ${count} deliveries, status ${outcome.status}`);
      const row = (await recordCall("record", "record-resend")).row;
      expectedGeneration += 1n;
      if (row.generation !== String(expectedGeneration) || row.body !== payload.body || row.metadata !== payload.metadata) {
        throw new Error(`record ${mode} did not apply exactly once`);
      }
      log(`record ${mode}: ${count} internal deliveries, one generation allocation, original success`);
      await new Promise((resolve) => setTimeout(resolve, 300));
    }
    // Hold delivery after Worker admission but before the object transaction.
    // Every retry with this ID is held beyond the same original apply window.
    const lateId = mutationId();
    proxy.forwarded.set(lateId, 0);
    proxy.holdUntil.set(lateId, Date.now() + EdgeZeroStore.internals.LIMITS.applyWindowMs + 1_000);
    const late = await recordCall("record-put", "record-too-late", recordValue("late", "add"), lateId);
    if (proxy.forwarded.get(lateId) < 1 || late.status !== 504 || late.error !== "deadline" || late.instance !== instance) {
      throw new Error(`late delivery did not reach the object's deadline check: ${JSON.stringify(late)}`);
    }
    if ((await recordCall("record", "record-too-late")).status !== 404) throw new Error("late mutation applied");
    proxy.holdUntil.delete(lateId);
    log("record delivery held beyond the apply window: rejected, no row created");
    const survivingRecord = (await recordCall("record", "record-resend")).row;
    // A remembered rejection must remain rejected even after deleting its row.
    const rejectedId = mutationId();
    const rejectedPayload = recordValue("must-not-appear", "add");
    if ((await recordCall("record-put", "record-resend", rejectedPayload, rejectedId)).status !== 412) throw new Error("expected initial Add rejection");
    if ((await recordCall("record-delete", "record-resend")).status !== 204) throw new Error("record delete failed");
    if ((await recordCall("record-put", "record-resend", rejectedPayload, rejectedId)).status !== 412) throw new Error("replayed rejection changed into a write");
    if ((await recordCall("record", "record-resend")).status !== 404) throw new Error("replayed rejection created a row");
    if ((await recordCall("record-put", "record-resend", recordValue("takeover-record"))).status !== 204) throw new Error("record recreation failed");
    const takeoverRecord = (await recordCall("record", "record-resend")).row;
    if (BigInt(takeoverRecord.generation) <= BigInt(survivingRecord.generation)) throw new Error("delete reused generation");

    // 3: acknowledged writes survive kill -9 of the owner and takeover by A.
    const keys = [];
    for (let index = 0; index < 10; index += 1) {
      const key = `takeover-${index}`;
      if ((await put(viaA, key, `v${index}`)).status !== 204) throw new Error(`write ${key} failed`);
      keys.push(key);
    }
    process.kill(-nodeB.pid, "SIGKILL");
    await waitFor(async () => (await read(viaA, "takeover-9")) === "v9", "takeover by node A", 90_000);
    for (const [index, key] of keys.entries()) {
      if (await read(viaA, key) !== `v${index}`) throw new Error(`acknowledged write ${key} lost after takeover`);
    }
    if (await read(viaA, "lost-reply") !== "once") throw new Error("lost-reply value changed after takeover");
    const afterTakeover = (await recordCall("record", "record-resend")).row;
    if (JSON.stringify(afterTakeover) !== JSON.stringify(takeoverRecord)) throw new Error("acknowledged record changed after takeover");
    if ((await recordCall("record-put", "record-resend", recordValue("stale", "if-generation-match", survivingRecord.generation))).status !== 412) {
      throw new Error("stale generation accepted after takeover");
    }
    if ((await recordCall("record-put", "record-resend", rejectedPayload, rejectedId)).status !== 412) throw new Error("remembered rejection lost after takeover");
    if ((await recordCall("record-put", "record-after-takeover", recordValue("new", "add"))).status !== 204) throw new Error("post-takeover write failed");
    const allocated = (await recordCall("record", "record-after-takeover")).row;
    if (BigInt(allocated.generation) <= BigInt(takeoverRecord.generation)) throw new Error("allocator moved backward after takeover");
    if (sha256(celld) !== binaryDigest) throw new Error("tested executable changed");
    log("Fleet proof passed: ordinary KV and record resend neutralized; records, metadata, allocator and rejection ledger survived owner loss.");
  } finally {
    await cleanup();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runFleetProof().catch((error) => { console.error(error.message); process.exitCode = 1; });
}
