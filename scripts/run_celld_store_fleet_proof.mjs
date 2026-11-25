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
import { randomBytes } from "node:crypto";
import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, connect } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

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
  const state = { dropNext: null, forwarded: new Map(), server: null };
  state.server = createServer((client) => {
    const upstream = connect(targetPort, "127.0.0.1");
    let dropping = false;
    client.on("data", (chunk) => {
      const text = chunk.toString("latin1");
      for (const [marker, count] of state.forwarded) {
        if (text.includes(marker)) state.forwarded.set(marker, count + 1);
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
    const close = () => { client.destroy(); upstream.destroy(); };
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

export async function runFleetProof({ env = process.env, log = console.log } = {}) {
  const celld = env.CELLD_BIN;
  if (!celld || !isAbsolute(celld)) throw new Error("CELLD_BIN must select an absolute official celld executable; no passing skip");
  const version = (await run(celld, ["--version"])).trim();
  log(`celld version: ${version}`);
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
    log("Fleet proof passed: resend neutralized, acknowledged writes survived owner loss.");
  } finally {
    await cleanup();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runFleetProof().catch((error) => { console.error(error.message); process.exitCode = 1; });
}
