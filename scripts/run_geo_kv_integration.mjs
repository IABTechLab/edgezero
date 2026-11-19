#!/usr/bin/env node
// Disposable local proof. Runs synthetic fixtures by default. Optionally add a
// permitted local City CSV with GEO_KV_CSV_DIR. No remote endpoints or downloads.
// GEO_KV_TOOL_BIN=/absolute/geo-kv-prototype GEO_KV_HOST_BIN=/absolute/edgezero-hosting-fixture \
// CELLD_BIN=/absolute/official/celld node scripts/run_geo_kv_integration.mjs
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { constants, createReadStream } from "node:fs";
import { access, cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import https from "node:https";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { start as startTls } from "./celld_store_tls_terminator.mjs";
import { fixtureClient } from "./native_aws_test_admin.mjs";
import { publishedEndpoint } from "./run_native_aws_integration.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
export const DYNAMO_IMAGE = "amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab";
const table = "edgezero-geo-proof";
const namespace = "geo-proof";
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");

export function localEndpoint(endpoint, protocol = "http:") {
  assert.match(endpoint, /^https?:\/\/127\.0\.0\.1:[1-9][0-9]*$/, "literal loopback endpoint required");
  const parsed = new URL(endpoint);
  assert.equal(parsed.protocol, protocol);
  assert.equal(parsed.hostname, "127.0.0.1", "proof endpoints must be literal loopback");
  assert(parsed.port && Number(parsed.port) > 0 && Number(parsed.port) <= 65535);
  assert.equal(parsed.username + parsed.password + parsed.search + parsed.hash, "");
  assert.equal(parsed.pathname, "/");
  return parsed.origin;
}

export function childEnvironment(source, work) {
  // In particular do not inherit endpoints, profiles, proxies, Geo selection,
  // bindings, FIXTURE_GEO_ARTIFACT, or real credentials from this shell.
  return {
    PATH: source.PATH ?? "/usr/bin:/bin", HOME: join(work, "home"), TMPDIR: work,
    NO_COLOR: "1", AWS_ACCESS_KEY_ID: "DUMMYLOCAL", AWS_SECRET_ACCESS_KEY: "DUMMYLOCAL",
    AWS_REGION: "us-east-1", AWS_EC2_METADATA_DISABLED: "true",
    AWS_CONFIG_FILE: join(work, "absent-aws-config"),
    AWS_SHARED_CREDENTIALS_FILE: join(work, "absent-aws-credentials"),
    AWS_IGNORE_CONFIGURED_ENDPOINT_URLS: "false", RUST_LOG: "off",
  };
}

export function serviceConfig(publisher, serving) {
  const policy = {
    version: 1, stores: [{ alias: "geo", namespace }],
    access: { mode: "authenticated", tokens: [
      { id: "publisher", sha256: hash(publisher), grants: [{ store: "geo", operations: ["read", "write", "delete", "list"] }] },
      { id: "serving", sha256: hash(serving), grants: [{ store: "geo", operations: ["read"] }] },
    ] },
  };
  return {
    name: "edgezero-store", main: "src/index.js", no_bundle: true, compatibility_date: "2026-10-01",
    durable_objects: { bindings: [{ name: "EDGEZERO_STORE", class_name: "EdgeZeroStore" }] },
    migrations: [{ tag: "v1", new_sqlite_classes: ["EdgeZeroStore"] }],
    vars: { EDGEZERO_STORE_POLICY: JSON.stringify(policy) },
  };
}

export function syntheticCsv(country = "US", subdivision = "CA") {
  return {
    "GeoLite2-City-Locations-en.csv": `geoname_id,country_iso_code,subdivision_1_iso_code\n1,${country},${subdivision}\n2,GB,\n3,XK,\n4,,\n`,
    "GeoLite2-City-Blocks-IPv4.csv": "network,geoname_id,registered_country_geoname_id\n"
      + Array.from({ length: 120 }, (_, i) => `192.0.2.${i * 2}/32,${i === 1 ? 3 : i === 2 ? 2 : 1},2\n`).join("")
      + "198.51.100.0/24,,1\n203.0.113.0/24,4,1\n",
    "GeoLite2-City-Blocks-IPv6.csv": "network,geoname_id\n2001:db8::/126,2\n",
  };
}

async function within(promise, label, ms = 30_000) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out`)), ms);
    })]);
  } finally { clearTimeout(timer); }
}

class Process {
  constructor(command, args, options, children) {
    this.output = "";
    this.child = spawn(command, args, { ...options, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    children.add(this);
    for (const stream of [this.child.stdout, this.child.stderr]) {
      stream.on("data", (chunk) => { this.output = (this.output + chunk).slice(-131072); });
    }
    this.done = new Promise((resolve) => {
      this.child.once("error", (error) => { this.result = { error }; resolve(this.result); });
      this.child.once("close", (code, signal) => { this.result = { code, signal }; resolve(this.result); });
    });
  }
  kill(signal = "SIGKILL") {
    if (!this.child.pid) return;
    try { process.kill(-this.child.pid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
  async success(timeoutMs = 60_000) {
    const result = await within(this.done, "child exit", timeoutMs);
    assert.equal(result.code, 0, `child failed: ${JSON.stringify(result)}\n${this.output}`);
    return this.output;
  }
}

async function waitFor(check, label, process) {
  const deadline = Date.now() + 60_000;
  let last;
  while (Date.now() < deadline) {
    if (process?.result) throw new Error(`${label} process exited: ${process.output}`);
    try { if (await check()) return; } catch (error) { last = error; }
    await delay(50);
  }
  throw new Error(`waiting for ${label} failed`, { cause: last });
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return `http://127.0.0.1:${server.address().port}`;
}

async function port() {
  const server = createServer();
  const endpoint = await listen(server);
  await new Promise((resolve) => server.close(resolve));
  return new URL(endpoint).port;
}

function request(url, { method = "GET", headers = {}, body, ca } = {}) {
  const parsed = new URL(url);
  localEndpoint(parsed.origin, parsed.protocol);
  return new Promise((resolve, reject) => {
    const client = parsed.protocol === "https:" ? https : http;
    const req = client.request(url, { method, headers, ca }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("error", reject);
      res.on("end", () => resolve({ status: res.statusCode, bytes: Buffer.concat(chunks) }));
    });
    const timer = setTimeout(() => req.destroy(new Error("local request timed out")), 3000);
    req.on("error", reject);
    req.on("close", () => clearTimeout(timer));
    req.end(body);
  });
}

// No redirects or environment proxy discovery. The publisher fault waits until
// the backend applied a write, then withholds its acknowledgment until we kill
// the publisher. Serving traffic has its own audit and cannot be mistaken for
// direct fixture administration.
export async function auditProxy(target, classify) {
  localEndpoint(target);
  const state = { calls: [], fault: null, heldReads: null };
  const server = http.createServer((incoming, outgoing) => {
    const call = { ...classify(incoming), responseBytes: 0 };
    state.calls.push(call);
    if (state.heldReads && call.role === "serving" && !call.write &&
        state.heldReads.remaining > 0) {
      incoming.resume();
      const gate = state.heldReads;
      gate.remaining--;
      if (gate.remaining === 0) gate.resolve();
      return; // Leave the request pending until its owner cancels or times out.
    }
    const upstream = http.request(`${target}${incoming.url}`, {
      method: incoming.method, headers: incoming.headers,
    }, (response) => {
      call.status = response.statusCode;
      response.on("data", (chunk) => { call.responseBytes += chunk.length; });
      if (state.fault && call.write) {
        const fault = state.fault;
        state.fault = null;
        response.resume();
        response.on("end", () => {
          fault.resolve({ call, status: response.statusCode });
          // Deliberately never send the successful reply. No retry here.
        });
      } else {
        outgoing.writeHead(response.statusCode, response.headers);
        response.pipe(outgoing);
      }
    });
    upstream.on("error", () => { if (!outgoing.destroyed) outgoing.destroy(); });
    outgoing.on("close", () => upstream.destroy());
    incoming.pipe(upstream);
  });
  state.endpoint = await listen(server);
  state.server = server;
  state.arm = () => new Promise((resolve) => { state.fault = { resolve }; });
  state.hold = (remaining) => new Promise((resolve) => { state.heldReads = { remaining, resolve }; });
  return state;
}

function hostEnvironment(base, endpoint, bindings, pin) {
  return {
    ...base, FIXTURE_MODE: "geo-kv", FIXTURE_BIND: new URL(endpoint).host,
    EDGEZERO__ADAPTER__HOST: "127.0.0.1", EDGEZERO__ADAPTER__PORT: new URL(endpoint).port,
    EDGEZERO__STORE_BINDINGS_FILE: bindings,
    EDGEZERO__ADAPTER__GEO__PROVIDER: "kv", EDGEZERO__ADAPTER__GEO__STORE_ID: "fixture_geo",
    EDGEZERO__ADAPTER__GEO__READER_IP: "forwarded-required", EDGEZERO__ADAPTER__GEO__PIN: pin,
    EDGEZERO__ADAPTER__GEO__LOOKUP_TIMEOUT_MS: "1000", EDGEZERO__ADAPTER__GEO__MAX_IN_FLIGHT: "4",
    EDGEZERO__ADAPTER__TRUSTED_PROXY_CIDRS: "127.0.0.1/32",
    EDGEZERO__ADAPTER__FORWARDING_HEADER_FAMILY: "forwarded",
    EDGEZERO__ADAPTER__SHUTDOWN_GRACE_SECONDS: "1",
  };
}

// Approximate process peak RSS, including runtime and SDK allocations, not
// individual index allocations. No reader addresses are written to the report.
async function peakRss(pid) {
  const status = await readFile(`/proc/${pid}/status`, "utf8");
  const match = /^VmHWM:\s+(\d+) kB$/m.exec(status);
  assert(match, "licensed-size proof requires Linux VmHWM");
  return Number(match[1]) * 1024;
}

async function sampleReaderAddresses(source, buildReport) {
  const workload = [];
  for (const [family, stats] of [[4, buildReport.ipv4], [6, buildReport.ipv6]]) {
    const stride = Math.max(1, Math.floor(stats.source_networks / 64));
    const input = createReadStream(join(source, `GeoLite2-City-Blocks-IPv${family}.csv`));
    const lines = createInterface({ input, crlfDelay: Infinity });
    let row = -1;
    try {
      for await (const line of lines) {
        row++;
        if (row === 0 || row % stride !== 0) continue;
        const address = line.split(",", 1)[0].split("/", 1)[0];
        workload.push(family === 4 ? address : `"[${address}]"`);
        if (row >= stride * 64) break;
      }
    } finally {
      lines.close();
      input.destroy();
    }
  }
  assert(workload.length > 0, "City workload requires source networks");
  return workload;
}

function latencyStats(samples) {
  const sorted = [...samples].sort((a, b) => a - b);
  const quantile = (value) => sorted[Math.ceil(value * sorted.length) - 1];
  return { p50_ms: quantile(0.5), p95_ms: quantile(0.95), p99_ms: quantile(0.99) };
}

export async function runIntegration({ env = process.env, log = console.log, backends = ["celld", "dynamodb"] } = {}) {
  const licensedSource = env.GEO_KV_CSV_DIR;
  if (licensedSource) assert(isAbsolute(licensedSource), "GEO_KV_CSV_DIR must be absolute");
  const binaries = { celld: env.CELLD_BIN, tool: env.GEO_KV_TOOL_BIN, host: env.GEO_KV_HOST_BIN };
  for (const [name, binary] of Object.entries(binaries)) {
    assert(binary && isAbsolute(binary), `${name} requires an explicitly selected absolute executable`);
    await access(binary, constants.X_OK);
  }
  const fingerprints = {};
  for (const [name, binary] of Object.entries(binaries)) fingerprints[name] = hash(await readFile(binary));
  const work = await mkdtemp(join(tmpdir(), "edgezero-geo-kv-"));
  const children = new Set();
  const servers = [];
  const container = `edgezero-geo-kv-${process.pid}-${randomBytes(4).toString("hex")}`;
  const network = `${container}-network`;
  let containerCreated = false;
  let networkCreated = false;
  let interrupted;
  const onInterrupt = () => {
    interrupted = true;
    for (const child of children) child.kill();
    for (const server of servers) server.closeAllConnections?.();
  };
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onInterrupt);
  const base = childEnvironment(env, work);
  const start = (command, args, extra = {}) => {
    assert(!interrupted, "proof interrupted");
    return new Process(command, args, { cwd: work, env: base, ...extra }, children);
  };
  const run = async (command, args, extra = {}, timeoutMs) => {
    const child = start(command, args, extra);
    try { return await child.success(timeoutMs); } catch (error) { child.kill(); throw error; }
  };
  const jsonFile = async (path, value) => writeFile(path, JSON.stringify(value, null, 2));
  const results = [];
  try {
    await mkdir(base.HOME);
    assert.equal((await run(binaries.celld, ["--version"])).trim(), "celld 0.6.3");
    assert(!(await run(binaries.celld, ["--help"])).includes("--store-api-config"), "modified celld is not stock proof");
    await run("docker", ["image", "inspect", DYNAMO_IMAGE]);
    // Cached image only, telemetry disabled, exclusively loopback-published.
    // A normal bridge permits host publication; this is not an egress sandbox.
    // All clients below receive explicit loopback service endpoints.
    await run("docker", ["network", "create", network]);
    networkCreated = true;
    await run("docker", ["run", "-d", "--pull=never", "--name", container, "--network", network,
      "--publish", "127.0.0.1::8000", DYNAMO_IMAGE,
      "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry"]);
    containerCreated = true;
    const dynamoEndpoint = publishedEndpoint(await run("docker", ["port", container, "8000/tcp"]));
    const dynamo = fixtureClient(dynamoEndpoint, "dynamodb");
    await waitFor(async () => { await dynamo("ListTables"); return true; }, "DynamoDB Local");
    await dynamo("CreateTable", {
      TableName: table, BillingMode: "PAY_PER_REQUEST",
      KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
      AttributeDefinitions: [{ AttributeName: "pk", AttributeType: "S" }, { AttributeName: "sk", AttributeType: "S" }],
    });

    const publisher = randomBytes(32).toString("hex");
    const serving = randomBytes(32).toString("hex");
    const service = join(work, "service");
    await mkdir(service);
    await cp(join(root, "services/celld-store/src"), join(service, "src"), { recursive: true });
    await jsonFile(join(service, "wrangler.json"), serviceConfig(publisher, serving));
    const cellPort = await port();
    const cell = start(binaries.celld, ["dev", ".", "--no-watch", "--logs", "--port", cellPort], { cwd: service });
    await waitFor(() => cell.output.includes(`celld listening on 127.0.0.1:${cellPort}`), "stock celld listener", cell);
    const cellAudit = await auditProxy(`http://127.0.0.1:${cellPort}`, (req) => ({
      role: req.headers.authorization === `Bearer ${serving}` ? "serving" : "publisher",
      operation: req.url.split("?")[0].split("/").at(-1), write: req.method !== "GET" && req.method !== "HEAD",
    }));
    servers.push(cellAudit.server);
    const cert = join(work, "cert.pem");
    const key = join(work, "key.pem");
    await run("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
      "-subj", "/CN=geo-kv-proof", "-addext", "subjectAltName=IP:127.0.0.1",
      "-addext", "basicConstraints=critical,CA:FALSE", "-addext", "extendedKeyUsage=serverAuth",
      "-keyout", key, "-out", cert]);
    const tls = startTls({ cert, key, target: new URL(cellAudit.endpoint).host, listen: "127.0.0.1:0" }, () => {});
    servers.push(tls);
    if (!tls.listening) await within(new Promise((resolve, reject) => { tls.once("listening", resolve); tls.once("error", reject); }), "TLS listener");
    const cellEndpoint = localEndpoint(`https://127.0.0.1:${tls.address().port}`, "https:");
    const ca = await readFile(cert);
    const cellBase = { ...base, SSL_CERT_FILE: cert };

    const dynamoPublish = await auditProxy(dynamoEndpoint, (req) => {
      const operation = req.headers["x-amz-target"]?.split(".").at(-1);
      return { role: "publisher", operation, write: operation === "PutItem" };
    });
    const dynamoServe = await auditProxy(dynamoEndpoint, (req) => {
      const operation = req.headers["x-amz-target"]?.split(".").at(-1);
      return { role: "serving", operation, write: !["DescribeTable", "GetItem"].includes(operation) };
    });
    servers.push(dynamoPublish.server, dynamoServe.server);
    const artifacts = [];
    for (const [name, country, subdivision] of [["a", "US", "CA"], ["b", "DE", "BE"]]) {
      const source = join(work, `csv-${name}`);
      const dir = join(work, `artifact-${name}`);
      await mkdir(source);
      for (const [file, bytes] of Object.entries(syntheticCsv(country, subdivision))) await writeFile(join(source, file), bytes);
      await run(binaries.tool, ["build", source, dir, "1024"]);
      const receipt = JSON.parse(await readFile(join(dir, "receipt.json")));
      const inventory = JSON.parse(await readFile(join(dir, "inventory.json")));
      const manifest = JSON.parse(await readFile(join(dir, receipt.manifest_key)));
      artifacts.push({ dir, source, receipt, inventory, manifest, pin: `${manifest.generation}@${Buffer.from(receipt.manifest_hash).toString("hex")}` });
    }

    const licensed = [];
    if (licensedSource) {
      for (const bytes of [16 * 1024, 64 * 1024]) {
        const dir = join(work, `licensed-${bytes}`);
        await run(binaries.tool, ["build", licensedSource, dir, String(bytes)], {}, 600_000);
        const receipt = JSON.parse(await readFile(join(dir, "receipt.json")));
        const manifest = JSON.parse(await readFile(join(dir, receipt.manifest_key)));
        const inventory = JSON.parse(await readFile(join(dir, "inventory.json")));
        const report = JSON.parse(await readFile(join(dir, "build-report.json")));
        const workload = await sampleReaderAddresses(licensedSource, report);
        licensed.push({ dir, source: licensedSource, receipt, manifest, inventory, report, workload,
          pin: `${manifest.generation}@${Buffer.from(receipt.manifest_hash).toString("hex")}` });
      }
    }
    const licensedResults = [];

    for (const backend of backends) {
      assert(["celld", "dynamodb"].includes(backend), "unknown proof backend");
      const audit = backend === "celld" ? cellAudit : dynamoPublish;
      const publisherEnv = backend === "celld" ? { ...cellBase, GEO_PUBLISH_TOKEN: publisher } : { ...base, AWS_ENDPOINT_URL_DYNAMODB: dynamoPublish.endpoint };
      const servingEnv = backend === "celld" ? { ...cellBase, GEO_SERVE_TOKEN: serving } : { ...base, AWS_ENDPOINT_URL_DYNAMODB: dynamoServe.endpoint };
      const settings = (reader) => backend === "celld"
        ? { backend, settings: { endpoint: cellEndpoint, store: "geo", access: { mode: "authenticated", token_env: reader ? "GEO_SERVE_TOKEN" : "GEO_PUBLISH_TOKEN" } } }
        : { backend, settings: { region: "us-east-1", table, namespace } };
      const publisherFile = join(work, `${backend}-publisher.json`);
      const servingFile = join(work, `${backend}-serving.json`);
      const bindings = join(work, `${backend}-bindings.toml`);
      await jsonFile(publisherFile, settings(false));
      await jsonFile(servingFile, settings(true));
      await writeFile(bindings, "version = 1\n[kv.fixture_geo]\n" + (backend === "celld"
        ? `provider = "celld-kv"\nendpoint = "${cellEndpoint}"\nstore = "geo"\n[kv.fixture_geo.access]\nmode = "authenticated"\ntoken_env = "GEO_SERVE_TOKEN"\n`
        : `provider = "aws-dynamodb"\nregion = "us-east-1"\ntable = "${table}"\nnamespace = "${namespace}"\n`));
      const tool = (args, reader = false) => run(binaries.tool, args, { env: reader ? servingEnv : publisherEnv });
      const publish = async (artifact) => JSON.parse(await tool(["publish", artifact.dir, publisherFile]));
      const verify = async (artifact) => {
        const observed = backend === "celld" ? cellAudit : dynamoServe;
        const before = observed.calls.length;
        const result = await tool(["verify-remote", artifact.dir, servingFile, artifact.source, "256"], true)
          .catch((error) => {
            const calls = observed.calls.slice(before);
            throw new Error(`${backend} verify failed after ${calls.length} calls; last responses: ${JSON.stringify(calls.slice(-8))}`, { cause: error });
          });
        return JSON.parse(result);
      };
      const expectFailure = async (args, reader = false) => {
        const child = start(binaries.tool, args, { env: reader ? servingEnv : publisherEnv });
        const outcome = await within(child.done, "expected CLI rejection", 30_000);
        assert.equal(outcome.code, 1, `expected failed command, got ${JSON.stringify(outcome)}\n${child.output}`);
      };
      const cellRequest = (operation, objectKey, value, token = publisher) => request(
        `${cellEndpoint}/v1/stores/geo/${operation}?key=${encodeURIComponent(objectKey)}`,
        { ca, method: operation === "value" ? "GET" : "POST", body: value,
          headers: { Authorization: `Bearer ${token}`, "x-edgezero-mutation-id": randomBytes(16).toString("base64url") } },
      );
      const keyOf = (objectKey) => ({ pk: { S: namespace }, sk: { S: objectKey } });
      const read = async (objectKey) => {
        if (backend === "celld") {
          const response = await cellRequest("value", objectKey);
          assert([200, 404].includes(response.status), `celld read status ${response.status}`);
          return response.status === 200 ? response.bytes : null;
        }
        const response = await dynamo("GetItem", { TableName: table, Key: keyOf(objectKey), ConsistentRead: true });
        return response.Item ? Buffer.from(response.Item.v.B, "base64") : null;
      };
      const put = async (objectKey, value) => {
        if (backend === "celld") assert.equal((await cellRequest("put", objectKey, value)).status, 204);
        else await dynamo("PutItem", { TableName: table, Item: { ...keyOf(objectKey), v: { B: value.toString("base64") } } });
      };
      const remove = async (objectKey) => {
        if (backend === "celld") assert.equal((await cellRequest("delete", objectKey)).status, 204);
        else await dynamo("DeleteItem", { TableName: table, Key: keyOf(objectKey) });
      };
      const [a, b] = artifacts;
      // Actual uncertain acknowledgment, not just a preseeded partial inventory.
      const applied = audit.arm();
      const uploader = start(binaries.tool, ["publish", a.dir, publisherFile], { env: publisherEnv });
      try { assert.equal((await within(applied, "interrupted applied publication")).status, backend === "celld" ? 204 : 200); }
      finally { uploader.kill(); await uploader.done; audit.fault = null; }
      assert.equal(await read(a.receipt.manifest_key), null, "manifest must not be published by interrupted uploader");
      const resumed = await publish(a);
      assert(resumed.identical >= 1 && resumed.written >= 1, "rerun must reconcile the applied write");
      assert.deepEqual(await publish(a), { objects: a.inventory.length, written: 0, identical: a.inventory.length });
      assert.equal((await verify(a)).oracle_samples, 512);

      const damaged = a.inventory.find((item) => item.key !== a.receipt.manifest_key);
      const original = await readFile(join(a.dir, damaged.key));
      await put(damaged.key, Buffer.from("conflicting immutable bytes"));
      await expectFailure(["publish", a.dir, publisherFile]);
      assert.equal((await read(damaged.key)).toString(), "conflicting immutable bytes", "conflict must not be overwritten");
      await put(damaged.key, original);
      await remove(damaged.key);
      await expectFailure(["verify-remote", a.dir, servingFile, a.source, "256"], true);
      await publish(a);
      assert.equal((await verify(a)).status, "verified");
      await publish(b);
      assert.equal((await verify(b)).status, "verified");

      if (backend === "celld") {
        assert.equal((await cellRequest("put", "must-not-write", Buffer.from("denied"), serving)).status, 403);
        assert.equal((await cellRequest("delete", a.receipt.manifest_key, undefined, serving)).status, 403);
        assert.equal(await read("must-not-write"), null);
      }
      const hosts = new Set();
      const launch = async (artifact) => {
        const started = performance.now();
        const endpoint = `http://127.0.0.1:${await port()}`;
        const cwd = join(work, `${backend}-host-${randomBytes(4).toString("hex")}`);
        await mkdir(cwd);
        const child = start(binaries.host, [], { cwd, env: hostEnvironment(servingEnv, endpoint, bindings, artifact.pin) });
        const host = { endpoint, child };
        hosts.add(host);
        await waitFor(async () => (await request(`${endpoint}/ready`)).status === 200, `${backend} managed readiness`, child);
        assert(child.output.includes("INITIALIZED"));
        host.startupMs = performance.now() - started;
        return host;
      };
      const stop = async (host) => {
        assert.equal((await request(`${host.endpoint}/stop`)).status, 200);
        await host.child.success();
        hosts.delete(host);
      };
      const geo = async (host, forwarded) => {
        const response = await request(`${host.endpoint}/geo`, {
          headers: { "cloudfront-viewer-country": "FR", ...(forwarded ? { Forwarded: `for=${forwarded}` } : {}) },
        });
        assert.equal(response.status, 200);
        const value = JSON.parse(response.bytes);
        assert.equal(value.immutable, true);
        assert.equal(value.credential_absent, true);
        assert.equal(value.geo.provider, "kv");
        return value.geo;
      };
      const hostA = await launch(a);
      const hostB = await launch(b);
      const servingCalls = () => backend === "celld" ? cellAudit.calls.filter((call) => call.role === "serving") : dynamoServe.calls;
      const budgetAudit = backend === "celld" ? cellAudit : dynamoServe;
      const blocked = budgetAudit.hold(4);
      const timedRequests = Array.from({ length: 4 }, () => geo(hostA, "192.0.2.238"));
      // Use a fully concurrent fixture before exercising ordinary cold or cached paths.
      const failures = Promise.all(timedRequests);
      failures.catch(() => {}); // Also join in finally if any earlier assertion fails.
      try {
        await within(blocked, "four lookup slots entered", 3000);
        const beforeSaturation = servingCalls().length;
        assert.equal((await geo(hostA, "192.0.2.238")).status, "provider_failure");
        assert.equal(servingCalls().length, beforeSaturation, "a saturated request must not begin I/O");
        for (const failure of await failures) assert.equal(failure.status, "provider_failure");
      } finally {
        budgetAudit.heldReads = null;
        await Promise.allSettled(timedRequests);
      }
      // The subsequent cold-path requests also prove timed-out work released its slots.
      const beforeMissing = servingCalls().length;
      assert.equal((await geo(hostA)).status, "missing_reader_ip");
      assert.equal((await geo(hostA, "unknown")).status, "missing_reader_ip");
      assert.equal(servingCalls().length, beforeMissing, "missing/untrusted reader must issue zero KV reads");
      await assert.rejects(request(`${hostA.endpoint}/geo-abort`, { headers: { Forwarded: "for=192.0.2.238" } }), /socket hang up/);
      assert.equal(servingCalls().length, beforeMissing, "aborted admission must issue zero KV reads");
      assert.equal((await geo(hostA, "192.0.2.1")).status, "no_match");
      assert.equal((await geo(hostA, "192.0.2.2")).status, "unsupported_geography");
      assert.equal((await geo(hostA, "198.51.100.1")).status, "no_match", "registered country must not substitute");
      const first = await geo(hostA, "192.0.2.0");
      assert.equal(first.country, "US");
      assert.equal(first.subdivision, "CA");
      assert.equal(first.trust_basis, "CheckedForwardedClient");
      assert.deepEqual(await geo(hostA, '"[::ffff:192.0.2.0]"'), first);
      const ipv6 = await geo(hostA, '"[2001:db8::1]"');
      assert.equal(ipv6.country, "GB");
      assert.equal(ipv6.subdivision, null);
      assert.equal((await geo(hostB, "192.0.2.0")).country, "DE");
      assert.deepEqual(await geo(hostA, "192.0.2.0"), first, "simultaneous B pin cannot change A cache");
      const rootKey = `geo/v1/${a.manifest.generation}/v4/${Buffer.from(a.manifest.ipv4.hash).toString("hex")}`;
      const leaves = a.inventory.filter((item) => item.key.includes("/v4/") && item.key !== rootKey);
      assert(leaves.length >= 2, "synthetic artifact must exercise uncached descendants");
      const coldLeaf = leaves.at(-1);
      const leafBytes = await readFile(join(a.dir, coldLeaf.key));
      await remove(coldLeaf.key);
      assert.equal((await geo(hostA, "192.0.2.238")).status, "provider_failure", "missing descendant is not a gap or fallback");
      await put(coldLeaf.key, Buffer.from("corrupt descendant"));
      assert.equal((await geo(hostA, "192.0.2.238")).status, "provider_failure", "corrupt descendant is not a gap or fallback");
      await put(coldLeaf.key, leafBytes);
      assert.equal((await geo(hostA, "192.0.2.238")).country, "US", "restored immutable descendant resolves");
      await stop(hostA);
      const restarted = await launch(a);
      assert.deepEqual(await geo(restarted, "192.0.2.0"), first, "restart retains explicit A pin");
      await stop(restarted);
      await stop(hostB);
      await verify(a);
      const rollback = await launch(a);
      assert.deepEqual(await geo(rollback, "192.0.2.0"), first, "reverified rollback to A");
      await stop(rollback);

      const rootBytes = await readFile(join(a.dir, rootKey));
      await remove(rootKey);
      const failedEndpoint = `http://127.0.0.1:${await port()}`;
      const failed = start(binaries.host, [], { env: hostEnvironment(servingEnv, failedEndpoint, bindings, a.pin) });
      assert.equal((await within(failed.done, "invalid root startup rejection")).code, 1);
      assert(!failed.output.includes("INITIALIZED"), "invalid root must fail before initialization/readiness");
      await assert.rejects(request(`${failedEndpoint}/ready`), /ECONNREFUSED/);
      await put(rootKey, rootBytes);
      if (backend === "dynamodb") {
        assert(servingCalls().some((call) => call.operation === "GetItem"));
        assert(servingCalls().some((call) => call.operation === "DescribeTable"));
        assert(servingCalls().every((call) => ["DescribeTable", "GetItem"].includes(call.operation)), "serving must issue no writes or scans");
      } else {
        assert(servingCalls().filter((call) => call.write).length === 2, "only the two intentional denied serving mutations");
      }
      assert.equal(hosts.size, 0);
      for (const artifact of [b, a]) {
        for (const item of artifact.inventory) await remove(item.key);
        for (const item of artifact.inventory) assert.equal(await read(item.key), null, "inventory retirement readback");
      }
      for (const artifact of licensed) {
        const startCall = audit.calls.length;
        const written = await run(binaries.tool, ["publish", artifact.dir, publisherFile], { env: publisherEnv }, 600_000)
          .then(JSON.parse)
          .catch((error) => {
            const calls = audit.calls.slice(startCall);
            throw new Error(`${backend} publish failed after ${calls.length} calls; last responses: ${JSON.stringify(calls.slice(-8))}`, { cause: error });
          });
        assert.equal(written.objects, artifact.inventory.length);
        const verifyAudit = backend === "celld" ? cellAudit : dynamoServe;
        const beforeVerify = verifyAudit.calls.length;
        const checked = await run(binaries.tool, [
          "verify-remote", artifact.dir, servingFile, artifact.source, "2048",
        ], { env: servingEnv }, 600_000)
          .then(JSON.parse)
          .catch((error) => {
            const calls = verifyAudit.calls.slice(beforeVerify);
            throw new Error(`${backend} verify failed after ${calls.length} calls; last responses: ${JSON.stringify(calls.slice(-8))}`, { cause: error });
          });
        assert.equal(checked.oracle_samples, 4096);
        const host = await launch(artifact);
        const startupRss = await peakRss(host.child.child.pid);
        const passes = [];
        // First pass starts with an empty serving cache. Repeating the same workload
        // measures reuse; neither pass claims production traffic or backend disk state.
        for (const pass of ["cold-cache-start", "repeated-pass"]) {
          const before = servingCalls().length;
          const durations = [];
          for (let offset = 0; offset < artifact.workload.length; offset += 4) {
            await Promise.all(artifact.workload.slice(offset, offset + 4).map(async (address) => {
              const startTime = performance.now();
              const outcome = await geo(host, address);
              assert(["located", "no_match", "unsupported_geography"].includes(outcome.status), "licensed-size lookup failed");
              durations.push(performance.now() - startTime);
            }));
          }
          const reads = servingCalls().slice(before);
          passes.push({ pass, requests: durations.length, ...latencyStats(durations),
            reads_per_request: reads.length / durations.length,
            backend_response_body_bytes_per_request: reads.reduce((total, call) => total + call.responseBytes, 0) / durations.length,
            peak_process_rss_bytes: await peakRss(host.child.child.pid) });
        }
        await stop(host);
        for (const item of artifact.inventory) await remove(item.key);
        // The run's loopback stores and temporary artifacts are also destroyed in finally.
        licensedResults.push({ backend, pin: artifact.pin, object_bytes: artifact.report.object_budget_bytes,
          objects: written.objects, encoded_bytes: artifact.report.encoded_bytes_including_manifest,
          ipv4_lookup_objects: artifact.report.ipv4_lookup_objects, ipv6_lookup_objects: artifact.report.ipv6_lookup_objects,
          source: artifact.manifest.source, inventory_sha256: hash(JSON.stringify(artifact.inventory)),
          oracle_samples: checked.oracle_samples, startup_ms: host.startupMs,
          startup_peak_process_rss_bytes: startupRss, request_concurrency: 4, passes });
        log(`${backend}: ${artifact.report.object_budget_bytes}-byte City objects published, verified and measured`);
      }
      results.push({ backend, pin: a.pin, objects: a.inventory.length, interrupted_resume: resumed,
        oracle_samples_per_generation: 512, serving_operations: [...new Set(servingCalls().map((call) => call.operation))],
        checks: ["interrupted-and-identical-publication", "conflict-preserved", "incomplete-tree-rejected", "read-only-serving",
          "managed-axum-ipv4-ipv6-mapped-gap-unsupported", "missing-reader-and-admission-abort-zero-reads",
          "missing-corrupt-descendant-provider-failure", "missing-root-prevents-readiness", "deadline-and-saturation-release-slots",
          "simultaneous-pins", "restart", "rollback", "inventory-retirement"] });
      log(`${backend}: synthetic publication, remote verification, managed requests and lifecycle passed`);
    }
    for (const [name, binary] of Object.entries(binaries)) assert.equal(hash(await readFile(binary)), fingerprints[name], `${name} executable changed during proof`);
    const report = { status: "passed", scope: licensedSource ? "loopback synthetic and licensed City local proof; no hosted qualification"
      : "loopback synthetic local proof only; no licensed data or hosted IAM qualification",
      binaries, sha256: fingerprints, dynamodb_image: DYNAMO_IMAGE,
      generations: artifacts.map((artifact) => ({ pin: artifact.pin, objects: artifact.inventory.length,
        encoded_bytes: artifact.inventory.reduce((total, item) => total + item.bytes, 0),
        inventory_sha256: hash(JSON.stringify(artifact.inventory)), source: artifact.manifest.source })),
      results, licensed_results: licensedResults };
    log(JSON.stringify(report, null, 2));
    return report;
  } finally {
    for (const child of children) child.kill();
    await Promise.allSettled([...children].map((child) => within(child.done, "cleanup child", 5000)));
    for (const server of servers.reverse()) {
      server.closeAllConnections?.();
      server.close();
    }
    // Cleanup uses the same cleared environment. Never touches unowned resources.
    const cleanupCommand = async (args) => {
      const child = new Process("docker", args, { cwd: work, env: base }, children);
      return child.success();
    };
    try {
      if (containerCreated) await cleanupCommand(["rm", "-f", container]);
      if (networkCreated) await cleanupCommand(["network", "rm", network]);
    } finally {
      await rm(work, { recursive: true, force: true });
      process.removeListener("SIGINT", onInterrupt);
      process.removeListener("SIGTERM", onInterrupt);
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runIntegration().catch((error) => { console.error(error.stack); process.exitCode = 1; });
}
