#!/usr/bin/env node
// Disposable real-service acceptance. CELLD_BIN must select official celld 0.6.3.
// VICEROY_BIN is required unless --skip-viceroy is used for staged host checks.
import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { createWriteStream } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { fixtureClient } from "./native_aws_test_admin.mjs";
import { publishedEndpoint } from "./run_native_aws_integration.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
// DynamoDB Local 3.3.1, same multi-architecture index digest as the native runner.
export const DYNAMODB_IMAGE = "amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab";
const accessKey = "AKIDPORTABLEFIXTURE";
const secretKey = "portable-fixture-secret";

export function parseArgs(argv) {
  const options = { skipViceroy: false };
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    if (name === "--skip-viceroy") options.skipViceroy = true;
    else if (name === "--output" && argv[index + 1] && !argv[index + 1].startsWith("--")) options.output = resolve(argv[++index]);
    else throw new Error("usage: run_portable_record_contract.mjs [--skip-viceroy] [--output DIR]");
  }
  return options;
}

export function requireTestCount(output, count) {
  const summaries = [...output.matchAll(/^test result: ok\. (\d+) passed; 0 failed; 0 ignored;/gm)];
  if (summaries.length !== 1 || Number(summaries[0][1]) !== count) {
    throw new Error(`expected exactly ${count} passing tests in one nonignored summary`);
  }
}

const quoted = (value) => JSON.stringify(value);
export function renderBindings({ dynamodbEndpoint, celldEndpoint, table }) {
  return `version = 1

[kv.dynamodb_records]
provider = "aws-dynamodb-records"
region = "us-east-1"
table = ${quoted(table)}
namespace = "portable-contract"
endpoint = ${quoted(dynamodbEndpoint)}
credentials = { secrets = "stores" }

[kv.celld_records]
provider = "celld-records"
endpoint = ${quoted(celldEndpoint)}
store = "records"
access = { mode = "authenticated", token_secret = { secrets = "stores" } }
`;
}

export function renderFastly(document, token) {
  // A TOML basic string safely escapes arbitrary document contents, including quotes.
  return `manifest_version = 3
name = "portable-record-contract"
language = "rust"
[local_server]
[local_server.backends]
[local_server.config_stores.edgezero_store_bindings]
format = "inline-toml"
[local_server.config_stores.edgezero_store_bindings.contents]
EDGEZERO__STORE_BINDINGS = ${quoted(document)}
[[local_server.secret_stores.stores]]
key = "KV__DYNAMODB_RECORDS__AWS_ACCESS_KEY_ID"
data = ${quoted(accessKey)}
[[local_server.secret_stores.stores]]
key = "KV__DYNAMODB_RECORDS__AWS_SECRET_ACCESS_KEY"
data = ${quoted(secretKey)}
[[local_server.secret_stores.stores]]
key = "KV__CELLD_RECORDS__CELLD_TOKEN"
data = ${quoted(token)}
`;
}

export function fixtureEnvironment(source, { work, bindings, ca, dynamodbEndpoint, token, config }) {
  const env = {};
  for (const [name, value] of Object.entries(source)) {
    if (["PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME", "RUSTC", "RUSTDOC", "RUSTFLAGS", "CARGO_BUILD_JOBS", "CARGO_TARGET_DIR", "EDGEZERO_GENERATED_TEST_TARGET_DIR", "VICEROY_BIN"].includes(name)) env[name] = value;
  }
  return {
    ...env, RUSTUP_TOOLCHAIN: "1.98.1", CARGO_NET_OFFLINE: "true", RUST_TEST_THREADS: "1",
    TMPDIR: work, AWS_ACCESS_KEY_ID: accessKey, AWS_SECRET_ACCESS_KEY: secretKey,
    AWS_REGION: "us-east-1", AWS_DEFAULT_REGION: "us-east-1", AWS_EC2_METADATA_DISABLED: "true",
    AWS_CONFIG_FILE: join(work, "empty-aws-config"), AWS_SHARED_CREDENTIALS_FILE: join(work, "empty-aws-credentials"),
    AWS_ENDPOINT_URL_DYNAMODB: dynamodbEndpoint, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS: "false",
    AWS_ACCOUNT_ID_ENDPOINT_MODE: "disabled", SSL_CERT_FILE: ca,
    KV__CELLD_RECORDS__CELLD_TOKEN: token, EDGEZERO_PORTABLE_RECORDS_BINDINGS: bindings,
    EDGEZERO_PORTABLE_RECORDS_CONFIG: config, EDGEZERO_PORTABLE_RECORDS_CA: ca,
  };
}

export function renderWrangler(token) {
  const policy = { version: 1, stores: [{ alias: "records", namespace: "portable-contract" }], access: {
    mode: "authenticated", tokens: [{ id: "portable-contract", sha256: createHash("sha256").update(token).digest("hex"),
      grants: [{ store: "records", operations: ["read", "write", "delete", "list"] }] }],
  } };
  return JSON.stringify({ name: "edgezero-store", main: "src/index.js", no_bundle: true,
    compatibility_date: "2026-10-01", durable_objects: { bindings: [{ name: "EDGEZERO_STORE", class_name: "EdgeZeroStore" }] },
    migrations: [{ tag: "v1", new_sqlite_classes: ["EdgeZeroStore"] }], vars: { EDGEZERO_STORE_POLICY: JSON.stringify(policy) },
  }, null, 2);
}

export class Cleanup {
  actions = [];
  add(action) { this.actions.push(action); }
  async run() {
    const failures = [];
    for (const action of this.actions.splice(0).reverse()) {
      try { await action(); } catch (error) { failures.push(error); }
    }
    if (failures.length) throw new AggregateError(failures, "portable service cleanup failed");
  }
}

export async function withCleanup(body) {
  const cleanup = new Cleanup();
  try { return await body(cleanup); } finally { await cleanup.run(); }
}

function signalGroup(pid, signal) {
  if (!Number.isInteger(pid)) return;
  try { process.kill(-pid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
}

// Every command owns a process group, including cargo, celld and its descendants.
export class Processes {
  children = new Set();
  interrupted;
  constructor(redactions = []) { this.redactions = redactions; }
  check() { if (this.interrupted) throw new Error(`Interrupted by ${this.interrupted}`); }
  interrupt(signal) {
    this.interrupted = signal;
    for (const child of this.children) signalGroup(child.pid, "SIGTERM");
    this.force ??= setTimeout(() => this.killAll(), 2000);
  }
  killAll() { for (const child of this.children) signalGroup(child.pid, "SIGKILL"); }
  async stop() {
    clearTimeout(this.force);
    this.killAll();
    await Promise.allSettled([...this.children].map((child) => new Promise((done) => {
      child.once("close", done);
    })));
  }
  start(command, args, { cwd = root, env = process.env, log, timeoutMs = 20 * 60_000 } = {}) {
    this.check();
    const child = spawn(command, args, { cwd, env, detached: true, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    const sink = log ? createWriteStream(log, { mode: 0o600 }) : undefined;
    this.children.add(child);
    const consume = (chunk) => {
      let text = chunk.toString();
      for (const secret of this.redactions) text = text.replaceAll(secret, "[redacted]");
      output = (output + text).slice(-1024 * 1024);
      sink?.write(text);
    };
    child.stdout.on("data", consume);
    child.stderr.on("data", consume);
    const done = new Promise((resolveCommand, reject) => {
      const timer = setTimeout(() => { signalGroup(child.pid, "SIGKILL"); }, timeoutMs);
      child.once("error", (error) => { clearTimeout(timer); sink?.end(); this.children.delete(child); reject(error); });
      child.once("close", (code, signal) => {
        clearTimeout(timer);
        sink?.end();
        // Kill descendants even if their immediate parent exited successfully.
        signalGroup(child.pid, "SIGKILL");
        this.children.delete(child);
        if (code === 0 && !this.interrupted) resolveCommand(output);
        else reject(new Error(`${command} failed: ${this.interrupted ?? signal ?? `exit ${code}`}${log ? `; see ${log}` : ""}`));
      });
    });
    // Long-lived services can exit while another command is running.
    done.catch(() => {});
    return { child, done, output: () => output };
  }
  async run(command, args, options) { return this.start(command, args, options).done; }
}

async function waitForLine(processes, service, marker) {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    processes.check();
    const line = service.output().split(/\r?\n/).find((item) => item.includes(marker));
    if (line) return line.split(marker)[1].trim().split(/\s/)[0];
    if (service.child.exitCode !== null || service.child.signalCode !== null) throw new Error(`service exited before ${marker}`);
    await delay(50);
  }
  throw new Error(`deadline waiting for ${marker}`);
}

async function reservePort() {
  const listener = net.createServer();
  await new Promise((yes, no) => { listener.once("error", no); listener.listen(0, "127.0.0.1", yes); });
  const port = listener.address().port;
  await new Promise((yes) => listener.close(yes));
  return port;
}

export async function main(argv = process.argv.slice(2), source = process.env) {
  const options = parseArgs(argv);
  if (!source.CELLD_BIN || !isAbsolute(source.CELLD_BIN)) throw new Error("CELLD_BIN must select an absolute official celld 0.6.3 binary");
  if (!options.skipViceroy && (!source.VICEROY_BIN || !isAbsolute(source.VICEROY_BIN))) throw new Error("VICEROY_BIN must select the pinned Viceroy binary");
  const output = options.output ?? await mkdtemp(join(tmpdir(), "edgezero-portable-evidence-"));
  await mkdir(output, { recursive: true });
  console.log(`Portable record evidence: ${output}`);
  const token = randomBytes(32).toString("hex");
  const processes = new Processes([token, secretKey]);
  const onInt = () => processes.interrupt("SIGINT");
  const onTerm = () => processes.interrupt("SIGTERM");
  process.on("SIGINT", onInt);
  process.on("SIGTERM", onTerm);
  try {
    await withCleanup(async (cleanup) => {
      const work = await mkdtemp(join(tmpdir(), "edgezero-portable-"));
      cleanup.add(() => rm(work, { recursive: true, force: true }));
      const container = `edgezero-portable-${work.split("/").at(-1)}`;
      // Register before acquisition, so partial docker starts are also removed.
      const docker = (args, timeoutMs = 30_000) => processes.run("docker", args, { timeoutMs });
      let containerCreated = false;
      cleanup.add(async () => {
        if (containerCreated) {
          // Cleanup must still run after an interrupt, independently of command readiness.
          const cleaning = new Processes();
          await cleaning.run("docker", ["rm", "-f", container], { timeoutMs: 30_000 });
        }
      });
      cleanup.add(() => processes.stop());
      const version = await processes.run(source.CELLD_BIN, ["--version"], { timeoutMs: 10_000 });
      if (version.trim() !== "celld 0.6.3") throw new Error("CELLD_BIN must report celld 0.6.3");
      const help = await processes.run(source.CELLD_BIN, ["--help"], { timeoutMs: 10_000 });
      if (help.includes("--store-api-config")) throw new Error("modified celld builds are not accepted");
      const celldSha256 = createHash("sha256").update(await readFile(source.CELLD_BIN)).digest("hex");
      await writeFile(join(output, "services.json"), JSON.stringify({ celld: version.trim(), celldSha256, dynamodbImage: DYNAMODB_IMAGE, qualification: false }, null, 2));
      await docker(["info"]);
      await docker(["pull", DYNAMODB_IMAGE], 180_000);
      containerCreated = true;
      await docker(["run", "-d", "--name", container, "--publish", "127.0.0.1::8000", DYNAMODB_IMAGE,
        "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry"]);
      const dynamodbEndpoint = publishedEndpoint(await docker(["port", container, "8000/tcp"]));
      const admin = fixtureClient(dynamodbEndpoint, "dynamodb");
      const deadline = Date.now() + 60_000;
      for (;;) {
        processes.check();
        try { await admin("ListTables"); break; } catch {
          if (Date.now() >= deadline) throw new Error("DynamoDB Local readiness deadline");
          await delay(200);
        }
      }
      const table = "portable-record-contract";
      await admin("CreateTable", { TableName: table, BillingMode: "PAY_PER_REQUEST",
        AttributeDefinitions: ["pk", "sk"].map((name) => ({ AttributeName: name, AttributeType: "S" })),
        KeySchema: [{ AttributeName: "pk", KeyType: "HASH" }, { AttributeName: "sk", KeyType: "RANGE" }],
      });
      const service = join(work, "service");
      await mkdir(join(service, "src"), { recursive: true });
      await copyFile(join(root, "services/celld-store/src/index.js"), join(service, "src/index.js"));
      await writeFile(join(service, "wrangler.json"), renderWrangler(token));
      const port = await reservePort();
      const celld = processes.start(source.CELLD_BIN, ["dev", ".", "--no-watch", "--logs", "--port", String(port)], {
        cwd: service, env: { PATH: source.PATH, HOME: join(work, "celld-home"), NO_COLOR: "1" }, log: join(output, "celld.log"),
      });
      const address = await waitForLine(processes, celld, "celld listening on ");
      if (address !== `127.0.0.1:${port}`) throw new Error("unexpected celld listener");
      const ca = join(output, "terminator-cert.pem");
      const key = join(work, "terminator-key.pem");
      const cert = (certPath, keyPath) => processes.run("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=portable-celld", "-addext", "subjectAltName=IP:127.0.0.1", "-addext", "basicConstraints=critical,CA:FALSE",
        "-addext", "extendedKeyUsage=serverAuth", "-keyout", keyPath, "-out", certPath]);
      await cert(ca, key);
      const wrongCa = join(work, "unrelated-cert.pem");
      await cert(wrongCa, join(work, "unrelated-key.pem"));
      const tls = processes.start(process.execPath, [join(root, "scripts/celld_store_tls_terminator.mjs"), "--cert", ca, "--key", key, "--target", address], { log: join(output, "tls.log") });
      const celldEndpoint = `https://${await waitForLine(processes, tls, "listening on ")}`;
      const bindings = join(work, "bindings.toml");
      const config = join(work, "fastly.toml");
      await writeFile(bindings, renderBindings({ dynamodbEndpoint, celldEndpoint, table }));
      const env = fixtureEnvironment(source, { work, bindings, ca, dynamodbEndpoint, token, config });
      await processes.run("cargo", ["run", "--offline", "--locked", "-p", "edgezero-cli", "--bin", "edgezero", "--", "config", "bindings", "pin", "--adapter", "fastly", "--store-bindings", bindings], { env, log: join(output, "pin.log") });
      const document = await readFile(bindings, "utf8");
      await writeFile(join(output, "bindings.toml"), document);
      await writeFile(config, renderFastly(document, token), { mode: 0o600 });
      const tests = async (names, filter, filename, overrides = {}) => {
        const result = await processes.run(join(root, "scripts/run_test_nonzero.sh"), [
          "--ignored", names.join("\n"), "cargo", "test", "--offline", "--locked", "-p", "edgezero-adapter-axum", "--test", "portable_interop", filter,
        ], { env: { ...env, ...overrides }, log: join(output, filename) });
        requireTestCount(result, names.length);
      };
      if (!options.skipViceroy) {
        await tests(["portable_native_seed_rows"], "portable_native_seed_rows", "seed.log");
        await processes.run(join(root, "scripts/test_integration.sh"), ["--adapter", "fastly", "--suite", "portable-record-contract", "--output", join(output, "viceroy")], { env, log: join(output, "harness.log") });
        await readFile(join(output, "viceroy/summary.json"));
      }
      await tests(["portable_celld_real_service_interop", "portable_dynamodb_real_service_interop"], "real_service_interop", "interop.log");
      await tests(["portable_celld_rejects_untrusted_certificate"], "rejects_untrusted_certificate", "untrusted-cert.log", { SSL_CERT_FILE: wrongCa });
      processes.check();
      await writeFile(join(output, "result.json"), JSON.stringify({ hostInterop: "passed", untrustedCertificate: "rejected", viceroy: options.skipViceroy ? "not-run" : "passed", qualification: false }, null, 2));
    });
    processes.check();
    console.log(options.skipViceroy ? "Host interop passed; Viceroy explicitly not run" : "Portable record contracts and host interop passed");
  } finally {
    await processes.stop();
    process.off("SIGINT", onInt);
    process.off("SIGTERM", onTerm);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => { console.error(error.message); process.exitCode = 1; });
}
