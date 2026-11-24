import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  Cleanup, DYNAMODB_IMAGE, Processes, fixtureEnvironment, parseArgs, renderBindings,
  renderFastly, renderWrangler, requireTestCount, withCleanup,
} from "./run_portable_record_contract.mjs";

const exec = promisify(execFile);
const runner = fileURLToPath(new URL("./run_portable_record_contract.mjs", import.meta.url));

test("full Viceroy execution is the default; staged host execution is explicit", () => {
  assert.deepEqual(parseArgs([]), { skipViceroy: false });
  assert.deepEqual(parseArgs(["--output", "artifacts", "--skip-viceroy"]), { output: resolve("artifacts"), skipViceroy: true });
  for (const args of [["--output"], ["--output", "--skip-viceroy"], ["--skip"], ["extra"]]) assert.throws(() => parseArgs(args), /usage/);
});

test("service test summaries must contain the exact expected nonzero count", () => {
  const summary = (count) => `test result: ok. ${count} passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.02s\n`;
  requireTestCount(summary(2), 2);
  for (const output of ["", summary(0), summary(1), summary(3), summary(2) + summary(2), summary(2).replace("0 ignored", "1 ignored")]) {
    assert.throws(() => requireTestCount(output, 2), /expected exactly 2/);
  }
});

test("bindings contain portable sources, exact fixture IDs and no hand-written pins", () => {
  const document = renderBindings({ dynamodbEndpoint: "http://127.0.0.1:8123", celldEndpoint: "https://127.0.0.1:9443", table: "portable-table" });
  assert.match(document, /\[kv.dynamodb_records\]/);
  assert.match(document, /namespace = "portable-contract"/);
  assert.match(document, /table = "portable-table"/);
  assert.match(document, /credentials = \{ secrets = "stores" \}/);
  assert.match(document, /\[kv.celld_records\]/);
  assert.match(document, /token_secret = \{ secrets = "stores" \}/);
  assert.doesNotMatch(document, /pinned|token_env|trusted-network|secret-key/);
});

test("Fastly fragment preserves the entire pinned document and derives secret names", () => {
  const document = 'version = 1\n# quotes " and \'\'\'\n[kv.celld_records.pinned]\ninstance = "AAAA"\n';
  const fragment = renderFastly(document, "test-token-quote\"\\value");
  const encoded = fragment.split("\n").find((line) => line.startsWith("EDGEZERO__STORE_BINDINGS = ")).split(" = ").slice(1).join(" = ");
  assert.equal(JSON.parse(encoded), document);
  assert.match(fragment, /manifest_version = 3/);
  assert.match(fragment, /\[local_server.config_stores.edgezero_store_bindings.contents\]/);
  for (const name of ["KV__DYNAMODB_RECORDS__AWS_ACCESS_KEY_ID", "KV__DYNAMODB_RECORDS__AWS_SECRET_ACCESS_KEY", "KV__CELLD_RECORDS__CELLD_TOKEN"]) assert.ok(fragment.includes(name));
  assert.match(fragment, /test-token-quote\\"\\\\value/);
});

test("celld policy hashes the token and grants exactly the fixture store operations", () => {
  const token = "fake-test-token-at-least-thirty-two-bytes";
  const wrangler = JSON.parse(renderWrangler(token));
  const policy = JSON.parse(wrangler.vars.EDGEZERO_STORE_POLICY);
  assert.equal(wrangler.main, "src/index.js");
  assert.equal(wrangler.no_bundle, true);
  assert.deepEqual(policy.stores, [{ alias: "records", namespace: "portable-contract" }]);
  assert.equal(policy.access.mode, "authenticated");
  assert.deepEqual(policy.access.tokens[0].grants, [{ store: "records", operations: ["read", "write", "delete", "list"] }]);
  assert.match(policy.access.tokens[0].sha256, /^[0-9a-f]{64}$/);
  assert.ok(!JSON.stringify(wrangler).includes(token));
});

test("environment isolates real credentials/proxies and supplies process-local CA trust", () => {
  const env = fixtureEnvironment({ PATH: "/bin", HOME: "/home/test", CARGO_TARGET_DIR: "/target", VICEROY_BIN: "/viceroy", AWS_PROFILE: "prod", AWS_SESSION_TOKEN: "prod-token", HTTPS_PROXY: "untrusted", SSL_CERT_FILE: "/wrong", RUSTUP_TOOLCHAIN: "old" }, {
    work: "/work", bindings: "/bindings", ca: "/cert", dynamodbEndpoint: "http://127.0.0.1:8123", token: "fake-token", config: "/fastly.toml",
  });
  assert.equal(env.CARGO_TARGET_DIR, "/target");
  assert.equal(env.RUSTUP_TOOLCHAIN, "1.98.1");
  assert.equal(env.SSL_CERT_FILE, "/cert");
  assert.equal(env.EDGEZERO_PORTABLE_RECORDS_CA, "/cert");
  assert.equal(env.EDGEZERO_PORTABLE_RECORDS_CONFIG, "/fastly.toml");
  assert.equal(env.EDGEZERO_PORTABLE_RECORDS_BINDINGS, "/bindings");
  assert.equal(env.AWS_ENDPOINT_URL_DYNAMODB, "http://127.0.0.1:8123");
  assert.equal(env.AWS_ACCESS_KEY_ID, "AKIDPORTABLEFIXTURE");
  assert.equal(env.KV__CELLD_RECORDS__CELLD_TOKEN, "fake-token");
  for (const name of ["AWS_PROFILE", "AWS_SESSION_TOKEN", "HTTPS_PROXY"]) assert.equal(env[name], undefined);
});

test("cleanup runs in reverse order after failure and still attempts every action", async () => {
  const calls = [];
  await assert.rejects(withCleanup(async (cleanup) => {
    cleanup.add(() => calls.push("directory"));
    cleanup.add(() => calls.push("docker"));
    cleanup.add(() => calls.push("processes"));
    throw new Error("suite failed");
  }), /suite failed/);
  assert.deepEqual(calls, ["processes", "docker", "directory"]);
  const cleanup = new Cleanup();
  cleanup.add(() => calls.push("last cleanup"));
  cleanup.add(() => { throw new Error("cleanup failure"); });
  await assert.rejects(cleanup.run(), /cleanup failed/);
  assert.equal(calls.at(-1), "last cleanup");
  await cleanup.run();
});

test("command failure and missing executables reject, never pass or leave owned children", async () => {
  const processes = new Processes();
  try {
    await assert.rejects(processes.run(process.execPath, ["-e", "process.exit(17)"]), /exit 17/);
    await assert.rejects(processes.run("/nonexistent-edgezero-command", []), /ENOENT/);
    assert.equal(processes.children.size, 0);
  } finally { await processes.stop(); }
});

test("required binaries are not silent skips", async () => {
  await assert.rejects(exec(process.execPath, [runner], { env: { PATH: "/nonexistent" } }), (error) => error.stderr.includes("CELLD_BIN"));
  await assert.rejects(exec(process.execPath, [runner], { env: { PATH: "/nonexistent", CELLD_BIN: "/fake/celld" } }), (error) => error.stderr.includes("VICEROY_BIN"));
});

async function fixture(t, hang = false) {
  const directory = await mkdtemp(join(tmpdir(), "edgezero-portable-unit-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const temporary = join(directory, "temporary");
  await mkdir(temporary);
  const log = join(directory, "docker.log");
  const binary = join(directory, "celld");
  await writeFile(binary, '#!/bin/sh\nif [ "$1" = "--version" ]; then echo "celld 0.6.3"; fi\n', { mode: 0o755 });
  await writeFile(join(directory, "docker"), `#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_DOCKER_LOG"
case "$1" in
  pull) ${hang ? "exit 0" : "exit 17"} ;;
  run) printf 'runtime-start\\n' >> "$MOCK_DOCKER_LOG"; sleep 60 ;;
  *) exit 0 ;;
esac
`, { mode: 0o755 });
  return { directory, temporary, log, env: { ...process.env, PATH: `${directory}:/usr/bin:/bin`, TMPDIR: temporary, CELLD_BIN: binary, MOCK_DOCKER_LOG: log } };
}

test("failed pinned-image acquisition propagates and removes disposable state", async (t) => {
  const setup = await fixture(t);
  await assert.rejects(exec(process.execPath, [runner, "--skip-viceroy", "--output", join(setup.directory, "evidence")], { env: setup.env }), (error) => error.stderr.includes("exit 17"));
  assert.ok((await readFile(setup.log, "utf8")).includes(`pull ${DYNAMODB_IMAGE}`));
  assert.deepEqual(await readdir(setup.temporary), []);
});

test("SIGINT kills an in-flight process group, removes a partially-started container and exits nonzero", async (t) => {
  const setup = await fixture(t, true);
  const child = spawn(process.execPath, [runner, "--skip-viceroy", "--output", join(setup.directory, "evidence")], { env: setup.env, stdio: "ignore" });
  const closed = new Promise((yes, no) => { child.once("error", no); child.once("close", yes); });
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  try {
    const deadline = Date.now() + 5000;
    while (!(await readFile(setup.log, "utf8").catch(() => "")).includes("runtime-start")) {
      assert.ok(Date.now() < deadline, "must reach synthetic docker run");
      await delay(20);
    }
    child.kill("SIGINT");
    assert.notEqual(await closed, 0);
    assert.match(await readFile(setup.log, "utf8"), /rm -f edgezero-portable-/);
    assert.deepEqual(await readdir(setup.temporary), []);
  } finally { clearTimeout(timer); child.kill("SIGKILL"); await closed; }
});
