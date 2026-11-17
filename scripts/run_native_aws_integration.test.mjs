import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { publishedEndpoint } from "./run_native_aws_integration.mjs";

const exec = promisify(execFile);
const runner = fileURLToPath(new URL("./run_native_aws_integration.mjs", import.meta.url));

test("published service ports must be one literal IPv4 loopback address", () => {
  assert.equal(publishedEndpoint("127.0.0.1:8000\n"), "http://127.0.0.1:8000");
  for (const address of [
    "0.0.0.0:8000", "localhost:8000", "[::1]:8000", "127.0.0.1:0",
    "127.0.0.1:65536", "127.0.0.1:8000\n0.0.0.0:8000", "127.0.0.1:8000/path", "",
  ]) {
    assert.throws(() => publishedEndpoint(address), /exclusively loopback/);
  }
});

test("missing Docker fails instead of silently skipping the suite", async () => {
  await assert.rejects(
    exec(process.execPath, [runner], { env: { PATH: "/nonexistent" }, timeout: 10_000 }),
    (error) => error.code !== 0 && error.stderr.includes("ENOENT"),
  );
});

async function fixture(t, {
  failPull = false, port = 8000, emptySuite = false, hang = false,
  suiteCode = 17, slowCleanup = false,
} = {}) {
  const directory = await mkdtemp(join(tmpdir(), "edgezero-runner-unit-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const temporary = join(directory, "temporary");
  await mkdir(temporary);
  const log = join(directory, "docker.log");
  // These commands only record runner decisions. Real-service coverage lives in Rust.
  await writeFile(join(directory, "docker"), `#!/bin/sh
printf '%s\\n' "$*" >> "$MOCK_DOCKER_LOG"
case "$1" in
  pull) exit ${failPull ? 1 : 0} ;;
  port) printf '127.0.0.1:${port}\\n' ;;
  logs) printf 'synthetic-service-log\\n' >&2 ;;
  rm) ${slowCleanup ? "printf 'cleanup-start\\n' >> \"$MOCK_DOCKER_LOG\"; sleep 0.2" : ":"} ;;
  *) exit 0 ;;
esac
`, { mode: 0o755 });
  await writeFile(join(directory, "cargo"), `#!/bin/sh
case "$*" in
  *--list*) ${emptySuite ? ":" : "printf 'generated_native_aws_emulator_contracts: test\\n'"} ;;
  *) ${hang ? "mkdir \"$TMPDIR/generated-project\"; printf log > \"$TMPDIR/generated-log\"; printf 'runtime-start\\n' >> \"$MOCK_DOCKER_LOG\"; sleep 60" : `exit ${suiteCode}`} ;;
esac
`, { mode: 0o755 });
  return {
    log, temporary,
    options: {
      env: { ...process.env, PATH: `${directory}:/usr/bin:/bin`, TMPDIR: temporary, MOCK_DOCKER_LOG: log },
      timeout: 15_000,
    },
  };
}

test("failed pinned image acquisition still cleans containers, network and temp files", async (t) => {
  const setup = await fixture(t, { failPull: true });
  await assert.rejects(exec(process.execPath, [runner], setup.options));
  const log = await readFile(setup.log, "utf8");
  assert.match(log, /pull amazon\/dynamodb-local@sha256:[a-f0-9]{64}/);
  assert.match(log, /rm -f edgezero-aws-/);
  assert.match(log, /network rm edgezero-aws-/);
  assert.deepEqual(await readdir(setup.temporary), []);
});

async function readyServer(t) {
  const server = http.createServer((request, response) => {
    request.resume();
    response.end(request.method === "POST" ? "{}" : "agent-ready\n");
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  return server.address().port;
}

test("suite failure is propagated and service stderr logs survive cleanup", async (t) => {
  const setup = await fixture(t, { port: await readyServer(t) });
  await assert.rejects(exec(process.execPath, [runner], setup.options), (error) => {
    assert.match(error.stderr, /Native AWS integration failed: exit 17/);
    assert.match(error.stderr, /synthetic-service-log/);
    return true;
  });
  const log = await readFile(setup.log, "utf8");
  assert.match(log, /network rm edgezero-aws-/);
  assert.doesNotMatch(log, /admin\.py|python|exec /);
  assert.deepEqual(await readdir(setup.temporary), []);
});

test("a zero-test suite fails the nonzero guard", async (t) => {
  const setup = await fixture(t, { port: await readyServer(t), emptySuite: true });
  await assert.rejects(exec(process.execPath, [runner], setup.options), (error) => {
    assert.match(error.stderr, /selected test suite contains zero tests/);
    return true;
  });
  assert.deepEqual(await readdir(setup.temporary), []);
});

async function interruptAt(setup, marker) {
  const child = spawn(process.execPath, [runner], { env: setup.options.env, stdio: "pipe" });
  child.stdout.resume();
  child.stderr.resume();
  const closed = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code));
  });
  const timer = setTimeout(() => child.kill("SIGKILL"), 12_000);
  try {
    const deadline = Date.now() + 5000;
    while (!(await readFile(setup.log, "utf8").catch(() => "")).includes(marker)) {
      assert.ok(Date.now() < deadline, `synthetic runner must reach ${marker}`);
      await delay(20);
    }
    child.kill("SIGTERM");
    assert.notEqual(await closed, 0);
    assert.match(await readFile(setup.log, "utf8"), /network rm edgezero-aws-/);
    assert.deepEqual(await readdir(setup.temporary), []);
  } finally {
    clearTimeout(timer);
    child.kill("SIGKILL");
    await closed;
  }
}

test("interrupting an active suite kills its process group and cleans generated files", async (t) => {
  const setup = await fixture(t, { port: await readyServer(t), hang: true });
  await interruptAt(setup, "runtime-start");
});

test("an interrupt during successful teardown cannot produce a green run", async (t) => {
  const setup = await fixture(t, { port: await readyServer(t), suiteCode: 0, slowCleanup: true });
  await interruptAt(setup, "cleanup-start");
});
