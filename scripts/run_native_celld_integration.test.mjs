import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { fixtureEnvironment, runCommand, runIntegration } from "./run_native_celld_integration.mjs";

const binary = resolve("fixture-celld");

function fixture(extra = {}) {
  const calls = [];
  const options = {
    env: { CELLD_BIN: binary, PATH: "/fixture/path", HOME: "/fixture/home", AWS_SECRET_ACCESS_KEY: "must-not-propagate", HTTP_PROXY: "must-not-propagate", SSL_CERT_FILE: "must-not-propagate" },
    executable: async () => {},
    digest: async () => "f".repeat(64),
    command: async (command, args, options) => {
      calls.push({ command, args, options });
      return { stdout: "--store-api-config PATH\n", stderr: "" };
    },
    log: () => {},
    ...extra,
  };
  return { calls, options };
}

test("fixture environment excludes deployment credentials, proxies and inherited trust inputs", () => {
  assert.deepEqual(fixtureEnvironment({
    PATH: "path", HOME: "home", CARGO_HOME: "cargo", RUSTUP_TOOLCHAIN: "1.95.0", CELLD_BIN: "ignored",
    AWS_ACCESS_KEY_ID: "secret", CELLD_APP_TOKEN: "secret", ALL_PROXY: "proxy", SSL_CERT_FILE: "ca",
  }, binary), { PATH: "path", HOME: "home", CARGO_HOME: "cargo", RUSTUP_TOOLCHAIN: "1.95.0", CELLD_BIN: binary });
});

test("missing binary is a failed prerequisite, never a passing skip", async () => {
  const { calls, options } = fixture({ env: {} });
  await assert.rejects(runIntegration(options), /CELLD_BIN must explicitly/);
  assert.equal(calls.length, 0);
});

test("unavailable executable stops before any command", async () => {
  const { calls, options } = fixture({ executable: async () => { throw new Error("not executable"); } });
  await assert.rejects(runIntegration(options), /not executable/);
  assert.equal(calls.length, 0);
});

test("vanilla celld cannot masquerade as embedded API acceptance", async () => {
  const { options } = fixture({ command: async () => ({ stdout: "unmodified celld help", stderr: "" }) });
  await assert.rejects(runIntegration(options), /no embedded scoped API/);
});

test("isolation precedes the exact nonzero generated TLS proof", async () => {
  const { calls, options } = fixture();
  await runIntegration(options);
  assert.equal(calls.length, 3);
  assert.equal(calls[0].command, binary);
  assert.deepEqual(calls[1].args, ["scripts/check_celld_scoped_isolation.sh", binary]);
  assert.equal(calls[2].command, "scripts/run_test_nonzero.sh");
  assert.equal(calls[2].args[0], "--ignored");
  const sentinel = "generated_native_consumer_uses_embedded_celld_over_tls";
  assert.equal(calls[2].args[1], sentinel);
  assert.equal(calls[2].args.at(-1), sentinel, "test filter must exclude the independent deterministic suite");
  assert.ok(calls[2].args.includes("--offline"));
  for (const call of calls) {
    assert.equal(call.options.env.AWS_SECRET_ACCESS_KEY, undefined);
    assert.equal(call.options.env.HTTP_PROXY, undefined);
    assert.equal(call.options.env.SSL_CERT_FILE, undefined);
  }
});

test("an isolation failure prevents generated acceptance", async () => {
  let count = 0;
  const { options } = fixture({ command: async () => {
    count += 1;
    if (count === 2) throw new Error("isolation unavailable");
    return { stdout: "--store-api-config", stderr: "" };
  } });
  await assert.rejects(runIntegration(options), /isolation unavailable/);
  assert.equal(count, 2);
});

test("changing the tested executable invalidates acceptance", async () => {
  let count = 0;
  const { options } = fixture({ digest: async () => (++count === 1 ? "a" : "b").repeat(64) });
  await assert.rejects(runIntegration(options), /executable changed/);
});

test("command failure is observable", async () => {
  await assert.rejects(runCommand(process.execPath, ["-e", "process.exit(7)"], {
    cwd: process.cwd(), env: {}, timeoutMs: 5_000,
  }), /code=7/);
});

test("command timeout terminates and reaps its owned descendant", { skip: process.platform === "win32" }, async () => {
  const root = await mkdtemp(join(tmpdir(), "celld-runner-test-"));
  const pidFile = join(root, "child.pid");
  try {
    const source = [
      "const {spawn}=require('node:child_process');",
      "const fs=require('node:fs');",
      "const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'});",
      "fs.writeFileSync(process.argv[1],String(child.pid));",
      "process.on('SIGTERM',()=>{child.once('exit',()=>process.exit(0));child.kill('SIGTERM');});",
      "setInterval(()=>{},1000);",
    ].join("");
    await assert.rejects(runCommand(process.execPath, ["-e", source, pidFile], {
      cwd: root, env: {}, timeoutMs: 500,
    }), /deadline exceeded/);
    const pid = Number(await readFile(pidFile, "utf8"));
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
