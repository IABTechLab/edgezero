import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { parse } from "yaml";

const root = fileURLToPath(new URL("../", import.meta.url));
const helper = join(root, "scripts/ci_viceroy.sh");
const digest = (value) => createHash("sha256").update(value).digest("hex");

function command(program, args, options = {}) {
  const result = spawnSync(program, args, { encoding: "utf8", ...options });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  return result.stdout.trim();
}

function workflow(path) {
  const value = parse(readFileSync(path, "utf8"), {
    version: "1.2",
    uniqueKeys: true,
  });
  assert.ok(
    value && typeof value === "object" && !Array.isArray(value),
    "workflow must be a mapping",
  );
  assert.ok(
    value.jobs && typeof value.jobs === "object" && !Array.isArray(value.jobs),
    "workflow jobs must be a mapping",
  );
  return value;
}

const tests = workflow(join(root, ".github/workflows/test.yml"));
const format = workflow(join(root, ".github/workflows/format.yml"));
const host = tests.jobs["host-tests"];
const adapters = tests.jobs["adapter-wasm-tests"];

const yamlContractName =
  "workflow YAML preserves trigger keys and scalar types";

test(yamlContractName, () => {
  assert.deepEqual(tests.on, {
    push: { branches: ["main"] },
    pull_request: { branches: ["main"] },
  });
  assert.equal(host.strategy["fail-fast"], false);
  const checkout = host.steps.find(
    (step) => step.uses === "actions/checkout@v6",
  );
  assert.equal(checkout.with["persist-credentials"], false);
  const toolchain = host.steps.find((step) =>
    step.uses?.startsWith("actions-rust-lang/setup-rust-toolchain@"),
  );
  assert.equal(toolchain.with.cache, "false");
  assert.equal(
    tests.concurrency.group,
    "${{ github.workflow }}-${{ github.ref }}",
  );
  const fetch = host.steps.find(
    (step) => step.name === "Fetch dependencies (locked)",
  );
  assert.ok(fetch.run.startsWith("cargo fetch --locked\n"));
  assert.ok(
    fetch.run.includes("--manifest-path examples/app-demo/Cargo.toml\n"),
  );
});

test("workflow parsing runs without external tools in PATH", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "edgezero-node-only-workflow-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const env = { ...process.env, PATH: directory };
  delete env.NODE_TEST_CONTEXT;
  const child = spawnSync(
    process.execPath,
    [
      "--test",
      "--test-reporter=tap",
      "--test-name-pattern",
      yamlContractName,
      fileURLToPath(import.meta.url),
    ],
    { cwd: root, encoding: "utf8", env, timeout: 10_000 },
  );
  assert.equal(child.status, 0, child.stderr || child.error?.message);
  assert.match(child.stdout, /^# pass 1$/m);
});

test("workflow parsing rejects malformed YAML, duplicate keys and non-mappings", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "edgezero-workflow-parser-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const path = join(directory, "workflow.yml");
  writeFileSync(path, "jobs: {}\n");
  assert.deepEqual(workflow(path), { jobs: {} });
  for (const source of [
    "jobs: [\n",
    "jobs:\n  duplicate: {}\n  duplicate: {}\n",
    "jobs: {}\njobs: {}\n",
    "jobs: []\n",
    "jobs: null\n",
    "[]\n",
    "",
  ]) {
    writeFileSync(path, source);
    assert.throws(() => workflow(path));
  }
});

const toolingInstall =
  "npm ci --prefix scripts --ignore-scripts --no-audit --no-fund";
const toolingTest = "node --test scripts/ci_tooling.test.mjs";

test("tooling dependencies are locked and installed before CI and local guards", () => {
  const owners = Object.values(tests.jobs).flatMap((job) =>
    job.steps.filter((step) => step.run?.includes(toolingInstall)),
  );
  assert.equal(owners.length, 1);
  assert.equal(owners[0].if, "matrix.suite == 'workspace'");
  assert.equal(owners[0]["continue-on-error"], undefined);
  assert.ok(
    owners[0].run.indexOf(toolingInstall) < owners[0].run.indexOf(toolingTest),
  );
  const local = readFileSync(join(root, "scripts/run_tests.sh"), "utf8");
  assert.ok(local.includes(toolingInstall));
  assert.ok(local.indexOf(toolingInstall) < local.indexOf(toolingTest));
  const manifest = JSON.parse(
    readFileSync(join(root, "scripts/package.json"), "utf8"),
  );
  const lock = JSON.parse(
    readFileSync(join(root, "scripts/package-lock.json"), "utf8"),
  );
  assert.equal(manifest.private, true);
  assert.deepEqual(Object.keys(manifest.dependencies), ["yaml"]);
  assert.match(manifest.dependencies.yaml, /^\d+\.\d+\.\d+$/);
  assert.equal(
    lock.packages["node_modules/yaml"].version,
    manifest.dependencies.yaml,
  );
  assert.match(lock.packages["node_modules/yaml"].integrity, /^sha512-/);
});

test("tooling install failure prevents the Node guard from running", (t) => {
  const step = host.steps.find((candidate) =>
    candidate.run?.includes(toolingTest),
  );
  assert.ok(step);
  const directory = mkdtempSync(join(tmpdir(), "edgezero-tooling-bootstrap-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const calls = join(directory, "calls.jsonl");
  for (const executable of ["npm", "node"]) {
    writeFileSync(
      join(directory, executable),
      `#!${process.execPath}
const fs = require("node:fs");
fs.appendFileSync(process.env.TOOLING_CALLS, JSON.stringify([${JSON.stringify(executable)}, ...process.argv.slice(2)]) + "\\n");
process.exit(${JSON.stringify(executable)} === "npm" ? Number(process.env.INSTALL_STATUS) : 0);
`,
      { mode: 0o755 },
    );
  }
  for (const status of [37, 0]) {
    writeFileSync(calls, "");
    const result = spawnSync(
      "/bin/bash",
      ["-euo", "pipefail", "-c", step.run],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          PATH: directory,
          TOOLING_CALLS: calls,
          INSTALL_STATUS: String(status),
        },
        timeout: 10_000,
      },
    );
    assert.equal(result.status, status, result.stderr || result.error?.message);
    const recorded = readFileSync(calls, "utf8")
      .trim()
      .split("\n")
      .map(JSON.parse);
    assert.deepEqual(
      recorded,
      status === 0
        ? [toolingInstall.split(" "), toolingTest.split(" ")]
        : [toolingInstall.split(" ")],
    );
  }
});

test("cross-runtime fixtures and acceptance assets have one integration-test home", () => {
  const home = join(root, "crates/edgezero-test-harness");
  for (const path of [
    "fixtures/reusable-app/Cargo.toml",
    "fixtures/request-fidelity/Cargo.toml",
    "acceptance/outbound-cutover-cases.json",
    "acceptance/ts-outbound-qualification-runbook.md",
  ]) {
    assert.ok(existsSync(join(home, path)), path);
  }
  for (const path of [
    "tests/fixtures/reusable-app",
    "tests/fixtures/request-fidelity",
  ]) {
    assert.equal(existsSync(join(root, path)), false, path);
  }
});

test("every adapter uses the same CI and local integration suite entrypoint", () => {
  const launcher = "scripts/test_integration.sh";
  assert.ok(existsSync(join(root, launcher)));
  assert.equal(
    existsSync(join(root, "scripts/smoke_test_reusable_app.sh")),
    false,
  );
  const local = readFileSync(join(root, "scripts/run_tests.sh"), "utf8");
  assert.doesNotMatch(
    local,
    /check_adapter_feature_matrix\.sh (cloudflare|fastly|spin) wasm32-/,
  );
  for (const adapter of ["axum", "fastly", "cloudflare", "spin"]) {
    const invocation = `${launcher} --adapter ${adapter} --suite ci`;
    assert.ok(local.includes(invocation), invocation);
  }
  const invocation = `${launcher} --adapter \${{ matrix.adapter }} --suite ci`;
  assert.equal(
    adapters.steps.filter((step) => step.run?.includes(invocation)).length,
    1,
  );
  assert.ok(
    host.steps.some(
      (step) =>
        step.if === "matrix.suite == 'axum'" &&
        step.run?.includes(`${launcher} --adapter axum --suite ci`),
    ),
  );
});

test("explicit Cargo caches are the sole owners in both workflows", () => {
  for (const flow of [tests, format]) {
    for (const job of Object.values(flow.jobs)) {
      const setups = job.steps.filter((s) =>
        s.uses?.startsWith("actions-rust-lang/setup-rust-toolchain@"),
      );
      if (!setups.length) continue;
      assert.equal(setups.length, 1);
      assert.equal(setups[0].with.cache, "false", job.name);
      assert.equal(
        job.steps.filter(
          (s) =>
            s.uses === "actions/cache@v5" &&
            s.with.path.includes("~/.cargo/registry/"),
        ).length,
        1,
      );
    }
  }
});

test("identical rustdoc and outbound/dependency audits execute only in the workspace host", () => {
  for (const token of [
    "cargo doc --offline --locked --workspace --all-features --no-deps",
    "bash scripts/check_outbound_legacy_api.sh",
    "node scripts/check_outbound_docs_contract.mjs",
    "node scripts/check_brotli_dependency_contract.mjs",
    "node scripts/check_flate_dependency_contract.mjs",
    "node scripts/check_serde_json_map_contract.mjs",
  ]) {
    const owners = [tests, format].flatMap((flow) =>
      Object.values(flow.jobs).flatMap((job) =>
        job.steps.filter((s) => s.run?.includes(token)),
      ),
    );
    assert.equal(owners.length, 1, token);
    assert.ok(host.steps.includes(owners[0]), token);
    assert.equal(owners[0].if, "matrix.suite == 'workspace'", token);
    if (token.startsWith("cargo doc")) {
      assert.equal(owners[0].env.RUSTDOCFLAGS, "-D warnings");
    }
  }
  assert.ok(
    host.steps.some((s) =>
      s.run?.includes("node --test scripts/ci_tooling.test.mjs"),
    ),
  );
});

test("adapter Clippy caches cover both isolated fixture build roots", () => {
  const cache = format.jobs["adapter-clippy"].steps.find(
    (step) => step.uses === "actions/cache@v5",
  );
  for (const path of [
    "crates/edgezero-test-harness/fixtures/reusable-app/target/",
    "crates/edgezero-test-harness/fixtures/request-fidelity/target/",
  ]) {
    assert.ok(cache.with.path.split("\n").includes(path), path);
  }
});

test("required host/check names stay intact and the aggregate fails closed", () => {
  assert.deepEqual(host.strategy.matrix.suite, [
    "workspace",
    "cli",
    "axum",
    "app-demo",
  ]);
  assert.equal(host.name, "${{ matrix.suite }} tests");
  assert.equal(adapters.name, "${{ matrix.adapter }} wasm tests");
  assert.equal(format.jobs.format.name, "cargo fmt");
  const aggregate = tests.jobs["cargo-test"];
  assert.equal(aggregate.name, "cargo test");
  assert.equal(aggregate.needs, "host-tests");
  assert.equal(aggregate.if, "always()");
  const gate = aggregate.steps.at(-1);
  assert.equal(gate.env.HOST_TESTS_RESULT, "${{ needs.host-tests.result }}");
  for (const result of ["success", "failure", "cancelled", "skipped", ""]) {
    const actual = spawnSync("bash", ["-euo", "pipefail", "-c", gate.run], {
      env: { ...process.env, HOST_TESTS_RESULT: result },
    });
    assert.equal(actual.status, result === "success" ? 0 : 1, result);
  }
});

test("Viceroy caches are isolated, exact-keyed, and never gate readiness execution", () => {
  const stock = adapters.steps.find(
    (s) => s.name === "Cache pinned stock Viceroy",
  );
  const repair = adapters.steps.find(
    (s) => s.name === "Cache isolated Viceroy readiness build",
  );
  assert.ok(stock, "stock executable needs a dedicated cache");
  assert.ok(repair, "repair inputs/artifacts need a dedicated cache");
  assert.equal(stock.with.path, "${{ runner.temp }}/viceroy-stock");
  assert.equal(repair.with.path, "${{ runner.temp }}/viceroy-readiness");
  for (const [cache, key] of [
    [stock, "stock-key"],
    [repair, "repair-key"],
  ]) {
    assert.equal(cache.uses, "actions/cache@v5");
    assert.equal(cache.if, "matrix.adapter == 'fastly'");
    assert.equal(cache.with["restore-keys"], undefined);
    assert.ok(cache.with.key.includes("runner.os"));
    assert.ok(cache.with.key.includes("runner.arch"));
    assert.ok(cache.with.key.includes(`steps.viceroy-version.outputs.${key}`));
  }
  assert.equal(
    adapters.env.VICEROY_REVISION,
    "a69d982d1d95e13c0fc9f92683b9118639bb4b04",
  );
  assert.equal(
    adapters.env.VICEROY_LOCK_SHA256,
    "d642d177081f42aec75d25c0d1124c013d3abcb081886251420a90e9c53c1a2a",
  );
  assert.equal(
    adapters.env.VICEROY_PATCH_SHA256,
    "8693ce34f82cc98dc272bf39379007b17a40dfc19138c55b445378fd9a163759",
  );
  for (const mode of ["keys", "stock", "repair"]) {
    const step = adapters.steps.find((s) =>
      s.run?.includes(`bash scripts/ci_viceroy.sh ${mode}`),
    );
    assert.ok(step, mode);
    assert.equal(step.if, "matrix.adapter == 'fastly'", mode);
    assert.equal(step["continue-on-error"], undefined);
  }
});

function fixture(t) {
  assert.ok(
    existsSync(helper),
    "CI Viceroy helper must validate cache identities and run regressions",
  );
  const directory = mkdtempSync(join(tmpdir(), "edgezero-ci-tooling-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const bin = join(directory, "bin");
  const workspace = join(directory, "workspace");
  const upstream = join(directory, "upstream");
  const temp = join(directory, "runner");
  for (const path of [
    bin,
    workspace,
    upstream,
    temp,
    join(workspace, "scripts"),
    join(upstream, "src"),
    join(workspace, "patches/viceroy"),
  ]) {
    mkdirSync(path, { recursive: true });
  }
  writeFileSync(
    join(workspace, ".tool-versions"),
    "rust 1.98.1\nviceroy 0.21.0\n",
  );
  copyFileSync(
    join(root, "scripts/run_test_nonzero.sh"),
    join(workspace, "scripts/run_test_nonzero.sh"),
  );
  writeFileSync(join(upstream, "Cargo.lock"), "fixture lock\n");
  writeFileSync(join(upstream, "Cargo.toml"), "fixture manifest\n");
  writeFileSync(join(upstream, "src/body.rs"), "stock body\n");
  const git = (...args) => command("git", ["-C", upstream, ...args]);
  git("init", "-q");
  git("add", ".");
  git(
    "-c",
    "user.name=CI Test",
    "-c",
    "user.email=ci@example.invalid",
    "commit",
    "-qm",
    "fixture",
  );
  git("tag", "v0.21.0");
  const revision = git("rev-parse", "HEAD");
  writeFileSync(join(upstream, "src/body.rs"), "repaired body\n");
  const patch = git("diff") + "\n";
  git("restore", "src/body.rs");
  writeFileSync(
    join(workspace, "patches/viceroy/0.21.0-body-readiness.patch"),
    patch,
  );
  writeFileSync(
    join(bin, "rustc"),
    '#!/usr/bin/env bash\nif [[ -n "${FAIL_RUSTC:-}" ]]; then exit 39; fi\nprintf "%s\\n" "${FAKE_RUSTC:-rustc 1.98.1 fixture}"\n',
    { mode: 0o755 },
  );
  writeFileSync(
    join(bin, "uname"),
    '#!/usr/bin/env bash\nif [[ -n "${FAIL_UNAME:-}" ]]; then exit 40; fi\nprintf "%s\\n" "${FAKE_PLATFORM:-Linux x86_64}"\n',
    { mode: 0o755 },
  );
  writeFileSync(
    join(bin, "cargo"),
    `#!${process.execPath}
const fs = require("node:fs"), path = require("node:path");
const args = process.argv.slice(2), env = process.env;
fs.appendFileSync(env.CARGO_LOG, JSON.stringify({ args, home: env.CARGO_HOME, target: env.CARGO_TARGET_DIR }) + "\\n");
if (env.FAIL_COMMAND === args[0] && (!env.FAIL_FILTER || args.includes(env.FAIL_FILTER))) process.exit(37);
if (args[0] === "--version") { console.log("cargo 1.98.1 fixture"); process.exit(0); }
if (args[0] === "install") {
  const root = args[args.indexOf("--root") + 1];
  fs.mkdirSync(path.join(root, "bin"), { recursive: true });
  fs.writeFileSync(path.join(root, "bin/viceroy"), "#!/usr/bin/env bash\\necho 'viceroy " + (env.INSTALLED_VERSION || "0.21.0") + "'\\n", { mode: 0o755 });
} else if (args[0] === "test") {
  const names = ["readiness_preserves_bytes_without_polling_the_source_again", "readiness_preserves_between_bytes_timeout_without_repolling_source"];
  if (!env.ZERO_TESTS) {
    for (const name of names) console.log("body::tests::" + name + (args.includes("--list") ? ": test" : ""));
    if (!args.includes("--list") && !env.MISSING_PASS) {
      for (const name of names) console.log("test body::tests::" + name + " ... ok");
    }
  }
  if (!args.includes("--list")) console.log("test result: ok. " + (env.ZERO_TESTS ? 0 : 2) + " passed; 0 failed; 0 ignored;");
} else if (args[0] === "build") {
  fs.mkdirSync(path.join(env.CARGO_TARGET_DIR, "debug"), { recursive: true });
  fs.writeFileSync(path.join(env.CARGO_TARGET_DIR, "debug/viceroy"), "repaired executable", { mode: 0o755 });
  if (env.MUTATE_STOCK) fs.appendFileSync(path.join(env.RUNNER_TEMP, "viceroy-stock/bin/viceroy"), "# mutated\\n");
} else if (args[0] !== "fetch") process.exit(99);
`,
    { mode: 0o755 },
  );
  const env = {
    ...process.env,
    PATH: `${bin}:${process.env.PATH}`,
    GITHUB_WORKSPACE: workspace,
    RUNNER_TEMP: temp,
    GITHUB_OUTPUT: join(directory, "output"),
    GITHUB_PATH: join(directory, "path"),
    CARGO_LOG: join(directory, "cargo.jsonl"),
    VICEROY_REVISION: revision,
    VICEROY_LOCK_SHA256: digest("fixture lock\n"),
    VICEROY_PATCH_SHA256: digest(patch),
    GIT_CONFIG_COUNT: "1",
    GIT_CONFIG_KEY_0: `url.${upstream}.insteadOf`,
    GIT_CONFIG_VALUE_0: "https://github.com/fastly/Viceroy.git",
  };
  function run(mode, extra = {}) {
    writeFileSync(env.GITHUB_OUTPUT, "");
    return spawnSync("bash", [helper, mode], {
      encoding: "utf8",
      env: { ...env, ...extra },
    });
  }
  function ok(mode, extra = {}) {
    const result = run(mode, extra);
    assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
    return readFileSync(env.GITHUB_OUTPUT, "utf8");
  }
  const calls = () =>
    readFileSync(env.CARGO_LOG, "utf8").trim().split("\n").map(JSON.parse);
  const stock = join(temp, "viceroy-stock/bin/viceroy");
  return { directory, workspace, temp, env, run, ok, calls, stock };
}

test("stock cache miss installs locked pin; verified hit preserves the exact binary", (t) => {
  const f = fixture(t);
  f.ok("stock");
  const before = readFileSync(f.stock);
  f.ok("stock");
  const installs = f.calls().filter((c) => c.args[0] === "install");
  assert.equal(installs.length, 1);
  assert.ok(installs[0].args.includes("--locked"));
  assert.ok(installs[0].args.includes("=0.21.0"));
  assert.deepEqual(readFileSync(f.stock), before);
  assert.ok(
    readFileSync(f.env.GITHUB_PATH, "utf8").includes(
      join(f.temp, "viceroy-stock/bin"),
    ),
  );
});

for (const damage of ["binary", "version", "identity", "compiler"]) {
  test(`stock cache revalidates ${damage} rather than trusting a cache hit`, (t) => {
    const f = fixture(t);
    f.ok("stock");
    if (damage === "binary")
      writeFileSync(f.stock, "#!/bin/sh\necho 'viceroy 0.21.0'\n# changed\n");
    if (damage === "version") {
      writeFileSync(f.stock, "#!/bin/sh\necho 'viceroy 0.21.00'\n");
      writeFileSync(
        join(f.temp, "viceroy-stock/executable.sha256"),
        digest(readFileSync(f.stock)) + "\n",
      );
    }
    if (damage === "identity")
      rmSync(join(f.temp, "viceroy-stock/inputs.sha256"));
    f.ok(
      "stock",
      damage === "compiler" ? { FAKE_RUSTC: "rustc different compiler" } : {},
    );
    assert.equal(f.calls().filter((c) => c.args[0] === "install").length, 2);
  });
}

test("stock install failure or wrong installed version cannot publish a usable runtime", (t) => {
  const f = fixture(t);
  assert.equal(f.run("stock", { FAIL_COMMAND: "install" }).status, 37);
  assert.notEqual(f.run("stock", { INSTALLED_VERSION: "0.21.00" }).status, 0);
  assert.equal(readFileSync(f.env.GITHUB_OUTPUT, "utf8"), "");
  assert.equal(existsSync(f.env.GITHUB_PATH), false);
});

test("cache keys bind compiler/platform/upstream/lock/patch/profile, not test results", (t) => {
  const f = fixture(t);
  const original = f.ok("keys");
  const parse = (output) =>
    Object.fromEntries(
      output
        .trim()
        .split("\n")
        .map((line) => line.split("=")),
    );
  const keys = parse(original);
  assert.match(keys["stock-key"], /^[a-f0-9]{64}$/);
  assert.match(keys["repair-key"], /^[a-f0-9]{64}$/);
  assert.notEqual(keys["stock-key"], keys["repair-key"]);
  for (const changed of [
    { FAKE_RUSTC: "rustc different compiler" },
    { FAKE_PLATFORM: "Linux aarch64" },
    { RUSTFLAGS: "-C opt-level=1" },
    { CARGO_PROFILE_DEV_OPT_LEVEL: "2" },
  ]) {
    const next = parse(f.ok("keys", changed));
    for (const key of ["stock-key", "repair-key"])
      assert.notEqual(next[key], keys[key]);
  }
  for (const variable of ["VICEROY_REVISION", "VICEROY_LOCK_SHA256"]) {
    assert.notEqual(
      parse(f.ok("keys", { [variable]: "different" }))["repair-key"],
      keys["repair-key"],
    );
  }
  const patch = join(
    f.workspace,
    "patches/viceroy/0.21.0-body-readiness.patch",
  );
  const modified = readFileSync(patch, "utf8") + "\n";
  writeFileSync(patch, modified);
  assert.notEqual(
    parse(f.ok("keys", { VICEROY_PATCH_SHA256: digest(modified) }))[
      "repair-key"
    ],
    keys["repair-key"],
  );
  const source = readFileSync(helper, "utf8");
  assert.match(source, /profile=dev debug=0 incremental=0/);
});

for (const [failure, status] of [
  [{ FAIL_RUSTC: "1" }, 39],
  [{ FAIL_COMMAND: "--version" }, 37],
  [{ FAIL_UNAME: "1" }, 40],
]) {
  test(`cache keys reject incomplete toolchain/platform identity ${JSON.stringify(failure)}`, (t) => {
    const f = fixture(t);
    assert.equal(f.run("keys", failure).status, status);
    assert.equal(readFileSync(f.env.GITHUB_OUTPUT, "utf8"), "");
  });
}

test("repair miss and hit both execute guarded regressions and build without changing stock", (t) => {
  const f = fixture(t);
  f.ok("stock");
  const before = readFileSync(f.stock);
  for (let run = 1; run <= 2; run++) {
    assert.match(
      f.ok("repair"),
      /executable=.*viceroy-readiness\/target\/debug\/viceroy/,
    );
    const executed = f
      .calls()
      .filter((c) => c.args[0] === "test" && !c.args.includes("--list"));
    assert.equal(executed.length, 2 * run);
    for (const call of executed) {
      assert.equal(call.home, join(f.temp, "viceroy-readiness/cargo-home"));
      assert.equal(call.target, join(f.temp, "viceroy-readiness/target"));
      assert.ok(
        call.args.includes("--offline") && call.args.includes("--locked"),
      );
    }
    assert.equal(f.calls().filter((c) => c.args[0] === "build").length, run);
    assert.deepEqual(readFileSync(f.stock), before);
  }
});

for (const failure of [
  { FAIL_COMMAND: "fetch" },
  { FAIL_COMMAND: "test", FAIL_FILTER: "body::tests::readiness_" },
  { FAIL_COMMAND: "test", FAIL_FILTER: "body::tests::" },
  { FAIL_COMMAND: "build" },
  { ZERO_TESTS: "1" },
  { MISSING_PASS: "1" },
  { MUTATE_STOCK: "1" },
]) {
  test(`warm repair fails closed for ${JSON.stringify(failure)}`, (t) => {
    const f = fixture(t);
    f.ok("stock");
    f.ok("repair");
    const result = f.run("repair", failure);
    assert.equal(result.status, failure.FAIL_COMMAND ? 37 : 1, result.stderr);
    assert.equal(readFileSync(f.env.GITHUB_OUTPUT, "utf8"), "");
  });
}

for (const variable of [
  "VICEROY_REVISION",
  "VICEROY_LOCK_SHA256",
  "VICEROY_PATCH_SHA256",
]) {
  test(`repair rejects mismatched ${variable} before testing or publishing`, (t) => {
    const f = fixture(t);
    f.ok("stock");
    assert.notEqual(f.run("repair", { [variable]: "0".repeat(64) }).status, 0);
    assert.equal(f.calls().filter((c) => c.args[0] === "test").length, 0);
    assert.equal(readFileSync(f.env.GITHUB_OUTPUT, "utf8"), "");
  });
}
