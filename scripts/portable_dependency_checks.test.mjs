import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const helper = fileURLToPath(
  new URL("./portable_dependency_checks.sh", import.meta.url),
);

const signingAssertion = readFileSync(
  new URL("./check_native_store_dependency_contract.sh", import.meta.url),
  "utf8",
).match(/^assert_sigv4\(\) \{\n[\s\S]*?^\}/m)[0];

for (const [expected, graph, status, diagnostic] of [
  ["present", "aws-sigv4 v1.6.0\n", 0, ""],
  ["absent", "serde v1.0.228\n", 0, ""],
  ["present", "serde v1.0.228\n", 1, "signing dependency is missing"],
  ["absent", "aws-sigv4 v1.6.0\n", 1, "signing dependency leaked"],
]) {
  test(`signing policy ${expected} checks ${JSON.stringify(graph)}`, () => {
    const result = spawnSync(
      "bash",
      [
        "-c",
        `set -euo pipefail\ncargo() { printf '%s' "$FAKE_GRAPH"; }\n${signingAssertion}\nassert_sigv4 "$1" -p test-provider`,
        "bash",
        expected,
      ],
      { encoding: "utf8", env: { ...process.env, FAKE_GRAPH: graph } },
    );
    assert.equal(result.status, status, result.stderr);
    assert.equal(result.stdout, "");
    assert.ok(result.stderr.includes(diagnostic), result.stderr);
    if (status !== 0) assert.match(result.stderr, /-p test-provider/);
  });
}

function check(graph) {
  return spawnSync(
    "bash",
    ["-c", 'source "$1" && assert_wasm_tokio_features', "bash", helper],
    { encoding: "utf8", input: graph },
  );
}

for (const graph of [
  "edgezero-core v0.1.0|\n",
  "worker v0.8.5|\ntokio v1.52.3|\n",
  "worker v0.9.0|\ntokio v1.53.0|\n",
  "tokio v1.52.3|io-util,macros,sync\n",
  "tokio v1.52.3| (*)\ntokio v1.53.0|sync (*)\n",
]) {
  test(`accepts portable graph ${JSON.stringify(graph)}`, () => {
    const result = check(graph);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout, "");
    assert.equal(result.stderr, "");
  });
}

for (const feature of [
  "fs",
  "full",
  "io-std",
  "io-uring",
  "net",
  "process",
  "rt",
  "rt-multi-thread",
  "signal",
  "time",
]) {
  test(`rejects Tokio ${feature} in WASM`, () => {
    const result = check(`tokio v1.52.3|io-util,${feature},sync\n`);
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /Tokio runtime\/native-I\/O feature/);
    assert.ok(result.stderr.includes(feature), result.stderr);
  });
}

test("checks every Tokio version, including duplicate-tree annotations", () => {
  const result = check("tokio v1.52.3|\ntokio v1.53.0|rt (*)\n");
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /tokio v1\.53\.0: rt/);
});

for (const [script, dependency, diagnostic] of [
  [
    "check_native_store_dependency_contract.sh",
    "tokio v1.52.3|",
    /dependency leaked into core/,
  ],
  [
    "check_native_store_dependency_contract.sh",
    "aws-sdk-dynamodb v1.0.0|",
    /dependency leaked into core/,
  ],
  [
    "check_geo_dependency_contract.sh",
    "tokio v1.52.3|",
    /Tokio dependency leaked into portable core/,
  ],
]) {
  test(`${script} still rejects ${dependency} in core`, (t) => {
    const directory = mkdtempSync(join(tmpdir(), "edgezero-portable-graph-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    writeFileSync(
      join(directory, "cargo"),
      `#!/usr/bin/env bash
root=''
features=''
for argument in "$@"; do
  if [[ "$argument" == geo-cloudfront ]]; then features=',geo-cloudfront'; fi
done
while [[ $# -gt 0 ]]; do
  if [[ "$1" == -p ]]; then root="$2"; shift; fi
  shift
done
if [[ "$root" == edgezero-adapter-axum ]]; then features="axum$features"; fi
printf '%s v0.1.0|%s\\n' "$root" "$features"
if [[ "$root" == edgezero-core ]]; then
  printf '%s\\n' "$FAKE_PORTABLE_DEPENDENCY"
else
  printf 'edgezero-core v0.1.0|\\n'
fi
`,
      { mode: 0o700 },
    );
    const result = spawnSync(
      "bash",
      [fileURLToPath(new URL(`./${script}`, import.meta.url))],
      {
        encoding: "utf8",
        env: {
          ...process.env,
          PATH: `${directory}:${process.env.PATH}`,
          FAKE_PORTABLE_DEPENDENCY: dependency,
        },
      },
    );
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, diagnostic);
  });
}

for (const graph of ["tokio v1.52.3\n", "tokio v1.52.3|rt|extra\n"]) {
  test(`rejects missing or malformed feature data ${JSON.stringify(graph)}`, () => {
    const result = check(graph);
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /missing or malformed Tokio feature data/);
  });
}
