import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import test from "node:test";

const script = fileURLToPath(new URL("./run_test_nonzero.sh", import.meta.url));

function run(
  mode,
  listed = ["tests::sentinel"],
  exit = 0,
  required = ["sentinel"],
) {
  const fixture = `
    const mode = ${JSON.stringify(mode)};
    const args = process.argv.slice(1);
    const selection = args.filter(arg => arg === "--ignored" || arg === "--include-ignored");
    if (JSON.stringify(selection) !== JSON.stringify(mode ? [mode] : [])) process.exit(87);
    if (args.includes("--list")) {
      for (const name of ${JSON.stringify(listed)}) console.log(name + ": test");
    } else {
      console.log("executed");
      process.exit(${exit});
    }
  `;
  return spawnSync(
    "sh",
    [
      script,
      ...(mode ? [mode] : []),
      ...required,
      process.execPath,
      "-e",
      fixture,
    ],
    { encoding: "utf8" },
  );
}

for (const mode of [null, "--ignored", "--include-ignored"]) {
  test(`guard forwards ${mode ?? "default"} selection to listing and execution`, () => {
    const result = run(mode);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /executed/);
  });

  test(`guard preserves ${mode ?? "default"} execution failure`, () => {
    assert.equal(run(mode, ["tests::sentinel"], 42).status, 42);
  });
}

test("guard rejects zero or missing sentinel tests before execution", () => {
  for (const listed of [[], ["tests::sentinel_suffix"]]) {
    const result = run(null, listed);
    assert.equal(result.status, 1, result.stderr);
    assert.doesNotMatch(result.stdout, /executed/);
  }
});

const inventory = JSON.parse(
  readFileSync(
    new URL(
      "../tests/fixtures/reusable-app/outbound-cutover-cases.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
for (const [environment, mode] of [
  ["axum/native-tcp", "--include-ignored"],
  ["unit/core", null],
]) {
  const required = inventory.cases
    .filter(
      (c) => c.environment === environment && c.implementation === "ready",
    )
    .flatMap((c) => [c.sentinel, ...(c.additional_sentinels ?? [])]);

  test(`${environment} inventory guard executes the whole suite once`, () => {
    const result = run(mode, [...required, "unrelated::test"], 0, [
      "--inventory",
      environment,
    ]);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout.match(/executed/g)?.length, 1);
  });

  test(`${environment} inventory guard rejects every missing required test`, () => {
    for (const name of required) {
      const result = run(
        mode,
        required.filter((test) => test !== name),
        0,
        ["--inventory", environment],
      );
      assert.equal(result.status, 1, `${name}: ${result.stderr}`);
      assert.match(result.stderr, /required sentinel test not listed/);
      assert.doesNotMatch(result.stdout, /executed/);
    }
  });

  test(`${environment} inventory guard preserves suite failure`, () => {
    assert.equal(
      run(mode, required, 42, ["--inventory", environment]).status,
      42,
    );
  });
}

test("inventory guard rejects unknown environments", () => {
  const unknown = run(null, [], 0, ["--inventory", "unknown"]);
  assert.equal(unknown.status, 1, unknown.stderr);
  assert.match(unknown.stderr, /no ready test sentinels/);
  assert.doesNotMatch(unknown.stdout, /executed/);
});
