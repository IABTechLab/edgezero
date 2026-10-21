import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

const script = fileURLToPath(new URL("./run_test_nonzero.sh", import.meta.url));

function run(mode, listed = ["tests::sentinel"], exit = 0) {
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
      "sentinel",
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
