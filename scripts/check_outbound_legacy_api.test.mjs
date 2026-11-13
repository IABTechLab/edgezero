import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const script = fileURLToPath(
  new URL("./check_outbound_legacy_api.sh", import.meta.url),
);
const scan = readFileSync(script, "utf8").split(
  "\nscan_detached_send_tuple() {",
)[0];

for (const [grepStatus, expected] of [
  [0, 1],
  [1, 0],
  [2, 2],
  [128, 128],
]) {
  test(`legacy scan maps git grep ${grepStatus} to ${expected}`, () => {
    const result = spawnSync(
      "bash",
      [
        "-c",
        `${scan}
      git() {
        if [ "$1" != grep ]; then return 99; fi
        if [ "$2" != -nE ]; then return 98; fi
        if [ "$3" != fixture ]; then return 97; fi
        if [ ${grepStatus} -eq 0 ]; then printf 'fixture.rs:1:match\n'; fi
        return ${grepStatus}
      }
      scan fixture crates
    `,
      ],
      { encoding: "utf8" },
    );
    assert.equal(result.status, expected, result.stderr);
    assert.equal(result.stdout, grepStatus === 0 ? "fixture.rs:1:match\n" : "");
  });
}

test("legacy gate fails closed when git grep fails but file enumeration succeeds", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "edgezero-legacy-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  writeFileSync(
    join(directory, "git"),
    `#!/usr/bin/env bash
    case "$1" in
      grep) echo 'fixture git grep failure' >&2; exit 128 ;;
      ls-files) exit 0 ;;
      *) exit 99 ;;
    esac
  `,
    { mode: 0o755 },
  );
  const result = spawnSync("bash", [script], {
    encoding: "utf8",
    env: { ...process.env, PATH: `${directory}:${process.env.PATH}` },
  });
  assert.equal(result.status, 1, result.stderr);
  assert.match(result.stderr, /fixture git grep failure/);
});
