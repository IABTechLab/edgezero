import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const script = fileURLToPath(new URL("./run_test_nonzero.sh", import.meta.url));

const summary = (passed) =>
  `test result: ok. ${passed} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n`;
const passedOutput = (names) =>
  names.map((name) => `test ${name} ... ok\n`).join("") + summary(names.length);

function run(
  mode,
  listed = ["tests::sentinel"],
  exit = 0,
  required = ["sentinel"],
  {
    output = passedOutput(listed),
    stderr = "",
    listExit = 0,
    env = process.env,
  } = {},
) {
  const fixture = `
    const mode = ${JSON.stringify(mode)};
    const args = process.argv.slice(1);
    const listing = args.includes("--list");
    const expected = [...(mode ? [mode] : []), ...(listing ? ["--list"] : [])];
    if (JSON.stringify(args) !== JSON.stringify(expected)) process.exit(87);
    if (listing) {
      for (const name of ${JSON.stringify(listed)}) console.log(name + ": test");
      process.exitCode = ${listExit};
    } else {
      console.log("executed");
      process.stdout.write(${JSON.stringify(output)});
      process.stderr.write(${JSON.stringify(stderr)});
      process.exitCode = ${exit};
    }
  `;
  return spawnSync(
    "bash",
    [
      script,
      ...(mode ? [mode] : []),
      ...required,
      process.execPath,
      "-e",
      fixture,
    ],
    { encoding: "utf8", env },
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

test("guard rejects empty sentinel names rather than accepting an unnamed summary", () => {
  for (const sentinel of ["", " ", "\nsentinel", "sentinel\n\nother"]) {
    const result = run(null, ["tests::sentinel"], 0, [sentinel], {
      output: summary(1),
    });
    assert.equal(result.status, 2, result.stderr);
    assert.doesNotMatch(result.stdout, /executed/);
  }
});

test("shared dependency helper explicitly invokes the Bash guard", () => {
  const helper = readFileSync(
    new URL("./check_shared_dep_pins.sh", import.meta.url),
    "utf8",
  );
  assert.match(helper, /^\s*bash scripts\/run_test_nonzero\.sh /m);
  assert.doesNotMatch(helper, /^\s*sh scripts\/run_test_nonzero\.sh /m);
});

test("guard preserves listing failure without executing", () => {
  const result = run(null, ["tests::sentinel"], 0, ["sentinel"], {
    listExit: 128,
  });
  assert.equal(result.status, 128, result.stderr);
  assert.doesNotMatch(result.stdout, /executed/);
});

for (const [name, output] of [
  ["empty execution", ""],
  ["zero passed tests", summary(0)],
  ["only ignored tests", "test tests::sentinel ... ignored\n" + summary(0)],
  ["a named pass without a summary", "test tests::sentinel ... ok\n"],
  [
    "a failed summary",
    "test tests::sentinel ... ok\ntest result: FAILED. 1 passed; 1 failed; 0 ignored;\n",
  ],
  [
    "a failed binary after a passing binary",
    passedOutput(["tests::sentinel"]) +
      "test result: FAILED. 0 passed; 1 failed; 0 ignored;\n",
  ],
  [
    "a success summary with failed tests",
    "test tests::sentinel ... ok\ntest result: ok. 1 passed; 1 failed; 0 ignored;\n",
  ],
  ["an unnamed positive summary", summary(1)],
  ["quiet output", "running 1 test\n.\n" + summary(1)],
  ["terse output without names", "running 2 tests\n..\n" + summary(2)],
  ["an unrelated pass", passedOutput(["tests::unrelated"])],
  ["a suffixed name", passedOutput(["tests::sentinel_suffix"])],
  ["a prefixed name", passedOutput(["tests::not_sentinel"])],
  [
    "an ignored sentinel",
    "test tests::sentinel ... ignored\n" + passedOutput(["tests::unrelated"]),
  ],
  [
    "a failed sentinel",
    "test tests::sentinel ... FAILED\n" + passedOutput(["tests::unrelated"]),
  ],
  ["a listed but not passed sentinel", "tests::sentinel: test\n" + summary(1)],
  [
    "a passed sentinel without a name boundary",
    "test not_tests::sentinel_extra ... ok\n" + summary(1),
  ],
]) {
  test(`guard rejects ${name} despite a successful exit`, () => {
    const result = run(null, ["tests::sentinel"], 0, ["sentinel"], { output });
    assert.equal(
      result.status,
      1,
      `${name}: ${result.stdout}\n${result.stderr}`,
    );
    assert.match(result.stdout, /executed/);
  });
}

for (const mode of [null, "--ignored", "--include-ignored"]) {
  test(`guard rejects ${mode ?? "default"} zero execution after a nonempty listing`, () => {
    const result = run(mode, ["tests::sentinel"], 0, ["sentinel"], {
      output: summary(0),
    });
    assert.equal(result.status, 1, result.stderr);
  });
}

test("guard preserves execution failure even without a summary", () => {
  const result = run(null, ["tests::sentinel"], 43, ["sentinel"], {
    output: "",
  });
  assert.equal(result.status, 43, result.stderr);
});

test("guard accepts default WASM named output without measured counts or format flags", () => {
  const output =
    "running 1 test\ntest tests::sentinel ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 filtered out; finished in 0.01s\n";
  const result = run(null, ["tests::sentinel"], 0, ["sentinel"], { output });
  assert.equal(result.status, 0, result.stderr);
});

test("guard accepts zero-test binaries around a passing binary", () => {
  const output = summary(0) + passedOutput(["tests::sentinel"]) + summary(0);
  const result = run(null, ["tests::sentinel"], 0, ["sentinel"], { output });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.match(/executed/g)?.length, 1);
});

test("guard accepts named results and summaries written to stderr", () => {
  const result = run(null, ["tests::sentinel"], 0, ["sentinel"], {
    output: "",
    stderr: passedOutput(["tests::sentinel"]),
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /test tests::sentinel \.\.\. ok/);
});

test("guard requires every newline-separated sentinel to pass", () => {
  const listed = ["tests::sentinel", "tests::second"];
  const required = ["sentinel\nsecond"];
  assert.equal(run(null, listed, 0, required).status, 0);
  for (const name of listed) {
    const result = run(null, listed, 0, required, {
      output: passedOutput(listed.filter((test) => test !== name)),
    });
    assert.equal(result.status, 1, result.stderr);
    assert.ok(
      result.stderr.includes(
        `required sentinel test not passed: ${name.split("::").at(-1)}`,
      ),
    );
  }
});

test("guard matches explicit sentinel suffixes literally, not as regular expressions", () => {
  assert.equal(run(null, ["tests::sentinel[0]"], 0, ["sentinel[0]"]).status, 0);
  const listed = run(null, ["tests::sentinel_extra"], 0, ["sentinel.*"]);
  assert.equal(listed.status, 1, listed.stderr);
  assert.doesNotMatch(listed.stdout, /executed/);
  const executed = run(null, ["tests::sentinel.*"], 0, ["sentinel.*"], {
    output: passedOutput(["tests::sentinel_extra"]),
  });
  assert.equal(executed.status, 1, executed.stderr);
});

test("guard preserves command and tee failures independently", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "edgezero-tee-test-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  writeFileSync(
    join(directory, "tee"),
    "#!/usr/bin/env bash\ncat >/dev/null\nexit 73\n",
    { mode: 0o755 },
  );
  const options = {
    env: { ...process.env, PATH: `${directory}:${process.env.PATH}` },
  };
  assert.equal(
    run(null, ["tests::sentinel"], 0, ["sentinel"], options).status,
    73,
  );
  assert.equal(
    run(null, ["tests::sentinel"], 42, ["sentinel"], options).status,
    42,
  );
});

test(
  "guard streams stdout and stderr before execution finishes",
  { timeout: 10000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "edgezero-stream-test-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const acknowledgement = join(directory, "streamed");
    const fixture = `
    if (process.argv.includes("--list")) {
      console.log("tests::sentinel: test");
    } else {
      console.log("streamed stdout");
      console.error("streamed stderr");
      const poll = setInterval(() => {
        if (require("node:fs").existsSync(${JSON.stringify(acknowledgement)})) {
          clearInterval(poll);
          console.log(${JSON.stringify(passedOutput(["tests::sentinel"]))});
        }
      }, 10);
      setTimeout(() => process.exit(88), 3000).unref();
    }
  `;
    const child = spawn("bash", [
      script,
      "sentinel",
      process.execPath,
      "-e",
      fixture,
    ]);
    t.after(() => child.kill());
    let output = "";
    let streamed = false;
    child.stdout.on("data", (chunk) => {
      output += chunk;
      if (
        output.includes("streamed stdout") &&
        output.includes("streamed stderr")
      ) {
        streamed = true;
        writeFileSync(acknowledgement, "observed");
      }
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    const status = await new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", resolve);
    });
    assert.equal(status, 0, stderr);
    assert.equal(streamed, true);
  },
);

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
  ["axum/native-tcp,unit/axum,axum/http1", "--include-ignored"],
  ["unit/core", null],
  ["unit/cloudflare", null],
  ["unit/fastly", null],
]) {
  const required = inventory.cases
    .filter(
      (c) =>
        environment.split(",").includes(c.environment) &&
        c.implementation === "ready",
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

  test(`${environment} inventory guard requires exact passed sentinel names`, () => {
    const name = required.at(-1);
    for (const replacement of [null, `other::${name}`, `${name}_suffix`]) {
      const passed = required.filter((test) => test !== name);
      if (replacement) passed.push(replacement);
      const result = run(mode, required, 0, ["--inventory", environment], {
        output: passedOutput(passed),
      });
      assert.equal(result.status, 1, `${name}: ${result.stderr}`);
      assert.match(result.stderr, /required sentinel test not passed/);
    }
  });

  test(`${environment} inventory guard rejects nonexact listed sentinel names`, () => {
    const listed = required.map((name) => `other::${name}`);
    const result = run(mode, listed, 0, ["--inventory", environment]);
    assert.equal(result.status, 1, result.stderr);
    assert.doesNotMatch(result.stdout, /executed/);
  });
}

test("inventory guard rejects unknown environments", () => {
  const unknown = run(null, [], 0, ["--inventory", "unknown"]);
  assert.equal(unknown.status, 1, unknown.stderr);
  assert.match(unknown.stderr, /no ready test sentinels/);
  assert.doesNotMatch(unknown.stdout, /executed/);
});
