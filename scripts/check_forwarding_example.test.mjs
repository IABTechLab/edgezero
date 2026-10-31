import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import * as checker from "./check_forwarding_example.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const guide = readFileSync(join(root, "docs/guide/proxying.md"), "utf8");
const section = (code = "fn example() {}", language = "rust") =>
  `## Forward An Inbound Request\n\n\`\`\`${language}\n${code}\n\`\`\`\n`;

test("extracts the real forwarding fence without changing its code", () => {
  assert.equal(typeof checker.extractForwardingExample, "function");
  const example = checker.extractForwardingExample(guide);
  assert.ok(example.includes("ctx.into_request()?"));
  assert.ok(example.includes("ResponseDecoding::Original"));
  assert.ok(guide.includes(example));
});

test("rejects missing, duplicate, empty and unclosed forwarding fences", () => {
  for (const input of [
    "## Other\n" +
      section().replace("## Forward An Inbound Request", "### Child"),
    section() + section(),
    "## Forward An Inbound Request\nNo example.\n",
    section() + "\n```rust\nfn second() {}\n```\n",
    "## Forward An Inbound Request\n```rust\nfn example() {}\n",
    section(""),
  ]) {
    assert.throws(() => checker.extractForwardingExample(input), /forwarding/i);
  }
});

test("rejects doctest annotations that can weaken the mandatory gate", () => {
  for (const language of [
    "rust,ignore",
    "rust,ignore-x86_64",
    "rust,no_run",
    "rust,compile_fail",
    "rust,should_panic",
    "rust,edition2018",
    "rs",
    "",
  ]) {
    assert.throws(
      () =>
        checker.extractForwardingExample(section("fn example() {}", language)),
      /plain rust/i,
    );
  }
});

test("does not include other sections or mistake fenced headings for sections", () => {
  const input =
    "## Before\n```text\n## Forward An Inbound Request\n```\n" +
    section("fn example() {}\n// ## Other") +
    "## After\n```rust\nfn unrelated() {}\n```\n";
  assert.equal(
    checker.extractForwardingExample(input),
    "fn example() {}\n// ## Other\n",
  );
});

function cargoOutput(change = () => {}) {
  const artifact = {
    reason: "compiler-artifact",
    manifest_path: join(root, "crates/edgezero-core/Cargo.toml"),
    target: { name: "edgezero_core", kind: ["lib"], edition: "2024" },
    filenames: ["/custom target/debug/libedgezero_core.rlib"],
    fresh: true,
  };
  const records = [artifact, { reason: "build-finished", success: true }];
  change(records);
  return records.map((record) => JSON.stringify(record)).join("\n");
}

test("locates fresh core artifacts without assuming the target directory", () => {
  assert.deepEqual(checker.coreArtifact(cargoOutput(), root), {
    rlib: "/custom target/debug/libedgezero_core.rlib",
    dependencies: "/custom target/debug/deps",
    edition: "2024",
  });
  const output = cargoOutput((records) => {
    records[0].filenames = ["/elsewhere/deps/libedgezero_core-123.rlib"];
  });
  assert.equal(
    checker.coreArtifact(output, root).dependencies,
    "/elsewhere/deps",
  );
});

test("rejects failed builds, missing or ambiguous artifacts and malformed JSON", () => {
  for (const output of [
    "not JSON",
    cargoOutput((records) => records.pop()),
    cargoOutput((records) => (records[1].success = false)),
    cargoOutput((records) => (records[0].manifest_path = "/other/Cargo.toml")),
    cargoOutput((records) => (records[0].filenames = [])),
    cargoOutput((records) => (records[0].target.edition = undefined)),
    cargoOutput((records) => records.unshift(structuredClone(records[0]))),
  ]) {
    assert.throws(
      () => checker.coreArtifact(output, root),
      /Cargo|artifact|edition/,
    );
  }
});

test("requires one listed and one passed non-ignored doctest", () => {
  checker.requireOneDoctest(
    "example.md - (line 2): test\n\n1 test, 0 benchmarks\n",
  );
  checker.requireOnePassedDoctest(
    "running 1 test\ntest example.md - (line 2) ... ok\n" +
      "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n",
  );
  for (const listing of [
    "0 tests, 0 benchmarks\n",
    "a: test\nb: test\n\n2 tests, 0 benchmarks\n",
    "a: test\n\n1 test, 0 benchmarks\nignored",
    "a: test\n",
  ]) {
    assert.throws(() => checker.requireOneDoctest(listing), /one.*doctest/i);
  }
  for (const result of [
    "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
    "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0s",
    "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
    "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n" +
      "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
  ]) {
    assert.throws(
      () => checker.requireOnePassedDoctest(result),
      /one.*doctest/i,
    );
  }
});

test("real rustdoc compiles the guide and rejects broken forwarding code", async (t) => {
  assert.equal(typeof checker.buildCore, "function");
  const artifact = checker.buildCore(root);
  const code = checker.extractForwardingExample(guide);
  await t.test(
    "actual example passes without calling the network handler",
    () => {
      checker.runDoctest(code, artifact, root);
    },
  );
  await t.test(
    "missing into_request question mark is a real compiler failure",
    () => {
      const broken = code.replace("ctx.into_request()?", "ctx.into_request()");
      assert.notEqual(broken, code);
      assert.throws(() => checker.runDoctest(broken, artifact, root), /E0308/);
    },
  );
  await t.test("invalid Rust fails compilation", () => {
    assert.throws(
      () => checker.runDoctest(code + "\nfn broken( {}", artifact, root),
      /doctest.*failed/s,
    );
  });
  await t.test("item bodies are not executed", () => {
    checker.runDoctest(
      code + '\nfn not_called() { panic!("do not execute"); }',
      artifact,
      root,
    );
  });
  await t.test("embedded fences cannot hide invalid Rust", () => {
    const payload = "fn main() {}\n```\n```text\nthis is not valid Rust";
    const extracted = checker.extractForwardingExample(
      "## Forward An Inbound Request\n~~~rust\n" + payload + "\n~~~\n",
    );
    assert.equal(extracted, payload + "\n");
    assert.throws(
      () => checker.runDoctest(extracted, artifact, root),
      /doctest.*failed/s,
    );
  });
  await t.test("embedded fences cannot enable a snippet-owned main", () => {
    const payload =
      'fn handler() { panic!("HANDLER_WAS_CALLED"); }\n' +
      "fn main() { handler(); }\n```\n```text\n";
    const extracted = checker.extractForwardingExample(
      "## Forward An Inbound Request\n````rust\n" + payload + "\n````\n",
    );
    assert.throws(
      () => checker.runDoctest(extracted, artifact, root),
      (error) =>
        error.message.includes("Couldn't compile the test") &&
        !error.message.includes("Test executable failed"),
    );
  });
  await t.test(
    "top-level executable statements cannot invoke the handler",
    () => {
      assert.throws(
        () =>
          checker.runDoctest(
            code + '\npanic!("do not execute");',
            artifact,
            root,
          ),
        /doctest.*failed/s,
      );
    },
  );
});
