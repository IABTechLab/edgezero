#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repository = fileURLToPath(new URL("../", import.meta.url));

export function extractForwardingExample(markdown) {
  const lines = markdown.split("\n");
  const examples = [];
  let sections = 0;
  let selected = false;
  let fence;
  for (const [index, rawLine] of lines.entries()) {
    const line = rawLine.replace(/\r$/u, "");
    if (fence) {
      if (
        new RegExp(`^ {0,3}${fence.marker}{${fence.length},}\\s*$`, "u").test(
          line,
        )
      ) {
        if (fence.selected) {
          if (fence.language !== "rust") {
            throw new Error("forwarding example must use a plain rust fence");
          }
          examples.push(lines.slice(fence.start + 1, index).join("\n") + "\n");
        }
        fence = undefined;
      }
      continue;
    }
    const opening = /^ {0,3}(`{3,}|~{3,})(.*)$/u.exec(line);
    if (opening) {
      fence = {
        marker: opening[1][0],
        length: opening[1].length,
        language: opening[2].trim(),
        start: index,
        selected,
      };
      continue;
    }
    const heading = /^(#{1,6})\s+(.+?)\s*#*\s*$/u.exec(line);
    if (heading && heading[1].length <= 2) {
      selected =
        heading[1] === "##" && heading[2] === "Forward An Inbound Request";
      if (selected) sections += 1;
    }
  }
  if (
    fence?.selected ||
    sections !== 1 ||
    examples.length !== 1 ||
    !examples[0].trim()
  ) {
    throw new Error(
      "forwarding section must contain exactly one closed, nonempty Rust fence",
    );
  }
  return examples[0];
}

export function coreArtifact(output, root = repository) {
  let records;
  try {
    records = output
      .split(/\r?\n/u)
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line));
  } catch {
    throw new Error("Cargo did not emit valid JSON artifact records");
  }
  const finished = records.filter(
    (record) => record.reason === "build-finished",
  );
  if (finished.length !== 1 || finished[0].success !== true) {
    throw new Error("Cargo build did not finish successfully");
  }
  const artifacts = records.filter(
    (record) =>
      record.reason === "compiler-artifact" &&
      record.manifest_path === join(root, "crates/edgezero-core/Cargo.toml") &&
      record.target?.name === "edgezero_core" &&
      record.target.kind?.includes("lib"),
  );
  if (artifacts.length !== 1) {
    throw new Error("Cargo must emit exactly one core library artifact");
  }
  const artifact = artifacts[0];
  const libraries = artifact.filenames.filter((name) => name.endsWith(".rlib"));
  if (libraries.length !== 1 || !isAbsolute(libraries[0])) {
    throw new Error(
      "Cargo core artifact must identify exactly one absolute rlib path",
    );
  }
  if (!["2015", "2018", "2021", "2024"].includes(artifact.target.edition)) {
    throw new Error("Cargo core artifact must identify its Rust edition");
  }
  const directory = dirname(libraries[0]);
  return {
    rlib: libraries[0],
    dependencies:
      basename(directory) === "deps" ? directory : join(directory, "deps"),
    edition: artifact.target.edition,
  };
}

function command(program, args, root, label) {
  try {
    return execFileSync(program, args, {
      cwd: root,
      encoding: "utf8",
      maxBuffer: 32 * 1024 * 1024,
      timeout: 10 * 60 * 1000,
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    throw new Error(
      `${label} failed\n${error.stdout ?? ""}\n${error.stderr ?? error.message}`,
    );
  }
}

export function buildCore(root = repository) {
  return coreArtifact(
    command(
      process.env.CARGO || "cargo",
      [
        "build",
        "--offline",
        "--locked",
        "-p",
        "edgezero-core",
        "--lib",
        "--message-format=json",
      ],
      root,
      "Cargo core build",
    ),
    root,
  );
}

export function requireOneDoctest(listing) {
  if (
    listing.split("\n").filter((line) => line.endsWith(": test")).length !==
      1 ||
    !/^1 test, 0 benchmarks\s*$/mu.test(listing) ||
    /ignored?/iu.test(listing)
  ) {
    throw new Error(
      "expected exactly one non-ignored listed forwarding doctest",
    );
  }
}

export function requireOnePassedDoctest(output) {
  const summaries = output
    .split("\n")
    .filter((line) => line.startsWith("test result:"));
  if (
    summaries.length !== 1 ||
    !/^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;/u.test(
      summaries[0],
    )
  ) {
    throw new Error(
      "expected exactly one passed, non-ignored forwarding doctest",
    );
  }
}

export function runDoctest(code, artifact, root = repository) {
  const temporary = mkdtempSync(join(tmpdir(), "edgezero-forwarding-"));
  try {
    const markdown = join(temporary, "forwarding.md");
    const delimiter = "`".repeat(
      (code.match(/`+/gu) ?? []).reduce(
        (length, run) => Math.max(length, run.length + 1),
        3,
      ),
    );
    // Compile item definitions, but never dispatch or poll the network handler.
    writeFileSync(
      markdown,
      delimiter + "rust\n" + code + "\n# fn main() {}\n" + delimiter + "\n",
    );
    const args = [
      "--test",
      markdown,
      "--edition",
      artifact.edition,
      "--extern",
      `edgezero_core=${artifact.rlib}`,
      "-L",
      `dependency=${artifact.dependencies}`,
      "--test-args=--color=never",
    ];
    const rustdoc = process.env.RUSTDOC || "rustdoc";
    requireOneDoctest(
      command(
        rustdoc,
        [...args, "--test-args=--list"],
        root,
        "rustdoc listing",
      ),
    );
    requireOnePassedDoctest(command(rustdoc, args, root, "forwarding doctest"));
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  try {
    const code = extractForwardingExample(
      readFileSync(join(repository, "docs/guide/proxying.md"), "utf8"),
    );
    runDoctest(code, buildCore());
    process.stdout.write(
      "forwarding example: one doctest passed; network handler not invoked\n",
    );
  } catch (error) {
    process.stderr.write(`forwarding example: ${error.message}\n`);
    process.exitCode = 1;
  }
}
