#!/usr/bin/env node
import { spawn } from "node:child_process";
import { constants, createReadStream } from "node:fs";
import { access } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isAbsolute, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const sentinel = "generated_native_consumer_uses_embedded_celld_over_tls";
const inherited = [
  "PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "RUSTC", "RUSTDOC",
  "RUSTFLAGS", "CARGO_BUILD_JOBS", "CARGO_TARGET_DIR", "EDGEZERO_GENERATED_TEST_TARGET_DIR",
];

export function fixtureEnvironment(source, binary) {
  const env = {};
  for (const name of inherited) {
    if (source[name] !== undefined) env[name] = source[name];
  }
  env.CELLD_BIN = binary;
  return env;
}

async function binaryDigest(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

// One process group per owned command. A failed or interrupted proof must not
// leave a generated app or celld descendant running.
export function runCommand(command, args, { cwd, env, timeoutMs = 600_000 }) {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(command, args, { cwd, env, detached: process.platform !== "win32", stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    let failure;
    let force;
    const kill = (signal) => {
      try {
        if (process.platform === "win32") child.kill(signal);
        else process.kill(-child.pid, signal);
      } catch (error) {
        if (error.code !== "ESRCH") failure ??= error;
      }
    };
    const stop = (reason) => {
      failure ??= new Error(reason);
      kill("SIGTERM");
      force ??= setTimeout(() => kill("SIGKILL"), 2_000);
      force.unref();
    };
    const onInterrupt = () => stop("native celld proof interrupted");
    process.once("SIGINT", onInterrupt);
    process.once("SIGTERM", onInterrupt);
    const timer = setTimeout(() => stop("native celld proof deadline exceeded"), timeoutMs);
    child.stdout.on("data", (chunk) => {
      process.stdout.write(chunk);
      stdout = (stdout + chunk.toString()).slice(-65_536);
    });
    child.stderr.on("data", (chunk) => {
      process.stderr.write(chunk);
      stderr = (stderr + chunk.toString()).slice(-65_536);
    });
    const clean = () => {
      clearTimeout(timer);
      if (force) clearTimeout(force);
      process.removeListener("SIGINT", onInterrupt);
      process.removeListener("SIGTERM", onInterrupt);
    };
    child.once("error", (error) => { clean(); reject(error); });
    child.once("close", (code, signal) => {
      clean();
      // Reap any descendant which outlived a failed parent before returning.
      if (failure || code !== 0) kill("SIGKILL");
      if (failure) reject(failure);
      else if (code !== 0) reject(new Error(`${command} failed: code=${code} signal=${signal}\n${stderr}`));
      else resolveCommand({ stdout, stderr });
    });
  });
}

export async function runIntegration({
  env = process.env,
  cwd = root,
  command = runCommand,
  executable = (path) => access(path, constants.X_OK),
  digest = binaryDigest,
  log = console.log,
} = {}) {
  const selected = env.CELLD_BIN;
  if (!selected || !isAbsolute(selected)) {
    throw new Error("CELLD_BIN must explicitly select an absolute modified celld executable; no passing skip");
  }
  const binary = resolve(selected);
  await executable(binary);
  const childEnv = fixtureEnvironment(env, binary);
  const help = await command(binary, ["--help"], { cwd, env: childEnv, timeoutMs: 10_000 });
  if (!help.stdout.includes("--store-api-config")) throw new Error("selected celld has no embedded scoped API");
  const fingerprint = await digest(binary);
  if (!/^[a-f0-9]{64}$/.test(fingerprint)) throw new Error("invalid celld executable fingerprint");
  log(`celld executable SHA-256 ${fingerprint}`);
  await command("bash", ["scripts/check_celld_scoped_isolation.sh", binary], { cwd, env: childEnv, timeoutMs: 60_000 });
  await command("scripts/run_test_nonzero.sh", [
    "--ignored", sentinel,
    "cargo", "test", "--offline", "--locked", "-p", "edgezero-cli", "--no-default-features",
    "--features", "cli,edgezero-adapter-axum", "--test", "celld_generated_proof", sentinel,
  ], { cwd, env: childEnv, timeoutMs: 600_000 });
  if (await digest(binary) !== fingerprint) throw new Error("celld executable changed during proof");
  log("Native generated celld TLS and disposable isolation proofs passed. Fleet acceptance remains separate.");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  runIntegration().catch((error) => { console.error(error.message); process.exitCode = 1; });
}
