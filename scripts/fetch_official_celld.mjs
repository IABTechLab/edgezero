#!/usr/bin/env node
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { gunzip } from "node:zlib";

// Official denoland/celld v0.6.3 release asset and GitHub release SHA256 digest.
// Confirmed with `gh release view v0.6.3 -R denoland/celld`.
// Binary identity: docs/superpowers/specs/2026-11-16-stock-celld-store-service-design.md:158,
// which also records the archive hash prefix/suffix and attestation verification.
export const CELLD_RELEASE_URL = "https://github.com/denoland/celld/releases/download/v0.6.3/celld-x86_64-unknown-linux-gnu.gz";
export const CELLD_ARCHIVE_SHA256 = "5084a083472cf036551a7eac63fa24d22bb9a56323c517f63205724c12783047";
export const CELLD_BINARY_SHA256 = "eb0a8158421b69d68dd5d28f2298a7d411dde498295833deeb1cc5ce53b7eb75";
export const CELLD_VERSION = "celld 0.6.3";

export function verifyDigest(bytes, expected, label) {
  const actual = createHash("sha256").update(bytes).digest("hex");
  if (actual !== expected) {
    throw new Error(`${label} SHA256 mismatch: expected ${expected}, got ${actual}`);
  }
}

async function download(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`celld download failed: HTTP ${response.status}`);
  return Buffer.from(await response.arrayBuffer());
}

// stdout is reserved for the absolute CELLD_BIN path. Every cache restore is
// reverified, and the executable is recreated from the verified archive.
export async function provisionCelld({
  cacheDirectory = join(process.env.XDG_CACHE_HOME || join(homedir(), ".cache"), "edgezero-stock-celld", "v0.6.3"),
  fetchArchive = download,
} = {}) {
  const directory = resolve(cacheDirectory);
  const archivePath = join(directory, "celld-x86_64-unknown-linux-gnu.gz");
  const binaryPath = join(directory, "celld");
  let archive;
  try {
    archive = await readFile(archivePath);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    archive = await fetchArchive(CELLD_RELEASE_URL);
  }
  verifyDigest(archive, CELLD_ARCHIVE_SHA256, "celld archive");
  const binary = await promisify(gunzip)(archive);
  verifyDigest(binary, CELLD_BINARY_SHA256, "celld binary");
  await mkdir(directory, { recursive: true });
  await writeFile(archivePath, archive);
  await writeFile(binaryPath, binary);
  await chmod(binaryPath, 0o755);
  const { stdout } = await promisify(execFile)(binaryPath, ["--version"], { timeout: 10_000 });
  if (stdout.trim() !== CELLD_VERSION) {
    throw new Error(`celld version mismatch: expected ${CELLD_VERSION}, got ${stdout.trim()}`);
  }
  return binaryPath;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  provisionCelld({ cacheDirectory: process.argv[2] }).then(
    (binary) => console.log(binary),
    (error) => { console.error(error.message); process.exitCode = 1; },
  );
}
