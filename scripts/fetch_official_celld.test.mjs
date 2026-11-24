import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  CELLD_ARCHIVE_SHA256,
  CELLD_BINARY_SHA256,
  CELLD_RELEASE_URL,
  provisionCelld,
  verifyDigest,
} from "./fetch_official_celld.mjs";

test("accepts matching bytes and rejects changed archive or binary bytes", () => {
  const bytes = Buffer.from("offline verification fixture");
  const digest = createHash("sha256").update(bytes).digest("hex");
  assert.doesNotThrow(() => verifyDigest(bytes, digest, "fixture"));
  for (const [label, expected] of [
    ["celld archive", CELLD_ARCHIVE_SHA256],
    ["celld binary", CELLD_BINARY_SHA256],
  ]) {
    assert.throws(
      () => verifyDigest(bytes, expected, label),
      (error) => error.message.startsWith(`${label} SHA256 mismatch:`),
    );
  }
});

test("rejects a bad download before decompression or binary use", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "edgezero-celld-download-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let downloads = 0;
  await assert.rejects(
    provisionCelld({
      cacheDirectory: directory,
      fetchArchive: async (url) => {
        downloads += 1;
        assert.equal(url, CELLD_RELEASE_URL);
        return Buffer.from("not gzip, must fail the digest check first");
      },
    }),
    /celld archive SHA256 mismatch:/,
  );
  assert.equal(downloads, 1);
  await assert.rejects(readFile(join(directory, "celld")), { code: "ENOENT" });
  await assert.rejects(readFile(join(directory, "celld-x86_64-unknown-linux-gnu.gz")), { code: "ENOENT" });
});

test("rechecks a cached archive and never trusts an existing executable", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "edgezero-celld-cache-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, "celld-x86_64-unknown-linux-gnu.gz"), "changed archive");
  const binary = join(directory, "celld");
  const marker = join(directory, "executed");
  await writeFile(binary, `#!/bin/sh\ntouch '${marker}'\necho 'celld 0.6.3'\n`, { mode: 0o755 });
  await assert.rejects(
    provisionCelld({
      cacheDirectory: directory,
      fetchArchive: () => assert.fail("cached archive must be verified without downloading"),
    }),
    /celld archive SHA256 mismatch:/,
  );
  await assert.rejects(readFile(marker), { code: "ENOENT" });
});
