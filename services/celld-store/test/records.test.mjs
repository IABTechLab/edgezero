import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  FakeStorage, TOKEN, authenticatedPolicy, call, createObject, environment,
  mutationId, trustedPolicy,
} from "./support.mjs";

const INSTANCE = "x-edgezero-store-instance";
const EXPECTED = "x-edgezero-expected-instance";
const RECORD = "x-edgezero-record-version";
const MAX_U64 = 18_446_744_073_709_551_615n;

async function handle(env, { alias = "state", token } = {}) {
  const response = await call(env, `/v1/stores/${alias}/capabilities`, { token });
  assert.equal(response.status, 200);
  const capabilities = await response.json();
  assert.equal(capabilities.records, true);
  assert.equal(capabilities.record_protocol, 1);
  const instance = response.headers.get(INSTANCE);
  assert.equal(instance, capabilities.instance);
  return { env, alias, token, instance };
}

function request(h, resource, query = {}, options = {}) {
  const params = new URLSearchParams(query);
  return call(h.env, `/v1/stores/${h.alias}/${resource}?${params}`, {
    token: h.token,
    ...options,
    headers: { [RECORD]: "1", [EXPECTED]: h.instance, ...options.headers },
  });
}

function write(h, key, body = "value", {
  mode = "overwrite", generation = null, metadata = null, ttl = null, id = mutationId(),
} = {}) {
  return request(h, "record-put", { key }, {
    method: "POST", id,
    body: JSON.stringify({
      body: Buffer.from(body).toString("base64"),
      metadata: metadata === null ? null : Buffer.from(metadata).toString("base64"),
      ttl_ms: ttl, mode, generation,
    }),
  });
}

const lookup = (h, key) => request(h, "record", { key });
const exists = (h, key) => request(h, "record", { key }, { method: "HEAD" });
const remove = (h, key, id = mutationId()) => request(h, "record-delete", { key }, { method: "POST", id });
const page = (h, prefix = "", limit = 100, cursor, mode = "partition") =>
  request(h, "record-keys", { prefix, limit, mode, ...(cursor === undefined ? {} : { cursor }) });
const entry = (env) => [...env.EDGEZERO_STORE.objects.values()][0];
const allocator = (env) => entry(env).storage.db.prepare("SELECT allocator FROM edgezero_meta").get().allocator;
const rawPut = (h, key, body) => call(h.env, `/v1/stores/${h.alias}/put?key=${encodeURIComponent(key)}`, {
  method: "POST", body, token: h.token, id: mutationId(),
});

async function record(h, key) {
  const response = await lookup(h, key);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get(RECORD), "1");
  assert.equal(response.headers.get(INSTANCE), h.instance);
  return response.json();
}

function internal(object, meta, body = null) {
  return object.fetch(new Request(`https://edgezero-store.internal/${meta.op}?meta=${encodeURIComponent(JSON.stringify(meta))}`, {
    method: "POST", headers: { "x-edgezero-internal-version": "1" }, body,
  }));
}

test("record capabilities are read-only and require the explicit protocol and instance", async () => {
  const env = environment(trustedPolicy);
  const h = await handle(env);
  assert.equal(allocator(env), "0");
  await handle(env);
  assert.equal(allocator(env), "0");
  assert.equal((await exists(h, "missing")).status, 404);
  assert.equal((await call(env, "/v1/stores/state/record?key=k")).status, 426);
  assert.equal((await request(h, "record", { key: "k" }, { headers: { [EXPECTED]: "" } })).status, 400);
  assert.equal((await request(h, "record", { key: "k" }, { headers: { [RECORD]: "2" } })).status, 426);
  assert.equal(entry(env).storage.db.prepare("SELECT count(*) AS n FROM edgezero_mutations").get().n, 0);
});

test("record Add and CAS have one winner, preserve bytes/metadata and share ordinary rows", async () => {
  const h = await handle(environment(trustedPolicy));
  const bytes = new Uint8Array([0, 255, 1, 128]);
  const statuses = await Promise.all([write(h, "p/k", bytes, { mode: "add", metadata: "é文字\n" }), write(h, "p/k", bytes, { mode: "add", metadata: "é文字\n" })]);
  assert.deepEqual(statuses.map((r) => r.status).sort(), [204, 412]);
  const first = await record(h, "p/k");
  assert.deepEqual(Buffer.from(first.body, "base64"), Buffer.from(bytes));
  assert.equal(Buffer.from(first.metadata, "base64").toString(), "é文字\n");
  assert.equal(first.generation, "1");
  const raw = await call(h.env, "/v1/stores/state/value?key=p%2Fk");
  assert.deepEqual(Buffer.from(await raw.arrayBuffer()), Buffer.from(bytes));
  const cas = await Promise.all([write(h, "p/k", "a", { mode: "if-generation-match", generation: first.generation }), write(h, "p/k", "b", { mode: "if-generation-match", generation: first.generation })]);
  assert.deepEqual(cas.map((r) => r.status).sort(), [204, 412]);
  const beforeOrdinary = await record(h, "p/k");
  assert.equal((await rawPut(h, "p/k", "ordinary")).status, 204);
  const ordinary = await record(h, "p/k");
  assert.equal(Buffer.from(ordinary.body, "base64").toString(), "ordinary");
  assert.equal(ordinary.metadata, null);
  assert.notEqual(ordinary.generation, beforeOrdinary.generation);
  assert.equal((await write(h, "p/k", "stale", { mode: "if-generation-match", generation: beforeOrdinary.generation })).status, 412);
  assert.equal((await write(h, "p/k", "", { metadata: "" })).status, 204);
  assert.equal((await record(h, "p/k")).metadata, null);
});

test("record ledger preserves successful and failed conditionals after intervening changes", async () => {
  const h = await handle(environment(trustedPolicy));
  h.env.EDGEZERO_STORE.deliveries = 2;
  const addId = mutationId();
  assert.equal((await write(h, "k", "first", { mode: "add", id: addId })).status, 204);
  assert.equal(allocator(h.env), "1");
  h.env.EDGEZERO_STORE.deliveries = 1;
  const rejectedId = mutationId();
  assert.equal((await write(h, "k", "must-not-appear", { mode: "add", id: rejectedId })).status, 412);
  assert.equal((await remove(h, "k")).status, 204);
  assert.equal((await write(h, "k", "must-not-appear", { mode: "add", id: rejectedId })).status, 412);
  assert.equal((await exists(h, "k")).status, 404);
  assert.equal((await write(h, "k", "replacement", { mode: "add" })).status, 204);
  const replacement = await record(h, "k");
  assert.equal((await write(h, "k", "first", { mode: "add", id: addId })).status, 204);
  assert.deepEqual(await record(h, "k"), replacement);
  assert.equal((await write(h, "k", "changed", { mode: "add", id: addId })).status, 400);
  const future = String(BigInt(replacement.generation) + 1n);
  const casId = mutationId();
  assert.equal((await write(h, "k", "cas", { mode: "if-generation-match", generation: future, id: casId })).status, 412);
  assert.equal((await rawPut(h, "k", "now-at-future-generation")).status, 204);
  assert.equal((await record(h, "k")).generation, future);
  assert.equal((await write(h, "k", "cas", { mode: "if-generation-match", generation: future, id: casId })).status, 412);
  const writtenCasId = mutationId();
  assert.equal((await write(h, "k", "cas", { mode: "if-generation-match", generation: future, id: writtenCasId })).status, 204);
  const afterCas = allocator(h.env);
  assert.equal((await write(h, "k", "cas", { mode: "if-generation-match", generation: future, id: writtenCasId })).status, 204);
  assert.equal(allocator(h.env), afterCas);
});

test("a replayed record delete does not erase a recreated value or reuse its token", async () => {
  const h = await handle(environment(trustedPolicy));
  await write(h, "k", "first");
  const first = await record(h, "k");
  const id = mutationId();
  assert.equal((await remove(h, "k", id)).status, 204);
  assert.equal((await write(h, "k", "new", { mode: "add" })).status, 204);
  const fresh = await record(h, "k");
  assert.notEqual(fresh.generation, first.generation);
  assert.equal((await remove(h, "k", id)).status, 204);
  assert.deepEqual(await record(h, "k"), fresh);
  assert.equal((await write(h, "k", "stale", { mode: "if-generation-match", generation: first.generation })).status, 412);
});

test("expected instance rejects writes and deletes before effects in a replacement store", async () => {
  const env = environment(trustedPolicy);
  const old = await handle(env);
  await write(old, "k", "old");
  const token = (await record(old, "k")).generation;
  env.EDGEZERO_STORE.objects.clear();
  const fresh = await handle(env);
  assert.notEqual(fresh.instance, old.instance);
  await write(fresh, "k", "new");
  assert.equal((await record(fresh, "k")).generation, token, "replacement store intentionally reuses the numeric generation");
  const before = allocator(env);
  assert.equal((await write(old, "k", "stale", { mode: "if-generation-match", generation: token })).status, 503);
  assert.equal((await remove(old, "k")).status, 503);
  assert.equal((await exists(old, "k")).status, 503);
  assert.equal((await page(old)).status, 503);
  assert.equal(allocator(env), before);
  assert.equal(Buffer.from((await record(fresh, "k")).body, "base64").toString(), "new");
});

test("native expiry filters lookup but physical existence and conditions hold until sweep", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1_000_000 });
  const h = await handle(environment(trustedPolicy));
  await write(h, "held", "before-expiry", { ttl: 10 });
  const before = await record(h, "held");
  t.mock.timers.tick(11);
  assert.equal((await lookup(h, "held")).status, 404);
  assert.equal((await exists(h, "held")).status, 200);
  assert.equal((await write(h, "held", "collision", { mode: "add" })).status, 412);
  assert.deepEqual((await (await page(h)).json()).keys, []);
  assert.equal((await write(h, "held", "no-expiry", { mode: "if-generation-match", generation: before.generation })).status, 204);
  await write(h, "swept", "expires", { ttl: 1 });
  const swept = await record(h, "swept");
  t.mock.timers.tick(2);
  await entry(h.env).object.alarm();
  assert.equal((await exists(h, "swept")).status, 404);
  await write(h, "swept", "fresh", { mode: "add" });
  assert.equal((await write(h, "swept", "stale", { mode: "if-generation-match", generation: swept.generation })).status, 412);
  t.mock.timers.tick(100_000);
  await entry(h.env).object.alarm();
  assert.equal(Buffer.from((await record(h, "held")).body, "base64").toString(), "no-expiry");
  assert.equal(entry(h.env).storage.db.prepare("SELECT expires_at_ms FROM edgezero_rows WHERE key='held'").get().expires_at_ms, null);
});

test("late delivery never applies, including after the original ledger entry is swept", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1_000_000 });
  const h = await handle(environment(trustedPolicy));
  const { object } = entry(h.env);
  const meta = { op: "record-put", key: "k", expectedInstance: h.instance, mode: "overwrite", generation: null, metadata: null, ttlMs: null, mutationId: mutationId(), applyBefore: 1_010_000 };
  assert.equal((await internal(object, meta, "once")).status, 204);
  t.mock.timers.tick(20_000);
  assert.equal((await internal(object, meta, "once")).status, 204);
  assert.equal(allocator(h.env), "1");
  const late = { ...meta, key: "late", mutationId: mutationId() };
  assert.equal((await internal(object, late, "never")).status, 504);
  t.mock.timers.tick(600_001);
  await object.alarm();
  assert.equal((await internal(object, meta, "once")).status, 504);
  assert.equal(allocator(h.env), "1");
});

test("record generations survive reopening, preserve exact u64 and exhaust without partial writes", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "stock-records-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const env = environment(trustedPolicy);
  const database = join(root, "store.sqlite");
  const objectName = "edgezero-store/v1/fixture-state";
  env.EDGEZERO_STORE.objects.set(objectName, createObject(new FakeStorage(database)));
  const h = await handle(env);
  await write(h, "k", "before");
  const old = (await record(h, "k")).generation;
  entry(env).storage.db.close();
  env.EDGEZERO_STORE.objects.set(objectName, createObject(new FakeStorage(database)));
  const reopened = await handle(env);
  assert.equal(reopened.instance, h.instance);
  await write(reopened, "k", "after");
  assert.equal((await write(reopened, "k", "stale", { mode: "if-generation-match", generation: old })).status, 412);
  entry(env).storage.db.prepare("UPDATE edgezero_meta SET allocator = ?").run(String(MAX_U64 - 1n));
  assert.equal((await write(h, "max", "last")).status, 204);
  const last = await record(h, "max");
  assert.equal(last.generation, String(MAX_U64));
  assert.equal((await write(h, "max", "must-roll-back")).status, 503);
  assert.deepEqual(await record(h, "max"), last);
  assert.equal(allocator(env), String(MAX_U64));
  assert.equal((await write(h, "max", "conflict", { mode: "add" })).status, 412);
  entry(env).storage.db.close();
});

test("record payload bounds preserve maximum binary values, UTF-8 metadata and escaped keys", async () => {
  const h = await handle(environment(trustedPolicy));
  const key = '\"é/'.repeat(100) + "x".repeat(112);
  assert.equal(Buffer.byteLength(key), 512);
  const body = new Uint8Array(1_048_576).fill(255);
  const metadata = "é".repeat(512);
  assert.equal((await write(h, key, body, { metadata })).status, 204);
  const stored = await record(h, key);
  assert.deepEqual(Buffer.from(stored.body, "base64"), Buffer.from(body));
  assert.equal(Buffer.from(stored.metadata, "base64").toString(), metadata);
  assert.equal((await write(h, key + "x")).status, 400);
  assert.equal((await write(h, "metadata-too-large", "v", { metadata: metadata + "é" })).status, 413);
  assert.equal((await write(h, "body-too-large", new Uint8Array(1_048_577))).status, 413);
  for (const value of [
    { body: "/w==", metadata: "/w==", mode: "overwrite" },
    { body: "YQ", mode: "overwrite" },
    { body: "YQ==", mode: "if-generation-match", generation: "0" },
    { body: "YQ==", mode: "if-generation-match", generation: 1 },
    { body: "YQ==", mode: "add", generation: "1" },
    { body: "YQ==", mode: "overwrite", extra: true },
    { body: "YQ==", mode: "overwrite", ttl_ms: 0 },
  ]) assert.equal((await request(h, "record-put", { key: "invalid" }, { method: "POST", id: mutationId(), body: JSON.stringify(value) })).status, 400);
});

test("record pagination is complete and binds prefix, mode, alias, instance and cursor kind", async () => {
  const policy = JSON.parse(trustedPolicy);
  policy.stores.push({ alias: "same", namespace: "fixture-state" });
  policy.access.grants.push({ store: "same", operations: ["read", "write", "delete", "list"] });
  const h = await handle(environment(JSON.stringify(policy)));
  const expected = Array.from({ length: 1001 }, (_, n) => `p/${String(n).padStart(4, "0")}`);
  for (const key of [...expected, "p-neighbour", "q/other"]) assert.equal((await write(h, key, "v")).status, 204);
  const first = await (await page(h, "p/", 1000)).json();
  assert.equal(first.keys.length, 1000);
  assert.ok(first.cursor);
  const last = await (await page(h, "p/", 1000, first.cursor)).json();
  assert.equal(last.cursor, null);
  assert.deepEqual([...first.keys, ...last.keys], expected);
  const one = await (await page(h, "p/", 1)).json();
  for (const response of [
    await page(h, "q/", 1, one.cursor),
    await page(h, "p/", 1, one.cursor, "ordinary"),
    await page({ ...h, alias: "same" }, "p/", 1, one.cursor),
    await page(h, "p/", 1, "malformed"),
    await call(h.env, `/v1/stores/state/keys?prefix=p%2F&limit=1&cursor=${encodeURIComponent(one.cursor)}`),
  ]) assert.equal(response.status, 400);
  const ordinary = await (await call(h.env, "/v1/stores/state/keys?prefix=p%2F&limit=1")).json();
  assert.equal((await page(h, "p/", 1, ordinary.cursor)).status, 400);
  const global = await (await page(h, "", 1000, undefined, "ordinary")).json();
  const globalLast = await (await page(h, "", 1000, global.cursor, "ordinary")).json();
  assert.deepEqual([...global.keys, ...globalLast.keys].sort(), [...expected, "p-neighbour", "q/other"].sort());
  const oldCursor = one.cursor;
  h.env.EDGEZERO_STORE.objects.clear();
  const fresh = await handle(h.env);
  assert.equal((await page(fresh, "p/", 1, oldCursor)).status, 400);
});

test("record endpoints retain the existing authorization grants", async () => {
  const h = await handle(environment(await authenticatedPolicy(["read"])), { token: TOKEN });
  assert.equal((await exists(h, "k")).status, 404);
  assert.equal((await write(h, "k")).status, 403);
  assert.equal((await remove(h, "k")).status, 403);
  assert.equal((await page(h)).status, 403);
  assert.equal((await lookup({ ...h, token: undefined }, "k")).status, 401);
});
