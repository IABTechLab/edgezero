import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { mock, test } from "node:test";
import {
  FakeStorage,
  TOKEN,
  authenticatedPolicy,
  call,
  createObject,
  environment,
  internals,
  mutationId,
  sha256,
  trustedPolicy,
  worker,
} from "./support.mjs";

const VERSION = "x-edgezero-store-version";
const INSTANCE = "x-edgezero-store-instance";

async function put(env, key, body, { ttl, id = mutationId(), token } = {}) {
  const query = `key=${encodeURIComponent(key)}${ttl === undefined ? "" : `&ttl_seconds=${ttl}`}`;
  return call(env, `/v1/stores/state/put?${query}`, { method: "POST", body, id, token });
}

async function get(env, key, token) {
  return call(env, `/v1/stores/state/value?key=${encodeURIComponent(key)}`, { token });
}

function rows(env) {
  const [{ storage }] = env.EDGEZERO_STORE.objects.values();
  return storage.db.prepare("SELECT key, generation, expires_at_ms FROM edgezero_rows ORDER BY key").all();
}

function allocator(env) {
  const [{ storage }] = env.EDGEZERO_STORE.objects.values();
  return storage.db.prepare("SELECT allocator FROM edgezero_meta").get().allocator;
}

test("policy parsing is strict and closed", () => {
  const valid = JSON.parse(trustedPolicy);
  assert.equal(internals.parsePolicy(trustedPolicy).mode, "trusted-network");
  const invalid = [
    { ...valid, extra: 1 },
    { ...valid, version: 2 },
    { ...valid, stores: [] },
    { ...valid, stores: [...valid.stores, valid.stores[0]] },
    { ...valid, stores: [{ alias: "bad/alias", namespace: "n" }] },
    { ...valid, stores: [{ alias: "state", namespace: "" }] },
    { ...valid, access: { mode: "trusted-network", grants: [{ store: "missing", operations: ["read"] }] } },
    { ...valid, access: { mode: "trusted-network", grants: [{ store: "state", operations: [] }] } },
    { ...valid, access: { mode: "trusted-network", grants: [{ store: "state", operations: ["read", "read"] }] } },
    { ...valid, access: { mode: "trusted-network", grants: [{ store: "state", operations: ["admin"] }] } },
    { ...valid, access: { mode: "authenticated", tokens: [{ id: "a", sha256: "ABC", grants: [] }] } },
    { ...valid, access: { mode: "authenticated", tokens: [] } },
    { ...valid, access: { mode: "anonymous" } },
  ];
  for (const policy of invalid) assert.throws(() => internals.parsePolicy(JSON.stringify(policy)));
  assert.throws(() => internals.parsePolicy("not json"));
});

test("query components decode once and reject malformed, duplicate or unknown input", () => {
  assert.equal(internals.decodeComponent("a+b%2B%C3%A9"), "a b+é");
  for (const raw of ["%", "%G0", "%C3", "%FF", "é"]) assert.throws(() => internals.decodeComponent(raw));
  assert.throws(() => internals.parseQuery("key=a&key=b", ["key"]));
  assert.throws(() => internals.parseQuery("other=a", ["key"]));
  assert.throws(() => internals.parseQuery("key", ["key"]));
  assert.deepEqual([...internals.parseQuery("key=a", ["key"])], [["key", "a"]]);
});

test("prefix upper bound follows code point order", () => {
  assert.equal(internals.prefixUpperBound(""), null);
  assert.equal(internals.prefixUpperBound("ab"), "ac");
  assert.equal(internals.prefixUpperBound("a\u{d7ff}"), "a\u{e000}");
  assert.equal(internals.prefixUpperBound("a\u{10ffff}"), "b");
  assert.equal(internals.prefixUpperBound("\u{10ffff}"), null);
});

test("every response names the protocol, and errors are closed categories", async () => {
  const env = environment(await authenticatedPolicy());
  const cases = [
    [call(env, "/v1/stores/state/value?key=k"), 401, "unauthenticated"],
    [call(env, "/v1/stores/state/value?key=k", { token: "x".repeat(40) }), 401, "unauthenticated"],
    [call(env, "/v1/stores/state/value?key=k", { token: "short" }), 401, "unauthenticated"],
    [call(env, "/v1/stores/other/value?key=k", { token: TOKEN }), 403, "forbidden"],
    [call(env, "/v1/stores/missing/value?key=k", { token: TOKEN }), 403, "forbidden"],
    [call(env, "/v2/stores/state/value?key=k", { token: TOKEN }), 426, "unsupported_version"],
    [call(env, "/v1/stores/state", { token: TOKEN }), 404, "not_found"],
    [call(env, "/v1/stores/state/value?key=", { token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/value?key=a%0Ab", { token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/value", { method: "PUT", token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/put?key=k", { method: "POST", token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/put?key=k&ttl_seconds=59", { method: "POST", token: TOKEN, id: mutationId() }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/keys?limit=0", { token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/keys?limit=1001", { token: TOKEN }), 400, "invalid_input"],
    [call(env, "/v1/stores/state/put?key=k", { method: "POST", token: TOKEN, id: mutationId(), body: new Uint8Array(1_048_577) }), 413, "limit_exceeded"],
    [call(env, "/v1/stores/state/delete?key=k", { method: "POST", token: TOKEN, id: mutationId(), body: "x" }), 400, "invalid_input"],
  ];
  for (const [pending, status, category] of cases) {
    const response = await pending;
    assert.equal(response.status, status, category);
    assert.equal(response.headers.get(VERSION), "1");
    assert.deepEqual(await response.json(), { error: category });
  }
  const readOnly = environment(await authenticatedPolicy(["read"]));
  assert.equal((await put(readOnly, "k", "v", { token: TOKEN })).status, 403);
});

test("a token may contain commas, but two Authorization headers never authenticate", async () => {
  const token = "comma,token-that-is-longer-than-32-bytes";
  const policy = JSON.parse(await authenticatedPolicy());
  policy.access.tokens[0].sha256 = await sha256(token);
  const env = environment(JSON.stringify(policy));
  assert.equal((await call(env, "/v1/stores/state/capabilities", { token })).status, 200);
  const headers = new Headers();
  headers.append("authorization", `Bearer ${token}`);
  headers.append("authorization", `Bearer ${token}`);
  const doubled = await worker.fetch(new Request("https://store.test/v1/stores/state/capabilities", { headers }), env);
  assert.equal(doubled.status, 401);
  assert.equal((await call(env, "/v1/stores/state/capabilities", { headers: { authorization: `bearer ${token}` } })).status, 401);
});

test("trusted-network mode never ignores a supplied credential", async () => {
  const env = environment(trustedPolicy);
  assert.equal((await get(env, "k", TOKEN)).status, 401);
  assert.equal((await get(env, "k")).status, 404);
});

test("an invalid deployment policy fails every request", async () => {
  const env = environment("{\"version\":1}");
  const response = await get(env, "k");
  assert.equal(response.status, 503);
  assert.deepEqual(await response.json(), { error: "unavailable" });
});

test("values round trip as exact bytes, distinct from missing keys", async () => {
  const env = environment(trustedPolicy);
  const missing = await get(env, "k");
  assert.equal(missing.status, 404);
  assert.match(missing.headers.get(INSTANCE), /^[A-Za-z0-9_-]{22}$/);
  const instance = missing.headers.get(INSTANCE);
  for (const body of [new Uint8Array([0, 255, 1]), new Uint8Array(0)]) {
    const written = await put(env, "a b/é", body);
    assert.equal(written.status, 204);
    assert.equal(written.headers.get(INSTANCE), instance);
    const read = await get(env, "a b/é");
    assert.equal(read.status, 200);
    assert.equal(read.headers.get("content-type"), "application/octet-stream");
    assert.deepEqual(new Uint8Array(await read.arrayBuffer()), body);
  }
  const head = await call(env, "/v1/stores/state/value?key=a%20b%2F%C3%A9", { method: "HEAD" });
  assert.equal(head.status, 200);
  assert.equal((await call(env, "/v1/stores/state/delete?key=a%20b%2F%C3%A9", { method: "POST", id: mutationId() })).status, 204);
  assert.equal((await call(env, "/v1/stores/state/delete?key=never", { method: "POST", id: mutationId() })).status, 204);
  assert.equal((await call(env, "/v1/stores/state/value?key=a%20b%2F%C3%A9", { method: "HEAD" })).status, 404);
  const capabilities = await call(env, "/v1/stores/state/capabilities");
  assert.deepEqual(await capabilities.json(), {
    service: "edgezero-celld-store",
    protocol: 1,
    format: "rows-v1",
    pagination: "keyset",
    no_expiry: true,
    mutation_ledger: true,
    records: false,
    max_value_bytes: 1_048_576,
    max_metadata_bytes: 1_024,
    instance,
  });
});

test("a duplicate delivery applies once and returns the first outcome", async () => {
  const env = environment(trustedPolicy);
  env.EDGEZERO_STORE.deliveries = 2;
  const id = mutationId();
  const first = await put(env, "dup", "one", { id });
  assert.equal(first.status, 204);
  assert.equal(allocator(env), "1", "the resent copy allocated nothing");
  env.EDGEZERO_STORE.deliveries = 1;
  assert.equal((await put(env, "dup", "one", { id })).status, 204, "a later duplicate still sees its outcome");
  assert.equal(allocator(env), "1");
  const reused = await put(env, "dup", "two", { id });
  assert.equal(reused.status, 400, "a reused ID with different content is not a duplicate");
  assert.equal(await (await get(env, "dup")).text(), "one");
  env.EDGEZERO_STORE.deliveries = 2;
  const deleted = await call(env, "/v1/stores/state/delete?key=dup", { method: "POST", id: mutationId() });
  assert.equal(deleted.status, 204);
  await put(env, "dup", "recreated");
  env.EDGEZERO_STORE.deliveries = 1;
  assert.equal(await (await get(env, "dup")).text(), "recreated");
});

test("a mutation held past its apply window does not apply, but its duplicate still answers", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 1_000_000 });
  const { object, ready } = createObject();
  await ready();
  const internal = (meta, body = null) =>
    object.fetch(new Request(`https://edgezero-store.internal/${meta.op}?meta=${encodeURIComponent(JSON.stringify(meta))}`, {
      method: "POST",
      headers: { "x-edgezero-internal-version": "1" },
      body,
    }));
  const applied = { op: "put", key: "k", mutationId: mutationId(), applyBefore: 1_010_000 };
  assert.equal((await internal(applied, "v")).status, 204);
  t.mock.timers.tick(20_000);
  assert.equal((await internal(applied, "v")).status, 204, "ledger answers before the deadline check");
  const late = { op: "put", key: "late", mutationId: mutationId(), applyBefore: 1_010_000 };
  const rejected = await internal(late, "v");
  assert.equal(rejected.status, 504);
  assert.deepEqual(await rejected.json(), { error: "deadline" });
  const lateDelete = { op: "delete", key: "k", mutationId: mutationId(), applyBefore: 1_010_000 };
  assert.equal((await internal(lateDelete)).status, 504);
  const value = await object.fetch(new Request(`https://edgezero-store.internal/value?meta=${encodeURIComponent(JSON.stringify({ op: "value", key: "k", head: false }))}`, {
    method: "POST",
    headers: { "x-edgezero-internal-version": "1" },
  }));
  assert.equal(await value.text(), "v");
});

test("TTL hides expired rows, renews on overwrite, and plain put removes expiry", async () => {
  mock.timers.enable({ apis: ["Date"], now: 5_000_000 });
  try {
    const env = environment(trustedPolicy);
    await put(env, "ttl", "a", { ttl: 60 });
    assert.equal(rows(env)[0].expires_at_ms, 5_060_000);
    mock.timers.tick(30_000);
    await put(env, "ttl", "b", { ttl: 60 });
    assert.equal(rows(env)[0].expires_at_ms, 5_090_000);
    mock.timers.tick(60_000);
    assert.equal((await get(env, "ttl")).status, 404);
    assert.deepEqual(await (await call(env, "/v1/stores/state/keys")).json(), { keys: [], cursor: null });
    await put(env, "ttl", "c");
    assert.equal(rows(env)[0].expires_at_ms, null);
    mock.timers.tick(365 * 24 * 3_600_000);
    assert.equal(await (await get(env, "ttl")).text(), "c");
  } finally {
    mock.timers.reset();
  }
});

test("alarms sweep expired rows and ledger entries but never the allocator", async (t) => {
  t.mock.timers.enable({ apis: ["Date"], now: 9_000_000 });
  const { object, storage, ready } = createObject();
  await ready();
  const internal = (meta, body) =>
    object.fetch(new Request(`https://edgezero-store.internal/${meta.op}?meta=${encodeURIComponent(JSON.stringify(meta))}`, {
      method: "POST",
      headers: { "x-edgezero-internal-version": "1" },
      body,
    }));
  await internal({ op: "put", key: "short", ttlSeconds: 60, mutationId: mutationId(), applyBefore: 9_010_000 }, "x");
  await internal({ op: "put", key: "forever", mutationId: mutationId(), applyBefore: 9_010_000 }, "y");
  assert.equal(storage.alarm, 9_060_000, "alarm moved to the earliest expiry");
  const writes = storage.alarmWrites;
  await internal({ op: "put", key: "forever", mutationId: mutationId(), applyBefore: 9_010_000 }, "z");
  assert.equal(storage.alarmWrites, writes, "a later deadline never rewrites the alarm");
  t.mock.timers.tick(60_000);
  await object.alarm();
  assert.deepEqual(storage.db.prepare("SELECT key FROM edgezero_rows").all().map((row) => row.key), ["forever"]);
  assert.equal(storage.alarm, 9_600_000, "next deadline is the ledger retention");
  t.mock.timers.tick(600_000);
  await object.alarm();
  assert.equal(storage.db.prepare("SELECT COUNT(*) AS n FROM edgezero_mutations").get().n, 0);
  assert.equal(storage.alarm, null);
  assert.equal(storage.db.prepare("SELECT allocator FROM edgezero_meta").get().allocator, "3");
  for (let index = 0; index < 3; index += 1) {
    t.mock.timers.tick(1_000);
    await internal({ op: "put", key: `steady-${index}`, mutationId: mutationId(), applyBefore: Date.now() + 10_000 }, "v");
  }
  // Two of the three entries have expired; the third expires 500 ms from now.
  t.mock.timers.tick(599_500);
  await object.alarm();
  assert.equal(storage.db.prepare("SELECT COUNT(*) AS n FROM edgezero_mutations").get().n, 1);
  assert.equal(storage.alarm, Date.now() + 60_000, "steady traffic cannot rearm the alarm back to back");
});

test("listing pages are bounded, ordered, prefix-exact and resumable to completion", async () => {
  const env = environment(trustedPolicy);
  for (const key of ["p/a", "p/b", "p/c", "p/d", "pa", "q"]) await put(env, key, "v");
  const seen = [];
  let cursor = null;
  let pages = 0;
  do {
    const query = `prefix=p%2F&limit=2${cursor === null ? "" : `&cursor=${cursor}`}`;
    const page = await (await call(env, `/v1/stores/state/keys?${query}`)).json();
    seen.push(...page.keys);
    cursor = page.cursor;
    pages += 1;
  } while (cursor !== null);
  assert.deepEqual(seen, ["p/a", "p/b", "p/c", "p/d"]);
  assert.equal(pages, 2, "an exact multiple ends without an extra empty page");
  const first = await (await call(env, "/v1/stores/state/keys?prefix=p%2F&limit=1")).json();
  assert.equal((await call(env, `/v1/stores/state/keys?prefix=q&limit=1&cursor=${first.cursor}`)).status, 400);
  assert.equal((await call(env, "/v1/stores/state/keys?prefix=p%2F&limit=1&cursor=AAAA")).status, 400);
  const all = await (await call(env, "/v1/stores/state/keys")).json();
  assert.deepEqual(all, { keys: ["p/a", "p/b", "p/c", "p/d", "pa", "q"], cursor: null });
});

test("a cursor from another store instance is rejected", async () => {
  const one = environment(trustedPolicy);
  for (const key of ["a", "b"]) await put(one, key, "v");
  const page = await (await call(one, "/v1/stores/state/keys?limit=1")).json();
  const two = environment(trustedPolicy);
  for (const key of ["a", "b"]) await put(two, key, "v");
  assert.equal((await call(two, `/v1/stores/state/keys?limit=1&cursor=${page.cursor}`)).status, 400);
});

test("reopening keeps the instance and allocator; lost metadata fails closed", async () => {
  const root = await mkdtemp(join(tmpdir(), "edgezero-store-test-"));
  try {
    const path = join(root, "cell.sqlite");
    const first = createObject(new FakeStorage(path));
    await first.ready();
    const instance = first.object.meta.instance;
    await first.object.fetch(new Request(`https://edgezero-store.internal/put?meta=${encodeURIComponent(JSON.stringify({ op: "put", key: "k", mutationId: mutationId(), applyBefore: Date.now() + 10_000 }))}`, {
      method: "POST",
      headers: { "x-edgezero-internal-version": "1" },
      body: "v",
    }));
    first.storage.db.close();
    const reopened = createObject(new FakeStorage(path));
    await reopened.ready();
    assert.equal(reopened.object.meta.instance, instance);
    assert.equal(reopened.object.meta.allocator, 1n);
    reopened.storage.db.exec("DELETE FROM edgezero_meta");
    reopened.storage.db.close();
    const broken = createObject(new FakeStorage(path));
    await broken.ready();
    const response = await broken.object.fetch(new Request(`https://edgezero-store.internal/capabilities?meta=${encodeURIComponent(JSON.stringify({ op: "capabilities" }))}`, {
      method: "POST",
      headers: { "x-edgezero-internal-version": "1" },
    }));
    assert.equal(response.status, 503);
    broken.storage.db.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("only the Worker's internal protocol reaches the object", async () => {
  const { object, ready } = createObject();
  await ready();
  const response = await object.fetch(new Request("https://edgezero-store.internal/value?meta=%7B%22op%22%3A%22value%22%2C%22key%22%3A%22k%22%7D", { method: "POST" }));
  assert.equal(response.status, 503);
});

test("the shipped deployment template carries fixed identities and a valid policy", async () => {
  const { readFile } = await import("node:fs/promises");
  const text = await readFile(new URL("../wrangler.template.jsonc", import.meta.url), "utf8");
  const config = JSON.parse(text.replace(/^\s*\/\/.*$/gm, ""));
  assert.equal(config.name, "edgezero-store");
  assert.equal(config.main, "src/index.js");
  assert.equal(config.no_bundle, true);
  assert.deepEqual(config.durable_objects.bindings, [{ name: "EDGEZERO_STORE", class_name: "EdgeZeroStore" }]);
  assert.deepEqual(config.migrations, [{ tag: "v1", new_sqlite_classes: ["EdgeZeroStore"] }]);
  assert.ok(internals.parsePolicy(config.vars.EDGEZERO_STORE_POLICY));
});
