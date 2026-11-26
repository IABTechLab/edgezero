// EdgeZero store service for stock celld.
//
// One module, deployed with `no_bundle`, so the official celld image can
// deploy it without esbuild. The Worker authenticates, authorizes and routes.
// Each data namespace is one SQLite Durable Object that applies every mutation
// in one synchronous transaction, records the mutation ID in that same
// transaction and answers duplicate deliveries with the first outcome.
//
// Stock celld resends a buffered Durable Object request once on a fresh peer
// tunnel after a pooled-connection failure, even if the owner already ran it
// (crates/celld/main/peer_tunnel.rs, send_inner, v0.6.3). The ledger is what
// keeps that resend from applying a mutation twice.

const SERVICE = "edgezero-celld-store";
const PROTOCOL = 1;
const FORMAT = "rows-v1";
const VERSION_HEADER = "x-edgezero-store-version";
const INSTANCE_HEADER = "x-edgezero-store-instance";
const MUTATION_HEADER = "x-edgezero-mutation-id";

const APPLY_BEFORE_HEADER = "x-edgezero-apply-before";
const INTERNAL_HEADER = "x-edgezero-internal-version";
const INTERNAL_VERSION = "1";
const OBJECT_PREFIX = "edgezero-store/v1/";

const LIMITS = Object.freeze({
  maxValueBytes: 1_048_576,
  maxMetadataBytes: 1_024,
  maxKeyBytes: 512,
  maxPageEntries: 1_000,
  defaultPageEntries: 100,
  maxCursorBytes: 2_048,
  minTtlSeconds: 60,
  maxTtlSeconds: 31_536_000,
  applyWindowMs: 10_000,
  ledgerRetentionMs: 600_000,
  sweepBatchRows: 256,
  // Reads already hide expired rows and the ledger outlives its window, so
  // sweeping is only cleanup. Spacing alarms bounds their bucket writes.
  sweepIntervalMs: 60_000,
  maxPolicyBytes: 262_144,
});

const OPERATIONS = new Set(["read", "write", "delete", "list"]);
const MAX_U64 = 18_446_744_073_709_551_615n;
const encoder = new TextEncoder();
const strictDecoder = new TextDecoder("utf-8", { fatal: true });

// ---------------------------------------------------------------------------
// Errors and responses

const ERRORS = Object.freeze({
  invalid_input: 400,
  unauthenticated: 401,
  forbidden: 403,
  not_found: 404,
  limit_exceeded: 413,
  unsupported_version: 426,
  unavailable: 503,
  deadline: 504,
});

class StoreError extends Error {
  constructor(category) {
    super(category);
    this.category = category;
  }
}

function fail(category) {
  throw new StoreError(category);
}

function errorResponse(category, head = false) {
  const status = ERRORS[category] ?? 503;
  const headers = { [VERSION_HEADER]: String(PROTOCOL) };
  if (head) return new Response(null, { status, headers });
  headers["content-type"] = "application/json";
  return new Response(JSON.stringify({ error: category in ERRORS ? category : "unavailable" }), {
    status,
    headers,
  });
}

// ---------------------------------------------------------------------------
// Validation helpers shared by the Worker and the object

function validKey(key, allowEmpty = false) {
  return (
    typeof key === "string" &&
    (allowEmpty || key.length > 0) &&
    encoder.encode(key).length <= LIMITS.maxKeyBytes &&
    !/[\u0000-\u001f\u007f-\u009f]/u.test(key)
  );
}

function validAlias(alias) {
  return typeof alias === "string" && /^[A-Za-z0-9_-]{1,64}$/.test(alias);
}

function validNamespace(namespace) {
  return (
    typeof namespace === "string" &&
    namespace.length > 0 &&
    encoder.encode(namespace).length <= 256 &&
    !/[\u0000-\u001f\u007f-\u009f]/u.test(namespace)
  );
}

function decimal(text) {
  if (typeof text !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(text)) fail("invalid_input");
  const value = BigInt(text);
  if (value > MAX_U64) fail("invalid_input");
  return value;
}

// Decode one query component exactly once. Malformed escapes and invalid
// UTF-8 fail instead of becoming replacement characters.
function decodeComponent(raw) {
  const bytes = [];
  for (let index = 0; index < raw.length; index += 1) {
    const code = raw.charCodeAt(index);
    if (code === 0x25) {
      const hex = raw.slice(index + 1, index + 3);
      if (!/^[0-9A-Fa-f]{2}$/.test(hex)) fail("invalid_input");
      bytes.push(Number.parseInt(hex, 16));
      index += 2;
    } else if (code === 0x2b) {
      bytes.push(0x20);
    } else if (code < 0x80) {
      bytes.push(code);
    } else {
      // The URL parser percent-encodes non-ASCII, so raw non-ASCII is foreign.
      fail("invalid_input");
    }
  }
  try {
    return strictDecoder.decode(new Uint8Array(bytes));
  } catch {
    return fail("invalid_input");
  }
}

function parseQuery(raw, allowed) {
  const out = new Map();
  if (raw === "") return out;
  for (const pair of raw.split("&")) {
    const split = pair.indexOf("=");
    if (split < 0) fail("invalid_input");
    const name = decodeComponent(pair.slice(0, split));
    const value = decodeComponent(pair.slice(split + 1));
    if (!allowed.includes(name) || out.has(name)) fail("invalid_input");
    out.set(name, value);
  }
  return out;
}

function base64url(bytes) {
  let text = "";
  for (const byte of bytes) text += String.fromCharCode(byte);
  return btoa(text).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

function fromBase64url(text) {
  if (!/^[A-Za-z0-9_-]*$/.test(text) || text.length % 4 === 1) fail("invalid_input");
  const padded = text.replaceAll("-", "+").replaceAll("_", "/") + "===".slice((text.length + 3) % 4);
  let binary;
  try {
    binary = atob(padded);
  } catch {
    return fail("invalid_input");
  }
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  if (base64url(bytes) !== text) fail("invalid_input");
  return bytes;
}

function hex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

// ---------------------------------------------------------------------------
// Policy

function exactKeys(value, required, optional = []) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return (
    required.every((key) => keys.includes(key)) &&
    keys.every((key) => required.includes(key) || optional.includes(key))
  );
}

function parseGrants(raw, stores) {
  if (!Array.isArray(raw)) throw new Error("grants");
  const grants = new Map();
  for (const grant of raw) {
    if (!exactKeys(grant, ["store", "operations"]) || !stores.has(grant.store)) {
      throw new Error("grant");
    }
    if (grants.has(grant.store) || !Array.isArray(grant.operations) || grant.operations.length === 0) {
      throw new Error("grant");
    }
    const operations = new Set();
    for (const operation of grant.operations) {
      if (!OPERATIONS.has(operation) || operations.has(operation)) throw new Error("operation");
      operations.add(operation);
    }
    grants.set(grant.store, operations);
  }
  return grants;
}

/** Parses and validates the deployment policy. Throws on any invalid input. */
function parsePolicy(source) {
  const text = typeof source === "string" ? source : JSON.stringify(source);
  if (typeof text !== "string" || encoder.encode(text).length > LIMITS.maxPolicyBytes) {
    throw new Error("policy size");
  }
  const raw = JSON.parse(text);
  if (!exactKeys(raw, ["version", "stores", "access"]) || raw.version !== 1) {
    throw new Error("policy root");
  }
  if (!Array.isArray(raw.stores) || raw.stores.length === 0) throw new Error("stores");
  const stores = new Map();
  for (const store of raw.stores) {
    if (!exactKeys(store, ["alias", "namespace"]) || !validAlias(store.alias)) {
      throw new Error("store");
    }
    if (!validNamespace(store.namespace) || stores.has(store.alias)) throw new Error("store");
    stores.set(store.alias, store.namespace);
  }
  const access = raw.access;
  if (access?.mode === "trusted-network") {
    if (!exactKeys(access, ["mode", "grants"])) throw new Error("access");
    return { stores, mode: "trusted-network", grants: parseGrants(access.grants, stores) };
  }
  if (access?.mode !== "authenticated" || !exactKeys(access, ["mode", "tokens"])) {
    throw new Error("access");
  }
  if (!Array.isArray(access.tokens) || access.tokens.length === 0) throw new Error("tokens");
  const ids = new Set();
  const verifiers = new Set();
  const tokens = [];
  for (const token of access.tokens) {
    if (!exactKeys(token, ["id", "sha256", "grants"])) throw new Error("token");
    if (typeof token.id !== "string" || !/^[A-Za-z0-9_.-]{1,64}$/.test(token.id) || ids.has(token.id)) {
      throw new Error("token id");
    }
    if (typeof token.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(token.sha256) || verifiers.has(token.sha256)) {
      throw new Error("token verifier");
    }
    ids.add(token.id);
    verifiers.add(token.sha256);
    const verifier = new Uint8Array(32);
    for (let index = 0; index < 32; index += 1) {
      verifier[index] = Number.parseInt(token.sha256.slice(index * 2, index * 2 + 2), 16);
    }
    tokens.push({ id: token.id, verifier, grants: parseGrants(token.grants, stores) });
  }
  return { stores, mode: "authenticated", tokens };
}

let cachedPolicy = { source: undefined, policy: undefined };

function loadPolicy(env) {
  const source = env.EDGEZERO_STORE_POLICY;
  if (cachedPolicy.source !== source) {
    let policy = null;
    try {
      policy = source === undefined ? null : parsePolicy(source);
    } catch {
      policy = null;
    }
    cachedPolicy = { source, policy };
  }
  return cachedPolicy.policy ?? fail("unavailable");
}

async function principalGrants(policy, headers) {
  const values = headers.get("authorization");
  if (policy.mode === "trusted-network") {
    // A supplied credential is never silently ignored.
    if (values !== null) fail("unauthenticated");
    return policy.grants;
  }
  // Headers joins repeated values with ", ". Tokens have no spaces, so a
  // combined value fails this grammar while a token may still contain commas.
  const match = values === null ? null : /^Bearer ([\x21-\x7e]{32,512})$/.exec(values);
  if (match === null) fail("unauthenticated");
  const token = match[1];
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(token)));
  let selected = null;
  // Compare against every verifier with no early exit.
  for (const candidate of policy.tokens) {
    let difference = 0;
    for (let index = 0; index < 32; index += 1) difference |= digest[index] ^ candidate.verifier[index];
    if (difference === 0) selected = candidate;
  }
  return selected === null ? fail("unauthenticated") : selected.grants;
}

// ---------------------------------------------------------------------------
// Worker

const ROUTES = Object.freeze({
  capabilities: { GET: { operation: null, query: [] } },
  value: {
    GET: { operation: "read", query: ["key"] },
    HEAD: { operation: "read", query: ["key"] },
  },
  keys: { GET: { operation: "list", query: ["prefix", "limit", "cursor"] } },
  put: { POST: { operation: "write", query: ["key", "ttl_seconds"] } },
  delete: { POST: { operation: "delete", query: ["key"] } },
});

function route(request) {
  const url = new URL(request.url);
  const parts = url.pathname.split("/");
  if (parts.length > 1 && /^v[0-9]+$/.test(parts[1]) && parts[1] !== "v1") {
    fail("unsupported_version");
  }
  if (parts.length !== 5 || parts[0] !== "" || parts[1] !== "v1" || parts[2] !== "stores") {
    fail("not_found");
  }
  const alias = parts[3];
  if (!validAlias(alias)) fail("invalid_input");
  const selected = ROUTES[parts[4]]?.[request.method];
  if (selected === undefined) fail("invalid_input");
  const query = parseQuery(url.search.slice(1), selected.query);
  return { alias, resource: parts[4], operation: selected.operation, query };
}

async function readBody(request, allowed) {
  const encoding = request.headers.get("content-encoding");
  if (encoding !== null && encoding.toLowerCase() !== "identity") fail("invalid_input");
  const declared = request.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || Number(declared) > LIMITS.maxValueBytes)) {
    fail(/^[0-9]+$/.test(declared) ? "limit_exceeded" : "invalid_input");
  }
  if (request.body === null) return new Uint8Array(0);
  const chunks = [];
  let size = 0;
  const reader = request.body.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > LIMITS.maxValueBytes) {
      await reader.cancel();
      fail("limit_exceeded");
    }
    chunks.push(value);
  }
  if (!allowed && size !== 0) fail("invalid_input");
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function internalRequest(op, meta, body) {
  const url = `https://edgezero-store.internal/${op}?meta=${encodeURIComponent(JSON.stringify(meta))}`;
  return new Request(url, {
    method: "POST",
    headers: { [INTERNAL_HEADER]: INTERNAL_VERSION },
    body: body === null ? null : body,
  });
}

function withVersion(response, head) {
  const headers = new Headers(response.headers);
  headers.set(VERSION_HEADER, String(PROTOCOL));
  return new Response(head ? null : response.body, { status: response.status, headers });
}

async function handleWorker(request, env) {
  const head = request.method === "HEAD";
  const received = Date.now();
  try {
    const selected = route(request);
    const policy = loadPolicy(env);
    const grants = await principalGrants(policy, request.headers);
    const namespace = policy.stores.get(selected.alias);
    const operations = grants.get(selected.alias);
    if (namespace === undefined || operations === undefined) fail("forbidden");
    if (selected.operation !== null && !operations.has(selected.operation)) fail("forbidden");
    const body = await readBody(request, selected.resource === "put");
    const meta = { op: selected.resource };
    const query = selected.query;
    if (selected.resource === "value" || selected.resource === "put" || selected.resource === "delete") {
      meta.key = query.get("key");
      if (!validKey(meta.key)) fail("invalid_input");
    }
    if (selected.resource === "put" && query.has("ttl_seconds")) {
      const ttl = decimal(query.get("ttl_seconds"));
      if (ttl < BigInt(LIMITS.minTtlSeconds) || ttl > BigInt(LIMITS.maxTtlSeconds)) fail("invalid_input");
      meta.ttlSeconds = Number(ttl);
    }
    if (selected.resource === "keys") {
      meta.prefix = query.get("prefix") ?? "";
      if (!validKey(meta.prefix, true)) fail("invalid_input");
      const limit = query.has("limit") ? decimal(query.get("limit")) : BigInt(LIMITS.defaultPageEntries);
      if (limit < 1n || limit > BigInt(LIMITS.maxPageEntries)) fail("invalid_input");
      meta.limit = Number(limit);
      if (query.has("cursor")) {
        const cursor = query.get("cursor");
        if (cursor.length === 0 || cursor.length > LIMITS.maxCursorBytes) fail("invalid_input");
        meta.cursor = cursor;
      }
    }
    if (selected.resource === "value") meta.head = head;
    if (selected.resource === "put" || selected.resource === "delete") {
      const id = request.headers.get(MUTATION_HEADER);
      if (id === null || !/^[A-Za-z0-9_-]{22}$/.test(id)) fail("invalid_input");
      meta.mutationId = id;
      meta.applyBefore = received + LIMITS.applyWindowMs;
    }
    const stub = env.EDGEZERO_STORE.get(env.EDGEZERO_STORE.idFromName(OBJECT_PREFIX + namespace));
    let response;
    try {
      response = await stub.fetch(internalRequest(selected.resource, meta, selected.resource === "put" ? body : null));
    } catch {
      // The owner may have run a mutation; the client reports possible commit.
      fail("unavailable");
    }
    return withVersion(response, head);
  } catch (error) {
    return errorResponse(error instanceof StoreError ? error.category : "unavailable", head);
  }
}

export default {
  fetch: handleWorker,
};

// ---------------------------------------------------------------------------
// Durable Object

const SCHEMA_VERSION = 1;

function tableExists(sql, name) {
  return (
    sql.exec("SELECT 1 AS found FROM sqlite_master WHERE type = 'table' AND name = ?", name).toArray().length > 0
  );
}

/** Opens or creates the storage format. Never resets an existing allocator. */
function openSchema(storage) {
  const sql = storage.sql;
  return storage.transactionSync(() => {
    const hasMeta = tableExists(sql, "edgezero_meta");
    const hasRows = tableExists(sql, "edgezero_rows");
    if (!hasMeta && hasRows) throw new Error("edgezero store metadata is missing");
    if (!hasMeta) {
      sql.exec(`CREATE TABLE edgezero_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        schema_version INTEGER NOT NULL,
        instance TEXT NOT NULL,
        allocator TEXT NOT NULL
      )`);
      sql.exec(`CREATE TABLE edgezero_rows (
        key TEXT PRIMARY KEY,
        body BLOB NOT NULL,
        metadata TEXT,
        generation TEXT NOT NULL,
        expires_at_ms INTEGER
      ) WITHOUT ROWID`);
      sql.exec("CREATE INDEX edgezero_rows_expiry ON edgezero_rows (expires_at_ms) WHERE expires_at_ms IS NOT NULL");
      sql.exec(`CREATE TABLE edgezero_mutations (
        id TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        outcome TEXT NOT NULL,
        expires_at_ms INTEGER NOT NULL
      ) WITHOUT ROWID`);
      sql.exec("CREATE INDEX edgezero_mutations_expiry ON edgezero_mutations (expires_at_ms)");
      const instance = base64url(crypto.getRandomValues(new Uint8Array(16)));
      sql.exec(
        "INSERT INTO edgezero_meta (singleton, schema_version, instance, allocator) VALUES (1, ?, ?, '0')",
        SCHEMA_VERSION,
        instance,
      );
    }
    return readMeta(sql);
  });
}

function readMeta(sql) {
  const rows = sql.exec("SELECT schema_version, instance, allocator FROM edgezero_meta WHERE singleton = 1").toArray();
  if (rows.length !== 1) throw new Error("edgezero store metadata is missing");
  const meta = rows[0];
  if (meta.schema_version !== SCHEMA_VERSION) throw new Error("unsupported edgezero store schema");
  if (typeof meta.instance !== "string" || !/^[A-Za-z0-9_-]{22}$/.test(meta.instance)) {
    throw new Error("invalid edgezero store instance");
  }
  const allocator = generation(meta.allocator, true);
  return { instance: meta.instance, allocator };
}

function generation(text, allowZero = false) {
  if (typeof text !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(text)) {
    throw new Error("invalid generation state");
  }
  const value = BigInt(text);
  if ((!allowZero && value === 0n) || value > MAX_U64) throw new Error("invalid generation state");
  return value;
}

function live(row, now) {
  return row !== undefined && (row.expires_at_ms === null || row.expires_at_ms > now);
}

// Smallest string greater than every string with this prefix, in UTF-8 byte
// order (which equals code point order). Null when no bound exists.
function prefixUpperBound(prefix) {
  const points = Array.from(prefix);
  while (points.length > 0) {
    let next = points.pop().codePointAt(0) + 1;
    if (next === 0xd800) next = 0xe000;
    if (next <= 0x10ffff) return points.join("") + String.fromCodePoint(next);
  }
  return null;
}

function encodeCursor(instance, prefix, after) {
  const id = fromBase64url(instance);
  const prefixBytes = encoder.encode(prefix);
  const afterBytes = encoder.encode(after);
  const bytes = new Uint8Array(1 + id.length + 2 + prefixBytes.length + 2 + afterBytes.length);
  let offset = 0;
  bytes[offset++] = 1;
  bytes.set(id, offset);
  offset += id.length;
  for (const part of [prefixBytes, afterBytes]) {
    bytes[offset++] = part.length >> 8;
    bytes[offset++] = part.length & 0xff;
    bytes.set(part, offset);
    offset += part.length;
  }
  return base64url(bytes);
}

function decodeCursor(token, instance, prefix) {
  const bytes = fromBase64url(token);
  const id = fromBase64url(instance);
  if (bytes.length < 1 + id.length + 4 || bytes[0] !== 1) fail("invalid_input");
  for (let index = 0; index < id.length; index += 1) {
    if (bytes[1 + index] !== id[index]) fail("invalid_input");
  }
  let offset = 1 + id.length;
  const parts = [];
  for (let part = 0; part < 2; part += 1) {
    if (offset + 2 > bytes.length) fail("invalid_input");
    const size = (bytes[offset] << 8) | bytes[offset + 1];
    offset += 2;
    if (offset + size > bytes.length) fail("invalid_input");
    try {
      parts.push(strictDecoder.decode(bytes.subarray(offset, offset + size)));
    } catch {
      fail("invalid_input");
    }
    offset += size;
  }
  if (offset !== bytes.length || parts[0] !== prefix || !validKey(parts[1]) || !parts[1].startsWith(prefix)) {
    fail("invalid_input");
  }
  return parts[1];
}

async function fingerprint(meta, body) {
  const head = encoder.encode(JSON.stringify([meta.op, meta.key, meta.ttlSeconds ?? null]));
  const bytes = new Uint8Array(head.length + 1 + (body?.length ?? 0));
  bytes.set(head, 0);
  bytes[head.length] = 0;
  if (body) bytes.set(body, head.length + 1);
  return hex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)));
}

function toArrayBuffer(bytes) {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

export class EdgeZeroStore {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.storage = state.storage;
    this.sql = state.storage.sql;
    this.meta = null;
    this.broken = false;
    state.blockConcurrencyWhile(async () => {
      try {
        this.meta = openSchema(this.storage);
      } catch {
        // Fail every request instead of reinitializing durable state.
        this.broken = true;
      }
    });
  }

  respond(status, body, contentType) {
    const headers = { [INSTANCE_HEADER]: this.meta.instance };
    if (contentType !== undefined) headers["content-type"] = contentType;
    return new Response(body, { status, headers });
  }

  error(category) {
    const response = errorResponse(category);
    const headers = new Headers(response.headers);
    headers.delete(VERSION_HEADER);
    if (this.meta !== null) headers.set(INSTANCE_HEADER, this.meta.instance);
    return new Response(response.body, { status: response.status, headers });
  }

  async fetch(request) {
    try {
      if (this.broken || this.meta === null) fail("unavailable");
      if (request.headers.get(INTERNAL_HEADER) !== INTERNAL_VERSION) fail("unavailable");
      const url = new URL(request.url);
      const op = url.pathname.slice(1);
      const meta = JSON.parse(url.searchParams.get("meta"));
      if (meta?.op !== op) fail("unavailable");
      switch (op) {
        case "capabilities":
          return this.respond(200, JSON.stringify(this.capabilities()), "application/json");
        case "value":
          return this.value(meta);
        case "keys":
          return this.keys(meta);
        case "put":
        case "delete": {
          const body = op === "put" ? new Uint8Array(await request.arrayBuffer()) : null;
          return await this.mutate(meta, body);
        }
        default:
          return fail("unavailable");
      }
    } catch (error) {
      return this.error(error instanceof StoreError ? error.category : "unavailable");
    }
  }

  capabilities() {
    return {
      service: SERVICE,
      protocol: PROTOCOL,
      format: FORMAT,
      pagination: "keyset",
      no_expiry: true,
      mutation_ledger: true,
      records: false,
      max_value_bytes: LIMITS.maxValueBytes,
      max_metadata_bytes: LIMITS.maxMetadataBytes,
      instance: this.meta.instance,
    };
  }

  row(key) {
    return this.sql
      .exec("SELECT body, metadata, generation, expires_at_ms FROM edgezero_rows WHERE key = ?", key)
      .toArray()[0];
  }

  value(meta) {
    const row = this.row(meta.key);
    if (!live(row, Date.now())) return this.error("not_found");
    if (meta.head) return this.respond(200, null);
    return this.respond(200, row.body, "application/octet-stream");
  }

  keys(meta) {
    const after = meta.cursor === undefined ? null : decodeCursor(meta.cursor, this.meta.instance, meta.prefix);
    const upper = prefixUpperBound(meta.prefix);
    const clauses = [after === null ? "key >= ?" : "key > ?"];
    const params = [after ?? meta.prefix];
    if (upper !== null) {
      clauses.push("key < ?");
      params.push(upper);
    }
    clauses.push("(expires_at_ms IS NULL OR expires_at_ms > ?)");
    params.push(Date.now(), meta.limit + 1);
    const rows = this.sql
      .exec(`SELECT key FROM edgezero_rows WHERE ${clauses.join(" AND ")} ORDER BY key LIMIT ?`, ...params)
      .toArray();
    const keys = rows.slice(0, meta.limit).map((row) => row.key);
    if (!keys.every((key) => key.startsWith(meta.prefix))) fail("unavailable");
    const cursor = rows.length > meta.limit ? encodeCursor(this.meta.instance, meta.prefix, keys.at(-1)) : null;
    return this.respond(200, JSON.stringify({ keys, cursor }), "application/json");
  }

  async mutate(meta, body) {
    if (!validKey(meta.key) || typeof meta.mutationId !== "string" || !Number.isSafeInteger(meta.applyBefore)) {
      fail("invalid_input");
    }
    if (body !== null && body.length > LIMITS.maxValueBytes) fail("limit_exceeded");
    const print = await fingerprint(meta, body);
    const outcome = this.storage.transactionSync(() => {
      const prior = this.sql
        .exec("SELECT fingerprint, outcome FROM edgezero_mutations WHERE id = ?", meta.mutationId)
        .toArray()[0];
      if (prior !== undefined) {
        // A duplicate delivery returns the first outcome and applies nothing.
        return prior.fingerprint === print ? prior.outcome : "invalid_input";
      }
      const now = Date.now();
      if (now > meta.applyBefore) return "deadline";
      let result;
      if (meta.op === "put") {
        const { allocator } = readMeta(this.sql);
        if (allocator === MAX_U64) throw new Error("generation allocator exhausted");
        const next = String(allocator + 1n);
        this.sql.exec("UPDATE edgezero_meta SET allocator = ? WHERE singleton = 1", next);
        const expires = meta.ttlSeconds === undefined ? null : now + meta.ttlSeconds * 1000;
        this.sql.exec(
          `INSERT INTO edgezero_rows (key, body, metadata, generation, expires_at_ms)
           VALUES (?, ?, NULL, ?, ?)
           ON CONFLICT (key) DO UPDATE SET body = excluded.body, metadata = NULL,
             generation = excluded.generation, expires_at_ms = excluded.expires_at_ms`,
          meta.key,
          toArrayBuffer(body),
          next,
          expires,
        );
        result = "written";
      } else {
        this.sql.exec("DELETE FROM edgezero_rows WHERE key = ?", meta.key);
        result = "deleted";
      }
      this.sql.exec(
        "INSERT INTO edgezero_mutations (id, fingerprint, outcome, expires_at_ms) VALUES (?, ?, ?, ?)",
        meta.mutationId,
        print,
        result,
        now + LIMITS.ledgerRetentionMs,
      );
      return result;
    });
    if (outcome === "written" || outcome === "deleted") {
      await this.rearm();
      return this.respond(204, null);
    }
    return this.error(outcome);
  }

  nextDeadline() {
    const row = this.sql
      .exec("SELECT MIN(expires_at_ms) AS next FROM edgezero_rows WHERE expires_at_ms IS NOT NULL")
      .toArray()[0];
    const ledger = this.sql.exec("SELECT MIN(expires_at_ms) AS next FROM edgezero_mutations").toArray()[0];
    const candidates = [row?.next, ledger?.next].filter((value) => typeof value === "number");
    return candidates.length === 0 ? null : Math.min(...candidates);
  }

  // A durable alarm write costs a bucket round trip in celld, so only move the
  // alarm earlier. The alarm handler recomputes the next deadline itself.
  async rearm() {
    const next = this.nextDeadline();
    if (next === null) return;
    const armed = await this.storage.getAlarm();
    if (armed === null || armed > next) await this.storage.setAlarm(Math.max(next, Date.now() + 1));
  }

  async alarm() {
    if (this.broken || this.meta === null) return;
    const now = Date.now();
    const batch = LIMITS.sweepBatchRows;
    const removed = this.storage.transactionSync(() => {
      const rows = this.sql
        .exec(
          "SELECT key FROM edgezero_rows WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= ? ORDER BY expires_at_ms LIMIT ?",
          now,
          batch,
        )
        .toArray();
      for (const row of rows) {
        this.sql.exec("DELETE FROM edgezero_rows WHERE key = ? AND expires_at_ms <= ?", row.key, now);
      }
      const ledger = this.sql
        .exec("SELECT id FROM edgezero_mutations WHERE expires_at_ms <= ? ORDER BY expires_at_ms LIMIT ?", now, batch)
        .toArray();
      for (const entry of ledger) this.sql.exec("DELETE FROM edgezero_mutations WHERE id = ?", entry.id);
      return Math.max(rows.length, ledger.length);
    });
    if (removed >= batch) {
      await this.storage.setAlarm(Date.now() + 1_000);
      return;
    }
    const next = this.nextDeadline();
    if (next === null) await this.storage.deleteAlarm();
    else await this.storage.setAlarm(Math.max(next, Date.now() + LIMITS.sweepIntervalMs));
  }
}

// celld accepts only handlers and classes as module exports, so tests reach
// the pure helpers through this static property.
EdgeZeroStore.internals = Object.freeze({
  SERVICE,
  PROTOCOL,
  FORMAT,
  VERSION_HEADER,
  INSTANCE_HEADER,
  MUTATION_HEADER,
  LIMITS,
  StoreError,
  decodeComponent,
  openSchema,
  parsePolicy,
  parseQuery,
  prefixUpperBound,
  resetPolicyCache() {
    cachedPolicy = { source: undefined, policy: undefined };
  },
  validAlias,
  validKey,
});
