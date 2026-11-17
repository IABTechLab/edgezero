// Node stand-ins for the celld Durable Object runtime pieces the service uses.
// SQLite is real (node:sqlite); routing and alarms are in-memory.
import { DatabaseSync } from "node:sqlite";
import worker, { EdgeZeroStore } from "../src/index.js";

export { worker, EdgeZeroStore };
export const internals = EdgeZeroStore.internals;

class FakeSql {
  constructor(db) {
    this.db = db;
  }

  exec(query, ...params) {
    const bound = params.map((value) => (value instanceof ArrayBuffer ? new Uint8Array(value) : value));
    const statement = this.db.prepare(query);
    const rows = /^\s*(SELECT|WITH)/i.test(query) ? statement.all(...bound) : (statement.run(...bound), []);
    const converted = rows.map((row) => {
      const out = {};
      for (const [key, value] of Object.entries(row)) {
        out[key] = value instanceof Uint8Array ? value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength) : value;
      }
      return out;
    });
    return { toArray: () => converted };
  }
}

export class FakeStorage {
  constructor(path = ":memory:") {
    this.db = new DatabaseSync(path);
    this.sql = new FakeSql(this.db);
    this.alarm = null;
    this.alarmWrites = 0;
  }

  transactionSync(callback) {
    this.db.exec("BEGIN");
    try {
      const result = callback();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  async getAlarm() {
    return this.alarm;
  }

  async setAlarm(time) {
    this.alarm = time;
    this.alarmWrites += 1;
  }

  async deleteAlarm() {
    this.alarm = null;
  }
}

export function createObject(storage = new FakeStorage()) {
  let ready;
  const state = {
    storage,
    blockConcurrencyWhile(callback) {
      ready = callback();
      return ready;
    },
  };
  const object = new EdgeZeroStore(state, {});
  return { object, storage, ready: () => ready };
}

/** A namespace whose stub can deliver each request more than once. */
export class FakeNamespace {
  constructor() {
    this.objects = new Map();
    this.deliveries = 1;
  }

  idFromName(name) {
    return name;
  }

  get(id) {
    return {
      fetch: async (request) => {
        if (!this.objects.has(id)) this.objects.set(id, createObject());
        const { object, ready } = this.objects.get(id);
        await ready();
        const body = request.body === null ? null : new Uint8Array(await request.arrayBuffer());
        let response;
        for (let attempt = 0; attempt < this.deliveries; attempt += 1) {
          response = await object.fetch(new Request(request.url, { method: request.method, headers: request.headers, body }));
        }
        return response;
      },
    };
  }
}

export const TOKEN = "fixture-token-not-a-production-credential-0123456789";

export async function sha256(text) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function authenticatedPolicy(operations = ["read", "write", "delete", "list"]) {
  return JSON.stringify({
    version: 1,
    stores: [{ alias: "state", namespace: "fixture-state" }, { alias: "other", namespace: "fixture-other" }],
    access: { mode: "authenticated", tokens: [{ id: "app", sha256: await sha256(TOKEN), grants: [{ store: "state", operations }] }] },
  });
}

export const trustedPolicy = JSON.stringify({
  version: 1,
  stores: [{ alias: "state", namespace: "fixture-state" }],
  access: { mode: "trusted-network", grants: [{ store: "state", operations: ["read", "write", "delete", "list"] }] },
});

export function environment(policy) {
  internals.resetPolicyCache();
  return { EDGEZERO_STORE_POLICY: policy, EDGEZERO_STORE: new FakeNamespace() };
}

let counter = 0;
export function mutationId() {
  counter += 1;
  return Buffer.from(`mutation-${String(counter).padStart(6, "0")}`).toString("base64url").slice(0, 22).padEnd(22, "A");
}

export function call(env, path, { method = "GET", body, token, id, headers = {} } = {}) {
  const all = { ...headers };
  if (token !== undefined) all.authorization = `Bearer ${token}`;
  if (id !== undefined) all["x-edgezero-mutation-id"] = id;
  return worker.fetch(new Request(`https://store.test${path}`, { method, headers: all, body }), env);
}
