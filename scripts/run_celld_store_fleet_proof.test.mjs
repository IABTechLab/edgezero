import assert from "node:assert/strict";
import { connect, createServer } from "node:net";
import test from "node:test";
import { EdgeZeroStore } from "../services/celld-store/src/index.js";
import { parseArgs } from "./celld_store_tls_terminator.mjs";
import { faultProxy, runFleetProof, serviceConfig } from "./run_celld_store_fleet_proof.mjs";

test("the deployed configuration carries a policy the service accepts", () => {
  const config = serviceConfig();
  assert.equal(config.name, "edgezero-store");
  assert.equal(config.no_bundle, true);
  assert.deepEqual(config.migrations, [{ tag: "v1", new_sqlite_classes: ["EdgeZeroStore"] }]);
  assert.equal(EdgeZeroStore.internals.parsePolicy(config.vars.EDGEZERO_STORE_POLICY).mode, "trusted-network");
});

test("a missing official binary fails before any command", async () => {
  await assert.rejects(runFleetProof({ env: {}, log: () => {} }), /CELLD_BIN must select/);
});

function listen(server) {
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve(server.address().port)));
}

function exchange(port, payload) {
  return new Promise((resolve) => {
    const socket = connect(port, "127.0.0.1", () => socket.write(payload));
    let received = "";
    socket.on("data", (chunk) => { received += chunk; });
    socket.on("close", () => resolve(received));
    socket.setTimeout(2_000, () => socket.destroy());
  });
}

test("the fault proxy forwards a marked request once, drops its reply, then passes traffic", async () => {
  const seen = [];
  const target = createServer((socket) => socket.on("data", (chunk) => {
    seen.push(chunk.toString());
    socket.end(`reply:${chunk}`);
  }));
  const targetPort = await listen(target);
  const probe = createServer();
  const proxyPort = await listen(probe);
  await new Promise((resolve) => probe.close(resolve));
  const proxy = faultProxy(proxyPort, targetPort);
  try {
    await new Promise((resolve) => proxy.server.once("listening", resolve));
    proxy.forwarded.set("marker", 0);
    proxy.dropNext = "marker";
    assert.equal(await exchange(proxyPort, "request-with-marker"), "");
    assert.deepEqual(seen, ["request-with-marker"], "the target ran the request");
    assert.equal(proxy.forwarded.get("marker"), 1);
    assert.equal(await exchange(proxyPort, "plain"), "reply:plain");
  } finally {
    proxy.server.close();
    target.close();
  }
});

test("the fault proxy can hold delivery before the target runs it", async () => {
  const target = createServer((socket) => socket.on("data", (chunk) => socket.end(`reply:${chunk}`)));
  const targetPort = await listen(target);
  const probe = createServer();
  const proxyPort = await listen(probe);
  await new Promise((resolve) => probe.close(resolve));
  const proxy = faultProxy(proxyPort, targetPort);
  try {
    await new Promise((resolve) => proxy.server.once("listening", resolve));
    const until = Date.now() + 100;
    proxy.holdUntil.set("held-marker", until);
    assert.equal(await exchange(proxyPort, "held-marker"), "reply:held-marker");
    assert.ok(Date.now() >= until, "delivery was not released before the selected deadline");
  } finally {
    proxy.server.close();
    target.close();
  }
});

test("the TLS terminator requires explicit certificate, key and target", () => {
  assert.throws(() => parseArgs([]), /required/);
  assert.throws(() => parseArgs(["--cert", "c"]), /required/);
  assert.throws(() => parseArgs(["--bogus", "x"]), /usage/);
  assert.deepEqual(parseArgs(["--cert", "c", "--key", "k", "--target", "127.0.0.1:1"]), {
    listen: "127.0.0.1:0",
    cert: "c",
    key: "k",
    target: "127.0.0.1:1",
  });
});
