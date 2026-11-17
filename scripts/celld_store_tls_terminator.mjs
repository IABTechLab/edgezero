#!/usr/bin/env node
// Test-only TLS terminator for the store service proof. celld does not
// terminate TLS, so deployments put a proxy in front of its public listener.
// This stands in for that proxy on loopback. It never retries a request.
import { readFileSync } from "node:fs";
import net from "node:net";
import tls from "node:tls";
import { pathToFileURL } from "node:url";

export function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    const value = argv[index + 1];
    if (!["--cert", "--key", "--target", "--listen"].includes(name) || value === undefined) {
      throw new Error(`usage: celld_store_tls_terminator.mjs --cert PEM --key PEM --target HOST:PORT [--listen HOST:PORT]`);
    }
    options[name.slice(2)] = value;
  }
  if (!options.cert || !options.key || !options.target) throw new Error("--cert, --key and --target are required");
  return { listen: "127.0.0.1:0", ...options };
}

function split(address) {
  const index = address.lastIndexOf(":");
  const port = Number(address.slice(index + 1));
  if (index <= 0 || !Number.isInteger(port) || port < 0 || port > 65_535) throw new Error(`invalid address ${address}`);
  return { host: address.slice(0, index), port };
}

export function start(options, log = console.log) {
  const target = split(options.target);
  const listen = split(options.listen);
  const server = tls.createServer({ cert: readFileSync(options.cert), key: readFileSync(options.key) }, (client) => {
    const upstream = net.connect(target.port, target.host);
    client.pipe(upstream);
    upstream.pipe(client);
    const close = () => {
      client.destroy();
      upstream.destroy();
    };
    client.on("error", close);
    upstream.on("error", close);
    client.on("close", close);
    upstream.on("close", close);
  });
  server.listen(listen.port, listen.host, () => {
    const { address, port } = server.address();
    log(`listening on ${address}:${port}`);
  });
  return server;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const server = start(parseArgs(process.argv.slice(2)));
    for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => { server.close(); process.exit(0); });
  } catch (error) {
    console.error(error.message);
    process.exitCode = 2;
  }
}
