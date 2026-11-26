const { lstatSync, realpathSync } = require("node:fs");
const { createRequire } = require("node:module");
const { dirname, isAbsolute, join, relative, sep } = require("node:path");

// Rust owns supervision and assertions; clients enter the native application socket.
async function serve() {
  const [executable, serialized] = process.argv.slice(2);
  const canonical = realpathSync(executable);
  const selected = createRequire(canonical);
  const packageRoot = dirname(selected.resolve("wrangler/package.json"));
  const location = relative(packageRoot, canonical);
  if (
    isAbsolute(location) ||
    location === ".." ||
    location.startsWith(`..${sep}`)
  ) {
    throw new Error("selected executable is outside the Wrangler package");
  }
  const wrangler = selected("wrangler");
  const {
    Miniflare,
    MiniflareCoreError,
    Log,
    LogLevel,
    compileModuleRules,
    convertV4MiniflareOptions,
    testRegExps,
  } = createRequire(selected.resolve("wrangler"))("miniflare");
  const options = JSON.parse(serialized);
  let worker;
  let stopping = false;
  let disposed;
  const dispose = () =>
    (disposed ??= Promise.resolve().then(() => worker?.dispose()));
  const stop = () => {
    stopping = true;
    if (worker)
      void dispose().catch(() => {
        process.exitCode = 1;
      });
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  const cancelledByShutdown = (error) =>
    stopping &&
    error instanceof MiniflareCoreError &&
    error.code === "ERR_DISPOSED";
  class RuntimeLog extends Log {
    error(error) {
      if (cancelledByShutdown(error)) return;
      console.error("Cloudflare fixture runtime failed");
      process.exitCode = 1;
      stop();
    }
  }
  try {
    const mapped = wrangler.unstable_getMiniflareWorkerOptions(options.config);
    const nativeOptions = new Set([
      "compatibilityDate",
      "compatibilityFlags",
      "modulesRules",
      "bindings",
      "kvNamespaces",
      "rootPath",
    ]);
    // Wrangler emits nested empty defaults for bindings that are not configured.
    const isConfigured = (value) =>
      value !== undefined &&
      (value === null ||
        typeof value !== "object" ||
        Object.values(value).some(isConfigured));
    const populatedExtras = Object.entries(mapped.workerOptions).some(
      ([name, value]) => !nativeOptions.has(name) && isConfigured(value),
    );
    const mainLocation = relative(options.modulesRoot, mapped.main);
    if (
      Object.keys(mapped.define).length ||
      mapped.externalWorkers.length ||
      populatedExtras ||
      (mapped.workerOptions.rootPath !== undefined &&
        (!isAbsolute(options.config) ||
          mapped.workerOptions.rootPath !== dirname(options.config))) ||
      Object.values(mapped.workerOptions.kvNamespaces).some(
        (value) =>
          !value ||
          typeof value !== "object" ||
          Object.keys(value).length !== 1 ||
          typeof value.id !== "string" ||
          !value.id,
      ) ||
      Object.keys(options.bindings).sort().join() !==
        "EDGEZERO_FIXTURE_LAUNCH_NONCE,FIXTURE_MODE" ||
      options.host !== "127.0.0.1" ||
      !Number.isInteger(options.port) ||
      options.port < 1 ||
      options.port > 65535 ||
      !isAbsolute(mapped.main) ||
      isAbsolute(mainLocation) ||
      mainLocation === ".." ||
      mainLocation.startsWith(`..${sep}`)
    ) {
      throw new Error(
        "fixture requires bundled source and no external workers",
      );
    }
    const root = realpathSync(options.modulesRoot);
    if (
      !Array.isArray(options.moduleFiles) ||
      !options.moduleFiles.length ||
      options.moduleFiles.length > 128
    ) {
      throw new Error("fixture requires a bounded module inventory");
    }
    const rules = compileModuleRules([
      { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
      ...mapped.workerOptions.modulesRules,
    ]);
    const names = new Set();
    const modules = [];
    for (const path of options.moduleFiles) {
      const name = relative(root, path);
      if (
        !isAbsolute(path) ||
        isAbsolute(name) ||
        name === ".." ||
        name.startsWith(`..${sep}`) ||
        !lstatSync(path).isFile() ||
        realpathSync(path) !== path ||
        names.has(name)
      ) {
        throw new Error(
          "fixture module is duplicate or outside its staged root",
        );
      }
      names.add(name);
      const rule = rules.find((candidate) =>
        testRegExps(candidate.include, name.split(sep).join("/")),
      );
      if (rule) modules.push({ type: rule.type, path });
    }
    const main = realpathSync(mapped.main);
    const first = modules.findIndex((module) => module.path === main);
    if (first < 0 || modules[first].type !== "ESModule") {
      throw new Error("fixture entrypoint is absent from its module inventory");
    }
    modules.unshift(...modules.splice(first, 1));
    // Resolve rules before conversion; the SDK intentionally refuses lossy rule conversion.
    const settings = { ...mapped.workerOptions };
    delete settings.modulesRules;
    const native = convertV4MiniflareOptions({
      ...settings,
      modules,
      modulesRoot: root,
      bindings: { ...mapped.workerOptions.bindings, ...options.bindings },
      host: options.host,
      port: 0,
      unsafeDirectSockets: [{
        host: options.host,
        port: options.port,
        entrypoint: "default",
        proxy: false,
      }],
      resourcePersistencePath: join(options.persist, "v3"),
      cf: false,
      telemetry: { enabled: false },
      log: new RuntimeLog(LogLevel.WARN),
      handleStructuredLogs: ({ message }) => console.log(message),
    });
    worker = new Miniflare(native);
    if (stopping) {
      await dispose();
      return;
    }
    const entry = await worker.ready;
    if (
      entry.protocol !== "http:" ||
      entry.hostname !== options.host ||
      !Number.isInteger(Number(entry.port)) ||
      Number(entry.port) < 1 ||
      Number(entry.port) > 65535 ||
      Number(entry.port) === options.port
    ) {
      throw new Error("fixture control listener is not distinct loopback HTTP");
    }
    // Bypass entry forwarding so this socket uses the guest's compatibility flags.
    const listener = await worker.unsafeGetDirectURL(undefined, "default");
    if (
      listener.protocol !== "http:" ||
      listener.hostname !== options.host ||
      Number(listener.port) !== options.port
    ) {
      throw new Error("fixture listener differs from configured address");
    }
    if (stopping) await dispose();
  } catch (error) {
    if (!cancelledByShutdown(error)) {
      process.exitCode = 1;
      console.error("Cloudflare fixture runtime startup failed");
    }
    await dispose().catch(() => {
      process.exitCode = 1;
    });
  }
}

serve().catch(() => {
  console.error("Cloudflare fixture runtime initialization failed");
  process.exitCode = 1;
});
