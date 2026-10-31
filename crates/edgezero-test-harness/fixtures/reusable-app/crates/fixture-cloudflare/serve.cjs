const { realpathSync } = require("node:fs");
const { createRequire } = require("node:module");
const { dirname, isAbsolute, join, relative, sep } = require("node:path");

// Rust owns supervision and assertions; this starts one native workerd listener.
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
  const { Miniflare, MiniflareCoreError, Log, LogLevel } = createRequire(
    selected.resolve("wrangler"),
  )("miniflare");
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
    worker = new Miniflare({
      ...mapped.workerOptions,
      scriptPath: mapped.main,
      modules: true,
      modulesRoot: options.modulesRoot,
      modulesRules: [
        { type: "ESModule", include: ["**/*.js", "**/*.mjs"] },
        ...mapped.workerOptions.modulesRules,
      ],
      bindings: { ...mapped.workerOptions.bindings, ...options.bindings },
      host: options.host,
      port: options.port,
      defaultPersistRoot: join(options.persist, "v3"),
      liveReload: false,
      cf: false,
      telemetry: { enabled: false },
      log: new RuntimeLog(LogLevel.WARN),
      handleStructuredLogs: ({ message }) => console.log(message),
    });
    if (stopping) {
      await dispose();
      return;
    }
    const listener = await worker.ready;
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
