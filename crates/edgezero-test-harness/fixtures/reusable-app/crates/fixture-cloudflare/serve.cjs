const { realpathSync } = require("node:fs");
const { createRequire } = require("node:module");
const { dirname, isAbsolute, relative, sep } = require("node:path");

// Rust owns supervision and deadlines; this only exposes Wrangler's inspector-free API.
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
  const { unstable_startWorker } = selected("wrangler");
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
  try {
    worker = await unstable_startWorker(options);
    worker.raw.on("error", () => {
      if (stopping) return;
      console.error("Cloudflare fixture runtime failed");
      process.exitCode = 1;
      stop();
    });
    if (stopping) {
      await dispose();
      return;
    }
    await worker.ready;
    if (stopping) await dispose();
  } catch {
    process.exitCode = 1;
    console.error("Cloudflare fixture runtime startup failed");
    await dispose().catch(() => {
      process.exitCode = 1;
    });
  }
}

serve().catch(() => {
  console.error("Cloudflare fixture runtime initialization failed");
  process.exitCode = 1;
});
