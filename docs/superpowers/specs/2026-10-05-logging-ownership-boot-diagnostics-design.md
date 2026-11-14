# Logging Ownership and Boot Diagnostics

## Contract

`Hooks::owns_logging() == true` means the application owns backend installation,
backend filters, and the `log` facade maximum. Adapters must not replace or
reconfigure that logger. Applications install it before the adapter entrypoint.

Core exposes a pure `resolve_logging_level(&EnvConfig) -> log::LevelFilter`
helper for `EDGEZERO__LOGGING__LEVEL`. It preserves the existing case-insensitive
parser and `Info` fallback for missing or invalid values; it does not install a
logger, read process environment, or change global filters. Manifest logging
resolution remains separate and is not implicitly applied by this helper.

`BOOT_LOG_TARGET` names `edgezero::boot`; `BOOT_LOG_LEVEL` is `Warn`. Adapter-owned
logging on all four adapters preserves errors and warnings on that target even when
ordinary logging is `Error` or `Off`. Each backend and the facade must admit the
maximum of the ordinary and boot levels, while the outer target-aware filter
continues to suppress ordinary records above their configured level. The boot
target is a category, not a new Fastly log endpoint.

Fastly standard and custom lifecycles use one app-aware managed initializer before
assembly or the custom callback. `owns_logging()` bypasses both configuration lookup
and installation. Custom lifecycles take the Hooks type explicitly; no old signature
or compatibility wrapper remains. Manual prebuilt dispatch still owns its setup.

Endpoint and stdout policy remain manifest-owned. An optional logging-only Config Store
linked as `edgezero_logging` may supply `EDGEZERO__LOGGING__LEVEL`. Missing store/key
preserves the manifest level; invalid or oversized values use the shared Info fallback.
Read at most 16 bytes, without retries, chunk reassembly or general runtime selectors.
Snapshot the parsed override once per instance; reused callbacks do not repeat host reads.
Store changes affect new instances, not an installed filter.
Unexpected host failures are fixed typed configuration errors, never echoed values.
The same wasm can use Debug locally and its manifest level when no override is linked.

Managed initialization is immutable per instance and compares the complete endpoint,
level and stdout policy. Identical setup succeeds without reinstalling; changed settings
return `ManagedLoggerError::ConfigurationChanged`. A foreign logger remains untouched.
Fastly records include timestamp, level, target and message, retaining the target's
relative position shared with the Workers and Spin formats.

Install adapter-owned logging before application configuration. Diagnostics
generated while resolving logger configuration must be retained as bounded
typed/static state and emitted after installation, not as an unbounded log
buffer. When an application owns logging, the same boot records use its backend
and filters; visibility depends on the application's policy.

Cloudflare and Spin expose `init_logger(level) -> Result<(), ManagedLoggerError>` and install
Workers-console and stderr backends respectively. Their standard runners resolve the runtime
environment once and initialize before assembly unless logging is application-owned. A shared
`ManagedLoggerInstallation` retains one immutable configuration: identical repeated setup succeeds,
different configuration returns `ConfigurationChanged`, and a foreign logger yields
`AlreadyInstalled` without replacement. Cloudflare off-target initialization returns
`UnsupportedTarget`. There is no compatibility no-op or new capability flag.

The bundled CLI must not install its CLI logger before running the Axum demo.
Axum's `run_app_with_preflight` runs reusable startup checks after logging setup
and before application configuration or listener binding. The demo uses that
boundary for capability validation. Axum returns logger-setup errors rather than
silently using a different backend. CLI terminal errors use stderr regardless of
the runtime filter.
Generated adapter entrypoints continue delegating to reusable adapter code;
logging policy must not be duplicated in templates.

## Acceptance

- Resolver coverage includes every level, absent/invalid values, and no global
  logging side effects.
- Ordinary records retain their configured filter; boot `Error`/`Warn` survive
  ordinary `Error`/`Off`; boot `Info`/`Debug`/`Trace` remain suppressed.
- Fastly backend filtering and the facade ceiling cannot contradict the boot
  target policy.
- Configuration executes after adapter-owned logger setup; application-owned
  logger/filter state is not changed.
- Startup warning deferral is bounded and emits each diagnostic once.
- Direct Axum and bundled demo use the same runtime logging configuration.
- Documentation, generator output, and demo describe ownership accurately.

This is a logging API/documentation improvement, not an outbound correctness or
provider-certification prerequisite.
