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

## Fastly Invalid Configuration And Pre-Callback Failure

AG approved fixing issues #506 and #505 in PR #275 on 2026-11-15. Validate the
effective Fastly logging endpoint before finalization at every manifest-loading
boundary, including the `app!` parser. Preserve existing case-insensitive adapter
lookup and whole-section replacement: specified per-adapter logging shadows the
global Fastly section, while an empty section inherits it. Diagnostics identify
the selected declared field without echoing the endpoint. Do not apply Fastly's
name rules to another adapter's configuration.
An empty section is not `endpoint = ""`; an empty endpoint remains invalid under
generic validation even when its section is shadowed.

The pinned SDK rejects empty endpoint names, LF and colon. Its documented reserved
names `stdout` and `stderr` are not provisioned endpoints. Reject those exact forms;
do not add trimming, case folding, speculative length limits or host calls to core
or the macro. Endpoint existence and host failures remain runtime concerns.

For `run_custom` and `serve_custom`, a managed-logger setup failure before callback
entry consumes the unread native request through existing canonical app-free error
egress. It attempts a bounded redacted 500 response, including HEAD suppression,
then returns the original typed logger error even if delivery fails. The error
response uses the existing finite default write budget and clock. Inspect typed
delivery error, commitment and fallback outcome, including successful-but-degraded
fallback. Report degradation through one bounded, best-effort category-only stderr
diagnostic using a fallible, non-panicking write, not the failed logger.
Do not retry or send a native replacement. Application callbacks, stores, handlers,
preparation/finalization and app observers do not run on this branch.

This is a narrow exception to callback-owned sending: errors returned after callback
entry still propagate unchanged and never cause an automatic response, even when
they have the same logger-error type. Retained standard serving inherits the change
through its existing delegation. One-shot standard entrypoint ordering is unchanged.

New Viceroy controls prescribe a healthy identity-bearing request before the setup
failure under test. They verify real GET/HEAD status and framing, skipped callbacks,
the returned typed error and retained-instance termination. They do not weaken the
existing first-response attestation gate or close the seven older assembly cases.

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
- Invalid effective Fastly endpoint names fail manifest loading with a field-specific
  diagnostic and become compile errors in `app!`; valid generated/demo endpoints remain valid.
- Custom-runner logger setup failure attempts one redacted 5xx before returning the
  original logger error. Callback errors never trigger that path; failed delivery never
  triggers a second send or replaces the original error.

This is a logging API/documentation improvement, not an outbound correctness or
provider-certification prerequisite.
