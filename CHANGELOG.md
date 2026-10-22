# Changelog

## Unreleased

### Portable HTTP And Request Lifecycles (PR #275)

The changes below cover [PR #275](https://github.com/IABTechLab/edgezero/pull/275), including its
implementation and review follow-ups. This is a hard API migration: retired APIs and enum variants
are removed rather than retained as compatibility aliases. Provider limitations are published
separately from the portable API and must be considered when declaring required capabilities.

### Main Integration

- Integrate main's IABTechLab repository transfer; align outbound guide links,
  generated READMEs and capability diagnostics with the canonical repository
  and GitHub Pages site.
- Merge main's Rust 1.98.1 toolchain and dependency upgrades without restoring
  retired outbound or lifecycle APIs. Keep audited decoder and SDK requirements
  exact across the workspace, demo and generated applications. Adapt SHA-256
  digest formatting without reintroducing full canonical-output staging.
- Validate shared scaffold, demo and fixture dependency requirements with TOML
  parsing, including exact SDK pins; regenerate all three workspace lockfiles.
- Retain the fallible application assembly, pinned runtime targets and adapter-owned
  ingress/egress while integrating immutable release deployment from main. Deployment
  capability checks run before preflight or provider work; fallback dispatch retains
  the selected target instead of rediscovering source from the working directory.
  Remove the Fastly direct-deploy wrapper; callers supply `AdapterDeployContext`.
- Retire Fastly runtime selector-store setup. Applications open stable logical store
  aliases; deployment selects and reconciles physical resource links. Logging policy
  comes from baked `Hooks::logging_for("fastly")`, including custom entrypoints.
- Align bounded Axum secret reads with the prefixed uppercase environment mapping
  used by unbounded reads, preserving independent byte caps and deadlines.

### Bounded Local Config Snapshots

- Hard-cut `ConfigStore::{get, get_bounded}` and matching handle APIs from owned `String`
  to shared `ConfigValue` UTF-8 payloads across all adapters and fixtures. Borrow via
  `as_str()`/`as_ref()`; ownership copies are explicit. Debug output is redacted.
- Load Axum immutable local snapshots before listener binding, with validated finite
  `EDGEZERO__ADAPTER__CONFIG_STORE__*` file/entry/key/value/resident/startup budgets.
  Enforce actual reads and decoder/index/payload staging charges, reject duplicate keys,
  fail startup on allocation-limit violations, and retain typed failed declared bindings.
- Make Axum `from_path`, `from_local_file` and `from_map` require `ConfigStoreLimits`;
  `from_map` is now fallible. Publish `resident_allocation_bytes`; push updates require
  server restart. Shared reads preserve independent byte caps and the application clock.
- Pin the decoder allocation audit to `serde_json 1.0.150`; stream canonical hashing
  without full-output staging and release the extractor's raw handle before secret reads.
- Add native TCP proofs for suppressed-body completion ownership, stalled producer and
  non-reading client deadlines, disconnect cleanup, exactly-once release and capacity
  recovery. These are local evidence, not hosted provider qualification; broad config
  allocation remains Unsupported and response egress remains BestEffort.

### Bounded Provider Config Reads

- Replace Cloudflare bounded KV text materialization with capped BYOB byte-stream reads,
  reusable 16 KiB buffers and acquired-reader cancellation on overflow, timeout or drop.
  Require valid UTF-8 without lossy expansion; raw `get` remains explicitly unbounded.
  Cancel late-arriving streams after an abandoned acquisition, consume rejected cancellation
  promises, and yield cooperatively between bounded groups of clock-checked reads.
  Require runtime GC cleanup support before KV dispatch; demo/generated Workers enable
  `enable_weak_ref` explicitly. GC retention is still excluded from allocation certification.
- Replace Fastly SDK auto-growing config reads with one caller-sized host-ABI buffer.
  Reject over-cap chunk declarations before fetching or allocating reassembly; borrow
  discriminator tokens rather than building a JSON tree. Bounded storage resolution checks
  transport integrity; typed envelope verification remains behind the extractor's structural scan.
- Hard-cut `FastlyConfigStore::try_open` to return `ConfigStoreError` rather than the SDK's
  error type. Reject duplicate pointer discriminators and preserve future-format overwrite guards.
- Publish narrow guest allocation charges and the remaining Spin/hosted evidence gaps.
  Broad config allocation stays Unsupported and config deadlines stay BestEffort.
- Add opt-in, revision-pinned lifecycle qualification fixtures for all four standard entrypoints,
  a bounded HTTPS probe runner and a fail-closed correlated ownership-evidence validator.
  Compilation and local tests are not hosted qualification; no capability is promoted.

### Declared KV Failure Bindings

- Preserve all declared KV bindings on Fastly, Cloudflare and Spin without opening stores
  before admission or routing. First use returns payload-free `KvError::OpenFailed` with
  `KvStoreOpenFailure::{MissingBinding, InvalidConfiguration, Unavailable}` (500/500/503).
- Share settled opening results within one request, retry on subsequent requests, and
  deduplicate boot warnings by compiled logical ID without caching provider availability.
  Spin cancellation leaves unfinished initialization retryable; operations release the
  initialization lock before provider I/O. Validate Cloudflare namespace method shapes.
- Retain explicit fail-before-dispatch required-store behavior and Axum startup semantics.
  Add SDK/source-harness tests and real local runtime cases for unused declarations,
  sanitized errors, warning deduplication, completion ownership and linked-store CRUD.
  These do not qualify hosted outage recovery, KV allocation, or stronger capabilities.

### Outbound HTTP And Batches

- Add a separate bounded local overflow characterization suite through all four standard
  entrypoints. Preserve exact cap boundaries, typed 413/no-retry rendering and ordinary upstream
  413 metadata. Axum, Viceroy and Spin pass; retain Cloudflare's post-overflow proxy failure,
  reproduced independently by a JavaScript-only control. No hosted capability is promoted.
- Add the exact 2 KiB chunked-ingress proof on all four local runtimes, preserve the original
  overflow/recovery sequence, and exercise built-in JSON-extractor overflow with custom rendering
  over Axum TCP. Prove collector drop waits for worker cleanup. Narrow Cloudflare's retained
  recovery failure to the local front-proxy path without substituting a diagnostic entrypoint.
- Exercise freshly generated Axum applications and the bundled demo over real TCP, including
  fixed/chunked fallback cap boundaries, exact 413/no-retry responses, canonical 405 Allow,
  stalled-read 408 responses and deferred unread-body/pipeline handling. Reuse the existing
  generated-project CI gate and demo contract tests; require the fallback sentinel and execute
  ignored demo tests explicitly. No application policies are replaced.

- Return typed `EdgeError::PayloadTooLarge` (413) for inbound reader/extractor and outbound
  upload overflow, with non-wire `RequestBodyOverflowOrigin`. Reject oversized buffered uploads
  before every adapter dispatch; retain existing prebuffered or aborting stream behavior.
  Reject oversized normalized declared lengths without polling or poisoning reusable bodies;
  expired inbound deadlines still win with 408 and consumed overflow stays sticky.
  Suppress `Retry-After` on generated overflow after rendering and final egress preparation,
  while preserving upstream 413 and config-503 retry policies. Align demo/scaffold fallback status.
- Verify actual downstream encoded relay through all four local entrypoints: 24 cases per
  adapter across gzip/deflate/Brotli, buffered/streamed modes, Original/206 preservation and
  Decode controls. Capture binary wire payloads without decoding, retain representation metadata
  and origin receipts, and reject conflicting framing. Keep hosted qualification separate.
- Retain local Cloudflare workerd 2026-09-10 characterization: 24 coding, 46 marker, five
  scheduling and five HTTP/1 framing checks. Require the original Workers body signal in framing
  assertions and continue independent scheduling checks after policy-probe failures. Record exact
  tool, source, JS/WASM and compatibility-setting identities without claiming hosted qualification.
- Add the portable `OutboundExchangeOwner` / `OutboundPending` ownership foundation. Teardown
  is local and exactly once; readiness caches success or the original error without polling body
  bytes, and cancellation releases uncollected work. Successful collection disarms pending
  cancellation before handing off a live streamed response. Producers must supply already-scheduled
  completion receivers and separate body cleanup, not lazy transport futures or duplicate SDK
  release guards. This does not yet add `HttpClient::begin` or migrate the dispatch trait.
- Add client-wide proxy-marker defaults and per-request `OutboundProxyMarker::{Inherit, On, Off}`,
  with effective-policy inspection and resolution before supplied-client delegation. Default stays
  on; off skips synthetic header insertion/accounting while preserving ordinary upstream fields.
  Relay demo/templates configure their client handle off. Carry the policy through single sends,
  batches, bodyless responses and parts round-trips on all four adapters.
  Adapter implementers must pass the request policy to `insert_proxy_header`; its prior
  unconditional signature is removed rather than retained as a compatibility overload.
- Correct Spin `outbound-header-fidelity` to BestEffort: a local direct-WASI control proves the
  host hides `Connection` before normalization while retaining a nominated extension field.
  Preserve visible-header normalization and upstream marker opt-out; do not claim raw fidelity.
- Add `ResponseDecoding::{Decode, Original}` with per-request readback and parts round-tripping
  across all four adapters. Original delivers recognized encoded bytes without decoder allocation;
  every 206 response now retains original coding, even with Decode selected. Keep normalization
  precedence, identity decoded caps, encoded/final limits, bodyless handling and provenance.
  Relay demo/templates select Original; JSON fanout keeps Decode. Local Axum/Viceroy/Spin/workerd
  probes do not constitute hosted qualification; hosted relay evidence remains pending.
- Add the portable `OutboundFuture` / `OutboundTime` foundation for pairing the application
  clock with an adapter-owned wakeup factory. Preserve absolute deadlines across early wakes,
  yield cooperatively, propagate timer errors unchanged, reject observed clock regression and
  release timer/factory state on termination or cancellation. This core foundation does not yet
  migrate client constructors or dispatch traits, or strengthen provider capabilities.
- Replace the proxy-client/service API with `HttpClient`, `OutboundHttpClient`, `OutboundRequest`,
  and `OutboundResponse`. Standard adapters inject the client into request extensions; handlers
  access it through `RequestContext::http_client()`. Missing manual wiring remains explicit.
- Add buffered and streamed uploads/responses, absolute deadlines, request-body bounds, explicit
  cache policy, and HTTP authority override independent of URI routing and TLS identity where
  supported. Automatic redirects and implicit provider response decoding are disabled.
- Request construction is fallible and validates absolute HTTP(S) targets. Dispatch accepts GET,
  HEAD, POST, PUT, PATCH, DELETE, and OPTIONS; GET/HEAD reject nonempty bodies. Ordinary `Host`
  headers are normalized away; use the explicit authority override instead.
- Default to a 1 MiB buffered response, an 8 MiB upload cap, a 32 MiB decoder budget, a 24-bit Brotli
  window limit, and a 30-second exchange budget when no timeout/deadline is supplied. Response-mode
  setters are last-write-wins; `stream_response()` opts into streamed delivery. Encoded, decoded,
  header, and chunk limits are separate opt-in controls.
- Accept streamed upload sources under the request cap and deadline, but collect them before
  provider dispatch on Axum and Cloudflare. Cloudflare no longer forwards upload chunks directly
  to Fetch as the old proxy implementation did; this API does not promise wire-streamed uploads.
- Expose `OutboundCachePolicy::{PlatformDefault, Bypass}`. Bypass disables provider caching on
  Cloudflare/Fastly and is a no-op on Axum/Spin, which do not install an outbound response cache.
- Add completion-driven fanout through `start_batch_until`, indexed `next`, explicit `cancel`,
  ordered `collect`, and `send_all_until`. Batch slots require buffered requests and responses;
  streaming exchanges use `send`.
- Return per-slot elapsed time and outcomes, preserving input indices while yielding observed
  completion order. Timing uses one batch-entry snapshot and includes preflight, provider setup,
  complete body collection, and guest-observation delays rather than claiming pure transport RTT.
- Define `observation_cutoff` as the point where batch observation stops, even if individual slots
  retain request budget. Earlier cutoffs are valid partial-result operations, not request timeouts.
- Distinguish `Completed`, `Cutoff`, and typed batch failure. Only cutoff permits unresolved slots
  in successful collection; `OutboundBatchFailure` retains both the precise error and previously
  collected, index-aligned slots. Cancellation releases the driver and returns unresolved indices.
- Require clean driver EOF before `Completed`, including zero-slot drivers. Trailing failures,
  duplicate/out-of-range indices, premature EOF, and cutoff after all slots resolve are rejected;
  poisoned batches cannot resume driver polling. External adapters have a documented, public
  `OutboundBatchDriverEvent` / `OutboundBatch::from_driver` construction protocol.
- Axum, Cloudflare, and Spin drive complete eligible exchanges concurrently. Fastly dispatches
  eligible inputs before harvesting ready pending handles, preserves handle-to-slot identity, and
  uses deterministic dynamic-backend identities that include exact timer budgets. An early
  `BatchCutoff`-attributed provider phase timeout terminates only its slot unless the actual
  observation cutoff has expired.
- Share the future-based batch driver through `OutboundBatch::from_futures` on Axum, Cloudflare,
  and Spin. Preserve recorded terminal timestamps, ready-result retention, index validation,
  and clean-EOF completion; Fastly retains its provider-handle driver.
- Add `ContentTypeProvenance::{Ineligible(ContentTypeIneligibility), SingleTextUnchanged, Unknown}`
  from original response-header multiplicity and UTF-8 evidence before filtering. Reasons are
  `Missing`, `Modified`, `MultipleValues`, `NonText`, and `Removed`. Axum/Fastly/Spin retain
  evidence on every response path; Cloudflare always reports `Unknown` because Fetch strings
  lose original bytes and field boundaries. No MIME validation, trimming, comma splitting,
  raw-header/body copies, or new capability is added. A known textual singleton containing a
  comma can remain eligible; application MIME validation, acceptance, and redaction still apply.
- Expose `content_type_provenance()`. `headers_mut()` conservatively invalidates an originally
  eligible result as `Ineligible(Modified)` without rewriting private original evidence;
  original ineligible/unknown evidence never upgrades. `into_response()` carries the effective
  enum as a response extension describing the historical outbound boundary only, not certifying
  later ordinary `http::Response` edits.
- Hard-cut response construction to
  `OutboundResponse::new(method, status, headers, body, clock, content_type_provenance)`,
  decomposition to `(method, status, headers, body, provenance)`, and normalization to
  `Result<ResponseHeadNormalization, EdgeError>` containing body disposition and provenance.
  Metadata is mandatory, never inferred from normalized headers; no old signatures remain.

| Retired outbound API                                                           | Required replacement                                                                                                                    |
| ------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| four- or five-argument `OutboundResponse::new`                                 | six-argument `new(method, status, headers, body, clock, content_type_provenance)` requiring the application clock and original evidence |
| four-element `OutboundResponse::into_parts()`                                  | five-tuple `(method, status, headers, body, provenance)` with effective `ContentTypeProvenance`                                         |
| `normalize_response_headers(..) -> Result<ResponseBodyDisposition, EdgeError>` | `Result<ResponseHeadNormalization, EdgeError>`; use `.body_disposition` and retain original `.content_type_provenance`                  |

### Encoding, Limits, And Error Classification

- Enforce independent encoded-transport, decoded-output, and final buffered-response caps.
  Decoded caps apply to identity and EdgeZero-decoded bodies, not raw passthrough encodings.
  Checks occur incrementally before appending excess output; response collection retains the
  configured bounds and original absolute deadline.
- Share the response-body policy through `ResponseBodyPolicy` and `apply_response_body_policy`
  across all four adapters. Declared lengths, encoded limits, decoding/header rewriting, decoded
  limits, and rechunking retain their order; transport lifetimes remain adapter-owned.
- Add `into_bytes_bounded`, `into_bytes_bounded_until`, `json_bounded`, and `json_bounded_until`
  for explicit response consumption. The `_until` helpers check the retained application clock
  cooperatively; pending-stream cancellation still depends on the adapter's deadline wrapper.
- Add response-header byte/count caps with typed overflow origins. Accounting includes every
  adapter-visible upstream field before normalization and the synthetic `x-edgezero-proxy` field.
  Preserve repeated headers, including `Set-Cookie`, and strip hop-by-hop fields consistently.
- Decode single bare `gzip`, zlib-wrapped `deflate`, and `br` codings. Validate multi-member gzip
  through transport EOF, reject trailing data after deflate/Brotli completion, and remove compressed
  representation headers only when EdgeZero decodes. Request bodies retain their declared encoding
  without implicit recompression.
- Replace bare `ContentEncoding::Passthrough` with `Passthrough(PassthroughReason)`, distinguishing
  `Stacked`, `Unsupported(token)`, and `Malformed` while retaining encoded bodies and headers.
- Reject excessive Brotli window bits before decoder allocation. Publish pinned Brotli and flate
  decoder memory-charge contracts and enforce the codec-independent `max_decoder_bytes` budget.
  Add opt-in `max_chunk_bytes` rechunking as an emitted-item bound, not a receive-allocation bound.
- Add `Deadline`, `DispatchBudget`, the EdgeZero-owned `MonotonicInstant` alias, and injectable
  `MonotonicClock`. Ingress, outbound clients, deferred response drains, config extraction, and
  egress use one application clock domain. Equality expiry wins; far-future deadlines are clamped
  and remaining dispatch budgets cannot grow beyond their entry duration. Backwards batch terminal
  observations and Fastly dispatch samples fail closed.
- Add typed timeout provenance (`BudgetSource`, including `BatchCutoff`), gateway transport/decode
  reasons, response-limit reasons, and store-extraction reasons. Distinguish initial unreachable
  failures, midstream transport failures, protocol failures, and content-coding layers without
  requiring message parsing where the provider exposes those distinctions. Opaque Cloudflare Fetch
  failures retain `BadGatewayReason::Unspecified`. Preserve timeout causes through decoder error
  conversion; upstream/response-limit failures use 502 and exhausted exchange budgets use 504.
- Use category-only public diagnostics for provider-backed failures so URLs, credentials, and
  provider error details cannot escape in framework HTTP error bodies.
- Add fixed-size, text-free `ProviderFailureDetail` catalogs for the pinned adapters and
  `EdgeError::provider_failure_detail()`. Explicit gateway/request/timeout/internal constructors attach
  detail without changing Display, messages, statuses, typed reasons or wire bytes. Retain it
  through response-body policies, buffered collection, pending handles, batch failures and sticky
  request-body replay. Existing internal sources and `From<anyhow::Error>` remain intact.
  Populate existing send/body SDK boundaries on all four adapters, Fastly backend/upload I/O,
  and Spin request completion. Preserve observed detail when existing deadline checks select a
  timeout, including bounded response collection. Record Cloudflare constructor/header/setup
  failures and Spin's five header errors, typed timeout-option rejections and unit-returning
  target rejections as `RequestPreparation`. Keep SDK-erased causes Unknown, advisory timeout
  options successful, and shared response conversion unmodified. Retain the first observed
  body-source failure in `ResponseEgressReport::source_failure` on all four adapters. Completion
  and observer receive the same bounded category/provider snapshot and original body tag, even
  if a deadline, cancellation or fallback wins. Recording changes no polling, clocks, terminal
  precedence or wire bytes. Rendered application errors and transport-only failures are not
  source failures. Exact report literals must add the new optional field; no compatibility
  shape remains. Diagnostic capability publication and hosted qualification remain follow-up work.
- Retain precommit response framing rules in `ResponseEgressReport::framing_failures`, with
  independent first-observed application/fallback slots using the existing `ResponseEgressFramingError`.
  Use shared attempt-owned normalization on all four adapters, including fallback and Axum
  revalidation, and log only fixed rule text at error level. Add `EdgeError::response_framing_error()`
  for an already-owned internal cause. Preserve Display, statuses, fallback bytes, terminal
  precedence, source categories, clocks and ownership. Exact report literals must add the new
  carrier; no compatibility report shape is retained. Consumer and hosted qualification remain open.
- Correct Fastly lifecycle test-helper gating so plain `fastly` WASM unit builds retain the
  required-store failure regression without enabling `test-utils` or changing production behavior.
- Hard-cut exact field construction/matching of `EdgeError::{BadGateway, BadRequest, GatewayTimeout, Internal}`
  to include `provider_detail: Option<ProviderFailureDetail>`; prefer semantic constructors and
  `..` in patterns. No wrapper variant or compatibility error type is introduced.

### Ingress Admission And Routing

- Resolve routes before admission and body polling on all four standard adapters. Publish stable
  `RouteId`, `RouteMetadata`, and `RouteResolution::{Matched, MethodNotAllowed, NotFound}`, including
  optional manifest-sourced route `class` metadata.
- Stamp request start with the application clock, expose absolute read deadlines, and retain an
  opaque `IngressGrant` through admitted processing. Request contexts can consume the grant once.
- Replace eager unbounded inbound reads with bounded body consumption and deadline-aware wrappers.
  Cloudflare and Spin race pending asynchronous reads with timers; Axum owns its connection-local
  read future. Fastly retains its synchronous-hostcall limitation.
- Add cached, fallible `RequestContext::body_bytes(max)` and bounded JSON/form consumption.
  Failed or cancelled drains poison the body cell so later readers cannot retry a partially consumed
  source. Default extractors are bounded; `ValidatedJsonWithin<T, MAX>` and
  `ValidatedFormWithin<T, MAX>` let handlers select their own compile-time caps.
- Add `ReadBodyBeforeFallback` for bounded discard before canonical 404/405. It carries the
  application grant, byte cap, read deadline, and buffered `on_exceeded` / `on_timeout` responses.
  Overflow or timeout takes precedence without invoking a route handler or middleware; refusal
  remains the zero-read overload path.
- Add non-response `AdmissionDecision::Abort`. It starts no response lifecycle, invokes no handler,
  middleware, or completion callback, and polls no body. Axum closes the owned HTTP/1 connection;
  other adapters delegate the final transport effect to their providers.
- Add defensive normalized request-target/header/framing checks and typed accounting summaries.
  These do not claim access to raw parser-boundary memory or ambiguous raw framing.
- Emit a sorted, deduplicated `Allow` header for canonical 405 responses, including successful
  fallback drains. Add an application-controlled framework-error renderer while preserving status,
  `Allow`, and the effective config `Retry-After` policy after custom rendering. Applications remain
  responsible for bounding their renderer's output.

### Bounded Axum HTTP/1 Ingress

- Enforce finite pre-admission limits through the existing Axum server entrypoints: 256 live
  connections, 64 KiB combined raw request heads, 100 raw fields, and a non-resetting 10-second
  idle-plus-header deadline by default. Explicit invalid settings fail startup before application
  configuration or listener binding. `AxumIngressConfig` publishes validated effective limits and
  parser receive-allocation assumptions.
- Replace per-connection spawned tasks with one bounded connection-future owner. At capacity,
  close the single transient accepted socket without a parser, HTTP refusal response, or waiter
  queue. Keep-alive retains its slot; disconnect, failure, timeout, cancellation, and shutdown
  release connection state. Preserve existing application body-read and response-write deadlines.
- Pin Hyper 1.12.0, bytes 1.12.1, and http 1.4.2 for the audited HTTP/1 resource boundary. Raw head and duplicate
  field-count limits precede admission, but independent raw-target/header accounting and strict
  ambiguous-framing rejection remain unsupported. HTTP/2 and socket upgrade ownership are not
  enabled. Kernel buffers, allocator overhead, and application allocations are not RSS-certified.
- Bound and compact response headers before Hyper reuses their map for request parsing, preserving
  duplicates and sensitive flags. Response fields share the configured head-byte budget and a
  derived `max(100, request_field_limit)` count cap; overflow closes the connection. Custom Hyper
  reason phrases are removed. Oversized spare capacity/backing buffers cannot persist in the
  parser cache.
- **Breaking:** `AxumDevServerConfig` now requires its validated `ingress` field in complete struct
  literals. Use `AxumIngressConfig::default()`, `from_env`, or `new`; generated entrypoints still
  delegate to `run_app`. Axum CLI Cargo children receive selected manifest environment defaults,
  respecting parent environment precedence and declaration order. The demo manifest and scaffold
  document the ingress settings without duplicating runtime enforcement.
- Recognize case-insensitive `EDGEZERO__` prefixes on Windows, where child environment-key aliases
  retain their first spelling. Unix key identity stays unchanged, and explicit invalid limits
  cannot be bypassed through a Windows case alias.

### Response Egress And Application Assembly

- Carry every response-producing admission outcome through `ResponseEgressEnvelope` and one
  `ResponseEgressAttempt`, retaining the application clock, policy, and response-scoped completion
  resource until the exactly-once terminal transition.
- Add non-clone `ResponseEgressCompletion`, generic `late_bound` resource installation, and
  left-to-right `join` composition. Completion runs before the global observer; abandonment and
  competing terminal paths release resources without duplicate reports.
- Refusal/fallback responses retain their admission-decision-owned completion resources. Detached
  normalized errors use `DetachedResponseEgressDecision::{Send { completion, deadline }, Abort}`;
  the application factory can reserve resources or abort without creating a response. Raw parser
  failures remain outside the application lifecycle.
- Support per-response absolute and egress-start-relative deadlines through
  `ResponseEgressDeadline`. An absolute `at(deadline)` can include preceding application queue time;
  `after(Duration)` resolves in `ResponseEgressEnvelope::begin` and excludes time before egress
  starts. Preserve the selected deadline through policy, framing, conversion, and delivery.
- Replace response-object handoff and whole-stream buffering with adapter-owned response pumps:
  Axum's Hyper connection, Cloudflare's writer/abort lifetime, Fastly's manual `stream_to_client`,
  and Spin's owned WASI response/body/result writers. Account accepted payload prefixes and release
  sources on timeout, disconnect, source failure, and terminal transport errors.
- Fix Spin streamed egress when its host response-result future succeeds before body production
  finishes. Retire that observation once, continue the same pending source/write under its deadline,
  and finish only after stream closure and successful body-result publication. Keep late success,
  disconnects and source/write failures distinct from end-client delivery guarantees.
- Share response framing rules for HEAD, body-forbidden statuses, declared-length validation,
  hop-by-hop stripping, unsupported upgrades, and bounded pre-commit error fallbacks. Fastly manual
  framing is enabled only when a body is actually transmitted.
- Validate Axum exact-length stream EOF before handing Hyper the final frame; preceding frames
  remain incremental. Validate transmitted zero-length streams before handoff under the original
  deadline. Excess/late-error tails fail, pending/empty tails stay deadline-bounded, detached finite
  fallbacks retain GET payloads, and HEAD remains bodyless. Registration expiry selects 504 rather
  than a conversion 500.
- Make assembly core-owned and fallible through `App::build::<A>(platform)` and
  `Hooks::configure(&mut App) -> Result<(), EdgeError>`. Install target metadata before configuration,
  propagate hook errors unchanged, and reject successful configuration that changes any platform
  fact, provenance, or unknown reason. Add `app!(..., configure = ...)` with a fallible callback
  whose errors propagate through `App::build`. Axum completes assembly before listener binding.
- Add Fastly's closed `run_app_with_hooks` lifecycle for raw-request preparation and routed-response
  finalization while retaining adapter-owned delivery and returning routed hook state only after
  terminal delivery.
- Integrate opt-in reusable application lifecycles with fallible assembly and canonical admission/
  egress. Cloudflare and Spin expose prebuilt `dispatch_app`; Fastly exposes `serve_app`,
  `serve_app_with_hooks`, and successful-only `lifecycle::Sandbox` retention. Fastly custom serving
  callbacks own delivery and return `Result<(), E>` without an SDK-generated second response.
  Reuse no request handles, bodies, extensions, registries, or pending transport work.

### Bounded Config And Secret Extraction

- Add `ConfigExtractionLimits` and typed bounded config/secret reads with per-value, backend-byte,
  and cumulative extraction caps under one extraction-start-relative timeout and application clock.
  Fastly chunk reconstruction and secret hydration share the same cumulative accounting/deadline.
- Bound JSON depth and node count before materializing a config envelope as `serde_json::Value`,
  so nested or oversized structural shapes are rejected before allocating the retained tree.
- Distinguish missing binding/value/secret, malformed envelope, unsupported envelope version,
  integrity failure, deserialization, validation, overflow, deadline, and provider-read failures
  through `StoreExtractionReason` and typed store errors rather than the previous coarse extraction
  classifications.
- Require provider implementations of `ConfigStore::get_bounded` and
  `SecretStore::get_bytes_bounded`; there is no default unbounded-read-then-check implementation.
  Return `BoundedStoreRead` with explicit backend bytes and an optional value.
- Preserve declared Cloudflare/Fastly config bindings when opening fails by installing typed failing
  handles. Optional missing-config policy cannot reinterpret a broken declared binding as absence.
  Defer Spin config-store opening until bounded extraction, after admission, so unrelated routes
  can still dispatch and opening participates in extraction timing.
- Race asynchronous Cloudflare/Spin reads with platform timers and recheck injected-clock expiry
  around ready results. Synchronous Axum/Fastly provider calls remain non-preemptible; byte and
  timeout controls do not certify provider allocation or finite cancellation latency.

### Platform resource metadata

- Add `Adapter::platform_metadata()` and application-visible `PlatformMetadata` with typed
  `PlatformFact::{Known, Unknown}` values, provenance, and unknown reasons for memory ceilings,
  concurrent live inbound population, and host ingress/framing memory accounting.
- Distinguish per-instance from per-execution ceilings and separate stack allowances. Publish
  canonical Axum, Cloudflare, Fastly, generic Spin, and opt-in Akamai Functions profiles; generic
  Spin remains runtime-configured rather than inheriting a hosted quota.
- Add checked, scope-aware `validate_memory_envelope` with `Fits`, `Exceeds`, and typed
  `Indeterminate` results. Reject scope mismatch and arithmetic overflow; unknown platform facts
  remain indeterminate rather than being replaced with guessed population or parser-memory bounds.

Applications and adapter implementations must migrate to the current platform-fact and response
construction APIs. The migration record includes APIs from earlier PR revisions as well as the
pre-PR release surface, so consumers pinned to intermediate commits can update directly. No
compatibility aliases are retained.

The focused platform-design table below preserves that earlier migration record. Its outbound
constructor row is historical and superseded by the six-argument outbound migration above.

| Removed or changed API                                      | Replacement                                                                               |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `MemoryCeilingSource`                                       | `PlatformResourceSource` inside `PlatformFact::Known`                                     |
| `MemoryCeiling::new(total, scope, stack, source)`           | `MemoryCeiling::new(total, scope, stack)` wrapped in `PlatformFact::known(value, source)` |
| `MemoryCeiling::source()`                                   | Match or inspect the enclosing `PlatformFact`                                             |
| `PlatformMetadata::new(Option<MemoryCeiling>)`              | `PlatformMetadata::new(memory, inbound_population, host_ingress_accounting)`              |
| `PlatformMetadata::memory_ceiling() -> Option<_>`           | `PlatformMetadata::memory_ceiling() -> PlatformFact<_>`                                   |
| `Adapter::memory_ceiling()`                                 | `Adapter::platform_metadata()`                                                            |
| overridable `Hooks::build_app()` and infallible `configure` | `App::build::<A>(platform)` plus fallible `Hooks::configure`                              |
| four-argument `OutboundResponse::new`                       | five-argument constructor requiring the application `MonotonicClock`                      |

### Logging Ownership And Boot Diagnostics

`owns_logging = true` now explicitly owns backend installation, backend filters, and the facade
maximum before entering an adapter runner. `resolve_logging_level(&EnvConfig)` shares runtime
level resolution. Managed Axum/Fastly loggers retain `edgezero::boot` warnings and errors at
`Warn` even when ordinary logging is `Error` or `Off`. Cloudflare/Spin initializers remain no-ops.

Fastly `runtime_env_config` now returns `FastlyRuntimeConfig` with an `env` field and bounded
deferred diagnostics. Custom callers must initialize logging and call `emit_boot_diagnostics()`;
the former direct `EnvConfig` return is removed, with no compatibility alias. Managed runners
initialize logging before application configuration. Axum now returns logger-setup errors instead
of ignoring them; preinstalled backends must use `owns_logging = true`.

Axum's `run_app_with_preflight` supports checks after logger setup and before configuration or
listener binding. The bundled demo uses it without first installing the CLI logger. Terminal CLI
errors remain visible on stderr regardless of runtime logging level.

### CLI, Capabilities, Demo, And Generator

- Add typed manifest capabilities and `Adapter::capability` with `Native`, `BoundedCooperative`,
  `BestEffort`, and `Unsupported`. Runtime-producing CLI operations validate required support
  before side effects; optional support emits boot diagnostics without blocking execution.
  Unknown names, duplicates, overlap, and malformed baked contracts fail closed.
- Add canonical outbound-host declarations, with an omitted declaration defaulting to HTTPS only
  and explicit `"*"` enabling HTTP and HTTPS. These configure platform plumbing, not destination
  authorization. Spin's selected component must match the canonical host contract.
- Pin CLI execution to the resolved application root, platform manifest, and component through
  `AdapterExecutionTarget` / `execute_target`, including manifest-defined shell commands. Reject
  ambiguous or escaping manifest selection instead of checking one project and executing another.
  Fastly deployment honors the selected manifest path.
- Migrate checked-in demos and generated applications to the new client, buffered fanout,
  passthrough classification, response limits, clocks, ingress policy, and fallible assembly.
  Include explicit missing-client 501 behavior and partial-result/failure examples.
- Keep runtime setup in reusable adapter runners rather than copying transport or logging code
  into templates. The demo illustrates completion composition and late-bound ownership; generated
  scaffolds retain finite admission/egress policies with core-owned empty lifecycle tokens.
- Gate `handlebars` behind the existing `edgezero-cli/cli` feature so runtime-only library consumers
  do not pull scaffolding or implicitly enable its `serde_json/preserve_order` dependency feature.

### Additional Hard Migrations

| Removed or changed API                                                                                | Replacement                                                                                                             |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `ProxyClient`, `ProxyHandle`, `ProxyRequest`, `ProxyResponse`, `ProxyService`, adapter `*ProxyClient` | `OutboundHttpClient`, `HttpClient`, `OutboundRequest`, `OutboundResponse`, client forwarding, adapter `*OutboundClient` |
| Infallible proxy request constructors and tuple `into_parts()`                                        | Fallible `OutboundRequest` constructors / `from_parts` and structured `OutboundRequestParts`                            |
| Proxy request extension access and `body_mut()`                                                       | Application-owned state and outbound request builders / structured parts                                                |
| `Body::from_stream` with arbitrary external errors                                                    | Typed `EdgeError` items, or `Body::from_external_stream` for external errors                                            |
| Synchronous/unbounded request-context body access                                                     | Async `body_bytes(max)`, `json_within(max)`, `form_within(max)`; fallible `take_body()` / `into_request()`              |
| `EdgeError::MethodNotAllowed.allowed: String`                                                         | Structured `Vec<Method>` used to render the mandatory `Allow` header                                                    |
| `config_out_of_date_from_serde`                                                                       | `store_deserialization_from_serde` and `EdgeError::StoreExtraction { reason, .. }`                                      |
| Store implementations with only unbounded reads                                                       | Required `ConfigStore::get_bounded` / `SecretStore::get_bytes_bounded` implementations                                  |
| Decoder helpers without a memory policy                                                               | Codec helpers accepting decoder budgets and Brotli window policy                                                        |
| Response-returning Fastly entrypoints and `#[fastly::main]`                                           | An undecorated `main` calling a send-owning adapter runner                                                              |
| Transport-blind response converters and stream-buffer fallback constants                              | Adapter-owned response-egress coordinators                                                                              |
| Fastly `runtime_env_config(...) -> EnvConfig`                                                         | `FastlyRuntimeConfig`, using `.env` and `emit_boot_diagnostics()` after logger setup                                    |

Earlier PR revisions additionally require these migrations:

| Superseded intermediate API                                      | Current API                                                                       |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `send_all` returning bare positional outcomes                    | `start_batch_until` / `next` / `cancel` / `collect`, or fallible `send_all_until` |
| Batch `cutoff` parameter described as a batch deadline           | Explicit `observation_cutoff` that stops observation regardless of slot budgets   |
| Bare `ContentEncoding::Passthrough`                              | `ContentEncoding::Passthrough(PassthroughReason)`                                 |
| Codec-specific `max_brotli_decoder_bytes`                        | Codec-independent `max_decoder_bytes`                                             |
| Admission refusal/fallback without response completion ownership | Decision-owned `ResponseEgressCompletion` transferred through ingress and egress  |
| Detached completion-only factory                                 | `DetachedResponseEgressDecision::Send { completion, deadline }` or `Abort`        |

### Dependencies And Toolchain

- Hard-cut platform SDKs to Fastly `0.13.1` (SDK/shared/sys/logger), Cloudflare `worker 0.8.5`, and
  `spin-sdk 7.0.0`, adapting Spin to its WASI HTTP 0.3 response/body/result resource protocol.
- Pin native outbound dependencies (`reqwest 0.13.4`, `url 2.5.8`) and audited decoder graphs,
  including `async-compression 0.4.43`, `compression-codecs 0.4.38`, `brotli-decompressor 5.0.1`,
  and `flate2 1.1.9`. Decoder charge changes require explicit pin/audit updates, not lockfile-only
  assumptions. Spin's SQLite config writer is reverified against Spin 4.1.0.
- Update runtime/tool pins to Spin CLI `4.1.0`, Viceroy `0.21.0`, and Wasmtime `48.0.2`; generated
  projects select corresponding SDK generations and applicable platform CLI/Viceroy pins. Root
  workspace dependencies use exact SDK pins. Rust `1.95.0`, Node `24.12.0`, and Fastly CLI `15.1.0`
  remain the workspace toolchain.

### Tests, Documentation, And Maintenance

- Expand native and provider contract coverage for deadlines, decoding, limits, batch ordering,
  driver failure/cancellation, admission/fallback precedence, clocks, framing, and resource release.
  Add Hyper raw-socket, Spin SDK-resource, Fastly closed-lifecycle, and demo logging regressions.
- Gate native/WASM feature combinations independently, include `test-utils` runtime paths, and
  require named nonzero test sentinels. Consolidate adapter clippy into one workflow matrix while
  retaining distinct per-target check names, including native Axum.
- Verify generated all-adapter projects and the excluded demo workspace. Fetch both locked
  dependency graphs before offline audits; enforce decoder dependency, JSON-map, documentation,
  public-rustdoc, and retired-API contracts in CI.
- Update outbound, ingress, egress, and configuration specs/plans, capability matrices, migration
  guides, and examples. Replace ordinary hand-built JSON values/fixtures with `serde_json::json!`,
  retaining raw input only for syntax, exact-byte, static-input, and serialization-free fallback
  requirements.

### Capability And Evidence Boundaries

- Axum alone reports native outbound and inbound-read deadline enforcement. Cloudflare outbound
  and streamed-upload deadlines remain `BestEffort` pending deployed host-observed cancellation
  evidence; Fastly and Spin retain their documented phase-timer/teardown limitations.
- Fastly fanout is dispatch-before-harvest, not guaranteed simultaneous overlap. Synchronous
  registration/body drain and bounded handle-selection groups limit completion order, cancellation,
  and slot isolation; dynamic backends require provider enablement. A 25 ms dispatch-slack guard can
  reject a still-live slot before synchronous cold registration, including heterogeneous hosts and
  budgets. Cloudflare header fidelity and authority override remain `BestEffort`; Spin authority
  override is rejected because routing and authority cannot be separated through its provider API.
- Response-egress abort, backpressure, completion, and write deadlines remain `BestEffort` on all
  adapters. An exactly-once guest lifecycle report does not prove end-client receipt or a finite
  provider teardown interval. Configuration-read deadlines also remain `BestEffort` everywhere.
- Raw ingress head limits/framing guarantees, complete outbound resource accounting, and complete
  config-read allocation bounds remain `Unsupported`. Provider parsing, materialization, SDK copies,
  hidden informational fields/trailers, allocator overhead, and host buffering are not certified by
  guest-visible byte, header, chunk, or decoder-policy caps.

See [the capability guide](docs/guide/capabilities.md) for the exact per-target matrix, accounting
boundaries, canonical platform facts, and evidence required before promoting support.
