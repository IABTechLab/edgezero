# Changelog

## Unreleased

### Portable HTTP And Request Lifecycles (PR #275)

The changes below cover [PR #275](https://github.com/IABTechLab/edgezero/pull/275), including its
implementation and review follow-ups. This is a hard API migration: retired APIs and enum variants
are removed rather than retained as compatibility aliases. Provider limitations are published
separately from the portable API and must be considered when declaring required capabilities.
The current outbound migration is summarized first. Earlier checkpoint entries below describe
their scope when introduced; the current summary supersedes their then-pending API work.

### Head-First Outbound Hard Cut

- Replace implementer `send` and raw-request batch hooks with paired `time`, explicit `progress`, synchronous
  support validation, prepared `dispatch_head`, eager `begin` and prepared batching. Core
  captures invocation time, performs validation/settlement once and finalizes provider heads.
  Remove duplicate adapter preparation, header normalization, decoding and response collection.
- Return owned local futures from facade `send`, `begin` and `send_all_until`; no caller stack
  frame is borrowed. Preserve original budgets, typed failures, independent caps, coding and
  provenance, direct native Image Optimizer dispatch and provider capability limitations.
- Settle unknown GET/HEAD bodies to EOF or the first nonempty byte before provider dispatch,
  racing paired timers with outbound and admitted inbound deadlines. Preserve certified empty
  bodies without reading, source errors and the inbound 408/outbound 504 distinction.
- Replace public batch receipt/collection fields and Option slots with identity-bound receipts,
  accessors, consuming decomposition and explicit AlreadyDelivered/Resolved/Unresolved states.
  Add nonblocking observation and lossless accumulator recovery. Producers emit raw indexed
  terminal observations from eagerly scheduled work; remove `from_futures`, `finish_batch_item`
  and public receipt construction rather than retaining compatibility helpers.
- Preserve malformed/trailing driver validation, partial results, logical own-deadline timing,
  strict cutoff equality and exactly-once local teardown. Migrate supplied clients, fixtures,
  demo, generator and documentation together. Explicit redirect policy uses the same pipeline;
  hosted qualification remains separate and this migration does not promote provider guarantees.
- Use one core scope implementation for callback/body ownership and revocable operation roots.
  Capture retained Send futures, begun exchanges and batches at invocation, before first polling.
  Retained clients resolve fresh request ownership, and later polling cannot reparent the work.
  Require the typed provider-progress hook; Axum, Cloudflare and Spin share the same independent
  implementation, while Fastly reuses its cooperative native multiplexer. Remove its private
  lifetime registry rather than maintaining an alternate ownership path.
- Retain response-owned work through terminal egress, not source EOF; revoke retained source
  handles before releasing completion resources. Transfer successful streamed outbound responses
  independently of their original callback. Keep recorded batch partials and typed scope failures.
- Start early response-owned work with `OutboundScopeOwner::response_body()` and attach the exact
  owner to its response. An unaccepted owner remains callback-bound; canonical egress accepts it
  before policy, framing or suppression. Share provider progress across the handoff without
  sharing callback cancellation. Failed application bodies close before bounded fallback delivery.
  Run source disposal under its captured identity, preserve terminal notifications after supported
  cleanup unwinding, and keep lifecycle rejection distinct from HTTP framing diagnostics.
- Replace public `Body::Once`/`Stream` construction and patterns with a private body carrier,
  `Body::from_bytes`/`from_stream` and `BodyPayload` at transport boundaries. Carry local body
  ownership without HTTP extensions, streamifying buffered bytes or extra payload copies.
- Share the revocable local task runner across Axum, Cloudflare and Spin. Fail scope registration
  before spawning and release work on executor drop even while its cancellation owner is retained.
  Use one source-binding implementation for outbound and downstream bodies, one core earlier-own-
  deadline cutoff predicate, and observation-only nonblocking readiness. Remove copied task states
  and the second Workers response-deadline wrapper without removing host yielding or abort ownership.
- Remove Fastly's elapsed-preparation slack rejection, its public constant and test-injection hook.
  Dispatch and cold registration still reject expired budgets and backwards clocks. Preserve B31
  backend identity/timers and original absolute deadlines instead of restarting time after settlement.
- Preserve earlier own-deadline timeout results when Fastly or Spin preparation crosses the
  observation cutoff, without starting expired provider work. Make supplied fixtures return
  cutoff rather than premature EOF for exact-cutoff requests. Race Fastly's pending response
  source against its write deadline without cancelling callback-owned siblings.

### Explicit Redirect Following

- Add `HttpClient::follow` with a required synchronous target policy and validated 1..=20 limit.
  Plain send, begin and batches remain unfollowed. Every hop uses the same supplied client and
  shared preparation/head finalization; no alternate transport or compatibility wrapper is added.
- Apply the specified status/method/body decision order, relative Location resolution and original
  duplicate/nontext evidence. Replay buffered `Bytes` without copying; never buffer a streamed
  upload for replay. Discard followed heads unread, including pending, erroneous, compressed and
  oversized bodies. Stopped/final responses retain ordinary response policies and typed failures.
- Strip only Authorization, Cookie and authority overrides from automatically prepared cross-origin
  requests. Preserve deliberate policy replacements, unchanged caps/options and arbitrary local
  policy errors with their redirect number. Applications retain initial and per-hop target policy.
- Keep the initial absolute deadline through policy replacements and final streamed consumption,
  while renewing per-hop timeouts and retaining a fresh request's normal default budget.
  Capture invocation ownership and cancel through the same scoped operation machinery as send.
- Publish `outbound-redirect-following` for the portable policy, separate from provider deadline,
  cancellation, allocation and hosted qualification guarantees. Preserve the native optimizer call.
  Add real-TCP Axum parity and two-origin tests, deterministic supplied-client tests, documentation
  guards, a mandatory core sentinel and execution of the existing Spin eager SDK component in CI.
- Stop reentrant scope cancellation before another dispatch, upload chunk or response-source
  read, including cancellation from terminal source disposal instead of exposing clean EOF.
  Reuse the shared source binder and retain clock history through stopped responses and
  transport cleanup. Borrow the existing collection engine for buffered finalization instead of
  allocating another payload box; keep public collector signatures and typed error precedence.
- Exercise the actual provider clients at their existing dispatch boundaries: Cloudflare guest
  fetch fixtures, local Spin HTTP wire controls and repaired-Viceroy Fastly progress probes.
  Compare plain Send with Follow for coding, caps and streamed final responses; verify unread
  intermediate heads, stopped-body failures, hop metadata and native optimizer unavailability.
  Run the Spin wire harness in its existing CI job and require the separate Fastly redirect
  inventory without weakening the original progress inventory. These tests do not qualify
  hosted cancellation, successful optimizer transformation or resource guarantees.
- Run the Spin host wire fixture in the existing Rust harness and remove the Python runner.
  Preserve all 68 redirect exchanges, 84 correlated origin requests and existing raw-framing
  and lifecycle controls. Reuse bounded HTTP capture, file-backed runtime logs and process-group
  teardown; reject late worker failures and unexpected runtime exits before publishing success.
  Keep one runner and no compatibility wrapper or additional host-language dependency.
- Reuse the Rust wire matrix for local Cloudflare workerd/origin redirect checks through the
  standard request-context client. Preserve the Spin inventory and require exact provider-specific
  source errors. Cover cleanup before runtime startup and distinguish confirmed requested SIGTERM
  wrapper exits from premature runtime exits. These local checks do not qualify hosted behavior.
- Add fixed Begin/Batch lifetime probes to the same Rust harness and production-client guests.
  Preserve actual host writes, guest observations, original cutoffs, typed failures and mixed
  collection states. Reject late or incomplete evidence; keep timeline misses and provider
  progress failures visible rather than widening tolerances or promoting qualification.
- Extend those probes with pending-head/body cutoff waits, explicit post-response origin
  release and malformed-evidence rejection. Capture operation return before report formatting
  and require origin phases before the actual guest-response return, not just later release.
  Keep the 200 ms cutoff and 205 ms return target, and distinguish a host-written head from
  proven SDK body-read entry. Production behavior and qualification levels are unchanged.
- Add a bounded local CPU-stretch probe with causal clock intervals, actual write-overlap
  validation and strict receipt cutoff checks. Reject clock stalls, ambiguous timing evidence,
  collection-stamped arrivals and early C bodies hidden behind late connection closure.
  Keep unresolved outcomes distinct from proof of the required source timing; no public API,
  transport or capability changes are introduced.
- Calibrate that probe with one bounded, same-client request while A can make progress.
  Keep the original observation/CPU targets independent of calibration, validate native
  payload-write causality, and reject uncertain timing without relaxing acceptance bounds.
- Replace test-origin accept sleeps with harness-only readiness and explicit shutdown wakes.
  Preserve connection/worker limits and I/O budgets; prove stop/admission races and bounded
  fallback after failed wakes without adding production dependencies or relaxing timing gates.
- Expose underlying version-probe failures in request-fidelity test assertions instead of
  hiding a subprocess error behind an expected-version assertion.
- Add the fourth-slot per-call timeout lifetime case through the existing production clients.
  Preserve the original slow-origin case, 300 ms timeout provenance, completed siblings and
  exact cutoff collection states. Record timeout-contract diagnostics separately without
  waiving source timing failures or claiming hosted qualification.
- Add the compressed-response cap lifetime case to the existing Rust harness and production-client
  fixtures. Preserve independent decoded/encoded/buffered limits and healthy siblings, require
  the original typed slot failure rather than a report-reader error, and prove early return in
  the source's host clock. Bound native encoding and partial-write evidence without adding guest
  dependencies or relaxing existing timing and qualification gates.
- Bind the core mixed-observation acceptance leg to real receipt/accumulator tests. Strengthen
  the three-slot Completed/Cutoff/typed-failure matrix, preserving bodies and original errors;
  discover all redirect and receipt sentinels before one existing grouped CI run. Adapter and
  hosted legs remain open, and command readiness does not imply qualification.
- Add response-handoff lifetime evidence on local Axum, workerd and Spin. Keep one pending begun
  origin and original batch under the exact attached response owner, and release source heads
  only after the client validates a bounded canonical prelude and the dispatch/scope witnesses.
  Collect the original jobs in the response body; test preexisting resource retention, no replay,
  strict arrival cutoffs, valid late collection and exactly-once terminal family release.
  Harden the shared fixture reader against ambiguous framing and premature prefix notification;
  preserve primary/partial client failures through cleanup. Fastly keeps all nine existing cases
  and records handoff as inapplicable. Local handoff passes do not waive full-suite timing failures,
  activate inventory legs or qualify hosted behavior. No production API or runtime-pin changes.
- Add a local Fastly normal-completion isolation probe: retain response-owned Begin/Batch handles
  through canonical egress, then successfully collect an already-pending callback sibling and
  send fresh work through the same real client. Bound and validate causal source/log witnesses;
  distinguish fixture-driven socket closure from provider cancellation. Extend the actual-route
  core unit with terminal-injection/drop rows that release pending owners before retained
  wrappers drop and prevent further producer polls. Fix a test-only accept-readiness assumption
  with bounded retries, preserving readiness, drain and peer assertions. Adapter failure paths,
  SDK teardown, strict timing and hosted qualification remain separate; no production API changes.
- Pin the coordinated Spin SDK executor upgrade to immutable Stackpop fork revision
  `6c59ed5b4fcb377072763aee0b27a3abdd8edb72`; propose it upstream in
  [spin-rust-sdk#169](https://github.com/spinframework/spin-rust-sdk/pull/169).
  Hard-migrate SDK spawn calls to `spawn_local`, using WASIp3 0.10.0 / wit-bindgen 0.63.0
  through one SDK executor. Align the root, demo, reusable/request-fidelity fixtures and generated manifests;
  compare Git source and revision as well as package version in dependency guards.
  Preserve absolute deadlines, cutoffs and all limits; scheduler-latency and hosted qualification
  remain separate gates. See the [dependency audit](docs/audits/2026-10-27-spin-sdk-eager-progress.md).
- Adapt to the reviewed SDK's join-handle return at one private provider launcher, keeping
  eager task cancellation with EdgeZero's existing owner. Run the native-request conversion
  contract inside the SDK executor without weakening admission/refusal assertions. Preserve
  the full Spin wire matrix and keep strict lifetime timing failures explicitly unqualified.

### Main Integration

- Integrate main's pre-dispatch hooks and immutable request-ingress fidelity metadata without
  bypassing route-aware admission, lazy inbound reads or owned response egress. Canonical
  adapters admit first, then run the hook exactly once before dispatch or fallback reads;
  refusals and aborts do not invoke the hook. Direct router calls run it before route lookup.
  Preserve method/path continuation checks and conservative provider-fidelity reporting.
- Route Cloudflare runtime-head conversion failures through detached Send/Abort policy and
  exactly-once egress completion. `CloudflareRequestContext::insert` now accepts `Arc<Context>`
  so request and error delivery share the same runtime ownership; no legacy overload remains.
- Migrate main's request-fidelity fixture to fallible application assembly, adapter-owned
  streaming delivery and the same signed Spin SDK source/revision. Extend the structured
  dependency guard and retain both Node contracts and separate browser-only ingress tests.
  Historical runtime observations do not qualify this merged candidate or promote capabilities.
- Move the shared native HTTP test runner to the unpublished, standalone
  `crates/edgezero-test-harness` workspace. Keep fixture applications separate, preserve guest
  build/output paths, record both lockfiles and remove the old runner package without aliases.
  Update active CI and commands; preserve all four redirect inventories and bind exact core/runtime
  acceptance tests without promoting hosted qualification or optional idle-body policy.

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

### Guest Validation

- Honor cooperative source wakes in Fastly's existing progress loop instead of entering a
  blocking host selection after a ready-to-repoll body yield. Preserve expiry publication,
  nested progress, retained wakers and normal waits on later pending polls. Keep the caller's
  waker out of persistent progress state, without changing deadlines, read sizes or runtime pins.
- Compile Spin's test-only helpers only with the feature that calls them. Verify both
  `spin` and `spin,test-utils` WASM all-targets builds, without suppressing unused-code
  diagnostics or changing runtime behavior.
- Keep Fastly's declaration ordering valid in CLI-only tests, where the runtime-feature
  ordering exception does not apply. Extend the existing conflicting module-file lint
  exception to native test builds without changing scheduling or transport behavior.
- Run Cloudflare guest contracts on pinned Node, matching the library guest suite. Assert
  native request-body presence before conversion, reuse scoped numeric-timer guards, and
  model only the fixed-length byte-stream surface missing from that host in a test fixture.
- Test exact bytes, length violations, backpressure, cancellation, abort and large declared
  lengths. Preserve exact canonical response-header assertions and exercise the default
  configuration-error renderer rather than a bodyless workaround. Guest tests are not
  Workers-host qualification; production transport and capability declarations are unchanged.

### Forwarding Documentation Gate

- Compile the forwarding example directly from the guide using offline/locked Cargo artifacts
  and rustdoc. Require one non-ignored passing test without invoking the network handler.
- Add real compiler regressions and workspace CI activation with pinned Node. This compile-only
  prerequisite does not replace runtime forwarding tests or provider qualification.

### Fastly Cooperative Receive Progress

- Replace blocking pending-request waits and selected-body drains with one private progress
  engine shared by the existing Send and Batch receive paths. Select borrowed handle identities,
  validate every result, rotate slot service, and perform at most one 16 KiB raw read per fresh
  matching readiness observation. Remove the superseded ownership-reassociation implementation.
- Preserve cached SDK Send behavior with nonblocking opaque-head polling and one shared positive
  1 ms pulse. Keep B31, original deadlines/cutoffs, typed source errors and response policies;
  leave direct native Image Optimizer dispatch unchanged. Ready results backpressure only their
  own slot, with exactly-once local lease cleanup on cancellation/terminal paths.
- Release batch drivers on terminal or poisoned observations even when callers retain the batch.
  Preserve provider details through the outer Fastly deadline guard and release stream sources
  before publishing terminal errors. Neither local cleanup nor a dropped pending request proves
  provider-side cancellation.
- Validate local/CI Fastly receive paths using the explicitly selected, hash-verified Viceroy
  source repair without replacing the installed runtime. Do not upgrade capabilities or claim
  hosted qualification. Callback-wide ownership, eager public modes, full pipeline migration
  and dispatch-slack retirement remain open.
- Gate the actual-client functional inventory through the existing harness's explicit
  `outbound-client-progress` suite. Preserve the broader `outbound-readiness` SDK diagnostic,
  strict timing assertions and failed observations separately. Functional CI requires complete
  client evidence and exactly the unqualified exit-2 disposition, not a qualification pass.

### Fastly Batch Lifetimes

- Retain the fixture callback's effective injected HTTP client for post-delivery work, including
  routed handler errors. Remove its manual Fastly client construction and prepare-time override;
  preserve state precedence, request isolation, streaming and fallback/no-attempt gates.

- Give each custom callback a fresh private batch owner; standalone send helpers establish a
  fallback owner, while nested helpers reuse the callback. Resolve ownership per operation so
  retained clients work in the next callback without retaining the previous request's scope.
- Release complete drivers, processing state and queued outcomes at scope exit even when batch
  handles are retained. Wake pending consumers and report typed internal scope closure once;
  do not misclassify it as a cutoff or discard results collected before failure.
- Revoke all roots before reentrant cleanup, keep the closing owner active until teardown ends,
  and reject new sends/batches before preparation or provider work. Clone/drop wakers outside
  driver borrows and observe closure before returning Pending.
- Independently own batches created during response-body production through transmission cleanup.
  Normal completion and source/write failure release them without cancelling callback work or
  post-delivery operations. Keep driver EOF validation and exactly-once guest cleanup intact.
- Preserve current transport, clocks, limits, B31, direct native image optimization and public
  APIs. Full Send-root cancellation, eager/head-first migration, callback-wide progress and
  provider cancellation/hosted qualification remain open; capabilities are unchanged.

### Fastly Mixed-Phase Readiness Diagnostic

- Extend the existing local SDK/ABI suite with pending-head/ready-body and pending-body/delayed-head
  cases, plus a multi-buffer truncated response. Require full 16 KiB reads, exact payload evidence,
  fresh per-read readiness and preserved terminal errors for buffered and streamed consumers.
- Append separately validated observations without changing the original 135-probe inventory or
  replaying earlier failures under a weaker rule. Retain stock and explicitly repaired runtime
  outcomes, including timing failures. This does not migrate production dispatch, approve a read
  maximum, change installed runtime pins, upgrade capabilities or qualify hosted behavior.
- Remove the Axum partial-result contract test's wall-clock race: gate the slow origin response
  and advance injected time only after collection stores the successful fast slot. Keep the
  25 ms logical cutoff, exact result assertions and an independent finite containment watchdog.

### Shared Outbound Header Policy

- Route all four adapter response paths and `OutboundHead::new` through
  `OutboundHeaderInput::normalize`. Charge original fields before filtering, retain Content-Type
  and Location evidence, then charge the optional synthetic marker once. Preserve unknown
  provider fidelity, bodyless/205 disposition and marker-off upstream fields. Reuse static
  adapter-marker bytes without another allocation or retained raw-header copy.
- Remove the separate public `normalize_response_headers` and `insert_proxy_header` helpers
  and their root exports. Adapter implementations use the shared operation; defensive final
  response normalization remains private and does not repeat accounting or marker insertion.
- Document hard-cut adoption without a second application transport, while retaining current
  positional/partial batch results, cancellation, cutoff, timing, typed errors, limits, encoding,
  provenance and ingress/egress ownership. Demo and template request APIs are unchanged here.
  This checkpoint does not complete eager/head-first/redirect dispatch or change SDK ownership,
  provider deadlines, backend policy, capability levels or hosted qualification.
- Correct existing Spin fallback contract expectations to require the exact `Content-Length`
  added by explicit buffered framing. Preserve status/body checks and all resource-release assertions.

### Spin Supported-Option Precedence

- Reject unsupported Host authority overrides before portable method/body/policy validation on
  both existing single-send and batch paths. Keep Image Optimizer rejection first and preserve
  the authority diagnostic. Pending GET/HEAD bodies, nonportable methods, invalid Brotli settings
  and zero timeouts cannot mask the unsupported setting or cause body polling.
- Add native and actual WASI SDK preflight regressions. This changes no public signature,
  transport, timer, capability, runtime pin or batch-cutoff semantics; full shared preparation
  and the head-first/eager migration remain pending.

### Shared Dispatch Validation

- Hard-cut the public adapter helper to `validate_for_dispatch(&request, OutboundSendMode)`.
  Migrate every in-tree caller without a one-argument wrapper. Core owns buffered Batch shape
  checks instead of four adapter copies; supported-option checks remain adapter-owned and first.
- Preserve method/upload-cap precedence, existing batch diagnostics, zero source polling and
  observation-cutoff behavior. Begin requires buffered uploads but permits streamed responses;
  Image Optimizer remains unavailable in Begin/Batch. This does not implement eager dispatch
  or redirect following, change handler/demo/template APIs, or upgrade provider capabilities.

### Spin Connect-Setting Fallback

- Treat every host connect-timeout setter refusal as advisory under the existing aggregate
  deadline race. Do not turn `Immutable` or `Other` into a request-fatal 500 or expose provider
  text. Keep first-byte/between-bytes typed setup failures unchanged.
- Add SDK outcome coverage and a local asynchronous Spin service probe for actual timer wakeup,
  ready success, expired-entry zero polling, timeout provenance and guest-owner release. This
  does not add independently configured connect timeouts or upgrade provider capabilities.

### Outbound Response Handoff Deadlines

- Check the selected absolute budget immediately before returning any successful outbound
  response on all four adapters. Fix Axum, Cloudflare and Spin bodyless/streamed head handoffs
  that could return late success after normalization; retain Fastly's existing enforcement.
- Replace duplicated early-success constructors with one checked return per adapter. Keep
  header/source/cap errors, body suppression, 205 handling, application clocks and lazy streams
  unchanged. A rejected handoff releases its native source; Cloudflare's already-disarmed
  known-empty 205 guard stays disarmed. No public API, capability or runtime pin changes.

### Axum Host-Known-Empty Ingress

- Preserve transport-certified `is_end_stream()` bodies as buffered empty after admission on
  Axum, for every method, without polling the native body. Keep request-context state Initial,
  not Cached, and ingress framing HostManaged. At that checkpoint, unknown streams and all
  other adapters were unchanged.
- Forward actual HTTP/1 GET/HEAD requests with absent framing or `Content-Length: 0` through
  the existing client API, even after the inbound read deadline. Chunked-empty and nonempty
  streamed GET controls still return 400 without contacting the origin and preserve connection
  boundaries. Demo and generated proxy handlers benefit from the same converter without API changes.
- Implement only P2's existing known-empty conversion subtask as an engineering sequencing
  clarification, not a new approval or full P2 completion. Core first-`body_bytes` deadline
  precedence stays unchanged. General outbound settlement and support/time/head hooks remain
  with atomic P1; no capability row, promotion, backend policy, timer or slack changes.

### Fastly SDK Body Absence

- Preserve only actual SDK body absence (`try_take_body() == None`) as core `Body::empty()`
  in direct conversion and admitted dispatch, without creating an SDK body. Every present
  SDK body remains lazy, even with known host length zero. Preserve prefetched read bytes
  and pending writes after the prepare hook, with no header/hint inference, repacking, read
  or flush during conversion. Request-context state remains Initial, not Cached; core's first
  `body_bytes` read-deadline check can still return 408.
- Add actual WASI SDK regressions showing payload remains in prefetched read and pending-write
  buffers despite zero host length. Host length is not EOF even beyond SDK buffering:
  Viceroy uses Hyper exact size hints, not a terminal-body signal.
- G3's actual `from_client()` requests always supplied a body, including absent framing and
  CL0. This checkpoint does not fix ordinary Fastly bodiless forwarding or general settlement.
  Demo/generated proxy APIs remain unchanged and share the converter; this does not establish
  raw Fastly GET proxy success. This is scope selection, not new user approval or a Native
  claim; no capability rows/promotions, B/backend policy, slack, timers or SDK/runtime pins change.

### Cloudflare Native Body Absence

- Preserve native Fetch `Request.body == null` as buffered empty in direct conversion and
  admitted dispatch, without a read. Remove the unsupported ArrayBuffer-only fallback and unused
  attachment helper. Every present body remains streamed, including an empty ArrayBuffer or
  Uint8Array; headers and SDK bookkeeping do not imply absence. Admission refusal/abort still
  precedes source inspection, and consumed present bodies keep their construction error.
- Keep context state Initial, HostManaged framing and the first-read deadline/poisoning contract.
  Demo/generated entrypoints inherit the converter unchanged. Add native WASM and actual local
  workerd source-presence regressions; this does not complete general settlement, eager/head-first
  migration, provider qualification or change SDK pins, capabilities or backend policy.

### Native Image Optimizer

- Record the requester's scoped 2026-10-24 timing/native-integration approval: original logical
  deadlines and late-result rejection remain; +5 ms stays a measured qualification target/test,
  not a hard scheduler guarantee. The synchronous optimizer blocking horizon is explicitly UNKNOWN
  until provider evidence. Preserve historical failures; hosted/performance qualification stays blocked.
- Add a Send-first optimizer checkpoint through existing `execute`/`process_response`, with typed
  request/parts attachment, support-first checks on all four adapters, direct SDK single-Send mapping,
  `BadGatewayReason::ImageOptimizerUnavailable` on the non-exhaustive core reason enum and post-return
  original attributed 504. Map SDK `SendErrorCause::ImageOptimizerUnsupported` to a 502 while preserving
  the existing message `Fastly outbound request failed` exactly; only the typed reason/provider detail
  distinguish this cause. Keep the ordinary SDK send branch
  and current trait unchanged; add no separate transport, default, alias or compatibility helper.
  This is an implementation sequencing decision, not approval of different signatures or a claim
  of full migration completion. Full EZ-1 settlement, Follow, B/backend policy, engine and atomic head-first
  migration stay open. Backend B/resource constraints, policy, timers and slack are not approved or
  changed; typed body-error/readiness safety remains required. No deployment or performance claim.
- Clarify the checkpoint's timing anchor: the current async `send` selects its budget at adapter
  method entry on first poll, before SDK preparation/registration, and preserves it across the native
  call. Facade-invocation capture and full P1 remain pending with the later atomic head-first/trait
  cut. Static inspection confirms native SDK `.send` selection, not runtime host entry. The
  `image_optimizer_unavailable_is_typed_on_viceroy` test proves option retention and typed unavailable
  errors only; SDK `.send_async` can return the same unsupported error without host entry. Synthetic
  successful-response cap/laziness checks do not establish hosted image transformation or qualification.
- Publish `outbound-image-optimizer` as BestEffort for Fastly single sends and Unsupported on
  other adapters. Add a Fastly-only `/image-demo` example to the demo and matching generated
  projects, retaining response limits, native default caching and explicit typed settings.
- Remove the redundant conditional source-error drop from Fastly's no-attempt constructor;
  retain original private source ownership and check the native `cli,test-utils` build too.

### Fastly B31 Backend Policy

- Adopt the approved Fastly B31 aggregate backend policy: stable versioned names, a shared
  verified registration registry, ceiling-rounded coupled timers and unchanged logical deadlines.
  Remove client namespaces and the exact-budget/quarter-split policy. Readiness, dispatch guards,
  runtime pins and hosted/resource qualification remain separate and unchanged.

### Qualification Evidence Preparation

- Exercise the B31 proposal's 62 timer vectors across coupled/explicit Connect scopes through
  the pinned SDK and stock local Viceroy, with typed overflow and setter-omission controls.
  This is local registration evidence only; production policy and qualification remain open.
- Prepare a pending Fastly G2/G4 backend-policy/readiness review packet. Compare literal B31/B1,
  execute full-domain boundary/timeout-anchor arithmetic, and retain separate SDK/provider,
  runtime, resource and qualification gates. No production policy or runtime changes.
- Reject empty measurement lists for comparison-required legs and mark hosted Image Optimizer
  acceptance as comparison-required. This checks evidence presence, not metric-policy sufficiency.
  Retain a fresh stock/repaired local recheck and replay its negative cleanup/timing/error results;
  runtime integration, hosted performance and Fastly policy approval remain open. No deployment.
- Record native Image Optimizer parity requirements and exact integration/test steps. Require
  direct typed SDK mapping, lazy bodies, unchanged backend reuse and no extra fetch, plus paired
  hosted application/SDK-only performance evidence. Runtime integration, numerical horizon/policy
  approval and performance qualification remain open; no optimizer execution is enabled.
- Add bounded native/WASI/empty-select timer controls and raw ABI timing inside the opt-in Fastly
  diagnostic. Retain lateness and matched stock/repaired artifacts without inferring a timer fix,
  changing +5 ms acceptance or replacing previous failed evidence. Production APIs/pins are unchanged.
- Expand the opt-in Fastly readiness diagnostic to eleven controlled origin cases, buffered and
  pull-streamed consumption, two read capacities and three repetitions: 135 probes / 147 requests.
  Require complete inventories, fresh per-slot readiness, unchanged absolute deadlines, typed
  source errors and joined bounded origins. Persist partial traces before failures. Retain negative
  timing evidence without widening +5 ms or promoting production/hosted guarantees.
- Carry a Viceroy 0.21.0 source patch with co-located readiness regressions and an explicit build/
  runtime-override recipe. Preserve typed errors and yield on empty/no-output frames. Retain
  stock-versus-repaired guest evidence separately; version pins, installed runtime, production
  transport and full G2/hosted qualification remain unchanged.
- Add an unaccepted TS qualification runbook source template with explicit freeze/build inputs,
  command/count ownership and separate approval/authorization gates. Mark full-outbound execution
  and frozen export as unimplemented; existing egress probes do not qualify the full package.
- Add `--assemble` and `--report` to the existing outbound evidence checker. Preserve per-leg
  outcomes, counts and raw SHA-256 hashes; drafts remain explicitly pre-freeze and unqualified.
- Require explicit clean/frozen provenance and linked SHA-256 build identities for release
  records. Aggregate independent blockers using the same rules as fail-closed release validation.
- Bound artifact hashing to regular files of at most 256 MiB with 64 KiB scratch space; refuse
  unsafe paths and evidence overwrites. Exercise the actual Node CLI from the fixture's Rust
  sentinel and existing workspace CI. No transport contract, runtime pin or capability changes.

### Policy-Aware Axum Servers

- **Breaking:** `AxumDevServer::{new, with_config}` consume an assembled `App`, not a router.
  Preserve configured ingress/admission, renderers, extraction limits, clocks, state, platform
  metadata and response-egress policies. `run_app` uses the same construction/serving path.
- Add `AxumDevServer::serve(listener, shutdown)` and `test-utils::AxumTestServer`, with a real
  bounded HTTP/1 listener and local executor. Explicit shutdown cancels connections and awaits
  worker cleanup and the serving result; drop only signals cancellation. Document skipped
  startup orchestration and absent foreign provider contexts in host-parity tests.
- Prefer the router's supplied `HttpClient` without constructing an unused default transport;
  otherwise construct the default once with the app clock. Remove the obsolete transport-only
  request-conversion helper. Warn once when normalized head limits exceed the raw parser budget.
- Add TCP policy, upload, streaming, HEAD, raw-head, local-work, store/state and cleanup controls.
  These do not promote capabilities or replace consumer/hosted qualification.

### Bounded Synthesized Response Headers

- Add `App::set_synthesized_response_headers` and immutable `SynthesizedResponseHeaders` for
  detached ingress errors, admitted source errors, canonical 404/405 errors and adapter-built
  fallbacks. Routed responses, routed handler errors, application admission refusals and
  unconfigured assembly failures remain undecorated.
- Validate at most 32 field values and 4096 serialized field-section bytes before installation;
  reject framing/coding fields and nonportable values with payload-free typed errors. Compact
  retained maps and field backing storage. Invalid replacements leave the valid policy intact.
- Preserve response-owned names and ordered repeated configured values. Canonical `Allow`,
  effective config retry policy and typed generated-overflow `Retry-After` suppression take
  precedence. Fallback decoration performs no application callback or repeat validation.
- Let Fastly `prepare` install a validated per-request replacement through its extension bag,
  including an explicitly empty set. Retain snapshots through initial preparation failures and
  Axum's emergency fallback without changing completion ownership or fallback deadlines.
- Align demo/scaffold configuration and tests. Local contract checks are separate from consumer,
  deployed runtime and frozen-candidate qualification; no capability is promoted.
- Add an independent four-runtime HTTP/1 wire suite for maximum-size repeated fields, normalized
  Fastly rejections, GET/HEAD fallbacks, request replacements and undecorated application paths.
  Preserve failed captures and verify actual EOF and final correlated terminal reports.
- Verify partial-configuration header exclusion on canonical assembly failures and generation-two
  policy after retained recovery. Cover Axum's startup-failure precedence without an HTTP response;
  correlate Fastly delivery reports per request and observe failed-state release before retry.

### Buffered Response Framing

- Derive exact buffered response lengths, including empty and framework fallback bodies, on
  Axum, Cloudflare and Spin. Preserve explicit source-length validation and lazy stream framing;
  suppressed HEAD/204/205/304 bodies gain no core-derived length.
- Stage Fastly buffers before a single SDK-equivalent Automatic handoff. Check deadlines around
  each staging write; never pump, finish or abandon a body consumed by a successful bulk send.
  Preserve strict body suppression and zero payload accounting. Do not restore HEAD bytes for
  size inference: pinned local SDK controls expose HTTP/2 failures and forbidden 205 payloads;
  exact HEAD/205 SDK parity remains unqualified pending hosted evidence. Host compression owns
  final wire lengths; hint-bearing declared streams use
  Automatic framing while retaining source-length enforcement.
- Hard-cut framework egress preparation and `ResponseEgressEnvelope::begin` to require an explicit
  `ResponseEgressFramingMode`; migrate demos, scaffolds and fixtures without compatibility aliases.
- Add `buffered-response-content-length`: Native on Axum, BestEffort on serverless adapters pending
  qualifying runtime/hosted evidence. Local probes do not complete hosted compression or consumer
  parity gates, and do not alter the failed Fastly outbound body-read readiness gate.

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
  Extend local wire checks to failed writes, deletes and lists, verify deleted state,
  correlate actual typed operation results, and recheck every completion after shutdown.
  These do not qualify hosted outage recovery, KV allocation, or stronger capabilities.

### Fastly Custom Delivery Reports

- Hard-cut `send_request_with_registries_and_hooks`, `send_with_registries_and_hooks`
  and `FastlyService::send_request_with_hooks` from `Result<Option<State>, _>` to
  owned `FastlyRequestDelivery<State>`. Distinguish no attempt, policy abort, detached
  and routed delivery; preserve routed finalizer state after failure.
- Retain exact terminal completion reports, last-body commitment and separate delivery
  errors. Permit native replacement only for no attempt or confirmed non-commitment,
  never unknown commitment, policy abort or committed failure. Public pre-attempt
  diagnostics are typed and payload-free; standard serving preserves original errors.
- Add consuming `ResponseEgressAttempt::into_terminal_report` with the existing live
  Drop semantics and no second terminal clock sample or notification. Standard Fastly
  run/serve/send policy is unchanged, including successful fallback handling.
- Migrate fixture callers and publish synthetic `delivery::testing` utilities. Demo
  and generated applications retain the unchanged standard entrypoints. Local runtime
  characterization does not qualify hosted delivery or promote capabilities.

### Fallible Application Assembly

- Hard-cut `Hooks::routes` and macro-generated `build_router` to
  `Result<RouterService, EdgeError>`. Route/state assembly failures return unchanged,
  skip configuration and never produce a partially configured application.
- Add `App::from_router::<A>` for externally built routes, applying the same name,
  configuration and complete platform-metadata check once. Raw constructors remain
  low-level tools, not compatibility assembly wrappers.
- Add borrowed `App::state::<T>` and `RouterService::state::<T>` access to the existing
  registry. Configuration, entrypoints and request extractors share registered interiors.
- Migrate adapter contracts, demo, generator and reusable fixtures. Verify real
  successful-only `Sandbox<App>` retention after route and configuration failures.
- Adopt one assembly-failure delivery contract across all serverless entrypoints and
  fallible macro state expressions (`state = load_state()?`). Add app-free
  `App::assembly_error_egress` and adapter `request::send_assembly_error` helpers;
  use the existing default renderer and owned converters, preserving typed status,
  internal-error redaction, HEAD suppression and one full boot diagnostic per attempt.
- Hard-remove host-generic assembly-error mappings and their test-only wrappers.
  Failed apps supply no configured callbacks or headers. The default finite write
  deadline uses the captured pre-assembly start and supplied clock; it does not
  interrupt synchronous construction. Axum logs the original failure on the boot target
  and still fails before listener binding, without constructing an HTTP response.
- Standard retained Fastly serving answers assembly failures and retries on the next
  request after successful delivery; custom callbacks receive the detached terminal
  report without building a temporary app. Preserve commitment-safe no-resend rules.
  Demo and generated entrypoints inherit this behavior without extra application plumbing.
  Local runtime characterization remains separate from consumer and hosted qualification.

### Outbound HTTP And Batches

- Add standalone validated Image Optimizer values with typed format/filter/focus selectors,
  a public seven-variant region enum with exact pinned-SDK token conversions, private settings
  and complete builder readback. Remove the string-backed region wrapper. Validate
  quality, nonzero dimensions and paired percentage focus without provider dependencies.
  Request attachment, support-first preflight and Fastly execution remain unimplemented/gated;
  these values do not enable optimization or add ignored request options.
- Add the portable head/finalizer foundation: original Location evidence and field fidelity,
  pre-normalization header caps, policy-controlled markers and exactly-once exchange ownership.
  Discard never reads payloads; finalization preserves coding, independent caps, bodyless/205
  semantics, paired-clock deadlines and typed failures. Yield between encoded reads before
  decoding so ready empty/no-output input cannot starve the deadline race. Contexts remain opaque;
  the live facade/adapter trait migration, progress scopes and redirect API are still pending.
  No compatibility path, new transport, provider pin or capability promotion is introduced.
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
  release timer/factory state on termination or cancellation. All four adapters now use the pair
  for live upload, fetch and response deadline handling. Hard-migrate clock-only constructors to
  Axum `try_with_time` / internal `with_transport_and_time` and serverless `with_time`, with no
  compatibility aliases. Standard request injectors pair the application clock with the existing
  runtime timer. Timer setup/failure precedes source polling; Fastly timer-only waits share its
  progress multiplexer, Workers validate scheduling/cancellation together, and Axum has no
  independent reqwest total-timeout clock. Dispatch-trait migration and hosted qualification
  remain separate; provider capability levels are unchanged.
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
  shape remains. Hosted qualification remains follow-up work.
- Publish `outbound-failure-diagnostics`: Native for Fastly/Spin's existing typed SDK-cause
  projection and BestEffort for Axum/Cloudflare's reduced cause visibility. Manifest parsing,
  required/optional gates, adapter support and published matrices use the same capability.
  This adds no transport guarantee, reconstructs no host-hidden cause and does not complete
  hosted qualification or the remaining outbound migration.
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
