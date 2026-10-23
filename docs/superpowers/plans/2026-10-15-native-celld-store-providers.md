# Native celld store providers implementation plan

- Status: embedded celld and EdgeZero provider integration are implemented. Provider, independent-feature, startup and generated-consumer checks have executed, including verified native TLS and disposable namespace isolation. The final integration checkpoint below records current results. Fleet, deployment and stress acceptance remain open. Work checkboxes describe implementation, not production acceptance.
- Governing spec: [Native celld store providers](../specs/2026-10-15-native-celld-store-providers-design.md).
- EdgeZero baseline: `350527e4b7387a86da3f4cdaea0e8459665aa0f6`, with the spec introduced at `32737dd29b8f1fefd6e035be8036780e58b2412c` and the approved structural-only CLI clarification applied afterward.
- celld research baseline: `90b43017241f81189453d326d05948f388b34652`, version 0.6.2. Recheck the selected source and executable before running proofs.
- Approved scope clarification: offline CLI validation stays structural-only and reports target provider availability as unknown. Do not add a Cargo feature-graph resolver.
- Authority: the user requested committing the spec/plan and starting implementation, then explicitly selected implementation inside celld with no separate service. This authorizes scoped celld source changes and EdgeZero integration under this plan. No push, production deployment or real fleet/AWS access is authorized.

## 1. Outcome and invariants

A native application can select celld independently for each declared configuration or KV store ID. Configuration is an immutable startup snapshot. KV operations remain live. AWS, celld, local stores and supplied handles can coexist in the same native runner without changing portable application declarations or HTTP handlers.

Required invariants:

- Keep core `ConfigStore`, `KvStore`, handles, registries, bounded-read contracts and `#[async_trait(?Send)]` future bounds unchanged.
- Applications call a scoped service API using store aliases. They never receive fleet HMAC secrets, fleet-bucket discovery credentials, internal node addresses or arbitrary runtime scopes.
- Support explicit trusted-network and authenticated listeners. Both modes enforce store and operation grants before backend dispatch. Authentication failure never selects anonymous access.
- Config preparation finishes before the app initializer, readiness and listener acceptance. A missing mapped document or failed remote call never falls back to local data.
- Validate the complete source plan, selected credentials, settings and compiled capabilities before opening runner-owned providers or polling supplied deferred futures.
- A celld-only deployment needs no AWS credentials and no local roots for remotely bound IDs. Local defaults still require their existing roots.
- No automatic replay of mutations with an ambiguous outcome, at either client or service layer. A timeout does not mean that no write occurred.
- Errors owned by EdgeZero contain fixed categories and validated logical IDs, not credentials, physical namespaces, URLs, response bodies or arbitrary source chains.
- Default-generated applications, AWS-only builds and WASM adapters do not acquire celld runtime, V8 or Worker tooling dependencies.

Out of scope: a separate gateway, sidecar or proxy service; a celld HTTP adapter, Worker bridge or application runtime; celld secrets; fleet provisioning; config publication; secret management; live config refresh; CAS, transactions or counters; dynamic provider registration; extraction of existing local stores; AWS redesign; process-wide memory guarantees; automatic fleet failover acceptance.

## 2. Embedded v1 decisions and release gates

The scoped API does not exist in the researched upstream celld version. The initial implementation now lives inside celld, not another service. Its listener schema, protocol, limits and restart-based policy adoption are recorded in the governing spec. The table below defines what must stay explicit and what needs release/acceptance evidence; provider settings must not bypass those remaining gates.

| Decision                 | Required output                                                                                                              | Constraint                                                                                                                                                                                                                                                                                                       |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| API placement            | Resolved by the user: inside the existing celld process                                                                      | No separately operated gateway, sidecar or proxy. Celld implementation worktree: `/home/pav/worktrees/feat-scoped-store-api`, branch `feat/scoped-store-api`, based on the researched pin. Initial implementation revision: `f72ae21a515859570622ac9d6ca9c19cf6870b81`. Release/upgrade acceptance remains open. |
| Listener policy          | Exact configuration schema for mode, bind address, alias mapping and operation grants                                        | Disabled by default; tokenless loopback default; explicit non-loopback opt-in and separately enforced network restrictions. No client-supplied caller identity.                                                                                                                                                  |
| Authenticated access     | Token verifier format, secret injection, reload, overlap, revocation and restart policy                                      | Random application bearer tokens over TLS. No issuance service or application fleet credentials. A documented bounded restart-based service policy is acceptable if hot reload is unnecessary.                                                                                                                   |
| HTTP protocol            | Version, methods, status/error schema, TTL units/rounding, cursor shape, malformed-input and body rules                      | Missing differs from empty. No mutation replay. An incompatible version fails explicitly. The implemented v1 protocol is pinned to the celld revision above; release acceptance remains open.                                                                                                                    |
| Transport policy         | Native TLS in the celld scoped listener, certificate validation, trusted-network HTTP opt-in and internal network protection | Bearer tokens require TLS. A private address is not proof of isolation or encryption. Reuse existing in-process owner dispatch rather than a local HTTP proxy. The application client and any retained celld peer transport must not redirect or discover ambient proxies.                                       |
| Limits and compatibility | Concrete service/client limits, deadlines, preparation quotas and supported celld revision policy                            | Account separately for raw bytes, backend JSON byte-array/base64 encoding, decoded payloads, key pages, cursors, metadata and retained snapshots. No claim that the portable 25 MiB ceiling is always reachable.                                                                                                 |
| Public settings          | Provider tags, feature names, alias/endpoint/access settings and policy ownership                                            | The implemented bindings and defaults are documented in the native celld guide; they remain unreleased. Settings contain references, not credentials.                                                                                                                                                            |

Keep the remaining release and operational acceptance gates explicit. The v1 protocol and binding examples below are implemented preview contracts, not a released compatibility promise. API placement is settled. The scoped listener remains inside celld.

## 3. Delivery order

```mermaid
flowchart TD
    Baseline[Stage 0: baseline and protocol decisions] --> Service[Stage 1: one scoped service implementation]
    Service --> Proof[Stage 2: native transport and authorization proof]
    Proof --> Settings[Stage 3: provider crate and typed settings]
    Settings --> HTTP[Stage 4: bounded scoped HTTP client]
    HTTP --> KV[Stage 5: live KV]
    HTTP --> Config[Stage 6: config snapshots]
    KV --> Compose[Stage 7: native source composition]
    Config --> Compose
    Compose --> CLI[Stage 8: generated features and structural CLI]
    CLI --> Consumer[Stage 9: generated application acceptance]
    Consumer --> Gates[Stage 10: docs and final checks]
```

The settings and client stages may use fake scoped-service fixtures after the contract is frozen, but only after Stage 2 proves the chosen real celld path is feasible. KV and config work can be researched independently. Keep shared manifests, lockfiles, settings, session state and native composition under one edit owner. Serialize Cargo commands that share a target directory.

## 4. File map and proposed API shape

Existing production files expected to change narrowly:

| Area                     | Files                                                                                                            | Change                                                                                            |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Workspace                | `Cargo.toml`, `Cargo.lock`                                                                                       | Native provider member, workspace dependency and reviewed HTTP feature selection                  |
| Native features          | `crates/edgezero-adapter-axum/Cargo.toml`, `src/lib.rs`                                                          | Settings-only dependency and independent `celld-config` / `celld-kv` forwarding                   |
| Bindings                 | `crates/edgezero-adapter-axum/src/native_bindings.rs`                                                            | Strict config/KV variants, capabilities and independent provider policy validation                |
| Credential capture       | `crates/edgezero-adapter-axum/src/run_options.rs`                                                                | Capture selected celld token names once without coupling them to `AgentAccessToken`               |
| Preparation              | `crates/edgezero-adapter-axum/src/native_stores.rs`, `src/dev_server.rs`                                         | Typed provider dispatch, separate preparation sessions and existing staging/readiness path        |
| Generated features       | `crates/edgezero-adapter-axum/src/templates/Cargo.toml.hbs`, CLI generator tests                                 | Default-off independent celld aliases, preserving AWS alias membership                            |
| CLI                      | `crates/edgezero-cli/src/config.rs`                                                                              | Structural celld validation, availability unknown and remote config-push refusal                  |
| Dependency checks and CI | `scripts/check_native_store_dependency_contract.sh`, `.github/workflows/test.yml`, applicable format workflow    | Isolated feature graphs, focused provider/startup/generated checks and explicit service-proof job |
| Documentation            | New `docs/guide/native-celld-stores.md`; existing Axum, configuration, CLI and navigation pages where applicable | Deployment boundaries, bindings, limits, operator publication and proof limitations               |

New EdgeZero files:

```text
crates/edgezero-store-celld/
  Cargo.toml
  src/lib.rs
  src/settings.rs
  src/client.rs
  src/preparation.rs
  src/config.rs
  src/kv.rs

crates/edgezero-adapter-axum/tests/
  native_celld_startup.rs
  fixtures/native_celld_host.rs

crates/edgezero-cli/tests/
  celld_generated_proof.rs

scripts/
  run_native_celld_integration.mjs
  run_native_celld_integration.test.mjs
```

Keep ordinary unit tests colocated. Separate test modules only when fixture size warrants them. Add one shared test-support module if actual repeated fixture code needs it, not a public transport/provider framework.

The scoped listener lives inside the celld repository, not in a new EdgeZero service package. Work in `/home/pav/worktrees/feat-scoped-store-api` on branch `feat/scoped-store-api`, based on celld `90b43017241f81189453d326d05948f388b34652`; keep the research checkout unchanged. Inspect celld's `main.rs`, `startup.rs`, `operator_cell.rs`, `kv_cli.rs`, `peer_auth.rs`, `crates/logic/kv.rs` and `js/services/kv.js`. Reuse existing owner dispatch inside the process after authorization. Keep pure policy decisions in `crates/logic` and effectful listener/credential/TLS handling in `crates/celld`. Do not depend on the full celld crate or copy its runtime into the EdgeZero application provider.

Implemented provider interface:

- `edgezero-store-celld` has no default features. Strict settings and redacted credential types remain available without the HTTP client or Tokio. Native-only `config` and `kv` features share one internal scoped HTTP implementation.
- Typed config settings map each local lookup key to a scoped store alias and remote key. The default key must name a mapping. Typed KV settings select one scoped store alias. Physical namespace mapping stays in service policy.
- A `CelldPreparation` session owns the aggregate config deadline, mapping/metadata and snapshot accounting, and compatible client reuse. Programmatic callers create and reuse an explicit session. Live KV contents are outside snapshot quotas.
- Factories return existing `ConfigStoreHandle` and `KvHandle` outputs. No new portable store trait, global provider registry or executor.
- Preserve `NativeStoreBindings::new(config, secrets)`, `with_kv`, `with_aws_limits`, existing AWS settings and ready/deferred override behavior. Add celld policy inputs without forcing existing callers through a new constructor.

Implemented deployment binding schema:

```toml
version = 1

[config.app]
provider = "celld-config"
endpoint = "http://127.0.0.1:7780"
default_key = "settings"

[config.app.access]
mode = "trusted-network"

[config.app.documents.settings]
store = "checkout-config"
key = "application-envelope"

[kv.state]
provider = "celld-kv"
endpoint = "https://stores.example.internal"
store = "checkout-state"

[kv.state.access]
mode = "authenticated"
token_env = "CHECKOUT_STORE_TOKEN"
```

The first alias has read-only grants; the second has the required KV operations. The example hostname is fictional. The service owns physical namespace selection, TLS and grants. An authenticated binding with a selected missing/empty token fails startup; it never becomes anonymous. Neither binding contains a fleet credential. Represent `access` as a validated tagged type, not an untyped field bag.

## 5. Stage 0: record baseline and close the contract gate

### Work

- [x] Record HEAD, dirty files and governing spec hash. Preserve the current approved spec edits and unrelated work. Recheck local repository instructions.
- [x] Inventory the actual core store contracts, native source plan, startup staging, bootstrap token capture and generated feature tests. Confirm that offline validation normalizes controls but does not resolve target capabilities.
- [x] Verify the selected celld source, executable version, backend protocol and test tooling. Record whether the scoped API will need an external change or release. At the researched pin, operator values use byte arrays inline and `put-base64`/`valueEncoding` above 1 MiB; listing uses a raw last-key `after`, not a scoped opaque cursor; there is no native `exists` operation. Specify the service's bounded existence implementation and cursor wrapper rather than assuming those HTTP operations map one-to-one.
- [ ] Finalize the remaining listener/protocol decisions in section 2 in the governing spec. Placement is resolved: the scoped API runs in celld, with native TLS when authenticated. Include an operation/error table, limit table and deployment trust-boundary diagram. Record implementation files and resulting celld revision.
- [x] Define a fixture policy using isolated local storage, dummy application tokens, explicit ports and resource ownership. No ambient cloud credentials, production fleet discovery, internet-exposed listeners or real AWS acceptance.
- [x] Record a focused baseline for existing source validation, startup, CLI structure and dependency graphs before production edits. Classify pre-existing failures once; do not silently exclude them.

### Exit gate

An embedded celld implementation, a frozen first-version contract and a safe runnable local setup. The user has selected placement and authorized implementation. Contract research alone is not runtime proof.

## 6. Stage 1: implement the scoped service boundary

### Work

- [x] Implement a disabled-by-default scoped listener inside the existing celld process. Reuse existing owner-routing effects, not a loopback HTTP bridge or separate binary. Keep alias/grant policy separate from dispatch, with no administrative proxy or arbitrary scope/address input.
- [x] Validate all listener policy at startup. Disabled means no listener. Enabled tokenless access defaults to loopback. Missing/invalid mode cannot select trusted-network access.
- [x] Resolve aliases and authorize the exact operation before constructing a backend request. Config aliases are read-only. Whole-namespace grants and dedicated config namespaces are sufficient for v1.
- [x] Implement authenticated credential verification, native TLS on the scoped listener and the defined replacement/revocation policy. Never log supplied tokens or verifier material. Trusted-network listeners grant the same configured permissions to every reachable caller.
- [x] Implement the approved versioned raw-byte value operations, existence check and bounded JSON key pages. Reject duplicate/malformed parameters and unsupported operations. Return a distinct missing response for reads and body-free existence responses.
- [ ] Keep discovery, fleet authentication, namespace-to-scope derivation and wire compatibility inside this boundary. Bound privileged encoded requests/responses independently of the public raw-byte budget, including inline byte-array expansion and the pinned base64 form. Reject over-cap writes before backend dispatch and over-cap responses before unbounded decoding, including encoding paths excluded by approved value caps.
- [x] Dispatch authorized operations through celld's existing in-process owner path. Do not add external operator discovery, a local HTTP client or a new privileged service where existing dispatch suffices. Audit any reused peer transport for redirect/proxy behavior and bounded parsing. The pinned external `operator_cell::Reachable` helper is research material, not the implementation to copy.
- [x] Define one retry owner. Permit only explicitly proven pre-dispatch refusals if the approved contract needs them; never replay an ambiguous mutation or add nested client/service retry loops.
- [ ] Apply finite request deadlines and body, page, cursor and concurrency limits. Bound work and memory under slow callers and disconnects. Do not claim dropping a request undoes an applied backend write.

### Tests and exit gate

Drive public handlers with an instrumented backend. Prove zero backend calls for out-of-grant aliases, config writes, malformed input and denied operations in both modes. Prove credential isolation, missing/revoked-token rejection, no auth fallback, disabled/loopback defaults, remote-tokenless opt-in rejection, body/page limits, deadline termination and redacted error responses. Test both backend encodings and their encoded/decoded budgets, or explicit over-cap rejection where a path is unsupported. Verify authorized calls reach only the existing owner-dispatch path, and audit/test redirect/proxy restrictions on any network hop that path uses. Test the actual chosen service implementation, not a second policy implementation in EdgeZero fixtures.

Before moving on, document how application workloads are prevented from reaching celld's internal listener or reading fleet credentials. An HTTP authorization test does not establish deployment network isolation.

## 7. Stage 2: prove scoped native access against real celld

### Work

- [ ] Start one disposable local fleet and the chosen service using the pinned executable/revision. Record commands, environment, acquisition and storage of fixture-only fleet credentials, listener addresses and cleanup.
- [x] Use a small native client without an application Worker. Through the service, exercise binary and empty put/get, missing reads, exists, overwrite, idempotent delete, prefix pages and two isolated stores.
- [x] Exercise trusted-network loopback and authenticated TLS with valid certificate verification. Use a test CA through test setup, never a production `accept_invalid_certs` switch.
- [ ] Drive incompatible protocol versions, invalid tokens, out-of-grant stores and config write denial. Observe backend dispatch counts for authorization denials.
- [x] Verify real operator-wire TTL set, renewal and plain-put reset, and document how fractional `Duration` values will map to the approved wire units without shortening lifetime. Record the value encodings and size range exercised; a small-value result does not verify the bucket-backed/base64 path.
- [x] Exercise a listing larger than one page and record ordering and continuation behavior. Preserve empty/partial pages where the backend can return them; deterministic malformed-page cases belong in later fixtures.
- [ ] Demonstrate network/credential isolation using the actual deployment arrangement selected for the proof. Host loopback success alone does not establish container or multi-host isolation.

### Exit gate

Retain a rerunnable command or script and explicit observed results for transport and authorization. The client has no fleet credentials and sends no internal runtime requests. A missing executable, failed fleet startup, absent scoped API or skipped authenticated case leaves this gate unverified. Do not proceed on the strength of source inspection alone.

## 8. Stage 3: add the native provider crate and strict settings

### Work

- [x] Add the workspace member/dependency with independent native-only `config` and `kv` features and no defaults. Keep settings usable in CLI-only builds without service clients, Tokio or celld runtime code.
- [x] Use existing workspace dependency versions where compatible. Review `reqwest` feature unification, since workspace defaults already include blocking/JSON support. Enable only necessary provider features and record the resolved graph rather than assuming manifest defaults prove isolation.
- [x] Implement strict endpoint, access, alias, document and limit settings. Reject unknown fields, invalid combinations, empty mappings, a missing default mapping, malformed URLs, userinfo, queries/fragments and unsupported endpoint paths.
- [x] Normalize caller input once into typed internal settings. Preserve exact remote keys and alias case; do not Unicode-normalize or reinterpret them as runtime scopes.
- [x] Validate finite positive limits, checked arithmetic and relationships among encoded/decoded/body/page and session budgets. Keep service limits and application/provider limits distinct.
- [x] Add a redacted celld token wrapper separate from AWS `AgentAccessToken`. A selected token source must resolve to a valid nonempty header-safe value. No ambient lookup inside the provider.
- [x] Define closed preparation/client error categories and fixed mappings into current portable errors. Do not add provider codes or source chains to core.

### Tests and exit gate

Settings-only tests cover every rejected shape, URL encoding boundary, token Debug redaction, case sensitivity, limit overflow and access-mode combination without network calls or ambient variables. Independently check settings-only, config-only and KV-only dependency graphs. No Axum, CLI, AWS, full celld, V8, `workers-rs` or `worker-build` dependency enters the provider graph.

## 9. Stage 4: implement the bounded scoped HTTP client

### Work

- [x] Construct requests using an encoding library and the frozen contract. Test empty prefixes, Unicode, reserved delimiters and percent encoding without allowing path/query injection or double decoding.
- [x] Disable redirects, ambient proxies and implicit mutation replay. Set finite connect and whole-operation deadlines. Preserve normal application logging; do not install a logger or subscriber.
- [x] Require TLS for bearer credentials and validate certificates. Enforce the explicit trusted-network HTTP policy selected at Stage 0. Do not infer permission from a private hostname/IP.
- [x] Stream and cap response bodies before accumulation. Request identity encoding and either reject non-identity encoding initially or implement separately budgeted wire/decoded limits under an approved contract. Bound error bodies too.
- [x] Apply endpoint, header, request-value, decoded-value, page-entry, key-byte and cursor bounds. A bounded value count alone does not bound page JSON bytes or nesting.
- [x] Distinguish valid empty values from misses and protocol failures. Validate success/error status combinations and response shapes. Unsupported versions, malformed pages and auth failures must not become misses.
- [x] Reuse compatible clients by normalized endpoint and complete transport/authorization policy. Never reuse one credential's authorized state for another. Keep secrets out of cache-key Debug and diagnostics.
- [x] Close errors at the transport boundary. Document dependency DEBUG/TRACE limitations separately if testing finds them; do not promise universal redaction from sanitizing owned errors alone.

### Tests and exit gate

Exercise the actual HTTP client against loopback HTTP/TLS fixtures. Cover exact key bytes, binary/empty bodies, all operation/error categories, bad versions, slow headers/body, chunked bodies, oversized declared/streamed bodies, compression refusal, malformed JSON, page/cursor limits, disconnects and certificate errors. Have a fixture apply a PUT/DELETE and then lose the reply; assert one request, a closed error and the retained effect. Add redirect and proxy sentinels proving no alternate destination receives credentials or mutations.

## 10. Stage 5: implement live KV

### Work

- [x] Implement existing `KvStore` get, put, TTL put, delete, exists and paginated prefix listing over the scoped client. Return existing `Bytes`/`KvPage` types; do not expose transport models.
- [x] Enforce actual provider/service limits in addition to `KvHandle` validation. Smaller configured/provider limits fail explicitly before requests, without truncation or chunking.
- [x] Preserve binary bytes, misses and empty values. Missing delete succeeds. Plain overwrite clears prior expiry; TTL overwrite renews it under verified celld semantics.
- [x] Validate and encode TTL with checked arithmetic and the approved rounding rule. Accept the public handle's 60-second minimum and reject unsupported direct-trait durations rather than silently clamping them.
- [x] Preserve bytewise lexicographic prefix order. Bind client continuation state to the configured endpoint/store/prefix and wire version as required; reauthorize every service page. A cursor never authorizes a different store or prefix.
- [x] Return one bounded page per operation, including short or empty continuation pages. Validate malformed, oversized, out-of-prefix, out-of-order or non-progressing continuation responses. Do not scan indefinitely to fill a page.
- [x] Create fresh runtime operation deadlines per call. Do not reuse an expired startup-preparation deadline for a live handle.

### Tests and exit gate

Run non-TTL portable contracts against the real provider using its scoped HTTP fixture and a native Tokio runtime. A current-thread runtime/`LocalSet` can drive the existing non-`Send` futures; `futures::executor::block_on` alone cannot drive Tokio HTTP/timers. The shared full contract macro contains direct sub-second TTL cases that celld rejects, so do not invoke it unchanged or change core semantics to fit celld. Add provider-specific TTL tests for valid durations, minimum rejection, overflow, rounding, renewal, expiry and reset. Cover namespace isolation, explicit shared aliases, NFC/NFD distinction, 512-byte keys, binary/empty values, page scope/progress and one-attempt ambiguous writes. Distinguish fixture expiry proof from the real 60-second expiry check in Stage 9.

## 11. Stage 6: implement immutable config preparation

### Work

- [x] Fetch every explicitly mapped document during one bounded `CelldPreparation` session, sequentially initially. Require all mapped documents; an unmapped local lookup key remains a successful miss.
- [x] Validate UTF-8 without parsing, reserializing, projecting JSON fields or resolving secrets. Preserve whitespace, empty documents and exact complete `BlobEnvelope` text.
- [x] Account for each mapping and its metadata before deduplication. Deduplicate successful fetches only for identical endpoint, alias, remote key and full access/transport policy. Count shared immutable payload allocations once.
- [x] Enforce per-document transfer/value limits, total retained snapshot bytes and an aggregate preparation deadline across all runner-owned celld config IDs. Bound transient buffers too, without describing these counters as an RSS guarantee.
- [x] On any failure drop staged state and publish no partial config registry. Never reuse a prior process snapshot or add a stale/local fallback.
- [x] Implement both ordinary and bounded core reads over immutable `ConfigValue`. Use the existing bounded-read helper where appropriate: deadline precedence, independent backend/value allowances, correct payload-byte accounting and zero bytes for a miss.
- [x] Keep typed envelope validation and nested secret resolution in core/app startup. A celld document may reference an independently bound AWS secret. Multiple documents/providers are not an atomic release.

### Tests and exit gate

Cover complete document preservation, empty/raw/typed documents, invalid UTF-8, missing mapped documents, unmapped keys, duplicate-target sharing versus credential isolation, mapping/metadata/per-value/session cap boundaries, zero/expired deadlines, failure cleanup and bounded snapshot reads. Prove preparation reads once and later `get` calls perform no network I/O. A remote edit remains invisible to a running snapshot and is adopted by a fresh preparation.

## 12. Stage 7: integrate existing native composition

### Work

- [x] Add typed celld config/KV variants and independent `NativeProviderCapabilities` fields. Replace current kind-map nonemptiness checks with actual-variant capability checks, so celld config/KV do not require AppConfig/DynamoDB features. Reject celld under secrets, unknown fields/tags and undeclared or conflicting sources.
- [x] Separate AWS and celld policy tables and mapping counters. Existing `[limits.aws]` documents and explicit `with_aws_limits` behavior remain compatible; celld-only policy must not require an AWS table, inherit AWS quotas or create an AWS session. A file celld policy must not conflict with an explicit AWS policy, or vice versa. Preserve existing public parse/read/validate signatures where possible through additive methods. Add only the celld policy inputs required by the approved budgets.
- [x] Replace the AWS-only config-source tuple with provider-specific typed source payloads carrying captured credentials. KV sources also need captured celld credentials. Do not reuse AWS token types or put opaque futures around session-borrowing built-in preparation.
- [x] Extend the existing bootstrap capture boundary to selected token names in celld config and KV bindings. `from_env` captures once; explicit options use only `with_bootstrap_values` or other supplied values. Missing, empty, duplicate and non-Unicode selected values fail before preparation. Unrelated ambient variables stay irrelevant.
- [x] Select preparation sessions by actual provider variants, not `BuiltIn(_)`. Create one celld session for selected celld bindings and the existing AWS session only for selected AWS bindings. Keep their state/budgets separate.
- [x] Extend the existing per-kind staging loops in `build_production_stores`. Preserve original defaults, shared handles, app initializer timing, readiness, failure cleanup and local root selection.
- [x] Register `fixtures/native_celld_host.rs` as a feature-gated `[[bin]]` and `native_celld_startup.rs` as the corresponding `[[test]]` in the Axum manifest, following the existing AWS fixture pattern. Verify all-target builds still work with celld features off.
- [x] Keep duplicate-source rejection and all-source validation before local opens, remote setup or supplied future polling. Existing ready handles remain caller-owned; the runner cannot undo their prior side effects.

### Tests and exit gate

Drive the public native runner in subprocess fixtures where environment capture matters. Cover local-only, AWS-only, celld-only and mixed providers across multiple IDs, plus ready and non-`Send` deferred inputs. Table-test actual-variant capabilities with celld-only/AWS-disabled, AWS-only/celld-disabled and each missing provider in mixed bindings. Separately prove mapping counters and policy-source conflicts are scoped to their provider. Count local opens, remote calls and deferred polls on invalid settings/capabilities/sources. Prove zero preparation on validation failure, no local fallback, no initializer/listener on failed setup, unchanged defaults and local roots only where needed. Include different config/KV tokens, one token used by both, explicit missing tokens and celld-only startup with ambient AWS discovery disabled. Reuse existing AWS regressions unchanged unless the changed dispatch requires an additive case.

## 13. Stage 8: generated features and structural-only CLI

### Work

- [x] Forward independent default-off `celld-config` and `celld-kv` features from Axum and generated native target manifests. Do not expand `aws-stores` or add an all-provider alias without a concrete need.
- [x] Add only settings support to CLI/native-bindings dependencies. The CLI need not compile live celld clients merely to parse deployment files.
- [x] Extend existing structural bindings validation to celld syntax, policy, declared IDs and conflicts. Continue reporting target provider availability as unknown regardless of requested feature names. Do not resolve Cargo metadata, probe endpoints, fetch tokens or validate remote authorization/content.
- [x] Reuse current Cargo argument normalization and forwarding unchanged where possible. Do not add a parallel resolver or rewrite arbitrary custom shell commands.
- [x] Generalize the config-push guard by selected remote config variant so both AWS-bound and celld-bound config IDs reject unsupported publication before writing local files. Keep local-bound IDs' current behavior.
- [x] Update generator/parser assertions and CLI messages, preserving existing command arguments and AWS behavior. Check `crates/edgezero-cli/src/args.rs` help text for inherited claims of target-feature resolution and describe normalization/forwarding without an availability verdict.

### Tests and exit gate

Test valid/invalid celld deployment documents through public `config validate`, including absent tokens, an unreachable endpoint and disabled target provider features. A structurally valid document still reports availability unknown and performs zero remote/token lookup. Verify feature controls are normalized/forwarded, not interpreted as a capability verdict. Prove selected remote config push fails without local side effects and local config push still works. Compile generated targets independently with neither feature, each feature alone, both and combined AWS features; startup, not offline CLI validation, rejects an uncompiled selected provider.

## 14. Stage 9: generated consumer and real-service acceptance

### Work

- [x] Generate an actual application using the existing CLI scaffold. Use portable logical declarations and handlers/extractors, the production native runner and a normal typed app initializer. Do not substitute a hand-written adapter host for this proof.
- [ ] First run against the deterministic scoped fixture to prove default-off rejection before I/O, feature forwarding, source conflicts, root handling, closed failures and CLI behavior.
- [ ] Then run the same generated native target against the chosen scoped service and real disposable celld. Test trusted-network loopback and authenticated TLS. Keep all fleet credentials in service/fleet setup, not the app's environment or filesystem view.
- [ ] Through normal application endpoints, observe binary/empty KV, put/get/delete/exists/list, multi-page order, two-store isolation and intentional sharing. Restart the app against the same fleet to prove persistence is not process-local state.
- [ ] Exercise a valid real TTL for at least the service's 60-second minimum, renewal and no-TTL reset. Use a bounded opt-in slow test and finite polling, not sub-second durations or a fixture clock presented as celld evidence.
- [ ] Publish a new complete config document using operator-owned test setup. The running app keeps its initialized snapshot; a fresh process adopts the new version. Missing, oversized, invalid UTF-8 or invalid typed config fails before readiness as applicable.
- [ ] Where installed fixtures permit it, compose celld config/KV with AWS secret fixtures and local bindings through the same generated app. Mark missing fixture coverage as unverified; do not obtain real AWS credentials to complete it.
- [ ] Verify network isolation and credential-file denial in the actual tested deployment arrangement. Retain authorization dispatch observations and fixture resource identities.

### Harness and exit gate

Add one explicit local-service harness using the repository's existing Node test-harness style if it reduces repeated setup. It must check tool/version availability, use isolated state and dummy/test credentials, clear ambient deployment credentials, set startup/operation/shutdown deadlines, reap owned process trees and clean up in a `finally` path. It must never kill an arbitrary listener or remove non-fixture storage. Unit-test orchestration and failure cleanup without launching real services.

The real-service command must fail or report an explicit unverified gate when prerequisites are missing, not exit successfully after skipping all assertions. Default unit tests stay deterministic and require no external services or credentials. Run real-service acceptance explicitly in a prepared job or with operator permission. Record exact command, app/service/celld revisions, tested features, outcome, elapsed time and redacted log path. Single-node proof does not establish fleet failover, crash recovery, encrypted overlays, large-value bucket operation, upgrades or performance.

## 15. Stage 10: documentation and stable integration checks

### Work

- [ ] Document the required celld build/version, embedded listener configuration, application bindings, operator-owned aliases/grants and both access modes. Include TLS and network restrictions, token injection, replacement/revocation, fleet credential separation and safe examples.
- [ ] Explain startup-only config adoption, non-atomic multi-document releases, optional independent secret providers, service/provider limits, TTL constraints, empty continuation pages, non-snapshot listing and ambiguous mutation outcomes.
- [ ] Explain that offline CLI validation is structural-only, startup checks compiled capabilities, and config push does not publish remote config. Keep resource provisioning/publication instructions operator-owned.
- [ ] Extend isolated dependency checks and focused CI commands. Use the nonzero-test wrapper for new ignored/generated suites so a mistaken test filter cannot produce a passing zero-test job.
- [ ] Add the real-service job only after the chosen service artifact and safe local setup are pinned and available. Keep it separate from default no-service unit suites.
- [ ] Inspect the combined diff, rerun affected gates at a stable checkpoint and record remaining blockers without changing unrelated work. Do not commit, push or deploy without a separate request.

### Planned commands

These are future checks, not results. New feature/test names below are proposed by this plan. Finalize them with the files they exercise. Use `--offline --locked` once the reviewed dependency graph is fetched and locked; dependency acquisition itself needs an explicit prepared environment.

```sh
# Focused provider units after their corresponding stages.
cargo test --offline --locked -p edgezero-store-celld --no-default-features --lib
cargo test --offline --locked -p edgezero-store-celld --no-default-features --features config --lib
cargo test --offline --locked -p edgezero-store-celld --no-default-features --features kv --lib
cargo test --offline --locked -p edgezero-store-celld --all-features --lib

# Native and CLI integration, serialized when using the same Cargo target directory.
cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,celld-config,celld-kv --lib
cargo test --offline --locked -p edgezero-adapter-axum --features celld-config,celld-kv,test-utils --test native_celld_startup
cargo test --offline --locked -p edgezero-adapter-axum --features aws-stores,aws-dynamodb,celld-config,celld-kv --lib
cargo test --offline --locked -p edgezero-cli --lib
scripts/run_test_nonzero.sh --ignored generated_celld_default_off_kv_and_config_restart cargo test --offline --locked -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum --test celld_generated_proof generated_celld_default_off_kv_and_config_restart

# Existing AWS regressions and isolated dependency contracts.
cargo test --offline --locked -p edgezero-store-aws --all-features --lib
cargo test --offline --locked -p edgezero-adapter-axum --features aws-stores,test-utils --test native_aws_startup
bash scripts/check_native_store_dependency_contract.sh

# Stable combined checkpoint and repository-required gates.
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo check --workspace --all-targets --features "fastly cloudflare spin"
cargo check -p edgezero-adapter-spin --target wasm32-wasip2 --features spin

# Explicit local-service harness once implemented and its prerequisites are prepared.
node --test scripts/run_native_celld_integration.test.mjs
CELLD_BIN=/absolute/path/to/modified/celld node scripts/run_native_celld_integration.mjs
```

Also run independent no-default/config-only/KV-only Axum/provider lint and compile scopes, existing generated AWS/DynamoDB proofs affected by shared dispatch/template changes, and selected Fastly/Cloudflare/Spin WASM graph/build checks. Extend `scripts/check_adapter_feature_matrix.sh` or add explicit provider combinations; its existing adapter/CLI/test-utils matrix does not cover celld feature isolation. For published docs changes run `npm run lint`, `npm run format` and `npm run build` from `docs/` with dependencies already prepared. Do not infer formatting coverage for process documents excluded by `docs/.prettierignore`.

Developer/CI dependency-graph audits do not add a capability resolver to the CLI. The dependency script must assert separate normal dependency graphs for settings-only CLI, new provider settings/config/KV modes, default native targets, each WASM adapter and AWS-only builds. Also inspect selected provider-root feature edges: default Axum already uses HTTP/Tokio, and CLI/default native builds may include celld settings types, so package presence alone cannot prove live-provider activation. Check both directions: celld must not add AWS service SDKs, and default/AWS-only graphs must not activate celld live transport features. Assert config-only versus KV-only feature separation and no full celld runtime, V8 or Worker tooling. Workspace `--all-features` alone cannot prove isolation.

## 16. Evidence, stopping rules and handoff

For each behavioral unit record `check | tested revision or input fingerprint | result | elapsed time | log`. Reuse passing evidence while its relevant source, fixture, dependency and environment inputs remain unchanged. Run narrow checks during editing, neighboring checks at each completed unit, and broader suites at the stable combined checkpoint.

The two highest-risk facts need direct executed evidence:

1. Out-of-grant access and config writes never reach the privileged backend in either mode. Handler source and policy design alone are insufficient. Instrument service dispatch, then exercise the real scoped service under the tested network boundary.
2. The native transport preserves values/expiry and never replays an ambiguous mutation. Inspect pinned wire handling, execute client fault fixtures and drive a generated app through the actual service/celld path. State exactly which claims each level proves.

Stop and revise the plan if safe scoped access cannot be delivered without app fleet authority, a full celld runtime dependency, a Worker bridge, a portable core contract change or an additional unapproved service. Do not conceal that change behind custom overrides or a test-only transport.

Handoff must include changed files, concrete protocol decisions, service/provider versions, completed stages, exact checks and results, known baseline failures, intentionally unexecuted acceptance cases and remaining operational risks. No stage is complete merely because a package compiles or a service starts.

## Stage 0 execution record

The user requested committing the spec/plan and starting implementation. Commit `a12265b52770c50ca7c5384bd1037037b892b0ea` contains both documentation changes. The checkout was clean immediately afterward; no production source changed. Governing spec blob: `6c84a41957fcb56047b2259eb6dd79fc3fd6beb6`.

Executed against the unchanged production source at that revision:

| Command                                                                                                                        | Outcome                                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum --lib native_bindings::tests`    | 11 passed; 35.9 seconds including initial compilation                                              |
| `cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum --lib`                           | 238 passed; native startup, source validation, local stores and bootstrap included                 |
| `cargo test --offline --locked -p edgezero-cli --no-default-features --features cli,edgezero-adapter-axum --lib bindings_only` | 2 passed; structural validation without app TOML and shared validation/push errors                 |
| `bash scripts/check_native_store_dependency_contract.sh`                                                                       | Passed existing isolated AWS/native/WASM dependency assertions; no celld implementation exists yet |

The last three commands ran sequentially in 20.3 seconds total. Full command output is retained in the implementation conversation. No baseline failures occurred in these scopes. Broader all-provider/generated/runtime suites have not run for this task.

The local celld research checkout is clean at the required pin. `celld` is not on PATH and no executable was found in that checkout's usual debug/release locations or the user Cargo/local bin locations. Docker is available, but no daemon/image/service check, image pull, fleet setup or credential access was performed. Source availability does not satisfy the real-service gate.

The user subsequently resolved placement: implement inside celld with no separate gateway/service. An isolated celld worktree now exists at `/home/pav/worktrees/feat-scoped-store-api`, branch `feat/scoped-store-api`, based on the researched pin. The first embedded implementation and local proof are recorded below. No production deployment, real fleet/AWS access or additional service was used.

## Embedded celld implementation checkpoint

The initial celld implementation is committed as `f72ae21a515859570622ac9d6ca9c19cf6870b81` on `feat/scoped-store-api`; the celld worktree was clean. At this checkpoint, EdgeZero changes after `a12265b5` were documentation-only; the later provider checkpoint follows below. Do not label the initial listener as a released celld capability. The clean celld repository was moved to `/home/pav/Projects/celld` and its worktree metadata repaired so the implementation branch does not depend on temporary Git storage; `/tmp/edgezero-celld-research` remains a compatibility symlink with unchanged source contents.

Implemented files:

- `crates/logic/scoped_store.rs`, its explicit integration test and module/manifest registration: std-only exact alias/principal/operation policy, validated once, actually called by the HTTP handler.
- `crates/celld/scoped_store.rs` and library export: strict capped JSON policy capture, token verifier checks, native TLS configuration, closed errors, HTTP operations, binary values and scope-bound key-page cursors.
- `crates/celld/main/store_api.rs`, `main.rs` and `main/cli.rs`: disabled-by-default third listener, startup file selection, admission accounting, existing owner dispatch/output gates, body/operation/connection budgets and shutdown socket closure.
- `crates/celld/generation.rs`: required reserved KV registration during enabled generation construction/reload, plus local built-in KV-only bootstrap without application source. The existing generation host requires a default fetch; the built-in 404 is not a KV bridge and scoped traffic never passes through it.
- `crates/celld/main/peer_tunnel.rs`: explicit no-replay policy, preserving legacy behavior for existing callers. The regression applies a pooled request then loses its reply and proves no fresh attempt in the scoped mode. Outer dispatch also stops after a failed scoped attempt.
- `crates/celld/tests/scoped_store_startup.rs`: real local binary, cleared environment, isolated storage and owned child processes. Explicit TLS proof uses disposable certificate material; slow expiry proof uses actual elapsed time.
- `docs/services/scoped-stores.md` and `docs/security.md`: embedded deployment/configuration and protocol, with remaining acceptance limits disclosed.

Two read-only investigations identified the peer-pool replay hazard and missing reserved-runtime registration before edits. The coordinator inspected their evidence. Zellij follow-up delivery failed, so worker assignments were not treated as acknowledged transfers; their processes were released and the coordinator completed the coupled implementation locally, preserving and inspecting the partial HTTP/config scaffold.

Build preparation fetched the exact locked dependencies without changing the lockfile. The initial offline check had failed because the checkout lacked cached packages. All celld checks use installed Rust 1.98.1, not the older global default.

Executed checks:

```sh
cargo test --offline --locked -p celld-logic --test scoped_store
cargo test --offline --locked -p celld --all-targets
cargo test --offline --locked -p celld --test scoped_store_startup -- --ignored --nocapture
cargo clippy --offline --locked -p celld -p celld-logic --all-targets -- -D warnings
cargo fmt --all -- --check
```

The proof inventory is six pure-policy tests, seven actual HTTP-handler/config tests, one pooled replay regression, one ordinary real-binary test, one explicit native TLS test and one explicit real TTL test. All 17 passed. The two ignored acceptance tests were explicitly selected and both passed; they were not counted as passing skips. Strict all-target Clippy and workspace formatting checks passed. The final native/test/lint/format batch took 84.7 seconds, including the 61.4-second real expiry proof. Tested code fingerprint: `e909661efdf160a14ec8f62ffc224ff5fb590d921fc1dd635140dd08585ed55b`, covering 182 tracked/untracked workspace source/manifests. It is SHA-256 of compact JSON containing sorted pairs of repository-relative path and file SHA-256 from `git ls-files --cached --others --exclude-standard crates Cargo.toml Cargo.lock`; documentation-only changes do not alter it. The TLS certificate fixture initially failed certificate validation because it was marked as a CA; the corrected fixture is an end-entity certificate and normal certificate checking remains enabled. An initial KV-only generation without a default fetch also failed; the engine's default-fetch requirement is now explicit in the built-in bootstrap rather than hidden in an application script. The 61-second expiry test observes expiry, renewal beyond the original expiry and plain overwrite removing expiry.

Remaining gates: deployment-level network/credential isolation, multi-node owner forwarding and reload acceptance, production persistence/replication and fault/shutdown/body-budget stress. The local standalone path emits `local reload requires replication`; it has not been claimed as a fleet recovery proof or silently suppressed. Celld's private conformance/simulation harness is not available in this checkout and has not run. No larger-value bucket, failover, upgrade or performance acceptance is claimed. Complete remaining scoped-access gates before treating Stage 2 as fully passed or adding stable EdgeZero provider tags.

## EdgeZero provider integration checkpoint

Implemented, still unreleased:

- `edgezero-store-celld` with settings-only availability, independent `config`/`kv` features, captured redacted tokens, bounded reqwest transport, immutable config snapshots and live KV. Public core traits and non-`Send` futures are unchanged.
- Per-variant native capabilities, typed config/KV credentials, separate AWS/celld preparation sessions and policies, provider-specific accounting and complete pre-I/O validation. Existing constructors, AWS policy methods, local roots, supplied handles and startup staging remain.
- Default-off Axum/generated feature forwarding, structural-only CLI validation and selected remote config-push refusal. Provider availability remains unknown offline.
- Public-runner and generated-consumer fixtures, TLS certificate and proxy tests, no-replay fault cases, dependency isolation checks and CI wiring. The new [native celld guide](../../guide/native-celld-stores.md) documents deployment constraints.
- The mixed public-runner regression loads celld config, resolves an AWS Secrets Manager reference through the real pinned SDK and serves a separate local environment secret. Its celld document exceeds the independent 128-byte AWS snapshot quota. An unrelated Agent token must not enter the trusted celld request.

The coordinator reviewed the workers' source-only handoffs, registered the test binaries and owned all Cargo commands. Worker formatting was never counted as compilation or behavior proof. Initial type/lint failures were fixed before the results below. One cancellation test initially cancelled before HTTP admission; it now observes an admitted stalled request before dropping the future. Config-document and live-KV value caps are independent and each retains the service ceiling.

### Executed checks

EdgeZero uses Rust/Cargo 1.95.0. All Cargo checks below used `--offline --locked` after the coordinator updated the lockfile offline. The lockfile adds only the new workspace package and dependency edges; existing registry versions did not move. Fixture TLS server dependencies are native dev dependencies.

| Check                                                                           | Result                                                                   | Runtime / log                                                                                       |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| Provider settings-only / config-only / KV-only / combined units                 | 4 / 27 / 30 / 38 passed                                                  | Runtime suites about 2.9 seconds each; `/tmp/edgezero-celld-provider*.log`                          |
| Independent provider feature Clippy, all targets                                | Passed in each mode                                                      | `/tmp/edgezero-celld-clippy-{settings,config,kv,provider}.log`                                      |
| Axum native-only / config-only / KV-only units and lib Clippy                   | 250 passed in each mode; Clippy passed                                   | `/tmp/edgezero-celld-native-{axum,celld-config,celld-kv}.log`                                       |
| Combined AWS/celld native units                                                 | 253 passed                                                               | `/tmp/edgezero-celld-combined.log`                                                                  |
| Public celld startup subprocesses                                               | 4 passed                                                                 | `/tmp/edgezero-celld-native.log`, repeated in combined run                                          |
| AWS startup and mixed celld/AWS/local subprocesses                              | 5 AWS-only and 6 combined passed                                         | 0.30 seconds runtime each; `/tmp/edgezero-celld-{aws-only,mixed-startup}-final.log`                 |
| Default CLI library                                                             | 233 passed                                                               | 0.12 seconds runtime; `/tmp/edgezero-celld-cli-default.log`                                         |
| Native dependency and provider-root feature isolation                           | Passed                                                                   | `/tmp/edgezero-celld-dependencies.log`                                                              |
| Generated celld consumer, five feature combinations                             | Passed; one explicitly selected ignored test                             | 16.40 seconds; `/tmp/edgezero-celld-generated-combined-final.log`                                   |
| Real embedded celld, generated native TLS consumer and namespace isolation      | Passed; one explicitly selected ignored test, including 61-second expiry | 64.48 seconds consumer runtime; `/tmp/edgezero-celld-real-final.log`                                |
| Existing generated AWS / DynamoDB fixtures                                      | Both passed, one selected test each                                      | 36.83 / 14.63 seconds runtime; `/tmp/edgezero-celld-{aws,dynamodb}-generated-regression.log`        |
| Provider integration runner Node tests                                          | 24 passed, including 9 celld tests                                       | 1.19 seconds; `/tmp/edgezero-celld-node-all.log`                                                    |
| Workspace formatting and all-target/all-feature Clippy                          | Passed                                                                   | Clippy 47.41 seconds; `/tmp/edgezero-celld-{fmt,clippy-workspace}.log`                              |
| Workspace all-target tests                                                      | 2,028 passed, 6 ignored across 34 targets                                | `/tmp/edgezero-celld-workspace-tests-final.log`                                                     |
| Workspace all-target check with Fastly/Cloudflare/Spin, then Spin WASI P2 check | Both passed                                                              | 16.55 / 19.53 seconds; `/tmp/edgezero-celld-{workspace-check,wasm-check}.log`                       |
| Docs offline dependency preparation, lint, format and site build                | Passed                                                                   | `/tmp/edgezero-celld-docs-*.log`; process specs/plans remain outside the site formatter's match set |
| Shell syntax and `git diff --check`                                             | Passed                                                                   | Both new/extended shell scripts checked                                                             |

The first whole-workspace test attempt ran in a Zellij TTY and blocked in the existing non-TTY consent test. Its stdin was confirmed as `/dev/pts/2`, and the test source explicitly assumes noninteractive CI. Only that owned test process and its waiting scheduler were stopped. The rerun used `exec </dev/null` and passed. No production fix or test exclusion was used. An earlier whole CLI-library run with only Axum registered also failed existing Spin-dependent tests; the default-feature CLI suite passed instead. Minimal-feature runs remain focused on the celld generated integration target.

The only executable-code changes after the broad checkpoint were two test additions and their CI wiring: the generated AWS-coenabled build and the mixed public-runner case. Other changes were documentation. Focused Clippy and executed regressions passed for both additions. Production inputs stayed unchanged, so unaffected broad results were reused.

### What the real command proves

The tested celld source remains clean at `f72ae21a515859570622ac9d6ca9c19cf6870b81`, built with Rust 1.98.1. Executable SHA-256 was `e7afc70cb7a5ae0149ee51b9bd5933f2e0c1185b5fea2fad8d14ba26f5af91a9`; the runner hashed it again after acceptance and required the same value.

`scripts/run_native_celld_integration.mjs` requires an absolute `CELLD_BIN`, checks the embedded API flag, preserves the selected Rust toolchain, excludes ambient deployment credentials/proxies/trust overrides from its command environment and owns process groups. Missing prerequisites fail, never pass by skipping. The Rust subprocess fixtures separately clear app/service environments and use only disposable data and dummy credentials.

The generated target exercised verified TLS, binary and empty KV, existence, prefix pagination, a real 60-second TTL, runtime calls after the 30-second preparation deadline, immutable startup config, operator publication and fresh-process adoption. Both the app and celld were restarted against the same disposable storage; config and non-TTL KV survived. Invalid credentials failed before listener readiness. The deterministic generated case additionally exercises delete, missing/invalid-UTF8/empty/invalid-typed config failures and builds neither feature, each feature, both, and both with all AWS features.

The separate namespace proof ran in private user/network/mount/PID namespaces. It observed allowed KV access and denial of config writes/unknown aliases, internal-listener access, service policy/credential files and server procfs visibility. It used a dummy fleet credential sentinel. This is a disposable arrangement, not the generated TLS app's filesystem layout or a production deployment certification.

Provider fault fixtures directly observed applied PUT/DELETE with lost replies on fresh and pooled connections, and timed-out applied PUT. Each retained its effect and received one request. TLS fixtures cover a trusted valid leaf, unknown CA, wrong SAN, expiry and distinct credentials on reused connections. Proxy/redirect sentinels and an oversized HTTP-header response passed. The service's previous direct handler/authorization and no-replay results remain in the embedded checkpoint above.

Final source-input fingerprint: `06d6e171ab3612ffaedff6b9e65fd8e38871fd3028c0807d76074d5723a6b130`, covering 289 files. It hashes compact JSON of sorted path/file-SHA pairs from `git ls-files --cached --others --exclude-standard crates Cargo.toml Cargo.lock scripts .github/workflows/test.yml`. This includes test fixtures and CI, excludes generated binaries and site/process docs, and is not a build attestation. The broad-checkpoint fingerprint before the final test-only additions was `eb2d91ea3790066400ef9bdd3d933b58d4df4f61896583b50a02a503b083b134`. Logs under `/tmp` are local artifacts, not durable CI evidence.

### Remaining acceptance

- No released/publicly available scoped celld artifact or pinned real-service CI job. The explicit local command is available and executed; default CI uses deterministic fixtures.
- No multi-node owner-forwarding, reload, failover, production replication/persistence, upgrade or sustained fault/shutdown/body-budget acceptance. The private celld conformance harness is absent.
- No production deployment isolation claim. The generated TLS and namespace proofs are separate arrangements.
- No combined generated app with live celld, AWS and local data bindings, nor generated two-KV-store isolation/sharing case. The mixed public runner, source-plan cases, provider/service isolation cases and AWS-coenabled generated build are executed evidence at their stated levels.
- No bucket-backed large-value acceptance. The scoped v1 raw ceiling is 1 MiB and rejects larger values.

The local celld reload-without-replication warning remains disclosed. No real AWS/fleet credentials, production deployment, push or new commit was used for this EdgeZero checkpoint. Owned services were reaped and temporary check panes closed.

## Draft publication preparation

The user subsequently authorized committing the EdgeZero changes and opening a draft PR. The PR is stacked on #414, targeting `issue-410-native-store-providers`, not main. The companion celld branch remains local and is a separate publication dependency.

Pre-publication locked metadata checks found stale `examples/app-demo/Cargo.lock` and `tests/fixtures/reusable-app/Cargo.lock`. Both were refreshed offline with only the new settings-only provider package and adapter edge added. No existing registry versions changed. Both locked graphs then passed, and `cargo check --offline --locked --manifest-path tests/fixtures/reusable-app/Cargo.toml -p fixture-axum` passed in 2.78 seconds using the shared native target directory. Full demo workspace tests and all three provider WASM target builds remain unexecuted for this publication step. These consumer lockfiles are outside the source fingerprint above.

## Related

- [Native celld provider spec](../specs/2026-10-15-native-celld-store-providers-design.md).
- [Native provider composition design](../specs/2026-10-07-native-store-providers-design.md).
- [Native provider implementation plan](2026-10-07-native-store-providers.md).
- [DynamoDB KV implementation and proof](2026-10-14-dynamodb-kv.md).
