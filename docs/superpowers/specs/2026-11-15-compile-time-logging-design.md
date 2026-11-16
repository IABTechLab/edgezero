# Compile-Time Logging For Hand-Written Hooks

**Issue:** [#504](https://github.com/IABTechLab/edgezero/issues/504)

**Delivery:** [PR #275](https://github.com/IABTechLab/edgezero/pull/275), as explicitly
requested after approval of the shared-macro design. No separate follow-up PR.

**Status:** Inline design approved; independent spec and plan reviews passed.
The user approved proceeding with implementation in the November 16 conversation.
The shared macro and consumer documentation are implemented and locally validated;
the plan records executed evidence. Publication and independent current-head
acceptance are tracked in issue #504 and PR #275. No hosted qualification is implied.

## Objective

A hand-written `Hooks` implementation must obtain the same compile-time logging
configuration as `app!` without parsing a manifest at runtime, copying settings into
Rust, or generating manifest routes. Preserve all existing outbound and lifecycle
contracts in PR #275.

## Public API

Re-export one expression macro through `edgezero_core`:

```rust
use edgezero_core::logging_config;
use edgezero_core::manifest::ResolvedLoggingConfig;

// Inside a hand-written impl Hooks:
fn logging_for(adapter: &str) -> ResolvedLoggingConfig {
    logging_config!("edgezero.toml", adapter)
}
```

The first argument is a string-literal manifest path. Resolve it against the
invoking crate's `CARGO_MANIFEST_DIR`, with the same absolute/relative-path behavior
as `app!`. The second is an expression coercible to `&str`, evaluated exactly once.
Preserve ordinary function-argument temporary lifetimes, including
`String::from("fastly").as_str()`.
An optional trailing comma is accepted. The result is `ResolvedLoggingConfig`;
expansion must not return from the enclosing function or capture caller bindings.

Do not add an impl-item macro, metadata-only application wrapper, new configuration
type, trait method, compatibility alias, or dependency.

## One Compiler Pipeline

Both entrypoints use one compiler-side manifest reader/parser/validator/finalizer
and one logging lookup/literal emitter. Extract only the helpers needed to share
those existing operations. Do not duplicate TOML interpretation or logging policy.

The logging-only macro stops before route/middleware symbol processing, app/router
generation and baked-JSON serialization. Schema-valid manifests can name handlers
or middleware that are not Rust symbols in the consuming crate; logging access must
not require resolving them. Shared manifest schema validation still applies.

Emit Rust configuration literals and case-insensitive lookup, not a runtime parser
or deserializer. Include an unused `::core::include_bytes!` dependency marker inside the
expression block so a manifest-only edit invalidates the compilation. Generated
code must not call `ManifestLoader`, `Manifest::from_baked_json`, TOML/JSON parsing,
or filesystem APIs. Returning an owned endpoint string can allocate, exactly as
`app!` does today; this is not an allocation-free lookup promise.

Preserve the existing resolution rules:

- A specified `[adapters.<name>.logging]` section takes precedence as a whole over
  `[logging.<name>]`; do not introduce field-by-field merging.
- Empty/partial sections behave exactly as the current manifest finalizer defines.
- All logging levels, endpoint and `echo_stdout` values retain their current types.
- Adapter lookup is ASCII case-insensitive, including custom adapter names.
- An unknown/unconfigured adapter returns Info, no endpoint and no stdout override.
- Existing schema errors, case-folded duplicate rejection and effective Fastly
  endpoint validation remain compile-time failures with existing diagnostic detail.

Generated identifiers and constructors must be hygienic. Use a private mixed-site
adapter parameter in an immediately invoked, typed, noncapturing closure; this
preserves temporary borrows without heap allocation. Qualify core/Option/Default
paths, primitive types, trait conversions and the dependency macro so caller bindings/macros cannot change the result
or defeat rebuild tracking. `app!` must use the same emitted lookup and qualified
dependency marker after refactoring.

## Runtime Boundaries

The macro supplies metadata only. It does not initialize or replace loggers, set
facade/backend filters, change `owns_logging`, read runtime overrides, or alter
Fastly's bounded override behavior. Applications retain those existing obligations.

Do not change routing, fallible assembly, admission, completion resources, response
framing/suppression, native Image Optimizer execution, outbound preparation,
deadlines, batching, per-slot outcomes, caps, error types, provider pins or capability
claims. The pending attestation repair remains separate work in the same PR.

Demo and scaffold applications already using `app!` continue through the same shared
emitter; do not convert them to hand-written hooks merely to demonstrate this API.

## Acceptance And Evidence

1. A consumer depending only on `edgezero-core` can use the macro in hand-written
   hooks and keep its own routes, with no generated application type.
2. Compare all three logging fields against both `app!` and test-only manifest
   resolution for four built-in adapters, custom/unknown names, mixed casing, all
   levels, absent logging and empty/partial overrides.
3. Prove single evaluation, temporary-borrow arguments, no enclosing-function return,
   nested-module use, repeated/multiple-manifest isolation, trailing commas and
   caller-name/macro hygiene.
4. Missing/unreadable/non-UTF-8 inputs, malformed TOML, invalid schema/levels and
   relevant endpoint errors fail at compile time, not as proc-macro panics.
5. A temporary consumer builds and executes with the expected values. With its
   Rust sources unchanged, mutate only the manifest and rebuild in the same target
   directory; observe the new values. A later invalid edit must fail compilation.
   Execute the built binary without the manifest present to exclude runtime reads.
6. Assert expansion excludes runtime parsing and route generation. Reuse the
   existing Rust compilation-test patterns; add no script, test framework or CI job.
7. Document the API in all four adapter guides, update the hand-written-hook
   example and changelog, and retain the complete prescribed validation matrix.

Passing compiler/local tests is not hosted lifecycle or resource qualification.
Issue closure and PR readiness require pushed implementation and independent review;
neither follows from this specification's approval alone.
