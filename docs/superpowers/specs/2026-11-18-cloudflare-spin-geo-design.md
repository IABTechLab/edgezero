# Cloudflare and Spin geographic context

## Requirements and existing implementation

Issue #480 requires country and first-level region through `ctx.geo()` on Cloudflare and Spin, with Axum's CloudFront selection driven by EdgeZero's manifest rather than Trusted Server configuration. Fastly preserves its existing geo. This supersedes the earlier Cloudflare-only proposal following scope clarification.

Stack on PR #502 (`spec/482-axum-geo-manifest-binding`, initial head `8cd7af96e94778df359e7ffed05be595fbfd901f`). It already implements Cloudflare platform metadata, Fastly geo, and Axum manifest binding. Reuse that code and test the combined acceptance. Do not import unrelated later #439 changes or implement a second copy on main.

## Cloudflare

Keep automatic platform selection. Read `cf.country` and `cf.regionCode`, never a client header or descriptive `region`. Preserve normalization, country-only results when subdivision is absent/unusable, and selected `MissingMetadata` for absent/unusable country including XX/T1. Strengthen managed contracts with independent field expectations, conflicting header values, and sequences through `run_app` and retained-app `dispatch_app`.

## Spin source and configuration

Spin has no native geographic object. Choose the issue's trusted-ingress option: a deployment-controlled fronting proxy supplies geographic assertions and authenticates them with a dedicated shared credential. This avoids adding asynchronous IP lookup, a dataset, or a provider vendor.

Introduce `edgezero_adapter_spin::geo::SpinGeoIngress`, a validated non-secret configuration value enabled by inserting it into App state from `Hooks::configure`. Its constructor takes the Spin variable name containing the credential. The dedicated headers are fixed: `x-edgezero-geo-country`, `x-edgezero-geo-region`, `x-edgezero-geo-token`. Configuration stores only the variable name; credential bytes never enter App state, extensions, diagnostics or normal callback views. An app without this configuration remains `NoSelection`. This requires no change to Axum's manifest schema or existing provider trait.

The normal Spin `run_app`, `run_app_with_platform`, and `dispatch_app` paths consume this state automatically through their shared request dispatch. Manual managed dispatcher paths with the configured App also receive the same treatment. Unmanaged request conversion retains its established contract and is not an authentication entry point.

Spin client-address headers are not a portable authentication source: pinned Spin runtime code retains user fields alongside generated metadata, and some runtimes omit client address entirely. Do not use the existing `.get("spin-client-addr")` as proof of ingress identity.

## Admission and credential handling

1. Privately capture at most one token, rejecting duplicate, empty, malformed or oversized values. Strip every token header from the managed request, even when geo is off. Validate original normalized header budgets so removing the token cannot bypass size/count limits. Admission callbacks receive a newly constructed sanitized head; the original head snapshot must not survive into them.
2. Keep no body read, secret lookup or geo resolution before successful request admission. Refused/aborted ingress retains its existing lifecycle.
3. With geo enabled, missing/invalid/duplicate token means selected `UntrustedIngress`, with no secret lookup. Otherwise asynchronously load the configured Spin variable under the admitted read deadline and the existing bounded-secret mechanism. Expected credentials must be 32–128 ASCII token bytes; use synthetic credentials only in tests. Lookup failure, missing variable, invalid credential configuration or deadline expiry produces a bounded provider failure outcome, never a permissive fallback or exposed backend error.
4. Compare equal-length validated credentials with the existing constant-time dependency. Length is not secret; do not claim memory zeroization or host allocation limits that the SDK cannot provide.
5. Only after matching the credential, capture bounded country/subdivision assertions. Identical or conflicting duplicate geographic fields, oversize, malformed, or subdivision-without-country assertions are invalid. Both fields absent means selected `MissingMetadata`; valid country with absent subdivision is country-only. Reuse existing CountryCode/SubdivisionCode/GeoAssertion semantics. Invalid authenticated assertions yield `InvalidAssertion`, preserving the distinction from a missing platform estimate.
6. Pass one owned outcome through `dispatch_admitted_with_geo` before router middleware/handlers. Public extensions or mutations of header fields cannot change it. Use truthful provenance `SpinIngress` and `AuthenticatedIngressHeader`, with a documented provider label `spin-ingress`; this does not claim Spin platform geolocation or a verified network topology.

Keep APIs minimal: fixed headers, one variable-name selection, existing secret lookup/timer machinery, existing admitted outcome handoff, no new I/O-capable provider trait. Core source/trust enums are already non-exhaustive; their extension must not broaden platform-specific constructors to fabricate Spin platform provenance.

## Deployment contract

The upstream proxy must obtain country/subdivision from its own trusted platform metadata, remove every viewer-supplied geographic/token header, replace assertions, and inject a high-entropy credential from secret storage. Proxy-to-Spin transport must be protected with TLS or an isolated private loopback/network; the credential is a bearer secret. The origin must not forward/echo the credential, and proxy logs must redact it. Rotate through coordinated proxy/variable updates; this version supports one active credential and makes no zero-downtime rotation promise.

Document both the supported header/variable binding and an example using a fronting platform's native geography. This is operator-configured ingress support, not automatic stock Spin geography or an IP geolocation service. Required local verification proves this mechanism through a disposable proxy and real Spin runtime. Actual hosted proxy configuration, metadata accuracy, transport isolation and Trusted Server adoption require separate deployment qualification.

## Acceptance and preservation evidence

- Cloudflare built Worker: US/CA, country-only, missing/XX/T1, header spoof rejection, request isolation, ordinary and missing routes, through the actual normal entry point under host-controlled metadata.
- Spin built component and local proxy: same country/region consumption through normal `run_app`; direct/missing/wrong/duplicate credential cannot obtain Located; authenticated malformed/duplicate assertions fail; token invisible to admission/middleware/handler; original header budgets still apply; private outcome survives extension tampering; off mode stays NoSelection; repeated requests and ordinary body/routes preserve behavior. Distinguish fixture platform metadata from deployed geolocation.
- Axum: execute the existing production-hosting manifest-selection test demonstrating country/region without a Trusted Server profile. No Axum production changes are planned.
- Run required Rust format, strict Clippy, workspace tests, feature compilation and Spin WASM check, plus Cloudflare WASM lib/contracts and affected Spin native/WASM/runtime suites. Documentation lint/format/build use pinned Node.
- Fresh independent design/plan, implementation and final self-reviews must reconcile all actionable blocking and optional findings. Create a signed, GitHub-verified draft PR against #502 after the required local gates pass. Closing #480 is justified only when all above acceptance evidence is established; otherwise retain the missing scope explicitly.

## Alternatives

CIDR admission based on `spin-client-addr` would depend on individual runtimes' treatment of user-supplied duplicates and omissions. A credential gate avoids that portability dependency. PR [#454](https://github.com/IABTechLab/edgezero/pull/454), reviewed at `925fe019`, already provides a portable asynchronous KV resolver and managed Axum serving. A Spin KV integration can reuse that reader, but still needs trusted viewer-IP admission, a KV binding, preparation/readiness, lookup budgets and cache-lifetime integration. The synchronous GeoProvider remains assertion-only and must not perform I/O. The ingress option is appropriate when a deployment already has trusted geographic assertions; it does not claim to supply standalone Spin IP lookup.
