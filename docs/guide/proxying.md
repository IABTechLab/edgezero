# Outbound HTTP

EdgeZero exposes one provider-neutral HTTP client to handlers. Adapters inject an `HttpClient`
into request extensions, while application code builds `OutboundRequest` values and receives
typed `OutboundResponse` or `EdgeError` outcomes.

## Hard-Cut Adoption

Replace an application's transport layer rather than wrapping it around EdgeZero or keeping
raw-SDK fallback clients. EdgeZero owns transport dispatch, backend registration, response
normalization, transport decoding, bounded buffering and adapter delivery. Applications retain
target authorization, signing, image-profile selection, content rewriting and scheduling policy.
An application-owned rewrite codec is not a second outbound transport.

The current client already provides indexed batch outcomes, partial-result preservation on
driver failure, observation cutoff and cancellation, per-slot elapsed time, typed errors and
independent response limits. These contracts remain requirements of consolidation, together
with encoding policy, original-header provenance and ingress/egress ownership. Simplifying
internals does not remove them or upgrade the published target capabilities.

The head-first client contract replaces the old implementer `send` and raw-vector batch APIs;
there are no compatibility wrappers. Core captures invocation time, validates support before body
polling, prepares requests and finalizes provider heads. Adapters retain I/O, scheduling and
transport ownership. Eager work and nonblocking batch observation use that same pipeline.
Explicit redirect following uses that same pipeline with an application-supplied target policy.
Hosted qualification remains separate; applications must gate adoption on the behavior and
target evidence they require, not maintain another SDK client or supply lazy futures as eager work.

Header-policy consolidation is active: all four adapters pass `OutboundHeaderInput` to the shared
`OutboundHead` constructor. Adapter authors migrate the retired separate header
helpers as described in [response provenance](./capabilities.md#outbound-content-type-provenance).
Ordinary handler, demo and template request APIs are unchanged by that consolidation.

The facade calls synchronous `validate_supported_options` before common method/body/policy
validation, budget rejection, timer creation, source polling or provider I/O. Implementers receive
opaque prepared contexts; delete local preparation and budget selection. Buffered Batch
restrictions remain. This does not change indexed results, partial failures, observation cutoff,
per-slot timing, independent caps, coding/provenance, application clocks or lifecycle ownership.

### Paired Clocks And Timers

Standard entrypoints pair the application's monotonic clock with their runtime's cancellable
timer and inject that client. Demo and generated apps use this path unchanged. Custom client
construction now takes `OutboundTime`: Axum uses `try_with_time`, while Cloudflare, Fastly and
Spin use `with_time`. The old clock-only constructors are removed, not deprecated aliases.

`OutboundTime::new(clock, factory)` requires a factory that returns an owned local wakeup future
for the supplied absolute deadline and clock domain. A fake clock must wake its pending timers
when advanced. Merely returning a permanently pending future or advancing time without waking
does not implement that contract. Early successful wakeups are rearmed against the original
deadline; operational timer errors propagate unchanged and do not become request timeouts.

Timers are checked before source polling, retained while pending and cancelled with their
exchange. Native transport total timeouts do not override the supplied pair. Fastly uses its
existing progress multiplexer for timer-only waits as well as transport readiness. Provider
phase timers, synchronous host calls and the published BestEffort limitations remain separate;
this integration is not a hosted capability promotion.

### Operation Ownership

All four adapters use one core scope implementation. Canonical entrypoints establish a callback
owner before application preparation and dispatch. Send futures, begun exchanges and batches
capture that owner at invocation, including never-polled futures. Returning, failing or supported
unwinding revokes complete live work even when an application retains its public handle. Later
observation reports typed Internal scope closure, not a timeout, cutoff or clean EOF. Already
recorded batch partials retain their existing failure and accumulation semantics.

Canonical send helpers reuse an outer callback owner. Without one, they establish an owner
through preparation and delivery and release it when the helper returns. Batches started while
polling a response body have a separate owner released when that transmission ends, including
source/write failure. Earlier callback work remains live, and post-delivery work uses the same
still-active callback. Retained clients resolve ownership on each invocation; they do not
retain the previous request's scope. Polling a callback-created batch inside a body does not
transfer its ownership.

Fastly's callback progress driver services scheduled Begin and Batch pipelines, including head
processing and bounded collection, while a handler or response source awaits other work. Captured
scope and runtime activation are installed and restored per poll, never held across `.await`.
The old private Fastly lifetime registry is removed, not used alongside core ownership.

Adapter implementers that spawn local work use the core `OutboundTask::new` runner/owner pair.
Registration is fallible before spawning. Dropping either side releases the producer even if
the other handle is retained; adapters supply only their provider spawn and abort operations.
Completion receivers observe already-scheduled work. `is_ready` and batch `check` do not drive
the SDK, create timers or read transport bodies.

Successful streamed collection transfers transport ownership into the outbound body's lifetime;
callback exit cannot abort that handed-off body. Downstream response egress has its own body scope
through terminal transmission or Drop. EOF releases its source but does not end that scope. Its
terminal transition releases response-owned SDK work before invoking completion callbacks, without
cancelling work owned by a still-active serving callback.

To start work during the handler that must continue after its response is returned, choose its
body ownership **at creation**, then attach that exact owner to the returned response:

```rust
let body_owner = OutboundScopeOwner::response_body();
let batch = body_owner.with(|| client.start_batch_until(requests, observation_cutoff));
let origin = client.send(origin_request).await?;
let response = response_with_batch_stream(origin, batch)?; // Application body production.
body_owner.attach(response)
```

The owner is provisional until canonical egress accepts the response. Retaining an unattached
owner or an attached-but-unaccepted response cannot bypass callback cleanup. Acceptance happens
before response policy, framing, suppression or body polling. Callback and body lifetimes share
provider progress, but not cancellation roots; a body batch started before the origin remains
serviceable while the origin is awaited and after callback return. A failed application's body
work closes before bounded fallback delivery. No deadlines are restarted during handoff.

`Body` now has a private payload and optional local ownership metadata. Use `Body::from_bytes`
or `Body::from_stream`, not the removed enum constructors. Transport code can destructure
`BodyPayload` through `Body::into_parts`, retaining its returned owner until terminal egress.
This changes neither buffered representation nor byte copies. Outbound requests cannot reuse a
body with response ownership. Ordinary handlers need no manual owner; the explicit attachment
above is for work deliberately started early on behalf of a later body.

Manual clients outside request entrypoints remain caller-owned: dropping the operation, pending
handle or batch cancels its work. A manual caller requiring a group lifetime can use
an `OutboundScopeOwner::callback()` binding and keep it alive while awaiting `owner.run(...)`.
Normal handlers and generated apps need no scope
boilerplate. A closed canonical scope never falls back to caller-owned work. Existing deadlines,
caps, source errors and provider-side cancellation guarantees are unchanged; hosted qualification
remains separate.

## Following Redirects

`HttpClient::follow` is an explicit per-call choice with a mandatory target policy and hop limit.
`OutboundRedirectLimit::new` accepts only 1..=20 and has no default. At most limit plus one
requests are sent; for example, limit 4 allows five requests. Plain `send`, `begin`, and batches
never follow redirects. This section supersedes the historical outbound spec's instruction to
read `Location` and issue another request when using `follow`; plain sends retain that rule.

```rust
use edgezero_core::{OutboundRedirectAction, OutboundRedirectLimit, OutboundRequest};

enum TargetPolicyError {
    TargetDenied,
}

// The initial target is application-authorized before this call.
let request = OutboundRequest::get("https://assets.example.com/start")?;
let result = client.follow(request, OutboundRedirectLimit::new(4)?, |hop| {
    let Some(next) = hop.prepared_request() else {
        return Ok(OutboundRedirectAction::Stop);
    };
    if next.uri().scheme_str() != Some("https")
        || next.uri().host() != Some("assets.example.com")
        || next.uri().port_u16().is_some_and(|port| port != 443)
    {
        return Err(TargetPolicyError::TargetDenied);
    }
    Ok(OutboundRedirectAction::FollowPrepared)
}).await;
```

The initial target is not passed to the redirect policy: authorize it before calling `follow`.
The policy is the only target authorization policy. An open policy also follows private or
link-local addresses. There is no built-in SSRF filter; select schemes, hosts, ports and any
application-specific network restrictions deliberately. A platform host declaration is not a
substitute for application authorization.

### Methods And Replay

Exactly 301, 302, 303, 307 and 308 are followable. Every other status is final, including 300,
304, 305 and 306. Rules apply to the request that received the answering response:

| Status   | Method of answering hop | Next method                       | Next body |
| -------- | ----------------------- | --------------------------------- | --------- |
| 301, 302 | GET, HEAD               | same                              | empty     |
| 301, 302 | POST                    | GET                               | empty     |
| 301, 302 | any other method        | not followed: MethodNotRewritable | none      |
| 303      | HEAD                    | HEAD                              | empty     |
| 303      | any other method        | GET                               | empty     |
| 307, 308 | any method              | same                              | replayed  |

For example, PUT, PATCH, DELETE and OPTIONS stop on 301/302, become empty GET on 303, and retain
their method and replayable body on 307/308. Buffered bodies replay byte for byte through cheap
`Bytes` sharing. Streamed uploads cannot be replayed: a 307/308 is final with `BodyNotReplayable`,
without consulting policy. An unknown inbound GET/HEAD body is settled once before the initial
hop; EOF makes it replayable as empty, while a nonempty byte is rejected before dispatch.

Dropping a body removes `Content-Type`, `Content-Length`, `Content-Encoding`, `Content-Language`,
`Content-Location`, and `Expect` from the prepared request. They stay absent on a subsequent
307/308 unless the trusted policy deliberately supplies replacement content and metadata.
An empty buffered body replayed by 307/308 retains its `Content-Type`; replay is not a rewrite.
Ordinary validation still prohibits nonempty GET/HEAD bodies.

### Targets And Credentials

The decision order is followable status, Location presence, method, replayability, limit,
Location resolution, then policy. A missing or single empty Location yields `NoLocation`.
Original Location multiplicity and text eligibility are captured before filtering; duplicate or
non-text values fail resolution as typed 502 Protocol errors rather than becoming a valid
singleton after filtering. Provider-hidden original fields remain unavailable. Relative paths,
`../next` and `//other.example/next` resolve against the answering request URI, and fragments
never reach the wire. Invalid URL references, user information and unusable HTTP(S) targets are
Protocol errors. Earlier method/replay/limit stop reasons take precedence over resolution errors.

A resolved non-HTTP target reaches the policy with no prepared request. The policy may `Stop`,
reject, or `Follow(request)` with its own valid target. Asking for `FollowPrepared` without an
available prepared request, including after `take_prepared_request()`, is a Protocol error.

When the target changes scheme, host, or effective port, the automatic credential rule removes
only `Authorization`, `Cookie`, and `host_authority_override` before calling the policy. These
carry over on same-origin hops. Other credential-bearing fields, including custom API-key headers
and `x-amz-*` signatures remain: the policy must remove or replace them when necessary. It may
deliberately authorize or re-sign a replacement request; EdgeZero does not strip credentials a
second time afterward. Ordinary request validation and normalization still apply. EdgeZero does
not add `Referer` or `Origin`.

The prepared request retains every unchanged option: timeout, deadline, cache policy and tags,
upload/response/header/decoder/chunk limits, response decoding, proxy marker, Image Optimizer,
TLS verification, connect timeout and stream idle policy. Use `take_prepared_request()` to modify
it, then return `Follow(request)`. A fresh request is a complete replacement with its own defaults;
caps and options are not implicitly copied into it. Subsequent automatic preparation inherits
the effective preceding request's options, not the initial defaults.

### Budgets, Bodies And Outcomes

The per-call timeout starts afresh at each hop, while the initial absolute deadline remains an
independent chain-wide upper bound, including policy execution and final streamed body consumption.
Initial GET/HEAD settlement counts against that bound too. A replacement request cannot remove
or extend that original deadline; an earlier policy deadline may shorten a hop. For a selected
next request, unsupported options are rejected before an expired budget. No expired hop is sent
and no late final success
is returned. A synchronous policy cannot be preempted; its rejection value remains unchanged even
if time passes during the callback. An accepted action remains subject to the original deadline.

Every hop uses the same supplied client and shared preparation/head-finalization pipeline,
including custom clients. A followed response body is discarded unread: it is not decoded,
buffered, delivered or drained for reuse, and its declared length or source failure cannot fail
the chain. Response-header caps and body-independent validation still apply to every head.
The final response, including stopped or limit-reached 3xx, uses ordinary decoding, caps, body
mode and idle policy. Thus a final body may still fail; a stop reason never bypasses body policy.
With no redirect, `follow` has the same response and limit behavior as `send`.

`OutboundFollowResult` exposes `response()`, `final_uri()`, `redirects_followed()` and
`stop_reason()`, or transfers them through `into_parts()`. The stop reason is present exactly
for a final followable status: `NoLocation`, `MethodNotRewritable`, `BodyNotReplayable`,
`LimitReached`, or `StoppedByPolicy`.

`OutboundFollowFailure::Hop` preserves the hop's original `EdgeError` with its zero-based hop
index (initial request = 0). `OutboundFollowFailure::Policy` carries the application's rejection
with its one-based redirect number, unchanged. Neither the policy nor its error needs `Send` or
`Clone`, and no `Sync`, `Debug` or standard error trait is required. The policy is synchronous
`FnMut`; the future owns it. Dropping the future cancels the current hop as a plain send would
and sends no later hop. Framework hop logs include status, scheme, host, port and path, not queries.

Spin requires plain-HTTP origins and targets outside a narrowed host list to be declared. Fastly
hops wait one at a time and use the same backend identity policy as equivalent single sends.
On Fastly, supported optimizer hops use the same native Image Optimizer call path, with no extra
fetch, buffering or serialization. This does not strengthen its unknown blocking horizon,
deadline/cancellation guarantees or hosted performance qualification. The `Native` redirect
capability describes shared policy only; see the [capability matrix](./capabilities.md#outbound-matrix).

## Native Image Optimization

Fastly single sends can request native image optimization from portable code:

```rust
use edgezero_core::{OutboundImageFormat, OutboundImageOptimizerOptions,
    OutboundImageOptimizerRegion, OutboundRequest};

let options = OutboundImageOptimizerOptions::new()
    .region(OutboundImageOptimizerRegion::UsEast)
    .format(OutboundImageFormat::WebP)
    .width(100)?;
let request = OutboundRequest::get("https://images.example.com/photo.jpg")?
    .image_optimizer(options)
    .max_encoded_response_bytes(2 * 1024 * 1024)
    .max_decoded_response_bytes(4 * 1024 * 1024)
    .max_response_header_bytes(64 * 1024)
    .max_response_header_count(100)
    .stream_response()
    .timeout(std::time::Duration::from_secs(2));
assert!(request.image_optimizer_options().is_some());
// Send through the existing request-context HttpClient.
```

Only bodiless GET/HEAD and `PlatformDefault` caching are supported. Do not combine this with
cache bypass. Region is required; no default region or transformation is invented. Other
adapters and Fastly begun requests and batch slots reject `image_optimizer` before body polling
or dispatch. Explicit redirect following uses the same supported single-send mapping per hop.
Viceroy and services without the feature return typed `BadGatewayReason::ImageOptimizerUnavailable`
(502), retaining text-free provider detail. There is no software fallback or automatic extra fetch.

The SDK's optimizer call is synchronous and cannot be cancelled while executing. Its complete
blocking horizon is **unknown**: a timeout does not promise prompt return from that call. When
control returns, the original absolute deadline wins, including over a late successful transform.
Normal header/body limits, content-encoding policy and lazy streamed delivery apply to the result.
Hosted transformation and performance parity remain unqualified.

EdgeZero retains the built target and signature-bearing headers subject to normal filtering.
The provider separately interprets optimizer query parameters, with explicit options taking
precedence. `preserve_origin_query(true)` preserves only other query parameters at the origin;
it does not stop that transformation. Query stripping before signing, SVG selection, profiles,
authorization and any preflight requests remain application policy. Forwarded inbound GET/HEAD
settlement and explicit redirect following use the same shared dispatch pipeline; neither adds
an optimizer fallback or another provider fetch.

## Forward An Inbound Request

This example preserves the inbound method and body, replaces the target URI, removes a private
header, and converts the outbound response back into a core response.

On Axum, the converter preserves a transport-certified empty body (`is_end_stream()`) as buffered
empty after admission, for any method, without reading it. Actual HTTP/1 GET/HEAD forwards with
absent framing or `Content-Length: 0` succeed through this path even after the inbound read
deadline, with no outbound body bytes, `Transfer-Encoding` or `Content-Length`.

Unknown bodies remain lazy until `send` or `begin` settles a GET/HEAD body. Empty chunks do not
establish emptiness: only EOF does. The first nonempty byte returns 400 before origin contact.
Settlement races the captured outbound deadline against the admitted inbound read deadline,
retaining 408/504 provenance and original source errors without restarting the outbound budget.
Batch still requires buffered uploads; buffer under an application cap before adding a slot.
Do not discard an inbound body to bypass validation. Other methods retain streamed uploads.

Fastly separately preserves actual SDK body absence: only `try_take_body() == None` becomes core
`Body::empty()`, without creating an SDK body. Both direct conversion and admitted dispatch keep
every present SDK body lazy, even when `known_length()` reports zero; that query omits SDK buffers
and is not an EOF signal. Normal Fastly `from_client()` requests carry a body handle even when
bodiless, so their emptiness is settled rather than guessed. Fastly's synchronous inbound host
read is not preemptible; its read-deadline behavior remains BestEffort. Do not remove a present SDK
body to force the absence path. See [Fastly ingress conversion](./adapters/fastly.md#ingress-body-conversion).

Cloudflare preserves a native Fetch request's `body == null` as buffered empty without reading
it. This applies to direct conversion and, after admission, normal dispatch. Every non-null body
remains streamed, even an empty ArrayBuffer, a closed stream or a body with `Content-Length: 0`.
There is no ArrayBuffer-only fallback or header-based inference. See
[Cloudflare ingress conversion](./adapters/cloudflare.md#ingress-body-conversion).

The certified Axum empty body or absent Fastly/Cloudflare body remains Initial, not Cached. `take_body`
and `into_request` expose buffered empty, but core's first `body_bytes` call still checks the
inbound read deadline and can return 408. Ingress framing remains `HostManaged`. Demo and generated
proxy handlers share their adapter's converter and the common settlement pipeline. Neither
settlement nor normalized requests establish raw parser guarantees or promote target capabilities.

```rust
use edgezero_core::action;
use edgezero_core::context::RequestContext;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{HeaderValue, Response, Uri};
use edgezero_core::outbound::{OutboundCachePolicy, OutboundRequest, ResponseDecoding};
use std::num::NonZeroU64;
use std::time::Duration;

#[action]
async fn forward_with_auth(
    RequestContext(ctx): RequestContext,
) -> Result<Response, EdgeError> {
    let client = ctx
        .http_client()
        .ok_or_else(|| EdgeError::not_implemented("outbound HTTP client not available"))?
        .with_proxy_marker(false);
    let target: Uri = "https://api.example.com/v1/data"
        .parse()
        .map_err(|_| EdgeError::bad_request("invalid upstream URI"))?;

    let mut request = OutboundRequest::from_request(ctx.into_request()?, target)?
        .response_decoding(ResponseDecoding::Original)
        .cache_policy(OutboundCachePolicy::Bypass)
        .timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .max_request_body_bytes(256 * 1024)
        .max_encoded_response_bytes(2 * 1024 * 1024)
        .max_decoded_response_bytes(4 * 1024 * 1024)
        .max_response_bytes(4 * 1024 * 1024)
        .max_response_header_bytes(64 * 1024)
        .max_response_header_count(100)
        .max_chunk_bytes(NonZeroU64::new(64 * 1024).unwrap_or(NonZeroU64::MIN));
    request.headers_mut().insert(
        "authorization",
        HeaderValue::from_static("Bearer secret-token"),
    );
    request.headers_mut().remove("cookie");

    let mut response = client.send(request).await?;
    response
        .headers_mut()
        .insert("x-forwarded-by", HeaderValue::from_static("edgezero"));
    response.into_response()
}
```

Outbound failures remain typed. Deadline expiry maps to `GatewayTimeout { cause }`; transport,
protocol, and codec failures map to inspectable `BadGatewayReason` values; resource limits use
`ResponseLimitReason`. Applications do not need to classify errors by matching messages.

`EdgeError::provider_failure_detail()` borrows optional, fixed-size provider evidence separately
from that public classification. `ProviderFailureDetail` identifies the adapter and observation
phase and holds only closed causes, numeric codes and I/O kinds, never provider text, URLs,
headers or bodies. Spin DNS text recognizes only `NOERROR`, `FORMERR`, `SERVFAIL`, `NXDOMAIN`,
`NOTIMP` and `REFUSED`; every other spelling is `Unknown`, while absent input remains absent.
The `bad_gateway_with_detail`, `bad_request_with_detail`, `gateway_timeout_with_detail` and `internal_with_detail`
constructors leave Display, messages, statuses, existing reasons and framework wire bytes unchanged.
Detail survives body policies, buffered collection, retained body errors, pending handles and
batch failures. Framework code does not log it; application logging and correlation remain policy.
The existing send and response-body SDK boundaries now populate it on all four adapters. Fastly
also records backend-registration and upload I/O failures; Spin distinguishes send from request
completion. Axum retains reqwest predicates and searches up to 32 source entries for an I/O kind
(reqwest's own predicate walks have their own behavior). Cloudflare recognizes only `AbortError`
and `TypeError` names and never infers a timeout from a name; text-only SDK errors remain Unknown.
When an existing deadline check replaces an observed error, the selected timeout keeps its safe
detail. Timer-only expiry, late successful results and EOF do not invent provider evidence.
Cloudflare request construction and Spin header, timeout-option and target setters record
`RequestPreparation`. This phase describes the observed API boundary, not transport progress;
an error returned by an SDK send operation still has the Send phase, including a reqwest builder
error. Cloudflare's SDK erases constructor exception types, so those causes remain Unknown.
Spin preserves all five header-error categories and fatal first-byte/between-bytes setter
rejections, discarding `Other` messages. A connect-setting refusal is advisory for every
`RequestOptionsError`: it does not fail the request or become the cause of a later timeout.
The unchanged outer race still bounds guest-visible pending work by the aggregate deadline;
the host may retain an opaque phase default. Unit-returning target setters record component
rejection without fabricating an HTTP transport error code. `NotSupported` remains advisory
for all timeout options.
Shared response-header conversion is not labeled as request preparation, and local validation
does not invent provider evidence.

Response completion callbacks and observers receive the same immutable
`ResponseEgressReport::source_failure` snapshot. It retains only the first converter-observed
body-source error's fixed category, optional provider detail and observation-time body kind.
A first error without provider detail is still an observation; later errors cannot replace it.
This is not the terminal cause: deadline or cancellation may win after a source error becomes
ready, and an application-body observation remains tagged `Application` through fallback delivery.
Recording adds no clock samples, source polls, callbacks or wire diagnostics. Rendered application
errors delivered successfully, clean EOF, and transport-only failures do not invent source errors.
Precommit normalization rejections appear separately in `report.framing_failures.application()`
and `.fallback()`, each an optional `ResponseEgressFramingError`. The first rule wins per body
kind, so a failed fallback cannot hide the application rule or overwrite its own earlier rule.
Recording and fixed-text error-level logging are initial-only; no header values or body bytes are logged.
After writing or termination, normalization still returns its original result without recording a
precommit observation.
`EdgeError::response_framing_error()` also reads this enum when an internal error already owns it.
Policy, deadline, host-head and source failures do not invent framing rules. Postcommit stream
length failures keep their existing source categories, not a new precommit classification.

Exact report literals must add `source_failure: None` and `framing_failures: Default::default()`
when there is no observation. There is no old-shape compatibility API. Stock fallbacks are
framing-valid; invalid-fallback attribution is verified directly at the shared normalization
boundary, not claimed as an observed adapter-generated failure.
[`outbound-failure-diagnostics`](/guide/capabilities#provider-failure-diagnostics) reports Native
for Fastly/Spin SDK-cause projection and BestEffort for Axum/Cloudflare's reduced visibility.
Hosted qualification and the remaining outbound migration remain open; these diagnostics do not
strengthen transport guarantees.

`ResponseDecoding::Original` preserves encoded payload bytes and the representation headers
that survive ordinary normalization. It does not restore hop-by-hop or invalid fields. Use it
for relays; the demo and generated proxy handler do so. Other requests default to
`ResponseDecoding::Decode`, which decodes a single bare gzip, deflate or Brotli coding. All 206
responses retain original coding under either choice, so partial representation offsets stay
meaningful. HEAD/304 metadata and bodyless handling retain their existing behavior.

Original coding skips decoder allocation and the decoded-byte cap for encoded payloads, not the
encoded or final buffered caps. Identity bodies still obey the decoded cap. `Accept-Encoding`
is independent: a supplied value survives unchanged and an absent value defaults to `identity`.
`response_decoding_setting()` exposes the choice before dispatch; `json()` parses delivered
bytes and never implicitly decodes an original encoded body.

## Adapter Marker

`HttpClient::with_proxy_marker(false)` configures a relay handle once. Other handles keep the
default-on policy. `OutboundRequest::proxy_marker(OutboundProxyMarker::On)` or `Off` overrides
that handle; the request default, `Inherit`, uses its client setting. The demo and generated relay
turn the marker off without per-request stripping, while JSON fanout keeps the default on.

Inspect `client.effective_proxy_marker_enabled(&request)` before sending. This combines both
settings without validation, body polling or I/O; `proxy_marker_setting()` alone may still be
`Inherit`. Supplied client implementations receive a resolved `On` or `Off` setting.

When off, EdgeZero adds no synthetic `x-edgezero-proxy` field or header-budget charge. It does
not remove or overwrite an upstream field with that name; ordinary normalization still applies,
including adapter-visible Connection nominations. When on, the marker overwrites upstream values
and consumes the same byte/count budget as before. Direct adapter calls with `Inherit` use default on.

## Construct A Request

Use `OutboundRequest::get`, `post`, or `new` for a new request. `from_request` is intended for
forwarding an inbound request and performs the same target validation and hop-by-hop header
normalization immediately.

```rust
use edgezero_core::outbound::{OutboundCachePolicy, OutboundRequest};
use std::num::NonZeroU64;
use std::time::Duration;

let request = OutboundRequest::post("https://api.example.com/events")?
    .cache_policy(OutboundCachePolicy::Bypass)
    .timeout(Duration::from_secs(2))
    .max_request_body_bytes(256 * 1024)
    .max_encoded_response_bytes(2 * 1024 * 1024)
    .max_decoded_response_bytes(4 * 1024 * 1024)
    .max_response_bytes(4 * 1024 * 1024)
    .max_response_header_bytes(64 * 1024)
    .max_response_header_count(100)
    .max_chunk_bytes(NonZeroU64::new(64 * 1024).unwrap_or(NonZeroU64::MIN))
    .max_brotli_window_bits(24)
    .max_decoder_bytes(32 * 1024 * 1024)
    .header("accept", "application/json")?
    .json(&payload)?;
let response = client.send(request).await?;
let decoded: ApiResponse = response.json()?;
```

`host_authority_override("virtual.example:8443")` changes only the outgoing HTTP authority. The
request URI still selects the connection destination, TLS SNI, and certificate identity. Axum and
Fastly support that separation, Cloudflare is BestEffort pending a deployed wire probe, and Spin
rejects the override during preflight.

Buffered response mode is the default. Call `stream_response()` for a streamed response and
consume `response.into_body()` while its absolute request deadline is still active. Use
`into_bytes_bounded`, `into_bytes_bounded_until`, or `json_bounded_until` when application code
performs an additional collection step.

Each adapter checks its selected absolute budget again before returning a successful response,
including bodyless responses and streamed response heads. Equality is expired: setup and
normalization cannot hand off a late success or restart the budget. This check does not read a
lazy payload or replace an earlier header/source/limit error. It does not upgrade provider
interruption guarantees; blocked host operations retain their documented capability limits.

`max_chunk_bytes` opt-in rechunks guest-visible response items before they reach the application.
It bounds emitted item shape, not allocation: split `Bytes` values can retain their original
backing allocation, and a provider SDK may allocate the source chunk before EdgeZero receives it.

### Connect Timeout

`connect_timeout` is independent of the aggregate timeout/deadline and defaults to unset. Zero is
invalid; a longer connect setting cannot extend the aggregate budget. Keep it a configuration
constant: Fastly includes connect settings in stable backend identity, so different settings can
use different connection pools. First-byte time is not reduced by a fixed connect share.

Fastly maps the setting to its SDK; hosted expiry and provider DNS/TLS phase coverage still need
qualification. Spin passes it as an advisory WASI option and retains the aggregate race if the
host refuses it. Axum and Cloudflare accept it at BestEffort, retaining aggregate enforcement;
they do not create per-request clients to emulate an independent phase timer.

## Begin An Exchange

`begin` settles a permitted unknown GET/HEAD body, then schedules the complete exchange before
returning `OutboundPending`. It does not wait for connection establishment or response headers.
Other streamed uploads are rejected; use `send` for them. Buffered responses are collected under
their caps even while the caller is not collecting the pending result.

```rust
use edgezero_core::OutboundRequest;
use std::time::Duration;

let pending = client
    .begin(
        OutboundRequest::get("https://api.example.com/data")?
            .timeout(Duration::from_secs(2))
            .max_response_bytes(64 * 1024),
    )
    .await?;
let response = pending.collect().await?;
```

`is_ready()` is a nonblocking, non-consuming check; errors are ready too. Dropping or cancelling
a pending handle stops its owned work. Successful collection transfers a streamed body's transport
ownership, so dropping the completed pending handle cannot abort that body. The original deadline
continues to govern it. Axum manual eager clients require an active Tokio `LocalSet`; standard
server entrypoints already supply one. See target guides for scheduling and teardown limitations.

## Batch Requests

`start_batch_until` accepts buffered request bodies and buffered response mode only. It yields
terminal slots in observed completion order while retaining each original input index. This lets
an application keep completed results when its own observation cutoff wins:

```rust
use std::time::Duration;

use edgezero_core::{EdgeError, OutboundBatchNext, OutboundBatchTermination};

let observation_cutoff = ctx
    .monotonic_clock()
    .deadline_after(Duration::from_secs(2));
let mut batch = client.start_batch_until(requests, observation_cutoff);
let termination = loop {
    match batch.next().await? {
        OutboundBatchNext::Item(item) => {
            let (index, result) = item.into_parts();
            match result.outcome {
                Ok(response) => record_success(index, result.elapsed, response.status()),
                Err(error) => record_failure(index, result.elapsed, error),
            }
        }
        OutboundBatchNext::Finished(termination) => break termination,
        _ => return Err(EdgeError::internal(anyhow::anyhow!("unsupported batch event"))),
    }
};
assert!(matches!(
    termination,
    OutboundBatchTermination::Completed | OutboundBatchTermination::Cutoff
));
```

Anchor relative observation cutoffs through the request clock as shown. `Deadline::after` uses the
process default clock and is intended only for code that also uses the default clock. The
observation cutoff stops batch observation regardless of any remaining per-request budget or later
per-request deadline. Passing an earlier observation cutoff intentionally leaves unresolved slots.

After the last item, continue calling `next()` to await clean driver EOF and receive
`Finished(Completed)`. Item count alone is not completion: a trailing driver failure, duplicate
index, or out-of-range index still fails collection while retaining the previously collected
slots. Custom drivers must finish after their last item; zero-slot drivers are validated too.

`client.send_all_until(requests, observation_cutoff).await` collects the same driver into an
index-aligned `OutboundCollectedSlot` values. `Resolved` owns a newly collected result;
`AlreadyDelivered` marks an item previously transferred by `check` or `next`; `Unresolved` marks
a cutoff-abandoned slot. `Completed` forbids unresolved slots, while `Cutoff` requires one.
A premature, malformed or failed driver returns `OutboundBatchFailure` with its exact error and
partial position states. Collections and items have read-only accessors and consuming
`into_parts`; their fields and batch identities cannot be reconstructed or mutated:

```rust
let results = match client
    .send_all_until(requests, observation_cutoff)
    .await
{
    Ok(results) => results,
    Err(failure) => {
        let (error, slots) = failure.into_parts();
        for (index, slot) in slots.into_iter().enumerate() {
            if let edgezero_core::OutboundCollectedSlot::Resolved(slot) = slot {
                retain_terminal_slot(index, slot);
            }
        }
        return Err(error);
    }
};
```

Driver failure never degrades into cutoff-like missing slots. Ordinary terminal results are timed
from the batch method-entry snapshot through their own terminal observation, including preflight
and body buffering. If an earlier per-request deadline has already expired, the result is a timeout
at that logical deadline even when its observation arrives later; its elapsed value ends at expiry.
This exception preserves earlier own-timeout results across a later observation cutoff. Elapsed is
not pure network RTT. Bound the request count and every per-request body limit; EdgeZero
intentionally has no global batch-concurrency or memory cap.

`check()` observes without waiting. An accumulator obtained from `batch.accumulator()` can retain
intact checked items after inspection, then merge the final success or failure collection. Merging
validates batch identity and fills only `AlreadyDelivered` positions. Insert/merge failures return
the exact original owned inputs; consuming an item's or collection's `into_parts` relinquishes
merge eligibility. `cancel()` returns every never-delivered index, including recorded results
that the caller has not yet taken.

On Axum, Cloudflare, and Spin, each child samples its terminal instant synchronously when it is
observed ready. Those samples follow poll-observation order in the shared monotonic clock domain,
so after one observation reaches the observation cutoff a later observation cannot be on time
unless an injected clock moves backwards. Fastly's provider-selected completion path remains
BestEffort as documented in the capability matrix.

## Platform Behavior

The API is portable, but not every runtime can provide identical timing, header, or lazy-stream
semantics. See [Capabilities](/guide/capabilities) for the exact support matrix, Fastly's dynamic
backend prerequisite, each adapter's lazy downstream handoff boundary, Cloudflare's manual
encoding boundaries, and complete resource-accounting exclusions.
