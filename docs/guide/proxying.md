# Outbound HTTP

EdgeZero exposes one provider-neutral HTTP client to handlers. Adapters inject an `HttpClient`
into request extensions, while application code builds `OutboundRequest` values and receives
typed `OutboundResponse` or `EdgeError` outcomes.

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
adapters and Fastly batch slots reject `image_optimizer` before body polling or dispatch.
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
settlement and optimizer redirect following are not implemented by this checkpoint.

## Forward An Inbound Request

This example preserves the inbound method and body, replaces the target URI, removes a private
header, and converts the outbound response back into a core response.

At this implementation checkpoint, use it only for methods that carry a body. Forwarded inbound
GET/HEAD requests still carry lazy bodies and fail with 400; bounded empty-body settlement is not
implemented yet. Do not discard an inbound body to bypass that check.

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
Spin preserves all five header-error categories and the rejected timeout option, discarding
`Other` messages. Unit-returning target setters record component rejection without fabricating
an HTTP transport error code. Unsupported timeout options remain advisory successes.
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

`max_chunk_bytes` opt-in rechunks guest-visible response items before they reach the application.
It bounds emitted item shape, not allocation: split `Bytes` values can retain their original
backing allocation, and a provider SDK may allocate the source chunk before EdgeZero receives it.

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
        OutboundBatchNext::Item(item) => match item.result.outcome {
            Ok(response) => record_success(item.index, item.result.elapsed, response.status()),
            Err(error) => record_failure(item.index, item.result.elapsed, error),
        },
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
index-aligned `Vec<Option<OutboundSlotResult>>` plus `results.termination`. `Completed` guarantees
every slot is present. Only `Cutoff` permits `None` in a successful result, meaning that slot was
unresolved when the observation cutoff won. A premature, malformed, or explicitly failed adapter
driver returns `OutboundBatchFailure` instead. Its `error` is the precise batch-level failure, and
its `slots` retains every terminal result collected before that failure:

```rust
let results = match client
    .send_all_until(requests, observation_cutoff)
    .await
{
    Ok(results) => results,
    Err(failure) => {
        for (index, slot) in failure.slots.into_iter().enumerate() {
            if let Some(slot) = slot {
                retain_terminal_slot(index, slot);
            }
        }
        return Err(failure.error);
    }
};
```

Driver failure never degrades into cutoff-like missing slots. Every terminal slot is timed from the
batch method-entry snapshot through its own terminal observation, including preflight and body
buffering. It is not pure network RTT. Bound the request count and every per-request body limit;
EdgeZero intentionally has no global batch-concurrency or memory cap.

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
