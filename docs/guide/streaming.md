# Streaming

EdgeZero preserves lazy streaming response bodies on every adapter. Each adapter owns body
delivery through its strongest platform boundary; no standard entrypoint collects the complete
downstream response before handoff.

## Streaming Responses

Use `Body::stream` to yield response chunks progressively:

```rust
use edgezero_core::action;
use edgezero_core::body::Body;
use edgezero_core::http::Response;
use bytes::Bytes;
use futures::stream;

#[action]
async fn stream_data() -> Response {
    let chunks = vec![
        Bytes::from_static(b"Hello"),
        Bytes::from_static(b" "),
        Bytes::from_static(b"World"),
    ];

    let body = Body::stream(stream::iter(chunks));

    Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(body)
        .unwrap()
}
```

## How Streaming Works

The router keeps `Body::Stream` intact until the adapter response coordinator:

1. Your handler returns `Body::stream(...)` with a `Stream` of chunks
2. Core validates response framing and wraps declared-length enforcement around the source
3. The adapter polls one chunk at a time under its host's demand or write readiness
4. Accepted bytes, deadline/abort events, and one terminal outcome are recorded by that adapter

Axum uses a connection-local Hyper body, Cloudflare a JavaScript stream writer, Fastly
`stream_to_client`, and Spin a WASI body writer. See [Capabilities](/guide/capabilities) for the
exact acceptance and completion boundary on each target.

Application retention does not change this ownership. Every dispatch creates
fresh admission and response-egress state, even when the same successfully built
`App` is reused. Keep body sources, native writers, completion resources, and
pending work request-local rather than storing them in the app cache.

Fastly's `serve_app` and `serve_app_with_hooks` retain an app through
`lifecycle::Sandbox<App>` while the adapter owns sending and streaming. The hooks
borrow request/response values; they do not return a native response for the SDK
to send again. Cloudflare's `dispatch_app::<A>(&app, req, env, ctx)` returns a
Worker response backed by its request-local coordinator. Spin's
`dispatch_app::<A>(&app, req)` returns a raw `SpinResponse` whose body and
transmission lifetime remain owned by the adapter coordinator. None of these
paths collect the complete response before handoff.

## Server-Sent Events

Stream events to clients with SSE:

```rust
use edgezero_core::action;
use edgezero_core::body::Body;
use edgezero_core::http::Response;
use bytes::Bytes;

#[action]
async fn events() -> Response {
    let events = async_stream::stream! {
        for i in 0..10 {
            let payload = format!("data: Event {}\n\n", i);
            yield Bytes::from(payload);
        }
    };

    Response::builder()
        .status(200)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::stream(events))
        .unwrap()
}
```

::: warning Completion semantics
All adapters preserve progressive production, but their response-egress capability cells remain
BestEffort because host acceptance or close does not prove end-client receipt. Fastly source polls
and hostcalls are synchronous and cannot be preempted.
:::

## Body Modes

Routes can specify their body handling mode in the manifest. This is parsed today and reserved
for future enforcement by adapters and router helpers:

```toml
[[triggers.http]]
path = "/upload"
methods = ["POST"]
handler = "my_app::handlers::upload"
body-mode = "buffered"  # or "stream"
```

| Mode       | Behavior                                              |
| ---------- | ----------------------------------------------------- |
| `buffered` | Body is fully read into memory before handler runs    |
| `stream`   | Body is passed as a stream for progressive processing |

## Outbound Response Decompression

By default, the outbound client decodes a single bare `gzip`, `deflate`, or `br` content coding
through shared `edgezero-core` decoders. `response_decoding(ResponseDecoding::Original)` opts out;
206 responses always retain original coding. Unknown, parameterized, or stacked codings also
pass through unchanged. Identity bodies keep their decoded-byte cap under either choice.
Encoded transport bytes, decoded output, and final buffered bytes have independent limits; see
[Capabilities](/guide/capabilities#limits-and-accounting).

```rust
let request = OutboundRequest::get("https://api.example.com/data")?
    .max_encoded_response_bytes(2 * 1024 * 1024)
    .max_decoded_response_bytes(8 * 1024 * 1024)
    .stream_response();
let body = client.send(request).await?.into_body();
```

Multi-member gzip streams are decoded through every member and drained to transport EOF.
For Brotli, the stream window is checked before decoder allocation and decoder state is charged
against the configured policy limit.

## Memory Considerations

Lazy streaming is useful for:

- Large file downloads
- Video/audio content
- Real-time data feeds
- Responses larger than available memory

::: warning Platform limits
Edge platforms have finite memory. Lazy response delivery removes whole-body collection but does
not bound provider queues, SDK allocations, source chunk size, or transport buffers. Bound source
chunks and application work, and use the capability matrix when certifying a deployment.
:::

## Chunked Transfer

Applications may omit `Content-Length` when the response size is unknown:

```rust
#[action]
async fn dynamic_content() -> Response {
    let stream = generate_content_stream();

    // No Content-Length header needed
    Response::builder()
        .status(200)
        .header("content-type", "application/octet-stream")
        .body(Body::stream(stream))
        .unwrap()
}
```

Untransformed transmitted buffered bodies carry an exact `Content-Length`, including zero for an
empty body. This includes framework errors and bounded fallbacks. A supplied buffered length must
match the source or conversion fails before commitment. HEAD/204/205/304 do not gain a core-derived
length. Fastly still delegates framing to Automatic, but does not restore suppressed HEAD bytes:
doing so causes HTTP/2 protocol failures in the pinned local runtime. Exact known-buffer HEAD/205
SDK parity remains an open gate; strict body suppression takes precedence.

Streams remain progressive: without a declared length the provider chooses transfer framing;
with a declared length EdgeZero enforces the source byte count and aborts a short or long stream.
No stream is buffered or polled merely to guess its length. Fastly preloads actual buffered bodies
before its non-streaming Automatic handoff, so an expired precommit deadline can produce the 504
fallback instead of truncating an already-committed buffered response.

Host transformations take precedence over source length. For Fastly host compression requested
with `x-compress-hint`, Automatic framing may recompute or omit client-visible `Content-Length`.
Any such hint also disables manual framing for declared streams; EdgeZero still validates their
untransformed source length. It deliberately does not interpret ambiguous hint values in the guest.
Hosted compression and HTTP/1.1/HTTP/2 SDK parity remain qualification gates, not claims inferred
from Viceroy. See the [response egress capability matrix](/guide/capabilities#response-egress-matrix).

## Framework Error Headers

Configure a bounded header set once during application configuration:

```rust
use edgezero_core::app::App;
use edgezero_core::http::{HeaderMap, HeaderValue};
use edgezero_core::EdgeError;

fn configure(app: &mut App) -> Result<(), EdgeError> {
    let mut headers = HeaderMap::new();
    headers.insert("x-application-version", HeaderValue::from_static("1.0.0"));
    app.set_synthesized_response_headers(headers)
        .map_err(EdgeError::internal)?;
    Ok(())
}
```

The set applies to normalized ingress errors, admitted source errors, canonical 404/405 responses,
and adapter-built egress fallbacks, including HEAD fallbacks. It does not decorate routed responses
or routed handler errors, application-built admission refusals, or host parser responses. Assembly
failures have no configured application and therefore no configured headers or renderer.

Limits are **32 field values** and **4096 bytes**, inclusive. Repeated values count separately;
bytes are `2 + sum(name.len() + value.len() + 4)`, including `: ` and CRLF per field and the final
CRLF, but excluding the status line. Values must be ASCII text with optional interior tabs and no
leading/trailing space or tab. Empty values are valid. Framing, hop-by-hop, authentication-proxy,
`Content-Type` and `Content-Encoding` fields are forbidden. Construction errors expose no values.
Accepted maps and backing storage are compacted; these bounds do not cover the caller's input
allocation, existing response/renderer fields, or provider-owned serialization and transport memory.

Existing response-owned names win; otherwise all configured values of that name are appended in
order. Canonical `Allow`, the effective config-503 retry value and typed body-overflow retry
suppression remain authoritative. The fixed fallback body and independent one-second budget do not
change, and fallback decoration runs neither application code nor validation again.

On Fastly, construct `SynthesizedResponseHeaders::new(fields)` inside `prepare` and insert the
validated value into the provided extension bag. It replaces the app set for that request before
egress preparation, including early normalized 414/431 errors. An empty validated set clears the
request's decoration; failed construction installs nothing and leaves the app set available.
The other adapters use the app snapshot. This guest-side policy does not qualify hosted delivery.

## Next Steps

- Learn about [Outbound HTTP](/guide/proxying) for upstream requests
- Explore adapter-specific streaming in the [Fastly](/guide/adapters/fastly), [Cloudflare](/guide/adapters/cloudflare), [Spin](/guide/adapters/spin), and [Axum](/guide/adapters/axum) guides
