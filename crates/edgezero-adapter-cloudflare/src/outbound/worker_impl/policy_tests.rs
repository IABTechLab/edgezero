use std::io::Write as _;

use edgezero_core::{Deadline, ResponseLimitReason};
use flate2::{Compression, write::GzEncoder};
use wasm_bindgen_test::wasm_bindgen_test;

use crate::test_runtime::NumericTimeoutGuard;

use super::*;

struct NativeProbe {
    value: JsValue,
    signal: web_sys::AbortSignal,
}

impl NativeProbe {
    fn assert_pulls(&self, expected: u32) {
        assert_eq!(
            Reflect::get(&self.value, &JsValue::from_str("pulls"))
                .expect("source pulls")
                .as_f64(),
            Some(f64::from(expected)),
        );
    }

    fn assert_aborted(&self, expected: bool) {
        assert_eq!(self.signal.aborted(), expected);
        assert_eq!(
            Reflect::get(&self.value, &JsValue::from_str("aborts"))
                .expect("source aborts")
                .as_f64(),
            Some(f64::from(u32::from(expected))),
            "native cancellation occurs at most once",
        );
    }
}

fn native_response(
    status: StatusCode,
    headers: &HeaderMap,
    bytes: &[u8],
) -> (WorkerResponse, AbortGuard, NativeProbe) {
    let controller = web_sys::AbortController::new().expect("abort controller");
    let signal = controller.signal();
    let value = Function::new_with_args(
        "bytes, signal",
        "const probe = {pulls: 0, aborts: 0};
         signal.addEventListener('abort', () => {probe.aborts++;});
         probe.response = new Response(new ReadableStream({pull(c) {
             probe.pulls++;
             if (bytes.length) c.enqueue(new Uint8Array(bytes));
             c.close();
         }}, {highWaterMark: 0}));
         return probe;",
    )
    .call2(&JsValue::UNDEFINED, &Uint8Array::from(bytes), &signal)
    .expect("native response probe");
    let raw = Reflect::get(&value, &JsValue::from_str("response"))
        .expect("native response")
        .dyn_into::<web_sys::Response>()
        .expect("response type");
    let fields = Headers::new();
    for (name, field) in headers {
        fields
            .append(name.as_str(), field.to_str().expect("expressible field"))
            .expect("native response field");
    }
    let response = WorkerResponse::from(raw)
        .with_status(status.as_u16())
        .with_headers(fields);
    assert_eq!(
        response_headers(&response).expect("native fields"),
        *headers
    );
    (
        response,
        AbortGuard::new(controller),
        NativeProbe { value, signal },
    )
}

async fn finish_native(
    response: WorkerResponse,
    guard: AbortGuard,
    method: Method,
    decoding: ResponseDecoding,
    mode: ResponseMode,
    decoded_cap: u64,
    encoded_cap: u64,
) -> Result<OutboundResponse, EdgeError> {
    let start = MonotonicInstant::now();
    let duration = Duration::from_secs(1);
    finish_fixture_response(
        response,
        guard,
        method,
        OutboundProxyMarker::Off,
        decoding,
        mode,
        DispatchBudget {
            cause: BudgetSource::PerCallTimeout,
            deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
            duration,
        },
        if decoding == ResponseDecoding::Original {
            0
        } else {
            edgezero_core::DEFAULT_MAX_DECODER_BYTES
        },
        24,
        None,
        Some(decoded_cap),
        Some(encoded_cap),
        None,
        None,
        native_time(MonotonicClock::new(move || start)),
    )
    .await
}

fn assert_limit(result: Result<Bytes, EdgeError>, expected: ResponseLimitReason) {
    let error = result.expect_err("response policy limit");
    assert!(
        matches!(error, EdgeError::ResponseTooLarge { reason, .. } if reason == expected),
        "expected {expected:?}, got {error:?}",
    );
}

fn gzip_fixture() -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(b"body").expect("gzip input");
    encoder.finish().expect("gzip fixture")
}

#[wasm_bindgen_test]
async fn native_bodyless_response_policy_precedes_caps_and_polling() {
    let _timers = NumericTimeoutGuard::install();
    for (method, status) in [
        (Method::HEAD, StatusCode::OK),
        (Method::GET, StatusCode::NOT_MODIFIED),
        (Method::GET, StatusCode::NO_CONTENT),
    ] {
        for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 0 },
                ResponseMode::Streamed,
            ] {
                let mut headers = HeaderMap::new();
                headers.insert("content-length", HeaderValue::from_static("1073741824"));
                headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                headers.insert("etag", HeaderValue::from_static("\"strong\""));
                let (native, guard, probe) = native_response(status, &headers, b"not gzip");
                let response =
                    finish_native(native, guard, method.clone(), decoding, mode, 0, 64 * 1024)
                        .await
                        .expect("bodyless disposition precedes payload limits");
                assert_eq!(response.status(), status);
                if status == StatusCode::NO_CONTENT {
                    headers.remove("content-length");
                }
                assert_eq!(
                    response.headers(),
                    &headers,
                    "baseline metadata is retained"
                );
                assert_eq!(response.body().as_bytes(), Some(b"".as_slice()));
                probe.assert_pulls(0);
                probe.assert_aborted(true);
                assert!(
                    response
                        .into_bytes_bounded(0)
                        .await
                        .expect("empty body")
                        .is_empty()
                );
                probe.assert_pulls(0);
                probe.assert_aborted(true);
            }
        }
    }
}

#[wasm_bindgen_test]
async fn native_identity_limits_cover_declared_observed_and_exact_boundaries() {
    let _timers = NumericTimeoutGuard::install();
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: 20 },
            ResponseMode::Streamed,
        ] {
            for (length, declared) in [(11_usize, true), (11, false), (10, true), (10, false)] {
                for coding in [None, Some("identity")] {
                    let mut headers = HeaderMap::new();
                    if declared {
                        headers.insert(
                            "content-length",
                            HeaderValue::from_str(&length.to_string()).expect("length"),
                        );
                    }
                    if let Some(value) = coding {
                        headers.insert("content-encoding", HeaderValue::from_static(value));
                    }
                    let (native, guard, probe) =
                        native_response(StatusCode::OK, &headers, &vec![b'a'; length]);
                    let result = async {
                        let response =
                            finish_native(native, guard, Method::GET, decoding, mode, 10, 20)
                                .await?;
                        if mode == ResponseMode::Streamed {
                            probe.assert_pulls(0);
                        }
                        response.into_bytes_bounded(20).await
                    }
                    .await;
                    if length == 11 {
                        assert_limit(result, ResponseLimitReason::DecodedBody);
                        probe.assert_pulls(u32::from(!declared));
                        probe.assert_aborted(true);
                    } else {
                        assert_eq!(result.expect("exact identity limit"), vec![b'a'; 10]);
                        probe.assert_pulls(1);
                        probe.assert_aborted(false);
                    }
                }
            }
        }
    }
}

#[wasm_bindgen_test]
async fn native_original_gzip_keeps_decoded_exemption_and_transport_limits() {
    let _timers = NumericTimeoutGuard::install();
    let gzip = gzip_fixture();
    let length = u64::try_from(gzip.len()).expect("gzip length");
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: length },
            ResponseMode::Streamed,
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("content-encoding", HeaderValue::from_static("gzip"));
            headers.insert(
                "content-length",
                HeaderValue::from_str(&length.to_string()).expect("length"),
            );
            let (native, guard, probe) = native_response(StatusCode::OK, &headers, &gzip);
            let result = async {
                let response =
                    finish_native(native, guard, Method::GET, decoding, mode, 1, length).await?;
                if decoding == ResponseDecoding::Original {
                    assert_eq!(response.headers(), &headers);
                }
                response.into_bytes_bounded(length).await
            }
            .await;
            if decoding == ResponseDecoding::Original {
                assert_eq!(result.expect("original coding ignores decoded cap"), gzip);
                probe.assert_aborted(false);
            } else {
                assert_limit(result, ResponseLimitReason::DecodedBody);
            }
            probe.assert_pulls(1);
        }
    }
    for streamed in [false, true] {
        for declared in [false, true] {
            for expected in [
                ResponseLimitReason::EncodedBody,
                ResponseLimitReason::BufferedBody,
            ] {
                if streamed && expected == ResponseLimitReason::BufferedBody {
                    continue;
                }
                let mut headers = HeaderMap::new();
                headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                if declared {
                    headers.insert(
                        "content-length",
                        HeaderValue::from_str(&length.to_string()).expect("length"),
                    );
                }
                let encoded_cap = if expected == ResponseLimitReason::EncodedBody {
                    length.checked_sub(1).expect("nonempty gzip fixture")
                } else {
                    length
                };
                let final_cap = if expected == ResponseLimitReason::BufferedBody {
                    length.checked_sub(1).expect("nonempty gzip fixture")
                } else {
                    length
                };
                let mode = if streamed {
                    ResponseMode::Streamed
                } else {
                    ResponseMode::Buffered {
                        max_bytes: final_cap,
                    }
                };
                let (native, guard, probe) = native_response(StatusCode::OK, &headers, &gzip);
                let processed = finish_native(
                    native,
                    guard,
                    Method::GET,
                    ResponseDecoding::Original,
                    mode,
                    1,
                    encoded_cap,
                )
                .await;
                let result = if expected == ResponseLimitReason::BufferedBody {
                    Err(processed
                        .expect_err("native buffered cap must reject before report collection"))
                } else {
                    async { processed?.into_bytes_bounded(256).await }.await
                };
                assert_limit(result, expected);
                let rejected_at_head =
                    declared && (expected == ResponseLimitReason::EncodedBody || !streamed);
                probe.assert_pulls(u32::from(!rejected_at_head));
                probe.assert_aborted(true);
            }
        }
    }
}

#[wasm_bindgen_test]
async fn native_response_normalization_preserves_policy_precedence() {
    let _timers = NumericTimeoutGuard::install();
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: 20 },
            ResponseMode::Streamed,
        ] {
            for case in [
                "nominated-over",
                "nominated-exact",
                "missing-over",
                "malformed",
            ] {
                let mut headers = HeaderMap::new();
                if case.starts_with("nominated-") {
                    headers.insert(
                        "connection",
                        HeaderValue::from_static("content-encoding, content-length, etag"),
                    );
                    headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                    headers.insert("content-length", HeaderValue::from_static("malformed"));
                    headers.insert("etag", HeaderValue::from_static("\"removed\""));
                } else if case == "malformed" {
                    headers.insert("content-length", HeaderValue::from_static("malformed"));
                } else {
                    assert_eq!(case, "missing-over");
                }
                let decoded_cap = if case == "nominated-exact" { 2 } else { 1 };
                let (native, guard, probe) = native_response(StatusCode::OK, &headers, b"ab");
                let result = async {
                    let response =
                        finish_native(native, guard, Method::GET, decoding, mode, decoded_cap, 20)
                            .await?;
                    for name in ["connection", "content-encoding", "content-length", "etag"] {
                        assert!(
                            !response.headers().contains_key(name),
                            "{name} must not reappear"
                        );
                    }
                    response.into_bytes_bounded(20).await
                }
                .await;
                if case == "malformed" {
                    let error = result.expect_err("surviving malformed length");
                    assert!(
                        matches!(
                            error,
                            EdgeError::BadGateway {
                                reason: BadGatewayReason::Protocol,
                                ..
                            }
                        ),
                        "{error:?}"
                    );
                    probe.assert_pulls(0);
                    probe.assert_aborted(true);
                } else if case == "nominated-exact" {
                    assert_eq!(
                        result.expect("nominated malformed length is ignored"),
                        b"ab".as_slice()
                    );
                    probe.assert_pulls(1);
                    probe.assert_aborted(false);
                } else {
                    assert_limit(result, ResponseLimitReason::DecodedBody);
                    probe.assert_pulls(1);
                    probe.assert_aborted(true);
                }
            }
        }
    }
}

async fn finish_opaque(
    mut native: WorkerResponse,
    guard: AbortGuard,
    headers: HeaderMap,
    decoding: ResponseDecoding,
    mode: ResponseMode,
    decoded_cap: u64,
) -> Result<OutboundResponse, EdgeError> {
    let start = MonotonicInstant::now();
    let source = guarded_response_stream(response_stream(&mut native)?, guard);
    let client = testing::fixture_client(
        native_time(MonotonicClock::new(move || start)),
        move |context| {
            // Fetch header strings cannot express raw 0xff; inject only at the existing shared head.
            OutboundHead::new(
                StatusCode::OK,
                OutboundHeaderInput::new(
                    "cloudflare",
                    headers,
                    OutboundHeaderFidelity::ProviderFieldBoundariesUnknown,
                ),
                source,
                context,
                OutboundExchangeOwner::new(|| {}),
            )
        },
    )?;
    let mut parts = OutboundRequest::get("https://fixture.invalid/")?.into_parts();
    parts.response_decoding = decoding;
    parts.response_mode = mode;
    parts.timeout = Some(Duration::from_secs(1));
    parts.max_decoded_response_bytes = Some(decoded_cap);
    parts.max_encoded_response_bytes = Some(20);
    parts.proxy_marker = OutboundProxyMarker::Off;
    client.send(OutboundRequest::from_parts(parts)?).await
}

#[wasm_bindgen_test]
async fn shared_head_opaque_fields_preserve_normalization_policy() {
    let _timers = NumericTimeoutGuard::install();
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: 20 },
            ResponseMode::Streamed,
        ] {
            for opaque_coding in [false, true] {
                let mut headers = HeaderMap::new();
                headers.insert("content-length", HeaderValue::from_static("2"));
                headers.insert(
                    "etag",
                    HeaderValue::from_bytes(&[0xff]).expect("opaque ETag"),
                );
                if opaque_coding {
                    headers.insert(
                        "content-encoding",
                        HeaderValue::from_bytes(&[0xff]).expect("opaque coding"),
                    );
                }
                let (native, guard, probe) =
                    native_response(StatusCode::OK, &HeaderMap::new(), b"ab");
                let response = finish_opaque(
                    native,
                    guard,
                    headers,
                    decoding,
                    mode,
                    if opaque_coding { 1 } else { 2 },
                )
                .await
                .expect("opaque representation classification");
                assert!(!response.headers().contains_key("etag"));
                assert_eq!(response.headers()["content-length"], "2");
                if opaque_coding {
                    assert_eq!(response.headers()["content-encoding"].as_bytes(), &[0xff]);
                } else {
                    assert!(!response.headers().contains_key("content-encoding"));
                }
                assert_eq!(
                    response
                        .into_bytes_bounded(20)
                        .await
                        .expect("original bytes"),
                    b"ab".as_slice()
                );
                probe.assert_pulls(1);
                probe.assert_aborted(false);
            }
        }
    }
}

#[wasm_bindgen_test]
async fn native_representation_fields_survive_original_and_partial_responses() {
    let _timers = NumericTimeoutGuard::install();
    let gzip = gzip_fixture();
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: 256 },
            ResponseMode::Streamed,
        ] {
            for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
                let mut headers = HeaderMap::new();
                for (name, value) in [
                    ("content-encoding", "gzip"),
                    ("etag", "\"strong\""),
                    ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                    ("vary", "Accept-Encoding"),
                    ("accept-ranges", "bytes"),
                    ("cache-control", "public, max-age=60"),
                    (
                        "content-digest",
                        "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
                    ),
                ] {
                    headers.insert(name, HeaderValue::from_static(value));
                }
                headers.insert(
                    "content-length",
                    HeaderValue::from_str(&gzip.len().to_string()).expect("length"),
                );
                headers.insert(
                    "content-range",
                    HeaderValue::from_str(&format!(
                        "bytes 0-{}/{}",
                        gzip.len().checked_sub(1).expect("nonempty gzip fixture"),
                        gzip.len().checked_mul(2).expect("small gzip fixture")
                    ))
                    .expect("range"),
                );
                let original =
                    decoding == ResponseDecoding::Original || status == StatusCode::PARTIAL_CONTENT;
                let (native, guard, probe) = native_response(status, &headers, &gzip);
                let response = finish_native(
                    native,
                    guard,
                    Method::GET,
                    decoding,
                    mode,
                    if original { 1 } else { 4 },
                    256,
                )
                .await
                .expect("representation response");
                if !original {
                    headers.remove("content-encoding");
                    headers.remove("content-length");
                }
                assert_eq!(
                    response.headers(),
                    &headers,
                    "conditional metadata changes are not adopted"
                );
                let expected = if original { gzip.as_slice() } else { b"body" };
                assert_eq!(
                    response
                        .into_bytes_bounded(256)
                        .await
                        .expect("representation body"),
                    expected
                );
                probe.assert_pulls(1);
                probe.assert_aborted(false);
            }
        }
    }
}

#[wasm_bindgen_test]
async fn native_reset_content_preserves_original_disposition() {
    let _timers = NumericTimeoutGuard::install();
    for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
        for mode in [
            ResponseMode::Buffered { max_bytes: 0 },
            ResponseMode::Streamed,
        ] {
            for length in [None, Some("0"), Some("9")] {
                for payload in [b"".as_slice(), b"not decoded".as_slice()] {
                    let mut headers = HeaderMap::new();
                    headers.insert("content-encoding", HeaderValue::from_static("gzip"));
                    if let Some(value) = length {
                        headers.insert("content-length", HeaderValue::from_static(value));
                    }
                    let (native, guard, probe) =
                        native_response(StatusCode::RESET_CONTENT, &headers, payload);
                    let response = finish_native(native, guard, Method::GET, decoding, mode, 0, 0)
                        .await
                        .expect("reset disposition precedes payload caps and decoding");
                    assert_eq!(response.status(), StatusCode::RESET_CONTENT);
                    headers.insert("content-length", HeaderValue::from_static("0"));
                    assert_eq!(response.headers(), &headers);
                    assert_eq!(response.body().as_bytes(), Some(b"".as_slice()));
                    // All returned lengths are zero; polling must still use the original positive-length evidence.
                    probe.assert_pulls(u32::from(length != Some("9")));
                    probe.assert_aborted(length == Some("9") || !payload.is_empty());
                    assert!(
                        response
                            .into_bytes_bounded(0)
                            .await
                            .expect("empty reset")
                            .is_empty()
                    );
                    probe.assert_pulls(u32::from(length != Some("9")));
                    probe.assert_aborted(length == Some("9") || !payload.is_empty());
                }
            }
        }
    }
}
