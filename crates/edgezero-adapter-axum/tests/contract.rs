#![cfg(all(test, feature = "axum"))]
#![expect(
    clippy::expect_used,
    clippy::tests_outside_test_module,
    reason = "this integration-test crate uses explicit fixture diagnostics and consists only of contract tests"
)]

use core::convert::Infallible;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_stream::stream as async_body_stream;
use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::HeaderValue;
use axum::http::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, HOST, SET_COOKIE};
use axum::response::Response;
use axum::routing::{get, post};
use bytes::Bytes;
use edgezero_adapter_axum::outbound::AxumOutboundClient;
use edgezero_core::body::Body as CoreBody;
use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError, ResponseLimitReason};
use edgezero_core::http::{HeaderMap, Method, StatusCode};
use edgezero_core::time::Deadline;
use edgezero_core::{
    FLATE_DECODER_FIXED_CHARGE_BYTES, HttpClient, MonotonicClock, OutboundBatchNext,
    OutboundBatchTermination, OutboundCachePolicy, OutboundHttpClient as _, OutboundProxyMarker,
    OutboundRequest, OutboundTime, PROXY_HEADER, ResponseDecoding,
};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures_util::future::{Either, select};
use futures_util::stream;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::sleep;

fn paired_cutoff_time(
    clock: MonotonicClock,
    advancement: Arc<Notify>,
    wake_count: Arc<AtomicUsize>,
) -> OutboundTime {
    OutboundTime::new(clock, move |deadline, paired_clock| {
        let notifier = Arc::clone(&advancement);
        let counter = Arc::clone(&wake_count);
        Box::pin(async move {
            let remaining = deadline
                .remaining_at(paired_clock.now())
                .unwrap_or_default();
            let notification = notifier.notified();
            let timeout = sleep(remaining);
            futures_util::pin_mut!(notification, timeout);
            if let Either::Left(((), _)) = select(notification, timeout).await {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        })
    })
}

fn gzip(input: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input).expect("gzip input");
    encoder.finish().expect("gzip output")
}

fn zlib(input: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input).expect("deflate input");
    encoder.finish().expect("deflate output")
}

async fn start_origin(router: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind origin");
    let address = listener.local_addr().expect("origin address");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve origin");
    });
    format!("http://{address}")
}

async fn start_coding_origin(coding: &'static str, origin_body: Vec<u8>) -> String {
    start_origin(Router::new().route(
        "/{status}",
        get(move |Path(status): Path<u16>, headers: HeaderMap| {
            let bytes = origin_body.clone();
            async move {
                assert_eq!(
                    headers.get("accept-encoding").expect("encoding"),
                    "gzip, deflate",
                    "request encoding negotiation must survive both response decoding choices"
                );
                Response::builder()
                    .status(status)
                    .header(CONTENT_ENCODING, coding)
                    .header(CONTENT_LENGTH, bytes.len())
                    .header("etag", "\"original\"")
                    .header("content-range", "bytes 0-3/8")
                    .body(Body::from(bytes))
                    .expect("origin response")
            }
        }),
    ))
    .await
}

#[tokio::test]
async fn marker_policy_reaches_transport_bodyless_redirect_and_batch_paths() {
    let origin = start_origin(Router::new().route(
        "/{status}",
        get(|Path(status): Path<u16>, headers: HeaderMap| async move {
            let mut response = Response::builder()
                .status(status)
                .header("location", "/unreachable");
            if headers.contains_key("x-fixture-upstream-marker") {
                response = response.header(PROXY_HEADER, "upstream");
            }
            response.body(Body::empty()).expect("origin response")
        }),
    ))
    .await;
    let client = HttpClient::with_client(AxumOutboundClient::try_new().expect("client"))
        .with_proxy_marker(false);
    for (method, status) in [
        (Method::GET, StatusCode::OK),
        (Method::GET, StatusCode::NO_CONTENT),
        (Method::HEAD, StatusCode::OK),
        (Method::GET, StatusCode::NOT_MODIFIED),
        (Method::GET, StatusCode::FOUND),
    ] {
        for override_setting in [
            OutboundProxyMarker::Inherit,
            OutboundProxyMarker::On,
            OutboundProxyMarker::Off,
        ] {
            for upstream in [false, true] {
                let mut request = OutboundRequest::new(
                    method.clone(),
                    format!("{origin}/{}", status.as_u16())
                        .parse()
                        .expect("target"),
                )
                .expect("request")
                .proxy_marker(override_setting)
                .max_encoded_response_bytes(0)
                .max_decoded_response_bytes(0)
                .max_response_bytes(0);
                if upstream {
                    request = request
                        .header("x-fixture-upstream-marker", "yes")
                        .expect("header");
                }
                let enabled = client.effective_proxy_marker_enabled(&request);
                let response = client.send(request).await.expect("response");
                assert_eq!(response.status(), status);
                let expected = if enabled {
                    Some("axum")
                } else if upstream {
                    Some("upstream")
                } else {
                    None
                };
                assert_eq!(
                    response
                        .headers()
                        .get(PROXY_HEADER)
                        .and_then(|value| value.to_str().ok()),
                    expected
                );
            }
        }
    }
    let requests = [OutboundProxyMarker::Inherit, OutboundProxyMarker::On]
        .into_iter()
        .map(|setting| {
            OutboundRequest::get(format!("{origin}/200"))
                .expect("request")
                .proxy_marker(setting)
        })
        .collect();
    let results = client
        .send_all_until(requests, Deadline::after(Duration::from_secs(2)))
        .await
        .expect("batch");
    for (slot, expected) in results.slots.into_iter().zip([None, Some("axum")]) {
        let response = slot.expect("resolved slot").outcome.expect("response");
        assert_eq!(
            response
                .headers()
                .get(PROXY_HEADER)
                .and_then(|value| value.to_str().ok()),
            expected
        );
    }
}

#[tokio::test]
async fn redirect_response_is_not_followed() {
    let origin = start_origin(
        Router::new()
            .route("/target", get(|| async { "followed" }))
            .route(
                "/redirect",
                get(|| async { (StatusCode::FOUND, [("location", "/target")]) }),
            ),
    )
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/redirect")).expect("request");

    let response = client.send(request).await.expect("response");

    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response
            .headers()
            .get(PROXY_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("axum")
    );
}

#[tokio::test]
async fn batch_collection_reports_per_slot_elapsed() {
    let origin = start_origin(Router::new().route("/", get(|| async { "ok" }))).await;
    let client = AxumOutboundClient::try_new().expect("client");
    let requests = vec![
        OutboundRequest::get(format!("{origin}/")).expect("reachable request"),
        OutboundRequest::new(
            Method::GET,
            "http://127.0.0.1:1/unreachable"
                .parse()
                .expect("unreachable URI"),
        )
        .expect("unreachable request"),
    ];

    let results = client
        .start_batch_until(requests, Deadline::after(Duration::from_secs(1)))
        .collect()
        .await
        .expect("valid batch driver");

    assert_eq!(results.slots.len(), 2);
    assert_eq!(results.termination, OutboundBatchTermination::Completed);
    results.slots[0]
        .as_ref()
        .expect("reachable slot resolved")
        .outcome
        .as_ref()
        .expect("reachable slot succeeds");
    assert!(matches!(
        results.slots[1]
            .as_ref()
            .expect("unreachable slot resolved")
            .outcome,
        Err(EdgeError::BadGateway {
            reason: BadGatewayReason::Unreachable,
            ..
        })
    ));
}

#[tokio::test]
async fn batch_preflight_precedence_and_indices() {
    let client = AxumOutboundClient::try_new().expect("client");
    let streamed_upload = OutboundRequest::post("https://example.com/upload")
        .expect("upload request")
        .body(CoreBody::stream(stream::iter([bytes::Bytes::from_static(
            b"body",
        )])));
    let streamed_response = OutboundRequest::get("https://example.com/stream")
        .expect("stream request")
        .stream_response();
    let method_error = OutboundRequest::get("https://example.com/get")
        .expect("GET request")
        .body(CoreBody::stream(stream::iter([bytes::Bytes::new()])));

    let results = client
        .start_batch_until(
            vec![streamed_upload, streamed_response, method_error],
            Deadline::after(Duration::from_secs(1)),
        )
        .collect()
        .await
        .expect("valid batch driver");

    assert_eq!(results.termination, OutboundBatchTermination::Completed);

    let messages: Vec<_> = results
        .slots
        .iter()
        .map(
            |slot| match &slot.as_ref().expect("preflight slot resolved").outcome {
                Err(EdgeError::BadRequest { message, .. }) => message.as_str(),
                other => panic!("expected preflight rejection, got {other:?}"),
            },
        )
        .collect();
    assert_eq!(
        messages,
        [
            "outbound batches require buffered request bodies; use send for a streamed upload",
            "outbound batches require buffered responses; use send for a streamed response",
            "GET/HEAD request must not carry a streamed body; emptiness cannot be determined without consuming the stream",
        ]
    );
}

#[tokio::test]
async fn oversized_uploads_never_contact_origin_and_upstream_413_remains_a_response() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let origin = start_origin(Router::new().route(
        "/upload",
        post(move || {
            let observed_calls = Arc::clone(&observed);
            async move {
                observed_calls.fetch_add(1, Ordering::SeqCst);
                Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .header("retry-after", "120")
                    .body(Body::empty())
                    .expect("upstream response")
            }
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    for streamed in [false, true] {
        let bytes = Bytes::from_static(b"123456789");
        let body = if streamed {
            CoreBody::stream(stream::iter([bytes]))
        } else {
            CoreBody::from(bytes)
        };
        let request = OutboundRequest::post(format!("{origin}/upload"))
            .expect("request")
            .body(body)
            .max_request_body_bytes(8);
        let error = client.send(request).await.expect_err("local overflow");
        assert_eq!(
            error.request_body_overflow_origin(),
            Some(edgezero_core::RequestBodyOverflowOrigin::OutboundUpload)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let request = OutboundRequest::post(format!("{origin}/upload"))
        .expect("request")
        .body("12345678")
        .max_request_body_bytes(8);
    let response = client.send(request).await.expect("ordinary upstream 413");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .expect("upstream retry"),
        "120"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn buffered_response_deadline_covers_body_completion() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            let body = async_body_stream! {
                yield Ok::<_, Infallible>(Bytes::from_static(b"first"));
                sleep(Duration::from_millis(100)).await;
                yield Ok::<_, Infallible>(Bytes::from_static(b"second"));
            };
            Response::new(Body::from_stream(body))
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/"))
        .expect("request")
        .timeout(Duration::from_millis(10));

    let error = client
        .send(request)
        .await
        .expect_err("body completion must share the absolute request deadline");

    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::PerCallTimeout,
            ..
        }
    ));
}

#[tokio::test]
async fn batch_preserves_request_deadline_provenance() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            sleep(Duration::from_millis(100)).await;
            "late"
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/"))
        .expect("request")
        .deadline(Deadline::after(Duration::from_millis(10)));

    let results = client
        .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
        .collect()
        .await
        .expect("valid batch driver");

    assert_eq!(results.termination, OutboundBatchTermination::Completed);

    assert!(matches!(
        results.slots[0]
            .as_ref()
            .expect("request deadline slot resolved")
            .outcome,
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::RequestDeadline,
            ..
        })
    ));
}

#[tokio::test]
async fn batch_yields_complete_exchanges_in_completion_order() {
    let origin = start_origin(
        Router::new()
            .route(
                "/slow",
                get(|| async {
                    sleep(Duration::from_millis(75)).await;
                    "slow"
                }),
            )
            .route("/fast", get(|| async { "fast" })),
    )
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let requests = vec![
        OutboundRequest::get(format!("{origin}/slow")).expect("slow request"),
        OutboundRequest::get(format!("{origin}/fast")).expect("fast request"),
    ];
    let mut batch = client.start_batch_until(requests, Deadline::after(Duration::from_secs(1)));

    let OutboundBatchNext::Item(first) = batch.next().await.expect("valid batch driver") else {
        panic!("expected first completion");
    };
    let OutboundBatchNext::Item(second) = batch.next().await.expect("valid batch driver") else {
        panic!("expected second completion");
    };

    assert_eq!(first.index, 1);
    assert_eq!(second.index, 0);
    assert!(matches!(
        batch.next().await.expect("valid batch driver"),
        OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
    ));
}

#[tokio::test(start_paused = true)]
async fn early_observation_cutoff_preserves_completed_slots_and_leaves_pending_none() {
    use std::future::Future as _;
    use std::sync::Mutex;
    use std::time::Instant;

    use edgezero_core::{MonotonicInstant, OutboundBatch, OutboundBatchDriverEvent};
    use futures_util::future::poll_fn;

    let slow_started = Arc::new(Notify::new());
    let release_slow = Arc::new(Notify::new());
    let started = Arc::clone(&slow_started);
    let release = Arc::clone(&release_slow);
    let origin = start_origin(
        Router::new()
            .route("/fast", get(|| async { "fast" }))
            .route(
                "/slow",
                get(move || {
                    let request_started = Arc::clone(&started);
                    let response_release = Arc::clone(&release);
                    async move {
                        request_started.notify_one();
                        response_release.notified().await;
                        "slow"
                    }
                }),
            ),
    )
    .await;
    let now = Arc::new(Mutex::new(MonotonicInstant::now()));
    let sampled = Arc::clone(&now);
    let clock = MonotonicClock::new(move || *sampled.lock().expect("fake clock"));
    let observation_cutoff = clock.deadline_after(Duration::from_millis(25));
    let clock_advanced = Arc::new(Notify::new());
    let timer_wakes = Arc::new(AtomicUsize::new(0));
    let time = paired_cutoff_time(clock, Arc::clone(&clock_advanced), Arc::clone(&timer_wakes));
    let client = AxumOutboundClient::try_with_time(time).expect("client");
    let mut batch = client.start_batch_until(
        vec![
            OutboundRequest::get(format!("{origin}/fast")).expect("fast request"),
            OutboundRequest::get(format!("{origin}/slow")).expect("slow request"),
        ],
        observation_cutoff,
    );
    let observed = OutboundBatch::from_driver(
        2,
        async_body_stream! {
            loop {
                match batch.next().await.expect("valid adapter batch driver") {
                    OutboundBatchNext::Item(item) => {
                        assert_eq!(item.index, 0);
                        assert_eq!(item.result.outcome.as_ref().expect("fast response").status(), StatusCode::OK);
                        yield OutboundBatchDriverEvent::Item(item);
                        // Advance the deadline without making the pending response ready.
                        slow_started.notified().await;
                        *now.lock().expect("fake clock") = observation_cutoff.instant();
                        clock_advanced.notify_waiters();
                    }
                    OutboundBatchNext::Finished(OutboundBatchTermination::Cutoff) => {
                        yield OutboundBatchDriverEvent::Cutoff;
                        return;
                    }
                    OutboundBatchNext::Finished(OutboundBatchTermination::Completed) => return,
                    OutboundBatchNext::Finished(_) | _ => panic!("unexpected batch outcome"),
                }
            }
        },
    );
    let collected = observed.collect();
    futures_util::pin_mut!(collected);
    let containment_started = Instant::now();
    let results = poll_fn(|context| {
        assert!(
            containment_started.elapsed() < Duration::from_secs(5),
            "batch collection exceeded the real-clock containment watchdog"
        );
        let result = collected.as_mut().poll(context);
        // Keep paused Tokio time from auto-advancing while loopback I/O is pending.
        if result.is_pending() {
            context.waker().wake_by_ref();
        }
        result
    })
    .await
    .expect("valid batch driver");

    release_slow.notify_one();
    assert!(
        timer_wakes.load(Ordering::SeqCst) > 0,
        "advancing the fake clock must wake the paired timer"
    );
    assert_eq!(results.termination, OutboundBatchTermination::Cutoff);
    assert!(results.slots[0].is_some());
    assert!(results.slots[1].is_none());
}

#[tokio::test]
async fn authority_override_changes_host_only_and_cache_bypass_adds_no_origin_header() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|headers: HeaderMap| async move {
            let host = headers
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing");
            let cache_control = headers
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("absent");
            format!("{host}|{cache_control}")
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/"))
        .expect("request")
        .host_authority_override("virtual.example:8443")
        .expect("authority override")
        .cache_policy(OutboundCachePolicy::Bypass);

    let body = client
        .send(request)
        .await
        .expect("response")
        .into_bytes_bounded(1_024)
        .await
        .expect("response body");

    assert_eq!(body, "virtual.example:8443|absent");
}

#[tokio::test]
async fn streamed_response_deadline_preserves_timeout_provenance() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            let body = async_body_stream! {
                sleep(Duration::from_secs(2)).await;
                yield Ok::<_, Infallible>(Bytes::from_static(b"late"));
            };
            Response::new(Body::from_stream(body))
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/"))
        .expect("request")
        .stream_response()
        .timeout(Duration::from_millis(500));

    let response = client.send(request).await.expect("response headers");
    let error = response
        .into_body()
        .into_bytes_bounded(1_024)
        .await
        .expect_err("stream body must retain the request deadline");

    assert!(matches!(
        error,
        EdgeError::GatewayTimeout {
            cause: BudgetSource::PerCallTimeout,
            ..
        }
    ));
}

#[tokio::test]
async fn encoded_and_decoded_limits_report_independent_origins() {
    let decoded = vec![b'x'; 4_096];
    let encoded = gzip(&decoded);
    let origin_body = encoded.clone();
    let origin = start_origin(Router::new().route(
        "/",
        get(move || {
            let bytes = origin_body.clone();
            async move {
                Response::builder()
                    .header(CONTENT_ENCODING, "gzip")
                    .body(Body::from(bytes))
                    .expect("origin response")
            }
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");

    let decoded_error = client
        .send(
            OutboundRequest::get(format!("{origin}/"))
                .expect("decoded-limit request")
                .max_encoded_response_bytes(u64::try_from(encoded.len()).expect("encoded length"))
                .max_decoded_response_bytes(128),
        )
        .await
        .expect_err("decoded response must exceed its independent limit");
    assert!(matches!(
        decoded_error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::DecodedBody,
            ..
        }
    ));

    let encoded_error = client
        .send(
            OutboundRequest::get(format!("{origin}/"))
                .expect("encoded-limit request")
                .max_encoded_response_bytes(
                    u64::try_from(encoded.len())
                        .expect("encoded length")
                        .saturating_sub(1),
                )
                .max_decoded_response_bytes(u64::try_from(decoded.len()).expect("decoded length")),
        )
        .await
        .expect_err("encoded response must exceed its independent limit");
    assert!(matches!(
        encoded_error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::EncodedBody,
            ..
        }
    ));
}

#[tokio::test]
async fn deflate_response_is_decoded_stripped_and_bounded() {
    let decoded = vec![b'd'; 4_096];
    let encoded = zlib(&decoded);
    let origin_body = encoded.clone();
    let origin = start_origin(Router::new().route(
        "/",
        get(move || {
            let bytes = origin_body.clone();
            async move {
                Response::builder()
                    .header(CONTENT_ENCODING, "deflate")
                    .header(CONTENT_LENGTH, bytes.len())
                    .body(Body::from(bytes))
                    .expect("origin response")
            }
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");

    let response = client
        .send(
            OutboundRequest::get(format!("{origin}/"))
                .expect("request")
                .max_decoder_bytes(FLATE_DECODER_FIXED_CHARGE_BYTES)
                .max_encoded_response_bytes(u64::try_from(encoded.len()).expect("encoded length"))
                .max_decoded_response_bytes(u64::try_from(decoded.len()).expect("decoded length")),
        )
        .await
        .expect("decoded response");
    assert!(response.headers().get(CONTENT_ENCODING).is_none());
    assert!(response.headers().get(CONTENT_LENGTH).is_none());
    assert_eq!(
        response
            .into_bytes_bounded(u64::try_from(decoded.len()).expect("decoded length"))
            .await
            .expect("decoded body"),
        decoded
    );

    let decoded_error = client
        .send(
            OutboundRequest::get(format!("{origin}/"))
                .expect("request")
                .max_decoded_response_bytes(128),
        )
        .await
        .expect_err("decoded response must exceed its limit");
    assert!(matches!(
        decoded_error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::DecodedBody,
            ..
        }
    ));
}

#[tokio::test]
async fn original_chunked_response_obeys_observed_buffer_cap() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            Response::builder()
                .header(CONTENT_ENCODING, "gzip")
                .body(Body::from_stream(stream::iter([Ok::<_, Infallible>(
                    Bytes::from_static(b"raw!"),
                )])))
                .expect("chunked origin response")
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let error = client
        .send(
            OutboundRequest::get(origin)
                .expect("request")
                .response_decoding(ResponseDecoding::Original)
                .max_decoder_bytes(0)
                .max_decoded_response_bytes(1)
                .max_encoded_response_bytes(20)
                .max_response_bytes(3),
        )
        .await
        .expect_err("observed final buffer overflow");
    assert!(matches!(
        error,
        EdgeError::ResponseTooLarge {
            reason: ResponseLimitReason::BufferedBody,
            ..
        }
    ));
}

#[tokio::test]
async fn original_and_partial_coding_survive_actual_transport_and_mixed_batch() {
    let plain = b"original compressed representation";
    for (coding, encoded) in [("gzip", gzip(plain)), ("deflate", zlib(plain))] {
        let origin = start_coding_origin(coding, encoded.clone()).await;
        let client = AxumOutboundClient::try_new().expect("client");
        let length = u64::try_from(encoded.len()).expect("encoded length");
        for (choice, status) in [
            (ResponseDecoding::Original, 200_u16),
            (ResponseDecoding::Original, 206),
            (ResponseDecoding::Decode, 206),
        ] {
            for streamed in [false, true] {
                let request = OutboundRequest::get(format!("{origin}/{status}"))
                    .expect("request")
                    .header("accept-encoding", "gzip, deflate")
                    .expect("header")
                    .response_decoding(choice)
                    .max_decoder_bytes(0)
                    .max_decoded_response_bytes(1)
                    .max_encoded_response_bytes(length)
                    .max_response_bytes(length);
                let outgoing = if streamed {
                    request.stream_response()
                } else {
                    request
                };
                let response = client.send(outgoing).await.expect("raw response");
                assert_eq!(
                    response.headers().get(CONTENT_ENCODING).expect("coding"),
                    coding
                );
                assert_eq!(
                    response
                        .headers()
                        .get(CONTENT_LENGTH)
                        .expect("length")
                        .as_bytes(),
                    length.to_string().as_bytes()
                );
                assert_eq!(
                    response.headers().get("etag").expect("etag"),
                    "\"original\""
                );
                assert_eq!(
                    response.headers().get("content-range").expect("range"),
                    "bytes 0-3/8"
                );
                assert_eq!(
                    response
                        .into_bytes_bounded(length)
                        .await
                        .expect("raw bytes"),
                    encoded
                );
            }
        }
        let requests = [ResponseDecoding::Original, ResponseDecoding::Decode]
            .into_iter()
            .map(|choice| {
                OutboundRequest::get(format!("{origin}/200"))
                    .expect("request")
                    .header("accept-encoding", "gzip, deflate")
                    .expect("header")
                    .response_decoding(choice)
            })
            .collect();
        let results = client
            .start_batch_until(requests, Deadline::after(Duration::from_secs(2)))
            .collect()
            .await
            .expect("batch");
        assert_eq!(results.termination, OutboundBatchTermination::Completed);
        for (index, slot) in results.slots.into_iter().enumerate() {
            let response = slot.expect("resolved").outcome.expect("response");
            let expected = if index == 0 {
                encoded.as_slice()
            } else {
                plain.as_slice()
            };
            assert_eq!(
                response.into_bytes_bounded(256).await.expect("bytes"),
                expected
            );
        }
    }
}

#[tokio::test]
async fn passthrough_coding_is_not_charged_to_decoded_limit() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            Response::builder()
                .header(CONTENT_ENCODING, "zstd")
                .body(Body::from("opaque"))
                .expect("origin response")
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");
    let request = OutboundRequest::get(format!("{origin}/"))
        .expect("request")
        .max_encoded_response_bytes(6)
        .max_decoded_response_bytes(1);

    let response = client.send(request).await.expect("response");
    let bytes = response.into_bytes_bounded(6).await.expect("response body");

    assert_eq!(bytes, "opaque");
}

#[tokio::test]
async fn repeated_response_headers_are_preserved() {
    let origin = start_origin(Router::new().route(
        "/",
        get(|| async {
            let mut response = Response::new(Body::empty());
            response
                .headers_mut()
                .append(SET_COOKIE, HeaderValue::from_static("a=1"));
            response
                .headers_mut()
                .append(SET_COOKIE, HeaderValue::from_static("b=2"));
            response
        }),
    ))
    .await;
    let client = AxumOutboundClient::try_new().expect("client");

    let response = client
        .send(OutboundRequest::get(format!("{origin}/")).expect("request"))
        .await
        .expect("response");
    let values: Vec<_> = response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().expect("header value"))
        .collect();

    assert_eq!(values, ["a=1", "b=2"]);
}
