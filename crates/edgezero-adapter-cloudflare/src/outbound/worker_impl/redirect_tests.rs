use core::convert::Infallible;
use std::io::Write as _;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edgezero_core::http::StatusCode;
use edgezero_core::{
    BadGatewayDecodeReason, BadGatewayReason, BudgetSource, ContentTypeProvenance, Deadline,
    EdgeError, HttpClient, MonotonicClock, MonotonicInstant, OutboundFollowFailure,
    OutboundRedirectAction, OutboundRedirectLimit, OutboundRedirectStopReason, OutboundRequest,
    OutboundResponse, ResponseLimitReason,
};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures_util::FutureExt as _;
use serde_json::{Value, json};
use wasm_bindgen_test::wasm_bindgen_test;
use worker::js_sys::{Function, JSON, Reflect};
use worker::wasm_bindgen::{JsCast as _, JsValue};

use crate::test_runtime::NumericTimeoutGuard;

use super::clock_tests::RestoreGlobal;
use super::{CloudflareOutboundClient, HostEventYield, native_time};

fn fetch_script(responses: &Value) -> (JsValue, RestoreGlobal) {
    let wire = serde_json::to_string(responses).expect("fixture JSON");
    let probe = Function::new_with_args("wire", include_str!("../../../tests/redirect_runtime.js"))
        .call1(&JsValue::UNDEFINED, &JsValue::from_str(&wire))
        .expect("install fetch fixture");
    let restore = Reflect::get(&probe, &JsValue::from_str("restore"))
        .expect("restore")
        .dyn_into::<Function>()
        .expect("restore function");
    (probe, RestoreGlobal::new(restore))
}

fn observations(probe: &JsValue) -> Value {
    let encoded = JSON::stringify(probe)
        .expect("observations JSON")
        .as_string()
        .expect("observations text");
    serde_json::from_str(&encoded).expect("observations")
}

fn request(path: &str, maximum: u64) -> OutboundRequest {
    OutboundRequest::get(format!("https://fixture.invalid/{path}"))
        .expect("request")
        .max_response_bytes(maximum)
        .timeout(Duration::from_secs(5))
}

fn response(status: u16, bytes: &[u8], headers: &Value) -> Value {
    json!({"status": status, "kind": "bytes", "bytes": bytes, "headers": headers})
}

fn encoded(coding: &str, bytes: &[u8]) -> Vec<u8> {
    match coding {
        "gzip" => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(bytes).expect("gzip input");
            encoder.finish().expect("gzip output")
        }
        "deflate" => {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(bytes).expect("deflate input");
            encoder.finish().expect("deflate output")
        }
        _ => bytes.to_vec(),
    }
}

async fn bytes(reply: OutboundResponse, maximum: u64) -> Vec<u8> {
    reply
        .into_bytes_bounded(maximum)
        .await
        .expect("response body")
        .to_vec()
}

#[wasm_bindgen_test]
async fn declared_encoded_cap_rejects_malformed_gzip_before_polling() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    let fixture = response(
        200,
        &[b'a'; 11],
        &json!([["content-encoding", "gzip"], ["content-length", "11"]]),
    );
    let (probe, _restore) = fetch_script(&json!([fixture, fixture]));
    let plain = client
        .send(request("declared", 10).max_encoded_response_bytes(10))
        .await
        .expect_err("declared encoded cap");
    let followed = client
        .follow(
            request("declared", 10).max_encoded_response_bytes(10),
            OutboundRedirectLimit::new(1).expect("limit"),
            |_| -> Result<OutboundRedirectAction, Infallible> {
                panic!("a final head must not invoke redirect policy")
            },
        )
        .await
        .expect_err("declared encoded cap");
    let OutboundFollowFailure::Hop { index, error } = followed;
    assert_eq!(index, 0);
    for failure in [plain, error] {
        assert!(matches!(
            failure,
            EdgeError::ResponseTooLarge {
                reason: ResponseLimitReason::EncodedBody,
                ..
            }
        ));
    }
    let seen = observations(&probe);
    assert_eq!(seen["requests"].as_array().expect("requests").len(), 2);
    assert_eq!(seen["pulls"], json!([0_u64, 0_u64]));
    assert_eq!(seen["aborts"], json!([1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn follow_without_redirect_matches_actual_fetch_coding_and_limits() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    for coding in ["identity", "gzip", "deflate", "unsupported"] {
        for (length, declared) in [10_usize, 11]
            .into_iter()
            .flat_map(|length| [false, true].map(move |declared| (length, declared)))
        {
            let payload = encoded(coding, &vec![b'x'; length]);
            let mut head = json!([["content-encoding", coding]]);
            if declared {
                head.as_array_mut()
                    .expect("headers")
                    .push(json!(["content-length", payload.len().to_string()]));
            }
            let fixture = response(200, &payload, &head);
            let (probe, _restore) = fetch_script(&json!([fixture, fixture]));
            let sent_result = client.send(request("plain", 10)).await;
            let following_result = client
                .follow(
                    request("follow", 10),
                    OutboundRedirectLimit::new(1).expect("limit"),
                    |_| Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared),
                )
                .await;
            match (sent_result, following_result) {
                (Ok(plain), Ok(followed)) => {
                    assert_eq!(length, 10);
                    assert_eq!(plain.status(), followed.response().status());
                    assert_eq!(plain.headers(), followed.response().headers());
                    assert_eq!(
                        plain.content_type_provenance(),
                        ContentTypeProvenance::Unknown
                    );
                    assert_eq!(followed.redirects_followed(), 0);
                    let (final_response, _, _, _) = followed.into_parts();
                    let actual = bytes(plain, 10).await;
                    assert_eq!(actual, vec![b'x'; 10]);
                    assert_eq!(actual, bytes(final_response, 10).await);
                }
                (Err(plain), Err(OutboundFollowFailure::Hop { index, error })) => {
                    assert_eq!(length, 11);
                    assert_eq!(index, 0);
                    assert_eq!(plain.status(), error.status());
                    assert_eq!(plain.message(), error.message());
                    match (plain, error) {
                        (
                            EdgeError::ResponseTooLarge {
                                reason: sent_reason,
                                ..
                            },
                            EdgeError::ResponseTooLarge {
                                reason: followed_reason,
                                ..
                            },
                        ) => {
                            assert_eq!(sent_reason, ResponseLimitReason::BufferedBody);
                            assert_eq!(sent_reason, followed_reason);
                        }
                        other => panic!("expected matching typed limit errors: {other:?}"),
                    }
                }
                other => panic!("plain/follow mismatch: {other:?}"),
            }
            let seen = observations(&probe);
            assert_eq!(seen["requests"].as_array().expect("requests").len(), 2);
        }
    }
}

fn unread_redirect(kind: &str) -> Value {
    let mut head = json!([["location", "/final"]]);
    match kind {
        "brotli" => head
            .as_array_mut()
            .expect("headers")
            .push(json!(["content-encoding", "br"])),
        "oversized" => head
            .as_array_mut()
            .expect("headers")
            .push(json!(["content-length", "10485760"])),
        "error" | "pending" => {}
        _ => panic!("unknown unread fixture kind"),
    }
    json!({"status":302_u16, "kind": if kind == "error" || kind == "pending" { kind } else { "bytes" }, "bytes": [], "headers":head})
}

#[wasm_bindgen_test]
async fn followed_fetch_heads_release_unread_bodies_before_next_hop() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    for kind in ["error", "brotli", "oversized", "pending"] {
        let (probe, _restore) = fetch_script(&json!([
            unread_redirect(kind),
            response(200, b"ok", &json!([]))
        ]));
        let result = client
            .follow(
                request("start", 1024 * 1024),
                OutboundRedirectLimit::new(4).expect("limit"),
                |_| Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared),
            )
            .await
            .expect("follow");
        assert_eq!(result.response().status(), StatusCode::OK);
        assert_eq!(
            result.final_uri().to_string(),
            "https://fixture.invalid/final"
        );
        assert_eq!(result.redirects_followed(), 1);
        let (final_response, _, _, _) = result.into_parts();
        assert_eq!(bytes(final_response, 16).await, b"ok");
        let seen = observations(&probe);
        assert_eq!(seen["requests"].as_array().expect("requests").len(), 2);
        assert_eq!(seen["pulls"][0], 0_u64);
        assert_eq!(seen["aborts"][0], 1_u64);
        assert_eq!(seen["aborts"][1], 0_u64);
    }
}

#[wasm_bindgen_test]
async fn stopped_fetch_heads_retain_decode_source_and_limit_errors() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    for (kind, at_limit) in ["error", "brotli", "oversized"]
        .into_iter()
        .flat_map(|kind| [false, true].map(|at_limit| (kind, at_limit)))
    {
        let replies = if at_limit {
            json!([unread_redirect("pending"), unread_redirect(kind)])
        } else {
            json!([unread_redirect(kind)])
        };
        let (probe, _restore) = fetch_script(&replies);
        let failed = client
            .follow(
                request("start", 1024 * 1024),
                OutboundRedirectLimit::new(1).expect("limit"),
                move |_| {
                    Ok::<_, Infallible>(if at_limit {
                        OutboundRedirectAction::FollowPrepared
                    } else {
                        OutboundRedirectAction::Stop
                    })
                },
            )
            .await
            .expect_err("final body policy");
        let OutboundFollowFailure::Hop { index, error } = failed;
        let final_index = usize::from(at_limit);
        assert_eq!(index, final_index);
        match kind {
            "error" => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Transport,
                    ..
                }
            )),
            "brotli" => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Decode(BadGatewayDecodeReason::Brotli),
                    ..
                }
            )),
            _ => assert!(matches!(
                error,
                EdgeError::ResponseTooLarge {
                    reason: ResponseLimitReason::BufferedBody,
                    ..
                }
            )),
        }
        let seen = observations(&probe);
        assert_eq!(
            seen["requests"].as_array().expect("requests").len(),
            final_index.saturating_add(1)
        );
        assert_eq!(
            seen["pulls"][final_index] == json!(0_u64),
            kind == "oversized"
        );
        // Empty Brotli fails decoding after clean transport EOF has already disarmed abort.
        assert_eq!(
            seen["aborts"][final_index],
            json!(usize::from(kind != "brotli"))
        );
        if at_limit {
            assert_eq!(seen["pulls"][0], 0_u64);
            assert_eq!(seen["aborts"][0], 1_u64);
        }
    }
}

#[wasm_bindgen_test]
async fn final_fetch_redirects_preserve_stop_metadata_and_body() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    for at_limit in [false, true] {
        let final_reply = response(302, b"stop", &json!([["location", "/further"]]));
        let replies = if at_limit {
            json!([unread_redirect("pending"), final_reply])
        } else {
            json!([final_reply])
        };
        let (probe, _restore) = fetch_script(&replies);
        let result = client
            .follow(
                request("start", 16),
                OutboundRedirectLimit::new(1).expect("limit"),
                move |_| {
                    Ok::<_, Infallible>(if at_limit {
                        OutboundRedirectAction::FollowPrepared
                    } else {
                        OutboundRedirectAction::Stop
                    })
                },
            )
            .await
            .expect("final redirect");
        assert_eq!(result.response().status(), StatusCode::FOUND);
        assert_eq!(result.redirects_followed(), usize::from(at_limit));
        assert_eq!(
            result.stop_reason(),
            Some(if at_limit {
                OutboundRedirectStopReason::LimitReached
            } else {
                OutboundRedirectStopReason::StoppedByPolicy
            })
        );
        let (reply, _, _, _) = result.into_parts();
        assert_eq!(bytes(reply, 16).await, b"stop");
        assert_eq!(
            observations(&probe)["requests"]
                .as_array()
                .expect("requests")
                .len(),
            usize::from(at_limit).saturating_add(1)
        );
    }
}

#[wasm_bindgen_test]
async fn streamed_fetch_final_response_matches_plain_send() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    for coding in ["identity", "gzip", "unsupported"] {
        let fixture = response(
            200,
            &encoded(coding, b"0123456789"),
            &json!([["content-encoding", coding]]),
        );
        let (probe, _restore) =
            fetch_script(&json!([fixture, unread_redirect("pending"), fixture]));
        let make_request = || {
            request("start", 10)
                .stream_response()
                .max_encoded_response_bytes(1024)
                .max_decoded_response_bytes(10)
                .max_chunk_bytes(NonZeroU64::new(3).expect("chunk"))
        };
        let plain = client.send(make_request()).await.expect("plain head");
        let followed = client
            .follow(
                make_request(),
                OutboundRedirectLimit::new(4).expect("limit"),
                |_| Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared),
            )
            .await
            .expect("followed head");
        assert!(plain.body().is_stream());
        assert!(followed.response().body().is_stream());
        assert_eq!(plain.status(), followed.response().status());
        assert_eq!(plain.headers(), followed.response().headers());
        let before = observations(&probe);
        assert_eq!(before["pulls"], json!([0_u64, 0_u64, 0_u64]));
        let (final_response, _, _, _) = followed.into_parts();
        let actual = bytes(plain, 10).await;
        assert_eq!(actual, b"0123456789");
        assert_eq!(actual, bytes(final_response, 10).await);
        assert_eq!(observations(&probe)["aborts"], json!([0_u64, 1_u64, 0_u64]));
    }
}

#[wasm_bindgen_test]
async fn stopped_pending_fetch_body_keeps_original_absolute_deadline() {
    let _timers = NumericTimeoutGuard::install();
    for at_limit in [false, true] {
        let now = Arc::new(Mutex::new(MonotonicInstant::now()));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
        let start = clock.now();
        let deadline =
            Deadline::at_instant(start.checked_add(Duration::from_secs(3)).expect("deadline"));
        let client =
            HttpClient::with_client(CloudflareOutboundClient::with_time(native_time(clock)));
        let replies = if at_limit {
            json!([unread_redirect("pending"), unread_redirect("pending")])
        } else {
            json!([unread_redirect("pending")])
        };
        let (probe, _restore) = fetch_script(&replies);
        let mut operation = client.follow(
            request("start", 16).deadline(deadline),
            OutboundRedirectLimit::new(1).expect("limit"),
            move |_| {
                Ok::<_, Infallible>(if at_limit {
                    OutboundRedirectAction::FollowPrepared
                } else {
                    OutboundRedirectAction::Stop
                })
            },
        );
        let final_index = usize::from(at_limit);
        for _ in 0_u8..4 {
            assert!(operation.as_mut().now_or_never().is_none());
            HostEventYield::after(Duration::from_millis(1))
                .await
                .expect("fetch settles");
        }
        assert!(
            observations(&probe)["pulls"][final_index]
                .as_u64()
                .expect("pulls")
                > 0
        );
        *now.lock().expect("clock") = deadline.instant();
        let OutboundFollowFailure::Hop { index, error } = operation.await.expect_err("deadline");
        assert_eq!(index, final_index);
        assert!(matches!(
            error,
            EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            }
        ));
        assert_eq!(
            observations(&probe)["aborts"],
            json!(vec![1_u64; final_index.saturating_add(1)])
        );
        if at_limit {
            assert_eq!(observations(&probe)["pulls"][0], 0_u64);
        }
    }
}

#[wasm_bindgen_test]
async fn dropping_follow_aborts_current_fetch_without_another_hop() {
    let _timers = NumericTimeoutGuard::install();
    let client = HttpClient::with_client(CloudflareOutboundClient::new());
    let (probe, _restore) = fetch_script(&json!([
        unread_redirect("pending"), {"kind":"pending-head"}
    ]));
    let mut operation = client.follow(
        request("start", 16),
        OutboundRedirectLimit::new(4).expect("limit"),
        |_| Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared),
    );
    assert!(operation.as_mut().now_or_never().is_none());
    HostEventYield::after(Duration::from_millis(1))
        .await
        .expect("first head");
    assert!(operation.as_mut().now_or_never().is_none());
    let seen = observations(&probe);
    assert_eq!(seen["requests"].as_array().expect("requests").len(), 2);
    assert_eq!(seen["aborts"], json!([1_u64, 0_u64]));
    drop(operation);
    let cancelled = observations(&probe);
    assert_eq!(cancelled["requests"].as_array().expect("requests").len(), 2);
    assert_eq!(cancelled["pulls"], json!([0_u64, 0_u64]));
    assert_eq!(cancelled["aborts"], json!([1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn replacement_fetch_stream_cannot_extend_original_chain_deadline() {
    let _timers = NumericTimeoutGuard::install();
    let now = Arc::new(Mutex::new(MonotonicInstant::now()));
    let sampled = Arc::clone(&now);
    let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
    let deadline = Deadline::at_instant(
        clock
            .now()
            .checked_add(Duration::from_secs(3))
            .expect("deadline"),
    );
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(native_time(clock)));
    let (probe, _restore) = fetch_script(&json!([
        unread_redirect("pending"), {"status":200_u16,"kind":"pending","headers":[]}
    ]));
    let result = client
        .follow(
            request("start", 16).deadline(deadline),
            OutboundRedirectLimit::new(4).expect("limit"),
            |_| {
                Ok::<_, Infallible>(OutboundRedirectAction::Follow(
                    request("replacement", 16).stream_response(),
                ))
            },
        )
        .await
        .expect("stream head");
    assert!(result.response().body().is_stream());
    assert_eq!(
        result.final_uri().to_string(),
        "https://fixture.invalid/replacement"
    );
    assert_eq!(observations(&probe)["pulls"], json!([0_u64, 0_u64]));
    *now.lock().expect("clock") = deadline.instant();
    let (reply, _, _, _) = result.into_parts();
    assert!(matches!(
        reply.into_bytes_bounded(16).await,
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::RequestDeadline,
            ..
        })
    ));
    assert_eq!(observations(&probe)["pulls"], json!([0_u64, 0_u64]));
    assert_eq!(observations(&probe)["aborts"], json!([1_u64, 1_u64]));
}
