use core::convert::Infallible;
use std::io::{Error as IoError, Write as _};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edgezero_core::http::StatusCode;
use edgezero_core::{
    BadGatewayDecodeReason, BadGatewayReason, BudgetSource, ContentTypeProvenance, Deadline,
    EdgeError, HttpClient, MonotonicClock, MonotonicInstant, OutboundBatchCheck,
    OutboundBatchTermination, OutboundCollectedSlot, OutboundFollowFailure, OutboundRedirectAction,
    OutboundRedirectLimit, OutboundRedirectStopReason, OutboundRequest, OutboundResponse,
    OutboundTime, ResponseLimitReason,
};
use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use futures::channel::oneshot;
use futures_util::FutureExt as _;
use serde_json::{Value, json};
use wasm_bindgen_test::wasm_bindgen_test;
use worker::js_sys::{Function, JSON, Reflect};
use worker::wasm_bindgen::{JsCast as _, JsValue};

use crate::test_runtime::NumericTimeoutGuard;

use super::clock_tests::RestoreGlobal;
use super::{CloudflareOutboundClient, HostEventYield, native_time};

type TimerSignal = (Deadline, Option<oneshot::Sender<Result<(), EdgeError>>>);

struct BatchTimeControl {
    drop_cost: Arc<Mutex<Duration>>,
    epoch: MonotonicInstant,
    now: Arc<Mutex<MonotonicInstant>>,
    signals: Arc<Mutex<Vec<TimerSignal>>>,
}

struct TimerDropCost {
    cost: Arc<Mutex<Duration>>,
    now: Arc<Mutex<MonotonicInstant>>,
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for TimerDropCost {
    fn drop(&mut self) {
        let cost = *self.cost.lock().expect("timer release cost");
        let mut now = self.now.lock().expect("manual clock");
        *now = now.checked_add(cost).expect("bounded release cost");
    }
}

impl BatchTimeControl {
    fn new() -> (Self, OutboundTime) {
        let epoch = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(epoch));
        let observed = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *observed.lock().expect("manual clock"));
        let signals = Arc::new(Mutex::new(Vec::new()));
        let registered = Arc::clone(&signals);
        let drop_cost = Arc::new(Mutex::new(Duration::ZERO));
        let release_cost = Arc::clone(&drop_cost);
        let release_clock = Arc::clone(&now);
        let time = OutboundTime::new(clock, move |deadline, _clock| {
            let (sender, receiver) = oneshot::channel();
            registered
                .lock()
                .expect("timer registrations")
                .push((deadline, Some(sender)));
            let release = TimerDropCost {
                cost: Arc::clone(&release_cost),
                now: Arc::clone(&release_clock),
            };
            Box::pin(async move {
                let _retained = release;
                receiver.await.expect("owned timer signal")
            })
        });
        (
            Self {
                drop_cost,
                epoch,
                now,
                signals,
            },
            time,
        )
    }

    fn active_timers(&self) -> usize {
        self.signals
            .lock()
            .expect("timer registrations")
            .iter()
            .filter(|(_, signal)| signal.as_ref().is_some_and(|sender| !sender.is_canceled()))
            .count()
    }

    fn advance_to(&self, milliseconds: u64) {
        *self.now.lock().expect("manual clock") = self
            .epoch
            .checked_add(Duration::from_millis(milliseconds))
            .expect("manual observation");
    }

    fn deadline(&self, milliseconds: u64) -> Deadline {
        Deadline::at_instant(
            self.epoch
                .checked_add(Duration::from_millis(milliseconds))
                .expect("original deadline"),
        )
    }

    fn wake_first(&self, deadline: Deadline, outcome: Result<(), EdgeError>) {
        let sender = self
            .signals
            .lock()
            .expect("timer registrations")
            .iter_mut()
            .find_map(|(registered, signal)| {
                if registered.instant() == deadline.instant()
                    && signal.as_ref().is_some_and(|sender| !sender.is_canceled())
                {
                    signal.take()
                } else {
                    None
                }
            })
            .expect("live original-deadline timer");
        sender.send(outcome).expect("timer still observed");
    }
}

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
            HostEventYield::new().await.expect("fetch settles");
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
    HostEventYield::new().await.expect("first head");
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

#[wasm_bindgen_test]
async fn batch_cutoff_does_not_wait_for_each_child_timer_notification() {
    let _timers = NumericTimeoutGuard::install();
    for kind in ["pending-head", "pending"] {
        let (control, time) = BatchTimeControl::new();
        let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
        let pending = json!({"status":200_u16,"kind":kind,"headers":[]});
        let (probe, restore) = fetch_script(&json!([pending, pending, pending]));
        control.advance_to(34);
        let cutoff = control.deadline(200);
        let mut batch = client.start_batch_until(
            (0_u8..3)
                .map(|index| request(&format!("pending-{index}"), 16))
                .collect(),
            cutoff,
        );
        HostEventYield::new()
            .await
            .expect("exchanges enter pending");
        let started = observations(&probe);
        assert_eq!(started["requests"].as_array().expect("requests").len(), 3);
        let expected_pulls = [u64::from(kind == "pending"); 3];
        assert_eq!(started["pulls"], json!(expected_pulls));
        control.advance_to(200);
        control.wake_first(cutoff, Ok(()));
        HostEventYield::new().await.expect("one timer notification");
        let terminal = batch.check().expect("cutoff observation");
        let released = observations(&probe)["aborts"].clone();
        assert_eq!(observations(&probe)["pulls"], started["pulls"]);
        assert_eq!(control.active_timers(), 0);
        control.advance_to(207);
        let retained = batch.check().expect("retained terminal");
        let collected = batch.collect().now_or_never();
        let after_drop = observations(&probe)["aborts"].clone();
        drop(restore);
        assert!(matches!(
            terminal,
            OutboundBatchCheck::Finished(OutboundBatchTermination::Cutoff)
        ));
        assert!(matches!(
            retained,
            OutboundBatchCheck::Finished(OutboundBatchTermination::Cutoff)
        ));
        assert_eq!(released, json!([1_u64, 1_u64, 1_u64]));
        assert_eq!(after_drop, released);
        let results = collected
            .expect("no further child wake needed")
            .expect("valid cutoff");
        assert!(
            results
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved))
        );
    }
}

#[wasm_bindgen_test]
async fn batch_cutoff_final_poll_preserves_an_unnotified_earlier_deadline() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"status":200_u16,"kind":"pending","headers":[]});
    let (probe, restore) = fetch_script(&json!([pending, pending, pending]));
    control.advance_to(34);
    let cutoff = control.deadline(200);
    let batch = client.start_batch_until(
        vec![
            request("own-deadline", 16).timeout(Duration::from_millis(46)),
            request("cutoff-first", 16),
            request("cutoff-second", 16),
        ],
        cutoff,
    );
    HostEventYield::new().await.expect("pending native reads");
    assert_eq!(observations(&probe)["pulls"], json!([1_u64, 1_u64, 1_u64]));
    control.advance_to(200);
    control.wake_first(cutoff, Ok(()));
    HostEventYield::new()
        .await
        .expect("only cutoff timer wakes");
    let result = batch.collect().now_or_never();
    let released = observations(&probe)["aborts"].clone();
    drop(restore);
    let results = result
        .expect("no own-timer notification needed")
        .expect("valid cutoff");
    assert_eq!(results.termination(), OutboundBatchTermination::Cutoff);
    let [
        OutboundCollectedSlot::Resolved(earlier),
        OutboundCollectedSlot::Unresolved,
        OutboundCollectedSlot::Unresolved,
    ] = results.slots()
    else {
        panic!("earlier timeout must survive cutoff: {:?}", results.slots());
    };
    assert_eq!(earlier.elapsed, Duration::from_millis(46));
    assert!(matches!(
        earlier.outcome,
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::PerCallTimeout,
            ..
        })
    ));
    assert_eq!(released, json!([1_u64, 1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn batch_cutoff_timer_failure_retains_partial_results_and_identity() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"status":200_u16,"kind":"pending","headers":[]});
    let (probe, restore) = fetch_script(&json!([
        response(200, b"ready", &json!([])),
        pending,
        pending
    ]));
    control.advance_to(34);
    let cutoff = control.deadline(200);
    let mut batch = client.start_batch_until(
        (0_u8..3)
            .map(|index| request(&format!("slot-{index}"), 16))
            .collect(),
        cutoff,
    );
    let mut accumulated = batch.accumulator();
    for _ in 0_u8..2 {
        HostEventYield::new()
            .await
            .expect("successful slot settles");
    }
    let OutboundBatchCheck::Item(item) = batch.check().expect("recorded partial") else {
        panic!("ready slot must be published before cutoff");
    };
    assert_eq!(item.index(), 0);
    accumulated.insert(item).expect("original receipt");
    let identity = Arc::new(IoError::other("batch timer failure"));
    control.advance_to(200);
    control.wake_first(
        cutoff,
        Err(EdgeError::internal(anyhow::anyhow!(Arc::clone(&identity)))),
    );
    HostEventYield::new()
        .await
        .expect("timer error wakes producer");
    let collected = batch.collect().now_or_never();
    let released = observations(&probe)["aborts"].clone();
    drop(restore);
    let failure = collected
        .expect("failed timer terminates promptly")
        .expect_err("timer failure");
    let merged_failure = accumulated
        .merge_failure(failure)
        .expect("retain completed slot");
    let original = merged_failure
        .error()
        .inner()
        .expect("original timer error")
        .downcast_ref::<Arc<IoError>>()
        .expect("original timer identity");
    assert!(Arc::ptr_eq(original, &identity));
    assert!(
        matches!(merged_failure.slots(), [OutboundCollectedSlot::Resolved(ready), OutboundCollectedSlot::Unresolved, OutboundCollectedSlot::Unresolved] if ready.outcome.is_ok())
    );
    assert_eq!(released, json!([0_u64, 1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn batch_cutoff_retains_an_operational_child_timer_error_before_expiry() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"status":200_u16,"kind":"pending","headers":[]});
    let (probe, restore) = fetch_script(&json!([pending, pending]));
    control.advance_to(34);
    let cutoff = control.deadline(200);
    let batch = client.start_batch_until(
        vec![
            request("own", 16).timeout(Duration::from_millis(46)),
            request("cutoff", 16),
        ],
        cutoff,
    );
    HostEventYield::new().await.expect("registered child timer");
    let identity = Arc::new(IoError::other("child timer failure"));
    control.advance_to(79);
    control.wake_first(
        control.deadline(80),
        Err(EdgeError::internal(anyhow::anyhow!(Arc::clone(&identity)))),
    );
    HostEventYield::new()
        .await
        .expect("child failure is recorded");
    control.advance_to(200);
    control.wake_first(cutoff, Ok(()));
    HostEventYield::new().await.expect("cutoff retires sibling");
    let collected = batch.collect().now_or_never();
    let released = observations(&probe)["aborts"].clone();
    drop(restore);
    let results = collected.expect("terminal batch").expect("valid cutoff");
    let [
        OutboundCollectedSlot::Resolved(child),
        OutboundCollectedSlot::Unresolved,
    ] = results.slots()
    else {
        panic!("child failure must be retained");
    };
    let original = child
        .outcome
        .as_ref()
        .expect_err("operational failure")
        .inner()
        .expect("typed timer error")
        .downcast_ref::<Arc<IoError>>()
        .expect("original identity");
    assert!(Arc::ptr_eq(original, &identity));
    assert_eq!(child.elapsed, Duration::from_millis(45));
    assert_eq!(released, json!([1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn batch_cutoff_includes_synchronous_cleanup_cost_before_terminal() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"status":200_u16,"kind":"pending","headers":[]});
    let (probe, restore) = fetch_script(&json!([pending, pending, pending]));
    control.advance_to(34);
    let cutoff = control.deadline(200);
    let mut batch = client.start_batch_until(
        (0_u8..3)
            .map(|index| request(&format!("pending-{index}"), 16))
            .collect(),
        cutoff,
    );
    HostEventYield::new().await.expect("pending bodies");
    control.advance_to(200);
    *control.drop_cost.lock().expect("release cost") = Duration::from_millis(2);
    control.wake_first(cutoff, Ok(()));
    HostEventYield::new().await.expect("cutoff sweep");
    let terminal = batch.check().expect("terminal after release");
    let observed = *control.now.lock().expect("manual clock");
    let released = observations(&probe)["aborts"].clone();
    let active = control.active_timers();
    drop(batch);
    drop(restore);
    assert!(matches!(
        terminal,
        OutboundBatchCheck::Finished(OutboundBatchTermination::Cutoff)
    ));
    assert_eq!(observed, control.deadline(208).instant());
    assert_eq!(released, json!([1_u64, 1_u64, 1_u64]));
    assert_eq!(active, 0);
}

#[wasm_bindgen_test]
async fn batch_cutoff_timer_retires_on_normal_completion_and_cancellation() {
    let _timers = NumericTimeoutGuard::install();
    for complete in [false, true] {
        let (control, time) = BatchTimeControl::new();
        let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
        let fixture = if complete {
            response(200, b"ready", &json!([]))
        } else {
            json!({"status":200_u16,"kind":"pending","headers":[]})
        };
        let (probe, restore) = fetch_script(&json!([fixture, fixture]));
        control.advance_to(34);
        let batch = client.start_batch_until(
            vec![request("first", 16), request("second", 16)],
            control.deadline(200),
        );
        for _ in 0_u8..2 {
            HostEventYield::new().await.expect("scheduled exchanges");
        }
        if complete {
            let results = batch
                .collect()
                .now_or_never()
                .expect("ready batch")
                .expect("completion");
            assert_eq!(results.termination(), OutboundBatchTermination::Completed);
        } else {
            assert_eq!(batch.cancel(), vec![0, 1]);
        }
        let active = control.active_timers();
        let released = observations(&probe)["aborts"].clone();
        drop(restore);
        assert_eq!(active, 0);
        let expected = [u64::from(!complete); 2];
        assert_eq!(released, json!(expected));
    }
}

#[wasm_bindgen_test]
async fn batch_cutoff_all_earlier_own_deadlines_complete_without_cutoff() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"status":200_u16,"kind":"pending","headers":[]});
    let (probe, restore) = fetch_script(&json!([pending, pending]));
    control.advance_to(34);
    let cutoff = control.deadline(200);
    let batch = client.start_batch_until(
        vec![
            request("first-own", 16).timeout(Duration::from_millis(46)),
            request("second-own", 16).timeout(Duration::from_millis(46)),
        ],
        cutoff,
    );
    HostEventYield::new().await.expect("pending own deadlines");
    control.advance_to(200);
    control.wake_first(cutoff, Ok(()));
    HostEventYield::new()
        .await
        .expect("only batch wake delivered");
    let collected = batch.collect().now_or_never();
    let released = observations(&probe)["aborts"].clone();
    drop(restore);
    let results = collected
        .expect("no own wake needed")
        .expect("completed own timeouts");
    assert_eq!(results.termination(), OutboundBatchTermination::Completed);
    assert!(results.slots().iter().all(|slot| matches!(slot, OutboundCollectedSlot::Resolved(result) if result.elapsed == Duration::from_millis(46) && matches!(result.outcome, Err(EdgeError::GatewayTimeout { cause: BudgetSource::PerCallTimeout, .. })))));
    assert_eq!(released, json!([1_u64, 1_u64]));
}

#[wasm_bindgen_test]
async fn batch_cutoff_detects_expiry_crossed_during_child_cleanup() {
    let _timers = NumericTimeoutGuard::install();
    let (control, time) = BatchTimeControl::new();
    let client = HttpClient::with_client(CloudflareOutboundClient::with_time(time));
    let pending = json!({"kind":"pending-head"});
    let (probe, restore) = fetch_script(&json!([pending, pending]));
    control.advance_to(34);
    let batch = client.start_batch_until(
        vec![
            request("own", 16).timeout(Duration::from_millis(46)),
            request("cutoff", 16),
        ],
        control.deadline(200),
    );
    HostEventYield::new().await.expect("pending native heads");
    control.advance_to(199);
    *control.drop_cost.lock().expect("release cost") = Duration::from_millis(1);
    control.wake_first(control.deadline(80), Ok(()));
    HostEventYield::new()
        .await
        .expect("child cleanup crosses cutoff");
    let collected = batch.collect().now_or_never();
    let released = observations(&probe)["aborts"].clone();
    drop(restore);
    let results = collected
        .expect("cutoff cannot park after cleanup crosses it")
        .expect("valid cutoff");
    assert_eq!(results.termination(), OutboundBatchTermination::Cutoff);
    assert!(
        matches!(results.slots(), [OutboundCollectedSlot::Resolved(own), OutboundCollectedSlot::Unresolved] if own.elapsed == Duration::from_millis(46))
    );
    assert_eq!(released, json!([1_u64, 1_u64]));
}
