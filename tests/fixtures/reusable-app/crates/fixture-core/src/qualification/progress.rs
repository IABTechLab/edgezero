//! Fixture-only orchestration; SDK operations stay in the provider fixture crates.

use bytes::Bytes;
use edgezero_core::app::{App, Hooks};
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Method, Response, Uri, response_builder};
use edgezero_core::outbound::{
    OutboundCachePolicy, OutboundProxyMarker, OutboundRequest, ResponseDecoding,
};
use edgezero_core::response_egress::{ResponseEgressObserver, ResponseEgressReport};
use edgezero_core::router::RouterService;
use edgezero_core::time::{Deadline, MonotonicInstant};
use edgezero_core::{Body, EdgeError, action};
use futures::{
    StreamExt,
    future::{LocalBoxFuture, join_all, poll_fn},
    task::AtomicWaker,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

pub type Job = LocalBoxFuture<'static, Result<u64, EdgeError>>;

pub trait ProbeRuntime: Send + Sync {
    fn adapter(&self) -> &'static str;
    fn perform(&self, target: String) -> Job;
    fn start(&self, target: String) -> Result<Job, EdgeError>;
    fn pause(&self, duration: Duration) -> LocalBoxFuture<'static, ()>;
}

pub struct ProgressHooks;
impl Hooks for ProgressHooks {
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder().build())
    }
}

#[derive(Default)]
pub struct DispatchReturn {
    returned: AtomicBool,
    waiting: AtomicWaker,
}

impl DispatchReturn {
    pub fn mark_returned(&self) {
        if !self.returned.swap(true, Ordering::SeqCst) {
            self.waiting.wake();
        }
    }

    pub fn is_returned(&self) -> bool {
        self.returned.load(Ordering::SeqCst)
    }

    pub async fn wait(&self) {
        poll_fn(|context| {
            if self.is_returned() {
                return std::task::Poll::Ready(());
            }
            self.waiting.register(context.waker());
            if self.is_returned() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
    }
}

struct State {
    runtime: Arc<dyn ProbeRuntime>,
    host_signal: Value,
    dispatch_returned: Option<Arc<DispatchReturn>>,
}

pub(super) fn pause(
    ctx: &RequestContext,
    duration: Duration,
) -> Result<LocalBoxFuture<'static, ()>, EdgeError> {
    let state = ctx
        .extensions()
        .get::<Arc<State>>()
        .ok_or_else(probe_failure)?;
    Ok(state.runtime.pause(duration))
}

pub(super) fn dispatch_returned(ctx: &RequestContext) -> Option<Arc<DispatchReturn>> {
    ctx.extensions()
        .get::<Arc<State>>()
        .and_then(|state| state.dispatch_returned.clone())
}

struct HandlerReturned(Arc<AtomicBool>);

struct FramingObserver;

impl ResponseEgressObserver for FramingObserver {
    fn complete(&self, report: &ResponseEgressReport) {
        if report.framing_failures.application().is_some()
            || report.framing_failures.fallback().is_some()
        {
            eprintln!(
                "{}",
                json!({"event":"framing-terminal", "application":report.framing_failures.application().map(|reason|format!("{reason:?}")),
                "fallback":report.framing_failures.fallback().map(|reason|format!("{reason:?}")),
                "outcome":format!("{:?}",report.outcome),"body_kind":format!("{:?}",report.body_kind),
                "bytes_written":report.bytes_written,"disposition":report.fallback_disposition.map(|value|format!("{value:?}")),"qualification":false})
            );
        }
    }
}
impl Drop for HandlerReturned {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub fn router(
    runtime: Arc<dyn ProbeRuntime>,
    host_signal: Value,
    dispatch_returned: Option<Arc<DispatchReturn>>,
) -> RouterService {
    RouterService::builder()
        .with_state(Arc::new(State {
            runtime,
            host_signal,
            dispatch_returned,
        }))
        .get("/progress/framing-trailers", framing_trailers)
        .get("/progress/framing/{case}", super::framing::probe)
        .route(
            "/progress/framing/{case}",
            Method::HEAD,
            super::framing::probe,
        )
        .get(
            "/progress/synthesized/{case}",
            super::response_headers::probe,
        )
        .route(
            "/progress/synthesized/{case}",
            Method::HEAD,
            super::response_headers::probe,
        )
        .get("/progress/redirect", super::redirect::probe)
        .get("/progress/lifetimes/{case}", super::lifetimes::probe)
        .get("/progress/{case}", progress)
        .post("/progress/{case}", progress)
        .post("/progress/overflow/{case}", super::overflow::probe)
        .build()
}

pub fn application(
    runtime: Arc<dyn ProbeRuntime>,
    host_signal: Value,
    dispatch_returned: Option<Arc<DispatchReturn>>,
    platform: edgezero_core::PlatformMetadata,
) -> Result<App, EdgeError> {
    let mut app = App::with_name_and_platform(
        router(runtime, host_signal, dispatch_returned),
        "outbound-progress-probe",
        platform,
    );
    app.set_response_egress_observer(FramingObserver);
    super::response_headers::configure(&mut app)?;
    Ok(app)
}

#[action]
async fn framing_trailers() -> Result<Response, EdgeError> {
    response_builder()
        .header("trailer", "private-token-secret")
        .body(Body::from("private-token-secret"))
        .map_err(EdgeError::internal)
}

pub fn probe_failure() -> EdgeError {
    EdgeError::internal(std::io::Error::other("outbound progress SDK probe failed"))
}

fn observed(mut job: Job, closed: Arc<AtomicBool>, polls: Arc<AtomicUsize>) -> Job {
    Box::pin(poll_fn(move |cx| {
        if !closed.load(Ordering::SeqCst) {
            polls.fetch_add(1, Ordering::SeqCst);
        }
        job.as_mut().poll(cx)
    }))
}

#[action]
async fn progress(ctx: RequestContext) -> Result<Response, EdgeError> {
    let state = ctx
        .extensions()
        .get::<Arc<State>>()
        .cloned()
        .ok_or_else(probe_failure)?;
    let case = ctx
        .path_params()
        .get("case")
        .ok_or_else(probe_failure)?
        .to_owned();
    if case == "empty" {
        let before = format!("{:?}", ctx.body_kind());
        let request = ctx.into_request()?;
        let streamed = request.body().is_stream();
        let method = request.method().as_str().to_owned();
        let body = request.into_body().into_bytes_bounded(64).await?;
        return json_response(json!({"case":case, "adapter":state.runtime.adapter(),
            "host_signal":state.host_signal, "body_kind_before":before, "body_is_stream":streamed,
            "bytes_read":body.len(), "method":method, "qualification":false}));
    }
    if case.starts_with("coding-") || case.starts_with("relay-") {
        return coding_probe(ctx, &case).await;
    }
    if case.starts_with("marker-") {
        return marker_probe(ctx, &case).await;
    }
    let token = ctx
        .headers()
        .get("x-request-token")
        .and_then(|v| v.to_str().ok())
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 64
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        .ok_or_else(|| EdgeError::bad_request("invalid probe token"))?
        .to_owned();
    if matches!(
        case.as_str(),
        "response-owned-ready" | "response-owned-timer-only"
    ) {
        return response_control(state, case, token);
    }
    let slots = match case.as_str() {
        "eager-single" | "lazy-single" => 1,
        "eager-batch" | "baseline-batch" | "response-owned" => 2,
        _ => return Err(EdgeError::bad_request("unknown progress probe")),
    };
    let base: Uri = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| EdgeError::bad_request("missing probe origin"))?
        .parse()
        .map_err(|_| EdgeError::bad_request("invalid probe origin"))?;
    if !crate::is_loopback_http(&base)
        || base.port_u16().is_none()
        || base.query().is_some()
        || base.path() != format!("/g3/{token}")
    {
        return Err(EdgeError::bad_request(
            "probe origin must be the controlled loopback path",
        ));
    }
    state.runtime.perform(format!("{base}/start")).await?;
    let closed = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let _returned = HandlerReturned(Arc::clone(&returned));
    let mut jobs = Vec::new();
    if case == "lazy-single" || case == "baseline-batch" {
        let client = ctx.http_client().ok_or_else(probe_failure)?;
        let requests = (0..slots)
            .map(|index| {
                OutboundRequest::new(
                    Method::GET,
                    format!("{base}/slot-{index}")
                        .parse()
                        .map_err(|_| probe_failure())?,
                )
                .map(|r| {
                    r.timeout(Duration::from_secs(2))
                        .max_response_bytes(64)
                        .cache_policy(OutboundCachePolicy::Bypass)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let job: Job = if case == "lazy-single" {
            Box::pin(async move {
                client
                    .send(requests.into_iter().next().ok_or_else(probe_failure)?)
                    .await?;
                Ok(0)
            })
        } else {
            let batch = client.start_batch_until(requests, Deadline::after(Duration::from_secs(3)));
            Box::pin(async move {
                let results = batch.collect().await.map_err(|_| probe_failure())?;
                if results.slots().len() != slots
                    || results
                        .slots()
                        .iter()
                        .any(|s| !matches!(s, edgezero_core::OutboundCollectedSlot::Resolved(r) if r.outcome.is_ok()))
                {
                    return Err(probe_failure());
                }
                Ok(0)
            })
        };
        jobs.push(observed(job, Arc::clone(&closed), Arc::clone(&polls)));
    } else {
        for index in 0..slots {
            let job = state.runtime.start(format!("{base}/slot-{index}"))?;
            jobs.push(observed(job, Arc::clone(&closed), Arc::clone(&polls)));
        }
    }
    let response_owned = case == "response-owned";
    let finish = async move {
        let pause_start = MonotonicInstant::now();
        state.runtime.pause(Duration::from_millis(100)).await;
        let pause_ns = pause_start.elapsed().as_nanos() as u64;
        state.runtime.perform(format!("{base}/end")).await?;
        closed.store(true, Ordering::SeqCst);
        let sizes = join_all(jobs)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, EdgeError>(
            json!({"adapter":state.runtime.adapter(), "case":case, "token":token, "slots":slots,
            "consumer_polls_before_window_end":polls.load(Ordering::SeqCst), "sizes":sizes,"pause_ns":pause_ns,
            "handler_returned":returned.load(Ordering::SeqCst),
            "dispatch_returned":state.dispatch_returned.as_ref().map(|r|r.is_returned()),
            "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"), "qualification":false}),
        )
    };
    if response_owned {
        let stream = futures::stream::once(async { Ok(Bytes::from_static(b" ")) }).chain(
            futures::stream::once(async move { finish.await.map(|v| Bytes::from(v.to_string())) }),
        );
        return response_builder()
            .header("content-type", "application/json")
            .body(Body::from_stream(stream))
            .map_err(EdgeError::internal);
    }
    json_response(finish.await?)
}

async fn marker_probe(ctx: RequestContext, case: &str) -> Result<Response, EdgeError> {
    let target: Uri = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(probe_failure)?
        .parse()
        .map_err(|_| probe_failure())?;
    let client = ctx
        .http_client()
        .ok_or_else(probe_failure)?
        .with_proxy_marker(false);
    let parts: Vec<_> = case.split('-').collect();
    let (method, path, setting) = if case == "marker-batch" {
        (
            Method::GET,
            "/marker/get/200/absent".to_owned(),
            OutboundProxyMarker::Inherit,
        )
    } else {
        let [
            "marker",
            method @ ("get" | "head"),
            status @ ("200" | "204" | "304" | "302"),
            shape @ ("absent" | "upstream" | "nominated"),
            setting @ ("inherit" | "on" | "off"),
        ] = parts.as_slice()
        else {
            return Err(EdgeError::bad_request("invalid marker probe"));
        };
        let setting = match *setting {
            "on" => OutboundProxyMarker::On,
            "off" => OutboundProxyMarker::Off,
            _ => OutboundProxyMarker::Inherit,
        };
        (
            if *method == "head" {
                Method::HEAD
            } else {
                Method::GET
            },
            format!("/marker/{method}/{status}/{shape}"),
            setting,
        )
    };
    if !crate::is_loopback_http(&target)
        || target.port_u16().is_none()
        || target.query().is_some()
        || target.path() != path
    {
        return Err(EdgeError::bad_request(
            "marker probe requires controlled loopback origin",
        ));
    }
    if case == "marker-batch" {
        let requests = [OutboundProxyMarker::Inherit, OutboundProxyMarker::On]
            .into_iter()
            .map(|setting| {
                OutboundRequest::new(Method::GET, target.clone())
                    .map(|request| request.proxy_marker(setting).max_response_bytes(0))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let results = client
            .send_all_until(
                requests,
                ctx.monotonic_clock().deadline_after(Duration::from_secs(2)),
            )
            .await
            .map_err(|failure| failure.into_parts().0)?;
        let mut markers = Vec::new();
        for slot in results.into_parts().0 {
            let edgezero_core::OutboundCollectedSlot::Resolved(slot) = slot else {
                return Err(probe_failure());
            };
            let response = slot.outcome?;
            markers.push(
                response
                    .headers()
                    .get("x-edgezero-proxy")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned),
            );
            response.into_bytes_bounded(0).await?;
        }
        return json_response(json!({"case":case,"markers":markers,"qualification":false}));
    }
    let request = OutboundRequest::new(method, target)?
        .proxy_marker(setting)
        .max_response_bytes(0);
    let enabled = client.effective_proxy_marker_enabled(&request);
    let response = client.send(request).await?;
    let status = response.status().as_u16();
    let markers: Vec<_> = response
        .headers()
        .get_all("x-edgezero-proxy")
        .iter()
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .collect();
    response.into_bytes_bounded(0).await?;
    json_response(
        json!({"case":case,"status":status,"markers":markers,"enabled":enabled,"qualification":false}),
    )
}

async fn coding_probe(ctx: RequestContext, case: &str) -> Result<Response, EdgeError> {
    let fields: Vec<_> = case.split('-').collect();
    let [
        kind @ ("coding" | "relay"),
        coding @ ("gzip" | "deflate" | "br"),
        mode @ ("buffered" | "streamed"),
        status @ ("200" | "206"),
        choice @ ("original" | "decode"),
    ] = fields.as_slice()
    else {
        return Err(EdgeError::bad_request("invalid coding probe"));
    };
    let target: Uri = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(probe_failure)?
        .parse()
        .map_err(|_| probe_failure())?;
    if !crate::is_loopback_http(&target)
        || target.port_u16().is_none()
        || target.query().is_some()
        || target.path() != format!("/coding/{coding}/{status}")
    {
        return Err(EdgeError::bad_request(
            "coding probe requires controlled loopback origin",
        ));
    }
    let original = *choice == "original" || *status == "206";
    let request = OutboundRequest::new(Method::GET, target)?
        .header("accept-encoding", "gzip, deflate, br")?
        .cache_policy(OutboundCachePolicy::Bypass)
        .response_decoding(if *choice == "original" {
            ResponseDecoding::Original
        } else {
            ResponseDecoding::Decode
        })
        .max_decoded_response_bytes(if original { 1 } else { 64 })
        .max_decoder_bytes(if original {
            0
        } else {
            edgezero_core::DEFAULT_MAX_DECODER_BYTES
        })
        .max_encoded_response_bytes(256)
        .max_response_bytes(256)
        .timeout(Duration::from_secs(2));
    let request = if *mode == "streamed" {
        request.stream_response()
    } else {
        request
    };
    let client = ctx.http_client().ok_or_else(probe_failure)?;
    let client = if *kind == "relay" {
        client.with_proxy_marker(false)
    } else {
        client
    };
    let response = client.send(request).await?;
    if *kind == "relay" {
        return response.into_response();
    }
    let status = response.status().as_u16();
    let encoding = response
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let length = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = response.into_bytes_bounded(256).await?;
    json_response(
        json!({"case":case,"status":status,"content_encoding":encoding,"content_length":length,"bytes":bytes.to_vec(),"qualification":false}),
    )
}

fn json_response(value: Value) -> Result<Response, EdgeError> {
    response_builder()
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .map_err(EdgeError::internal)
}

fn response_control(state: Arc<State>, case: String, token: String) -> Result<Response, EdgeError> {
    let returned = Arc::new(AtomicBool::new(false));
    let _returned = HandlerReturned(Arc::clone(&returned));
    let first_poll = Arc::new(OnceLock::new());
    let (first_state, first_returned, first_snapshot) = (
        Arc::clone(&state),
        Arc::clone(&returned),
        Arc::clone(&first_poll),
    );
    let prefix = futures::stream::once(async move {
        let _ = first_snapshot.set((
            first_returned.load(Ordering::SeqCst),
            first_state
                .dispatch_returned
                .as_ref()
                .map(|r| r.is_returned()),
        ));
        Ok(Bytes::from_static(b" "))
    });
    let tail = futures::stream::once(async move {
        let started = MonotonicInstant::now();
        if case == "response-owned-timer-only" {
            state.runtime.pause(Duration::from_millis(100)).await;
        }
        let pause_ns = started.elapsed().as_nanos() as u64;
        let (first_handler, first_dispatch) =
            first_poll.get().copied().ok_or_else(probe_failure)?;
        let report = json!({"adapter":state.runtime.adapter(),"case":case,"token":token,"pause_ns":pause_ns,
            "handler_returned_at_first_poll":first_handler,"dispatch_returned_at_first_poll":first_dispatch,
            "handler_returned":returned.load(Ordering::SeqCst),"dispatch_returned":state.dispatch_returned.as_ref().map(|r|r.is_returned()),
            "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"),"qualification":false});
        Ok(Bytes::from(report.to_string()))
    });
    response_builder()
        .header("content-type", "application/json")
        .body(Body::from_stream(prefix.chain(tail)))
        .map_err(EdgeError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::{
        BodyPayload, BudgetSource, HttpClient, IndependentOutboundProgress,
        OutboundBatchTermination, OutboundCollectedSlot, OutboundProgressRuntime,
    };
    use futures::executor::block_on;
    use std::rc::Rc;
    use std::sync::Mutex;

    #[test]
    fn dispatch_return_waiter_is_pending_until_marked_and_wakes_once() {
        use std::{
            future::Future,
            task::{Context, Poll, Wake, Waker},
        };
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let signal = DispatchReturn::default();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(signal.wait());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        assert!(!signal.is_returned());
        signal.mark_returned();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(waiting.as_mut().poll(&mut context), Poll::Ready(()));
        drop(waiting);
        signal.mark_returned();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(signal.is_returned());
        assert_eq!(block_on(signal.wait()), ());
    }

    #[derive(Clone, Default)]
    struct Fake(Arc<Mutex<Vec<String>>>);

    impl ProbeRuntime for Fake {
        fn adapter(&self) -> &'static str {
            "test"
        }
        fn perform(&self, target: String) -> Job {
            let events = Arc::clone(&self.0);
            Box::pin(async move {
                events
                    .lock()
                    .unwrap()
                    .push(target.rsplit('/').next().unwrap().into());
                Ok(0)
            })
        }
        fn start(&self, target: String) -> Result<Job, EdgeError> {
            self.0
                .lock()
                .unwrap()
                .push(target.rsplit('/').next().unwrap().into());
            Ok(Box::pin(async { Ok(0) }))
        }
        fn pause(&self, _duration: Duration) -> LocalBoxFuture<'static, ()> {
            let events = Arc::clone(&self.0);
            Box::pin(async move {
                events.lock().unwrap().push("wait".into());
            })
        }
    }

    fn request(case: &str, target: &str) -> edgezero_core::http::Request {
        edgezero_core::http::request_builder()
            .uri(format!("/progress/{case}"))
            .header("x-request-token", "one")
            .header("x-fixture-backend", target)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn lifetime_pause_reuses_only_the_configured_runtime_timer() {
        let runtime = Fake::default();
        let mut ctx =
            RequestContext::new(request("empty", "http://127.0.0.1:1"), Default::default());
        assert!(pause(&ctx, Duration::from_millis(100)).is_err());
        ctx.extensions_mut().insert(Arc::new(State {
            runtime: Arc::new(runtime.clone()),
            host_signal: Value::Null,
            dispatch_returned: None,
        }));
        let timer = pause(&ctx, Duration::from_millis(100)).unwrap();
        assert!(runtime.0.lock().unwrap().is_empty());
        block_on(timer);
        assert_eq!(*runtime.0.lock().unwrap(), ["wait"]);
    }

    #[test]
    fn empty_probe_observes_storage_before_transferring_initial_body() {
        for (body, streamed, expected) in [
            (Body::empty(), false, 0),
            (Body::from_stream(futures::stream::empty()), true, 0),
            (
                Body::from_stream(futures::stream::iter([Ok(Bytes::from_static(b"data"))])),
                true,
                4,
            ),
        ] {
            let router = router(Arc::new(Fake::default()), Value::Null, None);
            let mut input = request("empty", "http://127.0.0.1:1");
            *input.body_mut() = body;
            let response = block_on(router.oneshot(input)).unwrap();
            let probe: Value = serde_json::from_slice(
                &block_on(response.into_body().into_bytes_bounded(4096)).unwrap(),
            )
            .unwrap();
            assert_eq!(probe["body_kind_before"], "Initial");
            assert_eq!(probe["body_is_stream"], streamed);
            assert_eq!(probe["bytes_read"], expected);
        }
    }

    struct RelayClient {
        time: edgezero_core::OutboundTime,
        polls: Arc<AtomicUsize>,
        calls: Arc<Mutex<Vec<Value>>>,
    }

    impl edgezero_core::OutboundHttpClient for RelayClient {
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn time(&self) -> edgezero_core::OutboundTime {
            self.time.clone()
        }

        fn validate_supported_options(
            &self,
            _request: &OutboundRequest,
            _mode: edgezero_core::OutboundSendMode,
        ) -> Result<(), EdgeError> {
            Ok(())
        }

        fn dispatch_head(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> edgezero_core::OutboundFuture<edgezero_core::OutboundHead> {
            let (parts, context) = request.into_parts();
            self.calls.lock().unwrap().push(json!({
                "marker":format!("{:?}",parts.proxy_marker),"mode":format!("{:?}",parts.response_mode),
                "decoding":format!("{:?}",parts.response_decoding),"encoded":parts.max_encoded_response_bytes,
                "decoded":parts.max_decoded_response_bytes,"decoder":parts.max_decoder_bytes,
                "timeout":parts.timeout.map(|t|t.as_millis()),"cache":format!("{:?}",parts.cache_policy),
                "accept_encoding":parts.headers["accept-encoding"].to_str().unwrap(),"uri":parts.uri.to_string(),
            }));
            let raw = parts.response_decoding == ResponseDecoding::Original
                || parts.uri.path().ends_with("206");
            let bytes = if raw {
                Bytes::from_static(&[255, 0, 128])
            } else {
                Bytes::from_static(b"decoded")
            };
            let body = if matches!(parts.response_mode, edgezero_core::ResponseMode::Streamed) {
                let polls = Arc::clone(&self.polls);
                futures::stream::once(async move {
                    polls.fetch_add(1, Ordering::SeqCst);
                    Ok(bytes)
                })
                .boxed_local()
            } else {
                futures::stream::iter([Ok(bytes)]).boxed_local()
            };
            let mut headers = edgezero_core::http::HeaderMap::new();
            headers.insert("content-type", "application/octet-stream".parse().unwrap());
            headers.insert("etag", "\"fixture\"".parse().unwrap());
            if raw {
                headers.insert("content-encoding", "gzip".parse().unwrap());
                headers.insert("content-length", "3".parse().unwrap());
            }
            let head = edgezero_core::OutboundHead::new(
                if parts.uri.path().ends_with("206") {
                    edgezero_core::http::StatusCode::PARTIAL_CONTENT
                } else {
                    edgezero_core::http::StatusCode::OK
                },
                edgezero_core::OutboundHeaderInput::new(
                    "scripted",
                    headers,
                    edgezero_core::OutboundHeaderFidelity::AdapterVisibleFields,
                ),
                body,
                context,
                edgezero_core::OutboundExchangeOwner::new(|| {}),
            );
            Box::pin(futures::future::ready(head))
        }

        fn begin(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> Result<edgezero_core::OutboundPending, EdgeError> {
            let outcome = self.finish_ready(request);
            Ok(edgezero_core::OutboundPending::new(
                Box::pin(futures::future::ready(outcome)),
                edgezero_core::OutboundExchangeOwner::new(|| {}),
            ))
        }

        fn start_batch_until(
            &self,
            batch: edgezero_core::OutboundPreparedBatch,
        ) -> edgezero_core::OutboundBatch {
            let (context, requests) = batch.into_slots();
            if requests.is_empty()
                && context
                    .observation_cutoff()
                    .is_expired_at(context.clock().now())
            {
                return edgezero_core::OutboundBatch::cutoff(context);
            }
            let events = requests
                .into_iter()
                .map(|(index, request)| {
                    let outcome = self.finish_ready(request);
                    edgezero_core::OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: self.time.clock().now(),
                        outcome,
                    }
                })
                .collect::<Vec<_>>();
            edgezero_core::OutboundBatch::from_driver(
                context,
                futures::stream::iter(events),
                edgezero_core::OutboundExchangeOwner::new(|| {}),
            )
        }
    }

    impl RelayClient {
        fn finish_ready(
            &self,
            request: edgezero_core::OutboundDispatchRequest,
        ) -> Result<edgezero_core::OutboundResponse, EdgeError> {
            use edgezero_core::OutboundHttpClient as _;
            use futures::{FutureExt as _, TryFutureExt as _};
            let mut future = Box::pin(
                self.dispatch_head(request)
                    .and_then(edgezero_core::OutboundHead::finish),
            );
            // Fixed one-chunk replies need one cooperative turn before EOF.
            for _ in 0..2 {
                if let Some(outcome) = future.as_mut().now_or_never() {
                    return outcome;
                }
            }
            panic!("scripted response did not complete within its fixed poll bound");
        }
    }

    #[test]
    fn relay_batch_exact_cutoff_preserves_preflight_without_io() {
        for sibling in ["cutoff", "invalid", "own-timeout"] {
            let time = crate::outbound_test_support::TestTime::default();
            let own_deadline = Deadline::at_instant(time.clock().now());
            time.advance_to(
                own_deadline
                    .instant()
                    .checked_add(Duration::from_millis(1))
                    .expect("entry instant"),
            );
            let entered_at = time.clock().now();
            let polls = Arc::new(AtomicUsize::new(0));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let client = HttpClient::with_client(RelayClient {
                time: time.pair(),
                polls: Arc::clone(&polls),
                calls: Arc::clone(&calls),
            });
            let first = OutboundRequest::get("http://127.0.0.1:1234/coding/gzip/200").unwrap();
            let second = OutboundRequest::get("http://127.0.0.1:1234/coding/gzip/200").unwrap();
            let second = match sibling {
                "invalid" => second.stream_response(),
                "own-timeout" => second.deadline(own_deadline),
                _ => second,
            };
            let batch =
                client.start_batch_until(vec![first, second], Deadline::at_instant(entered_at));
            let results = block_on(batch.collect()).expect("exact cutoff is not driver EOF");
            let (collected, termination) = results.into_parts();
            assert_eq!(termination, OutboundBatchTermination::Cutoff);
            assert_eq!(collected.len(), 2);
            assert!(matches!(collected[0], OutboundCollectedSlot::Unresolved));
            let last = collected.into_iter().nth(1).expect("second slot");
            if sibling == "cutoff" {
                assert!(matches!(last, OutboundCollectedSlot::Unresolved));
            } else {
                let OutboundCollectedSlot::Resolved(rejected) = last else {
                    panic!("preflight outcome must be retained");
                };
                assert_eq!(rejected.elapsed, Duration::ZERO);
                let error = rejected.outcome.expect_err("preflight failure");
                if sibling == "invalid" {
                    assert_eq!(error.status(), 400);
                } else {
                    assert!(matches!(
                        error,
                        EdgeError::GatewayTimeout {
                            cause: BudgetSource::RequestDeadline,
                            ..
                        }
                    ));
                }
            }
            assert!(calls.lock().unwrap().is_empty());
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(time.clock().now(), entered_at);
        }
    }

    #[test]
    fn relay_handoff_preserves_bytes_and_stream_ownership_with_matching_policy() {
        for mode in ["streamed", "buffered"] {
            for (choice, status) in [
                ("original", "200"),
                ("original", "206"),
                ("decode", "206"),
                ("decode", "200"),
            ] {
                let polls = Arc::new(AtomicUsize::new(0));
                let calls = Arc::new(Mutex::new(Vec::new()));
                let client = edgezero_core::HttpClient::with_client(RelayClient {
                    time: crate::outbound_test_support::TestTime::default().pair(),
                    polls: Arc::clone(&polls),
                    calls: Arc::clone(&calls),
                });
                let router = router(Arc::new(Fake::default()), json!({}), None);
                let target = format!("http://127.0.0.1:1234/coding/gzip/{status}");
                for prefix in ["relay", "coding"] {
                    let case = format!("{prefix}-gzip-{mode}-{status}-{choice}");
                    let mut request = request(&case, &target);
                    request.extensions_mut().insert(client.clone());
                    let before = polls.load(Ordering::SeqCst);
                    let response = block_on(router.oneshot(request)).unwrap();
                    if prefix == "relay" {
                        assert_eq!(response.status().as_str(), status);
                        assert_eq!(response.headers()["etag"], "\"fixture\"");
                        assert_eq!(polls.load(Ordering::SeqCst), before);
                        assert_eq!(response.body().is_stream(), mode == "streamed");
                    }
                    let raw = choice == "original" || status == "206";
                    let expected = if raw {
                        Bytes::from_static(&[255, 0, 128])
                    } else {
                        Bytes::from_static(b"decoded")
                    };
                    let bytes = block_on(response.into_body().into_bytes_bounded(4096)).unwrap();
                    if prefix == "relay" {
                        assert_eq!(bytes, expected);
                    } else {
                        assert_eq!(
                            serde_json::from_slice::<Value>(&bytes).unwrap()["bytes"],
                            json!(expected.as_ref())
                        );
                    }
                }
                let recorded = calls.lock().unwrap();
                assert_eq!(recorded.len(), 2);
                assert_eq!(recorded[0]["marker"], "Off");
                assert_eq!(recorded[1]["marker"], "On");
                let mut relay = recorded[0].clone();
                relay["marker"] = json!("On");
                assert_eq!(relay, recorded[1]);
            }
        }
    }

    #[test]
    fn overflow_fixture_routes_exercise_the_real_inbound_reader() {
        let router = router(Arc::new(Fake::default()), json!({}), None);
        for len in [1023_usize, 1024, 1025] {
            let request = edgezero_core::http::request_builder()
                .method(Method::POST)
                .uri("/progress/overflow/inbound")
                .body(Body::stream(futures::stream::iter([Bytes::from(vec![
                    b'x';
                    len
                ])])))
                .unwrap();
            let response = block_on(router.oneshot(request)).unwrap();
            assert_eq!(response.status(), if len > 1024 { 413 } else { 200 });
            let bytes = block_on(response.into_body().into_bytes_bounded(256)).unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if len > 1024 {
                assert_eq!(value["error"]["kind"], "payload_too_large");
            } else {
                assert_eq!(value["received"], len);
            }
        }
    }

    #[test]
    fn overflow_fixture_rejects_uncontrolled_targets_before_client_lookup() {
        let router = router(Arc::new(Fake::default()), json!({}), None);
        for target in [
            "https://example.com/overflow/test/buffered-9",
            "http://127.0.0.1/overflow/test/buffered-9",
            "http://127.0.0.1:1234/other",
            "http://127.0.0.1:1234/overflow/test/buffered-9?q=1",
        ] {
            let request = edgezero_core::http::request_builder()
                .method(Method::POST)
                .uri("/progress/overflow/buffered-9")
                .header("x-request-token", "test")
                .header("x-fixture-backend", target)
                .body(Body::empty())
                .unwrap();
            let response = block_on(router.oneshot(request)).unwrap();
            assert_eq!(response.status(), 400);
        }
    }

    #[test]
    fn relay_rejects_invalid_cases_and_targets_before_client_lookup() {
        let router = router(Arc::new(Fake::default()), json!({}), None);
        for target in [
            "https://example.com/coding/gzip/200",
            "http://example.com:1234/coding/gzip/200",
            "http://127.0.0.1/coding/gzip/200",
            "http://127.0.0.1:1234/coding/gzip/200?q=1",
            "http://127.0.0.1:1234/other",
        ] {
            let response =
                block_on(router.oneshot(request("relay-gzip-streamed-200-original", target)))
                    .unwrap();
            assert_eq!(response.status(), 400);
        }
        for case in [
            "relay-gzip-other-200-original",
            "relay-gzip-streamed-201-original",
            "relay-unknown-streamed-200-original",
            "relay-gzip-streamed-200-other",
        ] {
            let response =
                block_on(router.oneshot(request(case, "http://127.0.0.1:1234/coding/gzip/200")))
                    .unwrap();
            assert_eq!(response.status(), 400);
        }
    }

    #[test]
    fn eager_probe_does_not_consume_jobs_until_after_unrelated_wait() {
        let runtime = Fake::default();
        let router = router(Arc::new(runtime.clone()), json!({"source":"test"}), None);
        let response =
            block_on(router.oneshot(request("eager-batch", "http://127.0.0.1:1234/g3/one")))
                .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            *runtime.0.lock().unwrap(),
            ["start", "slot-0", "slot-1", "wait", "end"]
        );
    }

    #[test]
    fn response_probe_retains_jobs_until_stream_consumption() {
        let runtime = Fake::default();
        let router = router(Arc::new(runtime.clone()), json!({}), None);
        let response =
            block_on(router.oneshot(request("response-owned", "http://127.0.0.1:1234/g3/one")))
                .unwrap();
        assert_eq!(*runtime.0.lock().unwrap(), ["start", "slot-0", "slot-1"]);
        let bytes = block_on(response.into_body().into_bytes_bounded(4096)).unwrap();
        let report: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(report["handler_returned"], true);
        assert_eq!(
            *runtime.0.lock().unwrap(),
            ["start", "slot-0", "slot-1", "wait", "end"]
        );
    }

    #[test]
    fn dropping_unconsumed_response_releases_its_unpolled_jobs_once() {
        struct Count(Arc<AtomicUsize>);
        impl Drop for Count {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Runtime(Arc<AtomicUsize>);
        impl ProbeRuntime for Runtime {
            fn adapter(&self) -> &'static str {
                "test"
            }
            fn perform(&self, _target: String) -> Job {
                Box::pin(async { Ok(0) })
            }
            fn start(&self, _target: String) -> Result<Job, EdgeError> {
                let count = Count(Arc::clone(&self.0));
                Ok(Box::pin(async move {
                    let _count = count;
                    std::future::pending().await
                }))
            }
            fn pause(&self, _duration: Duration) -> LocalBoxFuture<'static, ()> {
                Box::pin(async {})
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let router = router(Arc::new(Runtime(Arc::clone(&drops))), json!({}), None);
        let response =
            block_on(router.oneshot(request("response-owned", "http://127.0.0.1:1234/g3/one")))
                .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(response);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(router);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn response_controls_do_not_dispatch_or_poll_the_tail_inside_the_handler() {
        for case in ["response-owned-ready", "response-owned-timer-only"] {
            let runtime = Fake::default();
            let dispatched = Arc::new(DispatchReturn::default());
            let router = router(
                Arc::new(runtime.clone()),
                json!({}),
                Some(Arc::clone(&dispatched)),
            );
            let response =
                block_on(router.oneshot(request(case, "https://example.com/unused"))).unwrap();
            assert_eq!(response.status(), 200);
            assert!(runtime.0.lock().unwrap().is_empty());
            dispatched.mark_returned();
            let (payload, body_owner) = response.into_body().into_parts();
            let BodyPayload::Stream(mut stream) = payload else {
                panic!("control must stream")
            };
            let mut bytes = block_on(stream.next()).unwrap().unwrap().to_vec();
            assert_eq!(bytes, b" ");
            assert!(runtime.0.lock().unwrap().is_empty());
            bytes.extend(block_on(Body::from_stream(stream).into_bytes_bounded(4096)).unwrap());
            let report: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(report["case"], case);
            assert_eq!(report["handler_returned_at_first_poll"], true);
            assert_eq!(report["dispatch_returned_at_first_poll"], true);
            assert_eq!(report["handler_returned"], true);
            assert_eq!(report["dispatch_returned"], true);
            let expected: Vec<&str> = if case.ends_with("timer-only") {
                vec!["wait"]
            } else {
                vec![]
            };
            assert_eq!(*runtime.0.lock().unwrap(), expected);
            drop(body_owner);
        }
    }

    #[test]
    fn probe_rejects_remote_target_before_any_sdk_work() {
        let runtime = Fake::default();
        let router = router(Arc::new(runtime.clone()), json!({}), None);
        let response =
            block_on(router.oneshot(request("eager-single", "https://example.com/g3/one")))
                .unwrap();
        assert_eq!(response.status(), 400);
        assert!(runtime.0.lock().unwrap().is_empty());
    }
}
