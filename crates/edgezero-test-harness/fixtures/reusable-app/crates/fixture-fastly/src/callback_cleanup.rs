use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, Serve,
    delivery::{FastlyRequestDelivery, FastlyResponseCommitState},
    lifecycle::{run_custom, serve_custom},
    outbound::FastlyOutboundClient,
    request::FastlyService,
};
use edgezero_core::{
    Body, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock, OutboundBatch,
    OutboundBatchCheck, OutboundCollectedSlot, OutboundFuture, OutboundHttpClient, OutboundPending,
    OutboundRequest, OutboundResponse, OutboundScope, OutboundTime, ResponseEgressBodyKind,
    ResponseEgressOutcome, action,
    app::App,
    http::{Response, Uri},
    response_egress::{ResponseEgressObserver, ResponseEgressReport},
    router::RouterService,
};
use futures::{executor::block_on, stream};
use serde_json::{Value, json};
use std::{
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

const PREFIX: &str = "edgezero-lifetime-callback ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    AfterOk,
    AfterError,
    EarlyError,
    SetupError,
}

impl Mode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "after-ok" => Some(Self::AfterOk),
            "after-error" => Some(Self::AfterError),
            "early-error" => Some(Self::EarlyError),
            "setup-error" => Some(Self::SetupError),
            _ => None,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::AfterOk => "after-ok",
            Self::AfterError => "after-error",
            Self::EarlyError => "early-error",
            Self::SetupError => "setup-error",
        }
    }
    fn delivered(self) -> bool {
        matches!(self, Self::AfterOk | Self::AfterError)
    }
    fn result(self) -> Result<(), &'static str> {
        match self {
            Self::AfterOk => Ok(()),
            Self::SetupError => Err("setup-error"),
            _ => Err("callback-error"),
        }
    }
}

#[derive(Default)]
struct Witness {
    timers_started: AtomicUsize,
    timers_dropped: AtomicUsize,
    payload_dropped: AtomicUsize,
    payload_polls: AtomicUsize,
}

struct Release(Arc<Witness>, bool);
impl Drop for Release {
    fn drop(&mut self) {
        if self.1 {
            self.0.timers_dropped.fetch_add(1, Ordering::SeqCst);
        } else {
            self.0.payload_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct Reports(Arc<AtomicUsize>);
impl ResponseEgressObserver for Reports {
    fn complete(&self, _: &ResponseEgressReport) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Probe {
    token: String,
    base: Uri,
    mode: Mode,
    wrapper: &'static str,
    clock: MonotonicClock,
}
impl Probe {
    fn parse(
        request: &fastly::Request,
        wrapper: &'static str,
        clock: MonotonicClock,
    ) -> Result<Self, EdgeError> {
        let token = request
            .get_header_str("x-request-token")
            .ok_or_else(failed)?;
        check(
            !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
        )?;
        let base: Uri = request
            .get_header_str("x-fixture-backend")
            .ok_or_else(failed)?
            .parse()
            .map_err(|_| failed())?;
        check(
            fixture_core::is_loopback_http(&base)
                && base.port_u16().is_some_and(|port| port != 0)
                && base.query().is_none()
                && base
                    .authority()
                    .is_some_and(|host| !host.as_str().contains('@'))
                && base.path() == format!("/lifetimes/{token}/callback-exit"),
        )?;
        let mode = request
            .get_header_str("x-fixture-callback-mode")
            .and_then(Mode::parse)
            .ok_or_else(failed)?;
        Ok(Self {
            token: token.into(),
            base,
            mode,
            wrapper,
            clock,
        })
    }
    fn request(&self, role: &str) -> Result<OutboundRequest, EdgeError> {
        OutboundRequest::get(format!("{}/{role}", self.base))?
            .timeout(Duration::from_secs(3))
            .max_response_bytes(64)
            .header("x-fixture-at-us", "0")
    }
    fn record(&self, stage: &str, evidence: Value) -> Result<(), EdgeError> {
        let text=serde_json::to_string(&json!({"schema":1,"event":"callback-exit","wrapper":self.wrapper,
            "case":self.mode.name(),"token":self.token,"stage":stage,"evidence":evidence,"qualification":false})).map_err(EdgeError::internal)?;
        check(text.len() <= 2048)?;
        eprintln!("{PREFIX}{text}");
        Ok(())
    }
}

struct Saved {
    probe: Probe,
    client: HttpClient,
    scope: OutboundScope,
    witness: Arc<Witness>,
    reports: Arc<AtomicUsize>,
    send: OutboundFuture<OutboundResponse>,
    origin: OutboundPending,
    batch: OutboundBatch,
    post: Option<OutboundPending>,
}

fn failed() -> EdgeError {
    EdgeError::internal(std::io::Error::other("callback cleanup fixture failed"))
}
fn check(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(failed()) }
}
fn internal(error: &EdgeError) -> bool {
    matches!(error, EdgeError::Internal { .. })
}

pub(super) fn enabled() -> bool {
    matches!(
        option_env!("EDGEZERO_QUALIFICATION_SUITE"),
        Some("outbound-callback-run" | "outbound-callback-serve")
    )
}

pub(super) fn run(serving: bool) -> Result<(), fastly::Error> {
    check(
        option_env!("EDGEZERO_QUALIFICATION_SUITE")
            == Some(if serving {
                "outbound-callback-serve"
            } else {
                "outbound-callback-run"
            }),
    )?;
    let mut saved = None;
    let wrapper = if serving {
        "serve_custom"
    } else {
        "run_custom"
    };
    let mut invoke =
        |request: fastly::Request, state: &mut edgezero_adapter_fastly::lifecycle::Sandbox<()>| {
            callback(request, wrapper, state, &mut saved).unwrap_or(Err("fixture-error"))
        };
    let result = if serving {
        let summary = serve_custom(Serve::new().with_max_requests(1), &mut invoke);
        check(summary.requests() == 1)?;
        summary.into_result()
    } else {
        fastly::init();
        run_custom(fastly::Request::from_client(), invoke)
    };
    let saved = saved.ok_or_else(failed)?;
    check(result == saved.probe.mode.result())?;
    verify(saved)?;
    result.map_err(fastly::Error::msg)
}

fn callback(
    request: fastly::Request,
    wrapper: &'static str,
    state: &mut edgezero_adapter_fastly::lifecycle::Sandbox<()>,
    saved: &mut Option<Saved>,
) -> Result<Result<(), &'static str>, EdgeError> {
    check(saved.is_none())?;
    let witness = Arc::new(Witness::default());
    let base = FastlyOutboundClient::default().time();
    let clock = base.clock().clone();
    let timer_witness = Arc::clone(&witness);
    let time = OutboundTime::new(clock.clone(), move |deadline, _| {
        timer_witness.timers_started.fetch_add(1, Ordering::SeqCst);
        let owner = Release(Arc::clone(&timer_witness), true);
        let wait = base.wait_until(deadline);
        Box::pin(async move {
            let _owner = owner;
            wait.await
        })
    });
    let client = HttpClient::with_client(FastlyOutboundClient::with_time(time));
    let probe = Probe::parse(&request, wrapper, clock)?;
    let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
    let data = Release(Arc::clone(&witness), false);
    let source_witness = Arc::clone(&witness);
    let source = stream::poll_fn(move |_| {
        let _data = &data;
        source_witness.payload_polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    });
    let send = client.send(probe.request("unpolled")?.body(Body::from_stream(source)));
    let origin = block_on(client.begin(probe.request("origin")?.stream_response()))?;
    let requests = ["a", "b", "c"]
        .map(|role| probe.request(role))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let batch =
        client.start_batch_until(requests, probe.clock.deadline_after(Duration::from_secs(3)));
    let reports = Arc::new(AtomicUsize::new(0));
    *saved = Some(Saved {
        probe,
        client,
        scope,
        witness,
        reports,
        send,
        origin,
        batch,
        post: None,
    });
    let saved = saved.as_mut().ok_or_else(failed)?;
    barrier(saved, "ready")?;
    originals_pending(saved)?;
    saved.probe.record(
        "pending",
        json!({"origin_pending":true,"batch_pending":true,"callback_scope_live":true,
        "unpolled_polls":0,"request_drops":0,"live_timers":true}),
    )?;
    if saved.probe.mode.delivered() {
        deliver(request, saved)?;
        originals_pending(saved)?;
        saved.post = Some(block_on(saved.client.begin(saved.probe.request("post")?))?);
        barrier(saved, "post-ready")?;
        check(!saved.post.as_mut().ok_or_else(failed)?.is_ready())?;
    } else if saved.probe.mode == Mode::SetupError {
        check(state.setup_once(|| Err::<(), _>("setup-error")) == Err("setup-error"))?;
    }
    originals_pending(saved)?;
    check(saved.reports.load(Ordering::SeqCst) == usize::from(saved.probe.mode.delivered()))?;
    saved.probe.record(
        "returning",
        json!({"originals_pending":true,"callback_scope_live":true,
        "delivered":saved.probe.mode.delivered(),"post_pending":saved.post.is_some(),
        "terminal_reports":saved.reports.load(Ordering::SeqCst)}),
    )?;
    Ok(saved.probe.mode.result())
}

fn barrier(saved: &Saved, role: &str) -> Result<(), EdgeError> {
    let response = block_on(saved.client.send(saved.probe.request(role)?))?;
    check(
        response.status().as_u16() == 204 && block_on(response.into_bytes_bounded(64))?.is_empty(),
    )
}

fn originals_pending(saved: &mut Saved) -> Result<(), EdgeError> {
    check(
        saved.scope.check().is_ok()
            && !saved.origin.is_ready()
            && matches!(saved.batch.check()?, OutboundBatchCheck::Pending)
            && saved.witness.payload_polls.load(Ordering::SeqCst) == 0
            && saved.witness.payload_dropped.load(Ordering::SeqCst) == 0
            && saved.witness.timers_started.load(Ordering::SeqCst)
                > saved.witness.timers_dropped.load(Ordering::SeqCst),
    )
}

#[action]
async fn placeholder() -> Result<Response, EdgeError> {
    Ok(Response::new(Body::empty()))
}

fn deliver(request: fastly::Request, saved: &Saved) -> Result<(), EdgeError> {
    let mut bytes = serde_json::to_vec(
        &json!({"schema":1,"case":saved.probe.mode.name(),"wrapper":saved.probe.wrapper,
        "token":saved.probe.token,"originals_pending":true,"qualification":false}),
    )
    .map_err(EdgeError::internal)?;
    bytes.push(b'\n');
    check(bytes.len() <= 512)?;
    let length = bytes.len() as u64;
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        "content-type",
        "application/json".parse().map_err(EdgeError::internal)?,
    );
    let mut app = App::with_name_and_platform(
        RouterService::builder()
            .get("/progress/lifetimes/callback-exit", placeholder)
            .build(),
        "callback-cleanup",
        FASTLY_PLATFORM,
    );
    app.set_monotonic_clock(saved.probe.clock.clone());
    app.set_response_egress_observer(Reports(Arc::clone(&saved.reports)));
    let delivered = FastlyService::new(&app).send_request_with_hooks(
        request,
        |_, scratch| {
            scratch.insert(saved.client.clone());
        },
        |out| *out = response,
    );
    check(
        matches!(&delivered, FastlyRequestDelivery::Routed { .. })
            && !delivered.may_send_native_response(),
    )?;
    let result = delivered.delivery().ok_or_else(failed)?;
    check(
        result.error().is_none()
            && result.commitment() == FastlyResponseCommitState::Committed
            && result.report().outcome == ResponseEgressOutcome::Completed
            && result.report().bytes_written == length
            && result.report().body_kind == ResponseEgressBodyKind::Application
            && result.report().fallback_disposition.is_none()
            && result.report().framing_failures.application().is_none()
            && result.report().framing_failures.fallback().is_none()
            && saved.reports.load(Ordering::SeqCst) == 1,
    )
}

fn verify(saved: Saved) -> Result<(), EdgeError> {
    let Saved {
        probe,
        client: _client,
        scope,
        witness,
        reports,
        send,
        origin,
        batch,
        post,
    } = saved;
    let started = witness.timers_started.load(Ordering::SeqCst);
    check(
        scope.check().is_err()
            && started > 0
            && started <= 32
            && witness.timers_dropped.load(Ordering::SeqCst) == started
            && witness.payload_dropped.load(Ordering::SeqCst) == 1
            && witness.payload_polls.load(Ordering::SeqCst) == 0,
    )?;
    check(internal(&block_on(send).err().ok_or_else(failed)?))?;
    check(internal(
        &block_on(origin.collect()).err().ok_or_else(failed)?,
    ))?;
    let failure = block_on(batch.collect()).err().ok_or_else(failed)?;
    check(
        internal(failure.error())
            && failure.slots().len() == 3
            && failure
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved)),
    )?;
    let post_error = if let Some(pending) = post {
        check(internal(
            &block_on(pending.collect()).err().ok_or_else(failed)?,
        ))?;
        Some("Internal")
    } else {
        None
    };
    check(
        witness.timers_started.load(Ordering::SeqCst) == started
            && witness.timers_dropped.load(Ordering::SeqCst) == started
            && witness.payload_dropped.load(Ordering::SeqCst) == 1
            && witness.payload_polls.load(Ordering::SeqCst) == 0
            && reports.load(Ordering::SeqCst) == usize::from(probe.mode.delivered()),
    )?;
    probe.record("closed",json!({"callback_result":probe.mode.result().err().unwrap_or("Ok"),"callback_scope_closed":true,
        "send_error":"Internal","origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
        "post_error":post_error,"unpolled_polls":0,"request_drops":1,"all_timers_released":true,"release_before_handle_consumption":true,
        "release_stable":true,"terminal_reports":reports.load(Ordering::SeqCst)}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn callback_mode_accepts_only_exact_case_tokens() {
        for (token, mode) in [
            ("after-ok", Mode::AfterOk),
            ("after-error", Mode::AfterError),
            ("early-error", Mode::EarlyError),
            ("setup-error", Mode::SetupError),
        ] {
            assert_eq!(Mode::parse(token), Some(mode));
            assert_eq!(Mode::parse(&format!(" {token}")), None);
        }
        assert_eq!(Mode::parse(""), None);
        assert_eq!(Mode::parse("complete"), None);
    }
}
