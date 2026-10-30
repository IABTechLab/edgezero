use edgezero_adapter_fastly::{
    FASTLY_PLATFORM,
    delivery::{DeliveryError, FastlyRequestDelivery, FastlyResponseCommitState},
    lifecycle::run_custom,
    outbound::FastlyOutboundClient,
    request::FastlyService,
};
use edgezero_core::{
    Body, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock, MonotonicInstant,
    OutboundBatchCheck, OutboundCollectedSlot, OutboundHttpClient, OutboundRequest, OutboundScope,
    OutboundScopeOwner, ResponseEgressBodyKind, ResponseEgressDeadline, ResponseEgressOutcome,
    action,
    app::App,
    http::{Response, Uri},
    response_egress::{ResponseEgressObserver, ResponseEgressReport},
    router::RouterService,
};
use futures::{StreamExt as _, executor::block_on, future, stream};
use serde_json::{Value, json};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const RECORD_PREFIX: &str = "edgezero-lifetime-cleanup ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Complete,
    SourceError,
    WriteDeadline,
}

impl Mode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "complete" => Some(Self::Complete),
            "source-error" => Some(Self::SourceError),
            "write-deadline" => Some(Self::WriteDeadline),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Complete => "response-cleanup",
            Self::SourceError => "response-cleanup-source-error",
            Self::WriteDeadline => "response-cleanup-write-deadline",
        }
    }

    fn outcome(self) -> ResponseEgressOutcome {
        match self {
            Self::Complete => ResponseEgressOutcome::Completed,
            Self::SourceError => ResponseEgressOutcome::SourceError,
            Self::WriteDeadline => ResponseEgressOutcome::DeadlineExceeded,
        }
    }

    fn error(self) -> Option<DeliveryError> {
        match self {
            Self::Complete => None,
            Self::SourceError => Some(DeliveryError::Source),
            Self::WriteDeadline => Some(DeliveryError::Deadline),
        }
    }
}

#[derive(Clone, Default)]
struct TailWitness {
    source_error: Rc<Cell<bool>>,
    pending_poll: Rc<Cell<bool>>,
}

struct Reports(Arc<AtomicUsize>);

impl ResponseEgressObserver for Reports {
    fn complete(&self, _: &ResponseEgressReport) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Probe {
    token: String,
    base: Uri,
    clock: MonotonicClock,
    start: MonotonicInstant,
    mode: Mode,
}

impl Probe {
    fn parse(request: &fastly::Request, clock: MonotonicClock) -> Result<Self, EdgeError> {
        let token = request
            .get_header_str("x-request-token")
            .ok_or_else(failed)?;
        let base: Uri = request
            .get_header_str("x-fixture-backend")
            .ok_or_else(failed)?
            .parse()
            .map_err(|_| failed())?;
        let mode = request
            .get_header_str("x-fixture-cleanup-mode")
            .and_then(Mode::parse)
            .ok_or_else(failed)?;
        check(
            !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
        )?;
        check(
            fixture_core::is_loopback_http(&base)
                && base.port_u16().is_some_and(|port| port != 0)
                && base
                    .authority()
                    .is_some_and(|host| !host.as_str().contains('@'))
                && base.query().is_none()
                && base.path() == format!("/lifetimes/{token}/response-cleanup"),
        )?;
        let start = clock.now();
        Ok(Self {
            token: token.into(),
            base,
            clock,
            start,
            mode,
        })
    }

    fn request(&self, role: &str) -> Result<OutboundRequest, EdgeError> {
        let at = self
            .clock
            .now()
            .saturating_duration_since(self.start)
            .as_micros();
        OutboundRequest::get(format!("{}/{role}", self.base))
            .map(|request| {
                request
                    .timeout(Duration::from_secs(3))
                    .max_response_bytes(64)
            })?
            .header("x-fixture-at-us", at.to_string())
    }

    fn record(&self, stage: &str, evidence: Value) -> Result<(), EdgeError> {
        let line = serde_json::to_string(&json!({"schema":1,"event":"response-cleanup",
            "case":self.mode.name(),"token":self.token,"stage":stage,"evidence":evidence,"qualification":false}))
            .map_err(EdgeError::internal)?;
        check(line.len() <= 2048)?;
        eprintln!("{RECORD_PREFIX}{line}");
        Ok(())
    }
}

fn failed() -> EdgeError {
    EdgeError::internal(std::io::Error::other("response cleanup fixture failed"))
}

fn check(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(failed()) }
}

#[action]
async fn placeholder() -> Result<Response, EdgeError> {
    Ok(Response::new(Body::empty()))
}

pub(super) fn run(request: fastly::Request) -> Result<(), fastly::Error> {
    let provider = FastlyOutboundClient::default();
    let clock = provider.time().clock().clone();
    let client = HttpClient::with_client(provider);
    let probe = Probe::parse(&request, clock)?;
    let reports = Arc::new(AtomicUsize::new(0));
    let mut callback_scope = None;
    let result = run_custom::<(), _, _>(request, |request, _| {
        let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
        callback_scope = Some(scope.clone());
        deliver(request, &probe, &client, &scope, Arc::clone(&reports)).map_err(fastly::Error::from)
    });
    probe.record(
        "returned",
        json!({"result":if result.is_ok(){"Ok"}else{"Err"},
        "callback_scope_closed":callback_scope.as_ref().is_some_and(|scope| scope.check().is_err()),
        "terminal_reports":reports.load(Ordering::SeqCst)}),
    )?;
    result
}

fn deliver(
    request: fastly::Request,
    probe: &Probe,
    client: &HttpClient,
    callback_scope: &OutboundScope,
    reports: Arc<AtomicUsize>,
) -> Result<(), EdgeError> {
    let mut callback = block_on(client.begin(probe.request("callback")?))?;
    let owner = OutboundScopeOwner::response_body();
    let body_scope = owner.scope();
    let origin_request = probe.request("origin")?.stream_response();
    // Capture ownership when the original public future is created, before awaiting it.
    let mut origin = block_on(owner.with(|| client.begin(origin_request)))?;
    let requests = ["a", "b", "c"]
        .into_iter()
        .map(|role| probe.request(role))
        .collect::<Result<Vec<_>, _>>()?;
    let observation_cutoff = probe.clock.deadline_after(Duration::from_secs(3));
    let batch =
        Rc::new(RefCell::new(Some(owner.with(|| {
            client.start_batch_until(requests, observation_cutoff)
        }))));
    check(
        !callback.is_ready()
            && !origin.is_ready()
            && matches!(
                batch.borrow_mut().as_mut().ok_or_else(failed)?.check()?,
                OutboundBatchCheck::Pending
            ),
    )?;
    let ready = block_on(client.send(probe.request("ready")?))?;
    check(ready.status().as_u16() == 204 && block_on(ready.into_bytes_bounded(64))?.is_empty())?;
    check(
        !callback.is_ready()
            && !origin.is_ready()
            && matches!(
                batch.borrow_mut().as_mut().ok_or_else(failed)?.check()?,
                OutboundBatchCheck::Pending
            ),
    )?;
    let mut bytes = serde_json::to_vec(
        &json!({"schema":1,"case":probe.mode.name(),"token":probe.token,
        "origin_pending":true,"batch_pending":true,"callback_pending":true,"qualification":false}),
    )
    .map_err(EdgeError::internal)?;
    bytes.push(b'\n');
    check(bytes.len() <= 512)?;
    let prefix_len = u64::try_from(bytes.len()).map_err(EdgeError::internal)?;
    let witness = TailWitness::default();
    let tail_witness = witness.clone();
    let tail_probe = probe.clone();
    let tail_client = client.clone();
    let tail_batch = Rc::clone(&batch);
    let tail = stream::once(async move {
        if tail_probe.mode == Mode::Complete {
            return Ok(());
        }
        let ack = tail_client.send(tail_probe.request("ack")?).await?;
        check(ack.status().as_u16() == 204 && ack.into_bytes_bounded(64).await?.is_empty())?;
        if tail_probe.mode == Mode::SourceError {
            tail_witness.source_error.set(true);
            return Err(failed());
        }
        std::future::poll_fn(move |cx| {
            let mut saved = tail_batch.borrow_mut();
            let Some(batch) = saved.as_mut() else {
                return std::task::Poll::Ready(Err(failed()));
            };
            // `next` is cancellation-safe; keep no RefCell borrow across a pending poll.
            let mut next = std::pin::pin!(batch.next());
            let observed = std::future::Future::poll(next.as_mut(), cx);
            if observed.is_pending() {
                tail_witness.pending_poll.set(true);
            }
            observed
        })
        .await?;
        Err(failed())
    })
    .filter_map(|result: Result<(), EdgeError>| future::ready(result.err().map(Err)));
    let source = stream::iter([Ok(bytes.into())]).chain(tail);
    let mut response = Response::new(Body::from_stream(source));
    if probe.mode == Mode::WriteDeadline {
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::after(Duration::from_secs(1)));
    }
    response.headers_mut().insert(
        "content-type",
        "application/json".parse().map_err(EdgeError::internal)?,
    );
    let response = owner.attach(response)?;
    let mut app = App::with_name_and_platform(
        RouterService::builder()
            .get("/progress/lifetimes/response-cleanup", placeholder)
            .build(),
        "response-cleanup-probe",
        FASTLY_PLATFORM,
    );
    app.set_monotonic_clock(probe.clock.clone());
    app.set_response_egress_observer(Reports(Arc::clone(&reports)));
    let delivery = FastlyService::new(&app).send_request_with_hooks(
        request,
        |_, extensions| {
            extensions.insert(client.clone());
        },
        |output| {
            *output = response;
        },
    );
    check(matches!(&delivery, FastlyRequestDelivery::Routed { .. }))?;
    let report = delivery.delivery().ok_or_else(failed)?;
    check(
        report.error() == probe.mode.error()
            && report.commitment() == FastlyResponseCommitState::Committed
            && report.report().outcome == probe.mode.outcome()
            && report.report().body_kind == ResponseEgressBodyKind::Application
            && report.report().fallback_disposition.is_none()
            && report.report().framing_failures.application().is_none()
            && report.report().framing_failures.fallback().is_none()
            && report.report().bytes_written == prefix_len
            && !delivery.may_send_native_response()
            && reports.load(Ordering::SeqCst) == 1,
    )?;
    match probe.mode {
        Mode::SourceError => check(
            witness.source_error.get()
                && report.report().source_failure.is_some_and(|source| {
                    source.category() == "internal"
                        && source.body_kind() == ResponseEgressBodyKind::Application
                        && source.provider_failure_detail().is_none()
                }),
        )?,
        Mode::WriteDeadline => check(
            witness.pending_poll.get()
                && report.report().source_failure.is_none()
                && report.report().elapsed >= Duration::from_secs(1)
                && !observation_cutoff.is_expired_at(probe.clock.now()),
        )?,
        Mode::Complete => check(report.report().source_failure.is_none())?,
    }
    check(body_scope.check().is_err() && callback_scope.check().is_ok() && !callback.is_ready())?;
    check(matches!(
        block_on(origin.collect()),
        Err(EdgeError::Internal { .. })
    ))?;
    let batch = batch.borrow_mut().take().ok_or_else(failed)?;
    let failure = block_on(batch.collect()).err().ok_or_else(failed)?;
    check(
        matches!(failure.error(), EdgeError::Internal { .. })
            && failure.slots().len() == 3
            && failure
                .slots()
                .iter()
                .all(|slot| matches!(slot, OutboundCollectedSlot::Unresolved)),
    )?;
    let mut cleaned = json!({"outcome":format!("{:?}",probe.mode.outcome()),"terminal_reports":1,"body_scope_closed":true,
        "origin_error":"Internal","batch_error":"Internal","slots":["Unresolved","Unresolved","Unresolved"],
        "callback_scope_live":true,"callback_pending":true});
    if probe.mode != Mode::Complete {
        cleaned["delivery_error"] = json!(format!("{:?}", probe.mode.error().ok_or_else(failed)?));
        cleaned["source_error_reached"] = json!(witness.source_error.get());
        cleaned["pending_poll"] = json!(witness.pending_poll.get());
        cleaned["source_category"] = json!(
            report
                .report()
                .source_failure
                .map(|failure| failure.category())
        );
        cleaned["accepted_prefix"] = json!(true);
    }
    probe.record("cleaned", cleaned)?;
    let callback = block_on(callback.collect())?;
    check(
        callback.status().as_u16() == 200
            && block_on(callback.into_bytes_bounded(64))?.as_ref() == b"K",
    )?;
    let post = block_on(client.send(probe.request("post")?))?;
    check(
        post.status().as_u16() == 200 && block_on(post.into_bytes_bounded(64))?.as_ref() == b"P",
    )?;
    check(callback_scope.check().is_ok() && reports.load(Ordering::SeqCst) == 1)?;
    probe.record(
        "continued",
        json!({"callback_bytes":[75],"post_bytes":[80],"callback_scope_live":true,
        "terminal_reports":1,"same_client":true}),
    )
}

#[cfg(test)]
mod tests {
    use super::Mode;

    #[test]
    fn cleanup_mode_requires_one_exact_known_token() {
        for (token, mode) in [
            ("complete", Mode::Complete),
            ("source-error", Mode::SourceError),
            ("write-deadline", Mode::WriteDeadline),
        ] {
            assert_eq!(Mode::parse(token), Some(mode));
        }
        for token in [
            "",
            " Complete",
            "source",
            "deadline",
            "source-error,complete",
            "write-deadline\n",
        ] {
            assert_eq!(Mode::parse(token), None);
        }
    }
}
