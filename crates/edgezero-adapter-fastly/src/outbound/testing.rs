//! Obtain opaque test inputs through the real facade, never by reconstructing core policies.

use futures_util::FutureExt as _;
use futures_util::future::pending;
use std::cell::RefCell;
use std::iter::repeat_with;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(feature = "fastly", feature = "test-utils"))]
use std::task::{Context, Waker};
use std::time::Duration;

use edgezero_core::http::HeaderValue;
use edgezero_core::{
    Deadline, EdgeError, HttpClient, IndependentOutboundProgress, MonotonicClock, MonotonicInstant,
    OutboundBatch, OutboundBatchContext, OutboundDispatchRequest, OutboundExchangeOwner,
    OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
    OutboundProgressRuntime, OutboundRequest, OutboundSendMode, OutboundTime,
};

std::thread_local! {
    static DISPATCH: RefCell<Option<OutboundDispatchRequest>> = const { RefCell::new(None) };
    static BATCH: RefCell<Option<OutboundBatchContext>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy)]
enum CaptureMode {
    BatchContext,
    BatchRequest,
    BeginRequest,
    Dispatch,
}

struct HeadScript {
    dispatch: Box<dyn Fn(OutboundDispatchRequest) -> OutboundFuture<OutboundHead> + Send + Sync>,
    validate: fn(&OutboundRequest, OutboundSendMode) -> Result<(), EdgeError>,
}

struct Capture {
    mode: CaptureMode,
    script: Option<HeadScript>,
    time: OutboundTime,
}

impl Capture {
    fn new(time: OutboundTime, mode: CaptureMode) -> Self {
        Self {
            mode,
            script: None,
            time,
        }
    }
}

impl OutboundHttpClient for Capture {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        if matches!(self.mode, CaptureMode::BeginRequest) {
            DISPATCH.with(|saved| saved.replace(Some(request)));
        }
        Err(EdgeError::internal(anyhow::anyhow!(
            "capture fixture does not begin exchanges"
        )))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        if let Some(script) = &self.script {
            return (script.dispatch)(request);
        }
        DISPATCH.with(|saved| saved.replace(Some(request)));
        Box::pin(pending())
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, mut requests) = batch.into_slots();
        match self.mode {
            CaptureMode::BeginRequest | CaptureMode::Dispatch => {
                return OutboundBatch::cutoff(context);
            }
            CaptureMode::BatchRequest => {
                assert_eq!(requests.len(), 1, "one captured request");
                DISPATCH
                    .with(|saved| saved.replace(Some(requests.pop().expect("prepared request").1)));
                return OutboundBatch::cutoff(context);
            }
            CaptureMode::BatchContext => {}
        }
        let cutoff = context.observation_cutoff();
        BATCH.with(|saved| saved.replace(Some(context)));
        HttpClient::with_client(Capture::new(self.time.clone(), CaptureMode::Dispatch))
            .start_batch_until(Vec::new(), cutoff)
    }

    fn time(&self) -> OutboundTime {
        self.time.clone()
    }

    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        if let Some(script) = &self.script {
            return (script.validate)(request, mode);
        }
        Ok(())
    }
}

#[cfg(any(feature = "fastly", feature = "test-utils"))]
pub(super) fn dispatch(request: OutboundRequest, time: OutboundTime) -> OutboundDispatchRequest {
    let mut future =
        HttpClient::with_client(Capture::new(time, CaptureMode::Dispatch)).send(request);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    DISPATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade dispatched test request")
    })
}

pub(super) fn begin_dispatch(
    request: OutboundRequest,
    time: OutboundTime,
) -> OutboundDispatchRequest {
    let client = HttpClient::with_client(Capture::new(time, CaptureMode::BeginRequest));
    assert!(matches!(client.begin(request).now_or_never(), Some(Err(_))));
    DISPATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared begun request")
    })
}

pub(crate) fn batch_context(slot_count: usize) -> OutboundBatchContext {
    batch_context_at(slot_count, instant(), Duration::from_secs(60))
}

pub(super) fn batch_dispatch(
    request: OutboundRequest,
    time: OutboundTime,
    cutoff: Deadline,
) -> OutboundDispatchRequest {
    drop(
        HttpClient::with_client(Capture::new(time, CaptureMode::BatchRequest))
            .start_batch_until(vec![request], cutoff),
    );
    DISPATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared batch request")
    })
}

#[test]
fn captured_begun_request_keeps_tag_metadata_and_mode() {
    let request = OutboundRequest::get("https://origin.example.com/resource")
        .expect("request")
        .cache_policy(
            edgezero_core::OutboundCachePolicy::PlatformDefaultWithSurrogateKey(
                HeaderValue::from_static("example-all"),
            ),
        );
    let captured = begin_dispatch(request, batch_context(1).time().clone());
    let (parts, context) = captured.into_parts();
    assert_eq!(context.mode(), OutboundSendMode::Begin);
    assert!(
        matches!(parts.cache_policy, edgezero_core::OutboundCachePolicy::PlatformDefaultWithSurrogateKey(value) if value.as_bytes() == b"example-all")
    );
    assert!(!parts.headers.contains_key("surrogate-key"));
}

#[test]
fn captured_batch_request_keeps_the_facade_mode_and_budget() {
    let start = instant();
    let time = OutboundTime::new(MonotonicClock::new(move || start), |_, _| {
        Box::pin(pending())
    });
    let captured = batch_dispatch(
        OutboundRequest::get("https://fixture.invalid/")
            .unwrap()
            .timeout(Duration::from_millis(250)),
        time,
        Deadline::at_instant(start.checked_add(Duration::from_secs(1)).unwrap()),
    );
    let budget = captured.budget();
    let (_, context) = captured.into_parts();
    assert_eq!(context.mode(), OutboundSendMode::Batch);
    assert_eq!(budget.duration, Duration::from_millis(250));
    assert_eq!(context.entered_at(), start);
}

pub(super) fn batch_context_at(
    slot_count: usize,
    start: MonotonicInstant,
    duration: Duration,
) -> OutboundBatchContext {
    let first = AtomicBool::new(true);
    let observed = start.checked_add(duration.checked_div(2).unwrap()).unwrap();
    let time = OutboundTime::new(
        MonotonicClock::new(move || {
            if first.swap(false, Ordering::SeqCst) {
                start
            } else {
                observed
            }
        }),
        |_, _| Box::pin(pending()),
    );
    let client = HttpClient::with_client(Capture::new(time, CaptureMode::BatchContext));
    let requests = repeat_with(|| {
        OutboundRequest::get("https://example.com/")
            .unwrap()
            .timeout(duration)
    })
    .take(slot_count)
    .collect();
    drop(client.start_batch_until(
        requests,
        Deadline::at_instant(start.checked_add(duration).unwrap()),
    ));
    BATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared batch context")
    })
}

#[cfg(feature = "fastly")]
pub(crate) fn batch_context_using(
    slot_count: usize,
    time: OutboundTime,
    observation_cutoff: Deadline,
) -> OutboundBatchContext {
    let requests = repeat_with(|| {
        OutboundRequest::get("https://example.com/")
            .unwrap()
            .timeout(Duration::from_secs(60))
    })
    .take(slot_count)
    .collect();
    batch_context_for_requests(requests, time, observation_cutoff)
}

pub(super) fn batch_context_for_requests(
    requests: Vec<OutboundRequest>,
    time: OutboundTime,
    observation_cutoff: Deadline,
) -> OutboundBatchContext {
    drop(
        HttpClient::with_client(Capture::new(time, CaptureMode::BatchContext))
            .start_batch_until(requests, observation_cutoff),
    );
    BATCH.with(|saved| {
        saved
            .borrow_mut()
            .take()
            .expect("facade prepared timed context")
    })
}

#[cfg(not(feature = "fastly"))]
pub(crate) fn client(time: OutboundTime) -> HttpClient {
    HttpClient::with_client(Capture::new(time, CaptureMode::Dispatch))
}

pub(crate) fn owner() -> OutboundExchangeOwner {
    OutboundExchangeOwner::new(|| {})
}

std::thread_local! { static START: MonotonicInstant = MonotonicInstant::now(); }
pub(crate) fn instant() -> MonotonicInstant {
    START.with(|start| *start)
}

#[cfg(feature = "fastly")]
fn optimizer_options_fixture() -> (
    edgezero_core::OutboundImageOptimizerOptions,
    serde_json::Value,
) {
    use edgezero_core::{
        OutboundCropFocus, OutboundImageFormat, OutboundImageOptimizerOptions,
        OutboundImageOptimizerRegion, OutboundResizeFilter,
    };
    use fastly::image_optimizer::{
        Area, Crop, Format, ImageOptimizerOptions, ImageOptimizerRegion, PixelsOrPercentage,
        PointOrOffset, Position, ResizeAlgorithm,
    };
    let options = OutboundImageOptimizerOptions::new()
        .region(OutboundImageOptimizerRegion::Asia)
        .preserve_origin_query(true)
        .format(OutboundImageFormat::WebP)
        .quality(100)
        .expect("quality")
        .resize_filter(OutboundResizeFilter::Lanczos3)
        .width(320)
        .expect("width")
        .height(180)
        .expect("height")
        .crop(16, 9, OutboundCropFocus::Percent { x: 0, y: 100 })
        .expect("crop");
    let mut expected = ImageOptimizerOptions::from_region(ImageOptimizerRegion::Asia);
    expected.preserve_query_string_on_origin_request = Some(true);
    expected.format = Some(Format::WebP);
    expected.quality = Some(100);
    expected.resize_filter = Some(ResizeAlgorithm::Lanczos3);
    expected.width = Some(PixelsOrPercentage::Pixels(320));
    expected.height = Some(PixelsOrPercentage::Pixels(180));
    expected.crop = Some(Crop {
        size: Area::AspectRatio((16, 9)),
        mode: None,
        position: Some(Position {
            x: Some(PointOrOffset::Offset(0)),
            y: Some(PointOrOffset::Offset(100)),
        }),
    });
    (
        options,
        serde_json::to_value(expected).expect("expected SDK values"),
    )
}

#[cfg(feature = "fastly")]
fn optimizer_reply(index: usize, context: edgezero_core::OutboundResponseContext) -> OutboundHead {
    use edgezero_core::http::{HeaderMap, StatusCode};
    use edgezero_core::{OutboundHeaderFidelity, OutboundHeaderInput};
    use futures_util::{StreamExt as _, stream};

    let mut headers = HeaderMap::new();
    let status = if index == 1 {
        headers.insert("location", "/next".parse().expect("location"));
        StatusCode::FOUND
    } else {
        assert!(index < 3, "exactly one send and two Follow hops");
        StatusCode::OK
    };
    OutboundHead::new(
        status,
        OutboundHeaderInput::new(
            "fastly-recording",
            headers,
            OutboundHeaderFidelity::AdapterVisibleFields,
        ),
        stream::empty().boxed_local(),
        context,
        owner(),
    )
    .expect("scripted head")
}

#[cfg(feature = "fastly")]
#[test]
fn image_optimizer_follow_uses_native_mapping_on_each_prepared_hop() {
    use std::cell::Cell;
    use std::sync::{Arc, Mutex};

    use edgezero_core::http::StatusCode;
    use edgezero_core::{OutboundRedirectAction, OutboundRedirectLimit};
    use futures_util::future::FutureExt as _;

    let start = instant();
    let time = OutboundTime::new(MonotonicClock::new(move || start), |_, _| {
        Box::pin(pending())
    });
    let (options, expected) = optimizer_options_fixture();
    let records = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&records);
    let mut capture = Capture::new(time, CaptureMode::Dispatch);
    capture.script = Some(HeadScript {
        validate: super::validate_supported_options,
        dispatch: Box::new(move |request| {
            let (parts, context) = request.into_parts();
            let attached_options = parts.image_optimizer.expect("attached options");
            let native = super::image_optimizer::native_options(&attached_options)
                .expect("production mapper");
            let mut entries = observed.lock().expect("records");
            let index = entries.len();
            entries.push((
                context.mode(),
                parts.uri,
                attached_options,
                serde_json::to_value(native).expect("SDK values"),
            ));
            Box::pin(async move { Ok(optimizer_reply(index, context)) })
        }),
    });
    let client = HttpClient::with_client(capture);
    let request = || {
        OutboundRequest::get("https://fixture.invalid/first?width=8")
            .expect("request")
            .timeout(Duration::from_secs(15))
            .image_optimizer(options.clone())
    };
    client
        .send(request())
        .now_or_never()
        .expect("scripted send resolves")
        .expect("send");
    let policies = Rc::new(Cell::new(0_usize));
    let observed_policies = Rc::clone(&policies);
    let policy_options = options.clone();
    let response = client
        .follow(
            request(),
            OutboundRedirectLimit::new(1).expect("limit"),
            move |context| {
                observed_policies.set(observed_policies.get().saturating_add(1));
                assert_eq!(
                    context
                        .prepared_request()
                        .expect("prepared hop")
                        .image_optimizer_options(),
                    Some(&policy_options)
                );
                Ok::<_, ()>(OutboundRedirectAction::FollowPrepared)
            },
        )
        .now_or_never()
        .expect("scripted Follow resolves")
        .expect("follow");
    assert_eq!(response.response().status(), StatusCode::OK);
    assert_eq!(policies.get(), 1);
    let entries = records.lock().expect("records");
    assert_eq!(entries.len(), 3);
    for ((mode, target, attached, native), (expected_mode, expected_target)) in
        entries.iter().zip([
            (
                OutboundSendMode::Send,
                "https://fixture.invalid/first?width=8",
            ),
            (
                OutboundSendMode::Follow,
                "https://fixture.invalid/first?width=8",
            ),
            (OutboundSendMode::Follow, "https://fixture.invalid/next"),
        ])
    {
        assert_eq!(*mode, expected_mode);
        assert_eq!(target.to_string(), expected_target);
        assert_eq!(*attached, options);
        assert_eq!(*native, expected);
    }
}

#[cfg(feature = "fastly")]
pub(super) fn dispatch_at(
    request: OutboundRequest,
    time: OutboundTime,
    start: MonotonicInstant,
) -> OutboundDispatchRequest {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let capturing = Arc::new(AtomicBool::new(true));
    let during_preparation = Arc::clone(&capturing);
    let original = time.clock().clone();
    let clock = MonotonicClock::new(move || {
        if during_preparation.load(Ordering::SeqCst) {
            start
        } else {
            original.now()
        }
    });
    let prepared = dispatch(
        request,
        OutboundTime::new(clock, move |deadline, _| time.wait_until(deadline)),
    );
    capturing.store(false, Ordering::SeqCst);
    prepared
}
