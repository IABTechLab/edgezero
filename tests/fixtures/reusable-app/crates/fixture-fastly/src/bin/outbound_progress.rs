use edgezero_core::EdgeError;
use fastly::http::request::{PendingRequest, PollResult};
use fixture_core::qualification::progress::{self, Job, ProbeRuntime};
use futures::future::LocalBoxFuture;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

struct Runtime;

impl ProbeRuntime for Runtime {
    fn adapter(&self) -> &'static str {
        "fastly"
    }
    fn perform(&self, target: String) -> Job {
        Box::pin(async move { Runtime.start(target)?.await })
    }
    fn start(&self, target: String) -> Result<Job, EdgeError> {
        let pending = fastly::Request::get(target)
            .with_pass(true)
            .send_async("progress-origin")
            .map_err(|_| progress::probe_failure())?;
        Ok(complete(pending))
    }
    fn pause(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        // This is a synchronous guest suspension, not a background guest executor.
        Box::pin(async move {
            std::thread::sleep(duration);
        })
    }
}

fn complete(mut pending: PendingRequest) -> Job {
    Box::pin(async move {
        let started = Instant::now();
        loop {
            match pending.poll() {
                PollResult::Done(result) => {
                    let response = result.map_err(|_| progress::probe_failure())?;
                    if !response.get_status().is_success() {
                        return Err(progress::probe_failure());
                    }
                    // The controlled origin sends CL:0. Do not enter the failed G2 body-read path.
                    return Ok(0);
                }
                PollResult::Pending(next) => pending = next,
            }
            if started.elapsed() >= Duration::from_secs(2) {
                return Err(progress::probe_failure());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    })
}

fn telemetry(line: &str) {
    eprintln!("{line}");
}

fn main() -> Result<(), fastly::Error> {
    fastly::init();
    fixture_core::init_fixture_logging(telemetry)?;
    let mut request = fastly::Request::from_client();
    let signal = json!({"source":"fastly-sdk-before-conversion", "has_body":request.has_body(),
        "known_length":request.try_get_body_mut().and_then(fastly::Body::known_length)});
    if let Some(target) = request.get_header_str("x-fixture-backend") {
        let uri: edgezero_core::http::Uri = target.parse()?;
        if !fixture_core::is_loopback_http(&uri) || uri.port_u16().is_none() {
            return Err(progress::probe_failure().into());
        }
        fastly::Backend::builder(
            "progress-origin",
            uri.authority()
                .ok_or_else(progress::probe_failure)?
                .as_str(),
        )
        .connect_timeout(Duration::from_secs(2))
        .first_byte_timeout(Duration::from_secs(2))
        .between_bytes_timeout(Duration::from_secs(2))
        .finish()?;
    }
    let app = progress::application(
        Arc::new(Runtime),
        signal,
        None,
        edgezero_adapter_fastly::FASTLY_PLATFORM,
    );
    fixture_fastly::strict_delivery(
        edgezero_adapter_fastly::request::FastlyService::new(&app).send_request_with_hooks(
            request,
            |_, _| {},
            |_| {},
        ),
    )
}
