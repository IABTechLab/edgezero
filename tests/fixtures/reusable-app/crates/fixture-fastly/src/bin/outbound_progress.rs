use edgezero_core::EdgeError;
use fastly::http::request::{PendingRequest, PollResult};
use fixture_core::qualification::progress::{self, Job, ProbeRuntime};
use futures::future::LocalBoxFuture;
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "outbound_progress/cleanup.rs"]
mod cleanup;

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
    fixture_fastly::initialize_launch(env!("CARGO_BIN_NAME"));
    fastly::init();
    fixture_core::init_fixture_logging(telemetry)?;
    let mut request = fastly::Request::from_client();
    if request.get_path() == "/progress/lifetimes/response-cleanup" {
        return cleanup::run(request);
    }
    if let Some(case) = request.get_path().strip_prefix("/progress/reference/") {
        let source = fixture_core::qualification::framing::source(case)?;
        let (parts, body) = source.into_parts();
        let (payload, body_owner) = body.into_parts();
        let edgezero_core::BodyPayload::Once(bytes) = payload else {
            return Err(progress::probe_failure().into());
        };
        let mut response = fastly::Response::from_status(parts.status.as_u16())
            .with_framing_headers_mode(fastly::http::FramingHeadersMode::Automatic);
        for (name, value) in &parts.headers {
            response.append_header(name.as_str(), value.as_bytes());
        }
        response.set_body(bytes.as_ref());
        response.send_to_client();
        drop(body_owner);
        return Ok(());
    }
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
    let mut app = progress::application(
        Arc::new(Runtime),
        signal,
        None,
        edgezero_adapter_fastly::FASTLY_PLATFORM,
    )?;
    let headers_probe = fixture_core::qualification::response_headers::enabled();
    let probe_token = request
        .get_header_str("x-request-token")
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .map(str::to_owned);
    if headers_probe {
        fixture_core::qualification::response_headers::detached_witness(
            &mut app,
            probe_token.clone(),
        );
    }
    fixture_fastly::strict_delivery(
        edgezero_adapter_fastly::request::FastlyService::new(&app).send_request_with_hooks(
            request,
            |request, scratch| {
                if !headers_probe { return; }
                use edgezero_core::{SynthesizedResponseHeaders, SynthesizedResponseHeadersError};
                let mode = request.get_header_str("x-fixture-synthesized-mode").unwrap_or("app");
                let result = match mode {
                    "replace" => SynthesizedResponseHeaders::new(fixture_core::qualification::response_headers::fields("request")),
                    "empty" => SynthesizedResponseHeaders::new(edgezero_core::http::HeaderMap::new()),
                    "invalid" => {
                        let mut fields = fixture_core::qualification::response_headers::fields("request");
                        fields.insert("x-synthesized-fixture", "request!".parse().unwrap());
                        SynthesizedResponseHeaders::new(fields)
                    }
                    _ => { return; }
                };
                let outcome = match result {
                    Ok(headers) => { scratch.insert(headers); "installed" },
                    Err(SynthesizedResponseHeadersError::TooManyBytes) => "TooManyBytes",
                    Err(SynthesizedResponseHeadersError::TooManyFields) => "TooManyFields",
                    Err(_) => "unexpected-error",
                };
                eprintln!("{}",json!({"event":"synthesized-prepare","token":probe_token,"mode":mode,"outcome":outcome,"qualification":false}));
            },
            |_| {
                if headers_probe { eprintln!("{}",json!({"event":"synthesized-finalize","token":probe_token,"qualification":false})); }
            },
        ),
    )
}
