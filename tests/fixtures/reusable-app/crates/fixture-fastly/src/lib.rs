use edgezero_adapter_fastly::delivery::{
    FastlyNoAttemptReason, FastlyRequestDelivery, FastlyResponseCommitState,
};
use edgezero_adapter_fastly::lifecycle::{Sandbox, serve_custom};
use edgezero_adapter_fastly::outbound::FastlyOutboundClient;
use edgezero_adapter_fastly::request::{
    send_assembly_error, send_request_with_registries_and_hooks,
};
use edgezero_adapter_fastly::{FASTLY_PLATFORM, FastlyLogging, Serve, ServeSummary, init_logger};
use edgezero_core::app::{App, Hooks};
use edgezero_core::body::Body;
use edgezero_core::error::EdgeError;
use edgezero_core::http::{Extensions, Response as CoreResponse, StatusCode, Uri};
use edgezero_core::outbound::{HttpClient, OutboundRequest};
use edgezero_core::{
    AdmissionDecision, DEFAULT_INBOUND_READ_BUDGET, IngressGrant, MonotonicClock,
    ResponseEgressCompletion,
};
use fastly::{Error, Request, Response};
use fixture_core::{FixtureApp, Observation, observe};
use futures::executor::block_on;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
static INITIALIZATIONS: Mutex<Vec<serde_json::Value>> = Mutex::new(Vec::new());
static PANIC_IN_CONSTRUCTOR: AtomicBool = AtomicBool::new(false);

pub fn strict_delivery(result: FastlyRequestDelivery<()>) -> Result<(), Error> {
    match result {
        FastlyRequestDelivery::Routed { delivery, .. }
        | FastlyRequestDelivery::Detached(delivery) => delivery
            .error()
            .map_or(Ok(()), |error| Err(Error::msg(error.message()))),
        FastlyRequestDelivery::NoAttempt(reason) => Err(Error::msg(reason.message())),
        FastlyRequestDelivery::Aborted => Err(Error::msg("ingress admission aborted")),
        _ => Err(Error::msg("unsupported request delivery disposition")),
    }
}

fn delivery_observed<State>(result: &FastlyRequestDelivery<State>, obs: &Observation, token: &str) {
    let disposition = match result {
        FastlyRequestDelivery::Routed { .. } => "routed",
        FastlyRequestDelivery::Detached(_) => "detached",
        FastlyRequestDelivery::NoAttempt(_) => "no-attempt",
        FastlyRequestDelivery::Aborted => "aborted",
        _ => "unknown",
    };
    let delivery = result.delivery();
    println!(
        "{}",
        serde_json::json!({"event":"delivery_report", "instance":obs.instance,
        "ordinal":obs.ordinal,"token":token,"disposition":disposition,
        "commitment":delivery.map(|value|format!("{:?}",value.commitment())),
        "body_kind":delivery.map(|value|format!("{:?}",value.report().body_kind)),
        "outcome":delivery.map(|value|format!("{:?}",value.report().outcome)),
        "bytes_written":delivery.map(|value|value.report().bytes_written),
        "fallback":delivery.and_then(|value|value.report().fallback_disposition).map(|value|format!("{value:?}")),
        "may_send_native":result.may_send_native_response(),
        "no_attempt_reason":result.no_attempt_reason().map(|value|match value {
            FastlyNoAttemptReason::RequiredStore(_) => "required-store",
            FastlyNoAttemptReason::RequestHeadConversion(_) => "request-head-conversion",
            _ => "unknown",
        })})
    );
}

fn lifecycle(event: &str, observation: &Observation, token: &str) {
    println!(
        "{}",
        serde_json::json!({"event":event,"instance":observation.instance,"ordinal":observation.ordinal,"token":token,"cpu_ms":fastly::compute_runtime::elapsed_vcpu_ms().ok(),"heap_mib":fastly::compute_runtime::heap_memory_snapshot_mib().ok()})
    );
}
fn initialized(observation: &Observation, token: &str) {
    for mut event in INITIALIZATIONS.lock().unwrap().drain(..) {
        event["instance"] = observation.instance.clone().into();
        event["ordinal"] = observation.ordinal.into();
        event["token"] = token.into();
        println!("{event}");
    }
}
struct ConversionCompletion {
    armed: Arc<AtomicBool>,
    observation: Observation,
    token: String,
}
impl Drop for ConversionCompletion {
    fn drop(&mut self) {
        if self.armed.load(Ordering::SeqCst) {
            lifecycle("conversion_completed", &self.observation, &self.token);
        }
    }
}

pub fn serving() -> Serve {
    Serve::new()
        .with_max_requests(
            option_env!("FIXTURE_MAX_REQUESTS")
                .unwrap_or("10")
                .parse()
                .unwrap(),
        )
        .with_timeout(Duration::from_millis(500))
}
pub fn observation(req: &Request) -> Observation {
    let token = req.get_header_str("x-request-token").unwrap_or("missing");
    let value = observe(token, req.get_client_request_id());
    println!(
        "{}",
        serde_json::json!({"event":"request_start", "instance":value.instance,"token":token,
        "service_id":fastly::compute_runtime::service_id(),"ordinal":value.ordinal,"correlation":value.correlation,"native_id":value.native_id,
        "cpu_ms":fastly::compute_runtime::elapsed_vcpu_ms().ok(),
        "heap_mib":fastly::compute_runtime::heap_memory_snapshot_mib().ok()})
    );
    value
}
pub fn extend(req: &mut Request, extensions: &mut Extensions) {
    let value = observation(req);
    let token = req.get_header_str("x-request-token").unwrap_or("missing");
    initialized(&value, token);
    let armed = Arc::new(AtomicBool::new(false));
    extensions.insert(fixture_core::ResponseLifetime(
        Arc::new(ConversionCompletion {
            armed: armed.clone(),
            observation: value.clone(),
            token: token.to_owned(),
        }),
        armed,
    ));
    log::info!("fixture request {}", value.correlation);
    extensions.insert(value);
}
pub fn finish(summary: ServeSummary<Error>) -> Result<(), Error> {
    println!(
        "{}",
        serde_json::json!({"event":"sdk_summary", "instance":fixture_core::instance_id(), "attempted":summary.requests(),
        "handler_wall_ns":summary.time_handler().as_nanos(), "wait_wall_ns":summary.time_waited().as_nanos()})
    );
    summary.into_result()
}
fn install_logger(logging: &FastlyLogging) -> Result<(), Error> {
    if logging.use_fastly_logger {
        init_logger(
            logging.endpoint.as_deref().unwrap(),
            logging.level,
            logging.echo_stdout,
        )?;
    }
    Ok(())
}
fn post_commit_error(obs: &Observation, token: &str, error: &dyn std::fmt::Display) {
    println!(
        "{}",
        serde_json::json!({"event":"post_commit_error","instance":obs.instance,"ordinal":obs.ordinal,"token":token,"error":error.to_string()})
    );
}
pub fn rebuilt() -> Result<(), Error> {
    finish(serve_custom(serving(), |req, setup: &mut Sandbox<()>| {
        let stores = MeasuredApp::stores();
        let logging = FastlyLogging::from(MeasuredApp::logging_for("fastly"));
        setup.setup_once(|| install_logger(&logging))?;
        let app = App::build::<MeasuredApp>(FASTLY_PLATFORM)?;
        strict_delivery(send_request_with_registries_and_hooks(
            &app,
            stores,
            req,
            extend,
            |_response| (),
        ))
    }))
}

pub fn logger_negative() -> Result<(), Error> {
    finish(serve_custom(serving(), |req, _state: &mut Sandbox<()>| {
        let obs = observation(&req);
        let stores = MeasuredApp::stores();
        let logging = FastlyLogging::from(MeasuredApp::logging_for("fastly"));
        if let Err(error) = install_logger(&logging) {
            Response::from_status(500)
                .with_body(error.to_string())
                .send_to_client();
            return Err(error);
        }
        let app = App::build::<MeasuredApp>(FASTLY_PLATFORM)?;
        initialized(
            &obs,
            req.get_header_str("x-request-token").unwrap_or("missing"),
        );
        strict_delivery(send_request_with_registries_and_hooks(
            &app,
            stores,
            req,
            |_, extensions| {
                extensions.insert(obs);
            },
            |_response| (),
        ))
    }))
}

fn finalize_response(response: &mut CoreResponse, attempts: u64) {
    if let Some(fixture_core::Finalize(value)) =
        response.extensions_mut().remove::<fixture_core::Finalize>()
    {
        response
            .headers_mut()
            .append("x-fixture-finalized", value.parse().unwrap());
    }
    response
        .headers_mut()
        .insert("x-fixture-attempts", attempts.to_string().parse().unwrap());
}

fn custom_dispatch(req: Request, sandbox: &mut Sandbox<App>, reuse_app: bool) -> Result<(), Error> {
    let token = req
        .get_header_str("x-request-token")
        .unwrap_or("missing")
        .to_owned();
    let obs = observation(&req);
    assert_eq!(sandbox.requests(), obs.ordinal as u64);
    if req.get_path() == "/panic" {
        panic!("injected callback panic");
    }
    if req.get_path() == "/health" {
        Response::from_status(200)
            .with_header(
                "x-fixture-attempts",
                sandbox.initialization_attempts().to_string(),
            )
            .with_body_json(&serde_json::json!({"instance":obs.instance,
                "ordinal":obs.ordinal,"retained":sandbox.state().is_some()}))?
            .send_to_client();
        return Ok(());
    }
    let stores = MeasuredApp::stores();
    let logging = FastlyLogging::from(MeasuredApp::logging_for("fastly"));
    if let Err(error) = sandbox.setup_once(|| install_logger(&logging)) {
        eprintln!("logger setup failed: {error}");
        Response::from_status(503).send_to_client();
        return Ok(());
    }
    log::info!("fixture request {}", obs.correlation);
    let reject_framing = req.get_path() == "/probe/framing-fallback";
    let post_send = req.get_header_str("x-fixture-backend").and_then(|value| {
        let uri: Uri = value.parse().ok()?;
        let authority = uri.authority()?;
        matches!(uri.path(), "/chunks" | "/abort")
            .then(|| format!("http://{authority}/post-send?token={token}"))
    });
    // Per-request mode scopes application state to the current callback.
    let mut per_request = Sandbox::default();
    let state = if reuse_app { sandbox } else { &mut per_request };
    let clock = MonotonicClock::default();
    let start = clock.now();
    let result = state.initialize(|| {
        if req.get_path() == "/initialization-error" {
            Err(EdgeError::service_unavailable("state unavailable"))
        } else {
            App::build::<MeasuredApp>(FASTLY_PLATFORM)
        }
    });
    if let Err(error) = result {
        println!(
            "{}",
            serde_json::json!({"event":"assembly-rejected", "token":token,
            "instance":obs.instance,"ordinal":obs.ordinal,"retained":state.state().is_some(),
            "attempts":state.initialization_attempts()})
        );
        let delivered = send_assembly_error(req, error, start, clock);
        delivery_observed(&delivered, &obs, &token);
        lifecycle("guest_completed", &obs, &token);
        return strict_delivery(delivered);
    }
    initialized(&obs, &token);
    let app = state.state().unwrap();
    let attempts = state.initialization_attempts();
    let client = HttpClient::with_client(FastlyOutboundClient::with_clock(app.monotonic_clock()));
    let request_client = client.clone();
    let delivered = send_request_with_registries_and_hooks(
        app,
        stores,
        req,
        |raw, extensions| {
            raw.set_header("x-fixture-mutated", "true");
            extensions.insert(obs.clone());
            extensions.insert(request_client);
        },
        |response| {
            finalize_response(response, attempts);
            if reject_framing {
                response
                    .headers_mut()
                    .insert("trailer", "fixture-trailer".parse().unwrap());
            }
            client
        },
    );
    delivery_observed(&delivered, &obs, &token);
    let client = match delivered {
        FastlyRequestDelivery::NoAttempt(reason) => {
            Response::from_status(500)
                .with_body(reason.message())
                .send_to_client();
            lifecycle("guest_completed", &obs, &token);
            return Ok(());
        }
        FastlyRequestDelivery::Aborted | FastlyRequestDelivery::Detached(_) => {
            lifecycle("guest_completed", &obs, &token);
            return Ok(());
        }
        FastlyRequestDelivery::Routed {
            state: client,
            delivery,
        } => {
            if let Some(error) = delivery.error() {
                if delivery.commitment() == FastlyResponseCommitState::Committed {
                    post_commit_error(&obs, &token, &Error::msg(error.message()));
                } else {
                    lifecycle("uncommitted_delivery_error", &obs, &token);
                }
            }
            if delivery.commitment() != FastlyResponseCommitState::Committed
                || delivery.report().body_kind != edgezero_core::ResponseEgressBodyKind::Application
            {
                lifecycle("guest_completed", &obs, &token);
                return Ok(());
            }
            lifecycle("delivery_completed", &obs, &token);
            client
        }
        _ => return Err(Error::msg("unsupported request delivery disposition")),
    };
    if let Some(uri) = post_send {
        let outcome = block_on(async {
            let uri: Uri = uri.parse().map_err(EdgeError::internal)?;
            if !fixture_core::is_loopback_http(&uri) {
                return Err(EdgeError::bad_request("backend must be loopback HTTP"));
            }
            client
                .send(OutboundRequest::get(uri.to_string())?.stream_response())
                .await?
                .into_bytes_bounded(1024 * 1024)
                .await?;
            Ok::<(), EdgeError>(())
        });
        if let Err(error) = outcome {
            post_commit_error(&obs, &token, &error);
        }
    }
    println!(
        "{}",
        serde_json::json!({"event":"guest_completed", "instance":obs.instance,"token":token,
        "ordinal":obs.ordinal,"cpu_ms":fastly::compute_runtime::elapsed_vcpu_ms().ok(),
        "heap_mib":fastly::compute_runtime::heap_memory_snapshot_mib().ok()})
    );
    Ok(())
}
pub fn custom(reuse_sandbox: bool, reuse_app: bool) -> Result<(), Error> {
    if reuse_sandbox {
        finish(edgezero_adapter_fastly::lifecycle::serve_custom(
            serving(),
            move |req, state| custom_dispatch(req, state, reuse_app),
        ))
    } else {
        fastly::init();
        edgezero_adapter_fastly::lifecycle::run_custom(Request::from_client(), |req, state| {
            custom_dispatch(req, state, false)
        })
    }
}

pub struct MeasuredApp;
impl Hooks for MeasuredApp {
    fn logging_for(adapter: &str) -> edgezero_core::manifest::ResolvedLoggingConfig {
        if adapter.eq_ignore_ascii_case("fastly") {
            edgezero_core::manifest::ResolvedLoggingConfig {
                endpoint: Some("fixture-logs".to_owned()),
                ..Default::default()
            }
        } else {
            Default::default()
        }
    }

    fn routes() -> Result<edgezero_core::router::RouterService, EdgeError> {
        FixtureApp::routes()
    }
    fn stores() -> edgezero_core::app::StoresMetadata {
        let mut stores = FixtureApp::stores();
        stores.kv = Some(edgezero_core::app::StoreMetadata {
            default: "fixture_kv",
            ids: &["fixture_kv", "missing_fixture_kv"],
        });
        stores
    }
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        if PANIC_IN_CONSTRUCTOR.swap(false, Ordering::SeqCst) {
            println!(
                "{}",
                serde_json::json!({"event":"constructor_panic","instance":fixture_core::instance_id(),"source":"injected_inside_configure"})
            );
            panic!("injected failure inside MeasuredApp::configure");
        }
        let cpu_before = fastly::compute_runtime::elapsed_vcpu_ms().ok();
        let heap_before = fastly::compute_runtime::heap_memory_snapshot_mib().ok();
        let start = std::time::Instant::now();
        let rounds: usize = option_env!("FIXTURE_BUILD_ROUNDS")
            .unwrap_or("0")
            .parse()
            .unwrap();
        let source =
            serde_json::json!({"routes": ["one", "two"], "cache": {"capacity": 128}}).to_string();
        for _ in 0..rounds {
            let parsed: serde_json::Value = serde_json::from_str(&source).unwrap();
            std::hint::black_box(parsed);
        }
        FixtureApp::configure(app)?;
        app.set_ingress_admission_policy(|head| {
            let token = head
                .headers()
                .get("x-request-token")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned();
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::new(move |report| {
                    println!(
                        "{}",
                        serde_json::json!({"event":"egress_terminal","instance":fixture_core::instance_id(),
                            "token":token,"outcome":format!("{:?}",report.outcome),
                            "body_kind":format!("{:?}",report.body_kind),"bytes_written":report.bytes_written,
                            "fallback":report.fallback_disposition.map(|value|format!("{value:?}"))})
                    );
                }),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(DEFAULT_INBOUND_READ_BUDGET),
            }
        });
        INITIALIZATIONS.lock().unwrap().push(serde_json::json!({"event":"initialization", "wall_ns":start.elapsed().as_nanos(),
            "cpu_before_ms":cpu_before,"cpu_after_ms":fastly::compute_runtime::elapsed_vcpu_ms().ok(),
            "heap_before_mib":heap_before,"heap_after_mib":fastly::compute_runtime::heap_memory_snapshot_mib().ok(),
            "construction_rounds":rounds}));

        Ok(())
    }
}

/// Controlled fixture inputs exercise real adapter boundaries, not provider outages.
pub fn faults() -> Result<(), Error> {
    use edgezero_adapter_fastly::request::FastlyService;
    finish(serve_custom(
        serving(),
        |mut req: Request, retained: &mut Sandbox<App>| {
            let obs = observation(&req);
            let token = req
                .get_header_str("x-request-token")
                .unwrap_or("missing")
                .to_owned();
            let path = req.get_path().to_owned();
            if path == "/constructor-panic" {
                PANIC_IN_CONSTRUCTOR.store(true, Ordering::SeqCst);
            }
            retained.initialize(|| App::build::<MeasuredApp>(FASTLY_PLATFORM))?;
            let app = retained.state().unwrap();
            initialized(&obs, &token);
            if path == "/inbound-error" {
                let mut upstream =
                    Request::get("http://fault-origin/abort").send("fault-origin")?;
                req.set_body(upstream.take_body());
            }
            let prepare = |raw: &mut Request, extensions: &mut Extensions| {
                if path == "/collect-error" || path == "/inbound-error" {
                    raw.set_url(format!("http://example.com/probe{}", path));
                }
                extensions.insert(obs.clone());
            };
            let finalize = |response: &mut CoreResponse| {
                if path == "/collect-error" {
                    *response.status_mut() = StatusCode::OK;
                    *response.body_mut() = Body::from_stream(futures::stream::once(async {
                        Err(EdgeError::internal(std::io::Error::other(
                            "injected core stream failure",
                        )))
                    }));
                }
                response.status()
            };
            let result = if path == "/recoverable-store-error" || path == "/terminal-store-error" {
                FastlyService::new(app)
                    .with_kv("missing_fixture_store")
                    .require_kv()
                    .send_request_with_hooks(req, prepare, finalize)
            } else {
                send_request_with_registries_and_hooks(
                    app,
                    MeasuredApp::stores(),
                    req,
                    prepare,
                    finalize,
                )
            };
            delivery_observed(&result, &obs, &token);
            let (status, delivery) = match result {
                FastlyRequestDelivery::Routed { state, delivery } => (Some(state), delivery),
                FastlyRequestDelivery::Detached(delivery) => (None, delivery),
                FastlyRequestDelivery::Aborted => return Ok(()),
                FastlyRequestDelivery::NoAttempt(reason) => {
                    let error = Error::msg(reason.message());
                    println!(
                        "{}",
                        serde_json::json!({"event":"adapter_error","instance":obs.instance,"ordinal":obs.ordinal,"token":token,"path":path,"error":error.to_string(),"source":"controlled_fixture_input"})
                    );
                    if path == "/recoverable-store-error" {
                        // Explicit custom policy permits another callback; standard helpers propagate.
                        Response::from_status(503)
                            .with_body("injected selector failed")
                            .send_to_client();
                        return Ok(());
                    } else {
                        Response::from_status(500)
                            .with_body("injected selector failed")
                            .send_to_client();
                        lifecycle("terminal_error", &obs, &token);
                        return Err(error);
                    }
                }
                _ => return Err(Error::msg("unsupported request delivery disposition")),
            };
            if let Some(error) = delivery.error() {
                let error = Error::msg(error.message());
                if delivery.commitment() == FastlyResponseCommitState::Committed {
                    post_commit_error(&obs, &token, &error);
                }
                lifecycle("terminal_error", &obs, &token);
                return Err(error);
            }
            if path == "/inbound-error" {
                assert_eq!(status, Some(StatusCode::INTERNAL_SERVER_ERROR));
                lifecycle("ingress_error_response", &obs, &token);
            }
            lifecycle("delivery_completed", &obs, &token);
            Ok(())
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::http::Response as CoreResponse;

    #[test]
    fn finalization_preserves_streaming_body_and_request_local_headers() {
        for (attempts, token) in [(1, "first"), (2, "second")] {
            let mut response = CoreResponse::new(Body::stream(futures::stream::once(async {
                Body::from("chunk").into_bytes().unwrap()
            })));
            response
                .extensions_mut()
                .insert(fixture_core::Finalize(token.to_owned()));

            finalize_response(&mut response, attempts);

            assert_eq!(response.headers()["x-fixture-finalized"], token);
            assert_eq!(
                response.headers()["x-fixture-attempts"],
                attempts.to_string()
            );
            assert!(
                response
                    .extensions()
                    .get::<fixture_core::Finalize>()
                    .is_none()
            );
            assert!(matches!(response.body(), Body::Stream(_)));
        }
    }
}
