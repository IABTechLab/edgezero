use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, context::FastlyOutboundObservation, request::FastlyService,
};
use edgezero_core::{
    Body, EdgeError, MonotonicInstant, OutboundBatchTermination, OutboundCollectedSlot, action,
    app::App,
    context::RequestContext,
    extractor::State,
    http::{HeaderMap, Method, Response},
    outbound::test_support::NativeBodyWitness,
    router::RouterService,
};
use fixture_core::qualification::{build_identity, hosted::Plan};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid hosted qualification probe")
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn stamp(response: &mut fastly::Response) {
    if let Some(identity) = fixture_core::launch::value() {
        response.set_header(fixture_core::launch::HEADER, identity);
    }
}
fn us(start: MonotonicInstant, at: MonotonicInstant) -> Option<u64> {
    at.checked_duration_since(start)
        .and_then(|d| u64::try_from(d.as_micros()).ok())
}

fn settings(plan: &Plan) -> Value {
    json!({"timeout_ms":plan.timeout.map(|d|d.as_millis()),"deadline_ms":plan.deadline.map(|d|d.as_millis()),"phase_ms":plan.phase_millis(),"connect_ms":plan.connect.map(|d|d.as_millis()),"tls":plan.target.scheme_str()==Some("https"),"authority":plan.target.authority().map(|a|a.as_str()),"optimizer":plan.optimizer,"width":plan.optimizer.then_some(100),"format":plan.optimizer.then_some("webp"),"region":plan.optimizer.then_some(plan.region.as_str()),"preserve_query":plan.preserve_query,"query":if plan.case.starts_with("image-query-"){plan.target.query()}else{None}})
}

fn base(plan: &Plan, driver: &str) -> Result<Value, EdgeError> {
    let (revision, build_input) = build_identity()?;
    Ok(
        json!({"schema":1,"event":"hosted-outbound","driver":driver,"case":plan.case,"execution":plan.token,"revision":revision,"build_input_sha256":build_input,"settings":settings(plan),"qualification":false,"blocking_horizon":"Unknown"}),
    )
}

struct HostedState {
    plan: Plan,
    observed: Mutex<Option<(MonotonicInstant, NativeBodyWitness)>>,
}
struct EgressObserver(Arc<HostedState>);
impl edgezero_core::ResponseEgressObserver for EgressObserver {
    fn complete(&self, report: &edgezero_core::ResponseEgressReport) {
        let observation=self.0.observed.lock().expect("qualification witness").as_ref().map(|(start,witness)| {
            let snapshot=witness.snapshot();
            json!({"egress_terminal_us":us(*start,MonotonicInstant::now()),"head_us":snapshot.first_head.and_then(|at|us(*start,at)),
                "original_deadline_us":snapshot.preparation.as_ref().and_then(|prepared|us(*start,prepared.budget.deadline.instant())),
                "cause":snapshot.preparation.as_ref().map(|prepared|format!("{:?}",prepared.budget.cause)),
                "native_reads":snapshot.reads,"native_waits":snapshot.waits,"invalid":snapshot.invalid})
        });
        eprintln!(
            "{}",
            json!({"event":"hosted-egress","execution":self.0.plan.token,"outcome":format!("{:?}",report.outcome),"bytes_written":report.bytes_written,"observation":observation,"qualification":false})
        );
    }
}

fn parse(request: &fastly::Request) -> Result<(String, Plan, HeaderMap), EdgeError> {
    let path = request
        .get_path()
        .strip_prefix("/hosted/")
        .ok_or_else(invalid)?;
    let (driver, case) = path.split_once('/').ok_or_else(invalid)?;
    let method =
        Method::from_bytes(request.get_method().as_str().as_bytes()).map_err(|_| invalid())?;
    let mut headers = HeaderMap::new();
    for name in [
        "x-request-token",
        "x-qualification-target",
        "x-qualification-region",
        "accept",
    ] {
        let mut values = request.get_header_all(name);
        let first = values.next();
        if values.next().is_some() {
            return Err(invalid());
        }
        if let Some(value) = first {
            headers.insert(
                edgezero_core::http::HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| invalid())?,
                edgezero_core::http::HeaderValue::from_bytes(value.as_bytes())
                    .map_err(|_| invalid())?,
            );
        }
    }
    let plan = Plan::parse(case, method, &headers)?;
    if !matches!(driver, "native" | "candidate")
        || (driver == "native" && plan.case != "reuse" && !plan.optimizer)
    {
        return Err(invalid());
    }
    Ok((driver.to_owned(), plan, headers))
}

pub(super) fn run(request: fastly::Request) -> Result<(), fastly::Error> {
    let (driver, plan, headers) = match parse(&request) {
        Ok(plan) => plan,
        Err(_) => {
            let mut response = fastly::Response::from_status(400)
                .with_body("invalid hosted qualification probe\n");
            stamp(&mut response);
            response.send_to_client();
            return Ok(());
        }
    };
    if driver == "native" {
        return native(plan, &headers);
    }
    if driver != "candidate" {
        return Err(invalid().into());
    }
    let state = Arc::new(HostedState {
        plan,
        observed: Mutex::new(None),
    });
    let router = RouterService::builder()
        .middleware(fixture_core::launch::Identity)
        .with_state(state.clone())
        .route("/hosted/candidate/{case}", Method::GET, candidate)
        .route("/hosted/candidate/{case}", Method::HEAD, candidate)
        .build();
    let mut app = App::with_name_and_platform(router, "hosted-outbound", FASTLY_PLATFORM);
    app.set_response_egress_observer(EgressObserver(state));
    fixture_fastly::strict_delivery(FastlyService::new(&app).send_request_with_hooks(
        request,
        |_, _| {},
        |_| {},
    ))
}

#[action]
async fn candidate(
    ctx: RequestContext,
    State(state): State<Arc<HostedState>>,
) -> Result<Response, EdgeError> {
    let plan = &state.plan;
    let client = ctx.http_client().ok_or_else(invalid)?;
    let start = MonotonicInstant::now();
    let observation = FastlyOutboundObservation::current().ok_or_else(invalid)?;
    let mut report = base(plan, "candidate")?;
    let mut outcomes = Vec::new();
    for _ in 0..if plan.pair { 2 } else { 1 } {
        let mut request = plan.request(ctx.headers())?;
        let witness = NativeBodyWitness::new();
        witness.attach(&mut request)?;
        if plan.optimizer {
            *state.observed.lock().expect("qualification witness") = Some((start, witness.clone()));
        }
        let outcome = if plan.batch {
            let collected = client
                .send_all_until(
                    vec![request],
                    edgezero_core::Deadline::after(
                        plan.timeout.ok_or_else(invalid)? + Duration::from_secs(1),
                    ),
                )
                .await
                .map_err(|failure| failure.into_parts().0)?;
            if collected.termination() != OutboundBatchTermination::Completed {
                return Err(invalid());
            }
            let (mut slots, _) = collected.into_parts();
            match slots.pop().ok_or_else(invalid)? {
                OutboundCollectedSlot::Resolved(slot) => slot.outcome,
                _ => return Err(invalid()),
            }
        } else {
            client.send(request).await
        };
        let snapshot = witness.snapshot();
        let mut row = json!({"head_us":snapshot.first_head.and_then(|at|us(start,at)),"send_return_us":us(start,MonotonicInstant::now()),"native_reads":snapshot.reads,"native_waits":snapshot.waits,"host_call":snapshot.synchronous_call.map(|(entered,returned)|json!({"entered_us":us(start,entered),"returned_us":us(start,returned)})),"preparation":snapshot.preparation.as_ref().map(|p|json!({"entered_us":us(start,p.entered_at),"deadline_us":us(start,p.budget.deadline.instant()),"cause":format!("{:?}",p.budget.cause)}))});
        match outcome {
            Ok(response) => {
                row["status"] = json!(response.status().as_u16());
                row["headers"] = head_fields(response.headers());
                if plan.optimizer {
                    report["outcomes"] = json!([row]);
                    report["registrations"] = json!(observation.snapshot().registration_count());
                    eprintln!("{report}");
                    let mut response = response.into_response()?;
                    response.headers_mut().insert(
                        "x-qualification-report",
                        report.to_string().parse().map_err(EdgeError::internal)?,
                    );
                    return Ok(response);
                }
                match response.into_bytes_bounded(8 * 1024 * 1024).await {
                    Ok(bytes) => {
                        row["bytes"] = json!(bytes.len());
                        row["body_sha256"] = json!(digest(&bytes));
                    }
                    Err(error) => row["error"] = error_row(&error),
                };
            }
            Err(error) => row["error"] = error_row(&error),
        }
        let snapshot = witness.snapshot();
        row["native_reads"] = json!(snapshot.reads);
        row["native_waits"] = json!(snapshot.waits);
        row["terminal_us"] = json!(us(start, MonotonicInstant::now()));
        outcomes.push(row);
    }
    report["outcomes"] = json!(outcomes);
    let stats = observation.snapshot();
    report["registrations"] = json!(stats.registration_count());
    report["configurations"] = json!(stats.configuration_count());
    eprintln!("{report}");
    let mut response = Response::new(Body::from(report.to_string()));
    response.headers_mut().insert(
        "content-type",
        "application/json".parse().map_err(EdgeError::internal)?,
    );
    Ok(response)
}

fn error_row(error: &EdgeError) -> Value {
    let cause = match error {
        EdgeError::GatewayTimeout { cause, .. } => Some(format!("{cause:?}")),
        EdgeError::BadGateway {
            reason: edgezero_core::BadGatewayReason::Unreachable,
            ..
        } => Some("Unreachable".into()),
        _ => None,
    };
    json!({"kind":error.kind(),"status":error.status().as_u16(),"cause":cause})
}

fn head_fields(fields: &HeaderMap) -> Value {
    json!(
        fields
            .iter()
            .filter(|(name, _)| matches!(
                name.as_str(),
                "content-type"
                    | "content-length"
                    | "cache-control"
                    | "age"
                    | "x-cache"
                    | "x-origin-connection-id"
            ))
            .take(16)
            .map(|(name, value)| json!([
                name.as_str(),
                (value.as_bytes().len() <= 256)
                    .then(|| std::str::from_utf8(value.as_bytes()).ok())
                    .flatten()
            ]))
            .collect::<Vec<_>>()
    )
}

fn native(plan: Plan, headers: &HeaderMap) -> Result<(), fastly::Error> {
    if plan.case != "reuse" && !plan.optimizer {
        return Err(invalid().into());
    }
    let start = MonotonicInstant::now();
    let target = &plan.target;
    let authority = target.authority().ok_or_else(invalid)?.as_str();
    let host = target.host().ok_or_else(invalid)?;
    let port = target
        .port_u16()
        .unwrap_or(if target.scheme_str() == Some("https") {
            443
        } else {
            80
        });
    let phase = Duration::from_millis(plan.phase_millis());
    let identity = json!({"scheme":target.scheme_str(),"host":host,"port":port,"authority":authority,"phase_ms":plan.phase_millis(),"connect_ms":plan.connect.map(|d|d.as_millis())});
    let name = format!(
        "qualification_sdk_v1_{}",
        digest(identity.to_string().as_bytes())
    );
    let mut builder = fastly::Backend::builder(&name, format!("{host}:{port}"))
        .override_host(authority)
        .connect_timeout(plan.connect.unwrap_or(phase))
        .first_byte_timeout(phase)
        .between_bytes_timeout(phase);
    if target.scheme_str() == Some("https") {
        builder = builder.enable_ssl().check_certificate(host);
        if host.parse::<std::net::IpAddr>().is_err() {
            builder = builder.sni_hostname(host);
        }
    }
    let backend = builder.finish()?;
    let mut request = fastly::Request::new(plan.method.as_str(), target.to_string());
    request.set_header("x-request-token", &plan.token);
    request.set_header("accept-encoding", "identity");
    if plan.case == "reuse" {
        request.set_pass(true);
    }
    if let Some(accept) = headers.get("accept") {
        request.set_header("accept", accept.as_bytes());
    }
    if plan.optimizer {
        use edgezero_core::OutboundImageOptimizerRegion as Region;
        use fastly::image_optimizer::{
            Format, ImageOptimizerOptions, ImageOptimizerRegion, PixelsOrPercentage,
        };
        let region = match plan.region {
            Region::Asia => ImageOptimizerRegion::Asia,
            Region::Australia => ImageOptimizerRegion::Australia,
            Region::EuCentral => ImageOptimizerRegion::EuCentral,
            Region::EuWest => ImageOptimizerRegion::EuWest,
            Region::UsCentral => ImageOptimizerRegion::UsCentral,
            Region::UsEast => ImageOptimizerRegion::UsEast,
            Region::UsWest => ImageOptimizerRegion::UsWest,
        };
        let mut options = ImageOptimizerOptions::from_region(region);
        options.width = Some(PixelsOrPercentage::Pixels(100));
        options.format = Some(Format::WebP);
        options.preserve_query_string_on_origin_request = Some(plan.preserve_query);
        request.set_image_optimizer(options);
    }
    let entered = MonotonicInstant::now();
    let result = request.send(backend);
    let returned = MonotonicInstant::now();
    let mut report = base(&plan, "native")?;
    report["host_call"] = json!({"entered_us":us(start,entered),"returned_us":us(start,returned)});
    match result {
        Ok(mut response) => {
            report["status"] = json!(response.get_status().as_u16());
            eprintln!("{report}");
            response.set_header("x-qualification-report", report.to_string());
            stamp(&mut response);
            if plan.method == Method::HEAD {
                response.take_body();
            }
            response.send_to_client();
        }
        Err(_) => {
            report["error"] = json!({"kind":"native-sdk-send"});
            eprintln!("{report}");
            let mut response = fastly::Response::from_status(502).with_body(report.to_string());
            stamp(&mut response);
            response.send_to_client();
        }
    }
    Ok(())
}
