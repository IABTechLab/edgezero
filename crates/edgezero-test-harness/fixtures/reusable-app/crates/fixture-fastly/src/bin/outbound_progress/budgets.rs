use edgezero_adapter_fastly::{
    FASTLY_PLATFORM, context::FastlyOutboundObservation, lifecycle::run_custom,
    outbound::FastlyOutboundClient, request::FastlyService,
};
use edgezero_core::{
    BadGatewayReason, Body, BudgetSource, Deadline, EdgeError, HttpClient, MonotonicClock,
    MonotonicInstant, OutboundCollectedSlot, OutboundHttpClient, OutboundRequest, OutboundResponse,
    action,
    app::App,
    context::RequestContext,
    extractor::State,
    http::{Response, Uri},
    outbound::test_support::{NativeBodySnapshot, NativeBodyWitness},
    router::RouterService,
};
use fixture_core::qualification::progress::probe_failure;
use futures::stream;
use serde_json::{Value, json};
use std::{cell::Cell, rc::Rc, sync::Arc, task::Poll, time::Duration};

const VARIANT_TIMEOUT_MS: u64 = 500;

#[derive(Clone)]
struct Config {
    case: String,
    token: String,
    base: Uri,
    second: u16,
}

fn check(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(probe_failure()) }
}

fn at(start: MonotonicInstant, instant: MonotonicInstant) -> Result<u64, EdgeError> {
    instant
        .checked_duration_since(start)
        .and_then(|duration| u64::try_from(duration.as_micros()).ok())
        .ok_or_else(probe_failure)
}

fn deadline(start: MonotonicInstant, millis: u64) -> Result<Deadline, EdgeError> {
    start
        .checked_add(Duration::from_millis(millis))
        .map(Deadline::at_instant)
        .ok_or_else(probe_failure)
}

fn request(config: &Config, label: &str, timeout: u64) -> Result<OutboundRequest, EdgeError> {
    Ok(OutboundRequest::get(format!("{}/{label}", config.base))?
        .header("x-request-token", config.token.clone())?
        .timeout(Duration::from_millis(timeout))
        .max_response_bytes(64))
}

fn variant_request(config: &Config, label: &str) -> Result<OutboundRequest, EdgeError> {
    check(["baseline", "scheme", "port", "host", "connect"].contains(&label))?;
    let authority = if label == "port" {
        format!("127.0.0.1:{}", config.second)
    } else {
        config
            .base
            .authority()
            .ok_or_else(probe_failure)?
            .to_string()
    };
    let target = Uri::builder()
        .scheme(if label == "scheme" { "https" } else { "http" })
        .authority(authority)
        .path_and_query(format!("{}/identity", config.base.path()))
        .build()
        .map_err(EdgeError::internal)?;
    let mut input = OutboundRequest::get(target.to_string())?
        .header("x-request-token", config.token.clone())?
        .timeout(Duration::from_millis(VARIANT_TIMEOUT_MS))
        .max_response_bytes(64);
    if label == "host" {
        input = input.host_authority_override("fixture.invalid")?;
    } else if label == "connect" {
        input = input.connect_timeout(Duration::from_millis(250));
    }
    Ok(input)
}

fn request_settings(request: &OutboundRequest) -> Value {
    json!({"target":request.uri().to_string(),"method":request.method().as_str(),
        "scheme":request.uri().scheme_str(),"host":request.host_name(),"port":request.uri().port_u16(),
        "authority":request.host_authority(),"host_override":request.has_host_authority_override(),
        "connect_timeout_ms":request.connect_timeout_setting().map(|duration|duration.as_millis()),
        "cert_host":request.cert_host(),"sni_hostname":request.sni_hostname()})
}

fn stats(observation: &FastlyOutboundObservation) -> Result<Value, EdgeError> {
    let value = observation.snapshot();
    Ok(
        json!({"configurations":value.configuration_count(),"registrations":value.registration_count(),
        "total_registration_us":u64::try_from(value.total_registration_duration().as_micros()).map_err(EdgeError::internal)?,
        "max_registration_us":u64::try_from(value.max_registration_duration().as_micros()).map_err(EdgeError::internal)?}),
    )
}

fn record(
    label: &str,
    mode: &str,
    timeout: u64,
    snapshot: NativeBodySnapshot,
    start: MonotonicInstant,
    finished: MonotonicInstant,
    result: Result<Vec<u8>, EdgeError>,
) -> Result<Value, EdgeError> {
    let prepared = snapshot.preparation.as_ref().ok_or_else(probe_failure)?;
    let bad_gateway_reason = match &result {
        Err(EdgeError::BadGateway {
            reason: BadGatewayReason::Unreachable,
            ..
        }) => Some("Unreachable"),
        _ => None,
    };
    let sdk_outcome = match &result {
        Err(EdgeError::BadGateway {
            reason: BadGatewayReason::Unreachable,
            ..
        }) => Some("Unreachable"),
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::PerCallTimeout,
            ..
        }) => Some("PerCallTimeout"),
        Err(EdgeError::GatewayTimeout {
            cause: BudgetSource::ConnectTimeout,
            ..
        }) => Some("ConnectTimeout"),
        _ => None,
    };
    let (status, kind, cause, bytes) = match result {
        Ok(bytes) => (200, Value::Null, Value::Null, bytes),
        Err(error) => {
            let cause = match &error {
                EdgeError::GatewayTimeout { cause, .. } => json!(format!("{cause:?}")),
                _ => Value::Null,
            };
            (
                error.status().as_u16(),
                json!(error.kind()),
                cause,
                Vec::new(),
            )
        }
    };
    let timeout_wait = snapshot
        .last_wait_timeout
        .map(|wait| -> Result<Value, EdgeError> {
            Ok(
                json!({"kind":"BodyReadiness","outcome":format!("{:?}",wait.outcome),
            "started_us":at(start,wait.started_at)?,"observed_us":at(start,wait.observed_at)?}),
            )
        })
        .transpose()?;
    let operation_poll = snapshot
        .first_operation_poll
        .map(|poll| -> Result<Value, EdgeError> {
            Ok(
                json!({"pending":poll.pending,"started_us":at(start,poll.started_at)?,
                "observed_us":at(start,poll.observed_at)?}),
            )
        })
        .transpose()?;
    Ok(
        json!({"label":label,"mode":mode,"timeout_ms":timeout,"status":status,
        "error_kind":kind,"bad_gateway_reason":bad_gateway_reason,"sdk_outcome":sdk_outcome,"timeout_source":cause,"bytes":bytes,
        "entered_us":at(start,prepared.entered_at)?,
        "pending_acquired_us":snapshot.first_operation_acquisition.map(|instant|at(start,instant)).transpose()?,
        "finished_us":at(start,finished)?,"deadline_us":at(start,prepared.budget.deadline.instant())?,
        "budget_source":format!("{:?}",prepared.budget.cause),
        "native":{"invalid":snapshot.invalid,"attachments":snapshot.attachments,
            "preparations":snapshot.preparations,"operations":snapshot.operation_acquisitions,
            "preparation_target":prepared.target.to_string(),"preparation_mode":format!("{:?}",prepared.mode),
            "operation_polls":snapshot.operation_polls,"ready_operation_polls":snapshot.ready_operation_polls,
            "first_operation_poll":operation_poll,
            "operation_released_us":snapshot.first_operation_release.map(|instant|at(start,instant)).transpose()?,
            "operation_releases":snapshot.operation_releases,"heads":snapshot.heads,
            "sources":snapshot.source_acquisitions,"releases":snapshot.source_releases,
            "reads":snapshot.reads,"ready_reads":snapshot.ready_reads,"waits":snapshot.waits,"last_timeout":timeout_wait}}),
    )
}

async fn payload(result: Result<OutboundResponse, EdgeError>) -> Result<Vec<u8>, EdgeError> {
    let response = result?;
    check(response.status().as_u16() == 200)?;
    Ok(response.into_bytes_bounded(64).await?.to_vec())
}

async fn send(
    client: &HttpClient,
    clock: &MonotonicClock,
    start: MonotonicInstant,
    label: &str,
    timeout: u64,
    mut request: OutboundRequest,
) -> Result<Value, EdgeError> {
    let mode = if request.is_stream_response() {
        "Streamed"
    } else {
        "Buffered"
    };
    let witness = NativeBodyWitness::new();
    witness.attach(&mut request)?;
    let outcome = payload(client.send(request).await).await;
    let finished = clock.now();
    record(
        label,
        mode,
        timeout,
        witness.snapshot(),
        start,
        finished,
        outcome,
    )
}

async fn batch(
    client: &HttpClient,
    clock: &MonotonicClock,
    start: MonotonicInstant,
    config: &Config,
    remaining: u64,
    labels: &[&str],
    timeouts: &[u64],
) -> Result<(Value, Vec<Value>), EdgeError> {
    let mut witnesses = Vec::with_capacity(labels.len());
    let mut requests = Vec::with_capacity(labels.len());
    for (&label, &timeout) in labels.iter().zip(timeouts) {
        let witness = NativeBodyWitness::new();
        let mut input = request(config, label, timeout)?;
        witness.attach(&mut input)?;
        witnesses.push(witness);
        requests.push(input);
    }
    let anchor = clock.now();
    let observation_cutoff = deadline(anchor, remaining)?;
    let batch = client.start_batch_until(requests, observation_cutoff);
    let collected = batch
        .collect()
        .await
        .map_err(|failure| failure.into_parts().0)?;
    let collected_at = clock.now();
    let (slots, termination) = collected.into_parts();
    check(slots.len() == labels.len())?;
    let mut values = Vec::with_capacity(slots.len());
    for (index, slot) in slots.into_iter().enumerate() {
        let OutboundCollectedSlot::Resolved(slot) = slot else {
            return Err(probe_failure());
        };
        let snapshot = witnesses[index].snapshot();
        let entered = snapshot
            .preparation
            .as_ref()
            .ok_or_else(probe_failure)?
            .entered_at;
        let arrival = entered
            .checked_add(slot.elapsed)
            .ok_or_else(probe_failure)?;
        let elapsed = u64::try_from(slot.elapsed.as_micros()).map_err(EdgeError::internal)?;
        let outcome = payload(slot.outcome).await;
        let mut value = record(
            labels[index],
            "Buffered",
            timeouts[index],
            snapshot,
            start,
            arrival,
            outcome,
        )?;
        value["index"] = json!(index);
        value["arrival_elapsed_us"] = json!(elapsed);
        values.push(value);
    }
    Ok((
        json!({"cutoff_anchor_us":at(start,anchor)?,"cutoff_us":at(start,observation_cutoff.instant())?,
        "termination":format!("{termination:?}"),"collected_us":at(start,collected_at)?}),
        values,
    ))
}

#[action]
async fn probe(ctx: RequestContext, State(config): State<Config>) -> Result<Response, EdgeError> {
    let observation = FastlyOutboundObservation::current().ok_or_else(probe_failure)?;
    let client = ctx.http_client().ok_or_else(probe_failure)?;
    let clock = ctx.monotonic_clock().clone();
    let start = clock.now();
    let mut values = Vec::new();
    let mut snapshots = Vec::new();
    let mut batches = Vec::new();
    let mut settled = Value::Null;
    let mut termination = Value::Null;
    let mut cutoff_anchor = Value::Null;
    let mut observation_cutoff = Value::Null;
    match config.case.as_str() {
        "reuse" => {
            for remaining in [640, 655] {
                let (batch, observations) = batch(
                    &client,
                    &clock,
                    start,
                    &config,
                    remaining,
                    &["a", "b"],
                    &[800, 800],
                )
                .await?;
                batches.push(batch);
                values.extend(observations);
                snapshots.push(stats(&observation)?);
            }
        }
        "variants" => {
            // Configurable TLS is deferred; the mandatory native readback companion covers SSL.
            for label in ["baseline", "scheme", "port", "host", "connect"] {
                let input = variant_request(&config, label)?;
                let settings = request_settings(&input);
                let mut value =
                    send(&client, &clock, start, label, VARIANT_TIMEOUT_MS, input).await?;
                value["request"] = settings;
                values.push(value);
                snapshots.push(stats(&observation)?);
            }
        }
        "head" => values.push(
            send(
                &client,
                &clock,
                start,
                "head",
                100,
                request(&config, "head", 100)?,
            )
            .await?,
        ),
        "body" => {
            for label in ["buffered", "streamed"] {
                let mut input = request(&config, label, 500)?;
                if label == "streamed" {
                    input = input.stream_response();
                }
                values.push(send(&client, &clock, start, label, 500, input).await?);
            }
        }
        "slot" => {
            let (batch, observations) = batch(
                &client,
                &clock,
                start,
                &config,
                1000,
                &["silent", "a", "b"],
                &[200, 1000, 1000],
            )
            .await?;
            termination = batch["termination"].clone();
            cutoff_anchor = batch["cutoff_anchor_us"].clone();
            observation_cutoff = batch["cutoff_us"].clone();
            batches.push(batch);
            values = observations;
        }
        "settlement" => {
            let observed = Rc::new(Cell::new(None));
            let observed_body = Rc::clone(&observed);
            let body_clock = clock.clone();
            let body = Body::from_stream(stream::poll_fn(move |_| {
                std::thread::sleep(Duration::from_millis(26));
                observed_body.set(Some(body_clock.now()));
                Poll::Ready(None::<Result<_, EdgeError>>)
            }));
            values.push(
                send(
                    &client,
                    &clock,
                    start,
                    "settlement",
                    1000,
                    request(&config, "settlement", 1000)?.body(body),
                )
                .await?,
            );
            settled = json!(at(start, observed.get().ok_or_else(probe_failure)?)?);
        }
        _ => return Err(probe_failure()),
    }
    let mut report = json!({"case":config.case,"token":config.token,"qualification":false,
        "observations":values,"stats":snapshots,"batches":batches,"settled_us":settled,
        "termination":termination,"cutoff_anchor_us":cutoff_anchor,"cutoff_us":observation_cutoff});
    if config.case == "variants" {
        report["target_policy"] = json!("bound-non-listening");
        report["tls_policy"] = json!("fixed-strict");
        report["configurable_tls_deferred"] = json!(true);
    }
    let mut response = Response::new(Body::from(
        serde_json::to_vec(&report).map_err(EdgeError::internal)?,
    ));
    response.headers_mut().insert(
        "content-type",
        edgezero_core::http::HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

pub(super) fn run(request: fastly::Request) -> Result<(), fastly::Error> {
    let case = request
        .get_path()
        .strip_prefix("/progress/budgets/")
        .ok_or_else(probe_failure)?
        .to_owned();
    let token = request
        .get_header_str("x-request-token")
        .ok_or_else(probe_failure)?
        .to_owned();
    let base: Uri = request
        .get_header_str("x-fixture-backend")
        .ok_or_else(probe_failure)?
        .parse()?;
    let second: u16 = request
        .get_header_str("x-budget-second-port")
        .ok_or_else(probe_failure)?
        .parse()?;
    check(
        ["reuse", "variants", "head", "body", "slot", "settlement"].contains(&case.as_str())
            && !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && second > 0
            && fixture_core::is_loopback_http(&base)
            && base.port_u16().is_some_and(|port| port > 0)
            && base.path() == format!("/budget/{token}/{case}")
            && base.query().is_none(),
    )?;
    run_custom::<(), _, _>(request, |request, _| {
        let provider = Arc::new(FastlyOutboundClient::default());
        let time = provider.time();
        let client = HttpClient::new(provider);
        let router = RouterService::builder()
            .with_state(Config {
                case,
                token,
                base,
                second,
            })
            .get("/progress/budgets/{case}", probe)
            .build();
        let mut app = App::with_name_and_platform(router, "budget-probe", FASTLY_PLATFORM);
        app.set_monotonic_clock(time.clock().clone());
        fixture_fastly::strict_delivery(FastlyService::new(&app).send_request_with_hooks(
            request,
            |_, extensions| {
                extensions.insert(client);
            },
            |_| {},
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::{
        BadGatewayReason, BudgetSource, DispatchBudget, OutboundSendMode,
        outbound::test_support::{NativeBodyPreparation, NativeOperationPoll},
    };

    #[test]
    fn budget_variants_change_only_their_transport_setting_with_report_only_labels() {
        let config = Config {
            case: "variants".to_owned(),
            token: "owned".to_owned(),
            base: "http://127.0.0.1:4101/budget/owned/variants"
                .parse()
                .unwrap(),
            second: 4102,
        };
        let baseline = variant_request(&config, "baseline").unwrap().into_parts();
        for (label, scheme, port, authority, connect, cert) in [
            ("baseline", "http", 4101, "127.0.0.1:4101", None, None),
            (
                "scheme",
                "https",
                4101,
                "127.0.0.1:4101",
                None,
                Some("127.0.0.1"),
            ),
            ("port", "http", 4102, "127.0.0.1:4102", None, None),
            ("host", "http", 4101, "fixture.invalid", None, None),
            ("connect", "http", 4101, "127.0.0.1:4101", Some(250), None),
        ] {
            let input = variant_request(&config, label).unwrap();
            assert_eq!(
                request_settings(&input),
                json!({
                "target":format!("{scheme}://127.0.0.1:{port}/budget/owned/variants/identity"),
                "method":"GET","scheme":scheme,"host":"127.0.0.1","port":port,
                "authority":authority,"host_override":label=="host","connect_timeout_ms":connect,
                "cert_host":cert,"sni_hostname":null})
            );
            let parts = input.into_parts();
            assert_eq!(parts.uri.path_and_query(), baseline.uri.path_and_query());
            assert_eq!(parts.method, baseline.method);
            assert_eq!(parts.headers, baseline.headers);
            assert_eq!(parts.body.as_bytes(), baseline.body.as_bytes());
            assert_eq!(parts.timeout, baseline.timeout);
            assert_eq!(parts.timeout, Some(Duration::from_millis(500)));
            assert_eq!(
                parts.deadline.as_ref().map(Deadline::instant),
                baseline.deadline.as_ref().map(Deadline::instant)
            );
            assert_eq!(parts.response_mode, baseline.response_mode);
            assert_eq!(
                parts.response_mode,
                edgezero_core::ResponseMode::Buffered { max_bytes: 64 }
            );
            assert_eq!(
                parts.max_request_body_bytes,
                baseline.max_request_body_bytes
            );
            assert_eq!(
                parts.max_encoded_response_bytes,
                baseline.max_encoded_response_bytes
            );
            assert_eq!(
                parts.max_decoded_response_bytes,
                baseline.max_decoded_response_bytes
            );
            assert_eq!(parts.max_decoder_bytes, baseline.max_decoder_bytes);
            assert_eq!(
                parts.max_brotli_window_bits,
                baseline.max_brotli_window_bits
            );
            assert_eq!(parts.max_chunk_bytes, baseline.max_chunk_bytes);
            assert_eq!(
                parts.max_response_header_bytes,
                baseline.max_response_header_bytes
            );
            assert_eq!(
                parts.max_response_header_count,
                baseline.max_response_header_count
            );
            assert_eq!(parts.cache_policy, baseline.cache_policy);
            assert_eq!(parts.response_decoding, baseline.response_decoding);
            assert_eq!(parts.proxy_marker, baseline.proxy_marker);
            assert!(parts.image_optimizer.is_none());
        }
        assert!(variant_request(&config, "tls").is_err());
    }

    #[test]
    fn budget_record_keeps_sdk_outcomes_typed() {
        let start = MonotonicInstant::now();
        for (error, expected) in [
            (
                EdgeError::bad_gateway_with_reason("native", BadGatewayReason::Unreachable),
                Some("Unreachable"),
            ),
            (
                EdgeError::bad_gateway_with_reason("native", BadGatewayReason::Transport),
                None,
            ),
            (EdgeError::bad_gateway("native"), None),
            (
                EdgeError::gateway_timeout_caused("native", BudgetSource::PerCallTimeout),
                Some("PerCallTimeout"),
            ),
            (
                EdgeError::gateway_timeout_caused("native", BudgetSource::ConnectTimeout),
                Some("ConnectTimeout"),
            ),
            (EdgeError::gateway_timeout("native"), None),
        ] {
            let unreachable = expected == Some("Unreachable");
            let snapshot = NativeBodySnapshot {
                preparation: Some(NativeBodyPreparation {
                    target: "https://127.0.0.1:4101/budget/owned/variants/identity"
                        .parse()
                        .unwrap(),
                    mode: OutboundSendMode::Send,
                    entered_at: start,
                    budget: DispatchBudget {
                        cause: BudgetSource::PerCallTimeout,
                        deadline: deadline(start, 500).unwrap(),
                        duration: Duration::from_millis(500),
                    },
                }),
                first_operation_acquisition: Some(start),
                first_operation_poll: Some(NativeOperationPoll {
                    started_at: start,
                    observed_at: start,
                    pending: false,
                }),
                first_operation_release: Some(start),
                operation_polls: 1,
                ready_operation_polls: 1,
                ..NativeBodySnapshot::default()
            };
            let value = record(
                "scheme",
                "Buffered",
                500,
                snapshot,
                start,
                start,
                Err(error),
            )
            .unwrap();
            assert_eq!(
                value["bad_gateway_reason"],
                if unreachable {
                    json!("Unreachable")
                } else {
                    Value::Null
                }
            );
            assert_eq!(value["sdk_outcome"], json!(expected));
            assert_eq!(
                value["native"]["preparation_target"],
                "https://127.0.0.1:4101/budget/owned/variants/identity"
            );
            assert_eq!(value["native"]["preparation_mode"], "Send");
            assert_eq!(value["native"]["operation_polls"], 1);
            assert_eq!(value["native"]["ready_operation_polls"], 1);
            assert_eq!(
                value["native"]["first_operation_poll"],
                json!({"pending":false,"started_us":0,"observed_us":0})
            );
            assert_eq!(value["native"]["operation_released_us"], 0);
        }
    }
}
