//! Native transport controls. The decorator observes and delegates every adapter operation.

mod connect;
mod paired;

use std::{
    convert::Infallible,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use edgezero_core::context::RequestContext;
use edgezero_core::http::{Method, Response, Uri};
use edgezero_core::{
    Body, Deadline, EdgeError, HttpClient, OutboundBatch, OutboundBatchTermination,
    OutboundCachePolicy, OutboundCollectedSlot, OutboundDispatchRequest, OutboundFuture,
    OutboundHead, OutboundHttpClient, OutboundImageOptimizerOptions, OutboundImageOptimizerRegion,
    OutboundPending, OutboundPreparedBatch, OutboundProgressRuntime, OutboundRedirectAction,
    OutboundRedirectLimit, OutboundRequest, OutboundResponse, OutboundSendMode, OutboundTime,
    ResponseDecoding,
};
use serde_json::{Value, json};

pub const PARITY_CASES: &[&str] = &[
    "parity-default",
    "parity-marker-off",
    "parity-marker-on",
    "parity-post",
    "parity-head",
    "parity-204",
    "parity-304",
    "parity-redirect",
    "parity-final-cap",
    "parity-decoded-cap",
    "parity-encoded-cap",
    "parity-gzip",
    "parity-original",
    "parity-opaque",
    "parity-upload-cap",
    "parity-zero-timeout",
    "parity-expired",
    "parity-empty-post",
    "parity-empty-delete",
    "parity-empty-options",
];

#[derive(Clone, Copy)]
pub struct BackendSnapshot(pub fn() -> Value);

#[derive(Default)]
struct Activity {
    support: Vec<Value>,
    dispatches: usize,
    begins: usize,
    batches: usize,
}

struct ObservedClient {
    native: Arc<dyn OutboundHttpClient>,
    activity: Arc<Mutex<Activity>>,
}

impl OutboundHttpClient for ObservedClient {
    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        self.native.progress()
    }
    fn time(&self) -> OutboundTime {
        self.native.time()
    }
    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        let result = self.native.validate_supported_options(request, mode);
        self.activity
            .lock()
            .unwrap()
            .support
            .push(json!({"mode":format!("{mode:?}"),
            "optimizer":request.image_optimizer_options().is_some(),"accepted":result.is_ok()}));
        result
    }
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.activity.lock().unwrap().dispatches += 1;
        self.native.dispatch_head(request)
    }
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        self.activity.lock().unwrap().begins += 1;
        self.native.begin(request)
    }
    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        self.activity.lock().unwrap().batches += 1;
        self.native.start_batch_until(batch)
    }
}

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid native transport control")
}

fn field<'a>(ctx: &'a RequestContext, name: &str) -> Result<&'a str, EdgeError> {
    let mut values = ctx.headers().get_all(name).iter();
    let value = values.next().ok_or_else(invalid)?;
    if values.next().is_some() {
        return Err(invalid());
    }
    value.to_str().map_err(|_| invalid())
}

fn origin(text: &str) -> Result<Uri, EdgeError> {
    let uri: Uri = text.parse().map_err(|_| invalid())?;
    if uri.scheme_str() != Some("http")
        || uri.host() != Some("127.0.0.1")
        || !uri.port_u16().is_some_and(|p| p != 0)
        || uri.path() != "/"
        || uri.query().is_some()
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
    {
        return Err(invalid());
    }
    Ok(uri)
}

fn target(origin: &Uri, case: &str, token: &str) -> Result<Uri, EdgeError> {
    format!(
        "http://{}/transport/{case}/{token}",
        origin.authority().ok_or_else(invalid)?
    )
    .parse()
    .map_err(|_| invalid())
}

fn request(uri: Uri, token: &str, step: &str) -> Result<OutboundRequest, EdgeError> {
    OutboundRequest::new(Method::GET, uri)?
        .header("x-request-token", token)?
        .header("x-transport-step", step)
        .map(|r| {
            r.timeout(Duration::from_secs(3))
                .max_response_bytes(64)
                .max_encoded_response_bytes(256)
                .max_decoded_response_bytes(64)
        })
}

fn optimizer(request: OutboundRequest) -> OutboundRequest {
    request.image_optimizer(
        OutboundImageOptimizerOptions::new().region(OutboundImageOptimizerRegion::UsEast),
    )
}

fn error(error: &EdgeError) -> Value {
    let (reason, cause) = match error {
        EdgeError::BadGateway { reason, .. } => (Some(format!("{reason:?}")), None),
        EdgeError::ResponseTooLarge { reason, .. } => (Some(format!("{reason:?}")), None),
        EdgeError::GatewayTimeout { cause, .. } => (None, Some(format!("{cause:?}"))),
        _ => (None, None),
    };
    let rejection = match error.message().as_str() {
        "image_optimizer is not supported by axum"
        | "image_optimizer is not supported by cloudflare"
        | "image_optimizer is not supported by spin"
        | "image_optimizer is not supported by fastly begun requests or batches" => {
            Some("unsupported_optimizer")
        }
        "GET/HEAD request must not carry a body" => Some("get_body"),
        "image_optimizer requires GET or HEAD" => Some("optimizer_method"),
        text if text.contains("image_optimizer") && text.contains("cache") => {
            Some("optimizer_cache")
        }
        _ => None,
    };
    json!({"kind":error.kind(),"status":error.status().as_u16(),"reason":reason,"cause":cause,
        "rejection":rejection,"upload_origin":error.request_body_overflow_origin().map(|o|format!("{o:?}")),
        "provider":error.provider_failure_detail().map(|p|format!("{p:?}"))})
}

async fn response(result: Result<OutboundResponse, EdgeError>) -> Result<Value, EdgeError> {
    let response = match result {
        Ok(response) => response,
        Err(e) => return Ok(json!({"error":error(&e)})),
    };
    let status = response.status().as_u16();
    let mut headers = response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    headers.sort_unstable();
    match response.into_bytes_bounded(512).await {
        Ok(bytes) => Ok(json!({"status":status,"headers":headers,"bytes":bytes.to_vec()})),
        Err(e) => Ok(json!({"error":error(&e)})),
    }
}

fn snapshot(observe: Option<BackendSnapshot>) -> Value {
    observe.map_or(Value::Null, |observe| (observe.0)())
}

#[derive(Default)]
struct NativeObservation {
    #[cfg(feature = "native-body-witness")]
    witness: edgezero_core::outbound::test_support::NativeBodyWitness,
}

impl NativeObservation {
    fn attach(request: &mut OutboundRequest) -> Result<Self, EdgeError> {
        let observation = Self::default();
        #[cfg(feature = "native-body-witness")]
        observation.witness.attach(request)?;
        #[cfg(not(feature = "native-body-witness"))]
        let _ = request;
        Ok(observation)
    }

    fn report(&self) -> Value {
        #[cfg(feature = "native-body-witness")]
        {
            let state = self.witness.snapshot();
            json!({"attachments":state.attachments,"invalid":state.invalid,
                "preparations":state.preparations,"prepared":state.preparation.map(|p|
                    json!({"mode":format!("{:?}",p.mode),"target":p.target.to_string()})),
                "operation_acquisitions":state.operation_acquisitions,"operation_polls":state.operation_polls,
                "operation_releases":state.operation_releases,"source_acquisitions":state.source_acquisitions,
                "source_releases":state.source_releases,"heads":state.heads,"reads":state.reads,"waits":state.waits})
        }
        #[cfg(not(feature = "native-body-witness"))]
        Value::Null
    }
}

async fn send_step(
    client: &HttpClient,
    outgoing: OutboundRequest,
    label: &str,
    observe: Option<BackendSnapshot>,
) -> Result<Value, EdgeError> {
    let uri = outgoing.uri().to_string();
    let parts = outgoing.into_parts();
    let settings = json!({"timeout_ms":parts.timeout.map(|d|d.as_millis()),
        "connect_ms":parts.connect_timeout.map(|d|d.as_millis())});
    let mut outgoing = OutboundRequest::from_parts(parts)?;
    let witness = NativeObservation::attach(&mut outgoing)?;
    let before = snapshot(observe);
    let outcome = response(client.send(outgoing).await).await?;
    Ok(
        json!({"step":label,"uri":uri,"settings":settings,"outcome":outcome,"native":witness.report(),"before":before,"after":snapshot(observe)}),
    )
}

async fn batch_step(
    client: &HttpClient,
    mut requests: Vec<OutboundRequest>,
    observe: Option<BackendSnapshot>,
) -> Result<Value, EdgeError> {
    let uris = requests
        .iter()
        .map(|r| r.uri().to_string())
        .collect::<Vec<_>>();
    let before = snapshot(observe);
    let witnesses = requests
        .iter_mut()
        .map(NativeObservation::attach)
        .collect::<Result<Vec<_>, _>>()?;
    let results = client
        .send_all_until(requests, Deadline::after(Duration::from_secs(3)))
        .await
        .map_err(|failure| failure.into_parts().0)?;
    let (slots, termination) = results.into_parts();
    if termination != OutboundBatchTermination::Completed || slots.len() != uris.len() {
        return Err(invalid());
    }
    let mut outcomes = Vec::new();
    for (index, slot) in slots.into_iter().enumerate() {
        let OutboundCollectedSlot::Resolved(slot) = slot else {
            return Err(invalid());
        };
        outcomes.push(json!({"index":index,"outcome":response(slot.outcome).await?}));
    }
    Ok(
        json!({"step":"batch","uris":uris,"termination":"Completed","slots":outcomes,
        "native":witnesses.iter().map(NativeObservation::report).collect::<Vec<_>>(),
        "before":before,"after":snapshot(observe)}),
    )
}

fn parity_request(
    case: &str,
    uri: Uri,
    token: &str,
    step: &str,
) -> Result<OutboundRequest, EdgeError> {
    let method = match case {
        "parity-head" => Method::HEAD,
        "parity-post" | "parity-upload-cap" | "parity-empty-post" => Method::POST,
        "parity-empty-delete" => Method::DELETE,
        "parity-empty-options" => Method::OPTIONS,
        _ => Method::GET,
    };
    let mut request = OutboundRequest::new(method, uri)?
        .header("x-request-token", token)?
        .header("x-transport-step", step)?;
    if matches!(case, "parity-post" | "parity-upload-cap") {
        request = request.body("abc");
    }
    request = match case {
        "parity-final-cap" => request.max_response_bytes(1),
        "parity-decoded-cap" => request.max_decoded_response_bytes(1),
        "parity-encoded-cap" => request.max_encoded_response_bytes(1),
        "parity-original" => request
            .response_decoding(ResponseDecoding::Original)
            .max_decoded_response_bytes(1)
            .max_decoder_bytes(0),
        "parity-upload-cap" => request.max_request_body_bytes(1),
        "parity-zero-timeout" => request.timeout(Duration::ZERO),
        "parity-expired" => request.deadline(Deadline::after(Duration::ZERO)),
        "parity-marker-on" => request.proxy_marker(edgezero_core::OutboundProxyMarker::On),
        _ => request,
    };
    Ok(request)
}

/// Runs only within a real provider callback. Native progress, time and batch dispatch are delegated.
pub async fn probe(
    ctx: RequestContext,
    adapter: &str,
    native: Arc<dyn OutboundHttpClient>,
) -> Result<Response, EdgeError> {
    let case = ctx
        .path_params()
        .get("case")
        .ok_or_else(invalid)?
        .to_owned();
    let token = field(&ctx, "x-request-token")?.to_owned();
    if token.is_empty()
        || token.len() > 64
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(invalid());
    }
    let first = origin(field(&ctx, "x-origin")?)?;
    let second = origin(field(&ctx, "x-origin-second")?)?;
    if first == second {
        return Err(invalid());
    }
    let observe = ctx.extensions().get::<BackendSnapshot>().copied();
    if (adapter == "fastly") != observe.is_some() {
        return Err(invalid());
    }
    let activity = Arc::new(Mutex::new(Activity::default()));
    let client = HttpClient::with_client(ObservedClient {
        native: Arc::clone(&native),
        activity: Arc::clone(&activity),
    })
    .with_proxy_marker(paired::vectors::find(&case).map_or(
        !matches!(case.as_str(), "parity-marker-off" | "parity-marker-on"),
        |case| case.client_marker(),
    ));
    let before = snapshot(observe);
    let mut steps = Vec::new();
    if let Some(spec) = paired::vectors::find(&case) {
        steps.push(
            paired::run(
                &client,
                native.as_ref(),
                spec,
                target(&first, &case, &token)?,
                &token,
                adapter != "spin",
            )
            .await?,
        );
    } else if PARITY_CASES.contains(&case.as_str()) {
        let uri = target(&first, &case, &token)?;
        let original = parity_request(&case, uri.clone(), &token, "send")?;
        let reference = response(client.send(original).await).await?;
        let begun = client
            .begin(parity_request(&case, uri, &token, "begin")?)
            .await;
        let result = match begun {
            Ok(pending) => pending.collect().await,
            Err(error) => Err(error),
        };
        let observed = response(result).await?;
        steps.push(json!({"step":"parity","send":reference,"begin":observed}));
    } else {
        let plain = target(&first, "plain", &token)?;
        match case.as_str() {
            "connect-timeout" if adapter == "spin" => {
                steps
                    .extend(connect::run(&client, native.as_ref(), &first, &second, &token).await?);
            }
            "connect-short" if adapter == "cloudflare" => {
                let outgoing = request(plain, &token, "connect-short")?
                    .connect_timeout(Duration::from_millis(1));
                steps.push(send_step(&client, outgoing, "connect-short", observe).await?);
            }
            "head-900" if adapter == "fastly" => {
                let outgoing = request(target(&first, "head-900", &token)?, &token, "head-900")?
                    .timeout(Duration::from_secs(1));
                steps.push(send_step(&client, outgoing, "head-900", observe).await?);
            }
            "unsupported" if matches!(adapter, "cloudflare" | "spin") => {
                steps.push(
                    send_step(
                        &client,
                        optimizer(request(plain.clone(), &token, "send")?),
                        "send",
                        observe,
                    )
                    .await?,
                );
                steps.push(
                    batch_step(
                        &client,
                        vec![
                            optimizer(request(plain.clone(), &token, "batch")?),
                            request(plain, &token, "plain")?,
                        ],
                        observe,
                    )
                    .await?,
                );
            }
            "optimizer" | "post" | "body" | "bypass" | "identity" if adapter == "fastly" => {
                let mut outgoing = optimizer(request(plain.clone(), &token, "optimizer")?);
                if case == "post" {
                    outgoing = optimizer(
                        OutboundRequest::new(Method::POST, plain.clone())?
                            .header("x-request-token", token.clone())?
                            .header("x-transport-step", "optimizer")?
                            .timeout(Duration::from_secs(3)),
                    );
                }
                if case == "body" {
                    outgoing = outgoing.body("x");
                }
                if case == "bypass" {
                    outgoing = outgoing.cache_policy(OutboundCachePolicy::Bypass);
                }
                steps.push(send_step(&client, outgoing, "optimizer", observe).await?);
                if case == "identity" {
                    steps.push(
                        send_step(&client, request(plain, &token, "plain")?, "plain", observe)
                            .await?,
                    );
                }
            }
            "mixed" if adapter == "fastly" => {
                steps.push(
                    batch_step(
                        &client,
                        vec![
                            optimizer(request(plain.clone(), &token, "optimizer")?),
                            request(plain, &token, "plain")?,
                        ],
                        observe,
                    )
                    .await?,
                );
            }
            "target" if adapter == "fastly" => {
                let uri: Uri = format!(
                    "http://{}/transport/raw%2Ftarget/{token}?a=%2F&b=two%20words&a=1",
                    first.authority().ok_or_else(invalid)?
                )
                .parse()
                .map_err(|_| invalid())?;
                let outgoing = request(uri, &token, "target")?
                    .header("x-transport-proof", "application-value")?
                    .header("x-signature-test", "user-built-signature")?;
                steps.push(send_step(&client, outgoing, "target", observe).await?);
            }
            "redirect-identity" if adapter == "fastly" => {
                let a = target(&first, "redirect", &token)?;
                let b = target(&second, "second", &token)?;
                steps.push(
                    send_step(
                        &client,
                        request(a.clone(), &token, "single-a")?,
                        "single-a",
                        observe,
                    )
                    .await?,
                );
                steps.push(
                    send_step(
                        &client,
                        request(b, &token, "single-b")?,
                        "single-b",
                        observe,
                    )
                    .await?,
                );
                let parts = request(a, &token, "follow")?.into_parts();
                let settings = json!({"timeout_ms":parts.timeout.map(|d|d.as_millis()),
                    "connect_ms":parts.connect_timeout.map(|d|d.as_millis())});
                let outgoing = OutboundRequest::from_parts(parts)?;
                let stats = snapshot(observe);
                let followed = client
                    .follow(outgoing, OutboundRedirectLimit::new(2)?, |_| {
                        Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared)
                    })
                    .await
                    .map_err(|failure| match failure {
                        edgezero_core::OutboundFollowFailure::Hop { error, .. } => error,
                        edgezero_core::OutboundFollowFailure::Policy { error, .. } => {
                            match error {}
                        }
                    })?;
                let (response, uri, redirects, stop) = followed.into_parts();
                steps.push(json!({"step":"follow","uri":uri.to_string(),"redirects":redirects,
                    "stop":stop.map(|s|format!("{s:?}")),"settings":settings,"outcome":self::response(Ok(response)).await?,
                    "before":stats,"after":snapshot(observe)}));
            }
            "denied" if adapter == "spin" => {
                let result = client
                    .follow(
                        request(target(&first, "denied", &token)?, &token, "denied")?,
                        OutboundRedirectLimit::new(2)?,
                        |_| Ok::<_, Infallible>(OutboundRedirectAction::FollowPrepared),
                    )
                    .await;
                match result {
                    Err(edgezero_core::OutboundFollowFailure::Hop {
                        index,
                        error: failure,
                    }) => {
                        steps.push(json!({"step":"denied","hop":index,"outcome":{"error":error(&failure)}}));
                    }
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
    }
    let activity = activity.lock().unwrap();
    let report = json!({"adapter":adapter,"case":case,"token":token,"steps":steps,
        "activity":{"support":activity.support,"dispatches":activity.dispatches,"begins":activity.begins,"batches":activity.batches},
        "before":before,"after":snapshot(observe),"qualification":false});
    let encoded = report.to_string();
    if encoded.len() > 16 * 1024 {
        return Err(invalid());
    }
    Ok(Response::new(Body::from(encoded)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_transport_targets_reject_uncontrolled_origins() {
        for text in [
            "https://127.0.0.1:1234/",
            "http://localhost:1234/",
            "http://127.0.0.1/",
            "http://127.0.0.1:0/",
            "http://user@127.0.0.1:1234/",
            "http://127.0.0.1:1234/path",
            "http://127.0.0.1:1234/?x=1",
        ] {
            assert!(origin(text).is_err(), "{text}");
        }
        let base = origin("http://127.0.0.1:1234/").unwrap();
        assert_eq!(
            target(&base, "plain", "token").unwrap().to_string(),
            "http://127.0.0.1:1234/transport/plain/token"
        );
    }

    #[test]
    fn native_begin_parity_preserves_buffered_contract_settings_and_client_inheritance() {
        let uri: Uri = "http://127.0.0.1:1234/transport/plain/token"
            .parse()
            .unwrap();
        for case in PARITY_CASES {
            let request = parity_request(case, uri.clone(), "token", "begin").unwrap();
            assert!(!request.is_stream_body(), "{case}");
            assert_eq!(
                request.proxy_marker_setting(),
                if *case == "parity-marker-on" {
                    edgezero_core::OutboundProxyMarker::On
                } else {
                    edgezero_core::OutboundProxyMarker::Inherit
                }
            );
            assert_eq!(
                request.method(),
                if *case == "parity-head" {
                    Method::HEAD
                } else if matches!(
                    *case,
                    "parity-post" | "parity-upload-cap" | "parity-empty-post"
                ) {
                    Method::POST
                } else if *case == "parity-empty-delete" {
                    Method::DELETE
                } else if *case == "parity-empty-options" {
                    Method::OPTIONS
                } else {
                    Method::GET
                }
            );
        }
    }
}
