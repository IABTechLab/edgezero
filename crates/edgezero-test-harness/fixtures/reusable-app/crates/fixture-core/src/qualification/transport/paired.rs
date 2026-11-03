use super::NativeObservation;
use edgezero_core as core_api;
use edgezero_core::http::Uri;
use edgezero_core::{EdgeError, HttpClient, OutboundHttpClient, OutboundRequest};
use serde_json::{Value, json};

#[path = "../../../../../../../../edgezero-core/src/outbound/tests/buffered_parity/requests.rs"]
mod requests;
#[path = "../../../../../../../../edgezero-core/src/outbound/tests/buffered_parity/vectors.rs"]
#[allow(
    dead_code,
    reason = "origin eligibility is checked by the independent native controller"
)]
pub(super) mod vectors;

async fn capture(
    client: &HttpClient,
    native: &dyn OutboundHttpClient,
    case: vectors::Case,
    uri: Uri,
    token: &str,
    begin: bool,
) -> Result<Value, EdgeError> {
    let time = native.time();
    let clock = time.clock();
    let entered = clock.now();
    let mut outgoing = requests::make(
        case,
        uri,
        token,
        if begin { "begin" } else { "send" },
        entered,
    )?;
    let parts = outgoing.into_parts();
    let settings = requests::settings(&parts);
    outgoing = OutboundRequest::from_parts(parts)?;
    let witness = NativeObservation::attach(&mut outgoing)?;
    let result = if begin {
        match client.begin(outgoing).await {
            Ok(pending) => pending.collect().await,
            Err(error) => Err(error),
        }
    } else {
        client.send(outgoing).await
    };
    capture::collect(
        result,
        clock,
        entered,
        settings,
        case.client_marker(),
        || witness.report(),
    )
    .await
}

pub(super) async fn run(
    client: &HttpClient,
    native: &dyn OutboundHttpClient,
    case: vectors::Case,
    uri: Uri,
    token: &str,
) -> Result<Value, EdgeError> {
    let sent = capture(client, native, case, uri.clone(), token, false).await?;
    let begun = capture(client, native, case, uri, token, true).await?;
    Ok(json!({"step":"paired-contract","schema":1,"send":sent,"begin":begun}))
}
#[path = "../../../../../../../../edgezero-core/src/outbound/tests/buffered_parity/capture.rs"]
mod capture;
