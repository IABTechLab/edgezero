//! Shared marker observation over the caller's real client and prepared request.

use edgezero_core::{EdgeError, HttpClient, OutboundRequest};
use serde_json::{Value, json};

pub async fn run(
    client: &HttpClient,
    request: OutboundRequest,
    case: &str,
) -> Result<Value, EdgeError> {
    let configuration = json!({"method":request.method().as_str(),
        "setting":format!("{:?}",request.proxy_marker_setting()),"client_default":client.proxy_marker_enabled(),
        "token":request.headers().get("x-request-token").and_then(|value|value.to_str().ok())});
    let enabled = client.effective_proxy_marker_enabled(&request);
    let response = client.send(request.max_response_bytes(0)).await?;
    let status = response.status().as_u16();
    let markers: Vec<_> = response
        .headers()
        .get_all("x-edgezero-proxy")
        .iter()
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .collect();
    response.into_bytes_bounded(0).await?;
    Ok(
        json!({"case":case,"status":status,"markers":markers,"enabled":enabled,
        "request":configuration,"qualification":false}),
    )
}
