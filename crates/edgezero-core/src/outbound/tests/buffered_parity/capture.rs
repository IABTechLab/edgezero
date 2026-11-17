use super::core_api as api;
use api::{EdgeError, MonotonicClock, MonotonicInstant, OutboundResponse};
use futures::StreamExt as _;
use serde_json::{Value, json};

pub fn error_value(error: &EdgeError) -> Value {
    let reason = if let EdgeError::ResponseTooLarge { reason, .. } = error {
        Some(format!("{reason:?}"))
    } else if let EdgeError::BadGateway { reason, .. } = error {
        Some(format!("{reason:?}"))
    } else {
        None
    };
    let cause = if let EdgeError::GatewayTimeout { cause, .. } = error {
        Some(format!("{cause:?}"))
    } else {
        None
    };
    let rejection = if error.kind() == "bad_request" {
        match error.message().as_str() {
            "GET/HEAD request must not carry a body" => Some("get_body"),
            "Spin cannot separate the outbound Host authority from the connection target" => {
                Some("unsupported_authority")
            }
            _ => None,
        }
    } else {
        None
    };
    json!({"kind":error.kind(),"status":error.status().as_u16(),"reason":reason,"cause":cause,
    "rejection":rejection,"upload_origin":error.request_body_overflow_origin().map(|origin|format!("{origin:?}")),
    "provider":error.provider_failure_detail().map(|detail|{
        let cause=if let api::ProviderFailureDetail::Axum{cause,..}=detail {
            Some(format!("{cause:?}"))
        } else if let api::ProviderFailureDetail::Cloudflare{cause,..}=detail {
            Some(format!("{cause:?}"))
        } else if let api::ProviderFailureDetail::Fastly{cause,..}=detail {
            Some(format!("{cause:?}"))
        } else if let api::ProviderFailureDetail::Spin{cause,..}=detail {
            Some(format!("{cause:?}"))
        } else {
            None
        };
        json!({"adapter":format!("{:?}",detail.adapter()),"phase":format!("{:?}",detail.phase()),"cause":cause})
    })})
}

pub async fn collect<Observe>(
    result: Result<OutboundResponse, EdgeError>,
    clock: &MonotonicClock,
    entered: MonotonicInstant,
    settings: Value,
    client_marker: bool,
    observe: Observe,
) -> Result<Value, EdgeError>
where
    Observe: Fn() -> Value,
{
    let head_us = clock.now().saturating_duration_since(entered).as_micros();
    let response = match result {
        Ok(response) => response,
        Err(failure) => {
            return Ok(
                json!({"outcome":{"error":error_value(&failure)},"request":settings,"client_marker":client_marker,
            "head":null,"head_us":null,"terminal_us":head_us,"reads_at_head":null,"streamed":null,"stage":"operation","chunks":[],"native":observe()}),
            );
        }
    };
    let status = response.status().as_u16();
    let streamed = response.body().is_stream();
    let reads_at_head = observe()["reads"].clone();
    let mut headers = response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    headers.sort_by(|left, right| left.0.cmp(&right.0));
    let head = json!({"status":status,"headers":headers});
    let mut bytes = Vec::new();
    let mut chunks = Vec::new();
    let mut failure = None;
    match response.into_body().into_stream() {
        Ok(mut source) => {
            while let Some(next) = source.next().await {
                match next {
                    Ok(chunk) => {
                        if bytes.len().saturating_add(chunk.len()) > 512 {
                            return Err(EdgeError::bad_request("paired fixture report overflow"));
                        }
                        bytes.extend_from_slice(&chunk);
                        chunks.push(chunk.to_vec());
                    }
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
        }
        Err(body) => bytes = body.into_bytes_bounded(512).await?.to_vec(),
    }
    let outcome = match &failure {
        Some(failure) => json!({"error":error_value(failure)}),
        None => json!({"status":status,"headers":headers,"bytes":bytes}),
    };
    Ok(
        json!({"outcome":outcome,"request":settings,"client_marker":client_marker,"head":head,
        "head_us":head_us,"terminal_us":clock.now().saturating_duration_since(entered).as_micros(),
        "reads_at_head":reads_at_head,"streamed":streamed,"stage":if failure.is_some() {"body"} else {"complete"},"chunks":chunks,"native":observe()}),
    )
}
