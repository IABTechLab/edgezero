//! Synthetic byte-cap probes through the injected production client, not direct SDK requests.

use bytes::Bytes;
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Response, Uri, response_builder};
use edgezero_core::outbound::{OutboundCachePolicy, OutboundRequest};
use edgezero_core::{Body, EdgeError, action};
use serde_json::json;
use std::time::Duration;

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid controlled overflow probe")
}

fn upload_request(case: &str, uri: Uri) -> Result<OutboundRequest, EdgeError> {
    let (streamed, len) = match case {
        "buffered-7" => (false, 7),
        "buffered-8" | "upstream-413" => (false, 8),
        "buffered-9" => (false, 9),
        "streamed-7" => (true, 7),
        "streamed-8" => (true, 8),
        "streamed-9" => (true, 9),
        _ => return Err(invalid()),
    };
    let body = if streamed {
        Body::stream(futures::stream::iter([
            Bytes::from_static(b"xxxx"),
            Bytes::from(vec![b'x'; len - 4]),
        ]))
    } else {
        Body::from(vec![b'x'; len])
    };
    Ok(
        OutboundRequest::new(edgezero_core::http::Method::POST, uri)?
            .body(body)
            .max_request_body_bytes(8)
            .max_response_header_bytes(4096)
            .max_response_header_count(32)
            .max_encoded_response_bytes(64)
            .max_decoded_response_bytes(64)
            .cache_policy(OutboundCachePolicy::Bypass)
            .timeout(Duration::from_secs(2))
            .stream_response(),
    )
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let case = ctx.path_params().get("case").ok_or_else(invalid)?;
    if case == "inbound" {
        let received = ctx.body_bytes(1024).await?;
        return response_builder()
            .header("content-type", "application/json")
            .body(
                Body::json(&json!({"case":case,"received":received.len(),"qualification":false}))
                    .map_err(EdgeError::internal)?,
            )
            .map_err(EdgeError::internal);
    }
    let token = ctx
        .headers()
        .get("x-request-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .ok_or_else(invalid)?;
    let uri: Uri = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if !crate::is_loopback_http(&uri)
        || uri.port_u16().is_none()
        || uri.query().is_some()
        || uri.path() != format!("/overflow/{token}/{case}")
    {
        return Err(invalid());
    }
    let request = upload_request(case, uri)?;
    let client = ctx.http_client().ok_or_else(|| {
        EdgeError::internal(std::io::Error::other(
            "overflow probe has no injected client",
        ))
    })?;
    let response = client.send(request).await?;
    let report = json!({"case":case,"outcome":"response","upstream_status":response.status().as_u16(),
        "retry_after":response.headers().get("retry-after").and_then(|value|value.to_str().ok()),"qualification":false});
    // This proves upload/head handling only. Never enter Fastly's failed G2 body-read path.
    drop(response);
    response_builder()
        .header("content-type", "application/json")
        .body(Body::json(&report).map_err(EdgeError::internal)?)
        .map_err(EdgeError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_shapes_are_bounded_and_use_streamed_response_heads() {
        for mode in ["buffered", "streamed"] {
            for len in [7_usize, 8, 9] {
                let request = upload_request(
                    &format!("{mode}-{len}"),
                    Uri::from_static("http://127.0.0.1:1234/"),
                )
                .unwrap()
                .into_parts();
                assert_eq!(request.max_request_body_bytes, 8);
                assert!(matches!(
                    request.response_mode,
                    edgezero_core::ResponseMode::Streamed
                ));
                assert_eq!(request.body.is_stream(), mode == "streamed");
                let bytes =
                    futures::executor::block_on(request.body.into_bytes_bounded(16)).unwrap();
                assert_eq!(bytes.as_ref(), vec![b'x'; len]);
            }
        }
        assert!(
            upload_request(
                "streamed-1000000",
                Uri::from_static("http://127.0.0.1:1234/")
            )
            .is_err()
        );
    }
}
