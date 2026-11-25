use axum::body::Body as AxumBody;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Response, StatusCode};
use futures::executor::block_on;
use futures_util::{StreamExt as _, pin_mut};
use tracing::error;

use edgezero_core::body::Body;
use edgezero_core::http::Response as CoreResponse;

/// Convert an `EdgeZero` response into one consumable by Axum/Hyper.
///
/// Streaming responses are collected into an in-memory buffer. While this sacrifices
/// incremental flushing, it keeps the adapter compatible with the non-`Send` streaming type used by
/// `edgezero_core::Body` and works well for local development.
///
#[inline]
pub fn into_axum_response(response: CoreResponse) -> Response<AxumBody> {
    let (parts, core_body) = response.into_parts();
    let body = match core_body {
        Body::Once(bytes) => AxumBody::from(bytes),
        Body::Stream(stream) => {
            let result = block_on(async {
                let mut buf = Vec::new();
                pin_mut!(stream);
                while let Some(chunk) = stream.next().await {
                    let bytes = chunk?;
                    buf.extend_from_slice(&bytes);
                }
                Ok::<Vec<u8>, anyhow::Error>(buf)
            });
            match result {
                Ok(buf) => AxumBody::from(buf),
                Err(err) => {
                    error!("streaming response error: {err}");
                    return error_response_500("streaming response error");
                }
            }
        }
    };

    Response::from_parts(parts, body)
}

/// Build a minimal 500 response without any builder steps that could fail.
/// Used as a fallback on the request path so we never panic on synthesis.
fn error_response_500(message: &'static str) -> Response<AxumBody> {
    let mut response = Response::new(AxumBody::from(message));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::body::Body;
    use edgezero_core::http::{StatusCode, response_builder};
    use futures::stream;

    #[test]
    fn approved_config_diagnostics_stay_private_after_conversion() {
        use axum::body::to_bytes;
        use edgezero_core::config_diagnostics::contract_fixture::{self, CANARY, EXPLANATION};
        use edgezero_core::response::IntoResponse as _;
        let error = block_on(contract_fixture::extraction_error()).expect("extraction failure");
        assert_eq!(error.validation_diagnostics().len(), 1);
        assert!(error.operator_message().contains(EXPLANATION));
        let response = into_axum_response(error.into_response().expect("core response"));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "60");
        let bytes = block_on(to_bytes(response.into_body(), 4096)).expect("body");
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("config_out_of_date"));
        for hidden in [CANARY, EXPLANATION, "implementation_unavailable"] {
            assert!(!body.contains(hidden));
        }
    }

    #[test]
    fn converts_core_response_stream_into_axum_body() {
        let stream = stream::iter(vec![
            Ok::<_, anyhow::Error>(bytes::Bytes::from_static(b"hel")),
            Ok(bytes::Bytes::from_static(b"lo")),
        ]);
        let body = Body::from_stream(stream);
        let response = response_builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain")
            .body(body)
            .expect("response");

        let axum_response = into_axum_response(response);
        assert_eq!(axum_response.status(), StatusCode::OK);
        assert_eq!(
            axum_response
                .headers()
                .get("content-type")
                .expect("header")
                .to_str()
                .unwrap(),
            "text/plain"
        );

        let collected = block_on(async {
            let mut data = Vec::new();
            let mut body_stream = axum_response.into_body().into_data_stream();
            while let Some(result) = body_stream.next().await {
                let chunk = result.expect("chunk");
                data.extend_from_slice(&chunk);
            }
            data
        });

        assert_eq!(collected, b"hello");
    }
}
