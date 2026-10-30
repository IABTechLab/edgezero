use edgezero_core::context::RequestContext;
use edgezero_core::http::{HeaderMap, Method, Response, Uri, response_builder};
use edgezero_core::{
    Body, EdgeError, OutboundFollowFailure, OutboundRedirectAction, OutboundRedirectLimit,
    OutboundRedirectStopReason, OutboundRequest, OutboundResponse, action,
};
use serde_json::{Value, json};
use std::{cell::Cell, convert::Infallible, rc::Rc, time::Duration};

struct Probe {
    uri: Uri,
    mode: String,
    streamed: bool,
    encoded: bool,
}

fn invalid() -> EdgeError {
    EdgeError::bad_request("invalid redirect probe")
}

fn field<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, EdgeError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(invalid());
    }
    value
        .map(|value| value.to_str().map_err(|_| invalid()))
        .transpose()
}

fn flag(headers: &HeaderMap, name: &str) -> Result<bool, EdgeError> {
    match field(headers, name)? {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(invalid()),
    }
}

impl Probe {
    fn parse(headers: &HeaderMap) -> Result<Self, EdgeError> {
        let target = field(headers, "x-origin")?.ok_or_else(invalid)?;
        if target.len() > 2048 || target.contains('#') {
            return Err(invalid());
        }
        let uri: Uri = target.parse().map_err(|_| invalid())?;
        if !crate::is_loopback_http(&uri)
            || !uri.port_u16().is_some_and(|port| port != 0)
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || !uri.path().starts_with("/p5/")
            || uri.query().is_some()
        {
            return Err(invalid());
        }
        let mode = field(headers, "x-p5-mode")?.ok_or_else(invalid)?;
        if !matches!(mode, "send" | "follow" | "stop" | "limit") {
            return Err(invalid());
        }
        Ok(Self {
            uri,
            mode: mode.to_owned(),
            streamed: flag(headers, "x-p5-stream")?,
            encoded: flag(headers, "x-p5-encoded-cap")?,
        })
    }

    fn request(&self) -> Result<OutboundRequest, EdgeError> {
        let mut request = OutboundRequest::new(Method::GET, self.uri.clone())?
            .timeout(Duration::from_secs(3))
            .max_response_bytes(10);
        if self.streamed {
            request = request.stream_response().max_decoded_response_bytes(10);
        }
        if self.encoded {
            request = request.max_encoded_response_bytes(10);
        }
        Ok(request)
    }
}

fn error_report(error: &EdgeError) -> Value {
    let reason = match error {
        EdgeError::BadGateway { reason, .. } => format!("{reason:?}"),
        EdgeError::ResponseTooLarge { reason, .. } => format!("{reason:?}"),
        EdgeError::GatewayTimeout { cause, .. } => format!("{cause:?}"),
        _ => String::new(),
    };
    json!({"error":{
        "kind":error.kind(),"status":error.status().as_u16(),"reason":reason,
        "provider":error.provider_failure_detail().map(|detail|format!("{detail:?}")),
    }})
}

async fn response_report(
    response: OutboundResponse,
    final_uri: Uri,
    redirects: usize,
    stop: Option<OutboundRedirectStopReason>,
    policies: usize,
) -> Value {
    let status = response.status().as_u16();
    let streamed = response.body().is_stream();
    let mut headers = response
        .headers()
        .iter()
        .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    headers.sort_unstable();
    // This report bound exceeds the request's tested buffered and streamed limits.
    let outcome = match response.into_bytes_bounded(1024).await {
        Ok(bytes) => json!({"status":status,"headers":headers,"bytes":bytes.to_vec()}),
        Err(error) => error_report(&error),
    };
    json!({"outcome":outcome,"hop":redirects,"final_uri":final_uri.to_string(),
        "redirects":redirects,"stop":stop.map(|reason|format!("{reason:?}")),
        "streamed":streamed,"policies":policies})
}

#[action]
pub(super) async fn probe(ctx: RequestContext) -> Result<Response, EdgeError> {
    let probe = Probe::parse(ctx.headers())?;
    let client = ctx.http_client().ok_or_else(|| {
        EdgeError::internal(std::io::Error::other(
            "redirect probe has no injected client",
        ))
    })?;
    let request = probe.request()?;
    let policies = Rc::new(Cell::new(0_usize));
    let exchange = if probe.mode == "send" {
        client
            .send(request)
            .await
            .map(|response| (response, probe.uri, 0, None))
            .map_err(|error| (0, error))
    } else {
        let observed = Rc::clone(&policies);
        let stop = probe.mode == "stop";
        let limit = OutboundRedirectLimit::new(if probe.mode == "limit" { 1 } else { 4 })?;
        client
            .follow(request, limit, move |_| {
                observed.set(observed.get().saturating_add(1));
                Ok::<_, Infallible>(if stop {
                    OutboundRedirectAction::Stop
                } else {
                    OutboundRedirectAction::FollowPrepared
                })
            })
            .await
            .map(edgezero_core::OutboundFollowResult::into_parts)
            .map_err(|failure| match failure {
                OutboundFollowFailure::Hop { index, error } => (index, error),
                OutboundFollowFailure::Policy { error, .. } => match error {},
            })
    };
    let report = match exchange {
        Ok((response, uri, redirects, stop)) => {
            response_report(response, uri, redirects, stop, policies.get()).await
        }
        Err((hop, error)) => {
            json!({"outcome":error_report(&error),"hop":hop,"policies":policies.get()})
        }
    };
    response_builder()
        .header("content-type", "application/json")
        .body(Body::json(&report).map_err(EdgeError::internal)?)
        .map_err(EdgeError::internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbound_test_support::TestTime;
    use bytes::Bytes;
    use edgezero_core::http::{HeaderValue, StatusCode, request_builder};
    use edgezero_core::{ContentTypeProvenance, ResponseMode};
    use futures::{StreamExt, executor::block_on};
    use std::{cell::Cell, rc::Rc, time::Duration};

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-origin", "http://127.0.0.1:1234/p5/final"),
            ("x-p5-mode", "follow"),
            ("x-p5-stream", "false"),
            ("x-p5-encoded-cap", "false"),
        ] {
            headers.insert(name, value.parse().unwrap());
        }
        headers
    }

    fn response(headers: HeaderMap, body: Body) -> OutboundResponse {
        OutboundResponse::new(
            Method::GET,
            StatusCode::OK,
            headers,
            body,
            TestTime::default().clock(),
            ContentTypeProvenance::Unknown,
        )
    }

    #[test]
    fn redirect_probe_settings_keep_three_second_and_ten_byte_limits() {
        for mode in ["send", "follow", "stop", "limit"] {
            for streamed in [false, true] {
                for encoded in [false, true] {
                    let mut headers = headers();
                    headers.insert("x-p5-mode", mode.parse().unwrap());
                    headers.insert("x-p5-stream", streamed.to_string().parse().unwrap());
                    headers.insert("x-p5-encoded-cap", encoded.to_string().parse().unwrap());
                    let probe = Probe::parse(&headers).unwrap();
                    assert_eq!(probe.mode, mode);
                    assert_eq!(probe.streamed, streamed);
                    assert_eq!(probe.encoded, encoded);
                    let parts = probe.request().unwrap().into_parts();
                    assert_eq!(parts.method, Method::GET);
                    assert_eq!(parts.timeout, Some(Duration::from_secs(3)));
                    assert_eq!(parts.max_encoded_response_bytes, encoded.then_some(10));
                    if streamed {
                        assert!(matches!(parts.response_mode, ResponseMode::Streamed));
                        assert_eq!(parts.max_decoded_response_bytes, Some(10));
                    } else {
                        assert!(matches!(
                            parts.response_mode,
                            ResponseMode::Buffered { max_bytes: 10 }
                        ));
                    }
                }
            }
        }
    }

    #[test]
    fn redirect_probe_rejects_uncontrolled_or_ambiguous_input() {
        for target in [
            "https://127.0.0.1:1234/p5/final",
            "http://example.com:1234/p5/final",
            "http://user@127.0.0.1:1234/p5/final",
            "http://127.0.0.1/p5/final",
            "http://127.0.0.1:0/p5/final",
            "http://127.0.0.1:1234/other",
            "http://127.0.0.1:1234/p5/final?extra=true",
            "http://127.0.0.1:1234/p5/final#fragment",
        ] {
            let mut headers = headers();
            headers.insert("x-origin", target.parse().unwrap());
            assert!(Probe::parse(&headers).is_err(), "{target}");
        }
        for name in ["x-origin", "x-p5-mode", "x-p5-stream", "x-p5-encoded-cap"] {
            let mut headers = headers();
            headers.append(name, headers[name].clone());
            assert!(Probe::parse(&headers).is_err(), "{name}");
        }
        for (name, value) in [
            ("x-p5-mode", "other"),
            ("x-p5-stream", "TRUE"),
            ("x-p5-encoded-cap", "1"),
        ] {
            let mut headers = headers();
            headers.insert(name, value.parse().unwrap());
            assert!(Probe::parse(&headers).is_err());
        }
        let mut oversized = headers();
        oversized.insert(
            "x-origin",
            format!("http://127.0.0.1:1/p5/{}", "a".repeat(2048))
                .parse()
                .unwrap(),
        );
        assert!(Probe::parse(&oversized).is_err());
    }

    #[test]
    fn redirect_report_preserves_raw_bytes_metadata_and_explicit_null_stop() {
        let mut headers = HeaderMap::new();
        headers.append("x-value", HeaderValue::from_bytes(&[255, 128]).unwrap());
        headers.append("x-value", HeaderValue::from_static("second"));
        let report = block_on(response_report(
            response(headers, Body::from(vec![0, 255, 128])),
            Uri::from_static("http://127.0.0.1:1234/p5/final"),
            2,
            None,
            2,
        ));
        assert_eq!(report["outcome"]["bytes"], json!([0, 255, 128]));
        assert_eq!(
            report["outcome"]["headers"],
            json!([["x-value", b"second"], ["x-value", [255, 128]]])
        );
        assert_eq!(report["hop"], 2);
        assert_eq!(report["policies"], 2);
        assert_eq!(report["redirects"], 2);
        assert_eq!(report["final_uri"], "http://127.0.0.1:1234/p5/final");
        assert_eq!(report.get("stop"), Some(&Value::Null));
        assert_eq!(report["streamed"], false);
    }

    #[test]
    fn redirect_report_bounds_stream_consumption_and_keeps_final_metadata() {
        let polls = Rc::new(Cell::new(0));
        let counted = Rc::clone(&polls);
        let source = futures::stream::repeat(Bytes::from_static(&[b'x'; 512])).map(move |chunk| {
            counted.set(counted.get() + 1);
            Ok(chunk)
        });
        let report = block_on(response_report(
            response(HeaderMap::new(), Body::from_stream(source)),
            Uri::from_static("http://127.0.0.1:1234/p5/final"),
            1,
            Some(OutboundRedirectStopReason::LimitReached),
            1,
        ));
        assert_eq!(report["outcome"]["error"]["kind"], "response_too_large");
        assert!(report["outcome"].get("bytes").is_none());
        assert_eq!(report["stop"], "LimitReached");
        assert_eq!(report["hop"], 1);
        assert_eq!(report["streamed"], true);
        assert!(polls.get() <= 3, "unbounded report consumption");
    }

    #[test]
    fn redirect_report_does_not_echo_error_messages() {
        let report = error_report(&EdgeError::bad_request("private input"));
        assert_eq!(
            report,
            json!({"error":{"kind":"bad_request","status":400,"reason":"","provider":null}})
        );
    }

    #[test]
    fn redirect_probe_requires_the_context_client() {
        let router = edgezero_core::router::RouterService::builder()
            .get("/progress/redirect", probe)
            .build();
        let mut request = request_builder()
            .uri("/progress/redirect")
            .body(Body::empty())
            .unwrap();
        *request.headers_mut() = headers();
        let response = block_on(router.oneshot(request)).unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
