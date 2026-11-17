mod expanded;

use super::*;
use serde_json::{Value, json};

const CASES: &[&str] = &[
    "default",
    "marker-off",
    "marker-on",
    "post",
    "head",
    "204",
    "304",
    "redirect",
    "final-cap",
    "decoded-cap",
    "encoded-cap",
    "gzip",
    "original",
    "opaque",
    "upload-cap",
    "zero-timeout",
    "expired",
    "get-body",
    "transport-error",
];
const GZIP: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 1, 0, 240, 205, 17, 76, 10, 0, 0, 0,
];

struct Client {
    dispatches: Arc<AtomicUsize>,
    time: TestTime,
}

impl Client {
    fn head(&self, request: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        let (parts, context) = request.into_parts();
        let case = parts.uri.path().trim_start_matches('/');
        if case == "transport-error" {
            return Err(EdgeError::bad_gateway_with_reason(
                "scripted head failure",
                BadGatewayReason::Protocol,
            ));
        }
        let marker = if case == "marker-off" {
            OutboundProxyMarker::Off
        } else {
            OutboundProxyMarker::On
        };
        let payload = if matches!(case, "post" | "upload-cap") {
            b"abc".as_slice()
        } else {
            b"".as_slice()
        };
        if parts.proxy_marker != marker || parts.body.as_bytes() != Some(payload) {
            return Err(EdgeError::internal(IoError::other(
                "parity request normalization differs",
            )));
        }
        let status = match case {
            "204" => StatusCode::NO_CONTENT,
            "304" => StatusCode::NOT_MODIFIED,
            "redirect" => StatusCode::FOUND,
            _ => StatusCode::OK,
        };
        let bytes = match case {
            "gzip" | "original" => GZIP,
            "post" => b"abc",
            _ => b"ok",
        };
        let mut fields = HeaderMap::new();
        fields.insert("x-parity", HeaderValue::from_static("retained"));
        fields.insert("connection", HeaderValue::from_static("x-private"));
        fields.insert("x-private", HeaderValue::from_static("removed"));
        if case != "204" {
            fields.insert(
                "content-length",
                HeaderValue::from_str(&bytes.len().to_string()).map_err(EdgeError::internal)?,
            );
        }
        if matches!(case, "gzip" | "original" | "opaque") {
            fields.insert(
                "content-encoding",
                HeaderValue::from_static(if case == "opaque" { "x-parity" } else { "gzip" }),
            );
        }
        if case == "redirect" {
            fields.insert("location", HeaderValue::from_static("/next"));
        }
        OutboundHead::new(
            status,
            OutboundHeaderInput::new(
                "unit-parity",
                fields,
                OutboundHeaderFidelity::AdapterVisibleFields,
            ),
            Box::pin(stream::once(ready(Ok(Bytes::copy_from_slice(bytes))))),
            context,
            no_owner(),
        )
    }
}

impl OutboundHttpClient for Client {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        let head = self.head(request);
        Ok(OutboundPending::new(
            Box::pin(async move { head?.finish().await }),
            no_owner(),
        ))
    }
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        Box::pin(ready(self.head(request)))
    }
    fn progress(&self) -> Rc<dyn super::super::OutboundProgressRuntime> {
        Rc::new(super::super::IndependentOutboundProgress)
    }
    fn start_batch_until(&self, _batch: OutboundPreparedBatch) -> OutboundBatch {
        panic!("parity must not use batch dispatch")
    }
    fn time(&self) -> OutboundTime {
        self.time.pair()
    }
    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

fn request(case: &str, time: &TestTime) -> OutboundRequest {
    let method = if case == "head" {
        Method::HEAD
    } else if matches!(case, "post" | "upload-cap") {
        Method::POST
    } else {
        Method::GET
    };
    let mut request = OutboundRequest::new(
        method,
        format!("http://example.test/{case}").parse().expect("URI"),
    )
    .expect("request");
    if matches!(case, "post" | "upload-cap") {
        request = request.body("abc");
    }
    match case {
        "get-body" => request.body("x"),
        "marker-on" => request.proxy_marker(OutboundProxyMarker::On),
        "final-cap" => request.max_response_bytes(1),
        "decoded-cap" => request.max_decoded_response_bytes(1),
        "encoded-cap" => request.max_encoded_response_bytes(1),
        "original" => request
            .response_decoding(ResponseDecoding::Original)
            .max_decoded_response_bytes(1)
            .max_decoder_bytes(0),
        "upload-cap" => request.max_request_body_bytes(1),
        "zero-timeout" => request.timeout(Duration::ZERO),
        "expired" => request.deadline(Deadline::at_instant(time.now())),
        _ => request,
    }
}

async fn outcome(result: Result<OutboundResponse, EdgeError>) -> Value {
    match result {
        Ok(response) => {
            let status = response.status().as_u16();
            let mut headers = response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
                .collect::<Vec<_>>();
            headers.sort_unstable();
            match response.into_bytes_bounded(512).await {
                Ok(bytes) => json!({"status":status,"headers":headers,"bytes":bytes.to_vec()}),
                Err(error) => error_value(&error),
            }
        }
        Err(error) => error_value(&error),
    }
}

fn error_value(error: &EdgeError) -> Value {
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
    json!({"kind":error.kind(),"status":error.status().as_u16(),
        "reason":reason,"cause":cause,
        "upload_origin":error.request_body_overflow_origin().map(|origin|format!("{origin:?}")),
        "provider":error.provider_failure_detail().map(|detail|format!("{detail:?}"))})
}

#[test]
fn buffered_begin_collect_matches_send_defaults_status_headers_bytes_limits_and_causes() {
    block_on(async {
        for case in CASES {
            let time = TestTime::new(MonotonicInstant::now());
            let dispatches = Arc::new(AtomicUsize::new(0));
            let client = HttpClient::with_client(Client {
                time: time.clone(),
                dispatches: Arc::clone(&dispatches),
            })
            .with_proxy_marker(!matches!(*case, "marker-on" | "marker-off"));
            let sent = outcome(client.send(request(case, &time)).await).await;
            let begun = match client.begin(request(case, &time)).await {
                Ok(pending) => pending.collect().await,
                Err(error) => Err(error),
            };
            let collected = outcome(begun).await;
            assert_eq!(sent, collected, "{case}");
            let error = match *case {
                "final-cap" => Some((
                    "response_too_large",
                    502_u16,
                    Some("BufferedBody"),
                    None,
                    None,
                )),
                "decoded-cap" => Some(("response_too_large", 502, Some("DecodedBody"), None, None)),
                "encoded-cap" => Some(("response_too_large", 502, Some("EncodedBody"), None, None)),
                "transport-error" => Some(("bad_gateway", 502, Some("Protocol"), None, None)),
                "upload-cap" => {
                    Some(("payload_too_large", 413, None, None, Some("OutboundUpload")))
                }
                "get-body" => Some(("bad_request", 400, None, None, None)),
                "zero-timeout" => {
                    Some(("gateway_timeout", 504, None, Some("PerCallTimeout"), None))
                }
                "expired" => Some(("gateway_timeout", 504, None, Some("RequestDeadline"), None)),
                _ => None,
            };
            if let Some((kind, status, reason, cause, upload)) = error {
                assert_eq!(
                    sent,
                    json!({"kind":kind,"status":status,"reason":reason,"cause":cause,"upload_origin":upload,"provider":null}),
                    "{case}"
                );
            } else {
                let status: u16 = match *case {
                    "204" => 204,
                    "304" => 304,
                    "redirect" => 302,
                    _ => 200,
                };
                assert_eq!(sent["status"], status, "{case}");
                let bytes = match *case {
                    "head" | "204" | "304" => b"".as_slice(),
                    "post" => b"abc",
                    "gzip" => b"aaaaaaaaaa",
                    "original" => GZIP,
                    _ => b"ok",
                };
                assert_eq!(sent["bytes"], json!(bytes), "{case}");
                let fields = sent["headers"].as_array().expect("headers");
                assert!(fields.iter().any(
                    |field| field[0] == "x-parity" && field[1] == json!(b"retained".as_slice())
                ));
                assert!(
                    !fields
                        .iter()
                        .any(|field| matches!(field[0].as_str(), Some("connection" | "x-private")))
                );
                assert_eq!(
                    fields
                        .iter()
                        .filter(|field| field[0] == PROXY_HEADER)
                        .count(),
                    usize::from(*case != "marker-off")
                );
            }
            assert_eq!(
                dispatches.load(Ordering::SeqCst),
                if matches!(
                    *case,
                    "get-body" | "upload-cap" | "zero-timeout" | "expired"
                ) {
                    0
                } else {
                    2
                },
                "{case}"
            );
        }
    });
}
