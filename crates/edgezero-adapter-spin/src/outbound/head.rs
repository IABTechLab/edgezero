use edgezero_core::EdgeError;
use edgezero_core::body::BodyStream;
use edgezero_core::http::{HeaderMap, StatusCode};
use edgezero_core::outbound::{
    OutboundExchangeOwner, OutboundHead, OutboundHeaderFidelity, OutboundHeaderInput,
    OutboundResponseContext,
};

pub(super) fn response_head(
    status: StatusCode,
    fields: HeaderMap,
    encoded: BodyStream,
    context: OutboundResponseContext,
) -> Result<OutboundHead, EdgeError> {
    OutboundHead::new(
        status,
        OutboundHeaderInput::new("spin", fields, OutboundHeaderFidelity::AdapterVisibleFields),
        encoded,
        context,
        OutboundExchangeOwner::new(|| {}),
    )
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use bytes::Bytes;
    use edgezero_core::body::BodyStream;
    use edgezero_core::compression::{
        ContentEncoding, PassthroughReason, classify_content_encoding,
    };
    use edgezero_core::http::{HeaderMap, HeaderValue, Method, StatusCode};
    use edgezero_core::outbound::{
        HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundDispatchRequest,
        OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
        OutboundProgressRuntime, OutboundProxyMarker, OutboundRequest, OutboundSendMode,
        OutboundTime, ResponseDecoding, ResponseMode, validate_for_dispatch,
    };
    use edgezero_core::{BadGatewayReason, EdgeError, MonotonicClock, ResponseLimitReason};
    use futures::executor::block_on;
    use futures_util::{StreamExt as _, future::pending, stream::poll_fn};

    const GZIP: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0, 0,
    ];

    struct FixtureClient {
        fields: HeaderMap,
        payload: Bytes,
        polls: Arc<AtomicUsize>,
        releases: Arc<AtomicUsize>,
        status: StatusCode,
    }

    struct SourceLease(Arc<AtomicUsize>);

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for SourceLease {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl OutboundHttpClient for FixtureClient {
        fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            Err(EdgeError::internal(anyhow::anyhow!(
                "normalization fixture exercises Send only"
            )))
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            let mut payload = Some(self.payload.clone());
            let polls = Arc::clone(&self.polls);
            let lease = SourceLease(Arc::clone(&self.releases));
            let encoded: BodyStream = poll_fn(move |_context| {
                let _lease = &lease;
                polls.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(payload.take().map(Ok))
            })
            .boxed_local();
            let result = super::response_head(
                self.status,
                self.fields.clone(),
                encoded,
                request.into_parts().1,
            );
            Box::pin(async move { result })
        }

        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn start_batch_until(&self, _batch: OutboundPreparedBatch) -> OutboundBatch {
            panic!("normalization fixture exercises Send only");
        }

        fn time(&self) -> OutboundTime {
            OutboundTime::new(MonotonicClock::default(), |_deadline, _clock| {
                Box::pin(pending())
            })
        }

        fn validate_supported_options(
            &self,
            request: &OutboundRequest,
            mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            validate_for_dispatch(request, mode)
        }
    }

    fn representation_fields() -> HeaderMap {
        let mut fields = HeaderMap::new();
        for (name, value) in [
            ("content-encoding", "gzip"),
            ("content-length", "23"),
            ("content-range", "bytes 0-22/46"),
            ("etag", "\"strong\""),
            ("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ("vary", "Accept-Encoding"),
            ("accept-ranges", "bytes"),
            ("cache-control", "public, max-age=60"),
            (
                "content-digest",
                "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
            ),
        ] {
            fields.insert(name, HeaderValue::from_static(value));
        }
        fields
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "literal normalization cases retain input and assertions together"
    )]
    fn adapter_visible_normalization_preserves_policy_precedence() {
        for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 256 },
                ResponseMode::Streamed,
            ] {
                for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
                    for case in [
                        "nominated-over",
                        "nominated-exact",
                        "missing-over",
                        "etag",
                        "opaque",
                        "malformed",
                        "representation",
                    ] {
                        let mut fields = HeaderMap::new();
                        let mut payload = Bytes::from_static(b"ab");
                        let mut decoded_cap = 1;
                        match case {
                            "nominated-over" | "nominated-exact" => {
                                fields.insert(
                                    "connection",
                                    HeaderValue::from_static(
                                        "content-encoding, content-length, etag",
                                    ),
                                );
                                fields.insert("content-encoding", HeaderValue::from_static("gzip"));
                                fields.insert(
                                    "content-length",
                                    HeaderValue::from_static("malformed"),
                                );
                                fields.insert("etag", HeaderValue::from_static("\"removed\""));
                                if case == "nominated-exact" {
                                    decoded_cap = 2;
                                }
                            }
                            "etag" | "opaque" => {
                                fields.insert(
                                    "etag",
                                    HeaderValue::from_bytes(&[0xff]).expect("opaque ETag"),
                                );
                                fields.insert("content-length", HeaderValue::from_static("2"));
                                if case == "opaque" {
                                    fields.insert(
                                        "content-encoding",
                                        HeaderValue::from_bytes(&[0xff]).expect("opaque coding"),
                                    );
                                } else {
                                    decoded_cap = 2;
                                }
                            }
                            "malformed" => {
                                fields.insert(
                                    "content-length",
                                    HeaderValue::from_static("malformed"),
                                );
                            }
                            "representation" => {
                                fields = representation_fields();
                                payload = Bytes::from_static(GZIP);
                                decoded_cap = 11;
                            }
                            _ => {}
                        }
                        let original = fields.clone();
                        let polls = Arc::new(AtomicUsize::new(0));
                        let releases = Arc::new(AtomicUsize::new(0));
                        let client = HttpClient::with_client(FixtureClient {
                            fields,
                            payload,
                            polls: Arc::clone(&polls),
                            releases: Arc::clone(&releases),
                            status,
                        });
                        let mut parts = OutboundRequest::get("https://example.com/")
                            .expect("request")
                            .into_parts();
                        parts.proxy_marker = OutboundProxyMarker::Off;
                        parts.response_mode = mode;
                        parts.response_decoding = decoding;
                        parts.max_decoded_response_bytes = Some(decoded_cap);
                        parts.max_encoded_response_bytes = Some(256);
                        let outcome = block_on(async {
                            let response = client
                                .send(OutboundRequest::from_parts(parts).expect("request parts"))
                                .await?;
                            assert_eq!(response.status(), status);
                            match case {
                                "nominated-exact" => {
                                    for name in
                                        ["connection", "content-encoding", "content-length", "etag"]
                                    {
                                        assert!(
                                            !response.headers().contains_key(name),
                                            "{name} remains removed"
                                        );
                                    }
                                    assert_eq!(
                                        classify_content_encoding(response.headers()),
                                        ContentEncoding::Identity
                                    );
                                }
                                "etag" | "opaque" => {
                                    assert!(!response.headers().contains_key("etag"));
                                    assert_eq!(response.headers()["content-length"], "2");
                                    assert_eq!(
                                        classify_content_encoding(response.headers()),
                                        if case == "opaque" {
                                            ContentEncoding::Passthrough(
                                                PassthroughReason::Malformed,
                                            )
                                        } else {
                                            ContentEncoding::Identity
                                        }
                                    );
                                    if case == "opaque" {
                                        assert_eq!(
                                            response.headers()["content-encoding"].as_bytes(),
                                            &[0xff]
                                        );
                                    }
                                }
                                "representation" => {
                                    if decoding == ResponseDecoding::Original
                                        || status == StatusCode::PARTIAL_CONTENT
                                    {
                                        assert_eq!(response.headers(), &original);
                                    } else {
                                        assert!(
                                            !response.headers().contains_key("content-encoding")
                                        );
                                        assert!(!response.headers().contains_key("content-length"));
                                    }
                                }
                                _ => {}
                            }
                            response.into_bytes_bounded(256).await
                        });
                        if case == "malformed" {
                            assert!(
                                matches!(
                                    outcome,
                                    Err(EdgeError::BadGateway {
                                        reason: BadGatewayReason::Protocol,
                                        ..
                                    })
                                ),
                                "{outcome:?}"
                            );
                            assert_eq!(polls.load(Ordering::SeqCst), 0);
                        } else if matches!(case, "nominated-over" | "missing-over") {
                            assert!(
                                matches!(
                                    outcome,
                                    Err(EdgeError::ResponseTooLarge {
                                        reason: ResponseLimitReason::DecodedBody,
                                        ..
                                    })
                                ),
                                "{outcome:?}"
                            );
                            assert!(polls.load(Ordering::SeqCst) > 0);
                        } else {
                            let expected: &[u8] = if case == "representation" {
                                if decoding == ResponseDecoding::Original
                                    || status == StatusCode::PARTIAL_CONTENT
                                {
                                    GZIP
                                } else {
                                    b"aaaaaaaaaaa"
                                }
                            } else {
                                b"ab"
                            };
                            assert_eq!(outcome.expect(case).as_ref(), expected);
                        }
                        assert_eq!(
                            releases.load(Ordering::SeqCst),
                            1,
                            "exactly one source release: {case}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn adapter_visible_bodyless_and_reset_dispositions_precede_caps() {
        for decoding in [ResponseDecoding::Decode, ResponseDecoding::Original] {
            for mode in [
                ResponseMode::Buffered { max_bytes: 0 },
                ResponseMode::Streamed,
            ] {
                for (method, status) in [
                    (Method::HEAD, StatusCode::OK),
                    (Method::GET, StatusCode::NOT_MODIFIED),
                    (Method::GET, StatusCode::NO_CONTENT),
                    (Method::GET, StatusCode::RESET_CONTENT),
                ] {
                    for length in [None, Some("0"), Some("1073741824")] {
                        let polls = Arc::new(AtomicUsize::new(0));
                        let releases = Arc::new(AtomicUsize::new(0));
                        let mut fields = HeaderMap::new();
                        fields.insert("content-encoding", HeaderValue::from_static("gzip"));
                        if let Some(value) = length {
                            fields.insert("content-length", HeaderValue::from_static(value));
                        }
                        let client = HttpClient::with_client(FixtureClient {
                            fields,
                            payload: Bytes::from_static(b"not gzip"),
                            polls: Arc::clone(&polls),
                            releases: Arc::clone(&releases),
                            status,
                        });
                        let mut parts = OutboundRequest::new(
                            method.clone(),
                            "https://example.com/".parse().expect("URI"),
                        )
                        .expect("request")
                        .into_parts();
                        parts.proxy_marker = OutboundProxyMarker::Off;
                        parts.response_decoding = decoding;
                        parts.response_mode = mode;
                        parts.max_encoded_response_bytes = Some(0);
                        parts.max_decoded_response_bytes = Some(0);
                        parts.max_decoder_bytes = 0;
                        let response = block_on(
                            client.send(OutboundRequest::from_parts(parts).expect("request parts")),
                        )
                        .expect("bodyless response");
                        assert_eq!(response.headers()["content-encoding"], "gzip");
                        let expected = if status == StatusCode::NO_CONTENT {
                            None
                        } else if status == StatusCode::RESET_CONTENT {
                            Some("0")
                        } else {
                            length
                        };
                        assert_eq!(
                            response
                                .headers()
                                .get("content-length")
                                .and_then(|value| value.to_str().ok()),
                            expected
                        );
                        let reads = usize::from(
                            status == StatusCode::RESET_CONTENT && length != Some("1073741824"),
                        );
                        assert_eq!(polls.load(Ordering::SeqCst), reads);
                        assert_eq!(releases.load(Ordering::SeqCst), 1);
                        assert!(
                            block_on(response.into_bytes_bounded(0))
                                .expect("empty body")
                                .is_empty()
                        );
                        assert_eq!(
                            polls.load(Ordering::SeqCst),
                            reads,
                            "collection cannot poll a disposed source"
                        );
                        assert_eq!(releases.load(Ordering::SeqCst), 1);
                    }
                }
            }
        }
    }
}
