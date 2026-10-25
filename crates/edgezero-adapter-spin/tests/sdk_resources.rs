#![cfg(all(feature = "spin", feature = "test-utils", target_arch = "wasm32"))]

#[cfg(test)]
#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "SDK construction checks stay before the exhaustive classifier fixture"
)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use edgezero_adapter_spin::outbound::{
        SpinOutboundClient, deferred_clock_paths_hold_for_test, map_spin_host_error_for_test,
        map_spin_preparation_header_for_test, provider_deadline_diagnostics_hold_for_test,
        request_fields_for_test, request_target_errors_for_test, set_timeout_option_for_test,
    };
    use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
    use edgezero_core::error_detail::{
        ProviderFailureAdapter, ProviderFailureDetail, ProviderFailurePhase, SpinDnsRcode,
        SpinFailureCause, SpinFieldSize, SpinTimeoutOption,
    };
    use edgezero_core::http::{HeaderMap, HeaderValue};
    use edgezero_core::outbound::{OutboundHttpClient, OutboundRequest};
    use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
    use futures::executor::block_on;
    use spin_sdk::wasip3::http::types::{
        DnsErrorPayload, ErrorCode, FieldSizePayload, Fields, HeaderError, Request, RequestOptions,
        RequestOptionsError, Response, TlsAlertReceivedPayload,
    };
    use spin_sdk::wasip3::http_compat::BodyWriter;

    fn assert_outbound_client<Client>()
    where
        Client: OutboundHttpClient,
    {
    }

    #[test]
    fn spin_outbound_client_implements_portable_contract() {
        assert_outbound_client::<SpinOutboundClient>();
    }

    #[test]
    fn response_conversion_checks_deadline_before_handoff() {
        assert!(
            block_on(deferred_clock_paths_hold_for_test()),
            "response handoff must retain deadlines and release SDK body consumers"
        );
    }

    #[test]
    fn injected_clock_controls_method_entry_and_preflight_elapsed() {
        let start = MonotonicInstant::now();
        let completed = start
            .checked_add(Duration::from_millis(7))
            .expect("completed instant");
        let observations = Arc::new(Mutex::new(VecDeque::from([start, completed])));
        let clock_observations = Arc::clone(&observations);
        let client = SpinOutboundClient::with_clock(MonotonicClock::new(move || {
            clock_observations
                .lock()
                .expect("clock observations")
                .pop_front()
                .expect("clock observation")
        }));
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();

        let results = block_on(
            client
                .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
                .collect(),
        )
        .expect("valid batch driver");

        let slot = results.slots[0].as_ref().expect("resolved slot");
        assert_eq!(slot.elapsed, Duration::from_millis(7));
        assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
    }

    #[test]
    fn spin_unsupported_authority_precedes_generic_validation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;

        use edgezero_core::http::{Method, Uri};
        use edgezero_core::{Body, OutboundBatchTermination};
        use futures_util::stream::poll_fn;

        let start = MonotonicInstant::now();
        let client = SpinOutboundClient::with_clock(MonotonicClock::new(move || start));
        let observation_cutoff = Deadline::at_instant(
            start
                .checked_add(Duration::from_secs(1))
                .expect("observation cutoff"),
        );
        for method in [Method::GET, Method::HEAD, Method::CONNECT, Method::POST] {
            for batch in [false, true] {
                let polls = Arc::new(AtomicUsize::new(0));
                let source_polls = Arc::clone(&polls);
                let owner = Arc::new(());
                let released = Arc::downgrade(&owner);
                let source = poll_fn(move |_cx| {
                    let _keep_alive = Arc::clone(&owner);
                    source_polls.fetch_add(1, Ordering::SeqCst);
                    Poll::<Option<Result<bytes::Bytes, EdgeError>>>::Pending
                });
                let request =
                    OutboundRequest::new(method.clone(), Uri::from_static("https://example.com/"))
                        .expect("request")
                        .host_authority_override("virtual.example")
                        .expect("authority")
                        .body(Body::from_stream(source))
                        .max_brotli_window_bits(9)
                        .timeout(Duration::ZERO);
                assert!(released.upgrade().is_some());
                let error = if batch {
                    let results = block_on(
                        client
                            .start_batch_until(vec![request], observation_cutoff)
                            .collect(),
                    )
                    .expect("batch");
                    assert_eq!(results.termination, OutboundBatchTermination::Completed);
                    results
                        .slots
                        .into_iter()
                        .next()
                        .flatten()
                        .expect("slot")
                        .outcome
                        .expect_err("unsupported authority")
                } else {
                    block_on(client.send(request)).expect_err("unsupported authority")
                };
                assert!(matches!(error, EdgeError::BadRequest { .. }));
                assert_eq!(
                    error.message(),
                    "Spin cannot separate the outbound Host authority from the connection target"
                );
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert!(released.upgrade().is_none(), "rejected source released");
            }
        }
    }

    #[test]
    fn sdk_fields_preserve_duplicate_values() {
        let fields = Fields::new();
        fields
            .append("set-cookie", b"first=1")
            .expect("first field append");
        fields
            .append("set-cookie", b"second=2")
            .expect("second field append");

        assert_eq!(
            fields.get("set-cookie"),
            vec![b"first=1".to_vec(), b"second=2".to_vec()]
        );
    }

    #[test]
    fn sdk_request_options_execute_all_setters() {
        let options = RequestOptions::new();
        options
            .set_connect_timeout(Some(1_000_000))
            .expect("connect timeout is supported");
        options
            .set_first_byte_timeout(Some(2_000_000))
            .expect("first-byte timeout is supported");
        options
            .set_between_bytes_timeout(Some(3_000_000))
            .expect("between-bytes timeout is supported");
    }

    #[test]
    fn provider_diagnostics_survive_ready_deadlines() {
        assert!(block_on(provider_deadline_diagnostics_hold_for_test()));
    }

    #[test]
    fn provider_diagnostics_preserve_actual_preparation_header_rejection() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", HeaderValue::from_static("close"));
        let error = request_fields_for_test(&headers).expect_err("host forbids connection");
        assert_eq!(
            error.provider_failure_detail(),
            Some(&ProviderFailureDetail::Spin {
                phase: ProviderFailurePhase::RequestPreparation,
                cause: SpinFailureCause::HeaderForbidden,
            })
        );
        assert_preparation_wire_unchanged(
            EdgeError::bad_request("Spin rejected an outbound request header"),
            error,
        );
        headers.clear();
        headers.insert("x-test", HeaderValue::from_static("valid"));
        request_fields_for_test(&headers).expect("accepted field");
    }

    #[test]
    fn provider_diagnostics_preserve_preparation_setter_causes_without_text() {
        for (source, cause) in [
            (HeaderError::Forbidden, SpinFailureCause::HeaderForbidden),
            (HeaderError::Immutable, SpinFailureCause::HeaderImmutable),
            (
                HeaderError::InvalidSyntax,
                SpinFailureCause::HeaderInvalidSyntax,
            ),
            (
                HeaderError::SizeExceeded,
                SpinFailureCause::HeaderSizeExceeded,
            ),
            (HeaderError::Other(None), SpinFailureCause::HeaderOther),
            (
                HeaderError::Other(Some("provider-secret-token".to_owned())),
                SpinFailureCause::HeaderOther,
            ),
        ] {
            let error = map_spin_preparation_header_for_test(&source);
            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Spin {
                    phase: ProviderFailurePhase::RequestPreparation,
                    cause,
                })
            );
            assert_preparation_wire_unchanged(
                EdgeError::bad_request("Spin rejected an outbound request header"),
                error,
            );
        }
        for (option, name) in [
            (SpinTimeoutOption::Connect, "connect"),
            (SpinTimeoutOption::FirstByte, "first-byte"),
            (SpinTimeoutOption::BetweenBytes, "between-bytes"),
        ] {
            set_timeout_option_for_test(option, &Ok(())).expect("accepted option");
            set_timeout_option_for_test(option, &Err(RequestOptionsError::NotSupported))
                .expect("unsupported option stays advisory");
            for (source, cause, message) in [
                (
                    RequestOptionsError::Immutable,
                    SpinFailureCause::RequestOptionsImmutable(option),
                    format!("Spin outbound {name} timeout option is immutable"),
                ),
                (
                    RequestOptionsError::Other(Some("provider-secret-token".to_owned())),
                    SpinFailureCause::RequestOptionsOther(option),
                    format!("Spin host rejected the outbound {name} timeout option"),
                ),
                (
                    RequestOptionsError::Other(None),
                    SpinFailureCause::RequestOptionsOther(option),
                    format!("Spin host rejected the outbound {name} timeout option"),
                ),
            ] {
                let error =
                    set_timeout_option_for_test(option, &Err(source)).expect_err("rejection");
                assert_eq!(
                    error.provider_failure_detail(),
                    Some(&ProviderFailureDetail::Spin {
                        phase: ProviderFailurePhase::RequestPreparation,
                        cause,
                    })
                );
                assert_preparation_wire_unchanged(
                    EdgeError::internal(anyhow::anyhow!(message)),
                    error,
                );
            }
        }
    }

    fn assert_preparation_wire_unchanged(plain: EdgeError, diagnosed: EdgeError) {
        use edgezero_core::response::IntoResponse as _;

        assert_eq!(plain.to_string(), diagnosed.to_string());
        assert_eq!(plain.message(), diagnosed.message());
        assert_eq!(plain.kind(), diagnosed.kind());
        assert_eq!(plain.status(), diagnosed.status());
        assert!(
            !format!("{:?}", diagnosed.provider_failure_detail()).contains("provider-secret-token")
        );
        let before = plain.into_response().expect("plain response");
        let after = diagnosed.into_response().expect("diagnosed response");
        assert_eq!(before.status(), after.status());
        assert_eq!(before.headers(), after.headers());
        assert_eq!(before.body().as_bytes(), after.body().as_bytes());
    }

    #[test]
    fn provider_diagnostics_preserve_unit_target_rejection_without_fabricating_error_codes() {
        let expected = [
            (SpinFailureCause::RequestAuthorityRejected, "authority"),
            (SpinFailureCause::RequestMethodRejected, "method"),
            (SpinFailureCause::RequestPathRejected, "path"),
            (SpinFailureCause::RequestSchemeRejected, "scheme"),
        ];
        for (error, (cause, name)) in request_target_errors_for_test().into_iter().zip(expected) {
            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Spin {
                    phase: ProviderFailurePhase::RequestPreparation,
                    cause,
                })
            );
            assert_preparation_wire_unchanged(
                EdgeError::bad_request(format!("Spin rejected the outbound request {name}")),
                error,
            );
        }
    }

    #[test]
    fn sdk_request_and_response_completion_resources_construct() {
        let (request_writer, request_body, request_trailers) = BodyWriter::new();
        let (request, request_done) = Request::new(
            Fields::new(),
            Some(request_body),
            request_trailers,
            Some(RequestOptions::new()),
        );

        let (response_writer, response_body, response_trailers) = BodyWriter::new();
        let (response, response_done) =
            Response::new(Fields::new(), Some(response_body), response_trailers);

        drop((request, request_done, request_writer));
        drop((response, response_done, response_writer));
    }

    #[derive(Clone, Copy, Debug)]
    enum ExpectedError {
        BadRequest,
        Internal,
        Protocol,
        Timeout,
        Transport,
        Unknown,
        Unreachable,
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the pinned SDK table intentionally constructs every known ErrorCode variant"
    )]
    fn known_error_codes() -> Vec<(ErrorCode, ExpectedError)> {
        let field = FieldSizePayload {
            field_name: Some("x-test".to_owned()),
            field_size: Some(1),
        };
        vec![
            (ErrorCode::DnsTimeout, ExpectedError::Timeout),
            (
                ErrorCode::DnsError(DnsErrorPayload {
                    rcode: Some("NXDOMAIN".to_owned()),
                    info_code: Some(3),
                }),
                ExpectedError::Unreachable,
            ),
            (ErrorCode::DestinationNotFound, ExpectedError::Unreachable),
            (
                ErrorCode::DestinationUnavailable,
                ExpectedError::Unreachable,
            ),
            (
                ErrorCode::DestinationIpProhibited,
                ExpectedError::Unreachable,
            ),
            (
                ErrorCode::DestinationIpUnroutable,
                ExpectedError::Unreachable,
            ),
            (ErrorCode::ConnectionRefused, ExpectedError::Unreachable),
            (ErrorCode::ConnectionTerminated, ExpectedError::Transport),
            (ErrorCode::ConnectionTimeout, ExpectedError::Timeout),
            (ErrorCode::ConnectionReadTimeout, ExpectedError::Timeout),
            (ErrorCode::ConnectionWriteTimeout, ExpectedError::Timeout),
            (
                ErrorCode::ConnectionLimitReached,
                ExpectedError::Unreachable,
            ),
            (ErrorCode::TlsProtocolError, ExpectedError::Unreachable),
            (ErrorCode::TlsCertificateError, ExpectedError::Unreachable),
            (
                ErrorCode::TlsAlertReceived(TlsAlertReceivedPayload {
                    alert_id: Some(42),
                    alert_message: Some("fixture".to_owned()),
                }),
                ExpectedError::Unreachable,
            ),
            (ErrorCode::HttpRequestDenied, ExpectedError::BadRequest),
            (
                ErrorCode::HttpRequestLengthRequired,
                ExpectedError::Internal,
            ),
            (
                ErrorCode::HttpRequestBodySize(Some(1)),
                ExpectedError::BadRequest,
            ),
            (ErrorCode::HttpRequestMethodInvalid, ExpectedError::Internal),
            (ErrorCode::HttpRequestUriInvalid, ExpectedError::Internal),
            (ErrorCode::HttpRequestUriTooLong, ExpectedError::BadRequest),
            (
                ErrorCode::HttpRequestHeaderSectionSize(Some(1)),
                ExpectedError::BadRequest,
            ),
            (
                ErrorCode::HttpRequestHeaderSize(Some(field.clone())),
                ExpectedError::BadRequest,
            ),
            (
                ErrorCode::HttpRequestTrailerSectionSize(Some(1)),
                ExpectedError::Internal,
            ),
            (
                ErrorCode::HttpRequestTrailerSize(field.clone()),
                ExpectedError::Internal,
            ),
            (ErrorCode::HttpResponseIncomplete, ExpectedError::Protocol),
            (
                ErrorCode::HttpResponseHeaderSectionSize(Some(1)),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseHeaderSize(field.clone()),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseBodySize(Some(1)),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseTrailerSectionSize(Some(1)),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseTrailerSize(field),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseTransferCoding(Some("fixture".to_owned())),
                ExpectedError::Protocol,
            ),
            (
                ErrorCode::HttpResponseContentCoding(Some("fixture".to_owned())),
                ExpectedError::Protocol,
            ),
            (ErrorCode::HttpResponseTimeout, ExpectedError::Timeout),
            (ErrorCode::HttpUpgradeFailed, ExpectedError::Protocol),
            (ErrorCode::HttpProtocolError, ExpectedError::Protocol),
            (ErrorCode::LoopDetected, ExpectedError::Protocol),
            (ErrorCode::ConfigurationError, ExpectedError::Internal),
            (
                ErrorCode::InternalError(Some("fixture".to_owned())),
                ExpectedError::Unknown,
            ),
        ]
    }

    fn assert_expected_error(error: &EdgeError, expected: ExpectedError) {
        match expected {
            ExpectedError::BadRequest => assert!(matches!(error, EdgeError::BadRequest { .. })),
            ExpectedError::Internal => assert!(matches!(error, EdgeError::Internal { .. })),
            ExpectedError::Protocol => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Protocol,
                    ..
                }
            )),
            ExpectedError::Timeout => assert!(matches!(
                error,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::Unspecified,
                    ..
                }
            )),
            ExpectedError::Transport => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Transport,
                    ..
                }
            )),
            ExpectedError::Unknown => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Unspecified,
                    ..
                }
            )),
            ExpectedError::Unreachable => assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Unreachable,
                    ..
                }
            )),
        }
    }

    #[test]
    fn spin_error_code_table_is_exhaustive() {
        let now = MonotonicInstant::now();
        let live = Deadline::at_instant(
            now.checked_add(Duration::from_secs(30))
                .expect("live deadline"),
        );
        for (code, expected) in known_error_codes() {
            let mapped = map_spin_host_error_for_test(
                &code,
                live,
                BudgetSource::Default,
                now,
                ProviderFailurePhase::Send,
            );
            assert_expected_error(&mapped, expected);
            let detail = mapped
                .provider_failure_detail()
                .expect("every SDK cause retains detail");
            assert_eq!(detail.adapter(), ProviderFailureAdapter::Spin);
            assert_eq!(detail.phase(), ProviderFailurePhase::Send);

            let expired = map_spin_host_error_for_test(
                &code,
                Deadline::at_instant(now),
                BudgetSource::RequestDeadline,
                now,
                ProviderFailurePhase::Send,
            );
            assert!(matches!(
                expired,
                EdgeError::GatewayTimeout {
                    cause: BudgetSource::RequestDeadline,
                    ..
                }
            ));
        }
    }

    #[test]
    fn provider_diagnostics_preserve_spin_numbers_and_discard_provider_text() {
        let now = MonotonicInstant::now();
        let live = Deadline::at_instant(now.checked_add(Duration::from_secs(1)).expect("deadline"));
        for (source, expected) in [
            (
                ErrorCode::TlsAlertReceived(TlsAlertReceivedPayload {
                    alert_id: Some(u8::MAX),
                    alert_message: Some("provider-secret-token".to_owned()),
                }),
                SpinFailureCause::TlsAlertReceived {
                    alert_id: Some(u8::MAX),
                },
            ),
            (
                ErrorCode::DnsError(DnsErrorPayload {
                    rcode: Some("provider-secret-token".to_owned()),
                    info_code: Some(u16::MAX),
                }),
                SpinFailureCause::DnsError {
                    rcode: Some(SpinDnsRcode::Unknown),
                    info_code: Some(u16::MAX),
                },
            ),
            (
                ErrorCode::DnsError(DnsErrorPayload {
                    rcode: Some("NXDOMAIN".to_owned()),
                    info_code: None,
                }),
                SpinFailureCause::DnsError {
                    rcode: Some(SpinDnsRcode::NameError),
                    info_code: None,
                },
            ),
            (
                ErrorCode::DnsError(DnsErrorPayload {
                    rcode: None,
                    info_code: None,
                }),
                SpinFailureCause::DnsError {
                    rcode: None,
                    info_code: None,
                },
            ),
            (
                ErrorCode::HttpRequestHeaderSize(Some(FieldSizePayload {
                    field_name: Some("provider-secret-token".to_owned()),
                    field_size: Some(u32::MAX),
                })),
                SpinFailureCause::HttpRequestHeaderSize(Some(SpinFieldSize {
                    field_size: Some(u32::MAX),
                })),
            ),
            (
                ErrorCode::HttpRequestBodySize(Some(u64::MAX)),
                SpinFailureCause::HttpRequestBodySize(Some(u64::MAX)),
            ),
            (
                ErrorCode::HttpResponseBodySize(Some(u64::MAX)),
                SpinFailureCause::HttpResponseBodySize(Some(u64::MAX)),
            ),
            (
                ErrorCode::HttpResponseContentCoding(Some("provider-secret-token".to_owned())),
                SpinFailureCause::HttpResponseContentCoding,
            ),
            (
                ErrorCode::HttpResponseTransferCoding(Some("provider-secret-token".to_owned())),
                SpinFailureCause::HttpResponseTransferCoding,
            ),
            (
                ErrorCode::InternalError(Some("provider-secret-token".to_owned())),
                SpinFailureCause::InternalError,
            ),
        ] {
            let mapped = map_spin_host_error_for_test(
                &source,
                live,
                BudgetSource::Default,
                now,
                ProviderFailurePhase::Send,
            );
            assert_eq!(
                mapped.provider_failure_detail(),
                Some(&ProviderFailureDetail::Spin {
                    phase: ProviderFailurePhase::Send,
                    cause: expected,
                })
            );
            assert!(!format!("{mapped:?}").contains("provider-secret-token"));
        }
    }

    #[test]
    fn spin_timeout_provenance_before_and_after_deadline() {
        let now = MonotonicInstant::now();
        let live = Deadline::at_instant(
            now.checked_add(Duration::from_secs(30))
                .expect("live deadline"),
        );
        let early = map_spin_host_error_for_test(
            &ErrorCode::DnsTimeout,
            live,
            BudgetSource::PerCallTimeout,
            now,
            ProviderFailurePhase::Send,
        );
        assert!(matches!(
            early,
            EdgeError::GatewayTimeout {
                cause: BudgetSource::Unspecified,
                ..
            }
        ));

        let unreachable = map_spin_host_error_for_test(
            &ErrorCode::ConnectionRefused,
            live,
            BudgetSource::Default,
            now,
            ProviderFailurePhase::Send,
        );
        assert!(matches!(
            unreachable,
            EdgeError::BadGateway {
                reason: BadGatewayReason::Unreachable,
                ..
            }
        ));
    }
}
