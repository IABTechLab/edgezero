#![cfg(all(feature = "spin", feature = "test-utils", target_arch = "wasm32"))]

#[cfg(test)]
#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "SDK construction checks stay before the exhaustive classifier fixture"
)]
mod tests {
    use std::collections::VecDeque;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use edgezero_adapter_spin::outbound::{
        SpinOutboundClient, deferred_clock_paths_hold_for_test, map_spin_dispatch_error_for_test,
        map_spin_host_error_for_test, map_spin_preparation_header_for_test,
        provider_deadline_diagnostics_hold_for_test, request_fields_for_test,
        request_target_errors_for_test, request_timeout_settings_for_test,
        set_timeout_option_for_test,
    };
    use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
    use edgezero_core::error_detail::{
        ProviderFailureAdapter, ProviderFailureDetail, ProviderFailurePhase, SpinDnsRcode,
        SpinFailureCause, SpinFieldSize, SpinTimeoutOption,
    };
    use edgezero_core::http::{HeaderMap, HeaderValue, Method, Uri};
    use edgezero_core::outbound::{
        HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundBatchTermination,
        OutboundCollectedSlot, OutboundDispatchRequest, OutboundFuture, OutboundHead,
        OutboundHttpClient, OutboundPending, OutboundPreparedBatch, OutboundProgressRuntime,
        OutboundRequest, OutboundSendMode, OutboundTime,
    };
    use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
    use futures::executor::block_on;
    use spin_sdk::wasip3::http::types::{
        DnsErrorPayload, ErrorCode, FieldSizePayload, Fields, HeaderError, Request, RequestOptions,
        RequestOptionsError, Response, TlsAlertReceivedPayload,
    };
    use spin_sdk::wasip3::http_compat::BodyWriter;

    fn preflight_time(clock: MonotonicClock) -> OutboundTime {
        OutboundTime::new(clock, |_, _| {
            panic!("preflight must not register an outbound timer")
        })
    }

    fn assert_outbound_client<Client>()
    where
        Client: OutboundHttpClient,
    {
    }

    struct HandoffClient {
        inner: SpinOutboundClient,
        observed: Arc<Mutex<MonotonicInstant>>,
        handoff_at: MonotonicInstant,
    }

    impl OutboundHttpClient for HandoffClient {
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn time(&self) -> OutboundTime {
            self.inner.time()
        }

        fn validate_supported_options(
            &self,
            request: &OutboundRequest,
            mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            self.inner.validate_supported_options(request, mode)
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            self.inner.dispatch_head(request)
        }

        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            self.inner.begin(request)
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            *self.observed.lock().expect("paired clock") = self.handoff_at;
            self.inner.start_batch_until(batch)
        }
    }

    fn handoff_client(start: MonotonicInstant, handoff_at: MonotonicInstant) -> HttpClient {
        let observed = Arc::new(Mutex::new(start));
        let clock_now = Arc::clone(&observed);
        HttpClient::with_client(HandoffClient {
            inner: SpinOutboundClient::with_time(preflight_time(MonotonicClock::new(move || {
                *clock_now.lock().expect("paired clock")
            }))),
            observed,
            handoff_at,
        })
    }

    #[test]
    fn expired_batch_handoff_completes_an_earlier_own_timeout_without_runtime() {
        let start = MonotonicInstant::now();
        let observation_cutoff = Deadline::at_instant(
            start
                .checked_add(Duration::from_millis(100))
                .expect("cutoff"),
        );
        let handoff_at = start
            .checked_add(Duration::from_millis(150))
            .expect("handoff");
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .timeout(Duration::from_millis(50));
        let result = block_on(
            handoff_client(start, handoff_at)
                .start_batch_until(vec![request], observation_cutoff)
                .collect(),
        )
        .expect("an earlier own timeout must not be omitted at the expired handoff");
        assert_eq!(result.termination(), OutboundBatchTermination::Completed);
        let [OutboundCollectedSlot::Resolved(slot)] = result.slots() else {
            panic!("resolved own timeout");
        };
        assert_eq!(slot.elapsed, Duration::from_millis(50));
        assert!(matches!(
            slot.outcome,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
    }

    #[test]
    fn expired_batch_handoff_keeps_preflight_indices_and_cutoff_slots_without_runtime() {
        use bytes::Bytes;

        let start = MonotonicInstant::now();
        let observation_cutoff = Deadline::at_instant(
            start
                .checked_add(Duration::from_millis(100))
                .expect("cutoff"),
        );
        let handoff_at = start
            .checked_add(Duration::from_millis(150))
            .expect("handoff");
        let upload = Arc::<[u8]>::from(&b"buffered upload"[..]);
        let released = Arc::downgrade(&upload);
        let requests = vec![
            OutboundRequest::post("https://example.com/upload")
                .expect("request")
                .body(Bytes::from_owner(upload))
                .timeout(Duration::from_millis(50)),
            OutboundRequest::get("https://example.com/rejected")
                .expect("request")
                .stream_response(),
            OutboundRequest::get("https://example.com/equal")
                .expect("request")
                .timeout(Duration::from_millis(100)),
            OutboundRequest::get("https://example.com/later")
                .expect("request")
                .timeout(Duration::from_millis(200)),
            OutboundRequest::get("https://example.com/earlier")
                .expect("request")
                .deadline(Deadline::at_instant(
                    start
                        .checked_add(Duration::from_millis(30))
                        .expect("deadline"),
                )),
        ];
        let batch =
            handoff_client(start, handoff_at).start_batch_until(requests, observation_cutoff);
        assert!(
            released.upgrade().is_none(),
            "upload released before observation"
        );
        let result = block_on(batch.collect()).expect("expired mixed handoff");
        assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
        let [
            OutboundCollectedSlot::Resolved(own_timeout),
            OutboundCollectedSlot::Resolved(preflight),
            OutboundCollectedSlot::Unresolved,
            OutboundCollectedSlot::Unresolved,
            OutboundCollectedSlot::Resolved(earlier),
        ] = result.slots()
        else {
            panic!("input indices and unresolved cutoff slots retained");
        };
        assert_eq!(own_timeout.elapsed, Duration::from_millis(50));
        assert!(matches!(
            own_timeout.outcome,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert_eq!(preflight.elapsed, Duration::ZERO);
        assert!(matches!(
            preflight.outcome,
            Err(EdgeError::BadRequest { .. })
        ));
        assert_eq!(earlier.elapsed, Duration::from_millis(30));
        assert!(matches!(
            earlier.outcome,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::RequestDeadline,
                ..
            })
        ));
    }

    #[test]
    fn spin_outbound_client_implements_portable_contract() {
        assert_outbound_client::<SpinOutboundClient>();
    }

    #[test]
    fn eager_spawn_outside_p3_context_is_a_typed_setup_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;

        use edgezero_core::Body;
        use futures_util::stream::poll_fn;

        let client = HttpClient::with_client(SpinOutboundClient::new());
        for begin in [false, true] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&polls);
            let owner = Arc::new(());
            let released = Arc::downgrade(&owner);
            let source = poll_fn(move |_cx| {
                let _keep_alive = Arc::clone(&owner);
                observed.fetch_add(1, Ordering::SeqCst);
                Poll::<Option<Result<bytes::Bytes, EdgeError>>>::Pending
            });
            let request = OutboundRequest::new(
                if begin { Method::GET } else { Method::POST },
                Uri::from_static("https://example.com/"),
            )
            .expect("request")
            .body(Body::from_stream(source));
            let error = if begin {
                block_on(client.begin(request)).expect_err("missing P3 scheduler")
            } else {
                block_on(client.send(request)).expect_err("missing P3 scheduler")
            };
            assert!(matches!(error, EdgeError::Internal { .. }));
            assert_eq!(
                error.inner().expect("setup error").to_string(),
                "Spin outbound scheduling requires an active WASIp3 task"
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(released.upgrade().is_none(), "unpolled upload released");
        }
        let failure = block_on(
            client
                .start_batch_until(
                    vec![OutboundRequest::get("https://example.com/").expect("request")],
                    Deadline::after(Duration::from_secs(1)),
                )
                .collect(),
        )
        .expect_err("missing P3 batch scheduler");
        assert!(matches!(failure.error(), EdgeError::Internal { .. }));
        assert_eq!(
            failure.error().inner().expect("setup error").to_string(),
            "Spin outbound scheduling requires an active WASIp3 task"
        );
        assert!(matches!(
            failure.slots(),
            [OutboundCollectedSlot::Unresolved]
        ));
    }

    #[test]
    fn connect_timeout_reaches_provider_settings() {
        use std::mem::replace;
        for (configured, expected) in [
            (None, 40_000_000),
            (Some(Duration::from_millis(7)), 7_000_000),
            (Some(Duration::from_millis(80)), 40_000_000),
        ] {
            for refusal in [
                Ok(()),
                Err(RequestOptionsError::NotSupported),
                Err(RequestOptionsError::Immutable),
                Err(RequestOptionsError::Other(None)),
                Err(RequestOptionsError::Other(Some(
                    "provider-private".to_owned(),
                ))),
            ] {
                let mut retained = refusal;
                let mut settings = Vec::new();
                request_timeout_settings_for_test(
                    Duration::from_millis(40),
                    configured,
                    |option, value| {
                        settings.push((option, value));
                        if option == SpinTimeoutOption::Connect {
                            replace(&mut retained, Ok(()))
                        } else {
                            Ok(())
                        }
                    },
                )
                .expect("connect refusals retain aggregate enforcement");
                assert_eq!(
                    settings,
                    [
                        (SpinTimeoutOption::Connect, expected),
                        (SpinTimeoutOption::FirstByte, 40_000_000),
                        (SpinTimeoutOption::BetweenBytes, 40_000_000),
                    ]
                );
            }
        }
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
        let client = HttpClient::with_client(SpinOutboundClient::with_time(preflight_time(
            MonotonicClock::new(move || {
                clock_observations
                    .lock()
                    .expect("clock observations")
                    .pop_front()
                    .unwrap_or(completed)
            }),
        )));
        let request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();

        let results = block_on(
            client
                .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
                .collect(),
        )
        .expect("valid batch driver");

        let OutboundCollectedSlot::Resolved(slot) = &results.slots()[0] else {
            panic!("resolved slot");
        };
        assert_eq!(slot.elapsed, Duration::from_millis(7));
        assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
    }

    #[test]
    fn spin_unsupported_authority_precedes_generic_validation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;

        use edgezero_core::Body;
        use edgezero_core::http::{Method, Uri};
        use futures_util::stream::poll_fn;

        let start = MonotonicInstant::now();
        let client = HttpClient::with_client(SpinOutboundClient::with_time(preflight_time(
            MonotonicClock::new(move || start),
        )));
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
                    assert_eq!(results.termination(), OutboundBatchTermination::Completed);
                    let (slots, _) = results.into_parts();
                    let OutboundCollectedSlot::Resolved(slot) =
                        slots.into_iter().next().expect("slot")
                    else {
                        panic!("resolved unsupported authority");
                    };
                    slot.outcome.expect_err("unsupported authority")
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
    fn connect_timeout_refusals_remain_advisory() {
        for outcome in [
            Ok(()),
            Err(RequestOptionsError::NotSupported),
            Err(RequestOptionsError::Immutable),
            Err(RequestOptionsError::Other(None)),
            Err(RequestOptionsError::Other(Some(
                "provider-secret-token".to_owned(),
            ))),
        ] {
            set_timeout_option_for_test(SpinTimeoutOption::Connect, &outcome)
                .expect("connect settings retain aggregate deadline fallback");
        }
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

            for connect in [None, Some(Duration::from_millis(7))] {
                let dispatched = map_spin_dispatch_error_for_test(
                    &code,
                    live,
                    BudgetSource::Default,
                    now,
                    ProviderFailurePhase::Send,
                    connect,
                );
                if connect.is_some() && matches!(&code, ErrorCode::ConnectionTimeout) {
                    assert!(matches!(
                        dispatched,
                        EdgeError::GatewayTimeout {
                            cause: BudgetSource::ConnectTimeout,
                            ..
                        }
                    ));
                } else {
                    assert_expected_error(&dispatched, expected);
                }
                assert_eq!(
                    dispatched.provider_failure_detail(),
                    mapped.provider_failure_detail()
                );
            }

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

    #[test]
    fn connect_expiry_preserves_governing_budget_precedence() {
        let start = MonotonicInstant::now();
        let end = start.checked_add(Duration::from_secs(1)).expect("deadline");
        for (observed, expected) in [
            (start, BudgetSource::Unspecified),
            (end, BudgetSource::RequestDeadline),
            (
                end.checked_add(Duration::from_nanos(1))
                    .expect("late observation"),
                BudgetSource::RequestDeadline,
            ),
        ] {
            let error = map_spin_host_error_for_test(
                &ErrorCode::ConnectionTimeout,
                Deadline::at_instant(end),
                BudgetSource::RequestDeadline,
                observed,
                ProviderFailurePhase::Send,
            );
            assert!(
                matches!(&error, EdgeError::GatewayTimeout { cause, .. } if *cause == expected)
            );
            assert_eq!(
                error.provider_failure_detail(),
                Some(&ProviderFailureDetail::Spin {
                    phase: ProviderFailurePhase::Send,
                    cause: SpinFailureCause::ConnectionTimeout,
                })
            );
        }
    }

    #[test]
    fn connect_expiry_uses_each_request_setting_and_retains_provider_details() {
        let start = MonotonicInstant::now();
        let end = start.checked_add(Duration::from_secs(1)).expect("deadline");
        let late = end
            .checked_add(Duration::from_nanos(1))
            .expect("late observation");
        for selected in [
            BudgetSource::Default,
            BudgetSource::PerCallTimeout,
            BudgetSource::RequestDeadline,
            BudgetSource::BatchCutoff,
        ] {
            for phase in [
                ProviderFailurePhase::Send,
                ProviderFailurePhase::RequestBodyUpload,
                ProviderFailurePhase::ResponseBodyRead,
            ] {
                for configured in [None, Some(Duration::from_millis(7)), None] {
                    let mut request =
                        OutboundRequest::get("https://example.com/").expect("request");
                    if let Some(timeout) = configured {
                        request = request.connect_timeout(timeout);
                    }
                    let parts = request.into_parts();
                    assert_eq!(parts.connect_timeout, configured);
                    for observed in [start, end, late] {
                        let expected = if observed >= end {
                            selected
                        } else if configured.is_some() && phase == ProviderFailurePhase::Send {
                            BudgetSource::ConnectTimeout
                        } else {
                            BudgetSource::Unspecified
                        };
                        let error = map_spin_dispatch_error_for_test(
                            &ErrorCode::ConnectionTimeout,
                            Deadline::at_instant(end),
                            selected,
                            observed,
                            phase,
                            parts.connect_timeout,
                        );
                        assert!(
                            matches!(&error, EdgeError::GatewayTimeout { cause, .. } if *cause == expected)
                        );
                        assert_eq!(
                            error.provider_failure_detail(),
                            Some(&ProviderFailureDetail::Spin {
                                phase,
                                cause: SpinFailureCause::ConnectionTimeout,
                            })
                        );
                    }
                }
            }
        }
    }
}
