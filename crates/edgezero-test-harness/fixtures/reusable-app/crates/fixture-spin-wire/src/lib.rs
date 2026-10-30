#[cfg(target_arch = "wasm32")]
mod sdk {
    use core::str::from_utf8;
    use std::cell::{Cell, RefCell};
    use std::convert::Infallible;
    use std::env;
    use std::rc::Rc;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    use async_stream::stream;
    use bytes::Bytes;
    use edgezero_adapter_spin::outbound::SpinOutboundClient;
    use edgezero_adapter_spin::outbound::test_support::observe_response_head;
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::context::RequestContext;
    use edgezero_core::http::{Method, Response as CoreResponse, Uri};
    use edgezero_core::ingress::{AdmissionDecision, IngressGrant};
    use edgezero_core::outbound::{
        HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundBatchTermination,
        OutboundCollectedSlot, OutboundFollowFailure, OutboundFuture, OutboundRedirectAction,
        OutboundRedirectLimit, OutboundRequest, OutboundResponse, OutboundScope,
        OutboundScopeOwner,
    };
    use edgezero_core::response_egress::{
        ResponseEgressCompletion, ResponseEgressOutcome, ResponseEgressReport,
    };
    use edgezero_core::router::RouterService;
    use edgezero_core::{Body, EdgeError, ResponseDecoding, action};
    use fixture_spin_support::outbound::canonical_entrypoints_release_retained_handles;
    use futures_util::{StreamExt as _, stream};
    use serde_json::{Value, json};
    use spin_sdk::http::{FromRequest as _, IntoResponse as _, Request as SpinRequest};
    use spin_sdk::wasip3::exports::http::handler::Guest;
    use spin_sdk::wasip3::http::types::{ErrorCode, Fields, Request, Response};
    use spin_sdk::wasip3::{http, wit_future};

    struct BodylessWire;

    struct HandoffEvidence {
        callback: OutboundScope,
        eof: bool,
        retained: OutboundFuture<OutboundResponse>,
        scope: OutboundScope,
        upload: Weak<[u8]>,
    }

    struct HandoffHooks;

    std::thread_local! {
        static HANDOFF: RefCell<Option<HandoffEvidence>> = const { RefCell::new(None) };
    }

    impl Guest for BodylessWire {
        async fn handle(request: Request) -> Result<Response, ErrorCode> {
            let path = request.get_path_with_query().unwrap_or_default();
            if path == "/handoff" {
                let incoming = SpinRequest::from_request(request)?;
                return edgezero_adapter_spin::run_app::<HandoffHooks>(incoming)
                    .await
                    .map_err(|_error| {
                        ErrorCode::InternalError(Some("handoff fixture failed".to_owned()))
                    });
            }
            if path == "/scope" {
                drop(request);
                canonical_entrypoints_release_retained_handles()
                    .await
                    .map_err(|error| ErrorCode::InternalError(Some(error.to_string())))?;
                return empty_response();
            }
            let method = match path.as_str() {
                "/get" | "/get-settled" | "/p5" | "/policy" => Method::GET,
                "/head" | "/head-settled" => Method::HEAD,
                "/post" => Method::POST,
                _ => return Err(ErrorCode::HttpRequestUriInvalid),
            };
            let headers = request.get_headers();
            let origins = headers.get("x-origin");
            let origin = origins.first().ok_or(ErrorCode::HttpRequestUriInvalid)?;
            let uri: Uri = from_utf8(origin)
                .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?
                .parse()
                .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?;
            if uri.scheme_str() != Some("http")
                || uri.host() != Some("127.0.0.1")
                || uri.port_u16().is_none()
            {
                return Err(ErrorCode::HttpRequestUriInvalid);
            }
            if path == "/p5" {
                return p5_exchange(uri, &headers).await;
            }
            if path == "/policy" {
                return policy_exchange(uri, &headers).await;
            }
            let mut outgoing = OutboundRequest::new(method, uri)
                .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?;
            if path.ends_with("-settled") {
                outgoing = outgoing.body(Body::stream(stream::iter([Bytes::new(), Bytes::new()])));
            }
            if path == "/post" {
                outgoing = outgoing.body(Body::from("upload-control"));
            }
            HttpClient::with_client(SpinOutboundClient::new())
                .send(outgoing)
                .await
                .map_err(|_error| {
                    ErrorCode::InternalError(Some("outbound wire check failed".to_owned()))
                })?;
            empty_response()
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "fixture configures only its route and terminal proof"
    )]
    impl Hooks for HandoffHooks {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            app.set_ingress_admission_policy(|head| AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::new(check_handoff_terminal),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(5)),
            });
            Ok(())
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder()
                .get("/handoff", handoff_response)
                .build())
        }
    }

    #[expect(
        clippy::print_stdout,
        clippy::print_stderr,
        reason = "HTTP harness checks the SDK-terminal proof in component logs"
    )]
    fn check_handoff_terminal(report: &ResponseEgressReport) {
        let evidence = HANDOFF.with(|slot| slot.borrow_mut().take());
        if let Some(proof) = evidence
            && proof.eof
            && proof.callback.check().is_err()
            && proof.scope.check().is_err()
            && proof.upload.upgrade().is_none()
            && report.outcome == ResponseEgressOutcome::HostHandoff
        {
            drop(proof.retained);
            println!("PASS: Spin attached Begin/Batch handoff closed at SDK terminal");
            return;
        }
        eprintln!("FAIL: Spin attached Begin/Batch handoff terminal ownership");
    }

    fn empty_response() -> Result<Response, ErrorCode> {
        let headers = Fields::new();
        if let (Ok(nonce), Some(build)) = (
            env::var("EDGEZERO_FIXTURE_LAUNCH_NONCE"),
            option_env!("EDGEZERO_FIXTURE_BUILD_ID"),
        ) {
            let identity = format!("v1;{nonce};{build};spin-wire");
            headers
                .append("x-edgezero-fixture-launch", identity.as_bytes())
                .map_err(|_error| {
                    ErrorCode::InternalError(Some(
                        "fixture launch identity header rejected".to_owned(),
                    ))
                })?;
        }
        let (complete, trailers) = wit_future::new(|| Ok(None));
        drop(complete);
        let (response, result) = Response::new(headers, None, trailers);
        drop(result);
        Ok(response)
    }

    fn fixture_error(message: &'static str) -> EdgeError {
        EdgeError::internal(anyhow::anyhow!(message))
    }

    fn p5_error(error: &EdgeError) -> Value {
        let reason = if let EdgeError::BadGateway { reason, .. } = error {
            format!("{reason:?}")
        } else if let EdgeError::ResponseTooLarge { reason, .. } = error {
            format!("{reason:?}")
        } else if let EdgeError::GatewayTimeout { cause, .. } = error {
            format!("{cause:?}")
        } else {
            String::new()
        };
        json!({
            "error": {
                "kind": error.kind(),
                "status": error.status().as_u16(),
                "reason": reason,
                "provider": error.provider_failure_detail().map(|detail| format!("{detail:?}")),
            }
        })
    }

    async fn policy_exchange(uri: Uri, headers: &Fields) -> Result<Response, ErrorCode> {
        let case = headers
            .get("x-policy-case")
            .first()
            .and_then(|value| from_utf8(value).ok())
            .and_then(|value| value.parse::<u8>().ok())
            .filter(|value| *value < 26)
            .ok_or(ErrorCode::HttpRequestUriInvalid)?;
        let (original, streamed, encoded_cap, final_cap) = if case < 16 {
            (case & 8 != 0, case & 4 != 0, 20, 20)
        } else {
            let variant = case.saturating_sub(if case < 21 { 16 } else { 21 });
            (
                true,
                variant >= 3,
                if matches!(variant, 1 | 4) { 22 } else { 23 },
                if variant == 2 { 22 } else { 23 },
            )
        };
        let mut request = OutboundRequest::new(Method::GET, uri)
            .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?
            .timeout(Duration::from_secs(3))
            .response_decoding(if original {
                ResponseDecoding::Original
            } else {
                ResponseDecoding::Decode
            })
            .max_decoded_response_bytes(10)
            .max_encoded_response_bytes(encoded_cap)
            .max_response_bytes(final_cap)
            .max_decoder_bytes(0);
        if streamed {
            request = request.stream_response();
        }
        let operation = async {
            let client = HttpClient::with_client(SpinOutboundClient::new());
            let response = match client.send(request).await {
                Ok(response) => response,
                Err(error) => return (p5_error(&error), Some("send")),
            };
            let status = response.status().as_u16();
            let body_streamed = response.body().is_stream();
            let mut fields = response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
                .collect::<Vec<_>>();
            fields.sort_unstable();
            // This reporter is above every tested limit; it cannot supply an expected overflow.
            match response.into_bytes_bounded(1024).await {
                Ok(bytes) => (
                    json!({"status": status, "headers": fields, "bytes": bytes.to_vec(), "streamed": body_streamed}),
                    None,
                ),
                Err(error) => (p5_error(&error), Some("body")),
            }
        };
        let ((outcome, error_stage), polls, released) =
            observe_response_head(operation).await.map_err(|_error| {
                ErrorCode::InternalError(Some("response policy probe failed".to_owned()))
            })?;
        json!({
            "case": case,
            "request": {
                "decoding": if original { "original" } else { "decode" },
                "streamed": streamed,
                "decoded_cap": 10_u64,
                "encoded_cap": encoded_cap,
                "final_cap": (!streamed).then_some(final_cap),
                "decoder_cap": 0_u64,
            },
            "outcome": outcome,
            "error_stage": error_stage,
            "source": { "polls": polls, "released": released },
        })
        .to_string()
        .into_response()
    }

    async fn p5_exchange(uri: Uri, headers: &Fields) -> Result<Response, ErrorCode> {
        let modes = headers.get("x-p5-mode");
        let mode = modes.first().map(Vec::as_slice).unwrap_or_default();
        if !matches!(mode, b"send" | b"follow" | b"stop" | b"limit") {
            return Err(ErrorCode::HttpRequestUriInvalid);
        }
        let streamed = headers.get("x-p5-stream") == [b"true".to_vec()];
        let mut request = OutboundRequest::new(Method::GET, uri.clone())
            .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?
            .timeout(Duration::from_secs(3))
            .max_response_bytes(10);
        if streamed {
            request = request.stream_response().max_decoded_response_bytes(10);
        }
        if headers.get("x-p5-encoded-cap") == [b"true".to_vec()] {
            request = request.max_encoded_response_bytes(10);
        }
        let client = HttpClient::with_client(SpinOutboundClient::new());
        let policies = Rc::new(Cell::new(0_usize));
        let exchange = if mode == b"send" {
            client
                .send(request)
                .await
                .map(|response| (response, uri, 0, None))
                .map_err(|error| (0, error))
        } else {
            let observed = Rc::clone(&policies);
            let stop = mode == b"stop";
            let limit = OutboundRedirectLimit::new(if mode == b"limit" { 1 } else { 4 })
                .map_err(|_error| ErrorCode::HttpRequestUriInvalid)?;
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
            Ok((response, final_uri, redirects, stop)) => {
                let status = response.status().as_u16();
                let body_streamed = response.body().is_stream();
                let mut fields = response
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
                    .collect::<Vec<_>>();
                fields.sort_unstable();
                // This safety bound is above every tested cap, including streamed caps.
                let outcome = match response.into_bytes_bounded(1024).await {
                    Ok(bytes) => {
                        json!({"status": status, "headers": fields, "bytes": bytes.to_vec()})
                    }
                    Err(error) => p5_error(&error),
                };
                json!({
                    "outcome": outcome,
                    "hop": redirects,
                    "final_uri": final_uri.to_string(),
                    "redirects": redirects,
                    "stop": stop.map(|reason| format!("{reason:?}")),
                    "streamed": body_streamed,
                    "policies": policies.get(),
                })
            }
            Err((hop, error)) => json!({
                "outcome": p5_error(&error), "hop": hop, "policies": policies.get(),
            }),
        };
        report.to_string().into_response()
    }

    #[action]
    async fn handoff_response(
        RequestContext(ctx): RequestContext,
    ) -> Result<CoreResponse, EdgeError> {
        let base_uri: Uri = ctx
            .headers()
            .get("x-origin")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| fixture_error("handoff origin missing"))?
            .parse()
            .map_err(EdgeError::internal)?;
        if base_uri.scheme_str() != Some("http")
            || base_uri.host() != Some("127.0.0.1")
            || base_uri.port_u16().is_none()
        {
            return Err(fixture_error("handoff origin must be loopback"));
        }
        let base = format!(
            "http://{}",
            base_uri
                .authority()
                .ok_or_else(|| fixture_error("handoff authority"))?
        );
        let client = ctx
            .http_client()
            .ok_or_else(|| fixture_error("handoff client missing"))?;
        let callback = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let upload = Arc::<[u8]>::from(&b"retained beyond source EOF"[..]);
        let probe = Arc::downgrade(&upload);
        let unused = OutboundRequest::post(format!("{base}/handoff-unused"))?
            .body(Bytes::from_owner(upload));
        let retained = owner.with(|| client.send(unused));
        let origin_request =
            OutboundRequest::get(format!("{base}/handoff-origin"))?.stream_response();
        let origin = owner.with(|| client.begin(origin_request)).await?;
        let requests = ["A", "B", "C"]
            .map(|bidder| OutboundRequest::get(format!("{base}/handoff-bid-{bidder}")))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let observation_cutoff = ctx.monotonic_clock().deadline_after(Duration::from_secs(5));
        let batch = owner.with(|| client.start_batch_until(requests, observation_cutoff));
        let previous = HANDOFF.with(|slot| {
            slot.replace(Some(HandoffEvidence {
                callback,
                eof: false,
                retained,
                scope,
                upload: probe,
            }))
        });
        drop(previous);
        // All bidder requests are still pending when the origin head is handed to the handler.
        let origin_response = origin.collect().await?;
        owner.attach(CoreResponse::new(handoff_source(
            client,
            base,
            origin_response,
            batch,
        )))
    }

    fn handoff_source(
        client: HttpClient,
        base: String,
        origin_response: OutboundResponse,
        batch: OutboundBatch,
    ) -> Body {
        let source = stream! {
            let after_callback = HANDOFF.with(|slot| slot.borrow().as_ref()
                .is_some_and(|proof| proof.callback.check().is_err() && proof.scope.check().is_ok()));
            if !after_callback {
                yield Err(fixture_error("body polled before accepted callback handoff"));
                return;
            }
            let release = match OutboundRequest::get(format!("{base}/handoff-release")) {
                Ok(request) => request,
                Err(error) => { yield Err(error); return; }
            };
            if let Err(error) = client.send(release).await {
                yield Err(error);
                return;
            }
            let Ok(mut chunks) = origin_response.into_body().into_stream() else {
                yield Err(fixture_error("begun origin must remain streamed"));
                return;
            };
            while let Some(chunk) = chunks.next().await {
                match chunk {
                    Ok(bytes) => yield Ok(bytes),
                    Err(error) => { yield Err(error); return; }
                }
            }
            let results = match batch.collect().await {
                Ok(results) => results,
                Err(failure) => { yield Err(failure.into_parts().0); return; }
            };
            let (slots, termination) = results.into_parts();
            if termination != OutboundBatchTermination::Completed || slots.len() != 3 {
                yield Err(fixture_error("handoff batch did not complete"));
                return;
            }
            for slot in slots {
                let OutboundCollectedSlot::Resolved(result) = slot else {
                    yield Err(fixture_error("handoff batch lost a slot"));
                    return;
                };
                match result.outcome {
                    Ok(response) => match response.into_body().into_bytes_bounded(64).await {
                        Ok(bytes) => yield Ok(bytes),
                        Err(error) => { yield Err(error); return; }
                    },
                    Err(error) => { yield Err(error); return; }
                }
            }
            let live_at_eof = HANDOFF.with(|slot| {
                let mut evidence = slot.borrow_mut();
                evidence.as_mut().is_some_and(|proof| {
                    proof.eof = true;
                    proof.scope.check().is_ok() && proof.upload.upgrade().is_some()
                })
            });
            if !live_at_eof {
                yield Err(fixture_error("response owner closed before source EOF"));
            }
        };
        Body::from_stream(source)
    }

    http::service::export!(BodylessWire);
}
