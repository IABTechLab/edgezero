#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "wasm_bindgen_test functions are tests, but Clippy does not classify the generated wrappers as tests"
)]

#[cfg(all(test, feature = "cloudflare", target_arch = "wasm32"))]
#[path = "../src/test_runtime.rs"]
mod test_runtime;

// Compile-time check: CloudflareSecretStore implements SecretStore.
#[cfg(all(feature = "cloudflare", target_arch = "wasm32"))]
mod secret_store_compile_check {
    use edgezero_adapter_cloudflare::secret_store::CloudflareSecretStore;
    use edgezero_core::secret_store::SecretStore;

    fn assert_provider_impl<T>()
    where
        T: SecretStore,
    {
    }

    // Anonymous const whose initializer is a never-called fn pointer; the
    // type bound is checked at type-check time.
    const _: fn() = assert_provider_impl::<CloudflareSecretStore>;
}

#[cfg(all(test, feature = "cloudflare", target_arch = "wasm32"))]
#[cfg_attr(
    feature = "test-utils",
    expect(
        clippy::arbitrary_source_item_ordering,
        reason = "ingress contracts are grouped after the provider fixture tests"
    )
)]
mod tests {
    use crate::test_runtime::NumericTimeoutGuard;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use edgezero_adapter_cloudflare::context::CloudflareRequestContext;
    use edgezero_adapter_cloudflare::outbound::CloudflareOutboundClient;
    #[cfg(feature = "test-utils")]
    use edgezero_adapter_cloudflare::outbound::{
        deferred_clock_paths_hold_for_test, response_abort_lifecycle_holds_for_test,
    };
    #[cfg(feature = "test-utils")]
    use edgezero_adapter_cloudflare::request::deadline_body_releases_source_for_test;
    use edgezero_adapter_cloudflare::request::{CloudflareService, into_core_request};
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::body::Body;
    use edgezero_core::config_store::{
        BoundedStoreRead, ConfigStore, ConfigStoreError, ConfigStoreHandle,
        finish_bounded_config_read,
    };
    use edgezero_core::context::RequestContext;
    use edgezero_core::error::EdgeError;
    use edgezero_core::http::{Method, Response, StatusCode, response_builder};
    use edgezero_core::outbound::{
        HttpClient, OutboundCollectedSlot, OutboundRequest, OutboundSlotResult, OutboundTime,
    };
    use edgezero_core::router::RouterService;
    use edgezero_core::time::{Deadline, MonotonicClock, MonotonicInstant};
    use futures::channel::mpsc::unbounded;
    use futures::future::{Either, select};
    use futures::{StreamExt as _, pin_mut, stream};
    use std::io::Error as IoError;
    use wasm_bindgen_test::wasm_bindgen_test;
    use worker::js_sys::{Function, Object, Reflect};
    use worker::wasm_bindgen::{JsCast as _, JsValue};
    use worker::worker_sys::Context as WorkerSysContext;
    use worker::{Context, Env, Method as CfMethod, Request as CfRequest, RequestInit};

    struct FixedConfigStore(&'static str);

    #[cfg(feature = "test-utils")]
    struct FailingConfiguration;

    #[cfg(feature = "test-utils")]
    #[expect(
        clippy::missing_trait_methods,
        reason = "test hook exercises only adapter startup failure"
    )]
    impl Hooks for FailingConfiguration {
        fn configure(_app: &mut App) -> Result<(), EdgeError> {
            Err(EdgeError::service_unavailable(
                "private configuration detail",
            ))
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder().build())
        }
    }

    #[cfg(feature = "test-utils")]
    #[wasm_bindgen_test]
    fn failing_configuration() {
        let result =
            App::build::<FailingConfiguration>(edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM);
        let error = result.err().expect("configuration must not produce an app");
        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.message(), "private configuration detail");
    }

    #[async_trait::async_trait(?Send)]
    impl ConfigStore for FixedConfigStore {
        async fn get(
            &self,
            _key: &str,
        ) -> Result<Option<edgezero_core::ConfigValue>, ConfigStoreError> {
            Ok(Some(self.0.into()))
        }

        async fn get_bounded(
            &self,
            key: &str,
            clock: &MonotonicClock,
            deadline: Deadline,
            max_backend_bytes: u64,
            max_value_bytes: u64,
        ) -> Result<BoundedStoreRead<edgezero_core::ConfigValue>, ConfigStoreError> {
            if deadline.is_expired_at(clock.now()) {
                return Err(ConfigStoreError::DeadlineExceeded);
            }
            finish_bounded_config_read(
                self.get(key).await,
                clock,
                deadline,
                max_backend_bytes,
                max_value_bytes,
            )
        }
    }

    fn resolved_slot(slot: &OutboundCollectedSlot) -> &OutboundSlotResult {
        match slot {
            OutboundCollectedSlot::Resolved(result) => Some(result),
            OutboundCollectedSlot::AlreadyDelivered | OutboundCollectedSlot::Unresolved => None,
        }
        .expect("expected a resolved slot")
    }

    #[wasm_bindgen_test]
    async fn head_conversion_error_uses_detached_completion_or_abort() {
        use edgezero_core::response_egress::{
            DetachedResponseEgressDecision, ResponseEgressCompletion,
        };

        for abort in [false, true] {
            let factories = Arc::new(AtomicUsize::new(0));
            let completions = Arc::new(AtomicUsize::new(0));
            let observed_factory = Arc::clone(&factories);
            let observed_completion = Arc::clone(&completions);
            let (terminal_sender, mut terminal_receiver) = unbounded();
            let mut app = App::new(RouterService::builder().build());
            app.set_detached_response_egress_decision_factory(move |head| {
                assert_eq!(head.status(), StatusCode::BAD_REQUEST);
                observed_factory.fetch_add(1, Ordering::SeqCst);
                if abort {
                    DetachedResponseEgressDecision::Abort
                } else {
                    let terminal = Arc::clone(&observed_completion);
                    let notification_sender = terminal_sender.clone();
                    DetachedResponseEgressDecision::Send {
                        completion: ResponseEgressCompletion::new(move |_report| {
                            terminal.fetch_add(1, Ordering::SeqCst);
                            notification_sender
                                .unbounded_send(())
                                .expect("terminal receiver");
                        }),
                        deadline: head.write_deadline_after(Duration::from_secs(1)),
                    }
                }
            });
            let (env, context, _timers) = test_env_ctx();
            let mut request =
                CfRequest::new_with_init("https://example.com/reserved", &RequestInit::default())
                    .expect("request");
            request
                .headers_mut()
                .expect("mutable headers")
                .set("x-invalid", "a\u{1}b")
                .expect("runtime header");
            let result = CloudflareService::new(&app)
                .dispatch(request, env, context)
                .await;
            if abort {
                assert!(result.is_err());
            } else {
                let mut response = result.expect("detached response");
                assert_eq!(response.status_code(), 400);
                assert!(!response.bytes().await.expect("error body").is_empty());
                let notification = terminal_receiver.next();
                let timeout = worker::Delay::from(Duration::from_secs(1));
                pin_mut!(notification, timeout);
                assert!(matches!(
                    select(notification, timeout).await,
                    Either::Left((Some(()), _timeout))
                ));
            }
            assert_eq!(factories.load(Ordering::SeqCst), 1);
            assert_eq!(completions.load(Ordering::SeqCst), usize::from(!abort));
        }
    }

    fn build_test_app() -> App {
        async fn capture_uri(ctx: RequestContext) -> Result<Response, EdgeError> {
            let body = Body::text(ctx.uri().to_string());
            let response = response_builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("response");
            Ok(response)
        }

        async fn mirror_body(ctx: RequestContext) -> Result<Response, EdgeError> {
            let bytes = ctx.body_bytes(1024 * 1024).await?.to_vec();
            let response = response_builder()
                .status(StatusCode::OK)
                .body(Body::from(bytes))
                .expect("response");
            Ok(response)
        }

        async fn config_presence(ctx: RequestContext) -> Result<Response, EdgeError> {
            // Hard-cutoff: legacy `ctx.config_handle()` is
            // gone. The dispatch boundary now synthesises a one-id
            // `ConfigRegistry` from the wired `ConfigStoreHandle`, so
            // the registry-aware accessor resolves the same store.
            let present = if ctx.config_store_default().is_some() {
                "yes"
            } else {
                "no"
            };
            let response = response_builder()
                .status(StatusCode::OK)
                .body(Body::text(present))
                .expect("response");
            Ok(response)
        }

        async fn stream_response(_ctx: RequestContext) -> Result<Response, EdgeError> {
            let chunks = stream::iter(vec![
                Bytes::from_static(b"chunk-1"),
                Bytes::from_static(b"chunk-2"),
            ]);

            let response = response_builder()
                .status(StatusCode::OK)
                .body(Body::stream(chunks))
                .expect("response");
            Ok(response)
        }

        async fn config_value(ctx: RequestContext) -> Result<Response, EdgeError> {
            // Hard-cutoff: legacy `ctx.config_handle()` is
            // gone. See `config_presence` for the migration rationale.
            let value = match ctx.config_store_default() {
                Some(store) => store
                    .get("greeting")
                    .await
                    .ok()
                    .flatten()
                    .map_or_else(|| "missing".to_owned(), |value| value.to_string()),
                None => "missing".to_owned(),
            };
            let response = response_builder()
                .status(StatusCode::OK)
                .body(Body::text(value))
                .expect("response");
            Ok(response)
        }

        let router = RouterService::builder()
            .get("/uri", capture_uri)
            .post("/mirror", mirror_body)
            .get("/stream", stream_response)
            .get("/has-config", config_presence)
            .get("/config-value", config_value)
            .build();

        App::new(router)
    }

    fn cf_request(method: CfMethod, path: &str, body: Option<&[u8]>) -> CfRequest {
        use worker::js_sys::Uint8Array;

        let mut init = RequestInit::new();
        init.with_method(method);

        let headers = worker::Headers::new();
        headers.set("host", "example.com").expect("host header");
        headers.set("x-edgezero-test", "1").expect("custom header");
        init.with_headers(headers);

        if let Some(bytes) = body {
            let array = Uint8Array::from(bytes);
            init.with_body(Some(JsValue::from(array))); // Uint8Array -> JsValue
        }

        let url = format!("https://example.com{path}");
        let request = CfRequest::new_with_init(&url, &init).expect("cf request");
        assert_eq!(
            request.inner().body().is_some(),
            body.is_some(),
            "request fixture must preserve native body presence before conversion"
        );
        request
    }

    fn preflight_time(clock: MonotonicClock) -> OutboundTime {
        OutboundTime::new(clock, |_, _| {
            Box::pin(async {
                Err(EdgeError::internal(IoError::other(
                    "preflight must not register an outbound timer",
                )))
            })
        })
    }

    fn test_env_ctx() -> (Env, Context, NumericTimeoutGuard) {
        let timers = NumericTimeoutGuard::install();
        Function::new_no_args(include_str!("worker_runtime.js"))
            .call0(&JsValue::UNDEFINED)
            .expect("install guest-only Workers stream fixture");
        let env = Object::new().unchecked_into::<Env>();
        let context_object = Object::new();
        let wait_until = Function::new_with_args("_promise", "return undefined;");
        Reflect::set(
            &context_object,
            &JsValue::from_str("waitUntil"),
            wait_until.as_ref(),
        )
        .expect("install test waitUntil");
        let js_context = context_object.unchecked_into::<WorkerSysContext>();
        (env, Context::new(js_context), timers)
    }

    #[cfg(feature = "test-utils")]
    #[wasm_bindgen_test]
    fn deadline_body_releases_source_when_timeout_is_emitted() {
        assert!(deadline_body_releases_source_for_test());
    }

    #[wasm_bindgen_test]
    async fn dispatch_app_reuses_router_with_fresh_request_bodies() {
        struct TestApp;
        // This fixture deliberately exercises the default Hooks metadata.
        #[expect(
            clippy::missing_trait_methods,
            reason = "exercise default Hooks metadata and construction"
        )]
        impl Hooks for TestApp {
            fn routes() -> Result<RouterService, EdgeError> {
                Ok(build_test_app().router().clone())
            }
        }
        let app = App::build::<TestApp>(edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM)
            .expect("prebuilt application");
        for body in [b"first".as_slice(), b"second".as_slice()] {
            let (env, ctx, _timers) = test_env_ctx();
            let request = cf_request(CfMethod::Post, "/mirror", Some(body));
            let mut response =
                edgezero_adapter_cloudflare::dispatch_app::<TestApp>(&app, request, env, ctx)
                    .await
                    .expect("prebuilt dispatch response");
            assert_eq!(response.status_code(), 200);
            assert_eq!(response.bytes().await.expect("response bytes"), body);
        }
    }

    #[wasm_bindgen_test]
    async fn dispatch_passes_request_body_to_handlers() {
        let app = build_test_app();
        let req = cf_request(CfMethod::Post, "/mirror", Some(b"echo"));
        let (env, ctx, _timers) = test_env_ctx();

        let mut response = CloudflareService::new(&app)
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(response.status_code(), StatusCode::OK.as_u16());
        let bytes = response.bytes().await.expect("bytes");
        assert_eq!(bytes.as_slice(), b"echo");
    }

    #[wasm_bindgen_test]
    async fn dispatch_runs_router_and_returns_response() {
        let app = build_test_app();
        let req = cf_request(CfMethod::Get, "/uri", None);
        let (env, ctx, _timers) = test_env_ctx();

        let mut response = CloudflareService::new(&app)
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(response.status_code(), StatusCode::OK.as_u16());
        let body = response.text().await.expect("text");
        assert_eq!(body, "https://example.com/uri");
    }

    #[wasm_bindgen_test]
    async fn dispatch_streaming_route_preserves_chunks() {
        let app = build_test_app();
        let req = cf_request(CfMethod::Get, "/stream", None);
        let (env, ctx, _timers) = test_env_ctx();

        let mut response = CloudflareService::new(&app)
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(response.status_code(), StatusCode::OK.as_u16());
        let bytes = response.bytes().await.expect("bytes");
        assert_eq!(bytes.as_slice(), b"chunk-1chunk-2");
    }

    #[wasm_bindgen_test]
    async fn into_core_request_preserves_method_uri_headers_body_and_context() {
        let req = cf_request(CfMethod::Post, "/mirror?foo=bar", Some(b"payload"));
        let (env, ctx, _timers) = test_env_ctx();

        let core_request = into_core_request(req, env, ctx)
            .await
            .expect("core request");

        assert_eq!(core_request.method(), &Method::POST);
        assert_eq!(core_request.uri().path(), "/mirror");
        assert_eq!(core_request.uri().query(), Some("foo=bar"));

        let header = core_request
            .headers()
            .get("x-edgezero-test")
            .and_then(|value| value.to_str().ok());
        assert_eq!(header, Some("1"));

        assert!(CloudflareRequestContext::get(&core_request).is_some());

        let body = core_request
            .into_body()
            .into_bytes_bounded(1024)
            .await
            .expect("request body");
        assert_eq!(body.as_ref(), b"payload");
    }

    #[wasm_bindgen_test]
    async fn service_with_config_handle_injects_handle() {
        let app = build_test_app();
        let req = cf_request(CfMethod::Get, "/config-value", None);
        let (env, ctx, _timers) = test_env_ctx();
        let handle = ConfigStoreHandle::new(Arc::new(FixedConfigStore("hello from cf test")));

        let mut response = CloudflareService::new(&app)
            .with_config_handle(handle)
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(response.status_code(), StatusCode::OK.as_u16());
        let body = response.text().await.expect("text");
        assert_eq!(body, "hello from cf test");
    }

    #[wasm_bindgen_test]
    async fn service_with_config_missing_binding_preserves_typed_failure() {
        #[edgezero_core::action]
        async fn read_declared_config(ctx: RequestContext) -> Result<Response, EdgeError> {
            let binding = ctx
                .config_store_default_binding()
                .expect("declared binding must remain registered");
            let error = binding
                .handle
                .get(&binding.default_key)
                .await
                .expect_err("failed opening must not become a missing value");
            assert!(matches!(&error, ConfigStoreError::Unavailable { .. }));

            let clock = ctx.monotonic_clock();
            let bounded_error = binding
                .handle
                .get_bounded(
                    &binding.default_key,
                    &clock,
                    Deadline::at_instant(
                        clock
                            .now()
                            .checked_add(Duration::from_secs(1))
                            .expect("read deadline"),
                    ),
                    1024,
                    1024,
                )
                .await
                .expect_err("bounded reads must preserve the opening failure");
            assert!(matches!(
                bounded_error,
                ConfigStoreError::Unavailable { .. }
            ));

            Err(EdgeError::from(error))
        }

        // The empty environment cannot open this declared binding. It must
        // stay registered with a typed error rather than look unconfigured.
        let app = App::new(
            RouterService::builder()
                .get("/config-value", read_declared_config)
                .build(),
        );
        let req = cf_request(CfMethod::Get, "/config-value", None);
        let (env, ctx, _timers) = test_env_ctx();

        let mut response = CloudflareService::new(&app)
            .with_config("nonexistent_binding")
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(
            response.status_code(),
            StatusCode::SERVICE_UNAVAILABLE.as_u16()
        );
        let bytes = response.bytes().await.expect("default config error body");
        let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(
            payload,
            serde_json::json!({
                "error": {
                    "kind": "service_unavailable",
                    "message": "config store is unavailable",
                    "status": 503_u16
                }
            })
        );
    }

    #[wasm_bindgen_test]
    async fn service_without_config_keeps_registry_absent() {
        let app = build_test_app();
        let req = cf_request(CfMethod::Get, "/has-config", None);
        let (env, ctx, _timers) = test_env_ctx();

        let mut response = CloudflareService::new(&app)
            .dispatch(req, env, ctx)
            .await
            .expect("cf response");

        assert_eq!(response.status_code(), StatusCode::OK.as_u16());
        let body = response.text().await.expect("text");
        assert_eq!(body, "no");
    }

    #[wasm_bindgen_test]
    async fn outbound_clock_controls_preflight_elapsed_and_backwards_failure() {
        use std::collections::VecDeque;
        use std::sync::Mutex;

        fn scripted_clock(script: Vec<MonotonicInstant>) -> MonotonicClock {
            let observations = Arc::new(Mutex::new(VecDeque::from(script)));
            MonotonicClock::new(move || {
                let mut samples = observations.lock().expect("clock observations");
                let now = *samples.front().expect("clock observation");
                if samples.len() > 1 {
                    samples.pop_front();
                }
                now
            })
        }

        let start = MonotonicInstant::now();
        let completed = start
            .checked_add(Duration::from_millis(9))
            .expect("completed instant");
        let forward_client = HttpClient::with_client(CloudflareOutboundClient::with_time(
            preflight_time(scripted_clock(vec![start, completed])),
        ));
        let forward_request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();
        let observation_cutoff = Deadline::at_instant(
            start
                .checked_add(Duration::from_secs(1))
                .expect("cutoff instant"),
        );
        let forward_results = forward_client
            .start_batch_until(vec![forward_request], observation_cutoff)
            .collect()
            .await
            .expect("valid batch driver");
        assert_eq!(
            forward_results.termination(),
            edgezero_core::OutboundBatchTermination::Completed
        );
        assert_eq!(forward_results.slots().len(), 1);
        let forward_result = resolved_slot(
            forward_results
                .slots()
                .first()
                .expect("forward result slot"),
        );
        assert_eq!(forward_result.elapsed, Duration::from_millis(9));
        assert!(matches!(
            forward_result.outcome,
            Err(EdgeError::BadRequest { .. })
        ));

        let earlier = start
            .checked_sub(Duration::from_millis(1))
            .expect("earlier instant");
        let backwards_client = HttpClient::with_client(CloudflareOutboundClient::with_time(
            preflight_time(scripted_clock(vec![start, earlier])),
        ));
        let backwards_request = OutboundRequest::get("https://example.com/")
            .expect("request")
            .stream_response();
        let backwards_results = backwards_client
            .start_batch_until(vec![backwards_request], observation_cutoff)
            .collect()
            .await
            .expect("valid batch driver");
        assert_eq!(
            backwards_results.termination(),
            edgezero_core::OutboundBatchTermination::Completed
        );
        assert_eq!(backwards_results.slots().len(), 1);
        let backwards_result = resolved_slot(
            backwards_results
                .slots()
                .first()
                .expect("backwards result slot"),
        );
        assert_eq!(backwards_result.elapsed, Duration::ZERO);
        assert!(matches!(
            backwards_result.outcome,
            Err(EdgeError::Internal { .. })
        ));
    }

    #[wasm_bindgen_test]
    async fn expired_observation_cutoff_leaves_slot_unresolved() {
        let now = MonotonicInstant::now();
        let client = HttpClient::with_client(CloudflareOutboundClient::with_time(preflight_time(
            MonotonicClock::new(move || now),
        )));
        let request = OutboundRequest::get("https://example.com/").expect("request");

        let results = client
            .start_batch_until(vec![request], Deadline::at_instant(now))
            .collect()
            .await
            .expect("valid batch driver");

        assert_eq!(
            results.termination(),
            edgezero_core::OutboundBatchTermination::Cutoff
        );
        assert!(matches!(
            results.slots().first(),
            Some(OutboundCollectedSlot::Unresolved)
        ));
    }

    #[wasm_bindgen_test]
    async fn batch_preserves_three_preflight_slots_in_input_order() {
        let now = MonotonicInstant::now();
        let client = HttpClient::with_client(CloudflareOutboundClient::with_time(preflight_time(
            MonotonicClock::new(move || now),
        )));
        let streamed_upload = OutboundRequest::post("https://example.com/upload")
            .expect("upload request")
            .body(Body::stream(stream::iter([Bytes::from_static(b"body")])));
        let streamed_response = OutboundRequest::get("https://example.com/stream")
            .expect("stream request")
            .stream_response();
        let method_error = OutboundRequest::get("https://example.com/get")
            .expect("GET request")
            .body(Body::stream(stream::iter([Bytes::new()])));

        let results = client
            .start_batch_until(
                vec![streamed_upload, streamed_response, method_error],
                Deadline::at_instant(
                    now.checked_add(Duration::from_secs(1))
                        .expect("cutoff instant"),
                ),
            )
            .collect()
            .await
            .expect("valid batch driver");
        assert_eq!(
            results.termination(),
            edgezero_core::OutboundBatchTermination::Completed
        );
        let messages: Vec<_> = results
            .slots()
            .iter()
            .map(|slot| match &resolved_slot(slot).outcome {
                Err(EdgeError::BadRequest { message, .. }) => Some(message.as_str()),
                _ => None,
            })
            .collect();

        assert_eq!(
            messages,
            [
                Some(
                    "outbound batches require buffered request bodies; use send for a streamed upload"
                ),
                Some(
                    "outbound batches require buffered responses; use send for a streamed response"
                ),
                Some("outbound batches require buffered request bodies"),
            ]
        );
        assert!(results.slots().iter().all(|slot| {
            matches!(slot, OutboundCollectedSlot::Resolved(result) if result.elapsed == Duration::ZERO)
        }));
    }

    #[wasm_bindgen_test]
    async fn standard_service_installs_the_application_outbound_clock() {
        async fn elapsed(ctx: RequestContext) -> Result<Response, EdgeError> {
            let client = ctx
                .http_client()
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing HTTP client")))?;
            let request =
                OutboundRequest::get("https://example.com/")?.body(Body::from("invalid GET body"));
            let results = client
                .send_all_until(vec![request], Deadline::after(Duration::from_secs(1)))
                .await
                .map_err(|failure| failure.into_parts().0)?;
            let observed_slot = results
                .slots()
                .first()
                .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("missing outbound result")))?;
            let OutboundCollectedSlot::Resolved(result) = observed_slot else {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "unresolved outbound result"
                )));
            };
            if !matches!(&result.outcome, Err(EdgeError::BadRequest { .. })) {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "clock probe did not fail during batch preflight"
                )));
            }
            response_builder()
                .status(StatusCode::NO_CONTENT)
                .header(
                    "x-edgezero-elapsed-ms",
                    result.elapsed.as_millis().to_string(),
                )
                .body(Body::empty())
                .map_err(EdgeError::internal)
        }

        let start = MonotonicInstant::now();
        let completed = start
            .checked_add(Duration::from_millis(7))
            .expect("completed instant");
        let observations = Arc::new(AtomicUsize::new(0));
        let clock_observations = Arc::clone(&observations);
        let mut app = App::new(RouterService::builder().get("/clock", elapsed).build());
        app.set_monotonic_clock(MonotonicClock::new(move || {
            if clock_observations.fetch_add(1, Ordering::SeqCst) < 2 {
                start
            } else {
                completed
            }
        }));
        let req = cf_request(CfMethod::Get, "/clock", None);
        let (env, ctx, _timers) = test_env_ctx();

        let response = CloudflareService::new(&app)
            .dispatch(req, env, ctx)
            .await
            .expect("Cloudflare response");

        assert_eq!(response.status_code(), StatusCode::NO_CONTENT.as_u16());
        assert_eq!(
            response
                .headers()
                .get("x-edgezero-elapsed-ms")
                .expect("elapsed header"),
            Some("7".to_owned())
        );
        assert!(observations.load(Ordering::SeqCst) >= 3);
    }

    #[cfg(feature = "test-utils")]
    #[wasm_bindgen_test]
    async fn deferred_upload_and_response_paths_retain_the_injected_clock() {
        assert!(deferred_clock_paths_hold_for_test().await);
    }

    #[cfg(feature = "test-utils")]
    #[wasm_bindgen_test]
    async fn response_abort_lifecycle_is_enforced() {
        assert!(response_abort_lifecycle_holds_for_test().await);
    }

    #[cfg(feature = "test-utils")]
    mod ingress_contract {
        use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
        use std::future::Future as _;
        use std::io;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;
        use std::time::Duration;

        use edgezero_adapter_cloudflare::request::{
            dispatch_ingress_source_error_for_test, dispatch_ingress_stream_for_test,
            dispatch_ingress_with_for_test,
        };
        use edgezero_core::http::{HeaderMap, HeaderValue};
        use edgezero_core::ingress::{
            AdmissionDecision, BufferedIngressResponse, IngressGrant, IngressHeadLimits,
        };
        use edgezero_core::middleware::{Middleware, Next};
        use edgezero_core::response_egress::{
            DEFAULT_RESPONSE_WRITE_BUDGET, DetachedResponseEgressDecision,
            ResponseEgressCompletion, ResponseEgressEnvelope, ResponseEgressObserver,
            ResponseEgressOutcome, ResponseEgressReport,
        };
        use edgezero_core::router::{RouteMetadata, RouteResolution};
        use futures::future::poll_fn as poll_future;
        use futures::stream::poll_fn;
        use worker::{Delay, Response as CfResponse};

        use super::*;

        #[wasm_bindgen_test]
        async fn admission_abort() {
            let source_calls = Arc::new(AtomicUsize::new(0));
            let observed_source_calls = Arc::clone(&source_calls);
            let delivery_calls = Arc::new(AtomicUsize::new(0));
            let observed_delivery_calls = Arc::clone(&delivery_calls);
            let mut app = App::new(RouterService::builder().build());
            app.set_ingress_admission_policy(|_| AdmissionDecision::Abort);

            let error = dispatch_ingress_with_for_test(
                &app,
                Method::POST,
                "/missing".parse().expect("URI"),
                move || {
                    observed_source_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(stream::empty::<Result<Bytes, io::Error>>())
                },
                move |_response| {
                    observed_delivery_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect_err("abort must return a platform error");

            assert!(error.to_string().contains("ingress admission aborted"));
            assert_eq!(source_calls.load(Ordering::SeqCst), 0);
            assert_eq!(delivery_calls.load(Ordering::SeqCst), 0);
        }

        struct DropSignal(Arc<AtomicUsize>);

        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        struct CountingMiddleware(Arc<AtomicUsize>);

        #[derive(Clone)]
        struct RecordingEgressObserver(Arc<Mutex<Vec<ResponseEgressReport>>>);

        impl ResponseEgressObserver for RecordingEgressObserver {
            fn complete(&self, report: &ResponseEgressReport) {
                self.0.lock().expect("reports lock").push(report.clone());
            }
        }

        #[async_trait::async_trait(?Send)]
        impl Middleware for CountingMiddleware {
            async fn handle(
                &self,
                ctx: RequestContext,
                next: Next<'_>,
            ) -> Result<Response, EdgeError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                next.run(ctx).await
            }
        }

        fn guarded_fallback_app() -> (App, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let handler_calls = Arc::new(AtomicUsize::new(0));
            let handler_counter = Arc::clone(&handler_calls);
            let middleware_calls = Arc::new(AtomicUsize::new(0));
            let router = RouterService::builder()
                .get("/known", move |_ctx: RequestContext| {
                    let request_handler_calls = Arc::clone(&handler_counter);
                    async move {
                        request_handler_calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, EdgeError>("handler must not run")
                    }
                })
                .middleware(CountingMiddleware(Arc::clone(&middleware_calls)))
                .build();
            (App::new(router), handler_calls, middleware_calls)
        }

        fn fallback_app(
            max_body_bytes: usize,
            read_budget: Duration,
            grant_drops: &Arc<AtomicUsize>,
        ) -> (App, Arc<AtomicUsize>, Arc<AtomicUsize>, NumericTimeoutGuard) {
            let timers = NumericTimeoutGuard::install();
            let (mut app, handler_calls, middleware_calls) = guarded_fallback_app();
            let observed_grant_drops = Arc::clone(grant_drops);
            app.set_ingress_admission_policy(move |head| {
                assert!(matches!(
                    head.route_resolution(),
                    RouteResolution::MethodNotAllowed { .. } | RouteResolution::NotFound
                ));
                AdmissionDecision::ReadBodyBeforeFallback {
                    completion: ResponseEgressCompletion::empty(),
                    grant: IngressGrant::new(DropSignal(Arc::clone(&observed_grant_drops))),
                    max_body_bytes,
                    read_deadline: head.read_deadline_after(read_budget),
                    on_exceeded: terminal_response(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "overflow",
                        b"cloudflare overflow\0response",
                    ),
                    on_timeout: terminal_response(
                        StatusCode::GATEWAY_TIMEOUT,
                        "timeout",
                        b"cloudflare timeout response\n",
                    ),
                }
            });
            (app, handler_calls, middleware_calls, timers)
        }

        fn terminal_headers(marker: &'static str) -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-type",
                HeaderValue::from_static("application/octet-stream"),
            );
            headers.insert("x-ingress-terminal", HeaderValue::from_static(marker));
            headers
        }

        fn terminal_response(
            status: StatusCode,
            marker: &'static str,
            body: &'static [u8],
        ) -> BufferedIngressResponse {
            BufferedIngressResponse::new(status, terminal_headers(marker), Bytes::from_static(body))
        }

        fn internal_error_headers() -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert("content-length", HeaderValue::from_static("76"));
            headers.insert("content-type", HeaderValue::from_static("application/json"));
            headers
        }

        fn tracked_stream(
            chunks: Vec<Bytes>,
            grant_drops: &Arc<AtomicUsize>,
            source_drops: &Arc<AtomicUsize>,
            body_polls: &Arc<AtomicUsize>,
        ) -> impl futures::Stream<Item = Result<Bytes, io::Error>> + 'static {
            let observed_grant_drops = Arc::clone(grant_drops);
            let observed_body_polls = Arc::clone(body_polls);
            let source_drop = DropSignal(Arc::clone(source_drops));
            let mut pending_chunks = chunks.into_iter();
            poll_fn(move |_cx| {
                let _keep_source_alive = &source_drop;
                assert_eq!(observed_grant_drops.load(Ordering::SeqCst), 0);
                observed_body_polls.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(pending_chunks.next().map(Ok))
            })
        }

        fn pending_stream(
            grant_drops: &Arc<AtomicUsize>,
            source_drops: &Arc<AtomicUsize>,
            body_polls: &Arc<AtomicUsize>,
        ) -> impl futures::Stream<Item = Result<Bytes, io::Error>> + 'static {
            let observed_grant_drops = Arc::clone(grant_drops);
            let observed_body_polls = Arc::clone(body_polls);
            let source_drop = DropSignal(Arc::clone(source_drops));
            poll_fn(move |_cx| {
                let _keep_source_alive = &source_drop;
                assert_eq!(observed_grant_drops.load(Ordering::SeqCst), 0);
                observed_body_polls.fetch_add(1, Ordering::SeqCst);
                Poll::Pending
            })
        }

        fn error_stream(
            grant_drops: &Arc<AtomicUsize>,
            source_drops: &Arc<AtomicUsize>,
            body_polls: &Arc<AtomicUsize>,
        ) -> impl futures::Stream<Item = Result<Bytes, io::Error>> + 'static {
            let observed_grant_drops = Arc::clone(grant_drops);
            let observed_body_polls = Arc::clone(body_polls);
            let source_drop = DropSignal(Arc::clone(source_drops));
            let mut emitted = false;
            poll_fn(move |_cx| {
                let _keep_source_alive = &source_drop;
                assert_eq!(observed_grant_drops.load(Ordering::SeqCst), 0);
                observed_body_polls.fetch_add(1, Ordering::SeqCst);
                if emitted {
                    Poll::Ready(None)
                } else {
                    emitted = true;
                    Poll::Ready(Some(Err(io::Error::other(
                        "cloudflare ingress source failure",
                    ))))
                }
            })
        }

        fn assert_no_route_dispatch(handler_calls: &AtomicUsize, middleware_calls: &AtomicUsize) {
            assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
            assert_eq!(middleware_calls.load(Ordering::SeqCst), 0);
        }

        #[wasm_bindgen_test]
        async fn normalized_ingress_error_returns_typed_response_without_application_observation() {
            let reports = Arc::new(Mutex::new(Vec::new()));
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let completion_calls = Arc::new(AtomicUsize::new(0));
            let mut app = App::new(RouterService::builder().build());
            app.set_ingress_head_limits(
                IngressHeadLimits::default()
                    .with_max_request_target_bytes(4)
                    .expect("target limit"),
            );
            app.set_response_egress_observer(RecordingEgressObserver(Arc::clone(&reports)));
            let observed_factory_calls = Arc::clone(&factory_calls);
            let observed_completion_calls = Arc::clone(&completion_calls);
            app.set_detached_response_egress_decision_factory(move |head| {
                observed_factory_calls.fetch_add(1, Ordering::SeqCst);
                let terminal_calls = Arc::clone(&observed_completion_calls);
                DetachedResponseEgressDecision::Send {
                    completion: ResponseEgressCompletion::new(move |_report| {
                        terminal_calls.fetch_add(1, Ordering::SeqCst);
                    }),
                    deadline: head.write_deadline_after(DEFAULT_RESPONSE_WRITE_BUDGET),
                }
            });

            let egress = dispatch_ingress_stream_for_test(
                &app,
                Method::GET,
                "/too-long".parse().expect("URI"),
                stream::empty::<Result<Bytes, io::Error>>(),
            )
            .await
            .expect("typed response");
            let response = capture_response(egress);

            assert_eq!(response.status(), StatusCode::URI_TOO_LONG);
            assert!(reports.lock().expect("reports lock").is_empty());
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert_eq!(completion_calls.load(Ordering::SeqCst), 1);
        }

        #[wasm_bindgen_test]
        async fn normalized_ingress_error_decision_abort_uses_provider_error() {
            let (mut app, handler_calls, middleware_calls) = guarded_fallback_app();
            app.set_ingress_head_limits(
                IngressHeadLimits::default()
                    .with_max_request_target_bytes(4)
                    .expect("target limit"),
            );
            let reports = Arc::new(Mutex::new(Vec::new()));
            app.set_response_egress_observer(RecordingEgressObserver(Arc::clone(&reports)));
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let observed_factory_calls = Arc::clone(&factory_calls);
            app.set_detached_response_egress_decision_factory(move |_head| {
                observed_factory_calls.fetch_add(1, Ordering::SeqCst);
                DetachedResponseEgressDecision::Abort
            });
            let source_calls = Arc::new(AtomicUsize::new(0));
            let observed_source_calls = Arc::clone(&source_calls);
            let delivery_calls = Arc::new(AtomicUsize::new(0));
            let observed_delivery_calls = Arc::clone(&delivery_calls);

            let error = dispatch_ingress_with_for_test(
                &app,
                Method::GET,
                "/too-long".parse().expect("URI"),
                move || {
                    observed_source_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(stream::empty::<Result<Bytes, io::Error>>())
                },
                move |_egress| {
                    observed_delivery_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect_err("detached abort must return a platform error");

            assert!(error.to_string().contains("ingress admission aborted"));
            assert_eq!(source_calls.load(Ordering::SeqCst), 0);
            assert_eq!(delivery_calls.load(Ordering::SeqCst), 0);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert!(reports.lock().expect("reports lock").is_empty());
        }

        #[wasm_bindgen_test]
        async fn admission_policy_error_decision_send_uses_detached_completion() {
            let (mut app, handler_calls, middleware_calls) = guarded_fallback_app();
            let decision_completion_calls = Arc::new(AtomicUsize::new(0));
            let decision_resource_drops = Arc::new(AtomicUsize::new(0));
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let observed_decision_completion_calls = Arc::clone(&decision_completion_calls);
            let observed_decision_resource_drops = Arc::clone(&decision_resource_drops);
            let observed_grant_drops = Arc::clone(&grant_drops);
            app.set_ingress_admission_policy(move |head| {
                assert!(matches!(
                    head.route_resolution(),
                    RouteResolution::Matched(_)
                ));
                let decision_resource = DropSignal(Arc::clone(&observed_decision_resource_drops));
                let terminal_calls = Arc::clone(&observed_decision_completion_calls);
                AdmissionDecision::ReadBodyBeforeFallback {
                    completion: ResponseEgressCompletion::new(move |_report| {
                        let _resource = decision_resource;
                        terminal_calls.fetch_add(1, Ordering::SeqCst);
                    }),
                    grant: IngressGrant::new(DropSignal(Arc::clone(&observed_grant_drops))),
                    max_body_bytes: 1,
                    read_deadline: head.read_deadline_after(Duration::from_secs(1)),
                    on_exceeded: BufferedIngressResponse::text(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "too large",
                    ),
                    on_timeout: BufferedIngressResponse::text(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                    ),
                }
            });
            let reports = Arc::new(Mutex::new(Vec::new()));
            app.set_response_egress_observer(RecordingEgressObserver(Arc::clone(&reports)));
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let completion_calls = Arc::new(AtomicUsize::new(0));
            let observed_factory_calls = Arc::clone(&factory_calls);
            let observed_completion_calls = Arc::clone(&completion_calls);
            app.set_detached_response_egress_decision_factory(move |head| {
                observed_factory_calls.fetch_add(1, Ordering::SeqCst);
                let terminal_calls = Arc::clone(&observed_completion_calls);
                DetachedResponseEgressDecision::Send {
                    completion: ResponseEgressCompletion::new(move |_report| {
                        terminal_calls.fetch_add(1, Ordering::SeqCst);
                    }),
                    deadline: head.write_deadline_after(DEFAULT_RESPONSE_WRITE_BUDGET),
                }
            });
            let source_calls = Arc::new(AtomicUsize::new(0));
            let observed_source_calls = Arc::clone(&source_calls);

            let response = dispatch_ingress_with_for_test(
                &app,
                Method::GET,
                "/known".parse().expect("URI"),
                move || {
                    observed_source_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(stream::empty::<Result<Bytes, io::Error>>())
                },
                |egress| Ok(capture_response(egress)),
            )
            .await
            .expect("typed policy error response");

            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(source_calls.load(Ordering::SeqCst), 0);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
            assert_eq!(decision_completion_calls.load(Ordering::SeqCst), 0);
            assert_eq!(decision_resource_drops.load(Ordering::SeqCst), 1);
            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert_eq!(completion_calls.load(Ordering::SeqCst), 1);
            assert!(reports.lock().expect("reports lock").is_empty());
        }

        #[wasm_bindgen_test]
        async fn admission_policy_error_decision_abort_uses_provider_error() {
            let (mut app, handler_calls, middleware_calls) = guarded_fallback_app();
            let decision_resource_drops = Arc::new(AtomicUsize::new(0));
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let observed_decision_resource_drops = Arc::clone(&decision_resource_drops);
            let observed_grant_drops = Arc::clone(&grant_drops);
            app.set_ingress_admission_policy(move |head| {
                AdmissionDecision::ReadBodyBeforeFallback {
                    completion: ResponseEgressCompletion::new({
                        let resource = DropSignal(Arc::clone(&observed_decision_resource_drops));
                        move |_report| drop(resource)
                    }),
                    grant: IngressGrant::new(DropSignal(Arc::clone(&observed_grant_drops))),
                    max_body_bytes: 1,
                    read_deadline: head.read_deadline_after(Duration::from_secs(1)),
                    on_exceeded: BufferedIngressResponse::text(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "too large",
                    ),
                    on_timeout: BufferedIngressResponse::text(
                        StatusCode::REQUEST_TIMEOUT,
                        "timeout",
                    ),
                }
            });
            let reports = Arc::new(Mutex::new(Vec::new()));
            app.set_response_egress_observer(RecordingEgressObserver(Arc::clone(&reports)));
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let observed_factory_calls = Arc::clone(&factory_calls);
            app.set_detached_response_egress_decision_factory(move |_head| {
                observed_factory_calls.fetch_add(1, Ordering::SeqCst);
                DetachedResponseEgressDecision::Abort
            });
            let source_calls = Arc::new(AtomicUsize::new(0));
            let observed_source_calls = Arc::clone(&source_calls);
            let delivery_calls = Arc::new(AtomicUsize::new(0));
            let observed_delivery_calls = Arc::clone(&delivery_calls);

            let error = dispatch_ingress_with_for_test(
                &app,
                Method::GET,
                "/known".parse().expect("URI"),
                move || {
                    observed_source_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(stream::empty::<Result<Bytes, io::Error>>())
                },
                move |_egress| {
                    observed_delivery_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .expect_err("detached abort must return a platform error");

            assert!(error.to_string().contains("ingress admission aborted"));
            assert_eq!(source_calls.load(Ordering::SeqCst), 0);
            assert_eq!(delivery_calls.load(Ordering::SeqCst), 0);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
            assert_eq!(decision_resource_drops.load(Ordering::SeqCst), 1);
            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert!(reports.lock().expect("reports lock").is_empty());
        }

        #[wasm_bindgen_test]
        async fn post_admission_failure_uses_owned_error_egress() {
            let reports = Arc::new(Mutex::new(Vec::new()));
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let observed_grant_drops = Arc::clone(&grant_drops);
            let mut app = App::new(
                RouterService::builder()
                    .get("/owned", |_ctx: RequestContext| async move {
                        Ok::<_, EdgeError>("handler must not run")
                    })
                    .build(),
            );
            app.set_ingress_admission_policy(move |head| AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::empty(),
                grant: IngressGrant::new(DropSignal(Arc::clone(&observed_grant_drops))),
                read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            });
            app.set_response_egress_observer(RecordingEgressObserver(Arc::clone(&reports)));

            let egress = dispatch_ingress_source_error_for_test(
                &app,
                Method::GET,
                "/owned".parse().expect("URI"),
            )
            .await
            .expect("owned error egress");
            let response = capture_response(egress);

            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            let observed = reports.lock().expect("reports lock");
            assert_eq!(observed.len(), 1);
            let report = observed.first().expect("one report");
            assert_eq!(report.outcome, ResponseEgressOutcome::HostHandoff);
            assert_eq!(
                report.route.as_ref().map(RouteMetadata::pattern),
                Some("/owned")
            );
        }

        fn capture_response(envelope: ResponseEgressEnvelope) -> Response {
            let result = envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength);
            assert!(result.is_ok(), "test egress envelope must prepare");
            let Ok((prepared, _policy, mut attempt, clock)) = result else {
                return Response::new(Body::empty());
            };
            assert!(attempt.begin_writing());
            assert!(attempt.terminate(ResponseEgressOutcome::HostHandoff, clock.now()));
            prepared.into_response()
        }

        async fn assert_terminal_response(
            envelope: ResponseEgressEnvelope,
            status: StatusCode,
            expected_headers: HeaderMap,
            body: &[u8],
        ) {
            let response = capture_response(envelope);
            assert_eq!(response.status(), status);
            assert_eq!(response.headers(), &expected_headers);
            let bytes = response
                .into_body()
                .into_bytes_bounded(usize::MAX)
                .await
                .expect("captured body");
            assert_eq!(bytes.as_ref(), body);
        }

        async fn assert_worker_response(
            mut response: CfResponse,
            status: StatusCode,
            expected_headers: HeaderMap,
            body: &[u8],
        ) {
            assert_eq!(response.status_code(), status.as_u16());
            let mut actual_headers = response.headers().entries().collect::<Vec<_>>();
            actual_headers.sort_unstable();
            let mut expected_provider_headers = expected_headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().expect("UTF-8 test header").to_owned(),
                    )
                })
                .collect::<Vec<_>>();
            expected_provider_headers.sort_unstable();
            assert_eq!(actual_headers, expected_provider_headers);
            assert_eq!(response.bytes().await.expect("provider body"), body);
        }

        #[wasm_bindgen_test]
        async fn exact_cap_preserves_not_found_and_method_not_allowed() {
            for (path, expected_status) in [
                ("/missing", StatusCode::NOT_FOUND),
                ("/known", StatusCode::METHOD_NOT_ALLOWED),
            ] {
                let grant_drops = Arc::new(AtomicUsize::new(0));
                let source_drops = Arc::new(AtomicUsize::new(0));
                let body_polls = Arc::new(AtomicUsize::new(0));
                let (app, handler_calls, middleware_calls, _timers) =
                    fallback_app(4, Duration::from_secs(30), &grant_drops);
                let source = tracked_stream(
                    vec![Bytes::from_static(b"ab"), Bytes::from_static(b"cd")],
                    &grant_drops,
                    &source_drops,
                    &body_polls,
                );

                let envelope = dispatch_ingress_stream_for_test(
                    &app,
                    Method::POST,
                    path.parse().expect("URI"),
                    source,
                )
                .await
                .expect("response");
                let response = capture_response(envelope);

                assert_eq!(response.status(), expected_status);
                if expected_status == StatusCode::METHOD_NOT_ALLOWED {
                    assert_eq!(
                        response.headers().get("allow").expect("Allow header"),
                        "GET"
                    );
                }
                assert_eq!(body_polls.load(Ordering::SeqCst), 3);
                assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
                assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                assert_no_route_dispatch(&handler_calls, &middleware_calls);
            }
        }

        #[wasm_bindgen_test]
        async fn cap_plus_one_precedes_not_found_and_method_not_allowed() {
            for path in ["/missing", "/known"] {
                let grant_drops = Arc::new(AtomicUsize::new(0));
                let source_drops = Arc::new(AtomicUsize::new(0));
                let body_polls = Arc::new(AtomicUsize::new(0));
                let (app, handler_calls, middleware_calls, _timers) =
                    fallback_app(4, Duration::from_secs(30), &grant_drops);
                let source = tracked_stream(
                    vec![Bytes::from_static(b"abcd"), Bytes::from_static(b"e")],
                    &grant_drops,
                    &source_drops,
                    &body_polls,
                );

                let response = dispatch_ingress_stream_for_test(
                    &app,
                    Method::POST,
                    path.parse().expect("URI"),
                    source,
                )
                .await
                .expect("response");

                assert_terminal_response(
                    response,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    {
                        let mut headers = terminal_headers("overflow");
                        headers.insert("content-length", HeaderValue::from_static("28"));
                        headers
                    },
                    b"cloudflare overflow\0response",
                )
                .await;
                assert_eq!(body_polls.load(Ordering::SeqCst), 2);
                assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
                assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                assert_no_route_dispatch(&handler_calls, &middleware_calls);
            }
        }

        #[wasm_bindgen_test]
        async fn standard_service_enforces_fallback_overflow() {
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let (app, handler_calls, middleware_calls, _fallback_timers) =
                fallback_app(4, Duration::from_secs(30), &grant_drops);
            let req = cf_request(CfMethod::Post, "/missing", Some(b"abcde"));
            let (env, ctx, _timers) = test_env_ctx();

            let response = CloudflareService::new(&app)
                .dispatch(req, env, ctx)
                .await
                .expect("Cloudflare response");

            assert_worker_response(
                response,
                StatusCode::UNPROCESSABLE_ENTITY,
                {
                    let mut headers = terminal_headers("overflow");
                    headers.insert("content-length", HeaderValue::from_static("28"));
                    headers
                },
                b"cloudflare overflow\0response",
            )
            .await;
            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
        }

        #[wasm_bindgen_test]
        async fn wasm_timer_timeout_preserves_application_response() {
            for path in ["/missing", "/known"] {
                let grant_drops = Arc::new(AtomicUsize::new(0));
                let source_drops = Arc::new(AtomicUsize::new(0));
                let body_polls = Arc::new(AtomicUsize::new(0));
                let (app, handler_calls, middleware_calls, _timers) =
                    fallback_app(4, Duration::from_millis(100), &grant_drops);
                let source = pending_stream(&grant_drops, &source_drops, &body_polls);

                let response = dispatch_ingress_stream_for_test(
                    &app,
                    Method::POST,
                    path.parse().expect("URI"),
                    source,
                )
                .await
                .expect("response");

                assert_terminal_response(
                    response,
                    StatusCode::GATEWAY_TIMEOUT,
                    {
                        let mut headers = terminal_headers("timeout");
                        headers.insert("content-length", HeaderValue::from_static("28"));
                        headers
                    },
                    b"cloudflare timeout response\n",
                )
                .await;
                assert!(body_polls.load(Ordering::SeqCst) > 0);
                assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
                assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                assert_no_route_dispatch(&handler_calls, &middleware_calls);
            }
        }

        #[wasm_bindgen_test]
        async fn saturated_fallback_refuses_without_polling_body() {
            for path in ["/missing", "/known"] {
                let source_drops = Arc::new(AtomicUsize::new(0));
                let body_polls = Arc::new(AtomicUsize::new(0));
                let completion_calls = Arc::new(AtomicUsize::new(0));
                let observed_completion_calls = Arc::clone(&completion_calls);
                let (mut app, handler_calls, middleware_calls) = guarded_fallback_app();
                app.set_ingress_admission_policy(move |head| {
                    assert!(matches!(
                        head.route_resolution(),
                        RouteResolution::MethodNotAllowed { .. } | RouteResolution::NotFound
                    ));
                    AdmissionDecision::Refuse {
                        completion: {
                            let calls = Arc::clone(&observed_completion_calls);
                            ResponseEgressCompletion::new(move |_report| {
                                calls.fetch_add(1, Ordering::SeqCst);
                            })
                        },
                        response: response_builder()
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .header("x-ingress-refusal", "saturated")
                            .body(Body::from("cloudflare unavailable\n"))
                            .expect("refusal response"),
                    }
                });
                let unobserved_grant = Arc::new(AtomicUsize::new(0));
                let source = pending_stream(&unobserved_grant, &source_drops, &body_polls);

                let response = dispatch_ingress_stream_for_test(
                    &app,
                    Method::POST,
                    path.parse().expect("URI"),
                    source,
                )
                .await
                .expect("response");

                assert_terminal_response(
                    response,
                    StatusCode::SERVICE_UNAVAILABLE,
                    {
                        let mut headers = HeaderMap::new();
                        headers.insert("x-ingress-refusal", HeaderValue::from_static("saturated"));
                        headers.insert("content-length", HeaderValue::from_static("23"));
                        headers
                    },
                    b"cloudflare unavailable\n",
                )
                .await;
                assert_eq!(body_polls.load(Ordering::SeqCst), 0);
                assert_eq!(unobserved_grant.load(Ordering::SeqCst), 0);
                assert_eq!(source_drops.load(Ordering::SeqCst), 1);
                assert_eq!(completion_calls.load(Ordering::SeqCst), 1);
                assert_no_route_dispatch(&handler_calls, &middleware_calls);
            }
        }

        #[wasm_bindgen_test]
        async fn source_error_uses_worker_response_boundary_and_releases_lifecycle() {
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let source_drops = Arc::new(AtomicUsize::new(0));
            let body_polls = Arc::new(AtomicUsize::new(0));
            let (app, handler_calls, middleware_calls, _timers) =
                fallback_app(4, Duration::from_secs(30), &grant_drops);
            let source = error_stream(&grant_drops, &source_drops, &body_polls);

            let response = dispatch_ingress_stream_for_test(
                &app,
                Method::POST,
                "/missing".parse().expect("URI"),
                source,
            )
            .await
            .expect("standard error response");

            assert_terminal_response(
                response,
                StatusCode::INTERNAL_SERVER_ERROR,
                internal_error_headers(),
                br#"{"error":{"kind":"internal","message":"internal server error","status":500}}"#,
            )
            .await;
            assert_eq!(body_polls.load(Ordering::SeqCst), 1);
            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            assert_eq!(source_drops.load(Ordering::SeqCst), 1);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
        }

        #[wasm_bindgen_test]
        async fn poll_then_drop_releases_pending_source_and_grant() {
            let grant_drops = Arc::new(AtomicUsize::new(0));
            let source_drops = Arc::new(AtomicUsize::new(0));
            let body_polls = Arc::new(AtomicUsize::new(0));
            let (app, handler_calls, middleware_calls, _timers) =
                fallback_app(4, Duration::from_secs(30), &grant_drops);
            let source = pending_stream(&grant_drops, &source_drops, &body_polls);
            let mut dispatch = Box::pin(dispatch_ingress_stream_for_test(
                &app,
                Method::POST,
                "/missing".parse().expect("URI"),
                source,
            ));

            let first_poll = poll_future(|cx| Poll::Ready(dispatch.as_mut().poll(cx))).await;
            assert!(first_poll.is_pending());
            Delay::from(Duration::ZERO).await;
            let source_poll = poll_future(|cx| Poll::Ready(dispatch.as_mut().poll(cx))).await;
            assert!(source_poll.is_pending());
            assert!(body_polls.load(Ordering::SeqCst) > 0);
            assert_eq!(grant_drops.load(Ordering::SeqCst), 0);
            assert_eq!(source_drops.load(Ordering::SeqCst), 0);

            drop(dispatch);

            assert_eq!(grant_drops.load(Ordering::SeqCst), 1);
            assert_eq!(source_drops.load(Ordering::SeqCst), 1);
            assert_no_route_dispatch(&handler_calls, &middleware_calls);
        }
    }
}

#[cfg(test)]
mod native_tests {
    #[cfg(all(feature = "test-utils", not(target_arch = "wasm32")))]
    mod enabled {
        use bytes::Bytes;
        use edgezero_adapter_cloudflare::outbound::{
            generic_fetch_failure_for_test, timeout_error_for_test, validate_batch_request_for_test,
        };
        use edgezero_core::body::Body;
        use edgezero_core::error::{BadGatewayReason, BudgetSource, EdgeError};
        use edgezero_core::outbound::OutboundRequest;
        use futures_util::stream;
        use std::time::Duration;

        #[test]
        fn image_optimizer_rejection_precedes_other_preflight() {
            let request = OutboundRequest::get("https://example.com/image")
                .unwrap()
                .image_optimizer(edgezero_core::OutboundImageOptimizerOptions::new())
                .body(Body::from_stream(stream::pending()))
                .timeout(Duration::ZERO);
            let error = validate_batch_request_for_test(&request).expect_err("unsupported option");
            assert!(error.message().contains("image_optimizer"));
            assert!(error.message().contains("cloudflare"));
        }

        fn message(result: Result<(), EdgeError>) -> Option<String> {
            if let Err(EdgeError::BadRequest { message, .. }) = result {
                Some(message)
            } else {
                None
            }
        }

        #[test]
        fn buffered_upload_cap_has_typed_preflight_boundaries() {
            for len in [7_usize, 8, 9] {
                let request = OutboundRequest::post("https://example.com/upload")
                    .expect("request")
                    .body(vec![b'x'; len])
                    .max_request_body_bytes(8);
                let result = validate_batch_request_for_test(&request);
                if len > 8 {
                    let error = result.expect_err("overflow before dispatch");
                    assert_eq!(
                        error.request_body_overflow_origin(),
                        Some(edgezero_core::RequestBodyOverflowOrigin::OutboundUpload)
                    );
                } else {
                    result.expect("within cap");
                }
            }
        }

        #[test]
        fn batch_preflight_precedence_and_indices() {
            let streamed_upload = OutboundRequest::post("https://example.com/upload")
                .expect("request")
                .body(Body::stream(stream::iter([Bytes::from_static(b"body")])));
            let streamed_response = OutboundRequest::get("https://example.com/response")
                .expect("request")
                .stream_response();
            let get_stream = OutboundRequest::get("https://example.com/get")
                .expect("request")
                .body(Body::stream(stream::iter([Bytes::new()])));

            assert_eq!(
                [streamed_upload, streamed_response, get_stream]
                    .iter()
                    .map(|request| message(validate_batch_request_for_test(request)))
                    .collect::<Vec<_>>(),
                [
                    Some("outbound batches require buffered request bodies; use send for a streamed upload".to_owned()),
                    Some("outbound batches require buffered responses; use send for a streamed response".to_owned()),
                    Some("outbound batches require buffered request bodies".to_owned()),
                ]
            );
        }

        #[test]
        fn opaque_fetch_rejection_does_not_claim_connection_phase_evidence() {
            let error = generic_fetch_failure_for_test();
            assert!(matches!(
                error,
                EdgeError::BadGateway {
                    reason: BadGatewayReason::Unspecified,
                    ..
                }
            ));
        }

        #[test]
        fn worker_budget_timeouts_preserve_every_selected_source() {
            for selected in [
                BudgetSource::PerCallTimeout,
                BudgetSource::RequestDeadline,
                BudgetSource::Default,
            ] {
                assert!(matches!(
                    timeout_error_for_test(selected),
                    EdgeError::GatewayTimeout { cause, .. } if cause == selected
                ));
            }
        }
    }
}
