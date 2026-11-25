#![cfg(all(feature = "fastly", target_arch = "wasm32"))]

// Compile-time check: FastlySecretStore implements SecretStore.
mod secret_store_compile_check {
    use edgezero_adapter_fastly::secret_store::FastlySecretStore;
    use edgezero_core::secret_store::SecretStore;

    fn assert_provider_impl<T>()
    where
        T: SecretStore,
    {
    }

    // Anonymous const whose initializer is a never-called fn pointer; the
    // type bound is checked at type-check time.
    const _: fn() = assert_provider_impl::<FastlySecretStore>;
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use edgezero_adapter_fastly::context::FastlyRequestContext;
    use edgezero_adapter_fastly::request::{
        FastlyService, into_core_request, into_core_request_with_registries,
    };
    use edgezero_adapter_fastly::response::from_core_response;
    use edgezero_core::app::App;
    use edgezero_core::body::Body;
    use edgezero_core::config_store::{ConfigStore, ConfigStoreError, ConfigStoreHandle};
    use edgezero_core::context::RequestContext;
    use edgezero_core::delivery_compression::{
        DeliveryCompressionIntent, DeliveryCompressionSupport,
    };
    use edgezero_core::error::EdgeError;
    use edgezero_core::http::{Method, Response, StatusCode, response_builder};
    use edgezero_core::router::RouterService;
    use fastly::Request as FastlyRequest;
    use fastly::http::{Method as FastlyMethod, StatusCode as FastlyStatus};
    use futures::executor::block_on;
    use futures::stream;
    use std::sync::Arc;

    struct FixedConfigStore(&'static str);

    #[async_trait::async_trait(?Send)]
    impl ConfigStore for FixedConfigStore {
        async fn get(&self, _key: &str) -> Result<Option<String>, ConfigStoreError> {
            Ok(Some(self.0.to_owned()))
        }
    }

    fn build_test_app() -> App {
        async fn capture_uri(ctx: RequestContext) -> Result<Response, EdgeError> {
            let body = Body::text(ctx.request().uri().to_string());
            let response = response_builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("response");
            Ok(response)
        }

        async fn mirror_body(ctx: RequestContext) -> Result<Response, EdgeError> {
            let bytes = ctx.request().body().as_bytes().expect("buffered").to_vec();
            let response = response_builder()
                .status(StatusCode::OK)
                .body(Body::from(bytes))
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
            // gone. The dispatch boundary now synthesises a one-id
            // `ConfigRegistry` from the wired `ConfigStoreHandle`.
            let value = match ctx.config_store_default() {
                Some(store) => store
                    .get("greeting")
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| "missing".to_owned()),
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
            .get("/config", config_value)
            .build();

        App::new(router)
    }

    fn fastly_request(method: FastlyMethod, path: &str, body: Option<&[u8]>) -> FastlyRequest {
        // Viceroy validates Fastly request URLs at construction time, so the
        // contract tests must use absolute URLs instead of path-only strings.
        let mut req = FastlyRequest::new(method, format!("http://example.com{path}"));
        req.set_header("host", "example.com");
        req.set_header("x-edgezero-test", "1");
        if let Some(bytes) = body {
            req.set_body(bytes.to_vec());
        }
        req
    }

    #[test]
    fn into_core_request_preserves_method_uri_headers_body_and_context() {
        let req = fastly_request(FastlyMethod::POST, "/mirror?foo=bar", Some(b"payload"));
        let expected_ip = req.get_client_ip_addr();

        let core_request = into_core_request(req).expect("core request");

        assert_eq!(
            DeliveryCompressionSupport::from_request(&core_request),
            DeliveryCompressionSupport::GzipBrotli
        );
        assert_eq!(core_request.method(), &Method::POST);
        assert_eq!(core_request.uri().path(), "/mirror");
        assert_eq!(core_request.uri().query(), Some("foo=bar"));

        let headers = core_request.headers();
        assert_eq!(
            headers
                .get("x-edgezero-test")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );

        assert_eq!(
            core_request.body().as_bytes().expect("buffered"),
            b"payload"
        );

        let context = FastlyRequestContext::get(&core_request).expect("context");
        assert_eq!(context.client_ip, expected_ip);
    }

    #[test]
    fn registry_conversion_preserves_extensions_without_collecting_a_response() {
        use edgezero_core::app::{StoreMetadata, StoresMetadata};
        use edgezero_core::store_registry::SecretRegistry;
        let request = fastly_request(FastlyMethod::GET, "/stream", None);
        let core = into_core_request_with_registries(
            request,
            StoresMetadata {
                secrets: Some(StoreMetadata {
                    default: "fixture",
                    ids: &["fixture"],
                }),
                ..Default::default()
            },
            |raw, extensions| {
                extensions.insert(raw.get_url_str().to_owned());
            },
        )
        .expect("registry-aware conversion");
        assert_eq!(
            DeliveryCompressionSupport::from_request(&core),
            DeliveryCompressionSupport::GzipBrotli
        );
        assert!(core.extensions().get::<SecretRegistry>().is_some());
        assert_eq!(
            core.extensions().get::<String>().unwrap(),
            "http://example.com/stream"
        );
        let response = block_on(build_test_app().router().oneshot(core)).unwrap();
        assert!(matches!(response.body(), Body::Stream(_)));
    }

    #[test]
    fn from_core_response_translates_status_headers_and_streaming_body() {
        let response = response_builder()
            .status(StatusCode::CREATED)
            .header("x-edgezero-res", "1")
            .body(Body::stream(stream::iter(vec![
                Bytes::from_static(b"hello"),
                Bytes::from_static(b" "),
                Bytes::from_static(b"world"),
            ])))
            .expect("response");

        let mut fastly_response = from_core_response(response).expect("fastly response");

        assert_eq!(fastly_response.get_status(), FastlyStatus::CREATED);
        assert!(fastly_response.get_header("x-edgezero-res").is_some());
        assert_eq!(fastly_response.take_body_bytes(), b"hello world");
    }

    #[test]
    fn delivery_compression_standard_converter_finalizes_once_and_stream() {
        for streaming in [false, true] {
            let body = if streaming {
                Body::stream(stream::iter(vec![
                    Bytes::from_static(b"identity "),
                    Bytes::from_static(b"output"),
                ]))
            } else {
                Body::text("identity output")
            };
            let mut response = response_builder()
                .status(StatusCode::CREATED)
                .header("set-cookie", "a=1")
                .header("set-cookie", "b=2")
                .header("cache-control", "private, no-store")
                .header("etag", "\"keep\"")
                .header("vary", "Origin")
                .body(body)
                .unwrap();
            DeliveryCompressionSupport::GzipBrotli
                .mark_identity_response(&mut response)
                .unwrap();
            // Late header effects do not change the identity-producing body.
            response
                .headers_mut()
                .insert("content-encoding", "gzip".parse().unwrap());
            response
                .headers_mut()
                .insert("content-length", "9999".parse().unwrap());
            response
                .headers_mut()
                .insert("x-compress-hint", "off".parse().unwrap());
            let mut native = from_core_response(response).unwrap();
            assert_eq!(native.get_status(), FastlyStatus::CREATED);
            assert_eq!(native.get_header_str("x-compress-hint"), Some("on"));
            assert!(native.get_header("content-encoding").is_none());
            assert!(native.get_header("content-length").is_none());
            assert_eq!(
                native.get_header_str("cache-control"),
                Some("private, no-store")
            );
            assert_eq!(native.get_header_str("etag"), Some("\"keep\""));
            assert_eq!(
                native
                    .get_header_all("set-cookie")
                    .map(|value| value.to_str().unwrap())
                    .collect::<Vec<_>>(),
                vec!["a=1", "b=2"]
            );
            assert_eq!(
                native
                    .get_header_all("vary")
                    .map(|value| value.to_str().unwrap())
                    .collect::<Vec<_>>(),
                vec!["Origin", "Accept-Encoding"]
            );
            assert_eq!(native.take_body_bytes(), b"identity output");
        }
    }

    #[test]
    fn delivery_compression_unmarked_gzip_survives_standard_conversion() {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write as _;
        let mut compressor = GzEncoder::new(Vec::new(), Compression::default());
        compressor.write_all(b"encoded output").unwrap();
        let encoded = compressor.finish().unwrap();
        for streaming in [false, true] {
            let body = if streaming {
                Body::stream(stream::iter(vec![Bytes::from(encoded.clone())]))
            } else {
                Body::from(encoded.clone())
            };
            let response = response_builder()
                .header("content-encoding", "gzip")
                .header("content-length", encoded.len())
                .header("vary", "Origin")
                .body(body)
                .unwrap();
            let mut native = from_core_response(response).unwrap();
            assert_eq!(native.get_header_str("content-encoding"), Some("gzip"));
            assert_eq!(
                native.get_header_str("content-length"),
                Some(encoded.len().to_string().as_str())
            );
            assert_eq!(native.get_header_str("vary"), Some("Origin"));
            assert!(native.get_header("x-compress-hint").is_none());
            assert_eq!(native.take_body_bytes(), encoded);
        }
    }

    #[test]
    fn delivery_compression_stream_errors_keep_existing_propagation() {
        use std::io::Error as IoError;
        let body = Body::from_stream(stream::iter(vec![Err::<Bytes, _>(IoError::other(
            "delivery source failed",
        ))]));
        let mut response = Response::new(body);
        DeliveryCompressionSupport::GzipBrotli
            .mark_identity_response(&mut response)
            .unwrap();
        let error = from_core_response(response).expect_err("source failure must propagate");
        assert!(error.to_string().contains("delivery source failed"));
    }

    #[test]
    fn delivery_compression_dispatch_exposes_support_before_output_selection() {
        use edgezero_adapter_fastly::request::dispatch_with_registries;
        use edgezero_core::app::StoresMetadata;

        #[edgezero_core::action]
        async fn deliver(ctx: RequestContext) -> Result<Response, EdgeError> {
            let support = DeliveryCompressionSupport::from_request(ctx.request());
            assert_eq!(support, DeliveryCompressionSupport::GzipBrotli);
            let mut response = Response::new(Body::text("identity output"));
            support.mark_identity_response(&mut response)?;
            Ok(response)
        }
        let app = App::new(RouterService::builder().get("/delivery", deliver).build());
        let mut service_response = FastlyService::new(&app)
            .dispatch(fastly_request(FastlyMethod::GET, "/delivery", None))
            .unwrap();
        assert_eq!(
            service_response.get_header_str("x-compress-hint"),
            Some("on")
        );
        assert_eq!(service_response.take_body_bytes(), b"identity output");
        let mut registry_response = dispatch_with_registries(
            &app,
            fastly_request(FastlyMethod::GET, "/delivery", None),
            StoresMetadata::default(),
            |_, _| {},
        )
        .unwrap();
        assert_eq!(
            registry_response.get_header_str("x-compress-hint"),
            Some("on")
        );
        assert_eq!(registry_response.take_body_bytes(), b"identity output");
    }

    #[test]
    fn delivery_compression_custom_finalizer_does_not_poll_before_parts_split() {
        use edgezero_adapter_fastly::delivery_compression::finalize_response;
        use std::cell::Cell;
        use std::rc::Rc;
        use std::task::Poll;
        let request =
            into_core_request(fastly_request(FastlyMethod::GET, "/delivery", None)).unwrap();
        let support = DeliveryCompressionSupport::from_request(&request);
        assert_eq!(support, DeliveryCompressionSupport::GzipBrotli);
        let polls = Rc::new(Cell::new(0_usize));
        let observed = Rc::clone(&polls);
        let body = Body::stream(stream::poll_fn(move |_| {
            observed.set(observed.get() + 1);
            Poll::<Option<Bytes>>::Pending
        }));
        let mut response = Response::new(body);
        support.mark_identity_response(&mut response).unwrap();
        response
            .headers_mut()
            .insert("content-length", "9999".parse().unwrap());
        finalize_response(&mut response);
        assert_eq!(polls.get(), 0);
        let (parts, output_body) = response.into_parts();
        assert!(
            parts
                .extensions
                .get::<DeliveryCompressionIntent>()
                .is_some()
        );
        assert_eq!(parts.headers["x-compress-hint"], "on");
        assert!(!parts.headers.contains_key("content-length"));
        assert!(output_body.is_stream());
    }

    #[test]
    fn dispatch_runs_router_and_returns_response() {
        let app = build_test_app();
        let req = fastly_request(FastlyMethod::GET, "/uri", None);

        let mut response = FastlyService::new(&app)
            .dispatch(req)
            .expect("fastly response");

        assert_eq!(response.get_status(), FastlyStatus::OK);
        assert_eq!(response.take_body_bytes(), b"http://example.com/uri");
    }

    #[test]
    fn dispatch_streaming_route_preserves_chunks() {
        let app = build_test_app();
        let req = fastly_request(FastlyMethod::GET, "/stream", None);

        let mut response = FastlyService::new(&app)
            .dispatch(req)
            .expect("fastly response");

        assert_eq!(response.get_status(), FastlyStatus::OK);
        assert_eq!(response.take_body_bytes(), b"chunk-1chunk-2");
    }

    #[test]
    fn dispatch_passes_request_body_to_handlers() {
        let app = build_test_app();
        let req = fastly_request(FastlyMethod::POST, "/mirror", Some(b"echo"));

        let mut response = FastlyService::new(&app)
            .dispatch(req)
            .expect("fastly response");

        assert_eq!(response.get_status(), FastlyStatus::OK);
        assert_eq!(response.take_body_bytes(), b"echo");
    }

    #[test]
    fn service_with_config_handle_injects_handle() {
        let app = build_test_app();
        let req = fastly_request(FastlyMethod::GET, "/config", None);
        let handle = ConfigStoreHandle::new(Arc::new(FixedConfigStore("hello from fastly test")));

        let mut response = FastlyService::new(&app)
            .with_config_handle(handle)
            .dispatch(req)
            .expect("fastly response");

        assert_eq!(response.get_status(), FastlyStatus::OK);
        assert_eq!(response.take_body_bytes(), b"hello from fastly test");
    }
}
