#![allow(
    clippy::expect_used,
    clippy::missing_assert_message,
    clippy::unwrap_used,
    reason = "wasm_bindgen_test wrappers are not classified as tests by Clippy"
)]

#[cfg(all(test, feature = "cloudflare", target_arch = "wasm32"))]
mod tests {
    use edgezero_adapter_cloudflare::request::{CloudflareService, into_core_request};
    use edgezero_core::app::App;
    use edgezero_core::http::response_builder;
    use edgezero_core::http::{Method, Request, Response, StatusCode};
    use edgezero_core::ingress::{
        CapturedTarget, MAX_TARGET_BYTES, OriginSource, Preservation, RequestIngress,
        TargetUnavailable,
    };
    use edgezero_core::router::{PreDispatchHook, RouterService};
    use edgezero_core::{Body, EdgeError};
    use std::sync::Arc;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
    use worker::js_sys::{Function, Object, Reflect};
    use worker::wasm_bindgen::{JsCast as _, JsValue};
    use worker::worker_sys::Context as WorkerSysContext;
    use worker::{Context, Env, Request as CfRequest};

    wasm_bindgen_test_configure!(run_in_browser);

    struct IngressHook {
        continue_routing: bool,
    }

    #[async_trait::async_trait(?Send)]
    impl PreDispatchHook for IngressHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            assert!(request.extensions().get::<RequestIngress>().is_some());
            assert_eq!(request.method().as_str(), "EXAMPLE-METHOD");
            if self.continue_routing {
                Ok(None)
            } else {
                Ok(Some(
                    response_builder()
                        .status(418)
                        .header("allow", "POST")
                        .body(Body::empty())
                        .map_err(EdgeError::internal)?,
                ))
            }
        }
    }

    fn test_env_ctx() -> (Env, Context) {
        Function::new_no_args(include_str!("worker_runtime.js"))
            .call0(&JsValue::UNDEFINED)
            .expect("install guest-only Workers fixed-length stream fixture");
        let env = Object::new().unchecked_into::<Env>();
        let object = Object::new();
        let wait_until = Function::new_with_args("_promise", "return undefined;");
        Reflect::set(
            &object,
            &JsValue::from_str("waitUntil"),
            wait_until.as_ref(),
        )
        .expect("install test waitUntil");
        (
            env,
            Context::new(object.unchecked_into::<WorkerSysContext>()),
        )
    }

    #[wasm_bindgen_test]
    async fn ingress_invalid_runtime_header_value_is_a_bad_request() {
        let raw = web_sys::Request::new_with_str("https://example.com/reserved")
            .expect("should construct Web Request");
        raw.headers()
            .append("x-bad", "a\u{1}b")
            .expect("Fetch should accept SOH inside a header value");
        let (env, ctx) = test_env_ctx();
        let error = into_core_request(CfRequest::from(raw), env, ctx)
            .await
            .expect_err("core headers should reject SOH");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.to_string(), "invalid runtime header value");
    }

    #[wasm_bindgen_test]
    async fn ingress_preserves_runtime_extension_methods_and_header_strings() {
        for token in [
            "EXAMPLE-METHOD",
            "example-method",
            "GET",
            "POST",
            "PUT",
            "DELETE",
        ] {
            let init = web_sys::RequestInit::new();
            init.set_method(token);
            let headers = web_sys::Headers::new().expect("should create headers");
            headers
                .append("x-octets", "\u{ff}\u{80}\u{c3}\u{a9}")
                .expect("should append ByteString header");
            init.set_headers_headers(&headers);
            let raw =
                web_sys::Request::new_with_str_and_init("https://example.com/reserved", &init)
                    .expect("should create raw Web Request");
            let expected_method = raw.method();
            let (env, ctx) = test_env_ctx();
            let converted = into_core_request(CfRequest::from(raw), env, ctx)
                .await
                .expect("should convert raw request");
            assert_eq!(converted.method().as_str(), expected_method);
            assert_eq!(
                converted.headers()["x-octets"].as_bytes(),
                "\u{ff}\u{80}\u{c3}\u{a9}".as_bytes()
            );
        }
    }

    #[wasm_bindgen_test]
    async fn ingress_metadata_tracks_runtime_url_origin_and_coalescing() {
        let oversized = format!("https://example.com/{}", "a".repeat(MAX_TARGET_BYTES));
        for url in [
            "https://example.com/reserved?q=a%2Fb",
            "https://example.com/reserved/../ordinary?q=a%2Fb",
            "https://example.com/reserved/%2e%2e/ordinary",
            "https://example.com/reserved//%2F",
            oversized.as_str(),
        ] {
            let raw = web_sys::Request::new_with_str(url).expect("should construct Web Request");
            raw.headers()
                .append("x-repeat", "first")
                .expect("should append header");
            raw.headers()
                .append("x-repeat", "second")
                .expect("should append header");
            raw.headers()
                .set("origin", "https://spoof.example.com")
                .expect("should set spoofed origin");
            raw.headers()
                .set("forwarded", "proto=http;host=spoof.example.com")
                .expect("should set spoofed forwarding header");
            let exposed = raw.url();
            let (env, ctx) = test_env_ctx();
            let converted = into_core_request(CfRequest::from(raw), env, ctx)
                .await
                .expect("should convert Web Request");
            let ingress = converted
                .extensions()
                .get::<RequestIngress>()
                .expect("should insert metadata");
            match ingress.target() {
                CapturedTarget::Complete(target) => {
                    assert_eq!(target.value(), exposed);
                    assert_eq!(target.fidelity(), Preservation::Unknown);
                }
                CapturedTarget::Unavailable(reason) => {
                    assert_eq!(*reason, TargetUnavailable::TooLarge);
                }
            }
            assert_eq!(
                matches!(ingress.target(), CapturedTarget::Complete(_)),
                exposed.len() <= MAX_TARGET_BYTES
            );
            let origin = ingress.origin().expect("should expose runtime origin");
            assert_eq!(origin.scheme(), "https");
            assert_eq!(origin.authority(), "example.com");
            assert_eq!(origin.source(), OriginSource::RuntimeUri);
            assert_eq!(converted.headers().get_all("x-repeat").iter().count(), 1);
            assert_eq!(converted.headers()["x-repeat"], "first, second");
            assert_eq!(
                ingress
                    .header_fidelity(&"x-repeat".parse().expect("should parse name"))
                    .field_multiplicity(),
                Preservation::Unknown
            );
        }
    }

    #[wasm_bindgen_test]
    async fn ingress_non_http_runtime_url_has_no_origin() {
        let raw = web_sys::Request::new_with_str("ftp://example.com/reserved")
            .expect("should construct runtime URL");
        let (env, ctx) = test_env_ctx();
        let request = into_core_request(CfRequest::from(raw), env, ctx)
            .await
            .expect("should convert exposed URI");
        assert!(
            request
                .extensions()
                .get::<RequestIngress>()
                .expect("should insert metadata")
                .origin()
                .is_none()
        );
    }

    #[wasm_bindgen_test]
    async fn ingress_hook_intercepts_and_continues_through_service() {
        for continue_routing in [false, true] {
            let app = App::new(
                RouterService::builder()
                    .pre_dispatch_hook_arc(Arc::new(IngressHook { continue_routing }))
                    .route(
                        "/uri",
                        Method::from_bytes(b"EXAMPLE-METHOD").expect("method"),
                        capture_uri_for_ingress,
                    )
                    .build(),
            );
            let init = web_sys::RequestInit::new();
            init.set_method("EXAMPLE-METHOD");
            let url = if continue_routing {
                "https://example.com/uri"
            } else {
                "https://example.com/missing"
            };
            let raw = web_sys::Request::new_with_str_and_init(url, &init)
                .expect("should create extension request");
            let (env, ctx) = test_env_ctx();
            let mut response = CloudflareService::new(&app)
                .dispatch(CfRequest::from(raw), env, ctx)
                .await
                .expect("should dispatch hook");
            assert_eq!(
                response.status_code(),
                if continue_routing { 200 } else { 418 }
            );
            if continue_routing {
                assert_eq!(response.text().await.expect("continued body"), "continued");
            } else {
                assert_eq!(
                    response
                        .headers()
                        .get("allow")
                        .expect("should read header")
                        .as_deref(),
                    Some("POST")
                );
            }
        }
    }

    #[edgezero_core::action]
    async fn capture_uri_for_ingress() -> &'static str {
        "continued"
    }
}
