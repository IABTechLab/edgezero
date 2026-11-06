//! Regression coverage for the application boundary in the middleware guide.
#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use edgezero_core::body::Body;
    use edgezero_core::context::RequestContext;
    use edgezero_core::error::EdgeError;
    use edgezero_core::http::{Method, Request, Response, StatusCode, request_builder};
    use edgezero_core::middleware::{Middleware, Next, RequestTimingMiddleware};
    use edgezero_core::request_timing::RequestTimings;
    use edgezero_core::response::response_with_body;
    use edgezero_core::router::{PreDispatchHook, RouterService};
    use futures::executor::block_on;
    use futures::stream;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type Handle = RequestTimings<1>;

    // Keep the installation logic in sync with docs/guide/middleware.md.
    // The observer is test-only: unmatched requests do not return their extensions.
    struct TimingHook {
        observed: Arc<Mutex<Vec<Handle>>>,
    }

    #[async_trait(?Send)]
    impl PreDispatchHook for TimingHook {
        async fn handle(&self, request: &mut Request) -> Result<Option<Response>, EdgeError> {
            let existing = request.extensions().get::<Handle>().cloned();
            let timings = existing.unwrap_or_else(Handle::new);
            request.extensions_mut().insert(timings.clone());
            self.observed.lock().unwrap().push(timings);
            Ok(None)
        }
    }

    // Keep this application-owned example in sync with docs/guide/middleware.md.
    struct TimingBoundary;

    #[async_trait(?Send)]
    impl Middleware for TimingBoundary {
        #[expect(
            clippy::let_underscore_untyped,
            clippy::let_underscore_must_use,
            reason = "keep the compiled fixture identical to the guide's best-effort example"
        )]
        async fn handle(
            &self,
            mut ctx: RequestContext,
            next: Next<'_>,
        ) -> Result<Response, EdgeError> {
            let existing = ctx.request().extensions().get::<Handle>().cloned();
            let timings = existing.unwrap_or_else(Handle::new);
            ctx.request_mut().extensions_mut().insert(timings.clone());
            let result = next.run(ctx).await;
            let _ = timings.mark_headers_ready();
            result
        }
    }

    #[edgezero_core::action]
    async fn success(ctx: RequestContext) -> Result<Response, EdgeError> {
        let timings = ctx.request().extensions().get::<Handle>().unwrap().clone();
        timings.record(0, Duration::from_millis(3)).unwrap();
        let mut response = response_with_body(StatusCode::OK, Body::empty())?;
        // Test-only observation: the attachment middleware does not propagate handles.
        response.extensions_mut().insert(timings);
        Ok(response)
    }

    #[edgezero_core::action]
    async fn failure(_ctx: RequestContext) -> Result<Response, EdgeError> {
        Err(EdgeError::bad_request("boundary error"))
    }

    #[edgezero_core::action]
    async fn streaming(_ctx: RequestContext) -> Result<Response, EdgeError> {
        response_with_body(StatusCode::OK, Body::stream(stream::pending()))
    }

    fn router() -> RouterService {
        RouterService::builder()
            .middleware(TimingBoundary)
            .middleware(RequestTimingMiddleware::<1>::new().with_excluded_paths(&["/health"]))
            .get("/timed", success)
            .get("/health", success)
            .get("/error", failure)
            .get("/stream", streaming)
            .build()
    }

    fn request(method: Method, uri: &str, preinstalled: Option<&Handle>) -> Request {
        let mut request = request_builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        if let Some(timings) = preinstalled {
            request.extensions_mut().insert(timings.clone());
        }
        request
    }

    #[test]
    fn boundary_preserves_shared_handle_and_marks_success_and_error_results() {
        let router = router();
        for (uri, status, phase) in [
            ("/timed", StatusCode::OK, 10),
            ("/error", StatusCode::BAD_REQUEST, 7),
        ] {
            let timings = Handle::new();
            timings.record(0, Duration::from_millis(7)).unwrap();
            let before = timings.elapsed();
            let response =
                block_on(router.oneshot(request(Method::GET, uri, Some(&timings)))).unwrap();
            assert_eq!(response.status(), status);
            assert!(!response.headers().contains_key("server-timing"));
            if uri == "/error" {
                assert!(response.extensions().get::<Handle>().is_none());
            }
            timings
                .snapshot(|view| {
                    assert_eq!(view.phases, [Some(Duration::from_millis(phase))]);
                    assert!(view.headers_ready.unwrap() >= before);
                    assert_eq!(view.request_complete, None);
                })
                .unwrap();
        }
    }

    #[test]
    fn boundary_creates_fresh_handles_even_on_attachment_excluded_paths() {
        let router = router();
        let mut handles = Vec::new();
        for uri in ["/timed", "/health?check=1"] {
            let response = block_on(router.oneshot(request(Method::GET, uri, None))).unwrap();
            let timings = response.extensions().get::<Handle>().unwrap().clone();
            assert!(
                timings
                    .snapshot(|view| view.headers_ready.is_some())
                    .unwrap()
            );
            handles.push(timings);
        }
        handles[0].record(0, Duration::from_millis(5)).unwrap();
        assert_eq!(
            handles[0].snapshot(|view| view.phases).unwrap(),
            [Some(Duration::from_millis(8))]
        );
        assert_eq!(
            handles[1].snapshot(|view| view.phases).unwrap(),
            [Some(Duration::from_millis(3))]
        );
    }

    #[test]
    fn hook_installs_independent_handles_before_lookup_without_lifecycle_marks() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let router = RouterService::builder()
            .pre_dispatch_hook(TimingHook {
                observed: Arc::clone(&observed),
            })
            .middleware(RequestTimingMiddleware::<1>::new().with_excluded_paths(&["/health"]))
            .get("/timed", success)
            .get("/health", success)
            .build();
        for (method, uri, status) in [
            (Method::GET, "/timed", StatusCode::OK),
            (Method::GET, "/missing", StatusCode::NOT_FOUND),
            (Method::POST, "/timed", StatusCode::METHOD_NOT_ALLOWED),
            (Method::GET, "/health?check=1", StatusCode::OK),
        ] {
            let response = block_on(router.oneshot(request(method, uri, None))).unwrap();
            assert_eq!(response.status(), status);
            let timings = observed.lock().unwrap().last().unwrap().clone();
            timings
                .snapshot(|view| {
                    let phase = (status == StatusCode::OK).then_some(Duration::from_millis(3));
                    assert_eq!(view.phases, [phase]);
                    assert_eq!(view.headers_ready, None);
                    assert_eq!(view.request_complete, None);
                    assert_eq!(view.response_bytes, None);
                })
                .unwrap();
        }
        let handles = observed.lock().unwrap().clone();
        assert_eq!(handles.len(), 4);
        handles[0].record(0, Duration::from_millis(5)).unwrap();
        for (timings, expected) in handles.iter().zip([Some(8), None, None, Some(3)]) {
            assert_eq!(
                timings.snapshot(|view| view.phases).unwrap(),
                [expected.map(Duration::from_millis)]
            );
        }
    }

    #[test]
    fn hook_and_boundary_preserve_preinstalled_handles_and_result_limits() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let router = RouterService::builder()
            .pre_dispatch_hook(TimingHook {
                observed: Arc::clone(&observed),
            })
            .middleware(TimingBoundary)
            .middleware(RequestTimingMiddleware::<1>::new().with_excluded_paths(&["/health"]))
            .get("/timed", success)
            .get("/health", success)
            .get("/error", failure)
            .get("/stream", streaming)
            .build();
        for (method, uri, status, marked, phase) in [
            (Method::GET, "/timed", StatusCode::OK, true, 10),
            (Method::GET, "/health?check=1", StatusCode::OK, true, 10),
            (Method::GET, "/error", StatusCode::BAD_REQUEST, true, 7),
            (Method::GET, "/stream", StatusCode::OK, true, 7),
            (Method::GET, "/missing", StatusCode::NOT_FOUND, false, 7),
            (
                Method::POST,
                "/timed",
                StatusCode::METHOD_NOT_ALLOWED,
                false,
                7,
            ),
        ] {
            let timings = Handle::new();
            timings.record(0, Duration::from_millis(7)).unwrap();
            let before = timings.elapsed();
            let response = block_on(router.oneshot(request(method, uri, Some(&timings)))).unwrap();
            assert_eq!(response.status(), status);
            if uri == "/stream" {
                assert!(response.body().is_stream());
            }
            timings
                .snapshot(|view| {
                    assert_eq!(view.phases, [Some(Duration::from_millis(phase))]);
                    assert_eq!(view.headers_ready.is_some(), marked);
                    if let Some(headers_ready) = view.headers_ready {
                        assert!(headers_ready >= before);
                    }
                    assert_eq!(view.request_complete, None);
                    assert_eq!(view.response_bytes, None);
                })
                .unwrap();
            // Mutation through the hook's clone proves shared identity. The origin
            // is immutable within that handle, so neither layer can reset it.
            let installed = observed.lock().unwrap().last().unwrap().clone();
            installed.record(0, Duration::from_millis(2)).unwrap();
            assert_eq!(
                timings.snapshot(|view| view.phases).unwrap(),
                [Some(Duration::from_millis(phase + 2))]
            );
        }
    }

    #[test]
    fn boundary_skips_unmatched_routes_and_does_not_consume_streamed_bodies() {
        let router = router();
        for (method, uri, status) in [
            (Method::GET, "/missing", StatusCode::NOT_FOUND),
            (Method::POST, "/timed", StatusCode::METHOD_NOT_ALLOWED),
        ] {
            let timings = Handle::new();
            let response = block_on(router.oneshot(request(method, uri, Some(&timings)))).unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(timings.snapshot(|view| view.headers_ready).unwrap(), None);
        }
        let timings = Handle::new();
        let response =
            block_on(router.oneshot(request(Method::GET, "/stream", Some(&timings)))).unwrap();
        assert!(response.body().is_stream());
        timings
            .snapshot(|view| {
                assert!(view.headers_ready.is_some());
                assert_eq!(view.request_complete, None);
                assert_eq!(view.response_bytes, None);
            })
            .unwrap();
    }
}
