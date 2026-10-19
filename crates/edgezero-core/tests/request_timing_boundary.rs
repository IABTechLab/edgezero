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
    use edgezero_core::router::RouterService;
    use futures::executor::block_on;
    use futures::stream;
    use std::time::Duration;

    type Handle = RequestTimings<1>;

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
            let existing = ctx.extensions().get::<Handle>().cloned();
            let timings =
                existing.unwrap_or_else(|| Handle::with_data_and_clock((), ctx.monotonic_clock()));
            ctx.extensions_mut().insert(timings.clone());
            let result = next.run(ctx).await;
            let _ = timings.mark_headers_ready();
            result
        }
    }

    #[edgezero_core::action]
    async fn success(ctx: RequestContext) -> Result<Response, EdgeError> {
        let timings = ctx.extensions().get::<Handle>().unwrap().clone();
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
    fn application_clock_drives_marks_and_spans_without_taking_completion_ownership() {
        use edgezero_core::app::App;
        use edgezero_core::ingress::{
            AdmissionDecision, IngressDispatchOutcome, IngressFraming, IngressGrant,
            IngressHeadAccounting,
        };
        use edgezero_core::{MonotonicClock, MonotonicInstant, ResponseEgressCompletion};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};

        let origin = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(origin));
        let observed = Arc::new(Mutex::new(None::<Handle>));
        let handler_now = Arc::clone(&now);
        let handler_observed = Arc::clone(&observed);
        let router = RouterService::builder()
            .middleware(RequestTimingMiddleware::<1>::new())
            .get("/", move |ctx: RequestContext| {
                let handler_time = Arc::clone(&handler_now);
                let handler_capture = Arc::clone(&handler_observed);
                async move {
                    let timings = ctx.extensions().get::<Handle>().unwrap().clone();
                    assert_eq!(timings.elapsed(), Duration::ZERO);
                    let span = timings.span(0).unwrap();
                    *handler_time.lock().unwrap() = origin + Duration::from_millis(7);
                    drop(span);
                    timings.mark_headers_ready().unwrap();
                    *handler_capture.lock().unwrap() = Some(timings);
                    response_with_body(StatusCode::OK, Body::stream(stream::pending()))
                }
            })
            .build();
        let mut app = App::new(router);
        let source = Arc::clone(&now);
        app.set_monotonic_clock(MonotonicClock::new(move || *source.lock().unwrap()));
        let completions = Arc::new(AtomicUsize::new(0));
        let owner = Arc::clone(&completions);
        app.set_ingress_admission_policy(move |head| AdmissionDecision::Admit {
            completion: ResponseEgressCompletion::new({
                let calls = Arc::clone(&owner);
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                }
            }),
            grant: IngressGrant::empty(),
            read_deadline: head.read_deadline_after(Duration::from_secs(1)),
        });
        let outcome = block_on(app.dispatch_ingress(
            request(Method::GET, "/", None),
            origin,
            IngressHeadAccounting::HostManaged,
            IngressFraming::HostManaged,
        ))
        .unwrap();
        let IngressDispatchOutcome::Response(envelope) = outcome else {
            panic!("response");
        };
        let Ok((_body, _policy, mut attempt, clock)) = envelope.begin() else {
            panic!("egress");
        };
        assert!(attempt.begin_writing());
        assert!(attempt.complete(clock.now()));
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        let timings = observed.lock().unwrap().clone().unwrap();
        timings
            .snapshot(|view| {
                assert_eq!(view.elapsed, Duration::from_millis(7));
                assert_eq!(view.phases, [Some(Duration::from_millis(7))]);
                assert_eq!(view.headers_ready, Some(Duration::from_millis(7)));
                assert_eq!(view.request_complete, None);
            })
            .unwrap();
        *now.lock().unwrap() = origin + Duration::from_millis(11);
        timings.mark_request_complete().unwrap();
        assert_eq!(
            timings.snapshot(|view| view.request_complete).unwrap(),
            Some(Duration::from_millis(11))
        );
        assert_eq!(completions.load(Ordering::SeqCst), 1);
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
