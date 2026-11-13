use std::future::Future;
use std::sync::Arc;
use web_time::Instant;

use async_trait::async_trait;

use crate::context::RequestContext;
use crate::error::EdgeError;
use crate::handler::DynHandler;
use crate::http::Response;

pub type BoxMiddleware = Arc<dyn Middleware>;

pub struct FnMiddleware<F>
where
    F: Send + Sync + 'static,
{
    func: F,
}

impl<F> FnMiddleware<F>
where
    F: Send + Sync + 'static,
{
    #[inline]
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

#[async_trait(?Send)]
impl<F, Fut> Middleware for FnMiddleware<F>
where
    F: Fn(RequestContext, Next<'_>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response, EdgeError>>,
{
    #[inline]
    async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
        (self.func)(ctx, next).await
    }
}

#[async_trait(?Send)]
pub trait Middleware: Send + Sync + 'static {
    async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError>;
}

pub struct Next<'mw> {
    handler: &'mw dyn DynHandler,
    middlewares: &'mw [BoxMiddleware],
}

impl<'mw> Next<'mw> {
    #[inline]
    pub fn new(middlewares: &'mw [BoxMiddleware], handler: &'mw dyn DynHandler) -> Self {
        Self {
            handler,
            middlewares,
        }
    }

    /// # Errors
    /// Returns whatever error the next middleware or the final handler produces.
    #[inline]
    pub async fn run(self, ctx: RequestContext) -> Result<Response, EdgeError> {
        if let Some((head, tail)) = self.middlewares.split_first() {
            head.handle(ctx, Next::new(tail, self.handler)).await
        } else {
            self.handler.call(ctx).await
        }
    }
}

pub struct RequestLogger;

#[async_trait(?Send)]
impl Middleware for RequestLogger {
    #[inline]
    async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
        let method = ctx.request().method().clone();
        let path = ctx.request().uri().path().to_owned();
        let start = Instant::now();

        match next.run(ctx).await {
            Ok(response) => {
                let status = response.status();
                let elapsed = start.elapsed().as_millis();
                tracing::info!(
                    "request method={} path={} status={} elapsed_ms={}",
                    method,
                    path,
                    status.as_u16(),
                    elapsed
                );
                Ok(response)
            }
            Err(err) => {
                let status = err.status();
                let message = err.operator_message();
                let elapsed = start.elapsed().as_millis();
                tracing::error!(
                    "request method={} path={} status={} error={} elapsed_ms={}",
                    method,
                    path,
                    status.as_u16(),
                    message,
                    elapsed
                );
                Err(err)
            }
        }
    }
}

#[inline]
pub fn middleware_fn<F, Fut>(func: F) -> FnMiddleware<F>
where
    F: Fn(RequestContext, Next<'_>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Response, EdgeError>>,
{
    FnMiddleware::new(func)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::Body;
    use crate::handler::IntoHandler as _;
    use crate::http::{Method, Response, StatusCode, request_builder};
    use crate::params::PathParams;
    use crate::response::response_with_body;
    use futures::executor::block_on;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    struct RecordingMiddleware {
        log: Arc<Mutex<Vec<String>>>,
        name: &'static str,
    }

    struct ShortCircuit;

    #[async_trait(?Send)]
    impl Middleware for RecordingMiddleware {
        async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
            self.log.lock().unwrap().push(self.name.to_owned());
            next.run(ctx).await
        }
    }

    #[async_trait(?Send)]
    impl Middleware for ShortCircuit {
        async fn handle(
            &self,
            _ctx: RequestContext,
            _next: Next<'_>,
        ) -> Result<Response, EdgeError> {
            response_with_body(StatusCode::UNAUTHORIZED, Body::empty())
        }
    }

    fn empty_context() -> RequestContext {
        let request = request_builder()
            .method(Method::GET)
            .uri("/test")
            .body(Body::empty())
            .expect("request");
        RequestContext::new(request, PathParams::default())
    }

    async fn ok_handler(_ctx: RequestContext) -> Result<Response, EdgeError> {
        response_with_body(StatusCode::OK, Body::empty())
    }

    #[test]
    fn middleware_can_short_circuit() {
        let handler = ok_handler.into_handler();

        let middlewares: Vec<BoxMiddleware> = vec![Arc::new(ShortCircuit)];
        let response = block_on(Next::new(&middlewares, handler.as_ref()).run(empty_context()))
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn middleware_chain_runs_in_order() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let first = RecordingMiddleware {
            log: Arc::clone(&log),
            name: "first",
        };
        let second = RecordingMiddleware {
            log: Arc::clone(&log),
            name: "second",
        };

        let handler = (|_ctx: RequestContext| async move {
            response_with_body(StatusCode::OK, Body::empty())
        })
        .into_handler();

        let middlewares: Vec<BoxMiddleware> = vec![Arc::new(first), Arc::new(second)];

        let result = block_on(Next::new(&middlewares, handler.as_ref()).run(empty_context()))
            .expect("response");
        assert_eq!(result.status(), StatusCode::OK);

        let calls = log.lock().unwrap().clone();
        assert_eq!(calls, vec!["first".to_owned(), "second".to_owned()]);
    }

    #[test]
    fn middleware_fn_executes_closure() {
        let called = Arc::new(AtomicBool::new(false));
        let outer_flag = Arc::clone(&called);
        let middleware = middleware_fn(move |_ctx, _next| {
            let inner_flag = Arc::clone(&outer_flag);
            async move {
                inner_flag.store(true, Ordering::SeqCst);
                response_with_body(StatusCode::OK, Body::empty())
            }
        });

        let handler = ok_handler.into_handler();
        let middlewares: Vec<BoxMiddleware> = vec![Arc::new(middleware)];
        let response = block_on(Next::new(&middlewares, handler.as_ref()).run(empty_context()))
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn next_runs_handler_without_middlewares() {
        let handler = ok_handler.into_handler();
        let response =
            block_on(Next::new(&[], handler.as_ref()).run(empty_context())).expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn request_logger_passes_through_success() {
        let handler = ok_handler.into_handler();
        let response =
            block_on(RequestLogger.handle(empty_context(), Next::new(&[], handler.as_ref())))
                .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn request_logger_emits_approved_operator_diagnostic() {
        use crate::config_diagnostics::{
            ApprovedDiagnostic, ValidationPathSegment, ValidationReport,
        };
        use std::fmt::{Debug, Write as _};
        use std::sync::Mutex;
        use tracing::subscriber::with_default;
        use tracing::{
            Event, Metadata, Subscriber,
            field::{Field, Visit},
            span::{Attributes, Id, Record},
        };

        struct Capture(Arc<Mutex<String>>);
        #[expect(
            clippy::missing_trait_methods,
            reason = "typed value recording defaults route to record_debug"
        )]
        impl Visit for Capture {
            #[expect(
                clippy::use_debug,
                reason = "tracing Visit supplies values through the Debug trait"
            )]
            fn record_debug(&mut self, _field: &Field, value: &dyn Debug) {
                write!(self.0.lock().expect("capture"), "{value:?}")
                    .expect("String writes cannot fail");
            }
        }
        #[expect(
            clippy::missing_trait_methods,
            clippy::arbitrary_source_item_ordering,
            reason = "subscriber methods follow the trait lifecycle; defaults suffice for scoped capture"
        )]
        impl Subscriber for Capture {
            fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
                Id::from_u64(1)
            }
            fn record(&self, _span: &Id, _values: &Record<'_>) {}
            fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
            fn event(&self, event: &Event<'_>) {
                event.record(&mut Self(Arc::clone(&self.0)));
            }
            fn enter(&self, _span: &Id) {}
            fn exit(&self, _span: &Id) {}
        }
        let mut errors = validator::ValidationErrors::new();
        let mut detail = validator::ValidationError::new("unavailable");
        detail.message = Some("UNTRUSTED_LOG_MESSAGE_CANARY".into());
        errors.add("implementation", detail);
        let report = ValidationReport::from_errors(
            &errors,
            &[ApprovedDiagnostic {
                path: &[ValidationPathSegment::Field("implementation")],
                code: "unavailable",
                explanation: "Rebuild CLI and guest with feature bidder-x.",
            }],
        );
        let handler = (move |_ctx: RequestContext| {
            let safe_report = report.clone();
            async move { Err::<Response, EdgeError>(EdgeError::config_validation(safe_report)) }
        })
        .into_handler();
        let output = Arc::new(Mutex::new(String::new()));
        with_default(Capture(Arc::clone(&output)), || {
            let error =
                block_on(RequestLogger.handle(empty_context(), Next::new(&[], handler.as_ref())))
                    .expect_err("refused");
            assert_eq!(error.validation_diagnostics().len(), 1);
        });
        let captured = output.lock().expect("capture");
        assert!(captured.contains("Rebuild CLI and guest with feature bidder-x."));
        assert!(!captured.contains("UNTRUSTED_LOG_MESSAGE_CANARY"));
    }

    #[test]
    fn request_logger_propagates_error() {
        let handler = (|_ctx: RequestContext| async move {
            Err::<Response, EdgeError>(EdgeError::bad_request("boom"))
        })
        .into_handler();
        let err = block_on(RequestLogger.handle(empty_context(), Next::new(&[], handler.as_ref())))
            .expect_err("error");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
