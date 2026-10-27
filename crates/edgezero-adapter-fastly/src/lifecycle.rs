//! Lazy, successful-only state retention for custom Fastly dispatch.
//!
//! Only application-owned state belongs here. Native request handles, bodies,
//! metadata, extensions, and pending work must remain local to each callback.

#[cfg(any(feature = "fastly", test))]
use crate::outbound::scope::Guard as OutboundScope;
#[cfg(feature = "fastly")]
use fastly::http::serve::{HandlerResult, Serve, ServeSummary};

/// State owned by one invocation of a custom serving entry point.
///
/// A new sandbox can start at any request. Initialization must therefore be
/// repeatable; retaining state never guarantees its lifetime or persistence.
pub struct Sandbox<T> {
    initialization_attempts: u64,
    requests: u64,
    setup_complete: bool,
    state: Option<T>,
}

#[cfg(any(feature = "fastly", test))]
struct DeliveryResult<E>(Result<(), E>);

#[cfg(feature = "fastly")]
impl<E> HandlerResult for DeliveryResult<E> {
    type Error = E;

    fn send(self) -> Result<(), E> {
        self.0
    }
}

impl<T> Default for Sandbox<T> {
    #[inline]
    fn default() -> Self {
        Self {
            state: None,
            requests: 0,
            initialization_attempts: 0,
            setup_complete: false,
        }
    }
}

#[expect(
    clippy::arbitrary_source_item_ordering,
    reason = "group accessors before lifecycle operations"
)]
impl<T> Sandbox<T> {
    /// Successfully initialized application state, if any.
    #[inline]
    pub fn state(&self) -> Option<&T> {
        self.state.as_ref()
    }

    /// Attempted callbacks, including the current callback and early returns.
    #[inline]
    pub fn requests(&self) -> u64 {
        self.requests
    }

    /// Builder invocations, including failed attempts.
    #[inline]
    pub fn initialization_attempts(&self) -> u64 {
        self.initialization_attempts
    }

    /// Build only when empty; retain only success and return errors unchanged.
    ///
    /// An error can carry a request-local fallback router. The callback decides
    /// how to respond and whether to continue serving. A later call retries an
    /// unsuccessful build. Panics propagate normally.
    ///
    /// # Errors
    /// Returns the builder error unchanged without retaining it.
    #[inline]
    pub fn initialize<E, F>(&mut self, build: F) -> Result<(), E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        if self.state.is_none() {
            self.initialization_attempts = self.initialization_attempts.saturating_add(1);
            self.state = Some(build()?);
        }
        Ok(())
    }

    /// Run setup until it succeeds, independently of application initialization.
    ///
    /// For example, install logging after its configuration becomes available.
    /// This guard belongs to this `Sandbox`, not the process. Callers must make
    /// failed setup safe to retry: partial side effects are not rolled back.
    ///
    /// # Errors
    /// Returns the setup error, leaving setup eligible for retry.
    #[inline]
    pub fn setup_once<E, F>(&mut self, setup: F) -> Result<(), E>
    where
        F: FnOnce() -> Result<(), E>,
    {
        if !self.setup_complete {
            setup()?;
            self.setup_complete = true;
        }
        Ok(())
    }

    #[cfg(any(feature = "fastly", test))]
    fn handle<Q, R, F>(&mut self, request: Q, handler: F) -> R
    where
        F: FnOnce(Q, &mut Self) -> R,
    {
        self.requests = self.requests.saturating_add(1);
        let _scope = OutboundScope::callback();
        handler(request, self)
    }
}

/// Serve custom callbacks with lazy retained state and the supplied SDK limits.
///
/// The callback controls initialization, dispatch, finalization and streaming.
/// The callback owns sending and returns only a terminal `Result<(), E>`. The
/// SDK records that result without sending a synthetic error response. Errors
/// terminate serving, including failures after commitment, without a second send.
/// Callback-owned batch drivers are released at callback exit, even if a batch is retained.
/// Batches created while polling a response body have an independent delivery lifetime.
///
/// SDK limits are upper bounds, not a guarantee of reuse. This function does not
/// read configuration or enable reuse in existing entry points.
#[cfg(feature = "fastly")]
#[must_use = "inspect the serving summary or call into_result() to handle terminal errors"]
#[inline]
pub fn serve_custom<T, F, E>(serve: Serve, mut handler: F) -> ServeSummary<E>
where
    F: FnMut(fastly::Request, &mut Sandbox<T>) -> Result<(), E>,
{
    crate::init_fastly_abi();
    let mut sandbox = Sandbox::default();
    serve.run_with_context(
        |request, context: &mut Sandbox<T>| DeliveryResult(context.handle(request, &mut handler)),
        &mut sandbox,
    )
}

/// Handle one request with fresh state, without entering the SDK serving loop.
///
/// Takes a request already received by the caller. Sending remains owned by the
/// callback; this function never asks the SDK to render an error. Use an ordinary `main`.
/// Callback exit releases its batch drivers without waiting for retained handles to drop.
///
/// # Errors
/// Returns the callback error unchanged without another response attempt.
#[cfg(feature = "fastly")]
#[inline]
pub fn run_custom<T, E, F>(request: fastly::Request, handler: F) -> Result<(), E>
where
    F: FnOnce(fastly::Request, &mut Sandbox<T>) -> Result<(), E>,
{
    crate::init_fastly_abi();
    Sandbox::default().handle(request, handler)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use edgezero_core::{EdgeError, OutboundBatch, OutboundBatchDriverEvent};
    use futures::executor::block_on;
    use futures_util::stream::poll_fn;
    use std::task::Poll;

    use super::{DeliveryResult, Sandbox};
    use crate::outbound::scope::bind_batch;

    // Neither the retained value nor an error payload needs framework traits.
    struct App(u64);
    struct Fallback(&'static str);

    struct DriverOwner(Rc<Cell<usize>>);

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for DriverOwner {
        fn drop(&mut self) {
            self.0.set(self.0.get().saturating_add(1));
        }
    }

    fn pending_batch(released: Rc<Cell<usize>>) -> OutboundBatch {
        let owner = DriverOwner(released);
        let source = poll_fn(move |_cx| {
            let _keep_owner = &owner;
            Poll::<Option<OutboundBatchDriverEvent>>::Pending
        });
        OutboundBatch::from_driver(1, bind_batch(source))
    }

    #[test]
    fn callback_scope_exit_releases_retained_exchange() {
        let mut sandbox = Sandbox::<()>::default();
        for failed in [false, true] {
            let released = Rc::new(Cell::new(0));
            let saved = Rc::new(RefCell::new(None));
            let result = sandbox.handle((), |(), _| {
                *saved.borrow_mut() = Some(pending_batch(Rc::clone(&released)));
                assert_eq!(released.get(), 0);
                if failed {
                    Err("original callback error")
                } else {
                    Ok(())
                }
            });
            assert_eq!(result.is_err(), failed);
            if failed {
                assert_eq!(result, Err("original callback error"));
            }
            assert_eq!(released.get(), 1);
            let batch = saved.borrow_mut().take().expect("retained batch");
            assert!(matches!(
                block_on(batch.collect()).unwrap_err().error,
                EdgeError::Internal { .. }
            ));
            assert_eq!(released.get(), 1);
        }
        assert_eq!(sandbox.requests(), 2);
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn callback_unwind_releases_retained_exchange() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let released = Rc::new(Cell::new(0));
        let saved = Rc::new(RefCell::new(None));
        let mut sandbox = Sandbox::<()>::default();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                sandbox.handle((), |(), _| {
                    *saved.borrow_mut() = Some(pending_batch(Rc::clone(&released)));
                    panic!("original callback panic");
                });
            }))
            .is_err()
        );
        assert_eq!(released.get(), 1);
        let batch = saved.borrow_mut().take().expect("retained batch");
        assert!(matches!(
            block_on(batch.collect()).unwrap_err().error,
            EdgeError::Internal { .. }
        ));
        assert_eq!(released.get(), 1);
    }

    #[cfg(feature = "fastly")]
    #[test]
    fn retained_client_resolves_fresh_callback_scope() {
        use crate::outbound::FastlyOutboundClient;
        use edgezero_core::{
            Deadline, OutboundBatchNext, OutboundHttpClient as _, OutboundRequest,
        };
        use std::time::Duration;

        let client = FastlyOutboundClient::new();
        let mut sandbox = Sandbox::<()>::default();
        let mut previous = None;
        for _ in 0_u8..2 {
            let batch = sandbox.handle((), |(), _| {
                if let Some(stale) = previous.take() {
                    let error = block_on(OutboundBatch::collect(stale)).unwrap_err();
                    assert!(matches!(error.error, EdgeError::Internal { .. }));
                }
                let request = OutboundRequest::get("https://example.com")
                    .unwrap()
                    .stream_response();
                let mut batch = client
                    .start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)));
                let OutboundBatchNext::Item(_) = block_on(batch.next()).unwrap() else {
                    panic!("fresh scope");
                };
                batch
            });
            previous = Some(batch);
        }
        let failure = block_on(previous.unwrap().collect()).unwrap_err();
        assert!(matches!(failure.error, EdgeError::Internal { .. }));
        assert_eq!(sandbox.requests(), 2);
    }

    #[cfg(feature = "fastly")]
    #[test]
    fn callback_scope_exit_releases_retained_batch_driver() {
        use crate::outbound::FastlyOutboundClient;
        use edgezero_core::OutboundRequest;
        use edgezero_core::{Deadline, MonotonicClock, MonotonicInstant, OutboundHttpClient as _};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        struct ClockOwner(Arc<AtomicUsize>);

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for ClockOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let released = Arc::new(AtomicUsize::new(0));
        let owner = ClockOwner(Arc::clone(&released));
        let now = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || {
            let _keep_owner = &owner;
            now
        });
        let mut sandbox = Sandbox::<()>::default();
        let batch = sandbox.handle((), move |(), _| {
            let client = FastlyOutboundClient::with_clock(clock);
            // A queued preflight result still retains the whole unpolled driver.
            let request = OutboundRequest::get("https://example.com")
                .unwrap()
                .stream_response();
            client.start_batch_until(vec![request], Deadline::after(Duration::from_secs(1)))
        });

        assert_eq!(
            released.load(Ordering::SeqCst),
            1,
            "callback owns the driver"
        );
        let failure = block_on(batch.collect()).expect_err("closed callback scope");
        assert!(matches!(failure.error, EdgeError::Internal { .. }));
        assert_eq!(released.load(Ordering::SeqCst), 1, "release exactly once");
    }

    #[test]
    fn owned_delivery_retains_terminal_error_without_synthetic_send() {
        let result = DeliveryResult(Err::<(), _>("post-commit error"));
        assert_eq!(result.0, Err("post-commit error"));
    }

    #[cfg(feature = "fastly")]
    #[test]
    fn sdk_delivery_result_returns_terminal_error_without_synthetic_send() {
        use fastly::http::serve::HandlerResult as _;

        assert_eq!(
            DeliveryResult(Err::<(), _>("post-commit error")).send(),
            Err("post-commit error")
        );
        assert_eq!(DeliveryResult(Ok::<(), &str>(())).send(), Ok(()));
    }

    #[test]
    fn early_return_counts_request_without_setup_or_build() {
        let mut sandbox = Sandbox::<App>::default();
        sandbox.handle("health", |request, state| {
            assert_eq!(request, "health");
            assert_eq!(state.requests(), 1);
            assert_eq!(state.initialization_attempts(), 0);
            assert!(state.state().is_none());
            assert!(!state.setup_complete);
        });
    }

    #[test]
    fn failed_build_returns_payload_then_success_is_retained() {
        let mut sandbox = Sandbox::<App>::default();
        sandbox.handle("first", |_, state| {
            assert!(state.setup_once(|| Ok::<_, ()>(())).is_ok());
            let result = state.initialize(|| Err(Fallback("current request only")));
            assert!(matches!(result, Err(Fallback("current request only"))));
            assert!(state.state().is_none());
            assert_eq!(state.initialization_attempts(), 1);
        });
        for ordinal in 2..=4 {
            sandbox.handle(ordinal, |request, state| {
                assert!(
                    state
                        .setup_once::<(), _>(|| panic!("setup repeated"))
                        .is_ok()
                );
                assert!(
                    state
                        .initialize::<(), _>(|| {
                            assert_eq!(request, 2, "successful initialization repeated");
                            Ok(App(request))
                        })
                        .is_ok()
                );
                assert_eq!(state.state().unwrap().0, 2);
                assert_eq!(state.requests(), request);
                assert_eq!(state.initialization_attempts(), 2);
            });
        }
        let fresh = Sandbox::<App>::default();
        assert!(fresh.state().is_none());
        assert_eq!(fresh.requests(), 0);
        assert_eq!(fresh.initialization_attempts(), 0);
    }

    #[test]
    fn failed_app_assembly_retries_without_retaining_partial_state() {
        use edgezero_core::EdgeError;
        use edgezero_core::app::{App as AssembledApp, Hooks};
        use edgezero_core::router::RouterService;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static BUILDS: AtomicUsize = AtomicUsize::new(0);
        static CONFIGURES: AtomicUsize = AtomicUsize::new(0);
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct StateProbe;
        struct RetryHooks;

        #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
        impl Drop for StateProbe {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[expect(
            clippy::missing_trait_methods,
            reason = "tests route and configure failure retention"
        )]
        impl Hooks for RetryHooks {
            fn configure(app: &mut AssembledApp) -> Result<(), EdgeError> {
                if app.state::<Arc<StateProbe>>().is_none() {
                    return Err(EdgeError::service_unavailable("registered state missing"));
                }
                if CONFIGURES.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(EdgeError::service_unavailable("configuration unavailable"));
                }
                Ok(())
            }

            fn routes() -> Result<RouterService, EdgeError> {
                if BUILDS.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(EdgeError::service_unavailable("routes unavailable"));
                }
                Ok(RouterService::builder()
                    .with_state(Arc::new(StateProbe))
                    .build())
            }
        }

        let mut sandbox = Sandbox::<AssembledApp>::default();
        for (attempt, message) in [(1, "routes unavailable"), (2, "configuration unavailable")] {
            let error = sandbox
                .initialize(|| AssembledApp::build::<RetryHooks>(crate::FASTLY_PLATFORM))
                .unwrap_err();
            assert_eq!(error.message(), message);
            assert!(sandbox.state().is_none());
            assert_eq!(sandbox.initialization_attempts(), attempt);
        }
        assert_eq!(DROPS.load(Ordering::SeqCst), 1);
        sandbox
            .initialize(|| AssembledApp::build::<RetryHooks>(crate::FASTLY_PLATFORM))
            .unwrap();
        let retained = Arc::clone(sandbox.state().unwrap().state::<Arc<StateProbe>>().unwrap());
        sandbox
            .initialize::<EdgeError, _>(|| panic!("successful app must not be rebuilt"))
            .unwrap();
        assert_eq!(sandbox.initialization_attempts(), 3);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 3);
        assert_eq!(CONFIGURES.load(Ordering::SeqCst), 2);
        assert!(Arc::ptr_eq(
            &retained,
            sandbox.state().unwrap().state::<Arc<StateProbe>>().unwrap()
        ));
        drop(retained);
        drop(sandbox);
        assert_eq!(DROPS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failed_setup_can_retry_without_initializing_the_app() {
        let mut sandbox = Sandbox::<App>::default();
        assert_eq!(sandbox.setup_once(|| Err("not ready")), Err("not ready"));
        assert!(!sandbox.setup_complete);
        assert!(sandbox.setup_once(|| Ok::<_, ()>(())).is_ok());
        assert!(
            sandbox
                .setup_once::<(), _>(|| panic!("setup repeated"))
                .is_ok()
        );
        assert_eq!(sandbox.initialization_attempts(), 0);
        assert_eq!(sandbox.requests(), 0);
    }
}
