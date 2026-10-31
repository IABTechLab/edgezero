pub(crate) mod observation {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::{Rc, Weak};

    #[cfg(any(feature = "fastly", test))]
    use edgezero_core::MonotonicClock;
    use edgezero_core::{IndependentOutboundProgress, OutboundScope};

    use super::FastlyOutboundObservation;
    #[cfg(any(feature = "fastly", test))]
    use super::{Arc, Duration, FastlyOutboundStats, Mutex, PoisonError};

    #[cfg(any(feature = "fastly", test))]
    enum RegistrationDuration {
        ClockRegression,
        Measured(Duration),
        NotCalled,
    }

    struct Link(RefCell<Weak<Recorder>>);

    pub(crate) struct Recorder {
        configurations: RefCell<Option<HashSet<String>>>,
        handle: FastlyOutboundObservation,
    }

    #[cfg(any(feature = "fastly", test))]
    pub(crate) struct Guard(Rc<Recorder>);

    #[cfg(any(feature = "fastly", test))]
    pub(crate) struct Registration<'clock> {
        clock: &'clock MonotonicClock,
        elapsed: RegistrationDuration,
        record: Option<Rc<Recorder>>,
    }

    #[cfg(any(feature = "fastly", test))]
    impl Guard {
        pub(crate) fn enter() -> Option<Self> {
            let scope = OutboundScope::capture(Rc::new(IndependentOutboundProgress));
            if !scope.is_managed() || scope.check().is_err() {
                return None;
            }
            let record = Rc::new(Recorder {
                configurations: RefCell::new(Some(HashSet::new())),
                handle: FastlyOutboundObservation {
                    stats: Arc::new(Mutex::new(FastlyOutboundStats {
                        configurations: 0,
                        longest: Duration::ZERO,
                        registrations: 0,
                        total: Duration::ZERO,
                    })),
                },
            });
            let installed = scope
                .provider_state(|| Link(RefCell::new(Weak::new())))
                .ok()?;
            let mut entry = installed.0.borrow_mut();
            if entry.upgrade().is_some() {
                return None;
            }
            *entry = Rc::downgrade(&record);
            Some(Self(record))
        }
    }

    #[cfg(any(feature = "fastly", test))]
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Guard {
        fn drop(&mut self) {
            // The provider family may survive in an accepted response body; its weak link
            // must never extend recording or keep configuration keys past callback exit.
            self.0.configurations.borrow_mut().take();
        }
    }

    impl Recorder {
        #[cfg(any(feature = "fastly", test))]
        pub(crate) fn configuration(&self, key: String) {
            let inserted = self
                .configurations
                .borrow_mut()
                .as_mut()
                .is_some_and(|keys| keys.insert(key));
            if inserted {
                let mut stats = self
                    .handle
                    .stats
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                stats.configurations = stats.configurations.saturating_add(1);
            }
        }

        pub(super) fn handle(&self) -> FastlyOutboundObservation {
            self.handle.clone()
        }
    }

    #[cfg(any(feature = "fastly", test))]
    impl<'clock> Registration<'clock> {
        pub(crate) fn new(record: Option<Rc<Recorder>>, clock: &'clock MonotonicClock) -> Self {
            Self {
                clock,
                elapsed: RegistrationDuration::NotCalled,
                record,
            }
        }

        pub(crate) fn run<T>(&mut self, register: impl FnOnce() -> T) -> T {
            if let Some(record) = self
                .record
                .as_ref()
                .filter(|record| record.configurations.borrow().is_some())
            {
                let mut stats = record
                    .handle
                    .stats
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                stats.registrations = stats.registrations.saturating_add(1);
            }
            let started = self.clock.now();
            let result = register();
            let elapsed = self.clock.now().checked_duration_since(started);
            if let Some(duration) = elapsed
                && let Some(record) = self
                    .record
                    .as_ref()
                    .filter(|record| record.configurations.borrow().is_some())
            {
                let mut stats = record
                    .handle
                    .stats
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                stats.total = stats.total.saturating_add(duration);
                stats.longest = stats.longest.max(duration);
            }
            self.elapsed = elapsed.map_or(
                RegistrationDuration::ClockRegression,
                RegistrationDuration::Measured,
            );
            result
        }
    }

    #[cfg(any(feature = "fastly", test))]
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Registration<'_> {
        fn drop(&mut self) {
            // Declared outside the backend cache call so logging cannot reenter its mutex.
            match self.elapsed {
                RegistrationDuration::Measured(duration)
                    if duration >= Duration::from_millis(50) =>
                {
                    log::warn!("Fastly backend registration took at least 50 ms");
                }
                RegistrationDuration::ClockRegression => {
                    log::warn!(
                        "Fastly backend registration clock moved backwards; duration unavailable"
                    );
                }
                RegistrationDuration::Measured(_) | RegistrationDuration::NotCalled => {}
            }
        }
    }

    pub(crate) fn current() -> Option<Rc<Recorder>> {
        for_scope(&OutboundScope::capture(Rc::new(
            IndependentOutboundProgress,
        )))
    }

    pub(crate) fn for_scope(scope: &OutboundScope) -> Option<Rc<Recorder>> {
        if !scope.is_managed() || scope.check().is_err() {
            return None;
        }
        let record = scope
            .provider_state(|| Link(RefCell::new(Weak::new())))
            .ok()?
            .0
            .borrow()
            .upgrade()?;
        let active = record.configurations.borrow().is_some();
        active.then_some(record)
    }
}

use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use edgezero_core::http::Request;

/// Numeric registration observations for one Fastly serving callback.
///
/// Cloning this handle retains only numbers, not requests, configurations or outbound work.
/// The record freezes when its callback (or standalone helper) exits.
#[derive(Clone, Debug)]
pub struct FastlyOutboundObservation {
    stats: Arc<Mutex<FastlyOutboundStats>>,
}

/// An immutable, coherent snapshot of one callback's backend registration accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FastlyOutboundStats {
    configurations: u64,
    longest: Duration,
    registrations: u64,
    total: Duration,
}

/// Fastly-specific context data stored on each request.
#[derive(Clone, Debug)]
pub struct FastlyRequestContext {
    pub client_ip: Option<IpAddr>,
}

impl FastlyOutboundObservation {
    /// Returns the active Fastly callback's handle, or `None` outside its lifetime.
    #[must_use]
    #[inline]
    pub fn current() -> Option<Self> {
        observation::current().map(|record| record.handle())
    }

    /// Reads all counters together, without retaining any callback resource.
    #[must_use]
    #[inline]
    pub fn snapshot(&self) -> FastlyOutboundStats {
        *self.stats.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl FastlyOutboundStats {
    /// Distinct eligible backend configurations, including cache hits and failed attempts.
    #[must_use]
    #[inline]
    pub const fn configuration_count(&self) -> u64 {
        self.configurations
    }

    /// Longest completed registration call, including failed and late returns.
    #[must_use]
    #[inline]
    pub const fn max_registration_duration(&self) -> Duration {
        self.longest
    }

    /// Actual provider registration attempts, excluding cache hits and rejected preflight.
    #[must_use]
    #[inline]
    pub const fn registration_count(&self) -> u64 {
        self.registrations
    }

    /// Sum of completed registration durations in the paired application clock.
    #[must_use]
    #[inline]
    pub const fn total_registration_duration(&self) -> Duration {
        self.total
    }
}

impl FastlyRequestContext {
    #[inline]
    pub fn get(request: &Request) -> Option<&FastlyRequestContext> {
        request.extensions().get::<FastlyRequestContext>()
    }

    #[inline]
    pub fn insert(request: &mut Request, context: FastlyRequestContext) {
        request.extensions_mut().insert(context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::body::Body;
    use edgezero_core::http::{StatusCode, request_builder};
    use std::cell::RefCell;
    use std::net::IpAddr;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::str::FromStr as _;
    #[cfg(not(target_arch = "wasm32"))]
    use std::thread;

    struct RegistrationLogger;

    std::thread_local! {
        static REGISTRATION_LOGS: RefCell<Vec<(log::Level, String)>> = const { RefCell::new(Vec::new()) };
    }

    impl log::Log for RegistrationLogger {
        fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
            metadata.level() <= log::Level::Warn
        }

        fn flush(&self) {}

        fn log(&self, record: &log::Record<'_>) {
            if self.enabled(record.metadata()) {
                REGISTRATION_LOGS.with(|logs| {
                    logs.borrow_mut()
                        .push((record.level(), record.args().to_string()));
                });
            }
        }
    }

    fn public_wire(outcome: Result<u8, &str>) -> (StatusCode, Vec<u8>) {
        use edgezero_core::response::IntoResponse as _;
        use edgezero_core::{error::EdgeError, http::Response};
        let response = outcome.map_or_else(
            |_| {
                EdgeError::bad_gateway("backend registration failed")
                    .into_response()
                    .expect("public error response")
            },
            |byte| Response::new(Body::from_bytes(vec![byte])),
        );
        (
            response.status(),
            response.body().as_bytes().unwrap().to_vec(),
        )
    }

    #[test]
    fn registration_warning_obeys_fifty_millisecond_boundary() {
        use crate::lifecycle::with_callback;
        use edgezero_core::{MonotonicClock, MonotonicInstant};
        static LOGGER: RegistrationLogger = RegistrationLogger;
        log::set_logger(&LOGGER).expect("one logger in this unit-test binary");
        log::set_max_level(log::LevelFilter::Trace);
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().unwrap());
        for failed in [false, true] {
            for micros in [49_999, 50_000, 50_001] {
                *now.lock().unwrap() = start;
                REGISTRATION_LOGS.with(|logs| logs.borrow_mut().clear());
                let retained = with_callback(|| {
                    let record = observation::current().unwrap();
                    record.configuration("opaque-configuration-key".into());
                    let handle = FastlyOutboundObservation::current().unwrap();
                    let mut attempt = observation::Registration::new(Some(record), &clock);
                    let expected = if failed {
                        Err("provider-private-token")
                    } else {
                        Ok(7_u8)
                    };
                    let expected_wire = public_wire(expected);
                    let actual = attempt.run(|| {
                        *now.lock().unwrap() = start + Duration::from_micros(micros);
                        expected
                    });
                    assert_eq!(
                        actual, expected,
                        "observation must not change the provider result"
                    );
                    assert_eq!(public_wire(actual), expected_wire);
                    drop(attempt);
                    let stats = handle.snapshot();
                    assert_eq!(stats.registration_count(), 1);
                    assert_eq!(stats.configuration_count(), 1);
                    assert_eq!(
                        stats.total_registration_duration(),
                        Duration::from_micros(micros)
                    );
                    assert_eq!(
                        stats.max_registration_duration(),
                        Duration::from_micros(micros)
                    );
                    handle
                });
                assert_eq!(retained.snapshot().registration_count(), 1);
                REGISTRATION_LOGS.with(|records| {
                    let logs = records.borrow();
                    assert_eq!(logs.len(), usize::from(micros >= 50_000));
                    for (level, message) in logs.iter() {
                        assert_eq!(*level, log::Level::Warn);
                        assert_eq!(message, "Fastly backend registration took at least 50 ms");
                    }
                });
            }
        }

        for backwards in [false, true] {
            *now.lock().unwrap() = start + Duration::from_secs(1);
            REGISTRATION_LOGS.with(|logs| logs.borrow_mut().clear());
            with_callback(|| {
                let record = observation::current().unwrap();
                let handle = FastlyOutboundObservation::current().unwrap();
                let mut attempt = observation::Registration::new(Some(record), &clock);
                assert_eq!(
                    attempt.run(|| {
                        if backwards {
                            *now.lock().unwrap() = start;
                        }
                        Err::<(), _>("unchanged provider error")
                    }),
                    Err("unchanged provider error")
                );
                drop(attempt);
                assert_eq!(handle.snapshot().registration_count(), 1);
                assert_eq!(
                    handle.snapshot().total_registration_duration(),
                    Duration::ZERO
                );
                assert_eq!(
                    handle.snapshot().max_registration_duration(),
                    Duration::ZERO
                );
            });
            REGISTRATION_LOGS.with(|logs| {
                assert_eq!(logs.borrow().len(), usize::from(backwards));
                if backwards {
                    assert_eq!(
                        logs.borrow()[0].1,
                        "Fastly backend registration clock moved backwards; duration unavailable"
                    );
                }
            });
        }
    }

    #[test]
    fn registration_observation_cannot_resume_after_error_or_unwind() {
        use crate::lifecycle::with_callback;

        for unwind in [false, true]
            .into_iter()
            .filter(|unwind| !unwind || cfg!(panic = "unwind"))
        {
            let mut saved = None;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                with_callback(|| {
                    let record = observation::current().unwrap();
                    record.configuration("eligible".into());
                    saved = Some((FastlyOutboundObservation::current().unwrap(), record));
                    assert!(!unwind, "test callback unwind");
                    Err::<(), _>("callback failed")
                })
            }));
            assert_eq!(outcome.is_err(), unwind);
            let (handle, record) = saved.unwrap();
            record.configuration("must-not-repopulate".into());
            assert_eq!(handle.snapshot().configuration_count(), 1);
            assert!(FastlyOutboundObservation::current().is_none());
        }
    }

    #[test]
    fn accepted_body_does_not_extend_registration_recording() {
        use crate::lifecycle::with_callback;
        use edgezero_core::http::{HeaderMap, Method, Response, StatusCode, Version};
        use edgezero_core::response_egress::{
            ResponseEgressAttempt, ResponseEgressCompletion, ResponseEgressHead,
            ResponseEgressObserverHandle,
        };
        use edgezero_core::response_egress_framing::ResponseEgressFramingMode;
        use edgezero_core::{MonotonicClock, OutboundScopeOwner};

        let (handle, record, mut attempt, prepared, body_scope) = with_callback(|| {
            let body_owner = OutboundScopeOwner::response_body();
            let body_scope = body_owner.scope();
            let response = body_owner
                .attach(Response::new(Body::from_bytes("payload")))
                .unwrap();
            let clock = MonotonicClock::default();
            let now = clock.now();
            let headers = HeaderMap::new();
            let head = ResponseEgressHead::new(
                StatusCode::OK,
                Version::HTTP_11,
                &headers,
                None,
                now,
                None,
            );
            let mut attempt = ResponseEgressAttempt::new(
                &head,
                now,
                ResponseEgressCompletion::empty(),
                ResponseEgressObserverHandle::default(),
                clock,
            );
            let prepared = attempt
                .prepare_response(
                    ResponseEgressFramingMode::HostAutomatic,
                    &Method::GET,
                    response,
                )
                .unwrap();
            assert!(attempt.begin_writing());
            let record = observation::current().unwrap();
            record.configuration("before-delivery".into());
            let handle = FastlyOutboundObservation::current().unwrap();
            body_scope
                .with(|| {
                    assert_eq!(
                        FastlyOutboundObservation::current().unwrap().snapshot(),
                        handle.snapshot()
                    );
                })
                .unwrap();
            (handle, record, attempt, prepared, body_scope)
        });
        body_scope
            .with(|| assert!(FastlyOutboundObservation::current().is_none()))
            .unwrap();
        record.configuration("after-callback-exit".into());
        assert_eq!(handle.snapshot().configuration_count(), 1);
        assert!(attempt.complete(MonotonicClock::default().now()));
        drop(prepared);
        assert_eq!(handle.snapshot().configuration_count(), 1);
    }

    #[test]
    fn request_registration_observation_follows_scope() {
        use crate::lifecycle::with_callback;

        assert!(FastlyOutboundObservation::current().is_none());
        let retained = with_callback(|| {
            let handle = FastlyOutboundObservation::current().expect("callback observation");
            let record = observation::current().expect("record");
            record.configuration("first".into());
            with_callback(|| {
                record.configuration("first".into());
                record.configuration("second".into());
                assert_eq!(
                    FastlyOutboundObservation::current().unwrap().snapshot(),
                    handle.snapshot()
                );
            });
            assert_eq!(handle.snapshot().configuration_count(), 2);
            assert!(
                FastlyOutboundObservation::current().is_some(),
                "helper cannot freeze outer callback"
            );
            handle
        });
        assert!(FastlyOutboundObservation::current().is_none());
        assert_eq!(retained.snapshot().configuration_count(), 2);
        with_callback(|| {
            assert_eq!(
                FastlyOutboundObservation::current()
                    .unwrap()
                    .snapshot()
                    .configuration_count(),
                0
            );
        });
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            thread::spawn(move || retained.snapshot())
                .join()
                .unwrap()
                .configuration_count(),
            2
        );
    }

    #[test]
    fn inserts_and_retrieves_client_ip() {
        let mut request = request_builder()
            .uri("https://example.com")
            .body(Body::empty())
            .expect("request");

        let context = FastlyRequestContext {
            client_ip: Some(IpAddr::from_str("127.0.0.1").unwrap()),
        };
        FastlyRequestContext::insert(&mut request, context.clone());

        let retrieved = FastlyRequestContext::get(&request).expect("context");
        assert_eq!(retrieved.client_ip, context.client_ip);
    }

    #[test]
    fn get_returns_none_when_missing() {
        let request = request_builder()
            .uri("https://example.com")
            .body(Body::empty())
            .expect("request");

        assert!(FastlyRequestContext::get(&request).is_none());
    }
}
