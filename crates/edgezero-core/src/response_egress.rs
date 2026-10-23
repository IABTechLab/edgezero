//! Portable response-egress policy and exactly-once lifecycle reporting.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::error::{EdgeError, suppress_overflow_retry_after};
use crate::error_detail::ProviderFailureDetail;
use crate::http::{HeaderMap, Method, Response, StatusCode, Version};
use crate::response_egress_framing::{
    PreparedResponseEgress, ResponseEgressFramingError, ResponseEgressFramingMode,
    prepare_response_egress, response_egress_source_error_category,
};
use crate::response_headers::SynthesizedResponseHeaders;
use crate::router::RouteMetadata;
use crate::time::{DEADLINE_FAR_FUTURE, Deadline, MonotonicClock, MonotonicInstant};

/// Portable response-write budget used when an application installs no policy.
pub const DEFAULT_RESPONSE_WRITE_BUDGET: Duration = Duration::from_secs(30);

/// Internal safety budget used only when transmitting a bounded precommit fallback.
pub const RESPONSE_EGRESS_FALLBACK_SAFETY_BUDGET: Duration = Duration::from_secs(1);

/// Bounded error metadata visible to the detached-egress decision factory.
///
/// Raw parser failures that occur before `EdgeZero` captures this metadata remain outside the
/// portable response-egress lifecycle.
#[derive(Clone, Copy, Debug)]
pub struct DetachedResponseEgressHead<'head> {
    error_kind: &'static str,
    request_method: &'head Method,
    request_start: MonotonicInstant,
    status: StatusCode,
}

impl<'head> DetachedResponseEgressHead<'head> {
    /// Returns the stable public error category without diagnostic text.
    #[must_use]
    #[inline]
    pub const fn error_kind(&self) -> &'static str {
        self.error_kind
    }

    #[must_use]
    #[inline]
    pub(crate) const fn new(
        request_method: &'head Method,
        request_start: MonotonicInstant,
        status: StatusCode,
        error_kind: &'static str,
    ) -> Self {
        Self {
            error_kind,
            request_method,
            request_start,
            status,
        }
    }

    /// Returns the request method captured before body polling.
    #[must_use]
    #[inline]
    pub const fn request_method(&self) -> &'head Method {
        self.request_method
    }

    /// Returns the request start in the application's monotonic clock domain.
    #[must_use]
    #[inline]
    pub const fn request_start(&self) -> MonotonicInstant {
        self.request_start
    }

    /// Returns the status of the bounded error response.
    #[must_use]
    #[inline]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Builds a response-write deadline relative to this head's captured request start.
    ///
    /// The duration is clamped to [`DEADLINE_FAR_FUTURE`]. Arithmetic overflow fails closed by
    /// returning a deadline at the captured request start.
    #[must_use]
    #[inline]
    pub fn write_deadline_after(&self, duration: Duration) -> Deadline {
        let bounded = duration.min(DEADLINE_FAR_FUTURE);
        Deadline::at_instant(
            self.request_start
                .checked_add(bounded)
                .unwrap_or(self.request_start),
        )
    }
}

/// Application decision for a normalized ingress error before response construction.
#[non_exhaustive]
pub enum DetachedResponseEgressDecision {
    /// Terminates the request through the adapter's non-response abort/error boundary.
    Abort,
    /// Constructs detached egress and retains the supplied completion until terminal delivery.
    Send {
        completion: ResponseEgressCompletion,
        deadline: Deadline,
    },
}

/// Application-selected upper bound for one response's egress lifetime.
///
/// Insert this value into the response extensions before returning the response. `EdgeZero`
/// resolves it when egress begins, exposes the resulting absolute deadline to the egress policy
/// callback, and enforces the earlier of that deadline and the callback-selected write deadline.
#[derive(Clone, Copy, Debug)]
pub struct ResponseEgressDeadline(ResponseEgressDeadlineKind);

#[derive(Clone, Copy, Debug)]
enum ResponseEgressDeadlineKind {
    After(Duration),
    At(Deadline),
}

impl ResponseEgressDeadline {
    /// Selects a deadline relative to the instant response egress begins.
    ///
    /// The duration is clamped to [`DEADLINE_FAR_FUTURE`]. Arithmetic overflow fails closed by
    /// resolving to the sampled egress-start instant.
    #[must_use]
    #[inline]
    pub const fn after(duration: Duration) -> Self {
        Self(ResponseEgressDeadlineKind::After(duration))
    }

    /// Selects an absolute deadline in the application's monotonic clock domain.
    #[must_use]
    #[inline]
    pub const fn at(deadline: Deadline) -> Self {
        Self(ResponseEgressDeadlineKind::At(deadline))
    }

    #[must_use]
    #[inline]
    fn resolve_at(self, egress_started_at: MonotonicInstant) -> Deadline {
        match self.0 {
            ResponseEgressDeadlineKind::At(deadline) => deadline,
            ResponseEgressDeadlineKind::After(duration) => Deadline::at_instant(
                egress_started_at
                    .checked_add(duration.min(DEADLINE_FAR_FUTURE))
                    .unwrap_or(egress_started_at),
            ),
        }
    }
}

/// One finite absolute deadline selected for a response-conversion attempt.
#[derive(Clone, Copy, Debug)]
pub struct ResponseEgressPolicy {
    pub write_deadline: Deadline,
}

impl ResponseEgressPolicy {
    /// Clamps this policy to the portable far-future bound relative to egress start.
    ///
    /// An already-expired deadline remains valid and is not moved forward.
    ///
    /// # Errors
    /// Returns [`ResponseEgressOutcome::ConversionError`] if the clamp instant cannot be
    /// represented by the monotonic clock.
    #[inline]
    pub fn normalize_at(
        mut self,
        egress_started_at: MonotonicInstant,
    ) -> Result<Self, ResponseEgressOutcome> {
        let maximum = egress_started_at
            .checked_add(DEADLINE_FAR_FUTURE)
            .ok_or(ResponseEgressOutcome::ConversionError)?;
        if self.write_deadline.instant() > maximum {
            self.write_deadline = Deadline::at_instant(maximum);
        }
        Ok(self)
    }
}

/// Immutable response metadata visible to the synchronous egress policy callback.
///
/// This type deliberately has no response-body or mutation access.
#[derive(Clone, Copy, Debug)]
pub struct ResponseEgressHead<'head> {
    application_deadline: Option<Deadline>,
    headers: &'head HeaderMap,
    request_start: MonotonicInstant,
    route: Option<&'head RouteMetadata>,
    status: StatusCode,
    version: Version,
}

impl<'head> ResponseEgressHead<'head> {
    /// Returns the response-owned deadline after resolving it to an absolute instant.
    #[must_use]
    #[inline]
    pub fn application_deadline(&self) -> Option<Deadline> {
        self.application_deadline
    }

    #[must_use]
    #[inline]
    pub fn headers(&self) -> &'head HeaderMap {
        self.headers
    }

    /// Creates a body-blind response-head view for an adapter conversion attempt.
    #[must_use]
    #[inline]
    pub fn new(
        status: StatusCode,
        version: Version,
        headers: &'head HeaderMap,
        application_deadline: Option<Deadline>,
        request_start: MonotonicInstant,
        route: Option<&'head RouteMetadata>,
    ) -> Self {
        Self {
            application_deadline,
            headers,
            request_start,
            route,
            status,
            version,
        }
    }

    #[must_use]
    #[inline]
    pub fn request_start(&self) -> MonotonicInstant {
        self.request_start
    }

    #[must_use]
    #[inline]
    pub fn route(&self) -> Option<&'head RouteMetadata> {
        self.route
    }

    #[must_use]
    #[inline]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    #[inline]
    pub fn version(&self) -> Version {
        self.version
    }
}

/// Synchronous, body-blind policy callback retained by an application.
pub type ResponseEgressPolicyCallback = Arc<
    dyn for<'head> Fn(&ResponseEgressHead<'head>, MonotonicInstant) -> ResponseEgressPolicy
        + Send
        + Sync
        + 'static,
>;

/// Terminal result of one response-conversion attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseEgressOutcome {
    ClientDisconnected,
    Completed,
    ConversionError,
    DeadlineExceeded,
    HostHandoff,
    RequestCancelled,
    SourceError,
    TransportError,
    Unspecified,
}

/// Identifies which bounded response body contributed the reported byte count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseEgressBodyKind {
    Application,
    Fallback,
}

/// Records whether a selected fallback reached its finish boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseEgressFallbackDisposition {
    Aborted,
    Completed,
}

/// Independent, bounded precommit framing observations for the application and fallback body.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResponseEgressFramingFailures {
    application: Option<ResponseEgressFramingError>,
    fallback: Option<ResponseEgressFramingError>,
}

impl ResponseEgressFramingFailures {
    #[must_use]
    #[inline]
    pub const fn application(&self) -> Option<ResponseEgressFramingError> {
        self.application
    }

    #[must_use]
    #[inline]
    pub const fn fallback(&self) -> Option<ResponseEgressFramingError> {
        self.fallback
    }
}

/// Bounded terminal report for one response-conversion attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseEgressReport {
    pub body_kind: ResponseEgressBodyKind,
    pub bytes_written: u64,
    pub elapsed: Duration,
    pub fallback_disposition: Option<ResponseEgressFallbackDisposition>,
    /// First precommit framing rejection for each body kind; never a wire diagnostic.
    pub framing_failures: ResponseEgressFramingFailures,
    pub outcome: ResponseEgressOutcome,
    pub request_start: MonotonicInstant,
    pub route: Option<RouteMetadata>,
    /// First converter-observed body-source error, independent of the terminal outcome.
    pub source_failure: Option<ResponseEgressSourceFailure>,
}

/// Text-free snapshot of a body-source error observed during response conversion.
///
/// This is observation evidence, not the terminal cause. A deadline/cancellation can still win,
/// and an application-body observation remains tagged as such when fallback delivery follows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseEgressSourceFailure {
    body_kind: ResponseEgressBodyKind,
    category: &'static str,
    provider_detail: Option<ProviderFailureDetail>,
}

impl ResponseEgressSourceFailure {
    #[must_use]
    #[inline]
    pub const fn body_kind(&self) -> ResponseEgressBodyKind {
        self.body_kind
    }

    /// Returns the existing fixed source-error category, never its message or provider text.
    #[must_use]
    #[inline]
    pub const fn category(&self) -> &'static str {
        self.category
    }

    #[must_use]
    #[inline]
    pub const fn provider_failure_detail(&self) -> Option<&ProviderFailureDetail> {
        self.provider_detail.as_ref()
    }
}

type ResponseEgressCompletionCallback = Box<dyn FnOnce(&ResponseEgressReport) + Send + 'static>;

/// Non-clone owner of one response-scoped terminal callback.
///
/// The callback moves with the ingress/egress lifecycle and runs at most once. Use [`Self::empty`]
/// when no application resource needs to remain live through response transmission.
///
/// ```compile_fail
/// use edgezero_core::ResponseEgressCompletion;
///
/// let completion = ResponseEgressCompletion::empty();
/// let _duplicate = completion.clone();
/// ```
///
/// ```compile_fail
/// use edgezero_core::{Extensions, ResponseEgressCompletion};
///
/// let mut extensions = Extensions::new();
/// extensions.insert(ResponseEgressCompletion::empty());
/// ```
pub struct ResponseEgressCompletion {
    callback: Option<ResponseEgressCompletionCallback>,
}

/// Non-clone installer for one application resource retained through response egress.
///
/// Create a paired installer and completion with [`ResponseEgressCompletion::late_bound`]. The
/// installer remains usable after the completion closes only to report [`ResponseEgressResourceInstallError::Closed`].
///
/// ```compile_fail
/// use edgezero_core::ResponseEgressCompletion;
///
/// let (resource, _completion) = ResponseEgressCompletion::late_bound::<Vec<u8>>();
/// let _duplicate = resource.clone();
/// ```
pub struct ResponseEgressResource<T> {
    state: Arc<Mutex<ResponseEgressResourceState<T>>>,
}

impl<T> fmt::Debug for ResponseEgressResource<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseEgressResource")
            .finish_non_exhaustive()
    }
}

impl<T> ResponseEgressResource<T>
where
    T: Send + 'static,
{
    /// Installs the response-scoped resource exactly once.
    ///
    /// A rejected resource is dropped before this method returns and is never returned to the
    /// caller. `Closed` covers both terminal completion and completion abandonment.
    ///
    /// # Errors
    ///
    /// Returns [`ResponseEgressResourceInstallError::AlreadyInstalled`] when the slot already
    /// owns a resource, or [`ResponseEgressResourceInstallError::Closed`] after terminal egress or
    /// completion abandonment.
    #[inline]
    pub fn install(&self, resource: T) -> Result<(), ResponseEgressResourceInstallError> {
        let mut candidate = Some(resource);
        let install_error = {
            let mut state = self.state();
            if state.closed {
                Some(ResponseEgressResourceInstallError::Closed)
            } else if state.resource.is_some() {
                Some(ResponseEgressResourceInstallError::AlreadyInstalled)
            } else {
                state.resource = candidate.take();
                None
            }
        };
        drop(candidate);
        match install_error {
            Some(rejection) => Err(rejection),
            None => Ok(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, ResponseEgressResourceState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Stable reason a late-bound response-egress resource was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ResponseEgressResourceInstallError {
    #[error("a response-egress resource is already installed")]
    AlreadyInstalled,
    #[error("the response-egress resource owner is closed")]
    Closed,
}

struct ResponseEgressResourceOwner<T> {
    state: Option<Arc<Mutex<ResponseEgressResourceState<T>>>>,
}

impl<T> ResponseEgressResourceOwner<T> {
    fn close(mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        close_response_egress_resource(&state);
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl<T> Drop for ResponseEgressResourceOwner<T> {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        close_response_egress_resource(&state);
    }
}

struct ResponseEgressResourceState<T> {
    closed: bool,
    resource: Option<T>,
}

impl ResponseEgressCompletion {
    fn complete(&mut self, report: &ResponseEgressReport) {
        let Some(callback) = self.callback.take() else {
            return;
        };
        let result = catch_unwind(AssertUnwindSafe(|| callback(report)));
        if result.is_err() {
            log::error!("response-egress completion panicked after terminal transition");
        }
    }

    /// Creates a completion with no response-scoped callback.
    #[must_use]
    #[inline]
    pub const fn empty() -> Self {
        Self { callback: None }
    }

    /// Joins this completion with `other` into one response-scoped completion.
    ///
    /// At terminal egress, both callbacks receive the same borrowed report and run at most once
    /// in deterministic left-to-right order: this completion first, then `other`. Dropping the
    /// joined completion before terminal egress abandons both callbacks and releases resources
    /// captured by either callback exactly once.
    ///
    /// On targets where panics unwind, each callback has an independent panic boundary, so a
    /// panic in the left callback does not suppress the right callback. On panic-abort targets,
    /// the process may terminate during the left callback before the right callback runs.
    #[must_use]
    #[inline]
    pub fn join(mut self, mut other: Self) -> Self {
        Self::new(move |report| {
            self.complete(report);
            other.complete(report);
        })
    }

    /// Creates one late-bound application-resource installer and its paired completion.
    ///
    /// The completion closes the installer at terminal egress or when abandoned. An installed
    /// resource remains live until that close operation and is then dropped exactly once.
    #[must_use]
    #[inline]
    pub fn late_bound<T>() -> (ResponseEgressResource<T>, Self)
    where
        T: Send + 'static,
    {
        let state = Arc::new(Mutex::new(ResponseEgressResourceState {
            closed: false,
            resource: None,
        }));
        let resource = ResponseEgressResource {
            state: Arc::clone(&state),
        };
        let owner = ResponseEgressResourceOwner { state: Some(state) };
        let completion = Self::new(move |_report| owner.close());
        (resource, completion)
    }

    /// Retains one response-scoped callback until terminal egress.
    #[must_use]
    #[inline]
    pub fn new<Complete>(complete: Complete) -> Self
    where
        Complete: FnOnce(&ResponseEgressReport) + Send + 'static,
    {
        Self {
            callback: Some(Box::new(complete)),
        }
    }
}

/// Synchronous factory for one detached normalized-ingress-error disposition.
pub type DetachedResponseEgressDecisionFactory = Arc<
    dyn for<'head> Fn(&DetachedResponseEgressHead<'head>) -> DetachedResponseEgressDecision
        + Send
        + Sync
        + 'static,
>;

/// Synchronous observer for terminal response-egress reports.
pub trait ResponseEgressObserver: Send + Sync + 'static {
    fn complete(&self, report: &ResponseEgressReport);
}

struct NoopResponseEgressObserver;

impl ResponseEgressObserver for NoopResponseEgressObserver {
    #[inline]
    fn complete(&self, _report: &ResponseEgressReport) {}
}

/// Cloneable handle to an application response-egress observer.
#[derive(Clone)]
pub struct ResponseEgressObserverHandle {
    observer: Arc<dyn ResponseEgressObserver>,
}

/// Adapter-facing response plus immutable ingress metadata and app-owned egress hooks.
#[doc(hidden)]
pub struct ResponseEgressEnvelope {
    clock: MonotonicClock,
    completion: ResponseEgressCompletion,
    observer: ResponseEgressObserverHandle,
    policy: ResponseEgressPolicyCallback,
    request_method: Method,
    request_start: MonotonicInstant,
    response: Response,
    route: Option<RouteMetadata>,
    synthesized: bool,
    synthesized_headers: SynthesizedResponseHeaders,
}

pub(crate) struct ResponseEgressRequestMetadata {
    request_method: Method,
    request_start: MonotonicInstant,
    route: Option<RouteMetadata>,
}

impl ResponseEgressRequestMetadata {
    #[inline]
    pub(crate) const fn new(
        request_method: Method,
        request_start: MonotonicInstant,
        route: Option<RouteMetadata>,
    ) -> Self {
        Self {
            request_method,
            request_start,
            route,
        }
    }
}

/// Live response-egress state returned when application response preparation fails precommit.
#[doc(hidden)]
pub struct ResponseEgressBeginFailure {
    attempt: ResponseEgressAttempt,
    cause: ResponseEgressOutcome,
    clock: MonotonicClock,
}

impl ResponseEgressBeginFailure {
    /// Consumes the failure into the sole live attempt, its clock, and original cause.
    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (ResponseEgressAttempt, MonotonicClock, ResponseEgressOutcome) {
        (self.attempt, self.clock, self.cause)
    }
}

impl ResponseEgressEnvelope {
    /// Starts an owned response-egress attempt without pre-terminalizing fallback-eligible errors.
    ///
    /// # Errors
    /// Returns the sole live attempt, its injected clock, and a bounded precommit cause when the
    /// policy panics on an unwind-capable target or its deadline cannot be normalized. A target
    /// compiled with panic-abort cannot contain application callback panics.
    #[inline]
    pub fn begin(
        mut self,
        framing: ResponseEgressFramingMode,
    ) -> Result<
        (
            PreparedResponseEgress,
            ResponseEgressPolicy,
            ResponseEgressAttempt,
            MonotonicClock,
        ),
        Box<ResponseEgressBeginFailure>,
    > {
        let egress_started_at = self.clock.now();
        if self.synthesized {
            self.synthesized_headers.apply(self.response.headers_mut());
            suppress_overflow_retry_after(&mut self.response);
        }
        let application_deadline = self
            .response
            .extensions_mut()
            .remove::<ResponseEgressDeadline>()
            .map(|deadline| deadline.resolve_at(egress_started_at));
        let head = ResponseEgressHead::new(
            self.response.status(),
            self.response.version(),
            self.response.headers(),
            application_deadline,
            self.request_start,
            self.route.as_ref(),
        );
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            egress_started_at,
            self.completion,
            self.observer,
            self.clock.clone(),
        );
        attempt.synthesized_headers = self.synthesized_headers;
        let Ok(selected_policy) =
            catch_unwind(AssertUnwindSafe(|| (self.policy)(&head, egress_started_at)))
        else {
            log::error!("response-egress policy panicked before conversion");
            return Err(Box::new(ResponseEgressBeginFailure {
                attempt,
                cause: ResponseEgressOutcome::ConversionError,
                clock: self.clock,
            }));
        };
        let normalized_policy = match selected_policy.normalize_at(egress_started_at) {
            Ok(normalized) => normalized,
            Err(outcome) => {
                return Err(Box::new(ResponseEgressBeginFailure {
                    attempt,
                    cause: outcome,
                    clock: self.clock,
                }));
            }
        };
        let bounded_policy = ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(application_deadline.map_or(
                normalized_policy.write_deadline.instant(),
                |deadline| {
                    deadline
                        .instant()
                        .min(normalized_policy.write_deadline.instant())
                },
            )),
        };
        let Ok(prepared) = attempt.prepare_response(framing, &self.request_method, self.response)
        else {
            return Err(Box::new(ResponseEgressBeginFailure {
                attempt,
                cause: ResponseEgressOutcome::ConversionError,
                clock: self.clock,
            }));
        };
        Ok((prepared, bounded_policy, attempt, self.clock))
    }

    pub(crate) fn detached(
        response: Response,
        request_start: MonotonicInstant,
        request_method: Method,
        completion: ResponseEgressCompletion,
        clock: MonotonicClock,
    ) -> Self {
        Self::new(
            response,
            ResponseEgressRequestMetadata::new(request_method, request_start, None),
            completion,
            Arc::new(default_response_egress_policy),
            ResponseEgressObserverHandle::default(),
            clock,
        )
    }

    pub(crate) fn new(
        response: Response,
        metadata: ResponseEgressRequestMetadata,
        completion: ResponseEgressCompletion,
        policy: ResponseEgressPolicyCallback,
        observer: ResponseEgressObserverHandle,
        clock: MonotonicClock,
    ) -> Self {
        Self {
            clock,
            completion,
            observer,
            policy,
            request_method: metadata.request_method,
            request_start: metadata.request_start,
            response,
            route: metadata.route,
            synthesized: false,
            synthesized_headers: SynthesizedResponseHeaders::default(),
        }
    }

    /// Returns the original request method used for response framing.
    #[must_use]
    #[inline]
    pub fn request_method(&self) -> &Method {
        &self.request_method
    }

    /// Borrows the application response before egress policy and framing are evaluated.
    #[doc(hidden)]
    #[must_use]
    #[inline]
    pub fn response_mut(&mut self) -> &mut Response {
        &mut self.response
    }

    pub(crate) fn synthesized(mut self) -> Self {
        self.synthesized = true;
        self
    }

    /// Replaces the immutable policy before conversion, without decorating a routed/refused body.
    #[doc(hidden)]
    #[must_use]
    #[inline]
    pub fn with_synthesized_response_headers(
        mut self,
        headers: SynthesizedResponseHeaders,
    ) -> Self {
        self.synthesized_headers = headers;
        self
    }
}

impl ResponseEgressObserverHandle {
    fn complete(&self, report: &ResponseEgressReport) {
        let result = catch_unwind(AssertUnwindSafe(|| self.observer.complete(report)));
        if result.is_err() {
            log::error!("response-egress observer panicked after terminal transition");
        }
    }

    #[must_use]
    #[inline]
    pub fn new<Observer>(observer: Observer) -> Self
    where
        Observer: ResponseEgressObserver,
    {
        Self {
            observer: Arc::new(observer),
        }
    }
}

impl Default for ResponseEgressObserverHandle {
    #[inline]
    fn default() -> Self {
        Self::new(NoopResponseEgressObserver)
    }
}

enum AttemptState {
    Initial,
    Terminal(ResponseEgressReport),
    Writing,
}

/// Adapter-facing exactly-once response-egress completion guard.
///
/// The guard is intentionally non-clone. Dropping it before a terminal signal reports a
/// conversion error from the initial state or a transport error from the writing state.
#[doc(hidden)]
pub struct ResponseEgressAttempt {
    body_kind: ResponseEgressBodyKind,
    bytes_written: u64,
    clock: MonotonicClock,
    completion: ResponseEgressCompletion,
    egress_started_at: MonotonicInstant,
    fallback_cause: Option<ResponseEgressOutcome>,
    fallback_disposition: Option<ResponseEgressFallbackDisposition>,
    framing_failures: ResponseEgressFramingFailures,
    observer: ResponseEgressObserverHandle,
    request_start: MonotonicInstant,
    route: Option<RouteMetadata>,
    source_failure: Option<ResponseEgressSourceFailure>,
    state: AttemptState,
    synthesized_headers: SynthesizedResponseHeaders,
}

impl ResponseEgressAttempt {
    /// Accounts payload bytes accepted at the adapter's documented write boundary.
    ///
    /// Returns `false` outside the writing state. Overflow terminalizes the attempt as
    /// [`ResponseEgressOutcome::TransportError`] and also returns `false`.
    #[inline]
    pub fn account_bytes(&mut self, bytes: u64, observed_at: MonotonicInstant) -> bool {
        if !matches!(self.state, AttemptState::Writing) {
            return false;
        }
        let Some(total) = self.bytes_written.checked_add(bytes) else {
            self.fail_current_body(ResponseEgressOutcome::TransportError, observed_at);
            return false;
        };
        self.bytes_written = total;
        true
    }

    /// Selects the bounded adapter fallback while retaining the original precommit cause.
    #[inline]
    pub fn begin_fallback(&mut self, cause: ResponseEgressOutcome) -> bool {
        if !matches!(self.state, AttemptState::Initial)
            || self.fallback_cause.is_some()
            || !matches!(
                cause,
                ResponseEgressOutcome::ConversionError | ResponseEgressOutcome::DeadlineExceeded
            )
        {
            return false;
        }
        self.body_kind = ResponseEgressBodyKind::Fallback;
        self.fallback_cause = Some(cause);
        true
    }

    /// Moves an initial attempt into the writing state.
    ///
    /// Returns `false` after writing has already begun or the attempt is terminal.
    #[inline]
    pub fn begin_writing(&mut self) -> bool {
        if !matches!(self.state, AttemptState::Initial) {
            return false;
        }
        self.state = AttemptState::Writing;
        true
    }

    /// Successfully completes a writing attempt.
    ///
    /// Calling this in the initial state terminalizes as `ConversionError`; a converter must
    /// enter writing even for an empty response. Later terminal signals return `false`.
    #[inline]
    pub fn complete(&mut self, observed_at: MonotonicInstant) -> bool {
        if self.body_kind == ResponseEgressBodyKind::Fallback {
            return false;
        }
        match self.state {
            AttemptState::Initial => {
                self.transition(ResponseEgressOutcome::ConversionError, observed_at)
            }
            AttemptState::Writing => self.transition(ResponseEgressOutcome::Completed, observed_at),
            AttemptState::Terminal(_) => false,
        }
    }

    fn fail_current_body(
        &mut self,
        application_outcome: ResponseEgressOutcome,
        observed_at: MonotonicInstant,
    ) -> bool {
        if let Some(cause) = self.fallback_cause {
            self.fallback_disposition = Some(ResponseEgressFallbackDisposition::Aborted);
            self.transition(cause, observed_at)
        } else {
            self.transition(application_outcome, observed_at)
        }
    }

    /// Settles a selected fallback while preserving its original precommit cause.
    #[inline]
    pub fn finish_fallback(
        &mut self,
        disposition: ResponseEgressFallbackDisposition,
        observed_at: MonotonicInstant,
    ) -> bool {
        let Some(cause) = self.fallback_cause else {
            return false;
        };
        let valid_state = match disposition {
            ResponseEgressFallbackDisposition::Aborted => {
                matches!(self.state, AttemptState::Initial | AttemptState::Writing)
            }
            ResponseEgressFallbackDisposition::Completed => {
                matches!(self.state, AttemptState::Writing)
            }
        };
        if !valid_state {
            return false;
        }
        self.fallback_disposition = Some(disposition);
        self.transition(cause, observed_at)
    }

    /// Returns the exact terminal snapshot after its completion and observer notifications.
    ///
    /// A live attempt is abandoned using the same failure classification as `Drop`, not completed
    /// successfully. An already terminal attempt samples no clock and invokes no callback again.
    #[must_use]
    #[inline]
    pub fn into_terminal_report(mut self) -> ResponseEgressReport {
        let application_outcome = match &self.state {
            AttemptState::Terminal(report) => return report.clone(),
            AttemptState::Initial => ResponseEgressOutcome::ConversionError,
            AttemptState::Writing => ResponseEgressOutcome::TransportError,
        };
        let outcome = if let Some(cause) = self.fallback_cause {
            self.fallback_disposition = Some(ResponseEgressFallbackDisposition::Aborted);
            cause
        } else {
            application_outcome
        };
        self.publish_report(outcome, self.clock.now())
    }

    #[must_use]
    #[inline]
    pub fn new(
        head: &ResponseEgressHead<'_>,
        egress_started_at: MonotonicInstant,
        completion: ResponseEgressCompletion,
        observer: ResponseEgressObserverHandle,
        clock: MonotonicClock,
    ) -> Self {
        Self {
            body_kind: ResponseEgressBodyKind::Application,
            bytes_written: 0,
            clock,
            completion,
            egress_started_at,
            framing_failures: ResponseEgressFramingFailures::default(),
            fallback_cause: None,
            fallback_disposition: None,
            observer,
            request_start: head.request_start(),
            route: head.route().cloned(),
            source_failure: None,
            state: AttemptState::Initial,
            synthesized_headers: SynthesizedResponseHeaders::default(),
        }
    }

    /// Normalizes one response and retains its first precommit framing error per body kind.
    /// Recording and logging are initial-only; writing/terminal calls still normalize and return
    /// the original result without manufacturing a precommit observation.
    ///
    /// # Errors
    /// Returns the original framing error. Recording changes no clock, polling or terminal state.
    #[inline]
    pub fn prepare_response(
        &mut self,
        framing: ResponseEgressFramingMode,
        request_method: &Method,
        mut response: Response,
    ) -> Result<PreparedResponseEgress, ResponseEgressFramingError> {
        if self.body_kind == ResponseEgressBodyKind::Fallback
            && matches!(self.state, AttemptState::Initial)
        {
            self.synthesized_headers.apply(response.headers_mut());
        }
        prepare_response_egress(framing, request_method, response).inspect_err(|reason| {
            if !matches!(self.state, AttemptState::Initial) {
                return;
            }
            let slot = match self.body_kind {
                ResponseEgressBodyKind::Application => &mut self.framing_failures.application,
                ResponseEgressBodyKind::Fallback => &mut self.framing_failures.fallback,
            };
            slot.get_or_insert(*reason);
            log::error!(
                "response-egress {:?} framing validation failed before commit: {reason}",
                self.body_kind
            );
        })
    }

    fn publish_report(
        &mut self,
        mut outcome: ResponseEgressOutcome,
        observed_at: MonotonicInstant,
    ) -> ResponseEgressReport {
        let elapsed =
            if let Some(elapsed) = observed_at.checked_duration_since(self.egress_started_at) {
                elapsed
            } else {
                log::error!("response-egress monotonic clock moved backwards");
                outcome = ResponseEgressOutcome::Unspecified;
                Duration::ZERO
            };
        let report = ResponseEgressReport {
            bytes_written: self.bytes_written,
            body_kind: self.body_kind,
            elapsed,
            fallback_disposition: self.fallback_disposition,
            framing_failures: self.framing_failures,
            outcome,
            request_start: self.request_start,
            route: self.route.clone(),
            source_failure: self.source_failure,
        };
        self.state = AttemptState::Terminal(report.clone());
        self.completion.complete(&report);
        self.observer.complete(&report);
        report
    }

    /// Retains the first body-source error without changing state, clocks or terminal outcome.
    ///
    /// Returns false after an observation or terminal transition. Missing provider detail is
    /// meaningful and is never replaced by evidence from a later source or fallback body.
    #[inline]
    pub fn record_source_error(&mut self, error: &EdgeError) -> bool {
        if self.source_failure.is_some() || matches!(self.state, AttemptState::Terminal(_)) {
            return false;
        }
        self.source_failure = Some(ResponseEgressSourceFailure {
            body_kind: self.body_kind,
            category: response_egress_source_error_category(error),
            provider_detail: error.provider_failure_detail().copied(),
        });
        true
    }

    /// Copies the validated snapshot for a last-resort adapter fallback after ownership transfer.
    #[doc(hidden)]
    #[must_use]
    #[inline]
    pub fn synthesized_response_headers(&self) -> SynthesizedResponseHeaders {
        self.synthesized_headers.clone()
    }

    /// Terminalizes an attempt with the supplied outcome.
    ///
    /// `Completed` follows [`Self::complete`] state validation.
    #[inline]
    pub fn terminate(
        &mut self,
        outcome: ResponseEgressOutcome,
        observed_at: MonotonicInstant,
    ) -> bool {
        if self.body_kind == ResponseEgressBodyKind::Fallback {
            return false;
        }
        if matches!(
            outcome,
            ResponseEgressOutcome::Completed | ResponseEgressOutcome::HostHandoff
        ) {
            if outcome == ResponseEgressOutcome::HostHandoff
                && matches!(self.state, AttemptState::Writing)
            {
                self.transition(outcome, observed_at)
            } else {
                self.complete(observed_at)
            }
        } else {
            self.transition(outcome, observed_at)
        }
    }

    fn transition(
        &mut self,
        outcome: ResponseEgressOutcome,
        observed_at: MonotonicInstant,
    ) -> bool {
        if matches!(self.state, AttemptState::Terminal(_)) {
            return false;
        }
        self.publish_report(outcome, observed_at);
        true
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
)]
impl Drop for ResponseEgressAttempt {
    #[inline]
    fn drop(&mut self) {
        let outcome = match self.state {
            AttemptState::Initial => ResponseEgressOutcome::ConversionError,
            AttemptState::Writing => ResponseEgressOutcome::TransportError,
            AttemptState::Terminal(_) => return,
        };
        self.fail_current_body(outcome, self.clock.now());
    }
}

fn close_response_egress_resource<T>(shared: &Mutex<ResponseEgressResourceState<T>>) {
    let resource = {
        let mut state = shared.lock().unwrap_or_else(PoisonError::into_inner);
        state.closed = true;
        state.resource.take()
    };
    drop(resource);
}

/// Selects the portable default response-write policy.
#[must_use]
#[inline]
pub fn default_response_egress_policy(
    _head: &ResponseEgressHead<'_>,
    egress_started_at: MonotonicInstant,
) -> ResponseEgressPolicy {
    let deadline = egress_started_at
        .checked_add(DEFAULT_RESPONSE_WRITE_BUDGET)
        .unwrap_or(egress_started_at);
    ResponseEgressPolicy {
        write_deadline: Deadline::at_instant(deadline),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::error::Error;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::ptr::from_ref;
    use std::slice::from_ref as slice_from_ref;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::body::Body;
    use crate::error::{BadGatewayReason, EdgeError};
    use crate::error_detail::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};
    use crate::http::{HeaderMap, HeaderValue, Method, StatusCode, Version, response_builder};
    use crate::router::RouteMetadata;
    use crate::time::{DEADLINE_FAR_FUTURE, Deadline, MonotonicClock, MonotonicInstant};

    #[derive(Clone, Default)]
    struct RecordingObserver {
        reports: Arc<Mutex<Vec<ResponseEgressReport>>>,
    }

    impl RecordingObserver {
        fn reports(&self) -> Vec<ResponseEgressReport> {
            self.reports.lock().expect("reports lock").clone()
        }
    }

    impl ResponseEgressObserver for RecordingObserver {
        fn complete(&self, report: &ResponseEgressReport) {
            self.reports
                .lock()
                .expect("reports lock")
                .push(report.clone());
        }
    }

    struct PanickingObserver;

    impl ResponseEgressObserver for PanickingObserver {
        fn complete(&self, _report: &ResponseEgressReport) {
            panic!("observer panic");
        }
    }

    #[derive(Clone)]
    struct OrderedObserver(Arc<Mutex<Vec<&'static str>>>);

    impl ResponseEgressObserver for OrderedObserver {
        fn complete(&self, _report: &ResponseEgressReport) {
            self.0.lock().expect("ordered events lock").push("observer");
        }
    }

    struct CompletionDropProbe(Arc<Mutex<usize>>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for CompletionDropProbe {
        fn drop(&mut self) {
            let mut drops = self.0.lock().expect("completion drops lock");
            *drops = (*drops).saturating_add(1);
        }
    }

    struct ResourceDropProbe(Arc<AtomicUsize>);

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for ResourceDropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn head<'head>(
        headers: &'head HeaderMap,
        request_start: MonotonicInstant,
        route: Option<&'head RouteMetadata>,
    ) -> ResponseEgressHead<'head> {
        ResponseEgressHead::new(
            StatusCode::CREATED,
            Version::HTTP_2,
            headers,
            None,
            request_start,
            route,
        )
    }

    fn completed_report(request_start: MonotonicInstant) -> ResponseEgressReport {
        ResponseEgressReport {
            body_kind: ResponseEgressBodyKind::Application,
            bytes_written: 0,
            elapsed: Duration::ZERO,
            fallback_disposition: None,
            framing_failures: ResponseEgressFramingFailures::default(),
            outcome: ResponseEgressOutcome::Completed,
            request_start,
            route: None,
            source_failure: None,
        }
    }

    fn attempt(
        observer: &RecordingObserver,
        started_at: MonotonicInstant,
    ) -> ResponseEgressAttempt {
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);
        ResponseEgressAttempt::new(
            &head,
            started_at,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        )
    }

    #[test]
    fn terminal_report_extraction_preserves_callbacks_and_drop_semantics() {
        for (fallback, writing, terminal) in [
            (false, false, false),
            (false, true, false),
            (false, true, true),
            (true, false, false),
            (true, true, false),
            (true, true, true),
        ] {
            let observed = RecordingObserver::default();
            let completed = Arc::new(Mutex::new(Vec::new()));
            let captured = Arc::clone(&completed);
            let reads = Arc::new(AtomicUsize::new(0));
            let clock_reads = Arc::clone(&reads);
            let now = MonotonicInstant::now();
            let headers = HeaderMap::new();
            let mut attempt = ResponseEgressAttempt::new(
                &head(&headers, now, None),
                now,
                ResponseEgressCompletion::new(move |report| {
                    captured.lock().unwrap().push(report.clone());
                }),
                ResponseEgressObserverHandle::new(observed.clone()),
                MonotonicClock::new(move || {
                    clock_reads.fetch_add(1, Ordering::SeqCst);
                    now
                }),
            );
            attempt.record_source_error(&EdgeError::bad_request("private source"));
            prepare_extraction_state(&mut attempt, now, (fallback, writing, terminal));
            let before_reads = reads.load(Ordering::SeqCst);
            let report = attempt.into_terminal_report();
            assert_eq!(
                reads.load(Ordering::SeqCst) - before_reads,
                usize::from(!terminal)
            );
            assert_eq!(
                completed.lock().unwrap().as_slice(),
                slice_from_ref(&report)
            );
            assert_eq!(
                observed.reports.lock().unwrap().as_slice(),
                slice_from_ref(&report)
            );
            assert!(report.source_failure.is_some());
            assert_eq!(report.bytes_written, if writing { 3 } else { 0 });
            if fallback {
                assert_eq!(report.body_kind, ResponseEgressBodyKind::Fallback);
                assert_eq!(report.outcome, ResponseEgressOutcome::ConversionError);
                assert_eq!(
                    report.fallback_disposition,
                    Some(if terminal {
                        ResponseEgressFallbackDisposition::Completed
                    } else {
                        ResponseEgressFallbackDisposition::Aborted
                    })
                );
            } else {
                assert_eq!(report.body_kind, ResponseEgressBodyKind::Application);
                assert_eq!(
                    report.outcome,
                    if terminal {
                        ResponseEgressOutcome::Completed
                    } else if writing {
                        ResponseEgressOutcome::TransportError
                    } else {
                        ResponseEgressOutcome::ConversionError
                    }
                );
            }
        }
    }

    fn prepare_extraction_state(
        attempt: &mut ResponseEgressAttempt,
        now: MonotonicInstant,
        (fallback, writing, terminal): (bool, bool, bool),
    ) {
        if fallback {
            assert!(attempt.begin_fallback(ResponseEgressOutcome::ConversionError));
        }
        if writing {
            assert!(attempt.begin_writing());
            assert!(attempt.account_bytes(3, now));
        }
        if !terminal {
            return;
        }
        if fallback {
            assert!(attempt.finish_fallback(ResponseEgressFallbackDisposition::Completed, now));
        } else {
            assert!(attempt.complete(now));
        }
    }

    #[test]
    fn provider_diagnostics_reach_completion_and_observer_without_terminal_changes() {
        let detail = ProviderFailureDetail::Fastly {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: FastlyFailureCause::ConnectionTerminated,
        };
        for with_detail in [false, true] {
            for (fallback_stage, observed_body, terminal_outcome) in [
                (
                    0_u8,
                    ResponseEgressBodyKind::Application,
                    ResponseEgressOutcome::SourceError,
                ),
                (
                    1,
                    ResponseEgressBodyKind::Application,
                    ResponseEgressOutcome::ConversionError,
                ),
                (
                    2,
                    ResponseEgressBodyKind::Fallback,
                    ResponseEgressOutcome::ConversionError,
                ),
            ] {
                let observer = RecordingObserver::default();
                let completed = Arc::new(Mutex::new(Vec::new()));
                let completed_reports = Arc::clone(&completed);
                let started_at = MonotonicInstant::now();
                let headers = HeaderMap::new();
                let mut attempt = ResponseEgressAttempt::new(
                    &head(&headers, started_at, None),
                    started_at,
                    ResponseEgressCompletion::new(move |report| {
                        completed_reports
                            .lock()
                            .expect("completed reports")
                            .push(report.clone());
                    }),
                    ResponseEgressObserverHandle::new(observer.clone()),
                    MonotonicClock::new(move || started_at),
                );
                if fallback_stage == 2 {
                    assert!(attempt.begin_fallback(ResponseEgressOutcome::ConversionError));
                }
                let error = if with_detail {
                    EdgeError::bad_gateway_with_detail(
                        "provider-secret-token",
                        BadGatewayReason::Transport,
                        detail,
                    )
                } else {
                    EdgeError::bad_gateway_with_reason(
                        "provider-secret-token",
                        BadGatewayReason::Transport,
                    )
                };
                assert!(attempt.record_source_error(&error));
                assert!(
                    !attempt.record_source_error(&EdgeError::bad_gateway_with_detail(
                        "different-secret",
                        BadGatewayReason::Unreachable,
                        ProviderFailureDetail::Fastly {
                            phase: ProviderFailurePhase::Send,
                            cause: FastlyFailureCause::DnsTimeout
                        },
                    ))
                );
                if fallback_stage == 1 {
                    assert!(attempt.begin_fallback(ResponseEgressOutcome::ConversionError));
                }
                assert!(attempt.begin_writing());
                if fallback_stage == 0 {
                    assert!(attempt.terminate(ResponseEgressOutcome::SourceError, started_at));
                } else {
                    assert!(
                        attempt.finish_fallback(
                            ResponseEgressFallbackDisposition::Completed,
                            started_at
                        )
                    );
                }
                assert!(!attempt.record_source_error(&error));
                assert!(!attempt.terminate(ResponseEgressOutcome::TransportError, started_at));
                drop(attempt);
                let reports = observer.reports();
                assert_eq!(reports.len(), 1);
                assert_eq!(*completed.lock().expect("completion"), reports);
                let failure = reports[0].source_failure.as_ref().expect("source failure");
                assert_eq!(failure.category(), "bad_gateway_transport");
                assert_eq!(
                    failure.provider_failure_detail(),
                    with_detail.then_some(&detail)
                );
                assert_eq!(failure.body_kind(), observed_body);
                assert!(!format!("{failure:?}").contains("secret"));
                assert_eq!(reports[0].outcome, terminal_outcome);
                assert_eq!(reports[0].bytes_written, 0);
                assert_eq!(reports[0].elapsed, Duration::ZERO);
            }
        }
    }

    #[test]
    fn recording_source_failure_has_no_clock_or_terminal_side_effects() {
        for writing in [false, true] {
            let now = MonotonicInstant::now();
            let reads = Arc::new(AtomicUsize::new(0));
            let clock_reads = Arc::clone(&reads);
            let clock = MonotonicClock::new(move || {
                clock_reads.fetch_add(1, Ordering::SeqCst);
                now
            });
            let observer = RecordingObserver::default();
            let headers = HeaderMap::new();
            let mut attempt = ResponseEgressAttempt::new(
                &head(&headers, now, None),
                now,
                ResponseEgressCompletion::empty(),
                ResponseEgressObserverHandle::new(observer.clone()),
                clock,
            );
            if writing {
                assert!(attempt.begin_writing());
            }
            assert!(
                attempt.record_source_error(&EdgeError::internal(anyhow::anyhow!(
                    "provider-secret-token"
                )))
            );
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            assert!(observer.reports().is_empty());
            drop(attempt);
            assert_eq!(reads.load(Ordering::SeqCst), 1);
            let reports = observer.reports();
            assert_eq!(reports.len(), 1);
            assert_eq!(
                reports[0].outcome,
                if writing {
                    ResponseEgressOutcome::TransportError
                } else {
                    ResponseEgressOutcome::ConversionError
                }
            );
            let failure = reports[0].source_failure.as_ref().expect("source failure");
            assert_eq!(failure.category(), "internal");
            assert!(failure.provider_failure_detail().is_none());
            assert!(!format!("{failure:?}").contains("secret"));
        }
    }

    #[test]
    fn terminal_without_source_failure_rejects_later_observation() {
        let observer = RecordingObserver::default();
        let now = MonotonicInstant::now();
        let mut attempt = attempt(&observer, now);
        assert!(attempt.begin_writing());
        assert!(attempt.complete(now));
        assert!(!attempt.record_source_error(&EdgeError::bad_gateway("later-secret")));
        drop(attempt);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].source_failure.is_none());
    }

    #[test]
    fn framing_observer_preserves_each_typed_reason() {
        use crate::response_egress_framing::test_support::{FRAMING_RULES, framing_fixture};
        use futures::executor::block_on;

        for reason in FRAMING_RULES {
            for detached in [false, true] {
                let fixture = block_on(framing_fixture(reason, detached)).expect("fixture");
                let Err(failure) = fixture
                    .envelope
                    .begin(ResponseEgressFramingMode::ExplicitBufferedLength)
                else {
                    panic!("framing must reject the response")
                };
                let (mut attempt, clock, cause) = failure.into_parts();
                assert_eq!(cause, ResponseEgressOutcome::ConversionError);
                assert!(attempt.begin_fallback(cause));
                assert!(attempt.begin_writing());
                assert!(
                    attempt
                        .finish_fallback(ResponseEgressFallbackDisposition::Completed, clock.now())
                );
                let completed = fixture.completed.lock().expect("completion");
                assert_eq!(completed.len(), 1);
                assert_eq!(completed[0].framing_failures.application(), Some(reason));
                assert_eq!(completed[0].framing_failures.fallback(), None);
                assert!(completed[0].source_failure.is_none());
                assert!(!format!("{:?}", completed[0]).contains("secret"));
                let observed = fixture.observed.lock().expect("observer");
                if detached {
                    assert!(observed.is_empty());
                } else {
                    assert_eq!(*observed, *completed);
                }
            }
        }
    }

    #[test]
    fn framing_observations_preserve_both_body_slots_without_sampling_the_clock() {
        use crate::response_egress_framing::test_support::framing_response;

        let now = MonotonicInstant::now();
        let observer = RecordingObserver::default();
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        let clock = MonotonicClock::new(move || {
            clock_reads.fetch_add(1, Ordering::SeqCst);
            now
        });
        let headers = HeaderMap::new();
        let mut attempt = ResponseEgressAttempt::new(
            &head(&headers, now, None),
            now,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            clock,
        );
        assert!(attempt.record_source_error(&EdgeError::bad_gateway("private token=secret")));
        for reason in [
            ResponseEgressFramingError::InvalidContentLength,
            ResponseEgressFramingError::UnsupportedStatus,
        ] {
            let (method, response) = framing_response(reason);
            assert_eq!(
                attempt
                    .prepare_response(
                        ResponseEgressFramingMode::ExplicitBufferedLength,
                        &method,
                        response
                    )
                    .err(),
                Some(reason)
            );
        }
        assert!(attempt.begin_fallback(ResponseEgressOutcome::ConversionError));
        let reason = ResponseEgressFramingError::UnsupportedTrailers;
        let (method, response) = framing_response(reason);
        assert_eq!(
            attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    &method,
                    response
                )
                .err(),
            Some(reason)
        );
        let (later_method, later_response) =
            framing_response(ResponseEgressFramingError::UnsupportedStatus);
        assert_eq!(
            attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    &later_method,
                    later_response
                )
                .err(),
            Some(ResponseEgressFramingError::UnsupportedStatus)
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(observer.reports().is_empty());
        assert!(attempt.begin_writing());
        assert!(attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, now));
        let (terminal_method, terminal_response) =
            framing_response(ResponseEgressFramingError::UnsupportedTunnel);
        assert_eq!(
            attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    &terminal_method,
                    terminal_response
                )
                .err(),
            Some(ResponseEgressFramingError::UnsupportedTunnel)
        );
        drop(attempt);
        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].framing_failures.application(),
            Some(ResponseEgressFramingError::InvalidContentLength)
        );
        assert_eq!(reports[0].framing_failures.fallback(), Some(reason));
        assert_eq!(
            reports[0]
                .source_failure
                .expect("source observation")
                .category(),
            "bad_gateway_unspecified"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn framing_fallback_slot_retains_each_rule_from_the_real_normalizer() {
        use crate::response_egress_framing::test_support::{FRAMING_RULES, framing_response};
        for reason in FRAMING_RULES {
            let now = MonotonicInstant::now();
            let observer = RecordingObserver::default();
            let mut attempt = attempt(&observer, now);
            assert!(attempt.begin_fallback(ResponseEgressOutcome::ConversionError));
            let (method, response) = framing_response(reason);
            assert_eq!(
                attempt
                    .prepare_response(
                        ResponseEgressFramingMode::ExplicitBufferedLength,
                        &method,
                        response
                    )
                    .err(),
                Some(reason)
            );
            assert!(attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, now));
            let report = observer.reports().remove(0);
            assert_eq!(report.framing_failures.application(), None);
            assert_eq!(report.framing_failures.fallback(), Some(reason));
            assert_eq!(report.body_kind, ResponseEgressBodyKind::Fallback);
            assert_eq!(report.outcome, ResponseEgressOutcome::ConversionError);
            assert!(report.source_failure.is_none());
        }
    }

    #[test]
    fn expired_policy_does_not_invent_a_framing_reason() {
        let now = MonotonicInstant::now();
        let observer = RecordingObserver::default();
        let method = Method::GET;
        let response = Response::new(Body::from("valid"));
        let envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(method, now, None),
            ResponseEgressCompletion::empty(),
            Arc::new(move |_, _| ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(now),
            }),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::new(move || now),
        );
        let Ok((_prepared, policy, mut attempt, clock)) =
            envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("valid expired policy remains valid")
        };
        assert!(policy.write_deadline.is_expired_at(clock.now()));
        assert!(attempt.terminate(ResponseEgressOutcome::DeadlineExceeded, now));
        assert_eq!(
            observer.reports()[0].framing_failures,
            ResponseEgressFramingFailures::default()
        );
    }

    #[test]
    fn framing_rejection_after_terminal_does_not_create_an_observation() {
        use crate::response_egress_framing::test_support::framing_response;
        let now = MonotonicInstant::now();
        let observer = RecordingObserver::default();
        let mut attempt = attempt(&observer, now);
        assert!(attempt.begin_writing());
        assert!(attempt.complete(now));
        let (method, response) = framing_response(ResponseEgressFramingError::UnsupportedTrailers);
        assert_eq!(
            attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    &method,
                    response
                )
                .err(),
            Some(ResponseEgressFramingError::UnsupportedTrailers)
        );
        assert_eq!(
            attempt.framing_failures,
            ResponseEgressFramingFailures::default()
        );
        drop(attempt);
        assert_eq!(observer.reports().len(), 1);
        assert_eq!(
            observer.reports()[0].framing_failures,
            ResponseEgressFramingFailures::default()
        );
    }

    #[test]
    fn writing_attempt_does_not_mislabel_a_late_normalization_error() {
        use crate::response_egress_framing::test_support::framing_response;
        let now = MonotonicInstant::now();
        let observer = RecordingObserver::default();
        let reads = Arc::new(AtomicUsize::new(0));
        let clock_reads = Arc::clone(&reads);
        let clock = MonotonicClock::new(move || {
            clock_reads.fetch_add(1, Ordering::SeqCst);
            now
        });
        let headers = HeaderMap::new();
        let mut attempt = ResponseEgressAttempt::new(
            &head(&headers, now, None),
            now,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            clock,
        );
        assert!(attempt.begin_writing());
        let (method, response) = framing_response(ResponseEgressFramingError::UnsupportedTrailers);
        assert_eq!(
            attempt
                .prepare_response(
                    ResponseEgressFramingMode::ExplicitBufferedLength,
                    &method,
                    response
                )
                .err(),
            Some(ResponseEgressFramingError::UnsupportedTrailers)
        );
        assert!(observer.reports().is_empty());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(attempt.terminate(ResponseEgressOutcome::SourceError, now));
        drop(attempt);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            observer.reports()[0].framing_failures,
            ResponseEgressFramingFailures::default()
        );
    }

    #[test]
    fn default_policy_is_finite_and_exactly_thirty_seconds() {
        let headers = HeaderMap::new();
        let started_at = MonotonicInstant::now();
        let head = head(&headers, started_at, None);
        let policy = default_response_egress_policy(&head, started_at);

        assert_eq!(
            policy.write_deadline.instant(),
            started_at
                .checked_add(DEFAULT_RESPONSE_WRITE_BUDGET)
                .expect("default deadline")
        );
        assert_eq!(DEFAULT_RESPONSE_WRITE_BUDGET, Duration::from_secs(30));
    }

    #[test]
    fn detached_response_egress_head_deadline_is_anchored_to_request_start() {
        let request_start = MonotonicInstant::now()
            .checked_sub(Duration::from_mins(1))
            .expect("earlier request start");
        let head = DetachedResponseEgressHead::new(
            &Method::GET,
            request_start,
            StatusCode::BAD_REQUEST,
            "bad_request",
        );

        assert_eq!(
            head.write_deadline_after(Duration::from_secs(2)).instant(),
            request_start
                .checked_add(Duration::from_secs(2))
                .expect("relative deadline")
        );
    }

    #[test]
    fn detached_response_egress_head_deadline_clamps_to_far_future() {
        let request_start = MonotonicInstant::now();
        let head = DetachedResponseEgressHead::new(
            &Method::GET,
            request_start,
            StatusCode::BAD_REQUEST,
            "bad_request",
        );

        assert_eq!(
            head.write_deadline_after(Duration::MAX).instant(),
            request_start
                .checked_add(DEADLINE_FAR_FUTURE)
                .expect("maximum deadline")
        );
    }

    #[test]
    fn detached_response_egress_head_deadline_overflow_fails_closed() {
        let origin = MonotonicInstant::now();
        let mut low = 0_u64;
        let mut high = u64::MAX;
        while low < high {
            let midpoint = low + (high - low).div_ceil(2);
            if origin.checked_add(Duration::from_secs(midpoint)).is_some() {
                low = midpoint;
            } else {
                high = midpoint - 1;
            }
        }
        let request_start = origin
            .checked_add(Duration::from_secs(low))
            .expect("latest representable instant");
        assert!(request_start.checked_add(Duration::from_secs(1)).is_none());
        let head = DetachedResponseEgressHead::new(
            &Method::GET,
            request_start,
            StatusCode::BAD_REQUEST,
            "bad_request",
        );

        assert_eq!(
            head.write_deadline_after(Duration::from_secs(1)).instant(),
            request_start
        );
    }

    #[test]
    fn policy_normalization_clamps_far_future_and_preserves_expiry() {
        let started_at = MonotonicInstant::now();
        let maximum = started_at
            .checked_add(DEADLINE_FAR_FUTURE)
            .expect("maximum deadline");
        let beyond = maximum
            .checked_add(Duration::from_secs(1))
            .expect("deadline beyond maximum");
        let clamped = ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(beyond),
        }
        .normalize_at(started_at)
        .expect("normalization");
        assert_eq!(clamped.write_deadline.instant(), maximum);

        let expired = ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(started_at),
        }
        .normalize_at(started_at)
        .expect("expired policy remains valid");
        assert_eq!(expired.write_deadline.instant(), started_at);
    }

    #[test]
    fn head_is_body_blind_and_exposes_only_immutable_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert("x-test", HeaderValue::from_static("visible"));
        let request_start = MonotonicInstant::now();
        let route = RouteMetadata::new(Method::GET, "/items/{id}");
        let head = head(&headers, request_start, Some(&route));

        assert_eq!(head.status(), StatusCode::CREATED);
        assert_eq!(head.version(), Version::HTTP_2);
        assert_eq!(head.headers(), &headers);
        assert_eq!(head.request_start(), request_start);
        assert_eq!(head.route(), Some(&route));
    }

    #[test]
    fn observer_trait_is_object_safe_and_handle_has_a_noop_default() {
        fn assert_object_safe(_observer: &dyn ResponseEgressObserver) {}

        let observer = RecordingObserver::default();
        assert_object_safe(&observer);
        let _: ResponseEgressObserverHandle = ResponseEgressObserverHandle::default();
    }

    #[test]
    fn late_bound_resource_is_send_and_sync_for_send_not_sync_values() {
        fn assert_error<T>()
        where
            T: Error,
        {
        }
        fn assert_send_sync<T>()
        where
            T: Send + Sync,
        {
        }

        assert_error::<ResponseEgressResourceInstallError>();
        assert_send_sync::<ResponseEgressResource<Cell<u8>>>();
    }

    #[test]
    fn late_bound_install_and_close_race_releases_candidate_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let close_barrier = Arc::clone(&barrier);
        let (resource, completion) = ResponseEgressCompletion::late_bound();

        let install_result = thread::scope(|scope| {
            let close = scope.spawn(move || {
                close_barrier.wait();
                drop(completion);
            });
            barrier.wait();
            let result = resource.install(ResourceDropProbe(Arc::clone(&drops)));
            close.join().expect("close thread");
            result
        });

        assert!(matches!(
            install_result,
            Ok(()) | Err(ResponseEgressResourceInstallError::Closed)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_poisoned_state_recovers_and_releases_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        let poisoned_state = Arc::clone(&resource.state);
        let poison_result = catch_unwind(AssertUnwindSafe(move || {
            let _guard = poisoned_state.lock().expect("unpoisoned state");
            panic!("poison response resource state");
        }));
        assert!(poison_result.is_err());

        resource
            .install(ResourceDropProbe(Arc::clone(&drops)))
            .expect("poison recovery installation");
        drop(completion);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_install_before_terminal_releases_at_terminal_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (resource, mut completion) = ResponseEgressCompletion::late_bound();
        resource
            .install(ResourceDropProbe(Arc::clone(&drops)))
            .expect("first installation");

        assert_eq!(drops.load(Ordering::SeqCst), 0);
        completion.complete(&completed_report(MonotonicInstant::now()));
        completion.complete(&completed_report(MonotonicInstant::now()));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        drop(completion);
        drop(resource);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_terminal_before_install_rejects_and_releases_candidate() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (resource, mut completion) = ResponseEgressCompletion::late_bound();
        completion.complete(&completed_report(MonotonicInstant::now()));

        assert_eq!(
            resource.install(ResourceDropProbe(Arc::clone(&drops))),
            Err(ResponseEgressResourceInstallError::Closed)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_duplicate_install_rejects_and_releases_only_candidate() {
        let installed_drops = Arc::new(AtomicUsize::new(0));
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        resource
            .install(ResourceDropProbe(Arc::clone(&installed_drops)))
            .expect("first installation");

        assert_eq!(
            resource.install(ResourceDropProbe(Arc::clone(&rejected_drops))),
            Err(ResponseEgressResourceInstallError::AlreadyInstalled)
        );
        assert_eq!(installed_drops.load(Ordering::SeqCst), 0);
        assert_eq!(rejected_drops.load(Ordering::SeqCst), 1);

        drop(completion);
        assert_eq!(installed_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_abandonment_before_install_closes_surviving_installer() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        drop(completion);

        assert_eq!(
            resource.install(ResourceDropProbe(Arc::clone(&drops))),
            Err(ResponseEgressResourceInstallError::Closed)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn late_bound_abandonment_after_install_releases_with_surviving_installer() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        resource
            .install(ResourceDropProbe(Arc::clone(&drops)))
            .expect("first installation");

        drop(completion);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            resource.install(ResourceDropProbe(Arc::clone(&drops))),
            Err(ResponseEgressResourceInstallError::Closed)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn late_bound_competing_terminal_signals_release_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        resource
            .install(ResourceDropProbe(Arc::clone(&drops)))
            .expect("first installation");
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            started_at,
            completion,
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        );

        assert!(attempt.begin_writing());
        assert!(attempt.complete(started_at));
        assert!(!attempt.complete(started_at));
        assert!(!attempt.terminate(ResponseEgressOutcome::TransportError, started_at));
        drop(attempt);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(observer.reports().len(), 1);
    }

    #[test]
    fn late_bound_unbegun_envelope_releases_without_reporting() {
        let drops = Arc::new(AtomicUsize::new(0));
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let response = response_builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("response");
        let (resource, completion) = ResponseEgressCompletion::late_bound();
        resource
            .install(ResourceDropProbe(Arc::clone(&drops)))
            .expect("first installation");

        drop(ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, started_at, None),
            completion,
            Arc::new(default_response_egress_policy),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        ));

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(observer.reports().is_empty());
    }

    #[test]
    fn response_completion_runs_once_before_the_global_observer() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let completion_events = Arc::clone(&events);
        let observer = OrderedObserver(Arc::clone(&events));
        let started_at = MonotonicInstant::now();
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            started_at,
            ResponseEgressCompletion::new(move |_| {
                completion_events
                    .lock()
                    .expect("ordered events lock")
                    .push("completion");
            }),
            ResponseEgressObserverHandle::new(observer),
            MonotonicClock::default(),
        );

        assert!(attempt.begin_writing());
        assert!(attempt.complete(started_at));
        assert!(!attempt.complete(started_at));
        assert!(!attempt.terminate(ResponseEgressOutcome::TransportError, started_at));
        drop(attempt);

        assert_eq!(
            events.lock().expect("ordered events lock").as_slice(),
            &["completion", "observer"]
        );
    }

    #[test]
    fn response_completion_panic_does_not_suppress_the_global_observer() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut attempt = ResponseEgressAttempt::new(
                &head,
                started_at,
                ResponseEgressCompletion::new(|_| panic!("completion panic")),
                ResponseEgressObserverHandle::new(observer.clone()),
                MonotonicClock::default(),
            );
            assert!(attempt.terminate(ResponseEgressOutcome::ConversionError, started_at));
        }));

        result.unwrap_or_else(|_| panic!("completion panic escaped terminal transition"));
        assert_eq!(observer.reports().len(), 1);
    }

    #[test]
    fn response_egress_completion_join_runs_left_to_right_once_with_same_report() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let left_events = Arc::clone(&events);
        let right_events = Arc::clone(&events);
        let mut completion = ResponseEgressCompletion::new(move |report| {
            left_events.lock().expect("completion events lock").push((
                "left",
                from_ref(report).addr(),
                report.clone(),
            ));
        })
        .join(ResponseEgressCompletion::new(move |report| {
            right_events.lock().expect("completion events lock").push((
                "right",
                from_ref(report).addr(),
                report.clone(),
            ));
        }));
        let report = completed_report(MonotonicInstant::now());

        completion.complete(&report);
        completion.complete(&report);

        let recorded_events = events.lock().expect("completion events lock");
        assert_eq!(recorded_events.len(), 2);
        assert_eq!(recorded_events[0].0, "left");
        assert_eq!(recorded_events[1].0, "right");
        assert_eq!(recorded_events[0].1, recorded_events[1].1);
        assert_eq!(recorded_events[0].2, report);
        assert_eq!(recorded_events[1].2, report);
    }

    #[cfg(panic = "unwind")]
    #[test]
    fn response_egress_completion_join_left_panic_does_not_suppress_right() {
        let right_calls = Arc::new(AtomicUsize::new(0));
        let observed_right_calls = Arc::clone(&right_calls);
        let mut completion = ResponseEgressCompletion::new(|_| panic!("left completion panic"))
            .join(ResponseEgressCompletion::new(move |_| {
                observed_right_calls.fetch_add(1, Ordering::SeqCst);
            }));
        let report = completed_report(MonotonicInstant::now());

        let result = catch_unwind(AssertUnwindSafe(|| completion.complete(&report)));

        result.unwrap_or_else(|_| panic!("left completion panic escaped join"));
        assert_eq!(right_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn response_egress_completion_join_abandonment_releases_both_resources_once() {
        let left_drops = Arc::new(AtomicUsize::new(0));
        let right_drops = Arc::new(AtomicUsize::new(0));
        let left_probe = ResourceDropProbe(Arc::clone(&left_drops));
        let right_probe = ResourceDropProbe(Arc::clone(&right_drops));
        let completion = ResponseEgressCompletion::new(move |_| {
            let _retain_until_completion = &left_probe;
        })
        .join(ResponseEgressCompletion::new(move |_| {
            let _retain_until_completion = &right_probe;
        }));

        drop(completion);

        assert_eq!(left_drops.load(Ordering::SeqCst), 1);
        assert_eq!(right_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn response_egress_completion_join_repeated_terminal_attempts_run_each_once() {
        let left_calls = Arc::new(AtomicUsize::new(0));
        let right_calls = Arc::new(AtomicUsize::new(0));
        let observed_left_calls = Arc::clone(&left_calls);
        let observed_right_calls = Arc::clone(&right_calls);
        let completion = ResponseEgressCompletion::new(move |_| {
            observed_left_calls.fetch_add(1, Ordering::SeqCst);
        })
        .join(ResponseEgressCompletion::new(move |_| {
            observed_right_calls.fetch_add(1, Ordering::SeqCst);
        }));
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            started_at,
            completion,
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        );

        assert!(attempt.begin_writing());
        assert!(attempt.complete(started_at));
        assert!(!attempt.complete(started_at));
        assert!(!attempt.terminate(ResponseEgressOutcome::TransportError, started_at));
        drop(attempt);

        assert_eq!(left_calls.load(Ordering::SeqCst), 1);
        assert_eq!(right_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observer.reports().len(), 1);
    }

    #[test]
    fn unbegun_envelope_drops_completion_without_a_terminal_report() {
        let observer = RecordingObserver::default();
        let callback_calls = Arc::new(Mutex::new(0_usize));
        let completion_drops = Arc::new(Mutex::new(0_usize));
        let observed_calls = Arc::clone(&callback_calls);
        let probe = CompletionDropProbe(Arc::clone(&completion_drops));
        let started_at = MonotonicInstant::now();
        let response = response_builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .expect("response");

        drop(ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, started_at, None),
            ResponseEgressCompletion::new(move |_| {
                let _keep_probe_until_completion = &probe;
                let mut calls = observed_calls.lock().expect("completion calls lock");
                *calls += 1;
            }),
            Arc::new(default_response_egress_policy),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        ));

        assert_eq!(*callback_calls.lock().expect("completion calls lock"), 0);
        assert_eq!(*completion_drops.lock().expect("completion drops lock"), 1);
        assert!(observer.reports().is_empty());
    }

    #[test]
    fn every_outcome_is_reported_without_collapsing_variants_from_valid_states() {
        let outcomes = [
            ResponseEgressOutcome::Completed,
            ResponseEgressOutcome::ClientDisconnected,
            ResponseEgressOutcome::ConversionError,
            ResponseEgressOutcome::DeadlineExceeded,
            ResponseEgressOutcome::HostHandoff,
            ResponseEgressOutcome::RequestCancelled,
            ResponseEgressOutcome::SourceError,
            ResponseEgressOutcome::TransportError,
            ResponseEgressOutcome::Unspecified,
        ];

        for outcome in outcomes {
            let states = if matches!(
                outcome,
                ResponseEgressOutcome::Completed | ResponseEgressOutcome::HostHandoff
            ) {
                &[true][..]
            } else {
                &[false, true][..]
            };
            for &writing in states {
                let observer = RecordingObserver::default();
                let started_at = MonotonicInstant::now();
                let mut attempt = attempt(&observer, started_at);
                if writing {
                    assert!(attempt.begin_writing());
                }
                if outcome == ResponseEgressOutcome::Completed {
                    assert!(attempt.complete(started_at));
                } else {
                    assert!(attempt.terminate(outcome, started_at));
                }
                assert_eq!(observer.reports()[0].outcome, outcome);
            }
        }
    }

    #[test]
    fn completed_cannot_skip_the_writing_state() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut attempt = attempt(&observer, started_at);

        assert!(attempt.complete(started_at));
        assert_eq!(observer.reports().len(), 1);
        assert_eq!(
            observer.reports()[0].outcome,
            ResponseEgressOutcome::ConversionError
        );
    }

    #[test]
    fn host_handoff_cannot_skip_the_writing_state() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut attempt = attempt(&observer, started_at);

        assert!(attempt.terminate(ResponseEgressOutcome::HostHandoff, started_at));
        assert_eq!(observer.reports().len(), 1);
        assert_eq!(
            observer.reports()[0].outcome,
            ResponseEgressOutcome::ConversionError
        );
    }

    #[test]
    fn writing_completion_records_metadata_bytes_and_elapsed_once() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let terminal_at = started_at
            .checked_add(Duration::from_millis(25))
            .expect("terminal instant");
        let request_start = started_at
            .checked_sub(Duration::from_millis(10))
            .expect("request start");
        let route = RouteMetadata::new(Method::POST, "/submit");
        let headers = HeaderMap::new();
        let head = head(&headers, request_start, Some(&route));
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            started_at,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            MonotonicClock::default(),
        );

        assert!(attempt.begin_writing());
        assert!(attempt.account_bytes(0, started_at));
        assert!(attempt.account_bytes(7, started_at));
        assert!(attempt.complete(terminal_at));
        assert!(!attempt.complete(terminal_at));
        assert!(!attempt.terminate(ResponseEgressOutcome::TransportError, terminal_at));
        drop(attempt);

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].bytes_written, 7);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Application);
        assert_eq!(reports[0].elapsed, Duration::from_millis(25));
        assert_eq!(reports[0].fallback_disposition, None);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::Completed);
        assert_eq!(reports[0].request_start, request_start);
        assert_eq!(reports[0].route, Some(route));
    }

    #[test]
    fn drop_and_duplicate_signals_preserve_exactly_once_terminal_state() {
        let initial_observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        drop(attempt(&initial_observer, started_at));
        assert_eq!(initial_observer.reports().len(), 1);
        assert_eq!(
            initial_observer.reports()[0].outcome,
            ResponseEgressOutcome::ConversionError
        );

        let writing_observer = RecordingObserver::default();
        let mut writing = attempt(&writing_observer, started_at);
        assert!(writing.begin_writing());
        assert!(!writing.begin_writing());
        drop(writing);
        assert_eq!(writing_observer.reports().len(), 1);
        assert_eq!(
            writing_observer.reports()[0].outcome,
            ResponseEgressOutcome::TransportError
        );

        let disconnected_observer = RecordingObserver::default();
        let mut disconnected = attempt(&disconnected_observer, started_at);
        assert!(disconnected.begin_writing());
        assert!(disconnected.terminate(ResponseEgressOutcome::ClientDisconnected, started_at));
        drop(disconnected);
        assert_eq!(disconnected_observer.reports().len(), 1);
    }

    #[test]
    fn dropped_attempt_uses_its_injected_clock() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let completed_at = started_at
            .checked_add(Duration::from_millis(17))
            .expect("completed instant");
        let clock = MonotonicClock::new(move || completed_at);
        let headers = HeaderMap::new();
        let head = head(&headers, started_at, None);

        drop(ResponseEgressAttempt::new(
            &head,
            started_at,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::new(observer.clone()),
            clock,
        ));

        assert_eq!(observer.reports()[0].elapsed, Duration::from_millis(17));
    }

    #[test]
    fn byte_overflow_terminalizes_as_transport_error() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut attempt = attempt(&observer, started_at);
        assert!(attempt.begin_writing());
        assert!(attempt.account_bytes(u64::MAX, started_at));
        assert!(!attempt.account_bytes(1, started_at));
        assert!(!attempt.complete(started_at));

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].bytes_written, u64::MAX);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::TransportError);
    }

    #[test]
    fn zero_byte_completion_has_zero_accounting() {
        let completed_observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut completed = attempt(&completed_observer, started_at);
        assert!(completed.begin_writing());
        assert!(completed.complete(started_at));
        assert_eq!(completed_observer.reports()[0].bytes_written, 0);
    }

    #[test]
    fn backwards_clock_is_zero_elapsed_unspecified_and_still_once() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let earlier = started_at
            .checked_sub(Duration::from_millis(1))
            .expect("earlier instant");
        let mut attempt = attempt(&observer, started_at);
        assert!(attempt.begin_writing());
        assert!(attempt.complete(earlier));
        drop(attempt);

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].elapsed, Duration::ZERO);
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::Unspecified);
    }

    #[test]
    fn observer_panics_do_not_escape_terminal_or_drop_paths() {
        let direct = catch_unwind(AssertUnwindSafe(|| {
            let started_at = MonotonicInstant::now();
            let headers = HeaderMap::new();
            let head = head(&headers, started_at, None);
            let mut attempt = ResponseEgressAttempt::new(
                &head,
                started_at,
                ResponseEgressCompletion::empty(),
                ResponseEgressObserverHandle::new(PanickingObserver),
                MonotonicClock::default(),
            );
            assert!(attempt.terminate(ResponseEgressOutcome::ConversionError, started_at));
        }));
        direct.unwrap_or_else(|_| panic!("observer panic escaped direct completion"));

        let dropped = catch_unwind(AssertUnwindSafe(|| {
            let started_at = MonotonicInstant::now();
            let headers = HeaderMap::new();
            let head = head(&headers, started_at, None);
            drop(ResponseEgressAttempt::new(
                &head,
                started_at,
                ResponseEgressCompletion::empty(),
                ResponseEgressObserverHandle::new(PanickingObserver),
                MonotonicClock::default(),
            ));
        }));
        dropped.unwrap_or_else(|_| panic!("observer panic escaped guard drop"));
    }

    #[test]
    fn fallback_reports_its_bytes_and_disposition_with_original_cause() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut fallback = attempt(&observer, started_at);

        assert!(fallback.begin_fallback(ResponseEgressOutcome::ConversionError));
        assert!(fallback.begin_writing());
        assert!(fallback.account_bytes(7, started_at));
        assert!(
            fallback.finish_fallback(ResponseEgressFallbackDisposition::Completed, started_at,)
        );

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].bytes_written, 7);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Completed)
        );
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
    }

    #[test]
    fn fallback_selection_is_one_shot_and_preserves_its_first_cause() {
        let observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut fallback = attempt(&observer, started_at);

        assert!(fallback.begin_fallback(ResponseEgressOutcome::ConversionError));
        assert!(!fallback.begin_fallback(ResponseEgressOutcome::DeadlineExceeded));
        assert!(fallback.begin_writing());
        assert!(fallback.account_bytes(3, started_at));
        assert!(fallback.finish_fallback(ResponseEgressFallbackDisposition::Aborted, started_at,));

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].bytes_written, 3);
        assert_eq!(
            reports[0].fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Aborted)
        );
        assert_eq!(reports[0].outcome, ResponseEgressOutcome::ConversionError);
    }

    #[test]
    fn fallback_drop_and_accounting_overflow_abort_with_original_cause() {
        let drop_observer = RecordingObserver::default();
        let started_at = MonotonicInstant::now();
        let mut dropped = attempt(&drop_observer, started_at);
        assert!(!dropped.begin_fallback(ResponseEgressOutcome::SourceError));
        assert!(dropped.begin_fallback(ResponseEgressOutcome::DeadlineExceeded));
        drop(dropped);
        let dropped_report = &drop_observer.reports()[0];
        assert_eq!(
            dropped_report.outcome,
            ResponseEgressOutcome::DeadlineExceeded
        );
        assert_eq!(
            dropped_report.fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Aborted)
        );

        let overflow_observer = RecordingObserver::default();
        let mut overflow = attempt(&overflow_observer, started_at);
        assert!(overflow.begin_fallback(ResponseEgressOutcome::ConversionError));
        assert!(overflow.begin_writing());
        assert!(overflow.account_bytes(u64::MAX, started_at));
        assert!(!overflow.account_bytes(1, started_at));
        let overflow_report = &overflow_observer.reports()[0];
        assert_eq!(overflow_report.bytes_written, u64::MAX);
        assert_eq!(
            overflow_report.outcome,
            ResponseEgressOutcome::ConversionError
        );
        assert_eq!(
            overflow_report.fallback_disposition,
            Some(ResponseEgressFallbackDisposition::Aborted)
        );
    }

    #[test]
    fn policy_panic_returns_live_attempt_for_one_fallback_lifecycle() {
        let observer = RecordingObserver::default();
        let request_start = MonotonicInstant::now();
        let started_at = request_start
            .checked_add(Duration::from_millis(1))
            .expect("egress start");
        let clock = MonotonicClock::new(move || started_at);
        let response = response_builder()
            .status(StatusCode::OK)
            .body(Body::from("private token=secret"))
            .expect("response");
        let envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, request_start, None),
            ResponseEgressCompletion::empty(),
            Arc::new(|_, _| panic!("private token=secret")),
            ResponseEgressObserverHandle::new(observer.clone()),
            clock,
        );
        assert_eq!(envelope.request_method(), &Method::GET);

        let Err(failure) = envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength) else {
            panic!("policy panic must select fallback");
        };
        assert!(observer.reports().is_empty());
        let (mut attempt, failure_clock, cause) = failure.into_parts();
        assert_eq!(cause, ResponseEgressOutcome::ConversionError);
        assert_eq!(failure_clock.now(), started_at);
        assert!(attempt.begin_fallback(cause));
        assert!(attempt.begin_writing());
        assert!(attempt.finish_fallback(ResponseEgressFallbackDisposition::Completed, started_at,));

        let reports = observer.reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].body_kind, ResponseEgressBodyKind::Fallback);
        assert_eq!(reports[0].bytes_written, 0);
        assert_eq!(
            reports[0].framing_failures,
            ResponseEgressFramingFailures::default()
        );
    }

    #[test]
    fn framing_failure_drops_source_unpolled_and_returns_one_live_attempt() {
        use std::cell::Cell;
        use std::rc::Rc;
        use std::task::Poll;

        use futures_util::stream::poll_fn;

        struct DropSignal(Rc<Cell<usize>>);

        #[expect(
            clippy::missing_trait_methods,
            reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
        )]
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let observer = RecordingObserver::default();
        let request_start = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || request_start);
        let polls = Rc::new(Cell::new(0_usize));
        let drops = Rc::new(Cell::new(0_usize));
        let observed_polls = Rc::clone(&polls);
        let drop_signal = DropSignal(Rc::clone(&drops));
        let body = Body::from_stream(poll_fn(move |_| {
            let _keep_signal_alive = &drop_signal;
            observed_polls.set(observed_polls.get() + 1);
            Poll::Ready(Some(Ok(bytes::Bytes::from_static(b"private"))))
        }));
        let response = response_builder()
            .status(StatusCode::OK)
            .header("trailer", "x-checksum")
            .body(body)
            .expect("response");
        let envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, request_start, None),
            ResponseEgressCompletion::empty(),
            Arc::new(default_response_egress_policy),
            ResponseEgressObserverHandle::new(observer.clone()),
            clock,
        );

        let Err(failure) = envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength) else {
            panic!("framing failure must retain the attempt");
        };
        assert_eq!(polls.get(), 0);
        assert_eq!(drops.get(), 1);
        assert!(observer.reports().is_empty());
        let (mut attempt, _, cause) = failure.into_parts();
        assert_eq!(cause, ResponseEgressOutcome::ConversionError);
        assert!(attempt.begin_fallback(cause));
        assert!(
            attempt.finish_fallback(ResponseEgressFallbackDisposition::Aborted, request_start,)
        );
        assert_eq!(observer.reports().len(), 1);
    }

    #[test]
    fn response_carried_deadline_is_visible_to_policy_and_clamps_write_lifetime() {
        let observer = RecordingObserver::default();
        let request_start = MonotonicInstant::now();
        let egress_started_at = request_start
            .checked_add(Duration::from_millis(10))
            .expect("egress start");
        let application_deadline = Deadline::at_instant(
            request_start
                .checked_add(Duration::from_millis(50))
                .expect("application deadline"),
        );
        let callback_deadline = Deadline::at_instant(
            request_start
                .checked_add(Duration::from_millis(100))
                .expect("callback deadline"),
        );
        let mut response = response_builder()
            .status(StatusCode::OK)
            .body(Body::from("body"))
            .expect("response");
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::at(application_deadline));
        let envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, request_start, None),
            ResponseEgressCompletion::empty(),
            Arc::new(move |head, observed_start| {
                assert_eq!(observed_start, egress_started_at);
                assert_eq!(
                    head.application_deadline()
                        .map(|deadline| deadline.instant()),
                    Some(application_deadline.instant())
                );
                ResponseEgressPolicy {
                    write_deadline: callback_deadline,
                }
            }),
            ResponseEgressObserverHandle::new(observer),
            MonotonicClock::new(move || egress_started_at),
        );

        let Ok((prepared, policy, _attempt, _clock)) =
            envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("egress preparation failed");
        };

        assert_eq!(
            policy.write_deadline.instant(),
            application_deadline.instant()
        );
        assert!(
            prepared
                .into_response()
                .extensions()
                .get::<ResponseEgressDeadline>()
                .is_none()
        );
    }

    #[test]
    fn response_relative_deadline_resolves_from_injected_egress_start() {
        let request_start = MonotonicInstant::now();
        let egress_started_at = request_start
            .checked_add(Duration::from_millis(10))
            .expect("egress start");
        let expected_deadline = egress_started_at
            .checked_add(Duration::from_millis(40))
            .expect("relative deadline");
        let callback_deadline = Deadline::at_instant(
            egress_started_at
                .checked_add(Duration::from_millis(100))
                .expect("callback deadline"),
        );
        let mut response = response_builder()
            .status(StatusCode::OK)
            .body(Body::from("body"))
            .expect("response");
        response
            .extensions_mut()
            .insert(ResponseEgressDeadline::after(Duration::from_millis(40)));
        let envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::GET, request_start, None),
            ResponseEgressCompletion::empty(),
            Arc::new(move |head, observed_start| {
                assert_eq!(observed_start, egress_started_at);
                assert_eq!(
                    head.application_deadline()
                        .map(|deadline| deadline.instant()),
                    Some(expected_deadline)
                );
                ResponseEgressPolicy {
                    write_deadline: callback_deadline,
                }
            }),
            ResponseEgressObserverHandle::default(),
            MonotonicClock::new(move || egress_started_at),
        );

        let Ok((prepared, policy, _attempt, _clock)) =
            envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("egress preparation failed");
        };

        assert_eq!(policy.write_deadline.instant(), expected_deadline);
        assert!(
            prepared
                .into_response()
                .extensions()
                .get::<ResponseEgressDeadline>()
                .is_none()
        );
    }

    #[test]
    fn response_relative_deadline_zero_expires_at_egress_start() {
        let egress_started_at = MonotonicInstant::now();
        let deadline = ResponseEgressDeadline::after(Duration::ZERO).resolve_at(egress_started_at);

        assert_eq!(deadline.instant(), egress_started_at);
        assert!(deadline.is_expired_at(egress_started_at));
    }

    #[test]
    fn response_relative_deadline_clamps_to_portable_far_future() {
        let egress_started_at = MonotonicInstant::now();
        let expected = egress_started_at
            .checked_add(DEADLINE_FAR_FUTURE)
            .expect("portable far-future deadline");
        let deadline = ResponseEgressDeadline::after(Duration::MAX).resolve_at(egress_started_at);

        assert_eq!(deadline.instant(), expected);
    }

    #[test]
    fn response_absolute_deadline_is_not_reanchored_at_egress_start() {
        let absolute = MonotonicInstant::now();
        let egress_started_at = absolute
            .checked_add(Duration::from_secs(1))
            .expect("egress start");
        let deadline = ResponseEgressDeadline::at(Deadline::at_instant(absolute))
            .resolve_at(egress_started_at);

        assert_eq!(deadline.instant(), absolute);
        assert!(deadline.is_expired_at(egress_started_at));
    }

    #[test]
    fn adapter_response_hook_runs_before_policy_and_framing() {
        let request_start = MonotonicInstant::now();
        let observed_status = Arc::new(Mutex::new(None));
        let policy_status = Arc::clone(&observed_status);
        let response = response_builder()
            .status(StatusCode::OK)
            .body(Body::from("body"))
            .expect("response");
        let mut envelope = ResponseEgressEnvelope::new(
            response,
            ResponseEgressRequestMetadata::new(Method::HEAD, request_start, None),
            ResponseEgressCompletion::empty(),
            Arc::new(move |head, started_at| {
                *policy_status.lock().expect("policy status") = Some(head.status());
                ResponseEgressPolicy {
                    write_deadline: Deadline::at_instant(
                        started_at
                            .checked_add(Duration::from_secs(1))
                            .expect("write deadline"),
                    ),
                }
            }),
            ResponseEgressObserverHandle::default(),
            MonotonicClock::new(move || request_start),
        );

        *envelope.response_mut().status_mut() = StatusCode::CREATED;
        let Ok((prepared, _policy, _attempt, _clock)) =
            envelope.begin(ResponseEgressFramingMode::ExplicitBufferedLength)
        else {
            panic!("egress preparation failed");
        };
        let prepared_response = prepared.into_response();

        assert_eq!(
            *observed_status.lock().expect("observed status"),
            Some(StatusCode::CREATED)
        );
        assert_eq!(prepared_response.status(), StatusCode::CREATED);
        assert!(
            prepared_response
                .into_body()
                .into_bytes()
                .expect("buffered body")
                .is_empty()
        );
    }
}
