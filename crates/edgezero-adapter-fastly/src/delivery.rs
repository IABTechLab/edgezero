//! Owned request-delivery results for custom Fastly lifecycles.

/// SDK-free fixtures for the same finalization/report path used by production custom helpers.
#[cfg(feature = "test-utils")]
pub mod testing {
    use super::{
        DeliveryError, FastlyNoAttemptReason, FastlyRequestDelivery, FastlyResponseCommitState,
        FastlyResponseDelivery,
    };
    use edgezero_core::http::Response;
    use edgezero_core::response_egress::{ResponseEgressEnvelope, ResponseEgressReport};

    pub enum Dispatch {
        Aborted,
        Detached(ResponseEgressEnvelope),
        NoAttempt(FastlyNoAttemptReason),
        Routed(ResponseEgressEnvelope),
    }

    /// Supplies dispatch and delivery outcomes; does not simulate SDK head conversion or transport.
    #[inline]
    pub fn deliver_with_hooks<State, Finalize, Deliver>(
        dispatch: Dispatch,
        finalize: Finalize,
        deliver: Deliver,
    ) -> FastlyRequestDelivery<State>
    where
        Finalize: FnOnce(&mut Response) -> State,
        Deliver: FnOnce(ResponseEgressEnvelope) -> FastlyResponseDelivery,
    {
        let outcome = match dispatch {
            Dispatch::Aborted => Ok(super::DispatchOutcome::Aborted),
            Dispatch::Detached(envelope) => Ok(super::DispatchOutcome::Detached(envelope)),
            Dispatch::NoAttempt(reason) => Err(super::FastlyNoAttempt::new(
                reason,
                anyhow::Error::msg(reason.message()),
            )),
            Dispatch::Routed(envelope) => Ok(super::DispatchOutcome::Routed(envelope)),
        };
        super::finish_request(outcome, finalize, deliver)
    }

    /// Constructs a synthetic terminal fixture. Production constructors remain adapter-owned.
    #[inline]
    pub const fn response_delivery(
        commitment: FastlyResponseCommitState,
        report: ResponseEgressReport,
        result: Result<(), DeliveryError>,
    ) -> FastlyResponseDelivery {
        FastlyResponseDelivery::new(commitment, report, result)
    }
}

use std::fmt;

#[cfg(any(test, feature = "fastly", feature = "test-utils"))]
use edgezero_core::http::Response;
use edgezero_core::key_value_store::KvStoreOpenFailure;
#[cfg(any(test, feature = "fastly", feature = "test-utils"))]
use edgezero_core::response_egress::ResponseEgressEnvelope;
use edgezero_core::response_egress::{ResponseEgressOutcome, ResponseEgressReport};

/// Category of the last delivery failure, without provider text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryError {
    Conversion,
    Deadline,
    Source,
    Transport,
}

impl DeliveryError {
    #[cfg(any(test, feature = "fastly"))]
    pub(crate) const fn from_outcome(outcome: ResponseEgressOutcome) -> Self {
        match outcome {
            ResponseEgressOutcome::ConversionError => Self::Conversion,
            ResponseEgressOutcome::DeadlineExceeded => Self::Deadline,
            ResponseEgressOutcome::SourceError => Self::Source,
            ResponseEgressOutcome::ClientDisconnected
            | ResponseEgressOutcome::Completed
            | ResponseEgressOutcome::HostHandoff
            | ResponseEgressOutcome::RequestCancelled
            | ResponseEgressOutcome::TransportError
            | ResponseEgressOutcome::Unspecified
            | _ => Self::Transport,
        }
    }

    /// Fixed, payload-free diagnostic for the last delivery failure.
    #[must_use]
    #[inline]
    pub const fn message(self) -> &'static str {
        match self {
            Self::Conversion => "response conversion failed",
            Self::Deadline => "response write deadline exceeded",
            Self::Source => "response body source failed",
            Self::Transport => "response transport failed",
        }
    }

    #[must_use]
    #[inline]
    pub const fn outcome(self) -> ResponseEgressOutcome {
        match self {
            Self::Conversion => ResponseEgressOutcome::ConversionError,
            Self::Deadline => ResponseEgressOutcome::DeadlineExceeded,
            Self::Source => ResponseEgressOutcome::SourceError,
            Self::Transport => ResponseEgressOutcome::TransportError,
        }
    }
}

#[cfg(any(test, feature = "fastly", feature = "test-utils"))]
pub(crate) enum DispatchOutcome {
    Aborted,
    Detached(ResponseEgressEnvelope),
    Routed(ResponseEgressEnvelope),
}

/// A pre-attempt failure with safe public detail and a privately retained standard-path error.
pub struct FastlyNoAttempt {
    reason: FastlyNoAttemptReason,
    #[cfg(any(test, feature = "fastly"))]
    standard_error: anyhow::Error,
}

impl fmt::Debug for FastlyNoAttempt {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastlyNoAttempt")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl FastlyNoAttempt {
    #[must_use]
    #[inline]
    pub const fn message(&self) -> &'static str {
        self.reason.message()
    }

    #[cfg(any(test, feature = "fastly", feature = "test-utils"))]
    pub(crate) fn new(reason: FastlyNoAttemptReason, standard_error: anyhow::Error) -> Self {
        #[cfg(not(any(test, feature = "fastly")))]
        drop(standard_error);
        Self {
            reason,
            #[cfg(any(test, feature = "fastly"))]
            standard_error,
        }
    }

    #[must_use]
    #[inline]
    pub const fn reason(&self) -> FastlyNoAttemptReason {
        self.reason
    }
}

/// A failure before any egress attempt, completion, observer or send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FastlyNoAttemptReason {
    RequestHeadConversion(FastlyRequestHeadFailure),
    RequiredStore(KvStoreOpenFailure),
}

impl FastlyNoAttemptReason {
    #[must_use]
    #[inline]
    pub const fn message(self) -> &'static str {
        match self {
            Self::RequestHeadConversion(_) => "request head conversion failed",
            Self::RequiredStore(_) => "required store could not be opened",
        }
    }
}

/// The custom send's disposition, retaining routed finalization state even on delivery failure.
#[derive(Debug)]
#[must_use = "inspect disposition and commitment before sending or scheduling post-delivery work"]
#[non_exhaustive]
pub enum FastlyRequestDelivery<State> {
    /// Application policy intentionally selected no response. Replacement is forbidden.
    Aborted,
    Detached(FastlyResponseDelivery),
    /// No response attempt or completion occurred. A native response is permitted.
    NoAttempt(FastlyNoAttempt),
    Routed {
        state: State,
        delivery: FastlyResponseDelivery,
    },
}

impl<State> FastlyRequestDelivery<State> {
    #[must_use]
    #[inline]
    pub const fn delivery(&self) -> Option<&FastlyResponseDelivery> {
        match self {
            Self::Detached(delivery) | Self::Routed { delivery, .. } => Some(delivery),
            Self::Aborted | Self::NoAttempt(_) => None,
        }
    }

    #[cfg(any(test, feature = "fastly"))]
    pub(crate) fn into_standard_result(self) -> Result<Option<State>, anyhow::Error> {
        match self {
            Self::Aborted => Err(anyhow::Error::msg("ingress admission aborted")),
            Self::NoAttempt(failure) => Err(failure.standard_error),
            Self::Detached(delivery) => delivery.standard_result().map(|()| None),
            Self::Routed { state, delivery } => delivery.standard_result().map(|()| Some(state)),
        }
    }

    /// Whether a caller may send its own native response after this result.
    ///
    /// An attempted response has already notified completion and any applicable observer.
    /// A native replacement is outside `EdgeZero` accounting. Policy abort, unknown commitment
    /// and committed delivery never permit replacement, including after body failure.
    #[must_use]
    #[inline]
    pub const fn may_send_native_response(&self) -> bool {
        match self {
            Self::NoAttempt(_) => true,
            Self::Aborted => false,
            Self::Detached(delivery) | Self::Routed { delivery, .. } => {
                matches!(delivery.commitment, FastlyResponseCommitState::NotCommitted)
            }
        }
    }

    #[must_use]
    #[inline]
    pub const fn no_attempt_reason(&self) -> Option<FastlyNoAttemptReason> {
        match self {
            Self::NoAttempt(failure) => Some(failure.reason()),
            Self::Aborted | Self::Detached(_) | Self::Routed { .. } => None,
        }
    }
}

/// The observed, text-free request-head conversion boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FastlyRequestHeadFailure {
    InvalidUri,
    RequestBuilder,
}

/// Commitment of the last selected response body, not proof of client delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FastlyResponseCommitState {
    /// Fastly accepted the response head.
    Committed,
    /// The response head never reached the host send operation.
    NotCommitted,
    /// The host rejected the send; client-visible commitment cannot be established.
    Unknown,
}

/// One attempted response's last-body commitment and exact terminal completion report.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "inspect the terminal report; successful fallback delivery is not application delivery"]
pub struct FastlyResponseDelivery {
    commitment: FastlyResponseCommitState,
    report: ResponseEgressReport,
    result: Result<(), DeliveryError>,
}

impl FastlyResponseDelivery {
    #[must_use]
    #[inline]
    pub const fn commitment(&self) -> FastlyResponseCommitState {
        self.commitment
    }

    /// Last send/pump failure, independent of the fallback report's retained original cause.
    #[must_use]
    #[inline]
    pub fn error(&self) -> Option<DeliveryError> {
        self.result.err()
    }

    #[cfg(any(test, feature = "fastly", feature = "test-utils"))]
    pub(crate) const fn new(
        commitment: FastlyResponseCommitState,
        report: ResponseEgressReport,
        result: Result<(), DeliveryError>,
    ) -> Self {
        Self {
            commitment,
            report,
            result,
        }
    }

    #[must_use]
    #[inline]
    pub const fn report(&self) -> &ResponseEgressReport {
        &self.report
    }

    #[cfg(any(test, feature = "fastly"))]
    fn standard_result(&self) -> Result<(), anyhow::Error> {
        self.result
            .map_err(|error| anyhow::Error::msg(error.message()))
    }
}

#[cfg(any(test, feature = "fastly", feature = "test-utils"))]
pub(crate) fn finish_request<State, Finalize, Deliver>(
    outcome: Result<DispatchOutcome, FastlyNoAttempt>,
    finalize: Finalize,
    deliver: Deliver,
) -> FastlyRequestDelivery<State>
where
    Finalize: FnOnce(&mut Response) -> State,
    Deliver: FnOnce(ResponseEgressEnvelope) -> FastlyResponseDelivery,
{
    match outcome {
        Err(reason) => FastlyRequestDelivery::NoAttempt(reason),
        Ok(DispatchOutcome::Aborted) => FastlyRequestDelivery::Aborted,
        Ok(DispatchOutcome::Detached(envelope)) => {
            FastlyRequestDelivery::Detached(deliver(envelope))
        }
        Ok(DispatchOutcome::Routed(mut envelope)) => {
            let state = finalize(envelope.response_mut());
            let delivery = deliver(envelope);
            FastlyRequestDelivery::Routed { state, delivery }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::http::{HeaderMap, StatusCode, Version};
    use edgezero_core::key_value_store::{KvError, KvStoreOpenFailure};
    use edgezero_core::response_egress::{
        ResponseEgressAttempt, ResponseEgressCompletion, ResponseEgressHead,
        ResponseEgressObserverHandle, ResponseEgressOutcome,
    };
    use edgezero_core::time::{MonotonicClock, MonotonicInstant};

    fn report(outcome: ResponseEgressOutcome) -> edgezero_core::ResponseEgressReport {
        let now = MonotonicInstant::now();
        let headers = HeaderMap::new();
        let head =
            ResponseEgressHead::new(StatusCode::OK, Version::HTTP_11, &headers, None, now, None);
        let mut attempt = ResponseEgressAttempt::new(
            &head,
            now,
            ResponseEgressCompletion::empty(),
            ResponseEgressObserverHandle::default(),
            MonotonicClock::new(move || now),
        );
        assert!(attempt.begin_writing());
        assert!(attempt.terminate(outcome, now));
        attempt.into_terminal_report()
    }

    fn no_attempt(reason: FastlyNoAttemptReason) -> FastlyNoAttempt {
        let error = match reason {
            FastlyNoAttemptReason::RequiredStore(open_reason) => {
                anyhow::Error::new(KvError::OpenFailed {
                    reason: open_reason,
                })
            }
            FastlyNoAttemptReason::RequestHeadConversion(_) => {
                anyhow::Error::msg("request head conversion failed")
            }
        };
        FastlyNoAttempt::new(reason, error)
    }

    #[test]
    fn fastly_delivery_report_prevents_unsafe_replacement() {
        for commitment in [
            FastlyResponseCommitState::NotCommitted,
            FastlyResponseCommitState::Unknown,
            FastlyResponseCommitState::Committed,
        ] {
            let terminal = report(ResponseEgressOutcome::TransportError);
            let delivery = FastlyResponseDelivery::new(
                commitment,
                terminal.clone(),
                Err(DeliveryError::Transport),
            );
            assert_eq!(delivery.commitment(), commitment);
            assert_eq!(delivery.report(), &terminal);
            let result = FastlyRequestDelivery::Routed {
                state: 42_u8,
                delivery,
            };
            assert_eq!(
                result.may_send_native_response(),
                commitment == FastlyResponseCommitState::NotCommitted
            );
            let FastlyRequestDelivery::Routed {
                state,
                delivery: returned_delivery,
            } = result
            else {
                panic!("routed result lost its state");
            };
            assert_eq!(state, 42);
            assert_eq!(returned_delivery.error(), Some(DeliveryError::Transport));
        }
        for reason in [
            FastlyNoAttemptReason::RequestHeadConversion(FastlyRequestHeadFailure::InvalidUri),
            FastlyNoAttemptReason::RequiredStore(KvStoreOpenFailure::MissingBinding),
        ] {
            let result = FastlyRequestDelivery::<()>::NoAttempt(no_attempt(reason));
            assert!(result.may_send_native_response());
            assert_eq!(result.no_attempt_reason(), Some(reason));
        }
        assert!(!FastlyRequestDelivery::<()>::Aborted.may_send_native_response());
    }

    #[test]
    fn standard_serving_policy_keeps_delivered_fallbacks_successful() {
        let mut terminal = report(ResponseEgressOutcome::ConversionError);
        terminal.body_kind = edgezero_core::ResponseEgressBodyKind::Fallback;
        terminal.fallback_disposition =
            Some(edgezero_core::ResponseEgressFallbackDisposition::Completed);
        let delivery =
            FastlyResponseDelivery::new(FastlyResponseCommitState::Committed, terminal, Ok(()));
        assert_eq!(
            FastlyRequestDelivery::Routed {
                state: 7_u8,
                delivery: delivery.clone()
            }
            .into_standard_result()
            .unwrap(),
            Some(7)
        );
        assert_eq!(
            FastlyRequestDelivery::<()>::Detached(delivery)
                .into_standard_result()
                .unwrap(),
            None
        );
        for error in [
            DeliveryError::Conversion,
            DeliveryError::Deadline,
            DeliveryError::Source,
            DeliveryError::Transport,
        ] {
            let failed_delivery = FastlyResponseDelivery::new(
                FastlyResponseCommitState::Committed,
                report(error.outcome()),
                Err(error),
            );
            assert_eq!(
                FastlyRequestDelivery::Routed {
                    state: 7_u8,
                    delivery: failed_delivery
                }
                .into_standard_result()
                .unwrap_err()
                .to_string(),
                error.message()
            );
        }
        assert_eq!(
            FastlyRequestDelivery::<()>::Aborted
                .into_standard_result()
                .unwrap_err()
                .to_string(),
            "ingress admission aborted"
        );
        assert_eq!(
            FastlyRequestDelivery::<()>::NoAttempt(no_attempt(
                FastlyNoAttemptReason::RequestHeadConversion(FastlyRequestHeadFailure::InvalidUri)
            ))
            .into_standard_result()
            .unwrap_err()
            .to_string(),
            "request head conversion failed"
        );
    }

    #[test]
    fn standard_no_attempt_errors_keep_original_text() {
        let result = FastlyRequestDelivery::<()>::NoAttempt(no_attempt(
            FastlyNoAttemptReason::RequiredStore(KvStoreOpenFailure::MissingBinding),
        ));
        let error = result.into_standard_result().unwrap_err();
        assert_eq!(error.to_string(), "kv store open failed: missing binding");
        assert!(matches!(
            error.downcast_ref::<KvError>(),
            Some(KvError::OpenFailed {
                reason: KvStoreOpenFailure::MissingBinding
            })
        ));
        let cause =
            FastlyNoAttemptReason::RequestHeadConversion(FastlyRequestHeadFailure::RequestBuilder);
        let head_result = FastlyRequestDelivery::<()>::NoAttempt(FastlyNoAttempt::new(
            cause,
            anyhow::Error::msg("private-token conversion text"),
        ));
        assert_eq!(head_result.no_attempt_reason(), Some(cause));
        assert!(!format!("{head_result:?}").contains("private-token"));
        assert_eq!(
            head_result.into_standard_result().unwrap_err().to_string(),
            "private-token conversion text"
        );
    }

    #[test]
    fn abort_and_no_attempt_skip_finalization_and_delivery() {
        for outcome in [
            Ok(DispatchOutcome::Aborted),
            Err(no_attempt(FastlyNoAttemptReason::RequestHeadConversion(
                FastlyRequestHeadFailure::InvalidUri,
            ))),
            Err(no_attempt(FastlyNoAttemptReason::RequiredStore(
                KvStoreOpenFailure::MissingBinding,
            ))),
        ] {
            let aborted = matches!(outcome, Ok(DispatchOutcome::Aborted));
            let result = finish_request::<u8, _, _>(
                outcome,
                |_| panic!("unexpected finalization"),
                |_| panic!("unexpected delivery"),
            );
            assert_eq!(result.may_send_native_response(), !aborted);
            assert_eq!(matches!(result, FastlyRequestDelivery::Aborted), aborted);
        }
    }
}
