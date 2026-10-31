//! Narrow access to production boundaries for external test owners.

#[cfg(all(feature = "spin", feature = "test-utils"))]
use super::{BudgetSource, Deadline, Duration, ErrorCode, ProviderFailurePhase};
use super::{EdgeError, OutboundRequest, OutboundSendMode};
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
use std::future::Future;

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub use super::spin_impl::test_support::{
    cooperate_after_response_read, deadline_stream, empty_request_trailers, ensure_p3_context,
    map_spin_preparation_header, native_time, race_deadline, request_body_resources,
    request_fields, request_options, request_target_errors, request_timeout_settings,
    response_head, schedule_exchange, schedule_prepared_batch, set_timeout_option,
};

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub use crate::request::test_support::{
    deadline_body, dispatch_ingress_stream, dispatch_ingress_stream_with_timer,
    dispatch_ingress_with, dispatch_request,
};
#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub use crate::response::test_support::next_response_body;

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
pub const READY_ITEM_YIELD_QUOTA: usize = super::READY_ITEM_YIELD_QUOTA;

#[cfg(all(feature = "spin", target_arch = "wasm32"))]
#[doc(hidden)]
#[inline]
/// Returns the outcome, poll attempts, delivered items and source-release state.
pub async fn observe_response_head<F>(
    operation: F,
) -> Result<(F::Output, usize, usize, bool), EdgeError>
where
    F: Future,
{
    super::response_probe::observe_operation(operation).await
}

#[doc(hidden)]
#[inline]
pub fn validate_options(
    request: &OutboundRequest,
    mode: OutboundSendMode,
) -> Result<(), EdgeError> {
    super::validate_request(request, mode)
}

/// Runs the target-neutral Spin batch preflight contract in native tests.
///
/// # Errors
/// Returns the same portable validation or batch-shape error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_batch_request(request: &OutboundRequest) -> Result<(), EdgeError> {
    super::validate_batch_request(request)
}

/// Runs the target-neutral Spin request policy before any provider dispatch.
///
/// # Errors
/// Returns the same portable validation or unsupported-authority error as production dispatch.
#[cfg(feature = "test-utils")]
#[inline]
pub fn validate_request(request: &OutboundRequest) -> Result<(), EdgeError> {
    super::validate_request(request, OutboundSendMode::Send)
}

/// Exposes the real pinned SDK classifier to WASI resource contract tests.
#[cfg(all(feature = "spin", feature = "test-utils"))]
#[must_use]
#[inline]
pub fn map_spin_host_error(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
) -> EdgeError {
    super::map_spin_host_error(error, deadline, cause, observed_at, phase)
}

/// Exercises the dispatch-boundary classifier with the retained per-request connect setting.
#[cfg(all(feature = "spin", feature = "test-utils"))]
#[must_use]
#[inline]
pub fn map_spin_dispatch_error(
    error: &ErrorCode,
    deadline: Deadline,
    cause: BudgetSource,
    observed_at: edgezero_core::MonotonicInstant,
    phase: ProviderFailurePhase,
    connect_timeout: Option<Duration>,
) -> EdgeError {
    super::map_spin_dispatch_error(error, deadline, cause, observed_at, phase, connect_timeout)
}
