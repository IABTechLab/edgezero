//! Private-boundary bridge; scenarios live under tests/support.

use std::future::Future;
use std::time::Duration;

use edgezero_core::body::{Body, BodyStream};
use edgezero_core::error::EdgeError;
use edgezero_core::error_detail::{ProviderFailureDetail, SpinTimeoutOption};
use edgezero_core::http::{HeaderMap, Method};
use edgezero_core::outbound::{
    OutboundBatch, OutboundBatchContext, OutboundFuture, OutboundHead, OutboundPending,
    OutboundResponse, OutboundResponseContext, OutboundTime,
};
use edgezero_core::time::{DispatchBudget, MonotonicClock, MonotonicInstant};
use spin_sdk::wasip3::http::types::{
    ErrorCode, Fields, HeaderError, RequestOptions, RequestOptionsError, Response,
};
use spin_sdk::wasip3::http_compat::BodyWriter;
use spin_sdk::wasip3::wit_bindgen::{FutureReader, StreamReader};

type RequestBodyResources = (
    Option<BodyWriter>,
    Option<StreamReader<u8>>,
    FutureReader<Result<Option<Fields>, ErrorCode>>,
);
type Slot = (usize, DispatchBudget, OutboundFuture<OutboundResponse>);

#[doc(hidden)]
#[inline]
pub fn request_body_resources(method: &Method, body: &Body) -> RequestBodyResources {
    let prepared = super::prepare_request_body(method, body);
    (prepared.writer, prepared.contents, prepared.trailers)
}

#[doc(hidden)]
#[inline]
pub fn empty_request_trailers() -> Result<Option<Fields>, ErrorCode> {
    super::EMPTY_REQUEST_TRAILERS()
}

#[doc(hidden)]
#[inline]
pub fn ensure_p3_context() -> Result<(), EdgeError> {
    super::ensure_p3_context()
}

#[doc(hidden)]
#[inline]
#[must_use]
pub fn native_time(clock: MonotonicClock) -> OutboundTime {
    super::native_time(clock)
}

#[doc(hidden)]
#[inline]
pub fn response_head(
    response: Response,
    context: OutboundResponseContext,
) -> Result<OutboundHead, EdgeError> {
    super::response_head(response, context)
}

#[doc(hidden)]
#[inline]
pub fn request_options(
    budget: DispatchBudget,
    time: &OutboundTime,
    connect_timeout: Option<Duration>,
) -> Result<RequestOptions, EdgeError> {
    super::request_options(budget, time, connect_timeout)
}

#[doc(hidden)]
#[inline]
#[must_use]
pub fn deadline_stream(
    source: BodyStream,
    budget: DispatchBudget,
    time: OutboundTime,
) -> BodyStream {
    super::super::deadline_stream(source, budget, time)
}

#[doc(hidden)]
#[inline]
pub async fn cooperate_after_response_read(ready_reads: &mut usize) {
    super::super::cooperate_after_response_read(ready_reads).await;
}

#[doc(hidden)]
#[inline]
pub async fn race_deadline<Output, Operation, FailureDetail>(
    future: Operation,
    budget: DispatchBudget,
    time: &OutboundTime,
    previous: &mut MonotonicInstant,
    failure_detail: FailureDetail,
) -> Result<Output, EdgeError>
where
    Operation: Future<Output = Output>,
    FailureDetail: FnOnce(&Output) -> Option<ProviderFailureDetail>,
{
    super::super::race_deadline(future, budget, time, previous, failure_detail).await
}

#[doc(hidden)]
#[inline]
pub fn schedule_exchange(
    future: OutboundFuture<OutboundResponse>,
) -> Result<OutboundPending, EdgeError> {
    super::super::scheduling::schedule_exchange(future, super::spawn_outbound_task)
}

#[doc(hidden)]
#[inline]
pub fn schedule_prepared_batch(context: OutboundBatchContext, slots: Vec<Slot>) -> OutboundBatch {
    super::super::scheduling::schedule_prepared_batch(context, slots, super::spawn_outbound_task)
}

#[doc(hidden)]
#[inline]
pub fn request_timeout_settings<Setter>(
    remaining: Duration,
    configured_connect: Option<Duration>,
    setter: Setter,
) -> Result<(), EdgeError>
where
    Setter: FnMut(SpinTimeoutOption, u64) -> Result<(), RequestOptionsError>,
{
    super::apply_timeout_settings(
        super::super::request_timeouts(remaining, configured_connect),
        setter,
    )
}

#[doc(hidden)]
#[must_use]
#[inline]
pub fn map_spin_preparation_header(error: &HeaderError) -> EdgeError {
    super::map_spin_preparation_header(error)
}

#[doc(hidden)]
#[inline]
pub fn request_fields(headers: &HeaderMap) -> Result<(), EdgeError> {
    super::request_fields(headers).map(|_fields| ())
}

#[doc(hidden)]
#[inline]
pub fn set_timeout_option(
    option: SpinTimeoutOption,
    outcome: &Result<(), RequestOptionsError>,
) -> Result<(), EdgeError> {
    super::set_timeout_option(option, outcome)
}

#[doc(hidden)]
#[must_use]
#[inline]
pub fn request_target_errors() -> [EdgeError; 4] {
    [
        super::RequestTargetPart::Authority,
        super::RequestTargetPart::Method,
        super::RequestTargetPart::Path,
        super::RequestTargetPart::Scheme,
    ]
    .map(super::request_target_error)
}
