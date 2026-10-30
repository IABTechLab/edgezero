//! Exercise the actual WASI source-read race without a downstream writer.
use super::{
    BodyStream, Bytes, Deadline, EdgeError, IoFailureKind, MonotonicClock, ResponseEgressAttempt,
    SpinEgressIo as _, WasiEgressIo,
};
use edgezero_core::response_egress::ResponseEgressOutcome;

#[doc(hidden)]
#[inline]
pub async fn next_response_body(
    source: &mut BodyStream,
    deadline: Deadline,
    clock: &MonotonicClock,
    attempt: &mut ResponseEgressAttempt,
) -> Result<Option<Result<Bytes, EdgeError>>, ResponseEgressOutcome> {
    let mut io = WasiEgressIo {
        response_result: None,
        result_writer: None,
        stream_writer: None,
    };
    io.next_body(source, deadline, clock, attempt)
        .await
        .map_err(|kind| match kind {
            IoFailureKind::Deadline => ResponseEgressOutcome::DeadlineExceeded,
            IoFailureKind::Transport => ResponseEgressOutcome::TransportError,
        })
}
