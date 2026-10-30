use core::time::Duration;
use edgezero_adapter_spin::outbound::test_support::next_response_body;
use edgezero_core::error::BadGatewayReason;
use edgezero_core::error_detail::{ProviderFailureDetail, ProviderFailurePhase, SpinFailureCause};
use edgezero_core::http::{HeaderMap, StatusCode, Version};
use edgezero_core::response_egress::{
    ResponseEgressBodyKind, ResponseEgressCompletion, ResponseEgressHead,
    ResponseEgressObserverHandle, ResponseEgressOutcome, ResponseEgressReport,
};
use edgezero_core::time::MonotonicInstant;
use edgezero_core::{Deadline, EdgeError, MonotonicClock, ResponseEgressAttempt};
use futures_util::StreamExt as _;
use futures_util::stream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum SourceCase {
    AlreadyExpired,
    Eof,
    Ready,
    ReadyExpired,
}

fn attempt(
    now: MonotonicInstant,
    clock: MonotonicClock,
) -> (ResponseEgressAttempt, Arc<Mutex<Vec<ResponseEgressReport>>>) {
    let reports = Arc::new(Mutex::new(Vec::new()));
    let completed = Arc::clone(&reports);
    let headers = HeaderMap::new();
    let head = ResponseEgressHead::new(StatusCode::OK, Version::HTTP_11, &headers, None, now, None);
    (
        ResponseEgressAttempt::new(
            &head,
            now,
            ResponseEgressCompletion::new(move |report| {
                completed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(report.clone());
            }),
            ResponseEgressObserverHandle::default(),
            clock,
        ),
        reports,
    )
}

pub(super) async fn check_source_case(
    case: SourceCase,
    with_detail: bool,
) -> Result<(), EdgeError> {
    let now = MonotonicInstant::now();
    let deadline = now
        .checked_add(Duration::from_secs(10))
        .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("test deadline overflow")))?;
    let expired = Arc::new(AtomicBool::new(case == SourceCase::AlreadyExpired));
    let clock_expired = Arc::clone(&expired);
    let clock = MonotonicClock::new(move || {
        if clock_expired.load(Ordering::SeqCst) {
            deadline
        } else {
            now
        }
    });
    let polls = Arc::new(AtomicUsize::new(0));
    let source_polls = Arc::clone(&polls);
    let detail = ProviderFailureDetail::Spin {
        phase: ProviderFailurePhase::ResponseBodyRead,
        cause: SpinFailureCause::ConnectionTerminated,
    };
    let mut source = if case == SourceCase::Eof {
        stream::empty().boxed_local()
    } else {
        stream::once(async move {
            source_polls.fetch_add(1, Ordering::SeqCst);
            expired.store(case == SourceCase::ReadyExpired, Ordering::SeqCst);
            Err(if with_detail {
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
            })
        })
        .boxed_local()
    };
    let (mut attempt, reports) = attempt(now, clock.clone());
    if !attempt.begin_writing() {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "test writing state failed"
        )));
    }
    let result = next_response_body(
        &mut source,
        Deadline::at_instant(deadline),
        &clock,
        &mut attempt,
    )
    .await;
    let expected = match case {
        SourceCase::AlreadyExpired | SourceCase::ReadyExpired => {
            ResponseEgressOutcome::DeadlineExceeded
        }
        SourceCase::Eof => ResponseEgressOutcome::Completed,
        SourceCase::Ready => ResponseEgressOutcome::SourceError,
    };
    let actual = match result {
        Ok(Some(Err(_))) => ResponseEgressOutcome::SourceError,
        Ok(None) => ResponseEgressOutcome::Completed,
        Err(ResponseEgressOutcome::DeadlineExceeded) => ResponseEgressOutcome::DeadlineExceeded,
        _ => {
            return Err(EdgeError::internal(anyhow::anyhow!(
                "unexpected test read result"
            )));
        }
    };
    if actual != expected || !attempt.terminate(actual, clock.now()) {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "test terminal outcome changed"
        )));
    }
    drop(attempt);
    let retained = reports.lock().unwrap_or_else(PoisonError::into_inner);
    let report = retained
        .first()
        .ok_or_else(|| EdgeError::internal(anyhow::anyhow!("test terminal report missing")))?;
    let observed = matches!(case, SourceCase::Ready | SourceCase::ReadyExpired);
    if retained.len() != 1
        || report.bytes_written != 0
        || report.source_failure.is_some() != observed
        || polls.load(Ordering::SeqCst) != usize::from(observed)
    {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "test source observation changed"
        )));
    }
    check_report(report, with_detail.then_some(&detail))
}

fn check_report(
    report: &ResponseEgressReport,
    expected: Option<&ProviderFailureDetail>,
) -> Result<(), EdgeError> {
    if let Some(failure) = report.source_failure
        && (failure.body_kind() != ResponseEgressBodyKind::Application
            || failure.category() != "bad_gateway_transport"
            || failure.provider_failure_detail() != expected)
    {
        return Err(EdgeError::internal(anyhow::anyhow!(
            "test source diagnostic changed"
        )));
    }
    Ok(())
}

/// Exercises the production source-read race without creating a downstream writer.
///
/// # Errors
/// Returns an internal test error if observation or terminal behavior changes.
#[doc(hidden)]
#[inline]
pub async fn response_source_diagnostics_hold() -> Result<(), EdgeError> {
    for case in [
        SourceCase::AlreadyExpired,
        SourceCase::Eof,
        SourceCase::Ready,
        SourceCase::ReadyExpired,
    ] {
        for with_detail in [false, true] {
            check_source_case(case, with_detail).await?;
        }
    }
    Ok(())
}
