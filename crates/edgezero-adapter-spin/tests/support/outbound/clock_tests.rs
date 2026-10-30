use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use edgezero_core::error::BudgetSource;
use edgezero_core::time::{Deadline, MonotonicInstant};
use futures_util::stream;
use spin_sdk::wasip3::wit_bindgen::block_on;

use super::*;
use crate::outbound::test_support::{deadline_stream, request_options};
#[path = "mod.rs"]
mod scenarios;

fn scripted_clock(script: Vec<MonotonicInstant>) -> MonotonicClock {
    let last = *script.last().expect("clock observations");
    let observations = Arc::new(Mutex::new(VecDeque::from(script)));
    MonotonicClock::new(move || {
        observations
            .lock()
            .expect("clock observations")
            .pop_front()
            .unwrap_or(last)
    })
}

fn test_budget(start: MonotonicInstant, duration: Duration) -> DispatchBudget {
    DispatchBudget {
        cause: BudgetSource::PerCallTimeout,
        deadline: Deadline::at_instant(start.checked_add(duration).expect("deadline")),
        duration,
    }
}

#[test]
fn paired_constructors_do_not_sample_the_clock() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let observations = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&observations);
    let start = MonotonicInstant::now();
    let clock = MonotonicClock::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        start
    });
    let _client = SpinOutboundClient::with_time(native_time(clock));
    assert_eq!(observations.load(Ordering::SeqCst), 0);
}

#[test]
fn method_entry_and_preflight_elapsed_use_the_injected_clock() {
    let start = MonotonicInstant::now();
    let completed = start
        .checked_add(Duration::from_millis(9))
        .expect("completed instant");
    let client = edgezero_core::HttpClient::with_client(SpinOutboundClient::with_time(
        native_time(scripted_clock(vec![start, completed])),
    ));
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();

    let results = block_on(
        client
            .start_batch_until(
                vec![request],
                Deadline::at_instant(
                    start
                        .checked_add(Duration::from_secs(1))
                        .expect("cutoff instant"),
                ),
            )
            .collect(),
    )
    .expect("valid batch driver");

    let edgezero_core::OutboundCollectedSlot::Resolved(slot) = &results.slots()[0] else {
        panic!("resolved slot");
    };
    assert_eq!(slot.elapsed, Duration::from_millis(9));
    assert!(matches!(slot.outcome, Err(EdgeError::BadRequest { .. })));
}

#[test]
fn backwards_clock_fails_slot_without_invalid_elapsed() {
    let start = MonotonicInstant::now();
    let earlier = start
        .checked_sub(Duration::from_millis(1))
        .expect("earlier instant");
    let client = edgezero_core::HttpClient::with_client(SpinOutboundClient::with_time(
        native_time(scripted_clock(vec![start, earlier])),
    ));
    let request = OutboundRequest::get("https://example.com/")
        .expect("request")
        .stream_response();

    let results = block_on(
        client
            .start_batch_until(
                vec![request],
                Deadline::at_instant(
                    start
                        .checked_add(Duration::from_secs(1))
                        .expect("cutoff instant"),
                ),
            )
            .collect(),
    )
    .expect("valid batch driver");

    let edgezero_core::OutboundCollectedSlot::Resolved(slot) = &results.slots()[0] else {
        panic!("resolved slot");
    };
    assert_eq!(slot.elapsed, Duration::ZERO);
    assert!(matches!(slot.outcome, Err(EdgeError::Internal { .. })));
}

#[test]
fn backwards_clock_cannot_expand_the_selected_budget() {
    let start = MonotonicInstant::now();
    let budget = test_budget(start, Duration::from_millis(10));
    let earlier = start
        .checked_sub(Duration::from_millis(5))
        .expect("earlier instant");
    let clock = scripted_clock(vec![earlier]);

    assert_eq!(
        budget_remaining(budget, &native_time(clock)).expect("remaining budget"),
        budget.duration
    );
}

#[test]
fn request_option_preparation_consumes_injected_budget() {
    let start = MonotonicInstant::now();
    let budget = test_budget(start, Duration::from_millis(10));
    let observed = start
        .checked_add(Duration::from_millis(3))
        .expect("observed instant");
    let observations = Arc::new(Mutex::new(VecDeque::from([observed])));
    let clock_observations = Arc::clone(&observations);
    let clock = MonotonicClock::new(move || {
        clock_observations
            .lock()
            .expect("clock observations")
            .pop_front()
            .expect("clock observation")
    });

    let _options = request_options(budget, &native_time(clock), None).expect("request options");

    assert!(observations.lock().expect("clock observations").is_empty());
}

#[test]
fn response_conversion_checks_deadline_before_handoff() {
    block_on(scenarios::handoff_tests::check_response_handoff()).expect("handoff deadlines");
}

#[test]
fn response_stream_retains_clock_for_post_ready_expiry() {
    let start = MonotonicInstant::now();
    let budget = test_budget(start, Duration::from_millis(10));
    let observed = Arc::new(Mutex::new(start));
    let clock_now = Arc::clone(&observed);
    let time = native_time(MonotonicClock::new(move || {
        *clock_now.lock().expect("clock")
    }));
    let source = stream::once(async move {
        *observed.lock().expect("clock") = budget.deadline.instant();
        Ok(Bytes::from_static(b"body"))
    })
    .boxed_local();
    let mut body = deadline_stream(source, budget, time);

    let error = block_on(body.next())
        .expect("terminal item")
        .expect_err("post-ready expiry");

    assert!(matches!(error, EdgeError::GatewayTimeout { .. }));
}

#[cfg(feature = "test-utils")]
#[test]
fn converted_response_retains_injected_clock() {
    assert!(block_on(
        scenarios::converted_response_retains_injected_clock()
    ));
}

#[test]
fn converted_response_preserves_original_content_type_provenance() {
    block_on(scenarios::content_type_tests::check_converted_response_content_type_provenance())
        .expect("converted content-type provenance");
}
