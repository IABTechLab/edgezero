//! Bounded, request-owned observations of real SDK response phases.

use super::super::{OutboundRequest, OutboundResponseContext, OutboundSendMode};
use crate::body::BodyStream;
use crate::http::Uri;
use crate::{DispatchBudget, EdgeError, MonotonicClock, MonotonicInstant};
use bytes::Bytes;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use futures_util::future::poll_fn;
use futures_util::{Stream, StreamExt as _};
use std::io::Error as IoError;
use std::sync::{Arc, Mutex, MutexGuard};

/// The returned outcome of native readiness, not an SDK read poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBodyWaitOutcome {
    Failed,
    OtherReady,
    Ready,
    TimedOut,
}

/// An actual SDK resource followed by its observation-only release guard.
#[derive(Debug)]
pub struct NativeBodyOwner<Resource> {
    // Field order ensures resource destruction precedes the release observation.
    resource: Resource,
    witness: Option<NativeSourceRelease>,
}

/// A copy of the immutable policies observed at actual request preparation.
#[derive(Clone, Debug)]
pub struct NativeBodyPreparation {
    pub budget: DispatchBudget,
    pub entered_at: MonotonicInstant,
    pub mode: OutboundSendMode,
    pub target: Uri,
}

/// The first returned result of polling an actual SDK read.
#[derive(Clone, Copy, Debug)]
pub struct NativeBodyRead {
    pub observed_at: MonotonicInstant,
    pub pending: bool,
    pub started_at: MonotonicInstant,
}

/// Finite observation state; no source, body, waker or request context is retained.
#[derive(Clone, Debug, Default)]
pub struct NativeBodySnapshot {
    pub attachments: u64,
    pub failed_waits: u64,
    pub first_head: Option<MonotonicInstant>,
    pub first_read: Option<NativeBodyRead>,
    pub first_release: Option<MonotonicInstant>,
    pub first_wait: Option<NativeBodyWait>,
    pub first_wait_timeout: Option<NativeBodyWait>,
    pub heads: u64,
    pub invalid: bool,
    pub last_wait_timeout: Option<NativeBodyWait>,
    pub preparation: Option<NativeBodyPreparation>,
    pub preparations: u64,
    pub reads: u64,
    pub ready_reads: u64,
    pub ready_waits: u64,
    pub source_acquisitions: u64,
    pub source_releases: u64,
    pub waits: u64,
}

/// Entry and returned outcome of one native readiness call in the prepared clock.
#[derive(Clone, Copy, Debug)]
pub struct NativeBodyWait {
    pub observed_at: MonotonicInstant,
    pub outcome: NativeBodyWaitOutcome,
    pub started_at: MonotonicInstant,
}

/// Test-only reporting handle, explicitly attached to one outbound request.
#[derive(Clone, Debug, Default)]
pub struct NativeBodyWitness {
    state: Arc<Mutex<ObservationState>>,
}

#[derive(Debug, Default)]
struct ObservationState {
    previous: Option<MonotonicInstant>,
    snapshot: NativeBodySnapshot,
}

struct NativeSource {
    observe_reads: bool,
    owner: NativeBodyOwner<BodyStream>,
}

#[derive(Debug)]
struct NativeSourceRelease {
    recorder: NativeBodyRecorder,
}

impl<Resource> NativeBodyOwner<Resource> {
    /// Borrows the resource without separating it from its release observation.
    #[inline]
    pub fn get_mut(&mut self) -> &mut Resource {
        &mut self.resource
    }

    /// Borrows the resource without transferring ownership.
    #[must_use]
    #[inline]
    pub fn get_ref(&self) -> &Resource {
        &self.resource
    }

    /// Returns the recorder bound to this exact resource, if attached.
    #[must_use]
    #[inline]
    pub fn recorder(&self) -> Option<&NativeBodyRecorder> {
        self.witness.as_ref().map(|guard| &guard.recorder)
    }
}

impl ObservationState {
    fn sample(&mut self, at: MonotonicInstant) {
        if self.previous.is_some_and(|previous| at < previous) {
            self.snapshot.invalid = true;
        }
        self.previous = Some(at);
    }
}

/// Adapter recorder using only the prepared context's authoritative clock.
#[derive(Clone, Debug)]
pub struct NativeBodyRecorder {
    clock: MonotonicClock,
    witness: NativeBodyWitness,
}

impl NativeBodyWitness {
    /// Attaches once without changing request policies or transport behavior.
    ///
    /// # Errors
    /// Rejects reused witnesses or an already-observed request.
    #[inline]
    pub fn attach(&self, request: &mut OutboundRequest) -> Result<(), EdgeError> {
        let mut state = self.lock();
        if state.snapshot.attachments != 0 || request.native_body_witness.is_some() {
            state.snapshot.invalid = true;
            drop(state);
            if let Some(existing) = &request.native_body_witness {
                existing.lock().snapshot.invalid = true;
            }
            return Err(EdgeError::internal(IoError::other(
                "native body witness was reused",
            )));
        }
        state.snapshot.attachments = 1;
        drop(state);
        request.native_body_witness = Some(self.clone());
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, ObservationState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.snapshot.invalid = true;
                state
            }
        }
    }

    /// Creates an unattached witness with no runtime resources.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn prepare(
        &self,
        clock: MonotonicClock,
        mode: OutboundSendMode,
        budget: DispatchBudget,
        entered_at: MonotonicInstant,
        target: &Uri,
    ) -> NativeBodyRecorder {
        let mut state = self.lock();
        state.sample(entered_at);
        state.snapshot.preparations = state.snapshot.preparations.saturating_add(1);
        if state.snapshot.preparations != 1 || state.snapshot.attachments != 1 {
            state.snapshot.invalid = true;
        }
        state
            .snapshot
            .preparation
            .get_or_insert_with(|| NativeBodyPreparation {
                budget,
                entered_at,
                mode,
                target: target.clone(),
            });
        drop(state);
        NativeBodyRecorder {
            clock,
            witness: self.clone(),
        }
    }

    /// Returns an owned snapshot, not access to mutable recorder state.
    #[must_use]
    #[inline]
    pub fn snapshot(&self) -> NativeBodySnapshot {
        self.lock().snapshot.clone()
    }
}

impl NativeBodyRecorder {
    /// Records successful conversion of an actual SDK response head.
    #[inline]
    pub fn head_received(&self) {
        let at = self.clock.now();
        let mut state = self.witness.lock();
        state.sample(at);
        state.snapshot.heads = state.snapshot.heads.saturating_add(1);
        if state.snapshot.heads != 1 {
            state.snapshot.invalid = true;
        }
        state.snapshot.first_head.get_or_insert(at);
    }

    fn read_observed(
        &self,
        started_at: MonotonicInstant,
        observed_at: MonotonicInstant,
        pending: bool,
    ) {
        let mut state = self.witness.lock();
        state.sample(started_at);
        state.sample(observed_at);
        if state.snapshot.heads != 1 {
            state.snapshot.invalid = true;
        }
        state.snapshot.reads = state.snapshot.reads.saturating_add(1);
        if !pending {
            state.snapshot.ready_reads = state.snapshot.ready_reads.saturating_add(1);
        }
        state.snapshot.first_read.get_or_insert(NativeBodyRead {
            observed_at,
            pending,
            started_at,
        });
    }

    fn source_acquired(&self) {
        let mut state = self.witness.lock();
        state.snapshot.source_acquisitions = state.snapshot.source_acquisitions.saturating_add(1);
        if state.snapshot.source_acquisitions != 1 {
            state.snapshot.invalid = true;
        }
    }

    fn source_released(&self) {
        let at = self.clock.now();
        let mut state = self.witness.lock();
        state.sample(at);
        state.snapshot.source_releases = state.snapshot.source_releases.saturating_add(1);
        state.snapshot.first_release.get_or_insert(at);
        if state.snapshot.source_releases != 1 {
            state.snapshot.invalid = true;
        }
    }

    fn wait_observed(
        &self,
        started_at: MonotonicInstant,
        observed_at: MonotonicInstant,
        outcome: NativeBodyWaitOutcome,
    ) {
        let mut state = self.witness.lock();
        state.sample(started_at);
        state.sample(observed_at);
        if state.snapshot.heads != 1 || state.snapshot.source_acquisitions != 1 {
            state.snapshot.invalid = true;
        }
        let observation = NativeBodyWait {
            observed_at,
            outcome,
            started_at,
        };
        state.snapshot.waits = state.snapshot.waits.saturating_add(1);
        state.snapshot.first_wait.get_or_insert(observation);
        match outcome {
            NativeBodyWaitOutcome::Failed => {
                state.snapshot.failed_waits = state.snapshot.failed_waits.saturating_add(1);
            }
            NativeBodyWaitOutcome::OtherReady => {}
            NativeBodyWaitOutcome::Ready => {
                state.snapshot.ready_waits = state.snapshot.ready_waits.saturating_add(1);
            }
            NativeBodyWaitOutcome::TimedOut => {
                state.snapshot.first_wait_timeout.get_or_insert(observation);
                state.snapshot.last_wait_timeout = Some(observation);
            }
        }
    }
}

impl Stream for NativeSource {
    type Item = Result<Bytes, EdgeError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let source = &mut this.owner.resource;
        if !this.observe_reads {
            return source.as_mut().poll_next(cx);
        }
        let Some(witness) = &this.owner.witness else {
            return source.as_mut().poll_next(cx);
        };
        let recorder = &witness.recorder;
        let started = recorder.clock.now();
        let result = source.as_mut().poll_next(cx);
        recorder.read_observed(started, recorder.clock.now(), result.is_pending());
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.owner.resource.size_hint()
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for NativeSourceRelease {
    fn drop(&mut self) {
        self.recorder.source_released();
    }
}

/// Gets the recorder captured at request preparation, never an ambient current probe.
#[must_use]
#[inline]
pub fn native_body_recorder(context: &OutboundResponseContext) -> Option<NativeBodyRecorder> {
    context.native_body_recorder.clone()
}

/// Observes the original SDK future without adding polls, timers or wakeups.
#[inline]
pub async fn observe_sdk_read<F>(read: F, observation: Option<&NativeBodyRecorder>) -> F::Output
where
    F: Future,
{
    let Some(recorder) = observation else {
        return read.await;
    };
    let mut native = pin!(read);
    poll_fn(|cx| {
        let started = recorder.clock.now();
        let result = native.as_mut().poll(cx);
        let observed = recorder.clock.now();
        recorder.read_observed(started, observed, result.is_pending());
        result
    })
    .await
}

/// Observes one existing readiness call and returns its result unchanged.
/// Submitted indices bind each recorder to the body actually included in that call.
#[inline]
pub fn observe_sdk_wait<Classify, Output, Wait>(
    observations: &[(usize, NativeBodyRecorder)],
    wait: Wait,
    classify: Classify,
) -> Output
where
    Classify: Fn(&Output, usize) -> NativeBodyWaitOutcome,
    Wait: FnOnce() -> Output,
{
    let starts = observations
        .iter()
        .map(|(_, recorder)| recorder.clock.now())
        .collect::<Vec<_>>();
    let result = wait();
    for ((index, recorder), started) in observations.iter().zip(starts) {
        let observed = recorder.clock.now();
        recorder.wait_observed(started, observed, classify(&result, *index));
    }
    result
}

/// Owns the actual SDK resource until its destructor runs before the release observation.
#[must_use]
#[inline]
pub fn own_sdk_resource<Resource>(
    resource: Resource,
    observation: Option<NativeBodyRecorder>,
) -> NativeBodyOwner<Resource> {
    let witness = observation.map(|recorder| {
        recorder.source_acquired();
        NativeSourceRelease { recorder }
    });
    NativeBodyOwner { resource, witness }
}

fn bind_source(
    source: BodyStream,
    observation: Option<NativeBodyRecorder>,
    observe_reads: bool,
) -> BodyStream {
    let Some(recorder) = observation else {
        return source;
    };
    NativeSource {
        observe_reads,
        owner: own_sdk_resource(source, Some(recorder)),
    }
    .boxed_local()
}

/// Tracks source destruction after the SDK owner drops, without claiming its polls are reads.
#[must_use]
#[inline]
pub fn own_sdk_source(source: BodyStream, recorder: Option<NativeBodyRecorder>) -> BodyStream {
    bind_source(source, recorder, false)
}

/// Observes an exact SDK stream, before any cooperative or deadline wrapper can yield Pending.
#[must_use]
#[inline]
pub fn observe_sdk_stream(source: BodyStream, recorder: Option<NativeBodyRecorder>) -> BodyStream {
    bind_source(source, recorder, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BudgetSource, Deadline};
    use core::task::Poll;
    use core::time::Duration;
    use futures::FutureExt as _;
    use futures::executor::block_on;
    use futures_util::stream::poll_fn as stream_poll_fn;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Source(NativeBodyWitness);

    impl Stream for Source {
        type Item = Result<Bytes, EdgeError>;
        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, None)
        }
    }
    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Source {
        fn drop(&mut self) {
            assert_eq!(
                self.0.snapshot().source_releases,
                0,
                "inner owner drops first"
            );
        }
    }

    fn prepared() -> (
        NativeBodyWitness,
        NativeBodyRecorder,
        Arc<Mutex<MonotonicInstant>>,
    ) {
        let start = MonotonicInstant::now();
        let now = Arc::new(Mutex::new(start));
        let sampled = Arc::clone(&now);
        let clock = MonotonicClock::new(move || *sampled.lock().expect("clock"));
        let witness = NativeBodyWitness::new();
        let mut request = OutboundRequest::get("http://example.test/a").expect("request");
        witness.attach(&mut request).expect("attach");
        let budget = DispatchBudget {
            cause: BudgetSource::BatchCutoff,
            deadline: Deadline::at_instant(
                start
                    .checked_add(Duration::from_millis(200))
                    .expect("deadline"),
            ),
            duration: Duration::from_millis(200),
        };
        let recorder = witness.prepare(
            clock,
            OutboundSendMode::Batch,
            budget,
            start,
            &"http://example.test/a".parse().expect("target"),
        );
        (witness, recorder, now)
    }

    #[test]
    fn attachment_is_explicit_and_cannot_be_reused() {
        let witness = NativeBodyWitness::new();
        let mut request = OutboundRequest::get("http://example.test/a").expect("request");
        witness.attach(&mut request).expect("attach");
        assert_eq!(witness.snapshot().attachments, 1);
        let mut second = OutboundRequest::get("http://example.test/b").expect("request");
        assert!(witness.attach(&mut second).is_err());
        assert!(witness.snapshot().invalid);
    }

    #[test]
    fn native_pending_is_recorded_after_the_inner_poll_and_never_overwritten() {
        let (witness, recorder, now) = prepared();
        recorder.head_received();
        let before = *now.lock().expect("clock");
        let advanced = Arc::clone(&now);
        let read = poll_fn(move |_| {
            *advanced.lock().expect("clock") =
                before.checked_add(Duration::from_micros(1)).expect("tick");
            Poll::<()>::Pending
        });
        let observed = observe_sdk_read(read, Some(&recorder));
        assert!(witness.snapshot().first_read.is_none());
        assert!(observed.now_or_never().is_none());
        let snapshot = witness.snapshot();
        let first = snapshot.first_read.expect("actual SDK poll");
        assert!(first.pending);
        assert_eq!(first.started_at, before);
        assert_eq!(
            first.observed_at,
            before.checked_add(Duration::from_micros(1)).expect("tick")
        );
        block_on(observe_sdk_read(async {}, Some(&recorder)));
        assert!(
            witness
                .snapshot()
                .first_read
                .expect("first observation retained")
                .pending
        );
        assert_eq!(witness.snapshot().ready_reads, 1);
    }

    #[test]
    fn a_ready_first_read_is_not_pending_evidence() {
        let (witness, recorder, _now) = prepared();
        recorder.head_received();
        block_on(observe_sdk_read(async {}, Some(&recorder)));
        assert!(!witness.snapshot().first_read.expect("ready result").pending);
    }

    #[test]
    fn parts_roundtrip_preserves_the_attached_witness() {
        let witness = NativeBodyWitness::new();
        let mut request = OutboundRequest::get("http://example.test/a").expect("request");
        witness.attach(&mut request).expect("attach");
        let mut restored = OutboundRequest::from_parts(request.into_parts()).expect("roundtrip");
        let replacement = NativeBodyWitness::new();
        assert!(replacement.attach(&mut restored).is_err());
        assert!(witness.snapshot().invalid);
        assert!(replacement.snapshot().invalid);
    }

    #[test]
    fn backwards_clock_invalidates_evidence_permanently() {
        let (witness, recorder, now) = prepared();
        recorder.head_received();
        let start = *now.lock().expect("clock");
        *now.lock().expect("clock") = start
            .checked_sub(Duration::from_micros(1))
            .expect("backwards");
        block_on(observe_sdk_read(async {}, Some(&recorder)));
        *now.lock().expect("clock") = start
            .checked_add(Duration::from_millis(1))
            .expect("forward");
        assert!(witness.snapshot().invalid);
    }

    #[test]
    fn ownership_wrapper_drops_the_sdk_source_first_without_claiming_a_read() {
        let (witness, recorder, _now) = prepared();
        recorder.head_received();
        let mut source = own_sdk_source(Source(witness.clone()).boxed_local(), Some(recorder));
        assert!(source.next().now_or_never().is_none());
        assert!(witness.snapshot().first_read.is_none());
        assert_eq!(witness.snapshot().source_releases, 0);
        drop(source);
        assert_eq!(witness.snapshot().source_releases, 1);
    }

    #[test]
    fn exact_sdk_stream_pending_is_observed_without_extra_polls() {
        let (witness, recorder, _now) = prepared();
        recorder.head_received();
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);
        let native = stream_poll_fn(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        })
        .boxed_local();
        let mut source = observe_sdk_stream(native, Some(recorder));
        assert!(source.next().now_or_never().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(witness.snapshot().first_read.expect("SDK poll").pending);
        drop(source);
        assert_eq!(witness.snapshot().source_releases, 1);
    }

    #[test]
    fn absent_observers_preserve_native_results_and_poll_counts() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&polls);
        let read = poll_fn(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Err::<(), _>("native failure"))
        });
        assert_eq!(
            block_on(observe_sdk_read(read, None)),
            Err("native failure")
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let stream_polls = Arc::clone(&polls);
        let mut items = [
            Ok(Bytes::from_static(b"body")),
            Err(EdgeError::bad_request("native failure")),
        ]
        .into_iter();
        let native = stream_poll_fn(move |_| {
            stream_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(items.next())
        })
        .boxed_local();
        let mut source = own_sdk_source(observe_sdk_stream(native, None), None);
        assert_eq!(
            block_on(source.next()).expect("item").expect("bytes"),
            b"body"[..]
        );
        assert!(matches!(
            block_on(source.next()),
            Some(Err(EdgeError::BadRequest { .. }))
        ));
        assert!(block_on(source.next()).is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn native_resource_owner_releases_only_after_the_actual_resource() {
        let (witness, recorder, _now) = prepared();
        recorder.head_received();
        let mut owner = own_sdk_resource(Source(witness.clone()), Some(recorder));
        assert_eq!(owner.get_ref().0.snapshot().source_acquisitions, 1);
        assert_eq!(owner.get_mut().0.snapshot().source_releases, 0);
        assert!(owner.recorder().is_some());
        drop(owner);
        let snapshot = witness.snapshot();
        assert_eq!(snapshot.source_releases, 1);
        assert!(snapshot.first_release.is_some());
        assert!(snapshot.first_read.is_none());
    }

    #[test]
    fn native_wait_timeout_is_separate_from_read_pending_and_preserves_first_wait() {
        let (witness, recorder, now) = prepared();
        recorder.head_received();
        let _owner = own_sdk_resource(Source(witness.clone()), Some(recorder.clone()));
        let start = *now.lock().expect("clock");
        let calls = AtomicUsize::new(0);
        for (milliseconds, outcome) in [
            (10, NativeBodyWaitOutcome::OtherReady),
            (20, NativeBodyWaitOutcome::TimedOut),
            (200, NativeBodyWaitOutcome::TimedOut),
        ] {
            let actual = observe_sdk_wait(
                &[(4, recorder.clone())],
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    *now.lock().expect("clock") = start
                        .checked_add(Duration::from_millis(milliseconds))
                        .expect("return time");
                    outcome
                },
                |returned, index| {
                    assert_eq!(index, 4);
                    *returned
                },
            );
            assert_eq!(actual, outcome);
        }
        let snapshot = witness.snapshot();
        assert!(!snapshot.invalid);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(snapshot.waits, 3);
        assert_eq!(snapshot.ready_waits, 0);
        assert_eq!(snapshot.failed_waits, 0);
        assert_eq!(snapshot.first_wait.expect("first wait").started_at, start);
        assert_eq!(
            snapshot.first_wait.expect("first wait").outcome,
            NativeBodyWaitOutcome::OtherReady
        );
        assert_eq!(
            snapshot
                .first_wait_timeout
                .expect("first timeout")
                .observed_at,
            start.checked_add(Duration::from_millis(20)).expect("first")
        );
        let last = snapshot.last_wait_timeout.expect("last timeout");
        assert_eq!(last.outcome, NativeBodyWaitOutcome::TimedOut);
        assert_eq!(
            last.started_at,
            start.checked_add(Duration::from_millis(20)).expect("start")
        );
        assert_eq!(
            last.observed_at,
            start.checked_add(Duration::from_millis(200)).expect("end")
        );
        assert!(snapshot.first_read.is_none());
        assert_eq!(snapshot.reads, 0);
        assert_eq!(snapshot.ready_reads, 0);
    }

    #[test]
    fn native_wait_selection_is_attributed_to_only_its_submitted_body() {
        let (first, first_recorder, _first_clock) = prepared();
        let (second, second_recorder, _second_clock) = prepared();
        first_recorder.head_received();
        second_recorder.head_received();
        let _first_owner = own_sdk_resource(Source(first.clone()), Some(first_recorder.clone()));
        let _second_owner = own_sdk_resource(Source(second.clone()), Some(second_recorder.clone()));
        let selected = observe_sdk_wait(
            &[(2, first_recorder), (5, second_recorder)],
            || 5,
            |returned, index| {
                if *returned == index {
                    NativeBodyWaitOutcome::Ready
                } else {
                    NativeBodyWaitOutcome::OtherReady
                }
            },
        );
        assert_eq!(selected, 5);
        assert_eq!(first.snapshot().ready_waits, 0);
        assert_eq!(second.snapshot().ready_waits, 1);
        assert!(first.snapshot().first_wait_timeout.is_none());
        assert!(second.snapshot().last_wait_timeout.is_none());
        assert_eq!(
            first.snapshot().first_wait.expect("first").outcome,
            NativeBodyWaitOutcome::OtherReady
        );
        assert_eq!(
            second.snapshot().first_wait.expect("second").outcome,
            NativeBodyWaitOutcome::Ready
        );
    }

    #[test]
    fn failed_and_backwards_native_waits_do_not_supply_valid_evidence() {
        let (witness, recorder, now) = prepared();
        recorder.head_received();
        let _owner = own_sdk_resource(Source(witness.clone()), Some(recorder.clone()));
        let start = *now.lock().expect("clock");
        let result = observe_sdk_wait(
            &[(0, recorder.clone())],
            || Err::<(), _>("native failure"),
            |_, _| NativeBodyWaitOutcome::Failed,
        );
        assert_eq!(result, Err("native failure"));
        assert_eq!(witness.snapshot().failed_waits, 1);
        assert!(witness.snapshot().last_wait_timeout.is_none());
        observe_sdk_wait(
            &[(0, recorder)],
            || {
                *now.lock().expect("clock") = start
                    .checked_sub(Duration::from_micros(1))
                    .expect("backwards");
            },
            |(), _| NativeBodyWaitOutcome::TimedOut,
        );
        assert!(witness.snapshot().invalid);
        assert!(witness.snapshot().first_read.is_none());
    }

    #[test]
    fn unobserved_native_wait_calls_once_without_sampling_or_classifying() {
        let calls = AtomicUsize::new(0);
        let result = observe_sdk_wait(
            &[],
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("native failure")
            },
            |_, _| panic!("unobserved waits must not be classified"),
        );
        assert_eq!(result, Err("native failure"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
