#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use edgezero_core::http::{HeaderMap, StatusCode, Uri};
    use edgezero_core::{
        BudgetSource, Deadline, DispatchBudget, EdgeError, HttpClient, MonotonicClock,
        MonotonicInstant, OutboundBatch, OutboundBatchCheck, OutboundBatchDriverEvent,
        OutboundBatchNext, OutboundBatchTermination, OutboundCollectedSlot,
        OutboundDispatchRequest, OutboundExchangeOwner, OutboundFuture, OutboundHead,
        OutboundHeaderFidelity, OutboundHeaderInput, OutboundHttpClient, OutboundPending,
        OutboundPreparedBatch, OutboundProxyMarker, OutboundRequest, OutboundResponse,
        OutboundSendMode, OutboundTime,
    };
    use futures::executor::block_on;
    use futures_util::future::ready;
    use futures_util::task::{ArcWake, waker_ref};
    use futures_util::{FutureExt as _, StreamExt as _, stream};

    struct BatchRecord {
        indices: Vec<usize>,
        observation_cutoff: Deadline,
        slot_count: usize,
        started_at: MonotonicInstant,
    }

    #[derive(Clone)]
    struct FakeClock(Arc<Mutex<ClockState>>);

    struct ClockState {
        next_id: usize,
        now: MonotonicInstant,
        waiters: BTreeMap<usize, (Deadline, Option<Waker>)>,
    }

    struct Fixture {
        clock: FakeClock,
        state: Arc<Mutex<Recording>>,
    }

    #[derive(Default)]
    struct Recording {
        batches: Vec<BatchRecord>,
        exchanges_released: usize,
        groups_released: usize,
        requests: Vec<RequestRecord>,
        support_calls: Vec<(Uri, OutboundSendMode)>,
    }

    struct RequestRecord {
        budget: DispatchBudget,
        connect_timeout: Option<Duration>,
        entered_at: MonotonicInstant,
        mode: OutboundSendMode,
        uri: Uri,
    }

    struct SuppliedClient {
        state: Arc<Mutex<Recording>>,
        time: OutboundTime,
    }

    struct Timer {
        deadline: Deadline,
        id: usize,
        state: Arc<Mutex<ClockState>>,
    }

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl FakeClock {
        fn advance(&self, duration: Duration) {
            let wakers: Vec<_> = {
                let mut state = self.0.lock().expect("clock advancement");
                state.now = state.now.checked_add(duration).expect("bounded fake time");
                let now = state.now;
                state
                    .waiters
                    .values_mut()
                    .filter(|(deadline, _)| deadline.is_expired_at(now))
                    .filter_map(|(_, waker)| waker.take())
                    .collect()
            };
            for waker in wakers {
                waker.wake();
            }
        }

        fn clock(&self) -> MonotonicClock {
            let state = Arc::clone(&self.0);
            MonotonicClock::new(move || state.lock().expect("clock sample").now)
        }

        fn new() -> Self {
            Self(Arc::new(Mutex::new(ClockState {
                next_id: 0,
                now: MonotonicInstant::now(),
                waiters: BTreeMap::new(),
            })))
        }

        fn pair(&self) -> OutboundTime {
            let shared = Arc::clone(&self.0);
            OutboundTime::new(self.clock(), move |deadline, clock| {
                let now = clock.now();
                let mut state = shared.lock().expect("timer registration");
                assert_eq!(now, state.now);
                let id = state.next_id;
                state.next_id = state.next_id.saturating_add(1);
                state.waiters.insert(id, (deadline, None));
                Box::pin(Timer {
                    deadline,
                    id,
                    state: Arc::clone(&shared),
                })
            })
        }
    }

    impl Fixture {
        fn client(&self, shared_constructor: bool) -> HttpClient {
            let supplied = SuppliedClient {
                state: Arc::clone(&self.state),
                time: self.clock.pair(),
            };
            if shared_constructor {
                HttpClient::new(Arc::new(supplied))
            } else {
                HttpClient::with_client(supplied)
            }
        }

        fn new() -> Self {
            Self {
                clock: FakeClock::new(),
                state: Arc::new(Mutex::new(Recording::default())),
            }
        }
    }

    impl SuppliedClient {
        fn head(&self, prepared: OutboundDispatchRequest) -> Result<OutboundHead, EdgeError> {
            let budget = prepared.budget();
            let connect_timeout = prepared.request().connect_timeout_setting();
            let entered_at = prepared.entered_at();
            let uri = prepared.request().uri().clone();
            if prepared.clock().now() != prepared.time().clock().now() {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "prepared clocks differ"
                )));
            }
            let (parts, context) = prepared.into_parts();
            if parts.connect_timeout != connect_timeout || context.entered_at() != entered_at {
                return Err(EdgeError::internal(anyhow::anyhow!(
                    "prepared parts changed policy"
                )));
            }
            let record = RequestRecord {
                budget,
                connect_timeout,
                entered_at,
                mode: context.mode(),
                uri,
            };
            self.state
                .lock()
                .map_err(|_error| {
                    EdgeError::internal(anyhow::anyhow!("request recording lock poisoned"))
                })?
                .requests
                .push(record);
            let state = Arc::clone(&self.state);
            let local_owner = Rc::new(());
            OutboundHead::new(
                StatusCode::OK,
                OutboundHeaderInput::new(
                    "supplied",
                    HeaderMap::new(),
                    OutboundHeaderFidelity::AdapterVisibleFields,
                ),
                stream::empty().boxed_local(),
                context,
                OutboundExchangeOwner::new(move || {
                    drop(local_owner);
                    let mut recording = state.lock().expect("exchange release");
                    recording.exchanges_released = recording.exchanges_released.saturating_add(1);
                }),
            )
        }

        fn response(
            &self,
            prepared: OutboundDispatchRequest,
        ) -> Result<OutboundResponse, EdgeError> {
            self.head(prepared)?
                .finish()
                .now_or_never()
                .unwrap_or_else(|| {
                    Err(EdgeError::internal(anyhow::anyhow!(
                        "in-memory exchange unexpectedly pending"
                    )))
                })
        }
    }

    impl OutboundHttpClient for SuppliedClient {
        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            let completed = self.response(request);
            let local_owner = Rc::new(());
            Ok(OutboundPending::new(
                Box::pin(ready(completed)),
                OutboundExchangeOwner::new(move || drop(local_owner)),
            ))
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            let completed = self.head(request);
            Box::pin(ready(completed))
        }

        fn progress(&self) -> Rc<dyn edgezero_core::OutboundProgressRuntime> {
            Rc::new(edgezero_core::IndependentOutboundProgress)
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let record = BatchRecord {
                indices: Vec::new(),
                observation_cutoff: batch.observation_cutoff(),
                slot_count: batch.slot_count(),
                started_at: batch.started_at(),
            };
            assert_eq!(batch.clock().now(), batch.time().clock().now());
            let (context, slots) = batch.into_slots();
            assert_eq!(context.slot_count(), record.slot_count);
            assert_eq!(context.started_at(), record.started_at);
            assert_eq!(
                context.observation_cutoff().instant(),
                record.observation_cutoff.instant()
            );
            assert_eq!(context.clock().now(), context.time().clock().now());
            let completed_record = BatchRecord {
                indices: slots.iter().map(|(index, _)| *index).collect(),
                ..record
            };
            self.state
                .lock()
                .expect("batch recording")
                .batches
                .push(completed_record);
            if slots.is_empty() {
                return OutboundBatch::cutoff(context);
            }
            // Execute all eligible in-memory exchanges before handing out the observation stream.
            let events: Vec<_> = slots
                .into_iter()
                .map(|(index, prepared)| {
                    assert_eq!(prepared.entered_at(), context.started_at());
                    let outcome = self.response(prepared);
                    OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: context.clock().now(),
                        outcome,
                    }
                })
                .collect();
            let state = Arc::clone(&self.state);
            let local_owner = Rc::new(());
            OutboundBatch::from_driver(
                context,
                stream::iter(events),
                OutboundExchangeOwner::new(move || {
                    drop(local_owner);
                    let mut recording = state.lock().expect("group release");
                    recording.groups_released = recording.groups_released.saturating_add(1);
                }),
            )
        }

        fn time(&self) -> OutboundTime {
            self.time.clone()
        }

        fn validate_supported_options(
            &self,
            request: &OutboundRequest,
            mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            self.state
                .lock()
                .map_err(|_error| {
                    EdgeError::internal(anyhow::anyhow!("support recording lock poisoned"))
                })?
                .support_calls
                .push((request.uri().clone(), mode));
            if request.has_host_authority_override() {
                Err(EdgeError::bad_request(
                    "host_authority_override unsupported by supplied fixture",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
    impl Drop for Timer {
        #[inline]
        fn drop(&mut self) {
            assert!(
                self.state
                    .lock()
                    .expect("timer release")
                    .waiters
                    .remove(&self.id)
                    .is_some()
            );
        }
    }

    impl Future for Timer {
        type Output = Result<(), EdgeError>;

        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut state = self.state.lock().expect("timer poll");
            if self.deadline.is_expired_at(state.now) {
                Poll::Ready(Ok(()))
            } else {
                state.waiters.get_mut(&self.id).expect("registered timer").1 =
                    Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl ArcWake for WakeCount {
        fn wake(self: Arc<Self>) {
            Self::wake_by_ref(&self);
        }
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn assert_eager_batch(
        fixture: &Fixture,
        started_at: MonotonicInstant,
        observation_cutoff: Deadline,
    ) {
        let recording = fixture.state.lock().expect("eager recording");
        assert_eq!(
            recording.requests.len(),
            2,
            "already complete before first observation"
        );
        assert_eq!(recording.exchanges_released, 2);
        assert_eq!(recording.groups_released, 0);
        assert_eq!(recording.support_calls.len(), 4);
        assert!(
            recording
                .support_calls
                .iter()
                .all(|(_, mode)| *mode == OutboundSendMode::Batch)
        );
        let captured = recording.batches.first().expect("captured batch");
        assert_eq!(captured.indices, vec![0, 2]);
        assert_eq!(captured.slot_count, 4);
        assert_eq!(captured.started_at, started_at);
        assert_eq!(
            captured.observation_cutoff.instant(),
            observation_cutoff.instant()
        );
        assert_eq!(
            recording.requests.first().expect("zero").uri.path(),
            "/zero"
        );
        assert_eq!(recording.requests.get(1).expect("two").uri.path(), "/two");
        for request in &recording.requests {
            assert_eq!(request.entered_at, started_at);
            assert_eq!(
                request.budget.deadline.instant(),
                observation_cutoff.instant()
            );
            assert_eq!(request.budget.cause, BudgetSource::BatchCutoff);
        }
    }

    #[test]
    fn batch_producer_uses_prepared_context() {
        for shared_constructor in [false, true] {
            let fixture = Fixture::new();
            let client = fixture.client(shared_constructor);
            let started_at = fixture.clock.clock().now();
            let observation_cutoff = fixture
                .clock
                .clock()
                .deadline_after(Duration::from_millis(100));
            let requests = vec![
                OutboundRequest::get("https://example.com/zero").expect("request"),
                OutboundRequest::get("https://example.com/one")
                    .expect("request")
                    .stream_response(),
                OutboundRequest::get("https://example.com/two").expect("request"),
                OutboundRequest::get("https://example.com/three")
                    .expect("request")
                    .host_authority_override("override.example")
                    .expect("authority")
                    .timeout(Duration::ZERO),
            ];
            let mut batch = client.start_batch_until(requests, observation_cutoff);
            assert_eager_batch(&fixture, started_at, observation_cutoff);
            let mut accumulator = batch.accumulator();
            let OutboundBatchCheck::Item(first) = batch.check().expect("first observation") else {
                panic!("first receipt");
            };
            assert_eq!(first.index(), 0);
            accumulator.insert(first).expect("first receipt stored");
            let OutboundBatchNext::Item(preflight) =
                block_on(batch.next()).expect("preflight observation")
            else {
                panic!("preflight receipt");
            };
            assert_eq!(preflight.index(), 1);
            assert!(matches!(
                &preflight.result().outcome,
                Err(EdgeError::BadRequest { .. })
            ));
            accumulator
                .insert(preflight)
                .expect("preflight receipt stored");
            fixture.clock.advance(Duration::from_millis(200));
            let collected =
                block_on(batch.collect()).expect("recorded outcomes survive late collection");
            assert_eq!(collected.termination(), OutboundBatchTermination::Completed);
            assert!(matches!(
                collected.slots().first(),
                Some(OutboundCollectedSlot::AlreadyDelivered)
            ));
            let merged = accumulator
                .merge_success(collected)
                .expect("same-batch merge");
            let (slots, termination) = merged.into_parts();
            assert_eq!(termination, OutboundBatchTermination::Completed);
            let Some(OutboundCollectedSlot::Resolved(rejected)) = slots.get(3) else {
                panic!("unsupported preflight retained");
            };
            assert!(
                matches!(&rejected.outcome, Err(EdgeError::BadRequest { message, .. }) if message == "host_authority_override unsupported by supplied fixture")
            );
            assert!(
                slots
                    .iter()
                    .all(|slot| matches!(slot, OutboundCollectedSlot::Resolved(_)))
            );
            assert_eq!(
                fixture.state.lock().expect("group cleanup").groups_released,
                1
            );
            assert!(
                fixture
                    .clock
                    .0
                    .lock()
                    .expect("timer cleanup")
                    .waiters
                    .is_empty()
            );
        }
    }

    #[test]
    fn configured_connect_timeout_reaches_dispatch() {
        let fixture = Fixture::new();
        let entered_at = fixture.clock.clock().now();
        let deadline = fixture.clock.clock().deadline_after(Duration::from_secs(2));
        let operation: OutboundFuture<OutboundResponse> = {
            let client = fixture.client(true).with_proxy_marker(false);
            let request = OutboundRequest::get("https://example.com/send")
                .expect("request")
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(3))
                .deadline(deadline);
            assert!(!client.effective_proxy_marker_enabled(&request));
            client.send(request)
        };
        assert_eq!(
            fixture
                .state
                .lock()
                .expect("synchronous support hook")
                .support_calls
                .len(),
            1
        );
        fixture.clock.advance(Duration::from_millis(10));
        let response = block_on(operation).expect("owned send after client drop");
        assert!(!response.headers().contains_key("x-edgezero-proxy"));
        let recording = fixture.state.lock().expect("dispatch recording");
        let captured = recording.requests.first().expect("prepared request");
        assert_eq!(captured.connect_timeout, Some(Duration::from_secs(1)));
        assert_eq!(captured.entered_at, entered_at);
        assert_eq!(captured.budget.deadline.instant(), deadline.instant());
        assert_eq!(captured.budget.duration, Duration::from_secs(2));
        assert_eq!(captured.budget.cause, BudgetSource::RequestDeadline);
        assert_eq!(captured.mode, OutboundSendMode::Send);
    }

    #[test]
    fn connect_timeout_survives_request_parts_round_trip() {
        let fixture = Fixture::new();
        let request = OutboundRequest::get("https://example.com/parts")
            .expect("request")
            .connect_timeout(Duration::from_secs(1))
            .proxy_marker(OutboundProxyMarker::Off);
        let mut parts = request.into_parts();
        assert_eq!(parts.connect_timeout, Some(Duration::from_secs(1)));
        parts.connect_timeout = Some(Duration::from_secs(2));
        let rebuilt = OutboundRequest::from_parts(parts).expect("round trip");
        assert_eq!(
            rebuilt.connect_timeout_setting(),
            Some(Duration::from_secs(2))
        );
        let response = block_on(fixture.client(false).send(rebuilt)).expect("round-trip dispatch");
        assert!(!response.headers().contains_key("x-edgezero-proxy"));
        assert_eq!(
            fixture
                .state
                .lock()
                .expect("dispatch recording")
                .requests
                .first()
                .expect("prepared request")
                .connect_timeout,
            Some(Duration::from_secs(2))
        );
    }

    #[test]
    fn exact_observation_cutoff_is_unresolved_without_dispatch() {
        for shared_constructor in [false, true] {
            let fixture = Fixture::new();
            let observation_cutoff = Deadline::at_instant(fixture.clock.clock().now());
            let batch = fixture.client(shared_constructor).start_batch_until(
                vec![
                    OutboundRequest::get("https://example.com/expired")
                        .expect("request")
                        .timeout(Duration::from_secs(1)),
                ],
                observation_cutoff,
            );
            let result =
                block_on(batch.collect()).expect("exact cutoff is a normal unresolved batch");
            assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
            assert!(matches!(
                result.slots(),
                [OutboundCollectedSlot::Unresolved]
            ));
            let recording = fixture.state.lock().expect("zero dispatches");
            assert!(recording.requests.is_empty());
            assert_eq!(recording.exchanges_released, 0);
            assert_eq!(recording.groups_released, 0);
            assert_eq!(recording.support_calls.len(), 1);
            assert!(
                recording
                    .batches
                    .first()
                    .expect("prepared batch")
                    .indices
                    .is_empty()
            );
            let clock = fixture.clock.0.lock().expect("zero timers");
            assert_eq!(clock.next_id, 0);
            assert!(clock.waiters.is_empty());
        }
    }

    #[test]
    fn supplied_begin_returns_owned_ready_exchange() {
        let fixture = Fixture::new();
        let operation: OutboundFuture<OutboundPending> = fixture
            .client(false)
            .begin(OutboundRequest::get("https://example.com/begin").expect("request"));
        let mut pending = block_on(operation).expect("owned begin");
        assert_eq!(fixture.state.lock().expect("eager begin").requests.len(), 1);
        assert!(pending.is_ready());
        let response = block_on(pending.collect()).expect("completed exchange");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            fixture
                .state
                .lock()
                .expect("begin mode")
                .requests
                .first()
                .expect("request")
                .mode,
            OutboundSendMode::Begin
        );
    }

    #[test]
    fn supplied_timer_pair_wakes_and_unregisters() {
        let clock = FakeClock::new();
        let time = clock.pair();
        let deadline = time.clock().deadline_after(Duration::from_millis(50));
        let mut wait = time.wait_until(deadline);
        let wake = Arc::new(WakeCount::default());
        let waker = waker_ref(&wake);
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        clock.advance(Duration::from_millis(50));
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            wait.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(()))
        ));
        assert!(clock.0.lock().expect("timer cleanup").waiters.is_empty());
    }
}
