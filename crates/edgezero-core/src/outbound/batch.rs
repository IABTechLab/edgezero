use std::collections::VecDeque;
use std::fmt;
use std::iter;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::StreamExt as _;
use futures_util::future::poll_fn;
use futures_util::stream::{LocalBoxStream, Stream};

use super::{
    OutboundDispatchRequest, OutboundExchangeOwner, OutboundResponse, OutboundScope,
    OutboundSlotResult, OutboundTime, Revocable,
};
use crate::error::{BudgetSource, EdgeError};
use crate::time::{Deadline, DispatchBudget, MonotonicClock, MonotonicInstant};

type Observation = (usize, MonotonicInstant, Result<OutboundResponse, EdgeError>);
type ProducerObservation = fn(
    &mut BatchProducer,
    &OutboundBatchContext,
    &mut Context<'_>,
) -> Poll<Result<Option<OutboundBatchDriverEvent>, EdgeError>>;

/// Facade-validated, indexed requests and their single-use batch context.
pub struct OutboundPreparedBatch {
    context: OutboundBatchContext,
    slots: Vec<(usize, OutboundDispatchRequest)>,
}

/// Immutable timing, budget provenance and preflight outcomes captured by the facade.
#[derive(Debug)]
pub struct OutboundBatchContext {
    budgets: Vec<Option<DispatchBudget>>,
    observation_cutoff: Deadline,
    preflight: Vec<Observation>,
    scope: OutboundScope,
    started_at: MonotonicInstant,
    time: OutboundTime,
}

#[derive(Debug)]
struct BatchIdentity {
    slot_count: usize,
}

/// A single-use receipt for a validated result from one particular batch.
#[derive(Debug)]
pub struct OutboundBatchItem {
    identity: Rc<BatchIdentity>,
    index: usize,
    result: Box<OutboundSlotResult>,
}

/// The ownership state of one input position after collection.
#[derive(Debug)]
pub enum OutboundCollectedSlot {
    AlreadyDelivered,
    Resolved(OutboundSlotResult),
    Unresolved,
}

/// Input-index-aligned collection retaining its batch identity until consumed.
#[derive(Debug)]
pub struct OutboundBatchResults {
    identity: Rc<BatchIdentity>,
    slots: Vec<OutboundCollectedSlot>,
    termination: OutboundBatchTermination,
}

/// A precise driver failure together with every retained input position.
#[derive(Debug, thiserror::Error)]
#[error("outbound batch driver failed: {error}")]
pub struct OutboundBatchFailure {
    #[source]
    error: EdgeError,
    identity: Rc<BatchIdentity>,
    slots: Vec<OutboundCollectedSlot>,
}

/// Why observation stopped. Completion requires clean driver EOF.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundBatchTermination {
    Completed,
    Cutoff,
}

/// One ownership-transferring asynchronous observation.
#[derive(Debug)]
pub enum OutboundBatchNext {
    Finished(OutboundBatchTermination),
    Item(OutboundBatchItem),
}

/// One nonblocking observation, using the same ledger as asynchronous observation.
#[derive(Debug)]
pub enum OutboundBatchCheck {
    Finished(OutboundBatchTermination),
    Item(OutboundBatchItem),
    Pending,
}

/// Raw terminal observations of work already scheduled by the producer.
#[derive(Debug)]
pub enum OutboundBatchDriverEvent {
    Cutoff,
    Failed(EdgeError),
    Item {
        index: usize,
        completed_at: MonotonicInstant,
        outcome: Result<OutboundResponse, EdgeError>,
    },
}

/// Optional single-use receipt storage for mixed observation and collection.
#[derive(Debug)]
pub struct OutboundBatchAccumulator {
    identity: Rc<BatchIdentity>,
    slots: Vec<Option<OutboundSlotResult>>,
}

/// A rejected insertion, retaining the exact original receipt.
#[derive(Debug, thiserror::Error)]
#[error("outbound batch receipt insertion failed: {error}")]
pub struct OutboundBatchInsertFailure {
    #[source]
    error: EdgeError,
    item: OutboundBatchItem,
}

/// A rejected merge, retaining both unchanged inputs and the validation error.
#[derive(Debug, thiserror::Error)]
#[error("outbound batch collection merge failed: {error}")]
pub struct OutboundBatchMergeFailure {
    accumulator: OutboundBatchAccumulator,
    collection: Box<Result<OutboundBatchResults, OutboundBatchFailure>>,
    #[source]
    error: EdgeError,
}

struct RecordedItem {
    completed_at: MonotonicInstant,
    index: usize,
    result: OutboundSlotResult,
}

struct BatchProducer {
    owner: Option<OutboundExchangeOwner>,
    stream: LocalBoxStream<'static, OutboundBatchDriverEvent>,
}

impl BatchProducer {
    fn drive(
        &mut self,
        context: &OutboundBatchContext,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<OutboundBatchDriverEvent>, EdgeError>> {
        let mut receiver = self.stream.next();
        context.scope.poll(
            &context.time,
            context.started_at,
            cx,
            Pin::new(&mut receiver),
        )
    }

    fn observe(
        &mut self,
        context: &OutboundBatchContext,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<OutboundBatchDriverEvent>, EdgeError>> {
        context
            .scope
            .with(|| self.stream.poll_next_unpin(cx).map(Ok))
            .unwrap_or_else(|error| Poll::Ready(Err(error)))
    }
}

#[expect(clippy::missing_trait_methods, reason = "Drop::pin_drop is unstable")]
impl Drop for BatchProducer {
    fn drop(&mut self) {
        drop(self.owner.take());
    }
}

/// Validates producer observations without scheduling work or introducing an executor.
pub struct OutboundBatch {
    context: OutboundBatchContext,
    delivered: Vec<bool>,
    identity: Rc<BatchIdentity>,
    poisoned: bool,
    producer: Option<Revocable<BatchProducer>>,
    ready: VecDeque<RecordedItem>,
    resolved: Vec<bool>,
    seen: Vec<bool>,
    terminal: Option<Result<OutboundBatchTermination, EdgeError>>,
}

impl OutboundPreparedBatch {
    #[must_use]
    #[inline]
    pub fn clock(&self) -> &MonotonicClock {
        self.context.clock()
    }

    #[must_use]
    #[inline]
    pub fn into_slots(self) -> (OutboundBatchContext, Vec<(usize, OutboundDispatchRequest)>) {
        (self.context, self.slots)
    }

    pub(super) fn new(
        context: OutboundBatchContext,
        slots: Vec<(usize, OutboundDispatchRequest)>,
    ) -> Self {
        Self { context, slots }
    }

    #[must_use]
    #[inline]
    pub fn observation_cutoff(&self) -> Deadline {
        self.context.observation_cutoff()
    }

    #[must_use]
    #[inline]
    pub fn scope(&self) -> &OutboundScope {
        self.context.scope()
    }

    #[must_use]
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.context.slot_count()
    }

    #[must_use]
    #[inline]
    pub fn started_at(&self) -> MonotonicInstant {
        self.context.started_at()
    }

    #[must_use]
    #[inline]
    pub fn time(&self) -> &OutboundTime {
        self.context.time()
    }
}

impl OutboundBatchContext {
    #[must_use]
    #[inline]
    pub fn clock(&self) -> &MonotonicClock {
        self.time.clock()
    }

    pub(super) fn new(
        started_at: MonotonicInstant,
        time: OutboundTime,
        observation_cutoff: Deadline,
        budgets: Vec<Option<DispatchBudget>>,
        preflight: Vec<Observation>,
        scope: OutboundScope,
    ) -> Self {
        Self {
            budgets,
            observation_cutoff,
            preflight,
            scope,
            started_at,
            time,
        }
    }

    #[must_use]
    #[inline]
    pub fn observation_cutoff(&self) -> Deadline {
        self.observation_cutoff
    }

    #[must_use]
    #[inline]
    pub fn scope(&self) -> &OutboundScope {
        &self.scope
    }

    #[must_use]
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.budgets.len()
    }

    #[must_use]
    #[inline]
    pub fn started_at(&self) -> MonotonicInstant {
        self.started_at
    }

    #[must_use]
    #[inline]
    pub fn time(&self) -> &OutboundTime {
        &self.time
    }
}

impl OutboundBatchItem {
    #[must_use]
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (usize, OutboundSlotResult) {
        (self.index, *self.result)
    }

    #[must_use]
    #[inline]
    pub fn result(&self) -> &OutboundSlotResult {
        &self.result
    }
}

impl OutboundBatchResults {
    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (Vec<OutboundCollectedSlot>, OutboundBatchTermination) {
        (self.slots, self.termination)
    }

    #[must_use]
    #[inline]
    pub fn slots(&self) -> &[OutboundCollectedSlot] {
        &self.slots
    }

    #[must_use]
    #[inline]
    pub fn termination(&self) -> OutboundBatchTermination {
        self.termination
    }
}

impl OutboundBatchFailure {
    #[must_use]
    #[inline]
    pub fn error(&self) -> &EdgeError {
        &self.error
    }

    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (EdgeError, Vec<OutboundCollectedSlot>) {
        (self.error, self.slots)
    }

    #[must_use]
    #[inline]
    pub fn slots(&self) -> &[OutboundCollectedSlot] {
        &self.slots
    }
}

impl OutboundBatchInsertFailure {
    #[must_use]
    #[inline]
    pub fn error(&self) -> &EdgeError {
        &self.error
    }

    #[must_use]
    #[inline]
    pub fn into_parts(self) -> (EdgeError, OutboundBatchItem) {
        (self.error, self.item)
    }
}

impl OutboundBatchMergeFailure {
    #[must_use]
    #[inline]
    pub fn error(&self) -> &EdgeError {
        &self.error
    }

    #[inline]
    pub fn into_parts(
        self,
    ) -> (
        EdgeError,
        OutboundBatchAccumulator,
        Result<OutboundBatchResults, OutboundBatchFailure>,
    ) {
        (self.error, self.accumulator, *self.collection)
    }
}

impl OutboundBatch {
    #[must_use]
    #[inline]
    pub fn accumulator(&self) -> OutboundBatchAccumulator {
        OutboundBatchAccumulator {
            identity: Rc::clone(&self.identity),
            slots: iter::repeat_with(|| None)
                .take(self.identity.slot_count)
                .collect(),
        }
    }

    /// Releases scheduled work and returns every index never transferred to the consumer.
    #[must_use]
    #[inline]
    pub fn cancel(self) -> Vec<usize> {
        self.delivered
            .iter()
            .enumerate()
            .filter_map(|(index, delivered)| (!*delivered).then_some(index))
            .collect()
    }

    /// Observes without waiting or advancing the paired clock.
    ///
    /// # Errors
    /// Returns the exact driver failure or a typed protocol invariant error.
    #[inline]
    pub fn check(&mut self) -> Result<OutboundBatchCheck, EdgeError> {
        match self.poll_observation(
            &mut Context::from_waker(Waker::noop()),
            BatchProducer::observe,
        ) {
            Poll::Ready(result) => result,
            Poll::Pending => Ok(OutboundBatchCheck::Pending),
        }
    }

    /// Collects new results in input order while identifying previously transferred positions.
    ///
    /// # Errors
    /// Retains all partial outcomes alongside the exact driver or protocol error.
    #[inline]
    pub async fn collect(mut self) -> Result<OutboundBatchResults, OutboundBatchFailure> {
        let mut slots: Vec<_> = self
            .delivered
            .iter()
            .map(|delivered| {
                if *delivered {
                    OutboundCollectedSlot::AlreadyDelivered
                } else {
                    OutboundCollectedSlot::Unresolved
                }
            })
            .collect();
        loop {
            match self.next().await {
                Ok(OutboundBatchNext::Item(item)) => {
                    if let Some(slot) = slots.get_mut(item.index) {
                        *slot = OutboundCollectedSlot::Resolved(*item.result);
                    }
                }
                Ok(OutboundBatchNext::Finished(termination)) => {
                    return Ok(OutboundBatchResults {
                        identity: Rc::clone(&self.identity),
                        slots,
                        termination,
                    });
                }
                Err(error) => {
                    return Err(OutboundBatchFailure {
                        error,
                        identity: Rc::clone(&self.identity),
                        slots,
                    });
                }
            }
        }
    }

    /// Constructs a batch only when no transport work was scheduled.
    /// Premature cutoff and omitted earlier own-deadline outcomes are protocol failures.
    #[must_use]
    #[inline]
    pub fn cutoff(context: OutboundBatchContext) -> Self {
        let mut batch = Self::seed(context, None, None);
        if batch.terminal.is_none() {
            let terminal = if batch.resolved.iter().all(|resolved| *resolved) {
                Ok(OutboundBatchTermination::Completed)
            } else {
                batch.validate_cutoff()
            };
            batch.finish(terminal);
        }
        batch
    }

    fn drain(&mut self, cx: &mut Context<'_>, receiver: ProducerObservation) {
        while let Some(producer) = &mut self.producer {
            let observation = producer
                .with(cx, |value, context| receiver(value, &self.context, context))
                .unwrap_or_else(|error| Poll::Ready(Err(error)));
            let terminal = match observation {
                Poll::Pending => break,
                Poll::Ready(Err(error) | Ok(Some(OutboundBatchDriverEvent::Failed(error)))) => {
                    Err(error)
                }
                Poll::Ready(Ok(Some(OutboundBatchDriverEvent::Item {
                    index,
                    completed_at,
                    outcome,
                }))) => match self.record(index, completed_at, outcome, false) {
                    Ok(()) => continue,
                    Err(error) => Err(error),
                },
                Poll::Ready(Ok(Some(OutboundBatchDriverEvent::Cutoff))) => self.validate_cutoff(),
                Poll::Ready(Ok(None)) => {
                    if self.resolved.iter().all(|resolved| *resolved) {
                        Ok(OutboundBatchTermination::Completed)
                    } else {
                        Err(driver_error(
                            "outbound batch driver ended before every slot resolved",
                        ))
                    }
                }
            };
            self.finish(terminal);
            break;
        }
        self.sort_ready();
    }

    fn finish(&mut self, terminal: Result<OutboundBatchTermination, EdgeError>) {
        self.terminal = Some(terminal);
        drop(self.producer.take());
    }

    /// Takes ownership of an already-scheduled driver and its task-group cancellation guard.
    /// Ready observations are drained and ordered before delivery or terminal error reporting.
    #[must_use]
    #[inline]
    pub fn from_driver<S>(
        context: OutboundBatchContext,
        events: S,
        owner: OutboundExchangeOwner,
    ) -> Self
    where
        S: Stream<Item = OutboundBatchDriverEvent> + 'static,
    {
        Self::seed(context, Some(events.boxed_local()), Some(owner))
    }

    /// Waits for one new result or termination. Dropping a pending wait consumes no receipt.
    ///
    /// # Errors
    /// Returns the exact driver failure or a typed protocol invariant error.
    #[inline]
    pub async fn next(&mut self) -> Result<OutboundBatchNext, EdgeError> {
        // Pending is represented by Poll::Pending, never a successful waiting observation.
        poll_fn(|cx| match self.poll_observation(cx, BatchProducer::drive) {
            Poll::Ready(Ok(OutboundBatchCheck::Item(item))) => {
                Poll::Ready(Ok(OutboundBatchNext::Item(item)))
            }
            Poll::Ready(Ok(OutboundBatchCheck::Finished(termination))) => {
                Poll::Ready(Ok(OutboundBatchNext::Finished(termination)))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending | Poll::Ready(Ok(OutboundBatchCheck::Pending)) => Poll::Pending,
        })
        .await
    }

    fn poll_observation(
        &mut self,
        cx: &mut Context<'_>,
        receiver: ProducerObservation,
    ) -> Poll<Result<OutboundBatchCheck, EdgeError>> {
        if self.poisoned {
            return Poll::Ready(Err(driver_error(
                "outbound batch driver was polled after an invariant failure",
            )));
        }
        self.drain(cx, receiver);
        if let Some(item) = self.ready.pop_front() {
            if let Some(delivered) = self.delivered.get_mut(item.index) {
                *delivered = true;
            }
            return Poll::Ready(Ok(OutboundBatchCheck::Item(OutboundBatchItem {
                identity: Rc::clone(&self.identity),
                index: item.index,
                result: Box::new(item.result),
            })));
        }
        match self.terminal.take() {
            Some(Ok(termination)) => {
                self.terminal = Some(Ok(termination));
                Poll::Ready(Ok(OutboundBatchCheck::Finished(termination)))
            }
            Some(Err(error)) => {
                self.poisoned = true;
                Poll::Ready(Err(error))
            }
            None => Poll::Pending,
        }
    }

    fn record(
        &mut self,
        index: usize,
        completed_at: MonotonicInstant,
        outcome: Result<OutboundResponse, EdgeError>,
        preflight: bool,
    ) -> Result<(), EdgeError> {
        let Some(seen) = self.seen.get_mut(index) else {
            return Err(driver_error(
                "outbound batch driver emitted an out-of-range slot index",
            ));
        };
        if *seen {
            return Err(driver_error(
                "outbound batch driver emitted a duplicate slot index",
            ));
        }
        if completed_at > self.context.clock().now() {
            return Err(driver_error(
                "outbound batch driver emitted a future terminal observation",
            ));
        }
        *seen = true;
        let selected = self.context.budgets.get(index).copied().flatten();
        let expired = selected.filter(|budget| budget.deadline.is_expired_at(completed_at));
        let own_timeout = selected.filter(|budget| {
            budget.own_deadline_expired_before(self.context.observation_cutoff, completed_at)
        });
        let unbudgeted_rejection = preflight
            && selected.is_none()
            && !matches!(
                &outcome,
                Err(EdgeError::GatewayTimeout {
                    cause: BudgetSource::BatchCutoff,
                    ..
                })
            );
        if self.context.observation_cutoff.is_expired_at(completed_at)
            && own_timeout.is_none()
            && !unbudgeted_rejection
        {
            return Ok(());
        }
        // Only classified own-deadline timeouts use their logical expiry instant. Successful
        // responses and earlier typed failures keep their original terminal observation.
        let terminal_at = own_timeout.map_or(completed_at, |budget| budget.deadline.instant());
        let (elapsed, classified) =
            match completed_at.checked_duration_since(self.context.started_at) {
                None => (
                    Duration::ZERO,
                    Err(driver_error(
                        "monotonic clock moved backwards during outbound dispatch",
                    )),
                ),
                Some(observed_elapsed) => {
                    let classified_outcome = if let Some(budget) = expired {
                        Err(governing_timeout(budget, outcome.as_ref().err()))
                    } else {
                        outcome
                    };
                    let elapsed = if own_timeout.is_some() {
                        terminal_at.saturating_duration_since(self.context.started_at)
                    } else {
                        observed_elapsed
                    };
                    (elapsed, classified_outcome)
                }
            };
        if let Some(resolved) = self.resolved.get_mut(index) {
            *resolved = true;
        }
        self.ready.push_back(RecordedItem {
            completed_at: terminal_at,
            index,
            result: OutboundSlotResult::new(elapsed, classified),
        });
        Ok(())
    }

    fn seed(
        context: OutboundBatchContext,
        stream: Option<LocalBoxStream<'static, OutboundBatchDriverEvent>>,
        owner: Option<OutboundExchangeOwner>,
    ) -> Self {
        let count = context.slot_count();
        let (producer, setup_error) = match (stream, owner) {
            (Some(events), Some(cancellation)) => match Revocable::new(
                &context.scope,
                BatchProducer {
                    stream: events,
                    owner: Some(cancellation),
                },
            ) {
                Ok(value) => (Some(value), None),
                Err(error) => (None, Some(error)),
            },
            (None, None) => (None, context.scope.check().err()),
            _ => (
                None,
                Some(driver_error(
                    "outbound batch producer ownership was incomplete",
                )),
            ),
        };
        let mut batch = Self {
            context,
            delivered: vec![false; count],
            identity: Rc::new(BatchIdentity { slot_count: count }),
            poisoned: false,
            producer,
            ready: VecDeque::new(),
            resolved: vec![false; count],
            seen: vec![false; count],
            terminal: None,
        };
        for (index, completed_at, outcome) in mem::take(&mut batch.context.preflight) {
            if let Err(error) = batch.record(index, completed_at, outcome, true) {
                batch.finish(Err(error));
                break;
            }
        }
        if batch.terminal.is_none()
            && let Some(error) = setup_error
        {
            batch.finish(Err(error));
        }
        batch.sort_ready();
        batch
    }

    fn sort_ready(&mut self) {
        self.ready
            .make_contiguous()
            .sort_by_key(|item| (item.completed_at, item.index));
    }

    fn validate_cutoff(&self) -> Result<OutboundBatchTermination, EdgeError> {
        if self.resolved.iter().all(|resolved| *resolved) {
            return Err(driver_error(
                "outbound batch driver emitted cutoff after every slot resolved",
            ));
        }
        let observed_at = self.context.clock().now();
        if !self.context.observation_cutoff.is_expired_at(observed_at) {
            return Err(driver_error(
                "outbound batch driver emitted cutoff before the observation cutoff",
            ));
        }
        if self
            .context
            .budgets
            .iter()
            .zip(&self.resolved)
            .any(|(selected, resolved)| {
                !*resolved
                    && selected.is_some_and(|budget| {
                        budget.own_deadline_expired_before(
                            self.context.observation_cutoff,
                            observed_at,
                        )
                    })
            })
        {
            return Err(driver_error(
                "outbound batch driver omitted an earlier own-deadline timeout before cutoff",
            ));
        }
        Ok(OutboundBatchTermination::Cutoff)
    }
}

impl fmt::Debug for OutboundBatch {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboundBatch")
            .field("context", &self.context)
            .field("delivered", &self.delivered)
            .field("terminal", &self.terminal)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl OutboundBatchAccumulator {
    /// Inserts an intact receipt after validating batch identity, index and uniqueness.
    ///
    /// # Errors
    /// Returns the unchanged receipt and leaves this accumulator unchanged.
    #[inline]
    pub fn insert(&mut self, item: OutboundBatchItem) -> Result<(), OutboundBatchInsertFailure> {
        let error = if !Rc::ptr_eq(&self.identity, &item.identity) {
            Some(driver_error(
                "outbound batch receipt belongs to a different batch",
            ))
        } else if self.slots.len() != self.identity.slot_count {
            Some(driver_error(
                "outbound batch accumulator has an invalid slot count",
            ))
        } else {
            match self.slots.get_mut(item.index) {
                None => Some(driver_error(
                    "outbound batch receipt has an out-of-range slot index",
                )),
                Some(Some(_)) => Some(driver_error("outbound batch receipt was already inserted")),
                Some(slot @ None) => {
                    *slot = Some(*item.result);
                    return Ok(());
                }
            }
        };
        Err(OutboundBatchInsertFailure {
            error: error
                .unwrap_or_else(|| driver_error("outbound batch receipt validation failed")),
            item,
        })
    }

    /// Merges partial failure results without replacing the original driver error.
    ///
    /// # Errors
    /// Returns both unchanged inputs, keeping merge and driver errors distinct.
    #[inline]
    pub fn merge_failure(
        self,
        mut failure: OutboundBatchFailure,
    ) -> Result<OutboundBatchFailure, OutboundBatchMergeFailure> {
        if let Err(error) = self.validate_merge(&failure.identity, &failure.slots, None) {
            return Err(OutboundBatchMergeFailure {
                accumulator: self,
                collection: Box::new(Err(failure)),
                error,
            });
        }
        self.merge_slots(&mut failure.slots);
        Ok(failure)
    }

    fn merge_slots(self, slots: &mut [OutboundCollectedSlot]) {
        for (slot, receipt) in slots.iter_mut().zip(self.slots) {
            if let Some(result) = receipt {
                *slot = OutboundCollectedSlot::Resolved(result);
            }
        }
    }

    /// Substitutes stored receipts only for matching `AlreadyDelivered` positions.
    ///
    /// # Errors
    /// Returns both unchanged inputs if any receipt is missing, foreign or conflicting.
    #[inline]
    pub fn merge_success(
        self,
        mut result: OutboundBatchResults,
    ) -> Result<OutboundBatchResults, OutboundBatchMergeFailure> {
        if let Err(error) =
            self.validate_merge(&result.identity, &result.slots, Some(result.termination))
        {
            return Err(OutboundBatchMergeFailure {
                accumulator: self,
                collection: Box::new(Ok(result)),
                error,
            });
        }
        self.merge_slots(&mut result.slots);
        Ok(result)
    }

    fn validate_merge(
        &self,
        identity: &Rc<BatchIdentity>,
        slots: &[OutboundCollectedSlot],
        termination: Option<OutboundBatchTermination>,
    ) -> Result<(), EdgeError> {
        if !Rc::ptr_eq(&self.identity, identity) {
            return Err(driver_error(
                "outbound batch collection belongs to a different batch",
            ));
        }
        if slots.len() != self.identity.slot_count || self.slots.len() != self.identity.slot_count {
            return Err(driver_error(
                "outbound batch merge has an invalid slot count",
            ));
        }
        let has_unresolved = slots
            .iter()
            .any(|slot| matches!(slot, OutboundCollectedSlot::Unresolved));
        if matches!(termination, Some(OutboundBatchTermination::Completed)) && has_unresolved
            || matches!(termination, Some(OutboundBatchTermination::Cutoff)) && !has_unresolved
        {
            return Err(driver_error(
                "outbound batch collection has an invalid termination",
            ));
        }
        for (slot, receipt) in slots.iter().zip(&self.slots) {
            match (slot, receipt) {
                (OutboundCollectedSlot::AlreadyDelivered, None) => {
                    return Err(driver_error(
                        "outbound batch merge is missing a delivered receipt",
                    ));
                }
                (
                    OutboundCollectedSlot::Resolved(_) | OutboundCollectedSlot::Unresolved,
                    Some(_),
                ) => {
                    return Err(driver_error(
                        "outbound batch receipt conflicts with collection slot",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn governing_timeout(budget: DispatchBudget, original: Option<&EdgeError>) -> EdgeError {
    let mut error =
        EdgeError::gateway_timeout_caused("outbound request deadline expired", budget.cause);
    if let EdgeError::GatewayTimeout {
        provider_detail, ..
    } = &mut error
    {
        *provider_detail = original
            .and_then(EdgeError::provider_failure_detail)
            .copied();
    }
    error
}

fn driver_error(message: &'static str) -> EdgeError {
    EdgeError::internal(anyhow::anyhow!(message))
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::error::Error as _;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use futures::executor::block_on;
    use futures_util::FutureExt as _;
    use futures_util::stream;

    use super::{
        OutboundBatch, OutboundBatchCheck, OutboundBatchContext, OutboundBatchDriverEvent,
        OutboundBatchItem, OutboundBatchNext, OutboundBatchTermination, OutboundCollectedSlot,
    };
    use crate::body::Body;
    use crate::error::{BudgetSource, EdgeError};
    use crate::http::{HeaderMap, Method, StatusCode};
    use crate::outbound::{
        ContentTypeProvenance, OutboundExchangeOwner, OutboundResponse, OutboundSlotResult,
        OutboundTime,
    };
    use crate::time::{Deadline, DispatchBudget, MonotonicClock, MonotonicInstant};

    struct Fixture {
        now: Arc<Mutex<MonotonicInstant>>,
        start: MonotonicInstant,
        time: OutboundTime,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("original nonclone driver error")]
    struct OriginalError(Arc<()>);

    impl Fixture {
        fn advance(&self, millis: u64) {
            *self.now.lock().expect("advance fake clock") = self.at(millis);
        }

        fn at(&self, millis: u64) -> MonotonicInstant {
            self.start
                .checked_add(Duration::from_millis(millis))
                .expect("fixture instant")
        }

        fn batch(&self, count: usize, events: Vec<OutboundBatchDriverEvent>) -> OutboundBatch {
            OutboundBatch::from_driver(
                self.context(count),
                stream::iter(events),
                OutboundExchangeOwner::new(|| {}),
            )
        }

        fn budget(&self, millis: u64, cause: BudgetSource) -> DispatchBudget {
            DispatchBudget {
                cause,
                deadline: Deadline::at_instant(self.at(millis)),
                duration: Duration::from_millis(millis),
            }
        }

        fn context(&self, count: usize) -> OutboundBatchContext {
            OutboundBatchContext::new(
                self.start,
                self.time.clone(),
                Deadline::at_instant(self.at(100)),
                vec![Some(self.budget(100, BudgetSource::BatchCutoff)); count],
                Vec::new(),
                crate::OutboundScope::capture(Rc::new(crate::IndependentOutboundProgress)),
            )
        }

        fn event(&self, index: usize, millis: u64) -> OutboundBatchDriverEvent {
            OutboundBatchDriverEvent::Item {
                index,
                completed_at: self.at(millis),
                outcome: Ok(self.response(Body::from("original response"))),
            }
        }

        fn new() -> Self {
            let start = MonotonicInstant::now()
                .checked_add(Duration::from_hours(1))
                .expect("synthetic clock");
            let now = Arc::new(Mutex::new(start));
            let captured_now = Arc::clone(&now);
            let clock = MonotonicClock::new(move || *captured_now.lock().expect("fake clock"));
            Self {
                now,
                start,
                time: OutboundTime::new(clock, |_, _| {
                    panic!("batch ledger must not create timers")
                }),
            }
        }

        fn response(&self, body: Body) -> OutboundResponse {
            OutboundResponse::new(
                Method::GET,
                StatusCode::OK,
                HeaderMap::new(),
                body,
                self.time.clock().clone(),
                ContentTypeProvenance::Unknown,
            )
        }
    }

    fn checked_item(batch: &mut OutboundBatch) -> OutboundBatchItem {
        let OutboundBatchCheck::Item(item) = batch.check().expect("valid observation") else {
            panic!("expected checked item");
        };
        item
    }

    fn next_item(batch: &mut OutboundBatch) -> OutboundBatchItem {
        let OutboundBatchNext::Item(item) = block_on(batch.next()).expect("valid observation")
        else {
            panic!("expected next item");
        };
        item
    }

    fn assert_resolved(slot: &OutboundCollectedSlot, elapsed: u64) {
        let OutboundCollectedSlot::Resolved(result) = slot else {
            panic!("expected resolved slot");
        };
        assert_eq!(result.elapsed, Duration::from_millis(elapsed));
    }

    #[test]
    fn mixed_batch_collection_preserves_delivery_state() {
        for cutoff in [false, true] {
            let fixture = Fixture::new();
            fixture.advance(101);
            let mut events = vec![
                fixture.event(0, 30),
                fixture.event(2, 20),
                fixture.event(1, 10),
            ];
            if cutoff {
                events.push(OutboundBatchDriverEvent::Cutoff);
            }
            let mut batch = fixture.batch(if cutoff { 4 } else { 3 }, events);
            assert_eq!(checked_item(&mut batch).index(), 1);
            assert_eq!(next_item(&mut batch).index(), 2);
            let result = block_on(batch.collect()).expect("mixed collection");
            assert_resolved(result.slots().first().expect("slot zero"), 30);
            assert!(matches!(
                result.slots().get(1),
                Some(OutboundCollectedSlot::AlreadyDelivered)
            ));
            assert!(matches!(
                result.slots().get(2),
                Some(OutboundCollectedSlot::AlreadyDelivered)
            ));
            if cutoff {
                assert!(matches!(
                    result.slots().get(3),
                    Some(OutboundCollectedSlot::Unresolved)
                ));
                assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
            } else {
                assert_eq!(result.termination(), OutboundBatchTermination::Completed);
            }
        }
    }

    #[test]
    fn arrival_ledger_preserves_late_collection_and_cancel_safety() {
        let fixture = Fixture::new();
        fixture.advance(101);
        let mut batch = fixture.batch(
            4,
            vec![
                fixture.event(0, 99),
                fixture.event(2, 20),
                fixture.event(1, 20),
                fixture.event(3, 100),
                OutboundBatchDriverEvent::Cutoff,
            ],
        );
        for (index, elapsed) in [(1, 20), (2, 20), (0, 99)] {
            let item = checked_item(&mut batch);
            assert_eq!(item.index(), index);
            assert_eq!(item.result().elapsed, Duration::from_millis(elapsed));
            item.result().outcome.as_ref().expect("on-time response");
        }
        for _ in 0_u8..2 {
            assert!(matches!(
                batch.check().expect("stable cutoff"),
                OutboundBatchCheck::Finished(OutboundBatchTermination::Cutoff)
            ));
        }
        assert_eq!(batch.cancel(), vec![3]);

        let mut pending_batch = OutboundBatch::from_driver(
            fixture.context(1),
            stream::pending(),
            OutboundExchangeOwner::new(|| {}),
        );
        assert!(pending_batch.next().now_or_never().is_none());
        assert_eq!(
            pending_batch.cancel(),
            vec![0],
            "cancelled wait consumed no index"
        );
    }

    #[test]
    fn earlier_connect_timeout_survives_late_collection() {
        let fixture = Fixture::new();
        fixture.advance(200);
        let mut context = fixture.context(1);
        context.budgets = vec![Some(fixture.budget(80, BudgetSource::PerCallTimeout))];
        let mut batch = OutboundBatch::from_driver(
            context,
            stream::iter([OutboundBatchDriverEvent::Item {
                index: 0,
                completed_at: fixture.at(10),
                outcome: Err(EdgeError::gateway_timeout_caused(
                    "original connect timeout",
                    BudgetSource::ConnectTimeout,
                )),
            }]),
            OutboundExchangeOwner::new(|| {}),
        );
        let item = next_item(&mut batch);
        assert!(matches!(
            &item.result().outcome,
            Err(EdgeError::GatewayTimeout { cause: BudgetSource::ConnectTimeout, message, .. })
                if message == "original connect timeout"
        ));
        assert_eq!(item.result().elapsed, Duration::from_millis(10));
        assert!(matches!(
            block_on(batch.next()).expect("completed"),
            OutboundBatchNext::Finished(OutboundBatchTermination::Completed)
        ));
    }

    #[test]
    fn own_deadline_exception_uses_retained_metadata_not_collection_time() {
        for cause in [
            BudgetSource::PerCallTimeout,
            BudgetSource::RequestDeadline,
            BudgetSource::Default,
        ] {
            let fixture = Fixture::new();
            fixture.advance(200);
            let mut context = fixture.context(3);
            context.budgets = vec![
                Some(fixture.budget(50, cause)),
                Some(fixture.budget(100, cause)),
                Some(fixture.budget(80, cause)),
            ];
            let batch = OutboundBatch::from_driver(
                context,
                stream::iter([
                    fixture.event(0, 150),
                    fixture.event(1, 100),
                    fixture.event(2, 79),
                    OutboundBatchDriverEvent::Cutoff,
                ]),
                OutboundExchangeOwner::new(|| {}),
            );
            let result = block_on(batch.collect()).expect("own deadline exception");
            let Some(OutboundCollectedSlot::Resolved(timeout)) = result.slots().first() else {
                panic!("own deadline must be delivered");
            };
            assert!(matches!(
                &timeout.outcome,
                Err(EdgeError::GatewayTimeout { cause: actual, .. }) if *actual == cause
            ));
            assert_eq!(timeout.elapsed, Duration::from_millis(50));
            assert!(matches!(
                result.slots().get(1),
                Some(OutboundCollectedSlot::Unresolved)
            ));
            let Some(OutboundCollectedSlot::Resolved(early)) = result.slots().get(2) else {
                panic!("earlier success is retained");
            };
            early.outcome.as_ref().expect("retained earlier response");
        }
    }

    #[test]
    fn late_own_timeouts_are_ordered_at_logical_expiry_with_index_ties() {
        let fixture = Fixture::new();
        fixture.advance(200);
        let mut context = fixture.context(4);
        context.budgets = vec![
            Some(fixture.budget(50, BudgetSource::PerCallTimeout)),
            Some(fixture.budget(100, BudgetSource::BatchCutoff)),
            Some(fixture.budget(100, BudgetSource::BatchCutoff)),
            Some(fixture.budget(50, BudgetSource::RequestDeadline)),
        ];
        let mut batch = OutboundBatch::from_driver(
            context,
            stream::iter([
                fixture.event(3, 90),
                fixture.event(0, 150),
                fixture.event(2, 60),
                fixture.event(1, 40),
            ]),
            OutboundExchangeOwner::new(|| {}),
        );
        for (index, elapsed) in [(1, 40), (0, 50), (3, 50), (2, 60)] {
            let item = checked_item(&mut batch);
            assert_eq!(item.index(), index);
            assert_eq!(item.result().elapsed, Duration::from_millis(elapsed));
            if index == 0 || index == 3 {
                assert!(matches!(
                    &item.result().outcome,
                    Err(EdgeError::GatewayTimeout { .. })
                ));
            } else {
                item.result()
                    .outcome
                    .as_ref()
                    .expect("unchanged timely success");
            }
        }
        assert!(matches!(
            batch.check().expect("clean EOF"),
            OutboundBatchCheck::Finished(OutboundBatchTermination::Completed)
        ));
    }

    #[test]
    fn preflight_errors_without_selected_budget_survive_cutoff_unchanged() {
        let fixture = Fixture::new();
        fixture.advance(150);
        let mut context = fixture.context(2);
        context.budgets = vec![None, Some(fixture.budget(100, BudgetSource::BatchCutoff))];
        context.preflight = vec![(
            0,
            fixture.at(120),
            Err(EdgeError::bad_request("unsupported option before budget")),
        )];
        let result =
            block_on(OutboundBatch::cutoff(context).collect()).expect("late preflight retained");
        let Some(OutboundCollectedSlot::Resolved(preflight)) = result.slots().first() else {
            panic!("preflight must remain deliverable");
        };
        assert_eq!(preflight.elapsed, Duration::from_millis(120));
        assert!(
            matches!(&preflight.outcome, Err(EdgeError::BadRequest { message, .. }) if message == "unsupported option before budget")
        );
        assert!(matches!(
            result.slots().get(1),
            Some(OutboundCollectedSlot::Unresolved)
        ));
        assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
    }

    #[test]
    fn budgetless_batch_cutoff_preflight_stays_unresolved_at_entry() {
        for from_driver in [false, true] {
            let fixture = Fixture::new();
            let context = OutboundBatchContext::new(
                fixture.start,
                fixture.time.clone(),
                Deadline::at_instant(fixture.start),
                vec![None, None],
                vec![
                    (
                        0,
                        fixture.start,
                        Err(EdgeError::gateway_timeout_caused(
                            "effective budget is zero",
                            BudgetSource::BatchCutoff,
                        )),
                    ),
                    (
                        1,
                        fixture.start,
                        Err(EdgeError::bad_request("unsupported option before budget")),
                    ),
                ],
                crate::OutboundScope::capture(Rc::new(crate::IndependentOutboundProgress)),
            );
            let batch = if from_driver {
                OutboundBatch::from_driver(
                    context,
                    stream::iter([OutboundBatchDriverEvent::Cutoff]),
                    OutboundExchangeOwner::new(|| {}),
                )
            } else {
                OutboundBatch::cutoff(context)
            };
            let result = block_on(batch.collect()).expect("expired observation cutoff");
            assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
            assert!(matches!(
                result.slots().first(),
                Some(OutboundCollectedSlot::Unresolved)
            ));
            let Some(OutboundCollectedSlot::Resolved(unsupported)) = result.slots().get(1) else {
                panic!("unsupported-option preflight must be retained");
            };
            assert_eq!(unsupported.elapsed, Duration::ZERO);
            assert!(matches!(
                &unsupported.outcome,
                Err(EdgeError::BadRequest { message, .. }) if message == "unsupported option before budget"
            ));
        }
    }

    #[test]
    fn future_terminal_observations_preserve_partials_and_poison() {
        for own_deadline in [false, true] {
            for collect in [false, true] {
                let fixture = Fixture::new();
                fixture.advance(10);
                let mut context = fixture.context(2);
                if own_deadline {
                    context.budgets[1] = Some(fixture.budget(50, BudgetSource::PerCallTimeout));
                }
                let releases = Rc::new(Cell::new(0_usize));
                let owner_releases = Rc::clone(&releases);
                let mut batch = OutboundBatch::from_driver(
                    context,
                    stream::iter([fixture.event(0, 10), fixture.event(1, 60)]),
                    OutboundExchangeOwner::new(move || {
                        owner_releases.set(owner_releases.get() + 1);
                    }),
                );
                if collect {
                    let failure = block_on(batch.collect()).expect_err("future observation");
                    assert!(matches!(failure.error(), EdgeError::Internal { .. }));
                    assert_eq!(
                        failure.error().message(),
                        "internal error: outbound batch driver emitted a future terminal observation"
                    );
                    assert_resolved(failure.slots().first().expect("earlier partial"), 10);
                    assert!(matches!(
                        failure.slots().get(1),
                        Some(OutboundCollectedSlot::Unresolved)
                    ));
                } else {
                    let earlier = checked_item(&mut batch);
                    assert_eq!(earlier.index(), 0);
                    assert_eq!(earlier.result().elapsed, Duration::from_millis(10));
                    earlier.result().outcome.as_ref().expect("earlier success");
                    let error = batch.check().expect_err("future observation");
                    assert!(matches!(&error, EdgeError::Internal { .. }));
                    assert_eq!(
                        error.message(),
                        "internal error: outbound batch driver emitted a future terminal observation"
                    );
                    assert!(
                        batch
                            .check()
                            .expect_err("poisoned driver")
                            .message()
                            .contains("polled after an invariant failure")
                    );
                    assert_eq!(batch.cancel(), vec![1]);
                }
                assert_eq!(
                    fixture.time.clock().now(),
                    fixture.at(10),
                    "checking must not advance time"
                );
                assert_eq!(
                    releases.get(),
                    1,
                    "release the malformed driver exactly once"
                );
            }
        }
    }

    #[test]
    fn future_preflight_observations_preserve_earlier_partials() {
        let fixture = Fixture::new();
        let mut context = fixture.context(2);
        context.budgets = vec![None, None];
        context.preflight = vec![
            (
                0,
                fixture.start,
                Err(EdgeError::bad_request("earlier preflight")),
            ),
            (
                1,
                fixture.at(10),
                Err(EdgeError::bad_request("future preflight")),
            ),
        ];
        let failure =
            block_on(OutboundBatch::cutoff(context).collect()).expect_err("future preflight");
        assert!(matches!(failure.error(), EdgeError::Internal { .. }));
        assert_eq!(
            failure.error().message(),
            "internal error: outbound batch driver emitted a future terminal observation"
        );
        assert_resolved(failure.slots().first().expect("earlier partial"), 0);
        let Some(OutboundCollectedSlot::Resolved(earlier)) = failure.slots().first() else {
            panic!("earlier preflight retained");
        };
        assert!(
            matches!(&earlier.outcome, Err(EdgeError::BadRequest { message, .. }) if message == "earlier preflight")
        );
        assert!(matches!(
            failure.slots().get(1),
            Some(OutboundCollectedSlot::Unresolved)
        ));
    }

    #[test]
    fn cutoff_requires_expiry_and_every_earlier_own_deadline_observation() {
        for omit_timeout in [false, true] {
            let fixture = Fixture::new();
            let mut context = fixture.context(1);
            if omit_timeout {
                fixture.advance(101);
                context.budgets = vec![Some(fixture.budget(50, BudgetSource::RequestDeadline))];
            }
            let batch = OutboundBatch::from_driver(
                context,
                stream::iter([OutboundBatchDriverEvent::Cutoff]),
                OutboundExchangeOwner::new(|| {}),
            );
            let failure = block_on(batch.collect()).expect_err("invalid cutoff");
            let expected = if omit_timeout {
                "omitted an earlier own-deadline"
            } else {
                "before the observation cutoff"
            };
            assert!(failure.error().to_string().contains(expected));
            assert!(matches!(
                failure.slots().first(),
                Some(OutboundCollectedSlot::Unresolved)
            ));
        }
    }

    #[test]
    fn no_scheduled_work_preserves_preflight_and_rejects_premature_cutoff() {
        let fixture = Fixture::new();
        let mut context = fixture.context(2);
        context.preflight = vec![(
            0,
            fixture.at(0),
            Err(EdgeError::bad_request("exact preflight")),
        )];
        fixture.advance(100);
        let result = block_on(OutboundBatch::cutoff(context).collect()).expect("expired cutoff");
        let Some(OutboundCollectedSlot::Resolved(preflight)) = result.slots().first() else {
            panic!("preflight retained");
        };
        assert!(
            matches!(&preflight.outcome, Err(EdgeError::BadRequest { message, .. }) if message == "exact preflight")
        );
        assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
        assert_eq!(
            block_on(OutboundBatch::cutoff(fixture.context(0)).collect())
                .expect("empty")
                .termination(),
            OutboundBatchTermination::Completed
        );

        let early_fixture = Fixture::new();
        let failure = block_on(OutboundBatch::cutoff(early_fixture.context(1)).collect())
            .expect_err("premature");
        assert!(
            failure
                .error()
                .to_string()
                .contains("before the observation cutoff")
        );
        let mut resolved_context = early_fixture.context(1);
        resolved_context.preflight = vec![(
            0,
            early_fixture.at(0),
            Err(EdgeError::bad_request("resolved preflight")),
        )];
        assert_eq!(
            block_on(OutboundBatch::cutoff(resolved_context).collect())
                .expect("all preflight resolved")
                .termination(),
            OutboundBatchTermination::Completed
        );
    }

    #[test]
    fn preflight_seeding_orders_and_applies_own_deadline_exception() {
        let fixture = Fixture::new();
        fixture.advance(150);
        let mut context = fixture.context(3);
        context.budgets = vec![
            Some(fixture.budget(50, BudgetSource::PerCallTimeout)),
            None,
            None,
        ];
        context.preflight = vec![
            (
                0,
                fixture.at(120),
                Err(EdgeError::bad_gateway("late preflight")),
            ),
            (
                2,
                fixture.at(10),
                Err(EdgeError::bad_request("second index")),
            ),
            (
                1,
                fixture.at(10),
                Err(EdgeError::bad_request("first index")),
            ),
        ];
        let mut batch = OutboundBatch::cutoff(context);
        assert_eq!(checked_item(&mut batch).index(), 1);
        assert_eq!(checked_item(&mut batch).index(), 2);
        let item = checked_item(&mut batch);
        assert_eq!(item.index(), 0);
        assert!(matches!(
            &item.result().outcome,
            Err(EdgeError::GatewayTimeout {
                cause: BudgetSource::PerCallTimeout,
                ..
            })
        ));
        assert!(matches!(
            batch.check().expect("all preflight resolved"),
            OutboundBatchCheck::Finished(OutboundBatchTermination::Completed)
        ));
    }

    #[test]
    fn malformed_driver_and_trailing_events_preserve_partials_and_poison() {
        let fixture = Fixture::new();
        fixture.advance(100);
        for (count, trailing, diagnostic) in [
            (2, None, "ended before every slot resolved"),
            (1, Some(fixture.event(0, 20)), "duplicate slot index"),
            (1, Some(fixture.event(1, 20)), "out-of-range slot index"),
            (
                1,
                Some(OutboundBatchDriverEvent::Cutoff),
                "cutoff after every slot resolved",
            ),
            (
                1,
                Some(OutboundBatchDriverEvent::Failed(EdgeError::bad_gateway(
                    "exact finalization failure",
                ))),
                "exact finalization failure",
            ),
        ] {
            let mut events = vec![fixture.event(0, 10)];
            events.extend(trailing);
            let mut batch = fixture.batch(count, events);
            assert_eq!(checked_item(&mut batch).index(), 0);
            assert!(
                batch
                    .check()
                    .expect_err("precise first failure")
                    .to_string()
                    .contains(diagnostic)
            );
            assert!(
                batch
                    .check()
                    .expect_err("poisoned")
                    .to_string()
                    .contains("polled after an invariant failure")
            );
        }
        let failure = block_on(fixture.batch(2, vec![fixture.event(1, 10)]).collect())
            .expect_err("premature EOF");
        assert!(matches!(
            failure.slots().first(),
            Some(OutboundCollectedSlot::Unresolved)
        ));
        assert_resolved(failure.slots().get(1).expect("partial result"), 10);
        for event in [
            fixture.event(0, 10),
            OutboundBatchDriverEvent::Cutoff,
            OutboundBatchDriverEvent::Failed(EdgeError::bad_gateway("empty failed")),
        ] {
            block_on(fixture.batch(0, vec![event]).collect()).expect_err("malformed empty driver");
        }
    }

    #[test]
    fn owner_released_once_on_terminal_even_when_batch_is_retained() {
        for mode in 0_u8..5 {
            let fixture = Fixture::new();
            fixture.advance(101);
            let releases = Rc::new(Cell::new(0_usize));
            let captured = Rc::clone(&releases);
            let events = match mode {
                0 => vec![fixture.event(0, 10)],
                1 => vec![fixture.event(0, 10), OutboundBatchDriverEvent::Cutoff],
                2 => vec![
                    fixture.event(0, 10),
                    OutboundBatchDriverEvent::Failed(EdgeError::bad_gateway(
                        "setup failed after scheduling",
                    )),
                ],
                3 => vec![fixture.event(0, 10), fixture.event(0, 20)],
                _ => Vec::new(),
            };
            let count = if mode == 1 || mode == 2 { 2 } else { 1 };
            let mut batch = OutboundBatch::from_driver(
                fixture.context(count),
                stream::iter(events),
                OutboundExchangeOwner::new(move || captured.set(captured.get() + 1)),
            );
            assert_eq!(releases.get(), 0);
            let _observation = batch.check();
            assert_eq!(
                releases.get(),
                1,
                "release before retained results are drained"
            );
            drop(batch);
            assert_eq!(releases.get(), 1);
        }
    }

    #[test]
    fn pending_next_is_cancellation_safe_and_clean_eof_is_required() {
        let fixture = Fixture::new();
        let polls = Rc::new(Cell::new(0_usize));
        let source_polls = Rc::clone(&polls);
        let event = Rc::new(RefCell::new(Some(fixture.event(0, 10))));
        let source_event = Rc::clone(&event);
        let releases = Rc::new(Cell::new(0_usize));
        let source_releases = Rc::clone(&releases);
        let mut batch = OutboundBatch::from_driver(
            fixture.context(1),
            stream::poll_fn(move |_cx| {
                let poll = source_polls.get();
                source_polls.set(poll + 1);
                match poll {
                    0 | 2 => Poll::Pending,
                    1 => Poll::Ready(source_event.borrow_mut().take()),
                    _ => Poll::Ready(None),
                }
            }),
            OutboundExchangeOwner::new(move || source_releases.set(source_releases.get() + 1)),
        );
        assert!(batch.next().now_or_never().is_none());
        assert_eq!(releases.get(), 0);
        fixture.advance(10);
        assert_eq!(next_item(&mut batch).index(), 0);
        assert_eq!(releases.get(), 0, "last slot is not clean EOF");
        assert!(matches!(
            batch.check().expect("EOF"),
            OutboundBatchCheck::Finished(OutboundBatchTermination::Completed)
        ));
        assert_eq!(releases.get(), 1);
        assert!(matches!(
            batch.check().expect("repeated EOF"),
            OutboundBatchCheck::Finished(OutboundBatchTermination::Completed)
        ));
        assert_eq!(polls.get(), 4);
    }

    #[test]
    fn cancellation_includes_recorded_but_undelivered_indices() {
        let fixture = Fixture::new();
        fixture.advance(20);
        let mut batch = fixture.batch(4, vec![fixture.event(1, 10), fixture.event(2, 20)]);
        assert_eq!(checked_item(&mut batch).index(), 1);
        assert_eq!(batch.cancel(), vec![0, 2, 3]);
    }

    #[test]
    fn backward_observation_is_zero_elapsed_typed_internal() {
        let fixture = Fixture::new();
        let before = fixture
            .start
            .checked_sub(Duration::from_millis(1))
            .expect("before entry");
        let mut batch = fixture.batch(
            1,
            vec![OutboundBatchDriverEvent::Item {
                index: 0,
                completed_at: before,
                outcome: Ok(fixture.response(Body::empty())),
            }],
        );
        let item = checked_item(&mut batch);
        assert_eq!(item.result().elapsed, Duration::ZERO);
        assert!(matches!(
            &item.result().outcome,
            Err(EdgeError::Internal { .. })
        ));
    }

    #[test]
    fn batch_accumulator_rejects_foreign_or_missing_items() {
        let fixture = Fixture::new();
        fixture.advance(20);
        let mut batch = fixture.batch(2, vec![fixture.event(0, 10), fixture.event(1, 20)]);
        let accumulator = batch.accumulator();
        let receipt = checked_item(&mut batch);
        let collection = block_on(batch.collect()).expect("collection");
        let rejection = accumulator
            .merge_success(collection)
            .expect_err("missing receipt");
        assert!(
            rejection
                .error()
                .to_string()
                .contains("missing a delivered receipt")
        );
        let (_, mut recovered_accumulator, recovered_collection) = rejection.into_parts();
        recovered_accumulator
            .insert(receipt)
            .expect("receipt recovered");
        let recovered_success = recovered_collection.expect("original success tag");

        let foreign = fixture.batch(2, Vec::new()).accumulator();
        let foreign_rejection = foreign
            .merge_success(recovered_success)
            .expect_err("foreign collection");
        assert!(
            foreign_rejection
                .error()
                .to_string()
                .contains("different batch")
        );
        let (_, _, retained_collection) = foreign_rejection.into_parts();
        let result = recovered_accumulator
            .merge_success(retained_collection.expect("retained collection"))
            .expect("matching merge");
        assert_resolved(result.slots().first().expect("restored receipt"), 10);
        assert_resolved(result.slots().get(1).expect("collected result"), 20);
    }

    #[test]
    fn batch_collections_preserve_identity_until_consumed() {
        let fixture = Fixture::new();
        fixture.advance(101);
        for failure in [false, true] {
            let mut events = vec![
                fixture.event(0, 10),
                fixture.event(1, 20),
                fixture.event(2, 30),
            ];
            if failure {
                events.push(OutboundBatchDriverEvent::Failed(EdgeError::bad_gateway(
                    "original driver failure",
                )));
            } else {
                events.push(OutboundBatchDriverEvent::Cutoff);
            }
            let mut batch = fixture.batch(4, events);
            let mut accumulator = batch.accumulator();
            let receipt = checked_item(&mut batch);
            assert_eq!(receipt.index(), 0);
            receipt
                .result()
                .outcome
                .as_ref()
                .expect("inspected response");
            accumulator
                .insert(receipt)
                .expect("intact inspected receipt");
            let next_receipt = next_item(&mut batch);
            assert_eq!(next_receipt.index(), 1);
            accumulator.insert(next_receipt).expect("next receipt");
            if failure {
                let result = block_on(batch.collect()).expect_err("driver failure");
                assert!(
                    result
                        .error()
                        .to_string()
                        .contains("original driver failure")
                );
                assert_eq!(result.slots().len(), 4);
                let merged = accumulator.merge_failure(result).expect("failure merge");
                let (error, slots) = merged.into_parts();
                assert!(error.to_string().contains("original driver failure"));
                assert_resolved(slots.first().expect("merged receipt"), 10);
                assert_resolved(slots.get(1).expect("remaining result"), 20);
                assert_resolved(slots.get(2).expect("collected result"), 30);
                assert!(matches!(
                    slots.get(3),
                    Some(OutboundCollectedSlot::Unresolved)
                ));
            } else {
                let result = block_on(batch.collect()).expect("cutoff");
                assert_eq!(result.slots().len(), 4);
                assert_eq!(result.termination(), OutboundBatchTermination::Cutoff);
                let (slots, termination) = accumulator
                    .merge_success(result)
                    .expect("success merge")
                    .into_parts();
                assert_resolved(slots.first().expect("merged receipt"), 10);
                assert_eq!(termination, OutboundBatchTermination::Cutoff);
            }
        }
    }

    #[test]
    fn batch_recovery_returns_exact_nonclone_inputs() {
        let fixture = Fixture::new();
        fixture.advance(10);
        let body_releases = Rc::new(Cell::new(0_usize));
        let captured_releases = Rc::clone(&body_releases);
        let body_owner =
            OutboundExchangeOwner::new(move || captured_releases.set(captured_releases.get() + 1));
        let body = Body::from_stream(stream::poll_fn(move |_cx| {
            let _keep_owner = &body_owner;
            Poll::Pending
        }));
        let error_identity = Arc::new(());
        let original_error = EdgeError::internal(anyhow::Error::new(OriginalError(Arc::clone(
            &error_identity,
        ))));
        let mut batch = fixture.batch(
            2,
            vec![
                OutboundBatchDriverEvent::Item {
                    index: 0,
                    completed_at: fixture.at(10),
                    outcome: Ok(fixture.response(body)),
                },
                OutboundBatchDriverEvent::Failed(original_error),
            ],
        );
        let accumulator = batch.accumulator();
        let item = checked_item(&mut batch);
        let mut foreign = fixture.batch(2, Vec::new()).accumulator();
        let rejected = foreign.insert(item).expect_err("foreign insert");
        assert!(rejected.source().is_some());
        assert!(foreign.slots.iter().all(Option::is_none));
        let (_, recovered_item) = rejected.into_parts();
        assert_eq!(body_releases.get(), 0);

        let failure = block_on(batch.collect()).expect_err("precise driver error");
        let rejected_merge = accumulator
            .merge_failure(failure)
            .expect_err("missing receipt");
        assert!(rejected_merge.source().is_some());
        let (_, mut recovered_accumulator, recovered_collection) = rejected_merge.into_parts();
        let recovered_failure = recovered_collection.expect_err("original failure tag");
        let EdgeError::Internal { source, .. } = recovered_failure.error() else {
            panic!("original typed error");
        };
        assert!(Arc::ptr_eq(
            &source
                .downcast_ref::<OriginalError>()
                .expect("original source")
                .0,
            &error_identity
        ));
        recovered_accumulator
            .insert(recovered_item)
            .expect("recovered receipt");
        let merged = recovered_accumulator
            .merge_failure(recovered_failure)
            .expect("recovered merge");
        assert_eq!(
            body_releases.get(),
            0,
            "no loss during rejected insert or merge"
        );
        drop(merged);
        assert_eq!(
            body_releases.get(),
            1,
            "exactly one body owner survives recovery"
        );
    }

    #[test]
    fn accumulator_rejects_duplicate_index_count_and_conflict_before_mutation() {
        let fixture = Fixture::new();
        fixture.advance(10);
        let mut batch = fixture.batch(1, vec![fixture.event(0, 10)]);
        let mut accumulator = batch.accumulator();
        let receipt = checked_item(&mut batch);
        accumulator.insert(receipt).expect("first receipt");
        let duplicate = OutboundBatchItem {
            identity: Rc::clone(&accumulator.identity),
            index: 0,
            result: Box::new(OutboundSlotResult::new(
                Duration::ZERO,
                Err(EdgeError::bad_request("duplicate")),
            )),
        };
        let rejected = accumulator
            .insert(duplicate)
            .expect_err("private malformed duplicate");
        assert!(rejected.error().to_string().contains("already inserted"));
        assert!(accumulator.slots.first().expect("unchanged slot").is_some());
        let (_, mut malformed) = rejected.into_parts();
        malformed.index = 1;
        assert!(
            accumulator
                .insert(malformed)
                .expect_err("invalid index")
                .error()
                .to_string()
                .contains("out-of-range")
        );
        let mut collection = block_on(batch.collect()).expect("collection");
        collection.slots = vec![OutboundCollectedSlot::Resolved(OutboundSlotResult::new(
            Duration::ZERO,
            Err(EdgeError::bad_request("conflicting result")),
        ))];
        let rejected_conflict = accumulator.merge_success(collection).expect_err("conflict");
        assert!(rejected_conflict.error().to_string().contains("conflicts"));
        let (_, recovered_accumulator, recovered_collection) = rejected_conflict.into_parts();
        let mut recovered_success = recovered_collection.expect("original success");
        recovered_success
            .slots
            .push(OutboundCollectedSlot::Unresolved);
        let rejected_length = recovered_accumulator
            .merge_success(recovered_success)
            .expect_err("wrong length");
        assert!(
            rejected_length
                .error()
                .to_string()
                .contains("invalid slot count")
        );
    }

    #[test]
    fn batch_check_does_not_poll_timers_or_move_clock() {
        let fixture = Fixture::new();
        let mut batch = OutboundBatch::from_driver(
            fixture.context(1),
            stream::pending(),
            OutboundExchangeOwner::new(|| {}),
        );
        assert!(matches!(
            batch.check().expect("pending"),
            OutboundBatchCheck::Pending
        ));
        assert_eq!(fixture.time.clock().now(), fixture.start);
        let mut pending = Box::pin(batch.next());
        assert!(
            pending
                .as_mut()
                .poll_unpin(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(pending);
        assert_eq!(batch.cancel(), vec![0]);
    }
}
