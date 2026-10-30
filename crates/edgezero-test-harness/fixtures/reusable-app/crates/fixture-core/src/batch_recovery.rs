//! Shared fixture scenarios over the real adapter; no alternate transport or scheduler.

use std::{
    fmt,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use edgezero_core::http::Uri;
use edgezero_core::{
    EdgeError, HttpClient, MonotonicInstant, OutboundBatch, OutboundBatchAccumulator,
    OutboundBatchCheck, OutboundBatchFailure, OutboundBatchItem, OutboundBatchMergeFailure,
    OutboundBatchNext, OutboundBatchResults, OutboundBatchTermination, OutboundCollectedSlot,
    OutboundDispatchRequest, OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending,
    OutboundPreparedBatch, OutboundProgressRuntime, OutboundRequest, OutboundScope,
    OutboundSendMode, OutboundSlotResult, OutboundTime,
};
use serde_json::{Value, json};

type Collection = Result<OutboundBatchResults, OutboundBatchFailure>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Terminal {
    Completed,
    Cutoff,
    Failure,
}

#[derive(Clone, Copy, Debug)]
struct Case {
    mixed: bool,
    terminal: Terminal,
}

#[derive(Debug)]
struct DriverFault(Arc<()>);

#[derive(Default)]
struct Fault {
    armed: Mutex<Option<EdgeError>>,
    delivered: AtomicUsize,
}

struct FaultClient {
    native: Arc<dyn OutboundHttpClient>,
    fault: Arc<Fault>,
}
struct FaultProgress {
    native: Rc<dyn OutboundProgressRuntime>,
    fault: Arc<Fault>,
}

impl fmt::Display for DriverFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("controlled batch driver failure")
    }
}
impl std::error::Error for DriverFault {}

impl OutboundProgressRuntime for FaultProgress {
    fn poll(
        &self,
        scope: &OutboundScope,
        time: &OutboundTime,
        entered: MonotonicInstant,
        context: &mut Context<'_>,
        inner: &mut dyn FnMut(&mut Context<'_>) -> Poll<()>,
    ) -> Poll<Result<(), EdgeError>> {
        let failure = self
            .fault
            .armed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(error) = failure {
            self.fault.delivered.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Err(error));
        }
        self.native.poll(scope, time, entered, context, inner)
    }
}

impl OutboundHttpClient for FaultClient {
    fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        self.native.begin(request)
    }
    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        self.native.dispatch_head(request)
    }
    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(FaultProgress {
            native: self.native.progress(),
            fault: Arc::clone(&self.fault),
        })
    }
    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        self.native.start_batch_until(batch)
    }
    fn time(&self) -> OutboundTime {
        self.native.time()
    }
    fn validate_supported_options(
        &self,
        request: &OutboundRequest,
        mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        self.native.validate_supported_options(request, mode)
    }
}

fn invalid() -> EdgeError {
    EdgeError::internal(std::io::Error::other(
        "batch recovery fixture contract failed",
    ))
}
fn ensure(valid: bool) -> Result<(), EdgeError> {
    if valid { Ok(()) } else { Err(invalid()) }
}

fn parse(case: &str, base: &Uri) -> Result<Case, EdgeError> {
    let (mixed, terminal) = match case {
        "mixed-completed" => (true, Terminal::Completed),
        "mixed-cutoff" => (true, Terminal::Cutoff),
        "mixed-failure" => (true, Terminal::Failure),
        "fresh-completed" => (false, Terminal::Completed),
        "fresh-cutoff" => (false, Terminal::Cutoff),
        "fresh-failure" => (false, Terminal::Failure),
        _ => return Err(invalid()),
    };
    let parts = base.path().split('/').collect::<Vec<_>>();
    ensure(
        crate::is_loopback_http(base)
            && base.query().is_none()
            && parts.len() == 5
            && parts[1] == "p5"
            && parts[2] == "batch"
            && !parts[3].is_empty()
            && parts[3].len() <= 64
            && parts[3]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && parts[4] == case,
    )?;
    Ok(Case { mixed, terminal })
}

fn snapshot(index: usize, result: &OutboundSlotResult) -> Result<Value, EdgeError> {
    let response = result.outcome.as_ref().map_err(|error| {
        log::warn!(target: "batch_recovery", "{}", json!({"event":"batch-recovery-slot-failure","index":index,"kind":error.kind(),"cause":if let EdgeError::GatewayTimeout {cause,..}=error {Some(format!("{cause:?}"))} else {None},"elapsed_ns":result.elapsed.as_nanos().to_string()}));
        invalid()
    })?;
    let bytes = response.body().as_bytes().ok_or_else(invalid)?;
    ensure(
        response.status().as_u16() == 200
            && bytes == [b'A' + u8::try_from(index).map_err(|_| invalid())?],
    )?;
    Ok(json!({"index":index,"bytes":bytes,"elapsed_ns":result.elapsed.as_nanos().to_string()}))
}

fn slots(collection: &Collection) -> &[OutboundCollectedSlot] {
    match collection {
        Ok(result) => result.slots(),
        Err(failure) => failure.slots(),
    }
}
fn slot_names(collection: &Collection) -> Vec<&'static str> {
    slots(collection)
        .iter()
        .map(|slot| match slot {
            OutboundCollectedSlot::AlreadyDelivered => "AlreadyDelivered",
            OutboundCollectedSlot::Resolved(_) => "Resolved",
            OutboundCollectedSlot::Unresolved => "Unresolved",
        })
        .collect()
}
fn merge(
    accumulator: OutboundBatchAccumulator,
    collection: Collection,
) -> Result<Collection, OutboundBatchMergeFailure> {
    match collection {
        Ok(result) => accumulator.merge_success(result).map(Ok),
        Err(failure) => accumulator.merge_failure(failure).map(Err),
    }
}
fn same_failure(error: &EdgeError, identity: &Arc<()>) -> bool {
    matches!(error, EdgeError::Internal { source, .. } if source.downcast_ref::<DriverFault>().is_some_and(|fault| Arc::ptr_eq(&fault.0, identity)))
}

struct ForeignRemainder {
    held: Vec<OutboundBatchItem>,
    collection: Collection,
    expected: Vec<Value>,
}

fn foreign_snapshot(index: usize, result: &OutboundSlotResult) -> Result<Value, EdgeError> {
    ensure(matches!(result.outcome, Err(EdgeError::BadRequest { .. })))?;
    Ok(
        json!({"index":index,"kind":"bad_request","elapsed_ns":result.elapsed.as_nanos().to_string()}),
    )
}

async fn foreign_batch(
    client: &HttpClient,
    time: &OutboundTime,
    base: &Uri,
    keep: &[usize],
) -> Result<(OutboundBatchAccumulator, ForeignRemainder), EdgeError> {
    // Public preflight errors provide three owned foreign outcomes without another HTTP exchange.
    let requests = (0..3)
        .map(|index| {
            OutboundRequest::get(format!("{base}/must-not-dispatch/{index}"))
                .map(OutboundRequest::stream_response)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut batch = client.start_batch_until(
        requests,
        time.clock().deadline_after(Duration::from_secs(1)),
    );
    let mut accumulator = batch.accumulator();
    let mut held = Vec::new();
    let mut expected = Vec::new();
    for index in 0..3 {
        let OutboundBatchCheck::Item(receipt) = batch.check()? else {
            return Err(invalid());
        };
        ensure(receipt.index() == index)?;
        expected.push(foreign_snapshot(index, receipt.result())?);
        if keep.contains(&index) {
            accumulator.insert(receipt).map_err(|_| invalid())?;
        } else {
            held.push(receipt);
        }
    }
    let collection = batch.collect().await;
    ensure(slot_names(&collection) == ["AlreadyDelivered"; 3])?;
    Ok((
        accumulator,
        ForeignRemainder {
            held,
            collection,
            expected,
        },
    ))
}

impl ForeignRemainder {
    fn restore(self, mut accumulator: OutboundBatchAccumulator) -> Result<(), EdgeError> {
        for receipt in self.held {
            accumulator.insert(receipt).map_err(|_| invalid())?;
        }
        let collection = merge(accumulator, self.collection)
            .map_err(|_| invalid())?
            .map_err(|_| invalid())?;
        ensure(collection.termination() == OutboundBatchTermination::Completed)?;
        let slots = collection.into_parts().0;
        ensure(slots.len() == self.expected.len())?;
        for (index, slot) in slots.into_iter().enumerate() {
            let OutboundCollectedSlot::Resolved(result) = slot else {
                return Err(invalid());
            };
            ensure(foreign_snapshot(index, &result)? == self.expected[index])?;
        }
        Ok(())
    }
}

async fn checked(
    mut batch: OutboundBatch,
    native: Arc<dyn OutboundHttpClient>,
) -> Result<(OutboundBatch, OutboundBatchItem), EdgeError> {
    let time = native.time();
    let limit = time.clock().deadline_after(Duration::from_secs(4));
    let entered = time.clock().now();
    let scope = OutboundScope::capture(native.progress());
    scope
        .run(
            time.clone(),
            entered,
            Box::pin(async move {
                loop {
                    if limit.is_expired_at(time.clock().now()) {
                        return Err(invalid());
                    }
                    match batch.check()? {
                        OutboundBatchCheck::Item(item) => return Ok((batch, item)),
                        OutboundBatchCheck::Finished(_) => return Err(invalid()),
                        OutboundBatchCheck::Pending => {}
                    }
                    time.wait_until(time.clock().deadline_after(Duration::from_millis(1)))
                        .await?;
                }
            }),
        )
        .await
}

async fn release(client: &HttpClient, base: &Uri, index: usize) -> Result<(), EdgeError> {
    let response = client
        .send(
            OutboundRequest::get(format!("{base}/release/{index}"))?
                .timeout(Duration::from_secs(4))
                .max_response_bytes(8),
        )
        .await?;
    ensure(response.status().as_u16() == 200 && response.into_bytes_bounded(8).await?.is_empty())
}

pub async fn run(
    native: Arc<dyn OutboundHttpClient>,
    base: Uri,
    name: &str,
) -> Result<Value, EdgeError> {
    let case = parse(name, &base)?;
    let time = native.time();
    let fault = Arc::new(Fault::default());
    let identity = Arc::new(());
    let native = Arc::new(FaultClient {
        native,
        fault: Arc::clone(&fault),
    });
    let client = HttpClient::new(native.clone());
    let requests = (0..3)
        .map(|index| {
            Ok(OutboundRequest::get(format!("{base}/slot/{index}"))?
                .timeout(Duration::from_secs(4))
                .max_response_bytes(8))
        })
        .collect::<Result<Vec<_>, EdgeError>>()?;
    let observation_cutoff = time.clock().deadline_after(Duration::from_secs(2));
    let mut batch = client.start_batch_until(requests, observation_cutoff);
    ensure(matches!(batch.check()?, OutboundBatchCheck::Pending))?;
    let mut accumulator = batch.accumulator();
    let mut observations = Vec::new();
    let mut receipt_zero = None;
    release(&client, &base, 0).await?;
    if case.mixed {
        let (returned, receipt) = checked(batch, native.clone()).await?;
        batch = returned;
        ensure(receipt.index() == 0)?;
        observations.push(json!({"via":"check","item":snapshot(0, receipt.result())?}));
        let (mut foreign, remainder) = foreign_batch(&client, &time, &base, &[2]).await?;
        let rejection = foreign.insert(receipt).err().ok_or_else(invalid)?;
        ensure(matches!(rejection.error(), EdgeError::Internal { .. }))?;
        receipt_zero = Some(rejection.into_parts().1);
        remainder.restore(foreign)?;
    }
    release(&client, &base, 1).await?;
    if case.mixed {
        let OutboundBatchNext::Item(receipt) = batch.next().await? else {
            return Err(invalid());
        };
        ensure(receipt.index() == 1)?;
        observations.push(json!({"via":"next","item":snapshot(1, receipt.result())?}));
        accumulator.insert(receipt).map_err(|_| invalid())?;
        ensure(matches!(batch.check()?, OutboundBatchCheck::Pending))?;
    }
    if case.terminal == Terminal::Completed {
        release(&client, &base, 2).await?;
    }
    if case.terminal == Terminal::Failure {
        *fault
            .armed
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some(EdgeError::internal(DriverFault(Arc::clone(&identity))));
    }
    // No await between arming the one-shot progress failure and polling this collection.
    let collection = batch.collect().await;
    ensure(slots(&collection).len() == 3)?;
    let before = slot_names(&collection);
    match (&collection, case.terminal) {
        (Err(failure), Terminal::Failure) => ensure(same_failure(failure.error(), &identity))?,
        (Ok(result), Terminal::Completed) => {
            ensure(result.termination() == OutboundBatchTermination::Completed)?
        }
        (Ok(result), Terminal::Cutoff) => {
            ensure(result.termination() == OutboundBatchTermination::Cutoff)?
        }
        _ => return Err(invalid()),
    }
    let collection = if let Some(receipt) = receipt_zero {
        ensure(before[0] == "AlreadyDelivered" && before[1] == "AlreadyDelivered")?;
        let rejected = merge(accumulator, collection).err().ok_or_else(invalid)?;
        ensure(matches!(rejected.error(), EdgeError::Internal { .. }))?;
        let (_, mut recovered, original_collection) = rejected.into_parts();
        let (foreign, remainder) = foreign_batch(&client, &time, &base, &[0, 1]).await?;
        let rejected = merge(foreign, original_collection)
            .err()
            .ok_or_else(invalid)?;
        ensure(matches!(rejected.error(), EdgeError::Internal { .. }))?;
        let (_, foreign, original_collection) = rejected.into_parts();
        remainder.restore(foreign)?;
        recovered.insert(receipt).map_err(|_| invalid())?;
        merge(recovered, original_collection).map_err(|_| invalid())?
    } else {
        ensure(!before.contains(&"AlreadyDelivered"))?;
        merge(accumulator, collection).map_err(|_| invalid())?
    };
    let after = slot_names(&collection);
    let (results, terminal, exact_error) = match collection {
        Ok(result) => {
            let (slots, terminal) = result.into_parts();
            (slots, format!("{terminal:?}"), false)
        }
        Err(failure) => {
            ensure(same_failure(failure.error(), &identity))?;
            let (_, slots) = failure.into_parts();
            (slots, "Failure".to_owned(), true)
        }
    };
    let outcomes = results
        .into_iter()
        .enumerate()
        .map(|(index, slot)| match slot {
            OutboundCollectedSlot::Resolved(result) => snapshot(index, &result).map(Some),
            OutboundCollectedSlot::Unresolved => Ok(None),
            OutboundCollectedSlot::AlreadyDelivered => Err(invalid()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if case.mixed || case.terminal != Terminal::Failure {
        ensure(outcomes[0].is_some() && outcomes[1].is_some())?;
    }
    ensure(if case.terminal == Terminal::Completed {
        outcomes[2].is_some()
    } else {
        outcomes[2].is_none()
    })?;
    for observation in &observations {
        let index = observation["item"]["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .ok_or_else(invalid)?;
        ensure(outcomes[index].as_ref() == Some(&observation["item"]))?;
    }
    Ok(
        json!({"case":name,"initial_pending":true,"observations":observations,"before_merge":before,"after_merge":after,"outcomes":outcomes,"terminal":terminal,"same_error":exact_error,"faults":fault.delivered.load(Ordering::SeqCst),"rejections":{"foreign_receipt":case.mixed,"missing_receipt":case.mixed,"foreign_collection":case.mixed,"foreign_outcomes_preserved":case.mixed},"qualification":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_cases_require_exact_controlled_targets() {
        for name in [
            "mixed-completed",
            "mixed-cutoff",
            "mixed-failure",
            "fresh-completed",
            "fresh-cutoff",
            "fresh-failure",
        ] {
            assert!(
                parse(
                    name,
                    &format!("http://127.0.0.1:1234/p5/batch/token/{name}")
                        .parse()
                        .unwrap()
                )
                .is_ok()
            );
            for base in [
                "https://example.com/p5/batch/token/mixed-completed",
                "http://127.0.0.1:1234/wrong",
                "http://127.0.0.1:1234/p5/batch/token/mixed-completed?secret=never-echo",
            ] {
                assert!(parse(name, &base.parse().unwrap()).is_err());
            }
        }
        assert!(
            parse(
                "unknown",
                &"http://127.0.0.1:1234/p5/batch/token/unknown"
                    .parse()
                    .unwrap()
            )
            .is_err()
        );
    }
}
