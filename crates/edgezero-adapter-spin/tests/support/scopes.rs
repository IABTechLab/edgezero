use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use edgezero_adapter_spin::outbound::test_support::dispatch_ingress_stream;
use edgezero_core::app::{App, Hooks};
use edgezero_core::context::RequestContext;
use edgezero_core::http::{Method, Response as CoreResponse, Uri};
use edgezero_core::outbound::{
    HttpClient, IndependentOutboundProgress, OutboundBatch, OutboundCollectedSlot,
    OutboundDispatchRequest, OutboundFuture, OutboundHead, OutboundHttpClient, OutboundPending,
    OutboundPreparedBatch, OutboundProgressRuntime, OutboundRequest, OutboundScopeOwner,
    OutboundSendMode, OutboundTime,
};
use edgezero_core::router::RouterService;
use edgezero_core::{Body, EdgeError, MonotonicClock, MonotonicInstant, action};
use futures_util::{FutureExt as _, stream};
use spin_sdk::http::body::stream as native_stream;
use spin_sdk::http::{FromRequest as _, IntoRequest as _, Request as SpinRequest};
use spin_sdk::wasip3::http::types::{ErrorCode, Fields, Response};
use spin_sdk::wasip3::http_compat::BodyWriter;
use spin_sdk::wasip3::wit_bindgen::FutureReader;

use super::{FIXTURE_RESPONSE, FixtureClient, SpinOutboundClient, native_time};

#[derive(Default)]
struct Retained {
    batch: Option<OutboundBatch>,
    begun: Option<OutboundPending>,
    leases: Vec<(BodyWriter, FutureReader<Result<(), ErrorCode>>)>,
    send: Option<OutboundFuture<edgezero_core::OutboundResponse>>,
    unpolled: Option<OutboundFuture<edgezero_core::OutboundResponse>>,
    upload: Option<Weak<[u8]>>,
}

struct DeferredClient(OutboundTime);

struct ScopeHooks;

#[expect(
    clippy::missing_trait_methods,
    reason = "fixture only supplies canonical routes"
)]
impl Hooks for ScopeHooks {
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .get("/retain", retain_handles)
            .build())
    }
}

struct AssemblyFailure;

#[expect(
    clippy::missing_trait_methods,
    reason = "fixture fails after retaining assembly work"
)]
impl Hooks for AssemblyFailure {
    fn configure(_app: &mut App) -> Result<(), EdgeError> {
        let mut retained = Retained::default();
        retain_unpolled(
            &mut retained,
            &HttpClient::with_client(SpinOutboundClient::new()),
        )?;
        let previous = RETAINED.with(|slot| slot.replace(Some(retained)));
        drop(previous);
        Err(EdgeError::service_unavailable(
            "scope fixture assembly failure",
        ))
    }

    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder().build())
    }
}

std::thread_local! {
    static RETAINED: RefCell<Option<Retained>> = const { RefCell::new(None) };
    static PREPARED: RefCell<Option<OutboundDispatchRequest>> = const { RefCell::new(None) };
}

impl OutboundHttpClient for DeferredClient {
    fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
        Err(failure("deferred fixture only captures sends"))
    }

    fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
        let previous = PREPARED.with(|slot| slot.replace(Some(request)));
        drop(previous);
        Box::pin(async { Err(failure("prepared request captured")) })
    }

    fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
        Rc::new(IndependentOutboundProgress)
    }

    fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
        let (context, _) = batch.into_slots();
        OutboundBatch::from_driver(
            context,
            stream::iter([edgezero_core::OutboundBatchDriverEvent::Failed(failure(
                "deferred fixture only captures sends",
            ))]),
            edgezero_core::OutboundExchangeOwner::new(|| {}),
        )
    }

    fn time(&self) -> OutboundTime {
        self.0.clone()
    }

    fn validate_supported_options(
        &self,
        _request: &OutboundRequest,
        _mode: OutboundSendMode,
    ) -> Result<(), EdgeError> {
        Ok(())
    }
}

fn failure(message: &'static str) -> EdgeError {
    EdgeError::internal(anyhow::anyhow!(message))
}

fn install_response(retained: &mut Retained) {
    let (writer, contents, trailers) = BodyWriter::new();
    let (response, done) = Response::new(Fields::new(), Some(contents), trailers);
    retained.leases.push((writer, done));
    let previous = FIXTURE_RESPONSE.with(|slot| slot.replace(Some(response)));
    drop(previous);
}

fn native_request() -> Result<SpinRequest, EdgeError> {
    let (sender, body) = native_stream::<Bytes>();
    drop(sender);
    let outgoing = SpinRequest::builder()
        .uri("http://fixture.invalid/retain")
        .body(body)
        .map_err(EdgeError::internal)?;
    let request = outgoing.into_request().map_err(EdgeError::internal)?;
    SpinRequest::from_request(request).map_err(EdgeError::internal)
}

fn retain_unpolled(retained: &mut Retained, client: &HttpClient) -> Result<(), EdgeError> {
    let upload = Arc::<[u8]>::from(&b"unpolled owned upload"[..]);
    retained.upload = Some(Arc::downgrade(&upload));
    retained.unpolled = Some(client.send(
        OutboundRequest::post("https://fixture.invalid/unpolled")?.body(Bytes::from_owner(upload)),
    ));
    Ok(())
}

#[action]
async fn retain_handles(RequestContext(ctx): RequestContext) -> Result<CoreResponse, EdgeError> {
    let mut retained = Retained::default();
    let native = ctx
        .http_client()
        .ok_or_else(|| failure("canonical client missing"))?;
    retain_unpolled(&mut retained, &native)?;
    let time = native_time(ctx.monotonic_clock());
    let client = HttpClient::with_client(FixtureClient(time.clone()));

    install_response(&mut retained);
    let mut send = client.send(OutboundRequest::get("https://fixture.invalid/send")?);
    if send.as_mut().now_or_never().is_some() {
        return Err(failure("SDK send must wait for its retained body reader"));
    }
    retained.send = Some(send);

    install_response(&mut retained);
    retained.begun = Some(
        client
            .begin(OutboundRequest::get("https://fixture.invalid/begin")?)
            .await?,
    );

    install_response(&mut retained);
    retained.batch = Some(client.start_batch_until(
        vec![OutboundRequest::get("https://fixture.invalid/batch")?],
        time.clock().deadline_after(Duration::from_secs(1)),
    ));
    let previous = RETAINED.with(|slot| slot.replace(Some(retained)));
    drop(previous);
    Ok(CoreResponse::new(Body::empty()))
}

pub(crate) async fn canonical_callback_releases_retained_handles() -> Result<(), EdgeError> {
    let app = App::build::<ScopeHooks>(edgezero_adapter_spin::SPIN_PLATFORM)?;
    let envelope = dispatch_ingress_stream(
        &app,
        Method::GET,
        Uri::from_static("/retain"),
        stream::empty::<Result<Bytes, io::Error>>(),
    )
    .await
    .map_err(EdgeError::internal)?;
    assert_retained_handles_closed().await?;
    drop(envelope);
    Ok(())
}

pub(crate) async fn canonical_entrypoints_release_retained_handles() -> Result<(), EdgeError> {
    let app = App::build::<ScopeHooks>(edgezero_adapter_spin::SPIN_PLATFORM)?;
    let response = edgezero_adapter_spin::dispatch_app::<ScopeHooks>(&app, native_request()?)
        .await
        .map_err(EdgeError::internal)?;
    assert_retained_handles_closed().await?;
    drop(response);

    let assembly_response = edgezero_adapter_spin::run_app::<AssemblyFailure>(native_request()?)
        .await
        .map_err(EdgeError::internal)?;
    if assembly_response.get_status_code() != 503 {
        return Err(failure("assembly scope fixture lost its typed response"));
    }
    let mut retained = take_retained()?;
    assert_upload_released(&retained)?;
    let future = retained
        .unpolled
        .take()
        .ok_or_else(|| failure("assembly upload missing"))?;
    if !matches!(future.await, Err(EdgeError::Internal { .. })) {
        return Err(failure(
            "assembly callback did not close its retained upload",
        ));
    }
    drop(assembly_response);
    Ok(())
}

fn take_retained() -> Result<Retained, EdgeError> {
    RETAINED
        .with(|slot| slot.borrow_mut().take())
        .ok_or_else(|| failure("canonical handler did not retain its outbound handles"))
}

fn assert_upload_released(retained: &Retained) -> Result<(), EdgeError> {
    if retained.upload.as_ref().and_then(Weak::upgrade).is_some() {
        return Err(failure("callback return retained an unpolled Send upload"));
    }
    Ok(())
}

async fn assert_retained_handles_closed() -> Result<(), EdgeError> {
    let mut retained = take_retained()?;
    assert_upload_released(&retained)?;
    for (writer, _completion) in &mut retained.leases {
        if writer.stream_writer.write_all(vec![1]).now_or_never() != Some(vec![1]) {
            return Err(failure("callback return retained an SDK response reader"));
        }
    }
    for future in [retained.send.take(), retained.unpolled.take()]
        .into_iter()
        .flatten()
    {
        if !matches!(future.await, Err(EdgeError::Internal { .. })) {
            return Err(failure("closed Send did not retain its scope failure"));
        }
    }
    let begun = retained
        .begun
        .take()
        .ok_or_else(|| failure("begun fixture missing"))?;
    if !matches!(begun.collect().await, Err(EdgeError::Internal { .. })) {
        return Err(failure("closed Begin did not retain its scope failure"));
    }
    let batch = retained
        .batch
        .take()
        .ok_or_else(|| failure("batch fixture missing"))?;
    let batch_failure = batch
        .collect()
        .await
        .err()
        .ok_or_else(|| failure("closed batch must fail"))?;
    if !matches!(batch_failure.error(), EdgeError::Internal { .. })
        || !matches!(batch_failure.slots(), [OutboundCollectedSlot::Unresolved])
    {
        return Err(failure("closed batch lost its scope failure or slot state"));
    }
    drop(retained);
    Ok(())
}

pub(crate) async fn foreign_poll_cannot_reparent_prepared_dispatch() -> Result<(), EdgeError> {
    let start = MonotonicInstant::now();
    let observed = Arc::new(Mutex::new(start));
    let clock = Arc::clone(&observed);
    let time = native_time(MonotonicClock::new(move || {
        clock.lock().map_or(start, |current| *current)
    }));
    let owner = OutboundScopeOwner::callback();
    let client = HttpClient::with_client(DeferredClient(time));
    let request =
        OutboundRequest::get("https://fixture.invalid/deferred")?.timeout(Duration::from_millis(1));
    let captured = owner.with(|| client.send(request));
    if !matches!(captured.await, Err(EdgeError::Internal { .. })) {
        return Err(failure("capture-only transport must fail"));
    }
    let prepared = PREPARED
        .with(|slot| slot.borrow_mut().take())
        .ok_or_else(|| failure("prepared fixture missing"))?;
    let deferred = SpinOutboundClient::new().dispatch_head(prepared);
    drop(owner);
    *observed
        .lock()
        .map_err(|_poisoned| failure("fixture clock poisoned"))? = start
        .checked_add(Duration::from_secs(1))
        .ok_or_else(|| failure("fixture clock overflow"))?;
    let foreign = OutboundScopeOwner::callback();
    let outcome = foreign.run(deferred).await;
    if !matches!(outcome, Err(EdgeError::Internal { .. })) {
        return Err(failure(
            "foreign poll reparented a closed prepared dispatch",
        ));
    }
    foreign.scope().check()?;
    Ok(())
}
