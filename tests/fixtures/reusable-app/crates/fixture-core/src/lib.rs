pub mod assembly;
#[cfg(test)]
mod outbound_test_support;
#[cfg(feature = "qualification")]
pub mod qualification;

use edgezero_core::app::{App, Hooks, StoreMetadata, StoresMetadata};
use edgezero_core::http::{Method, Response, response_builder};
use edgezero_core::key_value_store::KvError;
use edgezero_core::outbound::{HttpClient, OutboundRequest};
use edgezero_core::router::RouterService;
use edgezero_core::{action, body::Body, context::RequestContext, error::EdgeError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

static INSTANCE: OnceLock<String> = OnceLock::new();
static BUILDS: AtomicUsize = AtomicUsize::new(0);
static ORDINAL: AtomicUsize = AtomicUsize::new(0);
static INFLIGHT: AtomicUsize = AtomicUsize::new(0);
const MAX_BODY_BYTES: usize = 1024 * 1024;
static TELEMETRY: OnceLock<fn(&str)> = OnceLock::new();

struct FixtureLogger;
impl log::Log for FixtureLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            emit(
                serde_json::json!({"event":"fixture_log","instance":instance_id(),
                "level":record.level().to_string(),"target":record.target(),"message":record.args().to_string()}),
            );
        }
    }
    fn flush(&self) {}
}

pub fn init_fixture_logging(sink: fn(&str)) -> Result<(), EdgeError> {
    static LOGGER: FixtureLogger = FixtureLogger;
    static INSTALLED: OnceLock<Result<(), log::SetLoggerError>> = OnceLock::new();
    TELEMETRY.get_or_init(|| sink);
    INSTALLED
        .get_or_init(|| log::set_logger(&LOGGER))
        .as_ref()
        .map_err(|_error| {
            EdgeError::internal(std::io::Error::other("fixture logger installation failed"))
        })?;
    log::set_max_level(log::LevelFilter::Warn);
    Ok(())
}

fn emit(event: serde_json::Value) {
    if let Some(sink) = TELEMETRY.get() {
        sink(&event.to_string());
    }
}

pub fn instance_id() -> Option<&'static str> {
    INSTANCE.get().map(String::as_str)
}

pub fn retained_app<A: Hooks>(
    cell: &OnceLock<App>,
    platform: edgezero_core::PlatformMetadata,
) -> Result<&App, EdgeError> {
    if let Some(app) = cell.get() {
        return Ok(app);
    }
    let app = App::build::<A>(platform)?;
    // Keep an already-installed winner; configuration errors never enter the cell.
    let _ = cell.set(app);
    cell.get()
        .ok_or_else(|| EdgeError::internal(std::io::Error::other("application retention failed")))
}

/// Carry a fixture-owned lifetime observer through request and response conversion.
#[derive(Clone)]
pub struct ResponseLifetime(
    pub Arc<dyn Send + Sync>,
    pub Arc<std::sync::atomic::AtomicBool>,
);

/// Request-specific finalization data produced inside the router.
#[derive(Clone)]
pub struct Finalize(pub String);

#[derive(Clone)]
pub struct Observation {
    pub instance: String,
    pub ordinal: usize,
    pub correlation: String,
    pub native_id: bool,
}

pub fn observe(candidate: &str, correlation: Option<&str>) -> Observation {
    Observation {
        instance: INSTANCE.get_or_init(|| candidate.to_owned()).clone(),
        ordinal: ORDINAL.fetch_add(1, Ordering::SeqCst) + 1,
        correlation: correlation.unwrap_or(candidate).to_owned(),
        native_id: correlation.is_some(),
    }
}

pub struct FixtureApp;
pub struct OtherApp;

impl Hooks for FixtureApp {
    fn stores() -> StoresMetadata {
        StoresMetadata {
            config: Some(StoreMetadata {
                default: "fixture_config",
                ids: &["fixture_config"],
            }),
            kv: Some(StoreMetadata {
                default: "fixture_kv",
                ids: &["fixture_kv"],
            }),
            secrets: Some(StoreMetadata {
                default: "fixture_secrets",
                ids: &["fixture_secrets"],
            }),
        }
    }
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        BUILDS.fetch_add(1, Ordering::SeqCst);
        app.set_name("fixture");
        Ok(())
    }
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder()
            .middleware(ObserveRequest)
            .with_state(Arc::new(AtomicUsize::new(0)))
            .get("/probe/{id}", probe)
            .post("/probe/{id}", probe)
            .get("/bindings", bindings)
            .get("/declared-kv/free", declared_kv_free)
            .get("/declared-kv/{store}/{operation}", declared_kv)
            .get("/cookies", cookies)
            .get("/rendered-error", rendered_error)
            .get("/stream", backend)
            .get("/stream-error", backend)
            .get("/overlap/{id}", overlap)
            .get("/origin/{id}", backend)
            .build())
    }
}
/// Exercises an unused missing declaration beside a healthy default binding.
pub struct DeclaredKvApp;
impl Hooks for DeclaredKvApp {
    fn configure(app: &mut App) -> Result<(), EdgeError> {
        FixtureApp::configure(app)?;
        app.set_ingress_admission_policy(|head| {
            let token = head.headers().get("x-request-token").and_then(|value|value.to_str().ok()).unwrap_or("missing").to_owned();
            edgezero_core::AdmissionDecision::Admit {
                completion: edgezero_core::ResponseEgressCompletion::new(move |report| {
                    emit(serde_json::json!({"event":"egress_terminal","instance":instance_id(),"token":token,
                        "outcome":format!("{:?}",report.outcome),"body_kind":format!("{:?}",report.body_kind),
                        "bytes_written":report.bytes_written,"fallback":report.fallback_disposition.map(|value|format!("{value:?}"))}));
                }),
                grant: edgezero_core::IngressGrant::empty(),
                read_deadline: head.read_deadline_after(edgezero_core::DEFAULT_INBOUND_READ_BUDGET),
            }
        });
        Ok(())
    }
    fn owns_logging() -> bool {
        true
    }
    fn routes() -> Result<RouterService, EdgeError> {
        FixtureApp::routes()
    }
    fn stores() -> StoresMetadata {
        let mut stores = FixtureApp::stores();
        stores.kv = Some(StoreMetadata {
            default: "fixture_kv",
            ids: &["fixture_kv", "missing_fixture_kv"],
        });
        stores
    }
}
impl Hooks for OtherApp {
    fn routes() -> Result<RouterService, EdgeError> {
        Ok(RouterService::builder().get("/other", other).build())
    }
}

#[action]
async fn other(_ctx: RequestContext) -> Result<String, EdgeError> {
    Ok("other-app".into())
}

async fn record(ctx: &RequestContext) -> Result<serde_json::Value, EdgeError> {
    let body = ctx.body_bytes(MAX_BODY_BYTES).await?;
    let token = ctx
        .headers()
        .get("x-request-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("missing");
    let observation = ctx
        .extensions()
        .get::<Observation>()
        .cloned()
        .ok_or_else(|| {
            EdgeError::internal(std::io::Error::other("observation middleware absent"))
        })?;
    let shared = ctx
        .extensions()
        .get::<Arc<AtomicUsize>>()
        .unwrap()
        .fetch_add(1, Ordering::SeqCst)
        + 1;
    Ok(serde_json::json!({
        "instance": observation.instance, "ordinal": observation.ordinal,
        "correlation": observation.correlation, "native_id": observation.native_id,
        "builds": BUILDS.load(Ordering::SeqCst),
        "path": ctx.path_params().get("id"), "token": token,
        "mutated": ctx.headers().get("x-fixture-mutated").is_some(),
        "body": String::from_utf8_lossy(&body),
        "shared": shared, "inflight": INFLIGHT.load(Ordering::SeqCst),
    }))
}

#[action]
async fn probe(ctx: RequestContext) -> Result<String, EdgeError> {
    Ok(record(&ctx).await?.to_string())
}

#[action]
async fn cookies(_ctx: RequestContext) -> Result<Response, EdgeError> {
    let mut response = response_builder().body(Body::from("cookies")).unwrap();
    response
        .headers_mut()
        .append("set-cookie", "first=1; Path=/".parse().unwrap());
    response
        .headers_mut()
        .append("set-cookie", "second=2; Path=/".parse().unwrap());
    Ok(response)
}

#[action]
async fn rendered_error(_ctx: RequestContext) -> Result<Response, EdgeError> {
    Err(EdgeError::bad_request("fixture error"))
}

/// Fixtures accept only harness-controlled local origins, never arbitrary Internet targets.
pub fn is_loopback_http(uri: &edgezero_core::http::Uri) -> bool {
    uri.scheme_str() == Some("http") && matches!(uri.host(), Some("127.0.0.1" | "localhost"))
}

async fn fetch_backend(ctx: &RequestContext) -> Result<Response, EdgeError> {
    let uri = ctx
        .headers()
        .get("x-fixture-backend")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| EdgeError::bad_request("missing fixture backend"))?;
    let uri: edgezero_core::http::Uri = uri
        .parse()
        .map_err(|_| EdgeError::bad_request("invalid backend URI"))?;
    if !is_loopback_http(&uri) {
        return Err(EdgeError::bad_request("backend must be loopback HTTP"));
    }
    let mut request = OutboundRequest::new(Method::GET, uri)?;
    if matches!(ctx.uri().path(), "/stream" | "/stream-error") {
        request = request.stream_response();
    }
    ctx.http_client()
        .ok_or_else(|| EdgeError::internal(std::io::Error::other("missing HTTP client")))?
        .send(request)
        .await?
        .into_response()
}

#[action]
async fn backend(ctx: RequestContext) -> Result<Response, EdgeError> {
    fetch_backend(&ctx).await
}

struct InFlight;
impl Drop for InFlight {
    fn drop(&mut self) {
        INFLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}
#[action]
async fn overlap(ctx: RequestContext) -> Result<String, EdgeError> {
    INFLIGHT.fetch_add(1, Ordering::SeqCst);
    let _guard = InFlight;
    let before = record(&ctx).await?;
    let _response = fetch_backend(&ctx).await?;
    let mut after = record(&ctx).await?;
    after["before"] = before;
    Ok(after.to_string())
}

/// A missing server-side binding is a deployment fault, not a client error.
fn missing_binding(message: &'static str) -> EdgeError {
    EdgeError::internal(std::io::Error::other(message))
}

#[action]
async fn bindings(ctx: RequestContext) -> Result<String, EdgeError> {
    let config = ctx
        .config_store("fixture_config")
        .ok_or_else(|| missing_binding("config handle absent"))?;
    let marker = config.get("marker").await.map_err(EdgeError::internal)?;
    let default_marker = ctx
        .config_store_default()
        .ok_or_else(|| missing_binding("default config handle absent"))?
        .get("marker")
        .await
        .map_err(EdgeError::internal)?;
    let kv = ctx
        .kv_store("fixture_kv")
        .ok_or_else(|| missing_binding("KV handle absent"))?;
    kv.put("fixture-marker", &"local-fixture")
        .await
        .map_err(EdgeError::internal)?;
    let kv_marker: Option<String> = ctx
        .kv_store_default()
        .ok_or_else(|| missing_binding("default kv handle absent"))?
        .get("fixture-marker")
        .await
        .map_err(EdgeError::internal)?;
    let secret = ctx
        .secret_store("fixture_secrets")
        .ok_or_else(|| missing_binding("secret handle absent"))?;
    let named_secret = secret
        .get_bytes("fixture_marker")
        .await
        .map_err(EdgeError::internal)?;
    let default_secret = ctx
        .secret_store_default()
        .ok_or_else(|| missing_binding("default secret handle absent"))?
        .get_bytes("fixture_marker")
        .await
        .map_err(EdgeError::internal)?;
    Ok(serde_json::json!({
        "config": marker.as_deref() == Some("local-fixture") && marker == default_marker,
        "kv": kv_marker.as_deref() == Some("local-fixture"),
        "secrets": named_secret.is_some() && named_secret == default_secret,
        "unknown": ctx.kv_store("unknown").is_some(),
    })
    .to_string())
}

#[action]
async fn declared_kv_free(ctx: RequestContext) -> Result<String, EdgeError> {
    let mut report = record(&ctx).await?;
    report["declared_kv"] = serde_json::json!(ctx.kv_store("missing_fixture_kv").is_some());
    report["default_kv"] = serde_json::json!(ctx.kv_store_default().is_some());
    report["unknown_kv"] = serde_json::json!(ctx.kv_store("undeclared").is_some());
    Ok(report.to_string())
}

#[action]
async fn declared_kv(ctx: RequestContext) -> Result<String, EdgeError> {
    let mut report = record(&ctx).await?;
    let (store_label, name) = match ctx.path_params().get("store") {
        Some("missing") => ("missing", "missing_fixture_kv"),
        Some("linked") => ("linked", "fixture_kv"),
        _ => return Err(EdgeError::bad_request("unknown controlled KV store")),
    };
    let operation = match ctx.path_params().get("operation") {
        Some(operation @ ("read" | "write" | "delete" | "list")) => operation,
        _ => return Err(EdgeError::bad_request("unknown controlled KV operation")),
    };
    let receipt = |event: &str, outcome: Option<&str>, reason: Option<String>| {
        let event = serde_json::json!({"event":event,"token":report["token"],"instance":report["instance"],
            "store":store_label,"operation":operation,"outcome":outcome,"reason":reason});
        if let Some(sink) = TELEMETRY.get() {
            sink(&event.to_string());
        } else {
            // The ordinary Fastly fixture keeps its named logger; receipts use stdout.
            println!("{event}");
        }
    };
    receipt("declared_kv_enter", None, None);
    let store = ctx
        .kv_store(name)
        .ok_or_else(|| missing_binding("declared KV handle absent"))?;
    let outcome = match operation {
        "read" => store
            .get::<String>("f8-key")
            .await
            .map(|value| serde_json::json!(value)),
        "write" => store
            .put("f8-key", &"f8-value")
            .await
            .map(|()| serde_json::json!("written")),
        "delete" => store
            .delete("f8-key")
            .await
            .map(|()| serde_json::json!("deleted")),
        "list" => store
            .list_keys_page("f8-", None, 100)
            .await
            .map(|page| serde_json::json!(page.keys)),
        _ => return Err(EdgeError::bad_request("unknown controlled KV operation")),
    };
    let (status, reason) = match &outcome {
        Ok(_) => ("success", None),
        Err(KvError::OpenFailed { reason }) => ("open-failed", Some(format!("{reason:?}"))),
        Err(_) => ("operation-failed", None),
    };
    receipt("declared_kv_result", Some(status), reason);
    report["kv_outcome"] = outcome?;
    Ok(report.to_string())
}

struct ObserveRequest;
#[async_trait::async_trait(?Send)]
impl edgezero_core::middleware::Middleware for ObserveRequest {
    async fn handle(
        &self,
        mut ctx: RequestContext,
        next: edgezero_core::middleware::Next<'_>,
    ) -> Result<Response, EdgeError> {
        if let Some(captured) = ctx.extensions().get::<Arc<Mutex<Option<HttpClient>>>>() {
            *captured.lock().expect("request client capture lock") = ctx.http_client();
        }
        if ctx.extensions().get::<Observation>().is_none() {
            let candidate = ctx
                .headers()
                .get("x-request-token")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing");
            let observation = observe(candidate, None);
            emit(
                serde_json::json!({"event":"request_start","instance":observation.instance,
                "ordinal":observation.ordinal,"token":candidate,"correlation":observation.correlation,"native_id":observation.native_id}),
            );
            ctx.extensions_mut().insert(observation);
        }
        let lifetime = ctx.extensions().get::<ResponseLifetime>().cloned();
        let finalization = Finalize(
            ctx.headers()
                .get("x-request-token")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned(),
        );
        let mut response = next.run(ctx).await?;
        response.extensions_mut().insert(finalization);
        if let Some(lifetime) = lifetime {
            lifetime.1.store(true, Ordering::SeqCst);
            response.extensions_mut().insert(lifetime);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgezero_core::app::Hooks;
    use edgezero_core::body::Body;
    use edgezero_core::http::request_builder;
    use edgezero_core::outbound::{
        HttpClient, OutboundBatch, OutboundBatchDriverEvent, OutboundBatchTermination,
        OutboundCollectedSlot, OutboundDispatchRequest, OutboundExchangeOwner, OutboundFuture,
        OutboundHead, OutboundHttpClient, OutboundPending, OutboundPreparedBatch, OutboundSendMode,
        OutboundTime,
    };
    use edgezero_core::{
        BodyPayload, BudgetSource, Deadline, IndependentOutboundProgress, OutboundProgressRuntime,
        OutboundScopeOwner,
    };
    use futures::executor::block_on;
    use std::rc::Rc;
    use std::sync::Mutex;
    use std::task::Poll;
    use std::time::Duration;

    struct NamedClient {
        name: &'static str,
        time: OutboundTime,
        calls: Arc<AtomicUsize>,
    }

    impl NamedClient {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                time: crate::outbound_test_support::TestTime::default().pair(),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl OutboundHttpClient for NamedClient {
        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn time(&self) -> OutboundTime {
            self.time.clone()
        }

        fn validate_supported_options(
            &self,
            _request: &OutboundRequest,
            _mode: OutboundSendMode,
        ) -> Result<(), EdgeError> {
            Ok(())
        }

        fn dispatch_head(&self, _request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::future::ready(Err(EdgeError::bad_request(
                self.name,
            ))))
        }

        fn begin(&self, _request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(EdgeError::bad_request(self.name))
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let (context, requests) = batch.into_slots();
            if requests.is_empty()
                && context
                    .observation_cutoff()
                    .is_expired_at(context.clock().now())
            {
                return OutboundBatch::cutoff(context);
            }
            let events = requests
                .into_iter()
                .map(|(index, _request)| {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: self.time.clock().now(),
                        outcome: Err(EdgeError::bad_request(self.name)),
                    }
                })
                .collect::<Vec<_>>();
            OutboundBatch::from_driver(
                context,
                futures::stream::iter(events),
                OutboundExchangeOwner::new(|| {}),
            )
        }
    }

    #[test]
    fn early_body_batch_attachment_survives_eof_and_closes_on_response_drop() {
        for abandon_before_handoff in [false, true] {
            let scripted = NamedClient::new("scripted terminal failure");
            let calls = Arc::clone(&scripted.calls);
            let observation_cutoff = scripted.time.clock().deadline_after(Duration::from_secs(1));
            let client = HttpClient::with_client(scripted);
            let polls = Arc::new(AtomicUsize::new(0));
            let (response, scope) = {
                let owner = OutboundScopeOwner::response_body();
                let scope = owner.scope();
                let batch = owner.with(|| {
                    client.start_batch_until(
                        vec![OutboundRequest::get("https://example.com/early").unwrap()],
                        observation_cutoff,
                    )
                });
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "start before handler return"
                );
                let observed = Arc::clone(&polls);
                let source = futures::stream::once(async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    let results = batch
                        .collect()
                        .await
                        .map_err(|failure| failure.into_parts().0)?;
                    assert_eq!(results.termination(), OutboundBatchTermination::Completed);
                    let report = serde_json::json!({"slots":results.slots().len()});
                    Ok(bytes::Bytes::from(report.to_string()))
                });
                let response = owner
                    .attach(Response::new(Body::from_stream(source)))
                    .expect("attach the exact early body owner");
                (response, scope)
            };
            assert_eq!(
                polls.load(Ordering::SeqCst),
                0,
                "handler has returned without polling"
            );
            scope
                .check()
                .expect("response owns the early batch lifetime");
            if abandon_before_handoff {
                drop(response);
                assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                continue;
            }
            let (payload, body_owner) = response.into_body().into_parts();
            assert!(body_owner.is_some(), "handoff carries the early owner");
            let BodyPayload::Stream(source) = payload else {
                panic!("batch report is streamed");
            };
            let bytes = block_on(Body::from_stream(source).into_bytes_bounded(4096)).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                serde_json::json!({"slots":1})
            );
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            scope
                .check()
                .expect("EOF alone is not transmission completion");
            drop(body_owner);
            assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
        }
    }

    #[test]
    fn attached_body_metadata_is_rejected_before_outbound_dispatch() {
        let scripted = NamedClient::new("must not dispatch attached request metadata");
        let calls = Arc::clone(&scripted.calls);
        let client = HttpClient::with_client(scripted);
        let owner = OutboundScopeOwner::response_body();
        let scope = owner.scope();
        let response = owner
            .attach(Response::new(Body::empty()))
            .expect("response attachment");
        let request = OutboundRequest::post("https://example.com/invalid-attachment")
            .unwrap()
            .body(response.into_body());
        let error =
            block_on(client.send(request)).expect_err("response metadata is not a request payload");
        assert_eq!(error.status(), 400);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(scope.check(), Err(EdgeError::Internal { .. })));
    }

    #[test]
    fn supplied_client_scope_reactivates_after_yield_and_releases_unpolled_send() {
        let scripted = NamedClient::new("manual caller owns this exchange");
        let calls = Arc::clone(&scripted.calls);
        let client = HttpClient::with_client(scripted);
        let error =
            block_on(client.send(OutboundRequest::get("https://example.com/manual").unwrap()))
                .expect_err("manual caller must still reach the supplied client");
        assert_eq!(error.message(), "manual caller owns this exchange");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let lifetime = Arc::new(());
        let released = Arc::downgrade(&lifetime);
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |_cx| {
            let _held = &lifetime;
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        });
        let request = OutboundRequest::post("https://example.com/unpolled")
            .unwrap()
            .body(Body::from_stream(source));
        let callback = OutboundScopeOwner::callback();
        let mut first_poll = true;
        let mut retained = None;
        block_on(callback.run(async {
            futures::future::poll_fn(|cx| {
                if first_poll {
                    first_poll = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            retained = Some(client.send(request));
        }));
        let retained = retained.expect("send created after the second outer poll");
        assert!(!first_poll);
        assert!(released.upgrade().is_some());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(callback);
        assert!(
            released.upgrade().is_none(),
            "scope exit releases an unpolled root"
        );
        let error = block_on(retained).expect_err("closed invocation cannot dispatch later");
        assert!(matches!(error, EdgeError::Internal { .. }));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn named_batch_exact_cutoff_preserves_preflight_without_io() {
        for sibling in ["cutoff", "invalid", "own-timeout"] {
            let time = crate::outbound_test_support::TestTime::default();
            let own_deadline = Deadline::at_instant(time.clock().now());
            time.advance_to(
                own_deadline
                    .instant()
                    .checked_add(Duration::from_millis(1))
                    .expect("entry instant"),
            );
            let entered_at = time.clock().now();
            let calls = Arc::new(AtomicUsize::new(0));
            let client = HttpClient::with_client(NamedClient {
                name: "must not dispatch",
                time: time.pair(),
                calls: Arc::clone(&calls),
            });
            let first = OutboundRequest::get("https://example.com/first").unwrap();
            let second = OutboundRequest::get("https://example.com/second").unwrap();
            let second = match sibling {
                "invalid" => second.stream_response(),
                "own-timeout" => second.deadline(own_deadline),
                _ => second,
            };
            let batch =
                client.start_batch_until(vec![first, second], Deadline::at_instant(entered_at));
            let results = block_on(batch.collect()).expect("exact cutoff is not driver EOF");
            let (collected, termination) = results.into_parts();
            assert_eq!(termination, OutboundBatchTermination::Cutoff);
            assert_eq!(collected.len(), 2);
            assert!(matches!(collected[0], OutboundCollectedSlot::Unresolved));
            let last = collected.into_iter().nth(1).expect("second slot");
            if sibling == "cutoff" {
                assert!(matches!(last, OutboundCollectedSlot::Unresolved));
            } else {
                let OutboundCollectedSlot::Resolved(rejected) = last else {
                    panic!("preflight outcome must be retained");
                };
                assert_eq!(rejected.elapsed, Duration::ZERO);
                let error = rejected.outcome.expect_err("preflight failure");
                if sibling == "invalid" {
                    assert_eq!(error.status(), 400);
                } else {
                    assert!(matches!(
                        error,
                        EdgeError::GatewayTimeout {
                            cause: BudgetSource::RequestDeadline,
                            ..
                        }
                    ));
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(time.clock().now(), entered_at);
        }
    }

    #[test]
    fn capture_keeps_effective_client_for_success_and_rendered_handler_errors() {
        for state_client in [false, true] {
            let mut router = RouterService::builder()
                .middleware(ObserveRequest)
                .get("/cookies", cookies)
                .get("/error", rendered_error);
            if state_client {
                router =
                    router.with_state(HttpClient::with_client(NamedClient::new("state client")));
            }
            let app = App::new(router.build());
            let mut captures = Vec::new();
            for (path, expected_status) in [("/cookies", 200), ("/error", 400)] {
                let captured: Arc<Mutex<Option<HttpClient>>> = Arc::new(Mutex::new(None));
                let mut request = request_builder().uri(path).body(Body::empty()).unwrap();
                request
                    .extensions_mut()
                    .insert(HttpClient::with_client(NamedClient::new("request client")));
                request.extensions_mut().insert(Arc::clone(&captured));
                let response = block_on(app.router().oneshot(request)).unwrap();
                assert_eq!(response.status().as_u16(), expected_status);
                captures.push(captured);
            }
            for captured in captures {
                let client = captured
                    .lock()
                    .unwrap()
                    .take()
                    .expect("capture before handler");
                let error =
                    block_on(client.send(OutboundRequest::get("https://example.com").unwrap()))
                        .unwrap_err();
                assert_eq!(
                    error.message(),
                    if state_client {
                        "state client"
                    } else {
                        "request client"
                    }
                );
                assert!(
                    captured.lock().unwrap().take().is_none(),
                    "one request-owned transfer"
                );
            }
        }
    }

    #[test]
    fn capture_does_not_create_a_client_for_absent_or_unrouted_requests() {
        let app = App::build::<FixtureApp>(edgezero_core::PlatformMetadata::default()).unwrap();
        for (path, status) in [("/cookies", 200), ("/unknown", 404)] {
            let captured: Arc<Mutex<Option<HttpClient>>> = Arc::new(Mutex::new(None));
            let mut request = request_builder().uri(path).body(Body::empty()).unwrap();
            request.extensions_mut().insert(Arc::clone(&captured));
            if status == 404 {
                request
                    .extensions_mut()
                    .insert(HttpClient::with_client(NamedClient::new("not routed")));
            }
            let response = block_on(app.router().oneshot(request)).unwrap();
            assert_eq!(response.status().as_u16(), status);
            assert!(captured.lock().unwrap().take().is_none());
        }
    }

    #[test]
    fn retained_app_retries_failed_configuration_and_reuses_success() {
        static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
        struct RetryApp;
        impl Hooks for RetryApp {
            fn configure(_app: &mut App) -> Result<(), EdgeError> {
                if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(EdgeError::service_unavailable(
                        "injected configuration failure",
                    ))
                } else {
                    Ok(())
                }
            }

            fn routes() -> Result<RouterService, EdgeError> {
                Ok(RouterService::builder().build())
            }
        }

        let cell = OnceLock::new();
        let platform = edgezero_core::PlatformMetadata::default();
        assert!(retained_app::<RetryApp>(&cell, platform).is_err());
        assert!(cell.get().is_none());
        let first = retained_app::<RetryApp>(&cell, platform).unwrap();
        let second = retained_app::<RetryApp>(&cell, platform).unwrap();
        assert!(std::ptr::eq(first, second));
        assert_eq!(ATTEMPTS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn retained_app_returns_an_already_installed_winner() {
        static CELL: OnceLock<App> = OnceLock::new();
        struct RacingApp;
        impl Hooks for RacingApp {
            fn configure(app: &mut App) -> Result<(), EdgeError> {
                let winner = App::with_name(RouterService::builder().build(), "winner");
                assert!(CELL.set(winner).is_ok());
                app.set_name("discarded");
                Ok(())
            }

            fn routes() -> Result<RouterService, EdgeError> {
                Ok(RouterService::builder().build())
            }
        }

        let app =
            retained_app::<RacingApp>(&CELL, edgezero_core::PlatformMetadata::default()).unwrap();
        assert_eq!(app.name(), "winner");
    }

    #[test]
    fn probe_accepts_exact_body_bound_and_rejects_overflow() {
        let app = App::build::<FixtureApp>(edgezero_core::PlatformMetadata::default()).unwrap();
        for (size, status) in [(1024 * 1024, 200), (1024 * 1024 + 1, 413)] {
            let req = request_builder()
                .uri("/probe/bounded")
                .header("x-request-token", "bounded")
                .body(Body::from(vec![b'x'; size]))
                .unwrap();
            let response = block_on(app.router().oneshot(req)).unwrap();
            assert_eq!(response.status().as_u16(), status);
        }
    }

    #[test]
    fn retained_probe_echoes_each_request_and_preserves_cookies() {
        let app = App::build::<FixtureApp>(edgezero_core::PlatformMetadata::default()).unwrap();
        for (index, token) in ["one", "two"].into_iter().enumerate() {
            let req = request_builder()
                .uri(format!("/probe/{token}"))
                .header("x-request-token", token)
                .body(Body::from(token))
                .unwrap();
            let response = block_on(app.router().oneshot(req)).unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(response.body().as_bytes().unwrap()).unwrap();
            assert_eq!(value["token"], token);
            assert_eq!(value["path"], token);
            assert_eq!(value["body"], token);
            assert_eq!(value["shared"], index + 1);
        }
        let req = request_builder()
            .uri("/cookies")
            .body(Body::empty())
            .unwrap();
        let response = block_on(app.router().oneshot(req)).unwrap();
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    }

    #[test]
    fn rejects_remote_origins_and_keeps_concrete_apps_separate() {
        let app = App::build::<FixtureApp>(edgezero_core::PlatformMetadata::default()).unwrap();
        let other = App::build::<OtherApp>(edgezero_core::PlatformMetadata::default()).unwrap();
        let req = request_builder()
            .uri("/origin/x")
            .header("x-fixture-backend", "https://example.com/")
            .body(Body::empty())
            .unwrap();
        let response = block_on(app.router().oneshot(req)).unwrap();
        assert_eq!(response.status().as_u16(), 400);
        let req = request_builder().uri("/other").body(Body::empty()).unwrap();
        let response = block_on(other.router().oneshot(req)).unwrap();
        assert_eq!(response.body().as_bytes().unwrap(), b"other-app");
    }
}
