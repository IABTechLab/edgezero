use anyhow::Context as _;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::oneshot::{Sender, channel};
use tokio::task::JoinHandle;

use crate::dev_server::AxumDevServer;

/// An assembled application running through the production bounded HTTP/1 path.
///
/// Startup binds the configured address; use loopback port zero to obtain a free port.
/// This installs its own local executor, including under a plain `#[tokio::test]`.
/// It does not initialize logging, run startup preflight or create stores from hook metadata:
/// attach stores to the supplied server explicitly. Foreign provider contexts are absent.
/// Drop signals cancellation without blocking; await [`Self::shutdown`] to observe cleanup.
#[must_use]
pub struct AxumTestServer {
    address: SocketAddr,
    shutdown: Option<Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl AxumTestServer {
    /// Returns the actual bound address, including the assigned ephemeral port.
    #[must_use]
    #[inline]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Cancels active connections and awaits the serving result and owned resource cleanup.
    ///
    /// # Errors
    /// Returns the original serving failure or a serving-task join error.
    #[inline]
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.signal_shutdown();
        match self.task.take() {
            Some(task) => task.await.context("axum test server task failed")?,
            None => Ok(()),
        }
    }

    fn signal_shutdown(&mut self) {
        if let Some(sender) = self.shutdown.take() {
            let _sent = sender.send(());
        }
    }

    /// Binds and starts the supplied server without replacing its application or policies.
    ///
    /// # Errors
    /// Returns listener binding or address inspection failures. Later serving failures are
    /// returned by [`Self::shutdown`].
    #[inline]
    pub async fn start(server: AxumDevServer) -> anyhow::Result<Self> {
        let listener: TcpListener = server.bind_listener().await?;
        let address = listener
            .local_addr()
            .context("failed to inspect axum test listener")?;
        let (sender, receiver) = channel();
        let task = tokio::spawn(server.serve(listener, async move {
            let _closed = receiver.await;
        }));
        Ok(Self {
            address,
            shutdown: Some(sender),
            task: Some(task),
        })
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Drop::pin_drop is unstable and conflicts with drop"
)]
impl Drop for AxumTestServer {
    #[inline]
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

/// Returns a process-wide mutex used to serialize tests that mutate environment variables.
///
/// Both `secret_store` and `service` tests share this lock to avoid data races across
/// test threads when setting or clearing environment variables. Hold it for the whole
/// lifetime of any `edgezero_core::test_env::EnvOverride` — that is the safety contract
/// the guard relies on.
#[inline]
pub fn env_guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use std::io::Error as IoError;
    use std::iter::repeat_n;
    use std::mem::take;
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use edgezero_core::EdgeError;
    use edgezero_core::app::{App, Hooks};
    use edgezero_core::body::Body;
    use edgezero_core::config_store::ConfigExtractionLimits;
    use edgezero_core::context::RequestContext;
    use edgezero_core::extractor::{Path, State};
    use edgezero_core::http::{HeaderMap, Method, Response, StatusCode};
    use edgezero_core::ingress::{AdmissionDecision, IngressGrant, IngressHeadLimits};
    use edgezero_core::middleware::{Middleware, Next};
    use edgezero_core::outbound::{
        HttpClient, OutboundBatch, OutboundBatchDriverEvent, OutboundDispatchRequest,
        OutboundExchangeOwner, OutboundFuture, OutboundHead, OutboundHeaderFidelity,
        OutboundHeaderInput, OutboundHttpClient, OutboundPending, OutboundPreparedBatch,
        OutboundRequest, OutboundResponse, OutboundSendMode, OutboundTime,
    };
    use edgezero_core::response::IntoResponse as _;
    use edgezero_core::response_egress::{
        DetachedResponseEgressDecision, ResponseEgressCompletion, ResponseEgressObserver,
        ResponseEgressOutcome, ResponseEgressPolicy, ResponseEgressReport,
    };
    use edgezero_core::router::RouterService;
    use edgezero_core::{
        Deadline, IndependentOutboundProgress, MonotonicClock, MonotonicInstant,
        OutboundProgressRuntime,
    };
    use futures_util::future::ready;
    use futures_util::stream::{iter, poll_fn, unfold};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot::channel;
    use tokio::task::{spawn_local, yield_now};
    use tokio::time::{sleep, timeout};

    use super::AxumTestServer;
    use crate::dev_server::{AxumDevServer, AxumDevServerConfig};
    use crate::ingress_config::AxumIngressConfig;
    use crate::outbound::native_time;

    struct ParityState {
        configured: AtomicUsize,
        limits: ConfigExtractionLimits,
        start: MonotonicInstant,
    }

    #[derive(serde::Deserialize)]
    struct ParityPath {
        id: String,
    }

    struct ParityApp;

    #[expect(
        clippy::missing_trait_methods,
        reason = "test constructs its router from explicit settings"
    )]
    impl Hooks for ParityApp {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            let shared = Arc::clone(app.state::<Arc<ParityState>>().expect("registered state"));
            shared.configured.fetch_add(1, Ordering::SeqCst);
            app.set_name("configured parity app");
            app.set_config_extraction_limits(shared.limits)?;
            let start = shared.start;
            app.set_monotonic_clock(MonotonicClock::new(move || start));
            Ok(())
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Err(EdgeError::service_unavailable(
                "manual assembly must retain the supplied router",
            ))
        }
    }

    #[edgezero_core::action]
    async fn parity_handler(
        State(shared): State<Arc<ParityState>>,
        Path(path): Path<ParityPath>,
        ctx: RequestContext,
    ) -> Result<String, EdgeError> {
        assert_eq!(ctx.method(), Method::PUT);
        assert_eq!(path.id, "42");
        assert_eq!(ctx.request_start(), shared.start);
        assert_eq!(ctx.monotonic_clock().now(), shared.start);
        assert_eq!(ctx.config_extraction_limits(), shared.limits);
        let named = ctx.config_store("settings").expect("named config");
        let default = ctx.config_store_default().expect("default config");
        let deadline =
            Deadline::at_instant(shared.start.checked_add(shared.limits.timeout).unwrap());
        for handle in [named, default] {
            let read = handle
                .get_bounded(
                    "marker",
                    &ctx.monotonic_clock(),
                    deadline,
                    shared.limits.max_backend_bytes,
                    shared.limits.max_blob_bytes,
                )
                .await
                .unwrap();
            assert_eq!(read.value.unwrap().as_str(), "configured");
        }
        assert_eq!(
            ctx.config_store_default_binding().unwrap().default_key,
            "marker"
        );
        for secrets in [
            ctx.secret_store("credentials").unwrap(),
            ctx.secret_store_default().unwrap(),
        ] {
            assert_eq!(
                secrets.get_bytes("token").await.unwrap().unwrap(),
                bytes::Bytes::from_static(b"test-secret")
            );
        }
        let kv = ctx.kv_store("sessions").expect("named kv");
        let visits: u32 = kv.get_or("visits", 0).await.unwrap();
        let next_visit = visits.checked_add(1).unwrap();
        kv.put("visits", &next_visit).await.unwrap();
        assert_eq!(
            ctx.kv_store_default()
                .unwrap()
                .get::<u32>("visits")
                .await
                .unwrap(),
            Some(next_visit)
        );
        Ok(next_visit.to_string())
    }

    struct ConfiguredApp;

    struct DropProbe(Arc<AtomicUsize>);

    struct CountingMiddleware(Arc<AtomicUsize>);

    struct RecordingClient {
        calls: Arc<AtomicUsize>,
        time: OutboundTime,
    }

    struct Reports(Arc<StdMutex<Vec<ResponseEgressReport>>>);

    impl ResponseEgressObserver for Reports {
        fn complete(&self, report: &ResponseEgressReport) {
            self.0.lock().unwrap().push(report.clone());
        }
    }

    impl RecordingClient {
        fn finish_ready(
            &self,
            request: OutboundDispatchRequest,
        ) -> Result<OutboundResponse, EdgeError> {
            use futures_util::{FutureExt as _, TryFutureExt as _};
            let mut future = Box::pin(self.dispatch_head(request).and_then(OutboundHead::finish));
            // Fixed one-chunk replies need one cooperative turn before EOF.
            for _ in 0_u8..2 {
                if let Some(outcome) = future.as_mut().now_or_never() {
                    return outcome;
                }
            }
            Err(EdgeError::internal(IoError::other(
                "scripted response exceeded its fixed poll bound",
            )))
        }
    }

    impl OutboundHttpClient for RecordingClient {
        fn begin(&self, request: OutboundDispatchRequest) -> Result<OutboundPending, EdgeError> {
            let outcome = self.finish_ready(request);
            Ok(OutboundPending::new(
                Box::pin(ready(outcome)),
                OutboundExchangeOwner::new(|| {}),
            ))
        }

        fn dispatch_head(&self, request: OutboundDispatchRequest) -> OutboundFuture<OutboundHead> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (_, context) = request.into_parts();
            let head = OutboundHead::new(
                StatusCode::OK,
                OutboundHeaderInput::new(
                    "supplied",
                    HeaderMap::new(),
                    OutboundHeaderFidelity::AdapterVisibleFields,
                ),
                Box::pin(iter([Ok(bytes::Bytes::from_static(b"supplied"))])),
                context,
                OutboundExchangeOwner::new(|| {}),
            );
            Box::pin(ready(head))
        }

        fn progress(&self) -> Rc<dyn OutboundProgressRuntime> {
            Rc::new(IndependentOutboundProgress)
        }

        fn start_batch_until(&self, batch: OutboundPreparedBatch) -> OutboundBatch {
            let (context, requests) = batch.into_slots();
            let events = requests
                .into_iter()
                .map(|(index, request)| {
                    let outcome = self.finish_ready(request);
                    OutboundBatchDriverEvent::Item {
                        index,
                        completed_at: self.time.clock().now(),
                        outcome,
                    }
                })
                .collect::<Vec<_>>();
            OutboundBatch::from_driver(context, iter(events), OutboundExchangeOwner::new(|| {}))
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
    }

    #[async_trait::async_trait(?Send)]
    impl Middleware for CountingMiddleware {
        async fn handle(&self, ctx: RequestContext, next: Next<'_>) -> Result<Response, EdgeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            next.run(ctx).await
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "only assembly policies are under test"
    )]
    impl Hooks for ConfiguredApp {
        fn configure(app: &mut App) -> Result<(), EdgeError> {
            app.set_ingress_head_limits(
                IngressHeadLimits::default().with_max_request_target_bytes(64)?,
            );
            app.set_error_response_renderer(|error| {
                let mut response = error.into_response().expect("test renderer");
                response
                    .headers_mut()
                    .insert("x-renderer", "configured".parse().unwrap());
                response
            });
            Ok(())
        }

        fn routes() -> Result<RouterService, EdgeError> {
            Ok(RouterService::builder()
                .get("/", |_| async { Ok::<_, EdgeError>("hello") })
                .get("/target/{*rest}", |_| async { Ok::<_, EdgeError>("hello") })
                .build())
        }
    }

    fn server(app: App) -> AxumDevServer {
        AxumDevServer::with_config(
            app,
            AxumDevServerConfig {
                addr: SocketAddr::from(([127, 0, 0, 1], 0)),
                enable_ctrl_c: false,
                ..AxumDevServerConfig::default()
            },
        )
    }

    async fn exchange(address: SocketAddr, request: &[u8]) -> Vec<u8> {
        timeout(Duration::from_secs(5), async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(request).await.unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            response
        })
        .await
        .expect("bounded exchange")
    }

    async fn read_head(socket: &mut TcpStream) -> String {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(socket.read_u8().await.unwrap());
            assert!(head.len() <= 8192, "bounded test response head");
        }
        String::from_utf8(head).unwrap()
    }

    async fn read_chunk(socket: &mut TcpStream) -> Vec<u8> {
        let mut line = Vec::new();
        while !line.ends_with(b"\r\n") {
            line.push(socket.read_u8().await.unwrap());
            assert!(line.len() <= 32, "bounded chunk size line");
        }
        let size = usize::from_str_radix(String::from_utf8(line).unwrap().trim(), 16).unwrap();
        let mut chunk = vec![0; size];
        socket.read_exact(&mut chunk).await.unwrap();
        assert_eq!(socket.read_u16().await.unwrap(), 0x0d0a);
        chunk
    }

    #[tokio::test]
    async fn configured_target_limit_applies_on_tcp() {
        let target = format!("/target/{}", "a".repeat(92));
        assert_eq!(target.len(), 100);
        let app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let response = reqwest::get(format!("http://{}{target}", running.address()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::URI_TOO_LONG);
        assert_eq!(response.headers()["x-renderer"], "configured");
        running.shutdown().await.unwrap();

        let direct_app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = channel();
        let task = tokio::spawn(server(direct_app).serve(listener, async {
            let _closed = stopped.await;
        }));
        let direct_response = exchange(
            address,
            format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(direct_response.starts_with(b"HTTP/1.1 414"));
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();

        let unconfigured = App::new(ConfiguredApp::routes().unwrap());
        let control = AxumTestServer::start(server(unconfigured)).await.unwrap();
        let control_response = exchange(
            control.address(),
            format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await;
        assert!(control_response.starts_with(b"HTTP/1.1 200"));
        assert!(control_response.ends_with(b"\r\n\r\nhello"));
        control.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn configured_renderer_applies_to_not_found() {
        let app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let response = reqwest::get(format!("http://{}/missing", running.address()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()["x-renderer"], "configured");
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn admission_abort_prevents_response_and_dispatch() {
        let handlers = Arc::new(AtomicUsize::new(0));
        let middleware = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&handlers);
        let router = RouterService::builder()
            .get("/", move |_| {
                let calls = Arc::clone(&observed);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, EdgeError>("hello")
                }
            })
            .middleware(CountingMiddleware(Arc::clone(&middleware)))
            .build();
        let mut app = App::new(router);
        app.set_ingress_admission_policy(|_| AdmissionDecision::Abort);
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let mut socket = TcpStream::connect(running.address()).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        let _closed = timeout(Duration::from_secs(2), socket.read_to_end(&mut response))
            .await
            .unwrap();
        assert!(response.is_empty());
        assert_eq!(handlers.load(Ordering::SeqCst), 0);
        assert_eq!(middleware.load(Ordering::SeqCst), 0);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::redundant_test_prefix,
        reason = "matches the approved EZ-23 acceptance sentinel"
    )]
    async fn test_server_preserves_upload_and_read_deadline() {
        let router = RouterService::builder()
            .post("/", |ctx: RequestContext| async move {
                let bytes = ctx.body_bytes(2 * 1024 * 1024).await?;
                assert!(bytes.iter().all(|byte| *byte == b'x'));
                Ok::<_, EdgeError>(bytes.len().to_string())
            })
            .build();
        let mut app = App::new(router);
        app.set_ingress_admission_policy(|head| AdmissionDecision::Admit {
            completion: ResponseEgressCompletion::empty(),
            grant: IngressGrant::empty(),
            read_deadline: head.read_deadline_after(Duration::from_millis(500)),
        });
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let mut request = b"POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for _chunk in 0_u32..16 {
            request.extend_from_slice(b"10000\r\n");
            request.extend(repeat_n(b'x', 0x0001_0000));
            request.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"0\r\n\r\n");
        let response = exchange(running.address(), &request).await;
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(response.ends_with(b"1048576"));
        let timeout_response = exchange(
            running.address(),
            b"POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 1\r\n\r\n",
        )
        .await;
        assert!(timeout_response.starts_with(b"HTTP/1.1 408"));
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::redundant_test_prefix,
        reason = "matches the approved EZ-23 acceptance sentinel"
    )]
    async fn test_server_streams_before_source_completion() {
        let produced = Arc::new(AtomicUsize::new(0));
        let (acknowledge, acknowledgement_receiver) = channel();
        let acknowledgement = Arc::new(StdMutex::new(Some(acknowledgement_receiver)));
        let observed = Arc::clone(&produced);
        let router = RouterService::builder()
            .get("/", move |_| {
                let production = Arc::clone(&observed);
                let acknowledged = acknowledgement.lock().unwrap().take().unwrap();
                async move {
                    let body = async_stream::stream! {
                        production.store(1, Ordering::SeqCst);
                        yield Ok::<_, EdgeError>(bytes::Bytes::from_static(b"first"));
                        acknowledged.await.unwrap();
                        sleep(Duration::from_millis(50)).await;
                        production.store(2, Ordering::SeqCst);
                        yield Ok(bytes::Bytes::from_static(b"second"));
                        sleep(Duration::from_millis(50)).await;
                        production.store(3, Ordering::SeqCst);
                        yield Ok(bytes::Bytes::from_static(b"third"));
                    };
                    Ok::<_, EdgeError>(Response::new(Body::from_external_stream(body)))
                }
            })
            .build();
        let running = AxumTestServer::start(server(App::new(router)))
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            let mut socket = TcpStream::connect(running.address()).await.unwrap();
            socket
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let head = read_head(&mut socket).await;
            assert!(head.contains("transfer-encoding: chunked\r\n"));
            assert_eq!(read_chunk(&mut socket).await, b"first");
            assert_eq!(produced.load(Ordering::SeqCst), 1);
            acknowledge.send(()).unwrap();
            assert_eq!(read_chunk(&mut socket).await, b"second");
            assert_eq!(read_chunk(&mut socket).await, b"third");
            assert!(read_chunk(&mut socket).await.is_empty());
        })
        .await
        .unwrap();
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::redundant_test_prefix,
        reason = "matches the approved EZ-23 acceptance sentinel"
    )]
    async fn test_server_buffers_with_exact_content_length() {
        let running = AxumTestServer::start(server(
            App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap(),
        ))
        .await
        .unwrap();
        let response = exchange(
            running.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        let wire = String::from_utf8(response).unwrap();
        assert!(wire.contains("content-length: 5\r\n"));
        assert!(wire.ends_with("\r\n\r\nhello"));
        assert!(!wire.contains("transfer-encoding"));
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::redundant_test_prefix,
        reason = "matches the approved EZ-23 acceptance sentinel"
    )]
    async fn test_server_suppresses_head_payload() {
        let drops = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let resources = Arc::clone(&drops);
        let observed = Arc::clone(&polls);
        let router = RouterService::builder()
            .route("/", Method::HEAD, move |_| {
                let guard = DropProbe(Arc::clone(&resources));
                let body_polls = Arc::clone(&observed);
                async move {
                    let body = poll_fn(move |_| {
                        let _retained = &guard;
                        body_polls.fetch_add(1, Ordering::SeqCst);
                        Poll::Ready(Some(Ok::<_, EdgeError>(bytes::Bytes::from_static(
                            b"never",
                        ))))
                    });
                    Ok::<_, EdgeError>(Response::new(Body::from_external_stream(body)))
                }
            })
            .build();
        let running = AxumTestServer::start(server(App::new(router)))
            .await
            .unwrap();
        let response = exchange(
            running.address(),
            b"HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with(b"HTTP/1.1 200"));
        let head_end = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            .checked_add(4);
        assert_eq!(
            head_end,
            Some(response.len()),
            "HEAD must transmit no bytes after its first head terminator"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raw_head_limit_precedes_admission() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&admissions);
        let mut app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        app.set_ingress_admission_policy(move |head| {
            observed.fetch_add(1, Ordering::SeqCst);
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::empty(),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(2)),
            }
        });
        let configured = AxumDevServer::with_config(
            app,
            AxumDevServerConfig {
                ingress: AxumIngressConfig::new(2, 8192, 4, Duration::from_secs(2)).unwrap(),
                addr: SocketAddr::from(([127, 0, 0, 1], 0)),
                enable_ctrl_c: false,
            },
        );
        let running = AxumTestServer::start(configured).await.unwrap();
        let response = exchange(running.address(), b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nA: 1\r\nA: 2\r\nA: 3\r\n\r\n").await;
        assert!(response.starts_with(b"HTTP/1.1 431"));
        assert_eq!(admissions.load(Ordering::SeqCst), 0);
        let recovered = exchange(
            running.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(recovered.starts_with(b"HTTP/1.1 200"));
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::redundant_test_prefix,
        reason = "matches the approved EZ-23 acceptance sentinel"
    )]
    async fn test_server_shutdown_releases_live_source() {
        let sources = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&sources);
        let router = RouterService::builder()
            .get("/", move |_| {
                let guard = DropProbe(Arc::clone(&observed));
                async move {
                    let mut first = true;
                    let body = poll_fn(move |_| {
                        let _retained = &guard;
                        if take(&mut first) {
                            Poll::Ready(Some(Ok::<_, EdgeError>(bytes::Bytes::from_static(
                                b"first",
                            ))))
                        } else {
                            Poll::Pending
                        }
                    });
                    Ok::<_, EdgeError>(Response::new(Body::from_external_stream(body)))
                }
            })
            .build();
        let completion_counter = Arc::clone(&completions);
        let mut app = App::new(router);
        app.set_ingress_admission_policy(move |head| {
            let completed = Arc::clone(&completion_counter);
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::new(move |_| {
                    completed.fetch_add(1, Ordering::SeqCst);
                }),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(2)),
            }
        });
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let mut socket = TcpStream::connect(running.address()).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            read_head(&mut socket).await;
            assert_eq!(read_chunk(&mut socket).await, b"first");
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), running.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sources.load(Ordering::SeqCst), 1);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn foreign_platform_app_keeps_configured_policies() {
        let platform = edgezero_adapter_cloudflare::CLOUDFLARE_PLATFORM;
        let app = App::build::<ConfiguredApp>(platform).unwrap();
        assert_eq!(app.platform(), platform);
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let response = exchange(
            running.address(),
            format!(
                "GET /{} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                "a".repeat(99)
            )
            .as_bytes(),
        )
        .await;
        assert!(response.starts_with(b"HTTP/1.1 414"));
        assert!(
            String::from_utf8(response)
                .unwrap()
                .contains("x-renderer: configured\r\n")
        );
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn plain_tokio_test_runs_local_handler_work() {
        let calls = Arc::new(AtomicUsize::new(0));
        let supplied = HttpClient::with_client(RecordingClient {
            calls: Arc::clone(&calls),
            time: native_time(MonotonicClock::default()),
        });
        let router = RouterService::builder()
            .with_state(supplied)
            .get("/", |ctx: RequestContext| async move {
                let pending = ctx
                    .http_client()
                    .expect("supplied client")
                    .begin(OutboundRequest::get("https://fixture.invalid/local")?)
                    .await?;
                let response = pending.collect().await?;
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    response.into_bytes_bounded(16).await?,
                    b"supplied".as_slice()
                );
                let values = Rc::new(["local", "stream", "complete"]);
                let (sender, receiver) = mpsc::channel(1);
                spawn_local(async move {
                    for value in values.iter() {
                        sender
                            .send(Ok::<_, EdgeError>(bytes::Bytes::copy_from_slice(
                                value.as_bytes(),
                            )))
                            .await
                            .unwrap();
                        yield_now().await;
                    }
                });
                let body = unfold(receiver, |mut stream_receiver| async {
                    stream_receiver
                        .recv()
                        .await
                        .map(|value| (value, stream_receiver))
                });
                Ok::<_, EdgeError>(Response::new(Body::from_external_stream(body)))
            })
            .build();
        let running = AxumTestServer::start(server(App::new(router)))
            .await
            .unwrap();
        let body = timeout(Duration::from_secs(2), async {
            reqwest::get(format!("http://{}/", running.address()))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(body, "localstreamcomplete");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn state_client_skips_real_transport_initialization() {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_uri = format!("http://{}/should-not-connect", target.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let supplied_client = HttpClient::with_client(RecordingClient {
            calls: Arc::clone(&calls),
            time: native_time(MonotonicClock::default()),
        })
        .with_proxy_marker(false);
        let router = RouterService::builder()
            .with_state(supplied_client)
            .get("/", move |ctx: RequestContext| {
                let uri = target_uri.clone();
                async move {
                    let client = ctx.http_client().expect("registered client");
                    assert!(!client.proxy_marker_enabled());
                    let response = client.send(OutboundRequest::get(&uri)?).await?;
                    assert_eq!(response.status(), StatusCode::OK);
                    Ok::<_, EdgeError>("sent")
                }
            })
            .build();
        let running = AxumTestServer::start(server(App::new(router)))
            .await
            .unwrap();
        let response = exchange(
            running.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        timeout(Duration::from_millis(25), target.accept())
            .await
            .expect_err("supplied client must not open a target socket");
        running.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn configured_egress_policy_clock_and_observer_survive_server_ownership() {
        let reports = Arc::new(StdMutex::new(Vec::new()));
        let policies = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let start = MonotonicInstant::now()
            .checked_add(Duration::from_secs(60))
            .unwrap();
        let mut app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        app.set_monotonic_clock(MonotonicClock::new(move || start));
        app.set_response_egress_observer(Reports(Arc::clone(&reports)));
        let observed = Arc::clone(&policies);
        app.set_response_egress_policy(move |head, began| {
            assert_eq!(head.request_start(), start);
            assert_eq!(began, start);
            observed.fetch_add(1, Ordering::SeqCst);
            ResponseEgressPolicy {
                write_deadline: Deadline::at_instant(
                    began.checked_add(Duration::from_secs(1)).unwrap(),
                ),
            }
        });
        let completion_counter = Arc::clone(&completions);
        app.set_ingress_admission_policy(move |head| {
            assert_eq!(head.request_start(), start);
            let completed = Arc::clone(&completion_counter);
            AdmissionDecision::Admit {
                completion: ResponseEgressCompletion::new(move |_| {
                    completed.fetch_add(1, Ordering::SeqCst);
                }),
                grant: IngressGrant::empty(),
                read_deadline: head.read_deadline_after(Duration::from_secs(1)),
            }
        });
        let running = AxumTestServer::start(server(app)).await.unwrap();
        for _request in 0_u32..2 {
            let response = exchange(
                running.address(),
                b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await;
            assert!(response.starts_with(b"HTTP/1.1 200"));
        }
        running.shutdown().await.unwrap();
        assert_eq!(policies.load(Ordering::SeqCst), 2);
        assert_eq!(completions.load(Ordering::SeqCst), 2);
        let observed_reports = reports.lock().unwrap();
        assert_eq!(observed_reports.len(), 2);
        for report in observed_reports.iter() {
            assert_eq!(report.request_start, start);
            assert_eq!(report.outcome, ResponseEgressOutcome::HostHandoff);
        }
    }

    #[tokio::test]
    async fn configured_detached_decision_sends_or_aborts_before_admission() {
        for abort in [false, true] {
            let decisions = Arc::new(AtomicUsize::new(0));
            let completions = Arc::new(AtomicUsize::new(0));
            let mut app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
            app.set_ingress_admission_policy(|_| panic!("invalid head must precede admission"));
            app.set_response_egress_policy(|_, _| {
                panic!("detached response skips routed egress policy")
            });
            let observed = Arc::clone(&decisions);
            let completed = Arc::clone(&completions);
            app.set_detached_response_egress_decision_factory(move |head| {
                observed.fetch_add(1, Ordering::SeqCst);
                if abort {
                    DetachedResponseEgressDecision::Abort
                } else {
                    let terminal_counter = Arc::clone(&completed);
                    DetachedResponseEgressDecision::Send {
                        completion: ResponseEgressCompletion::new(move |_| {
                            terminal_counter.fetch_add(1, Ordering::SeqCst);
                        }),
                        deadline: Deadline::at_instant(
                            head.request_start()
                                .checked_add(Duration::from_secs(1))
                                .unwrap(),
                        ),
                    }
                }
            });
            let running = AxumTestServer::start(server(app)).await.unwrap();
            let mut socket = TcpStream::connect(running.address()).await.unwrap();
            socket
                .write_all(
                    format!(
                        "GET /{} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                        "a".repeat(99)
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            let _closed = timeout(Duration::from_secs(2), socket.read_to_end(&mut response))
                .await
                .unwrap();
            if abort {
                assert!(response.is_empty());
            } else {
                assert!(response.starts_with(b"HTTP/1.1 414"));
            }
            running.shutdown().await.unwrap();
            assert_eq!(decisions.load(Ordering::SeqCst), 1);
            assert_eq!(completions.load(Ordering::SeqCst), usize::from(!abort));
        }
    }

    #[tokio::test]
    async fn dropped_test_server_releases_live_connections_without_blocking() {
        let app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let mut socket = TcpStream::connect(running.address()).await.unwrap();
        socket.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        drop(running);
        let mut response = Vec::new();
        let _closed = timeout(Duration::from_secs(2), socket.read_to_end(&mut response))
            .await
            .unwrap();
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn server_reports_binding_and_serving_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app = App::build::<ConfiguredApp>(crate::AXUM_PLATFORM).unwrap();
        let server = AxumDevServer::with_config(
            app,
            AxumDevServerConfig {
                addr: listener.local_addr().unwrap(),
                enable_ctrl_c: false,
                ..AxumDevServerConfig::default()
            },
        );
        assert!(AxumTestServer::start(server).await.is_err());
        let running = AxumTestServer {
            address: listener.local_addr().unwrap(),
            shutdown: None,
            task: Some(tokio::spawn(async {
                Err(anyhow::anyhow!("serving failure fixture"))
            })),
        };
        assert_eq!(
            running.shutdown().await.unwrap_err().to_string(),
            "serving failure fixture"
        );
    }

    #[tokio::test]
    async fn explicit_settings_preserve_state_store_and_policy_identity_across_requests() {
        use crate::config_store::AxumConfigStore;
        use crate::config_store_limits::ConfigStoreLimits;
        use crate::key_value_store::PersistentKvStore;
        use edgezero_core::config_store::ConfigStoreHandle;
        use edgezero_core::key_value_store::KvHandle;
        use edgezero_core::secret_store::{InMemorySecretStore, SecretHandle};
        use edgezero_core::store_registry::{
            BoundSecretStore, ConfigRegistry, ConfigStoreBinding, KvRegistry, SecretRegistry,
        };

        let shared = Arc::new(ParityState {
            configured: AtomicUsize::new(0),
            limits: ConfigExtractionLimits {
                max_backend_bytes: 256,
                max_blob_bytes: 64,
                max_secret_bytes: 64,
                max_total_bytes: 512,
                timeout: Duration::from_millis(500),
                ..ConfigExtractionLimits::default()
            },
            start: MonotonicInstant::now()
                .checked_add(Duration::from_secs(60))
                .unwrap(),
        });
        let router = RouterService::builder()
            .with_state(Arc::clone(&shared))
            .route("/state/{id}", Method::PUT, parity_handler)
            .build();
        let app = App::from_router::<ParityApp>(router, crate::AXUM_PLATFORM).unwrap();
        assert_eq!(app.name(), "configured parity app");
        assert!(Arc::ptr_eq(
            app.state::<Arc<ParityState>>().unwrap(),
            &shared
        ));
        let config = ConfigStoreHandle::new(Arc::new(
            AxumConfigStore::from_map(
                [("marker".to_owned(), "configured".to_owned())],
                ConfigStoreLimits::default(),
            )
            .unwrap(),
        ));
        let directory = tempfile::tempdir().unwrap();
        let kv = KvHandle::new(Arc::new(
            PersistentKvStore::new(directory.path().join("state.redb")).unwrap(),
        ));
        let secrets = SecretHandle::new(Arc::new(InMemorySecretStore::new([(
            "provider/token",
            "test-secret",
        )])));
        let configured = server(app)
            .with_config_registry(ConfigRegistry::single_id(
                "settings".to_owned(),
                ConfigStoreBinding {
                    handle: config,
                    default_key: "marker".to_owned(),
                },
            ))
            .with_kv_registry(KvRegistry::single_id("sessions".to_owned(), kv))
            .with_secret_registry(SecretRegistry::single_id(
                "credentials".to_owned(),
                BoundSecretStore::new(secrets, "provider".to_owned()),
            ));
        let running = AxumTestServer::start(configured).await.unwrap();
        for expected in ["1", "2"] {
            let response = exchange(running.address(), b"PUT /state/42 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").await;
            assert!(response.starts_with(b"HTTP/1.1 200"));
            assert!(response.ends_with(expected.as_bytes()));
        }
        running.shutdown().await.unwrap();
        assert_eq!(shared.configured.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn configured_write_deadline_releases_pending_source_once() {
        let sources = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&sources);
        let router = RouterService::builder()
            .get("/", move |_| {
                let guard = DropProbe(Arc::clone(&observed));
                async move {
                    let mut first = true;
                    let body = poll_fn(move |_| {
                        let _retained = &guard;
                        if take(&mut first) {
                            Poll::Ready(Some(Ok::<_, EdgeError>(bytes::Bytes::from_static(
                                b"first",
                            ))))
                        } else {
                            Poll::Pending
                        }
                    });
                    Ok::<_, EdgeError>(Response::new(Body::from_external_stream(body)))
                }
            })
            .build();
        let reports = Arc::new(StdMutex::new(Vec::new()));
        let mut app = App::new(router);
        app.set_response_egress_observer(Reports(Arc::clone(&reports)));
        app.set_response_egress_policy(|_, began| ResponseEgressPolicy {
            write_deadline: Deadline::at_instant(
                began.checked_add(Duration::from_millis(500)).unwrap(),
            ),
        });
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let mut socket = TcpStream::connect(running.address()).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            read_head(&mut socket).await;
            assert_eq!(read_chunk(&mut socket).await, b"first");
            assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
        })
        .await
        .unwrap();
        running.shutdown().await.unwrap();
        assert_eq!(sources.load(Ordering::SeqCst), 1);
        let observed_reports = reports.lock().unwrap();
        assert_eq!(observed_reports.len(), 1);
        assert_eq!(
            observed_reports[0].outcome,
            ResponseEgressOutcome::DeadlineExceeded
        );
    }

    #[tokio::test]
    async fn admission_refusal_skips_dispatch_and_keeps_server_healthy() {
        let handlers = Arc::new(AtomicUsize::new(0));
        let middleware = Arc::new(AtomicUsize::new(0));
        let admissions = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&handlers);
        let router = RouterService::builder()
            .get("/", move |_| {
                let count = Arc::clone(&observed);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, EdgeError>("healthy")
                }
            })
            .middleware(CountingMiddleware(Arc::clone(&middleware)))
            .build();
        let mut app = App::new(router);
        let admission_count = Arc::clone(&admissions);
        let completion_count = Arc::clone(&completions);
        app.set_ingress_admission_policy(move |head| {
            if admission_count.fetch_add(1, Ordering::SeqCst) == 0 {
                let completed = Arc::clone(&completion_count);
                let mut response = Response::new(Body::from("refused"));
                *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                AdmissionDecision::Refuse {
                    completion: ResponseEgressCompletion::new(move |_| {
                        completed.fetch_add(1, Ordering::SeqCst);
                    }),
                    response,
                }
            } else {
                AdmissionDecision::Admit {
                    completion: ResponseEgressCompletion::empty(),
                    grant: IngressGrant::empty(),
                    read_deadline: head.read_deadline_after(Duration::from_secs(2)),
                }
            }
        });
        let running = AxumTestServer::start(server(app)).await.unwrap();
        let refused = exchange(
            running.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(refused.starts_with(b"HTTP/1.1 503"));
        assert!(refused.ends_with(b"refused"));
        assert_eq!(handlers.load(Ordering::SeqCst), 0);
        assert_eq!(middleware.load(Ordering::SeqCst), 0);
        let healthy = exchange(
            running.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(healthy.starts_with(b"HTTP/1.1 200"));
        assert!(healthy.ends_with(b"healthy"));
        running.shutdown().await.unwrap();
        assert_eq!(handlers.load(Ordering::SeqCst), 1);
        assert_eq!(middleware.load(Ordering::SeqCst), 1);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }
}
