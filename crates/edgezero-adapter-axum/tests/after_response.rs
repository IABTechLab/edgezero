//! After-response tasks through a real Axum listener and Hyper connection.
#![cfg(all(test, feature = "axum"))]
#![expect(
    clippy::expect_used,
    clippy::tests_outside_test_module,
    reason = "this integration-test crate uses explicit fixture diagnostics"
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use edgezero_adapter_axum::dev_server::{AxumDevServer, AxumDevServerConfig};
use edgezero_adapter_axum::ingress_config::AxumIngressConfig;
use edgezero_core::app::App;
use edgezero_core::body::Body;
use edgezero_core::context::RequestContext;
use edgezero_core::error::EdgeError;
use edgezero_core::http::response_builder;
use edgezero_core::outbound::OutboundRequest;
use edgezero_core::router::RouterService;
use futures::future::pending;
use futures::stream::{self, StreamExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const WAIT: Duration = Duration::from_secs(5);

async fn serve(router: RouterService) -> (SocketAddr, JoinHandle<anyhow::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = AxumDevServer::with_config(
        App::new(router),
        AxumDevServerConfig {
            addr: address,
            enable_ctrl_c: false,
            ingress: AxumIngressConfig::new(8, 8192, 100, Duration::from_secs(10))
                .expect("ingress"),
        },
    );
    (address, tokio::spawn(server.serve(listener, pending())))
}

async fn eventually(flag: &AtomicBool) -> bool {
    timeout(WAIT, async {
        while !flag.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

async fn read_until(stream: &mut TcpStream, buffer: &mut Vec<u8>, marker: &[u8]) {
    timeout(WAIT, async {
        while !buffer.windows(marker.len()).any(|window| window == marker) {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.expect("read");
            assert_ne!(read, 0, "connection closed before {marker:?}");
            buffer.extend_from_slice(chunk.get(..read).expect("read length"));
        }
    })
    .await
    .expect("expected response bytes");
}

#[tokio::test]
async fn task_starts_only_after_the_final_body_frame_on_a_kept_alive_connection() {
    let ran = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let shared_ran = Arc::clone(&ran);
    let shared_release = Arc::clone(&release);
    let router = RouterService::builder()
        .get("/gated", move |ctx: RequestContext| {
            let task_ran = Arc::clone(&shared_ran);
            let body_release = Arc::clone(&shared_release);
            async move {
                ctx.after_response(Duration::from_secs(5), async move {
                    task_ran.store(true, Ordering::SeqCst);
                })?;
                let chunks = stream::unfold(0_u8, move |step| {
                    let gate = Arc::clone(&body_release);
                    async move {
                        match step {
                            0 => Some((Ok::<_, EdgeError>(Bytes::from_static(b"first")), 1)),
                            1 => {
                                gate.notified().await;
                                Some((Ok(Bytes::from_static(b"final")), 2))
                            }
                            _ => None,
                        }
                    }
                });
                Ok::<_, EdgeError>(
                    response_builder()
                        .body(Body::from_stream(chunks))
                        .expect("response"),
                )
            }
        })
        .build();
    let (address, server) = serve(router).await;
    let mut client = TcpStream::connect(address).await.expect("connect");
    client
        .write_all(b"GET /gated HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("request");
    let mut received = Vec::new();
    read_until(&mut client, &mut received, b"first").await;
    sleep(Duration::from_millis(100)).await;
    assert!(
        !ran.load(Ordering::SeqCst),
        "the task must not start while the response body is still streaming"
    );
    release.notify_one();
    read_until(&mut client, &mut received, b"0\r\n\r\n").await;
    assert!(
        eventually(&ran).await,
        "the task runs once the final frame is handed to Hyper, without closing the connection"
    );
    // The keep-alive connection still serves requests after the task ran.
    client
        .write_all(b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("second request");
    read_until(&mut client, &mut received, b"404").await;
    server.abort();
}

#[tokio::test]
async fn client_disconnect_mid_stream_still_releases_the_task() {
    let ran = Arc::new(AtomicBool::new(false));
    let shared_ran = Arc::clone(&ran);
    let router = RouterService::builder()
        .get("/stalled", move |ctx: RequestContext| {
            let task_ran = Arc::clone(&shared_ran);
            async move {
                ctx.after_response(Duration::from_secs(5), async move {
                    task_ran.store(true, Ordering::SeqCst);
                })?;
                let chunks =
                    stream::once(async { Ok::<_, EdgeError>(Bytes::from_static(b"first")) })
                        .chain(stream::pending());
                Ok::<_, EdgeError>(
                    response_builder()
                        .body(Body::from_stream(chunks))
                        .expect("response"),
                )
            }
        })
        .build();
    let (address, server) = serve(router).await;
    let mut client = TcpStream::connect(address).await.expect("connect");
    client
        .write_all(b"GET /stalled HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("request");
    let mut received = Vec::new();
    read_until(&mut client, &mut received, b"first").await;
    sleep(Duration::from_millis(50)).await;
    assert!(!ran.load(Ordering::SeqCst), "the body is still open");
    drop(client);
    assert!(
        eventually(&ran).await,
        "dropping the connection drops the body and releases the task"
    );
    server.abort();
}

#[tokio::test]
async fn task_registered_before_an_error_response_still_runs() {
    let ran = Arc::new(AtomicBool::new(false));
    let shared_ran = Arc::clone(&ran);
    let router = RouterService::builder()
        .get("/fails", move |ctx: RequestContext| {
            let task_ran = Arc::clone(&shared_ran);
            async move {
                ctx.after_response(Duration::from_secs(5), async move {
                    task_ran.store(true, Ordering::SeqCst);
                })?;
                Err::<&'static str, _>(EdgeError::service_unavailable("handler failed"))
            }
        })
        .build();
    let (address, server) = serve(router).await;
    let mut client = TcpStream::connect(address).await.expect("connect");
    client
        .write_all(b"GET /fails HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("request");
    let mut received = Vec::new();
    read_until(&mut client, &mut received, b"503").await;
    assert!(
        eventually(&ran).await,
        "an error response still releases the task"
    );
    server.abort();
}

#[tokio::test]
async fn outbound_request_started_inside_a_task_reaches_the_upstream() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.expect("upstream");
    let target = format!(
        "http://{}/sync",
        upstream.local_addr().expect("upstream address")
    );
    let reached = Arc::new(AtomicBool::new(false));
    let succeeded = Arc::new(AtomicBool::new(false));
    let upstream_reached = Arc::clone(&reached);
    let accept = tokio::spawn(async move {
        let (mut socket, _peer) = upstream.accept().await.expect("accept");
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(socket.read_u8().await.expect("upstream read"));
        }
        assert!(head.starts_with(b"GET /sync "));
        upstream_reached.store(true, Ordering::SeqCst);
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("upstream write");
    });
    let shared_succeeded = Arc::clone(&succeeded);
    let router = RouterService::builder()
        .get("/sync", move |ctx: RequestContext| {
            let destination = target.clone();
            let task_succeeded = Arc::clone(&shared_succeeded);
            async move {
                let client = ctx.http_client().expect("outbound client");
                ctx.after_response(Duration::from_secs(5), async move {
                    let request = OutboundRequest::get(&destination).expect("outbound request");
                    if client.send(request).await.is_ok() {
                        task_succeeded.store(true, Ordering::SeqCst);
                    }
                })?;
                Ok::<_, EdgeError>("queued")
            }
        })
        .build();
    let (address, server) = serve(router).await;
    let mut client = TcpStream::connect(address).await.expect("connect");
    client
        .write_all(b"GET /sync HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("request");
    let mut received = Vec::new();
    read_until(&mut client, &mut received, b"queued").await;
    assert!(
        eventually(&reached).await,
        "the task's request reached the upstream"
    );
    assert!(
        eventually(&succeeded).await,
        "the task's outbound request completed"
    );
    accept.await.expect("upstream task");
    server.abort();
}
