use std::net::SocketAddr;
use std::pin::Pin;

use axum::body::{Body as AxumBody, BodyDataStream};
use axum::extract::connect_info::ConnectInfo;
use axum::http::{Request, request::Parts};
use edgezero_core::body::Body;
use edgezero_core::error::EdgeError;
use edgezero_core::http::Request as CoreRequest;
use edgezero_core::outbound::HttpClient;
use edgezero_core::time::{Deadline, MonotonicClock};
use futures_util::{StreamExt as _, stream};
use http_body::Body as _;
use tokio::time::timeout;

use crate::context::AxumRequestContext;
use crate::outbound::AxumOutboundClient;

/// Convert an Axum/Hyper request into an `EdgeZero` core request while preserving streaming bodies
/// and exposing connection metadata through `AxumRequestContext`.
///
/// # Errors
/// Returns an error if the outbound client cannot be initialized.
#[inline]
#[expect(
    clippy::unused_async,
    reason = "the public converter retains its established async API while request bodies remain lazy"
)]
pub async fn into_core_request(request: Request<AxumBody>) -> Result<CoreRequest, String> {
    let (parts, axum_body) = request.into_parts();
    let client = AxumOutboundClient::try_new()
        .map_err(|_error| "failed to build outbound HTTP client".to_owned())?;
    Ok(into_core_request_parts(
        parts,
        axum_body,
        None,
        HttpClient::with_client(client),
    ))
}

pub(crate) fn into_core_request_parts(
    parts: Parts,
    axum_body: AxumBody,
    read_lifetime: Option<(Deadline, MonotonicClock)>,
    outbound_client: HttpClient,
) -> CoreRequest {
    // A zero size hint can still leave trailers; only the transport's terminal signal is enough.
    let body = if axum_body.is_end_stream() {
        Body::empty()
    } else {
        match read_lifetime {
            Some((deadline, monotonic_clock)) => {
                deadline_body(axum_body.into_data_stream(), deadline, monotonic_clock)
            }
            None => Body::from_external_stream(axum_body.into_data_stream()),
        }
    };

    let mut core_request = CoreRequest::from_parts(parts, body);

    if let Some(remote_addr) = core_request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr)
    {
        core_request
            .extensions_mut()
            .remove::<ConnectInfo<SocketAddr>>();
        AxumRequestContext::insert(
            &mut core_request,
            AxumRequestContext {
                remote_addr: Some(remote_addr),
            },
        );
    }

    core_request.extensions_mut().insert(outbound_client);

    core_request
}

fn deadline_body(
    body_stream: BodyDataStream,
    deadline: Deadline,
    monotonic_clock: MonotonicClock,
) -> Body {
    let deadline_stream = stream::unfold(
        Some(Box::pin(body_stream)),
        move |stream_state: Option<Pin<Box<BodyDataStream>>>| {
            let clock = monotonic_clock.clone();
            async move {
                let mut state_stream = stream_state?;
                let Some(remaining) = deadline.remaining_at(clock.now()) else {
                    return Some((
                        Err(EdgeError::request_timeout(
                            "inbound body read deadline exceeded",
                        )),
                        None,
                    ));
                };
                let item = match timeout(remaining, state_stream.next()).await {
                    Ok(item) => item,
                    Err(_elapsed) => {
                        return Some((
                            Err(EdgeError::request_timeout(
                                "inbound body read deadline exceeded",
                            )),
                            None,
                        ));
                    }
                };
                if deadline.is_expired_at(clock.now()) {
                    return Some((
                        Err(EdgeError::request_timeout(
                            "inbound body read deadline exceeded",
                        )),
                        None,
                    ));
                }
                match item {
                    Some(Ok(bytes)) => Some((Ok(bytes), Some(state_stream))),
                    Some(Err(error)) => Some((Err(EdgeError::internal(error)), None)),
                    None => None,
                }
            }
        },
    );
    Body::from_stream(deadline_stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use edgezero_core::body::Body;
    use edgezero_core::http::{Method, StatusCode};
    use edgezero_core::time::MonotonicInstant;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    struct DropSignal(Arc<AtomicUsize>);

    struct EndSignalBody {
        ended: bool,
        polls: Arc<AtomicUsize>,
        release: DropSignal,
    }

    impl http_body::Body for EndSignalBody {
        type Data = Bytes;
        type Error = io::Error;

        fn is_end_stream(&self) -> bool {
            self.ended
        }

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(self.release.0.load(Ordering::SeqCst), 0);
            Poll::Ready(None)
        }

        fn size_hint(&self) -> http_body::SizeHint {
            http_body::SizeHint::with_exact(0)
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deadline_body_releases_source_when_timeout_is_emitted() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let signal = DropSignal(Arc::clone(&dropped));
        let source = stream::poll_fn(move |_cx| {
            let _keep_alive = &signal;
            Poll::<Option<Result<Bytes, io::Error>>>::Pending
        });
        let start = MonotonicInstant::now();
        let clock = MonotonicClock::new(move || start);
        let body = deadline_body(
            AxumBody::from_stream(source).into_data_stream(),
            Deadline::at_instant(start),
            clock,
        );
        let mut body_stream = body.into_stream().expect("stream");

        let error = body_stream
            .next()
            .await
            .expect("terminal timeout item")
            .expect_err("timeout");
        assert_eq!(error.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn host_known_empty_body_is_preserved_without_reading() {
        let client = HttpClient::with_client(AxumOutboundClient::try_new().expect("client"));
        for (method, admitted) in [
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::OPTIONS,
            Method::DELETE,
        ]
        .into_iter()
        .flat_map(|method| [false, true].map(|admitted| (method.clone(), admitted)))
        {
            let polls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let request = Request::builder()
                .method(method)
                .uri("/demo")
                .body(AxumBody::new(EndSignalBody {
                    ended: true,
                    polls: Arc::clone(&polls),
                    release: DropSignal(Arc::clone(&drops)),
                }))
                .expect("request");
            let now = MonotonicInstant::now();
            let lifetime =
                admitted.then(|| (Deadline::at_instant(now), MonotonicClock::new(move || now)));
            let (parts, native) = request.into_parts();
            let core = into_core_request_parts(parts, native, lifetime, client.clone());
            assert_eq!(core.body().as_bytes(), Some(b"".as_slice()));
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            drop(core);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn zero_size_hint_and_zero_content_length_do_not_certify_empty_body() {
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let request = Request::builder()
            .method(Method::GET)
            .uri("/demo")
            .header("content-length", "0")
            .body(AxumBody::new(EndSignalBody {
                ended: false,
                polls: Arc::clone(&polls),
                release: DropSignal(Arc::clone(&drops)),
            }))
            .expect("request");
        let core = into_core_request(request).await.expect("conversion");
        assert!(core.body().is_stream());
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(core);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unknown_body_preserves_empty_chunks_then_data_and_source_errors() {
        for source_error in [false, true] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&polls);
            let drops = Arc::new(AtomicUsize::new(0));
            let release = DropSignal(Arc::clone(&drops));
            let mut chunks = vec![
                Ok(Bytes::new()),
                if source_error {
                    Err(io::Error::other("inbound fixture source error"))
                } else {
                    Ok(Bytes::from_static(b"data"))
                },
            ]
            .into_iter();
            let source = stream::poll_fn(move |_cx| {
                let _retain_release = &release;
                observed.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(chunks.next())
            });
            let request = Request::builder()
                .method(Method::GET)
                .uri("/demo")
                .header("content-length", "0")
                .body(AxumBody::from_stream(source))
                .expect("request");
            let core = into_core_request(request).await.expect("conversion");
            assert!(core.body().is_stream());
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            let outcome = core.into_body().into_bytes_bounded(4).await;
            if source_error {
                assert_eq!(
                    outcome.expect_err("original source failure").status(),
                    StatusCode::INTERNAL_SERVER_ERROR
                );
            } else {
                assert_eq!(outcome.expect("data survives"), Bytes::from_static(b"data"));
            }
            assert!(polls.load(Ordering::SeqCst) >= 2);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn converts_request_and_records_connect_info() {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/demo")
            .header("x-test", "1")
            .body(AxumBody::from("payload"))
            .expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo::<SocketAddr>("127.0.0.1:4000".parse().unwrap()));

        let core_request = into_core_request(request)
            .await
            .expect("request conversion");
        assert_eq!(core_request.method(), &Method::POST);
        assert_eq!(core_request.uri().path(), "/demo");
        assert_eq!(core_request.headers()["x-test"], "1");
        match core_request.body() {
            Body::Stream(_) => {} // streaming bodies stay streaming
            Body::Once(_) => panic!("body should remain streaming"),
        }

        let context = AxumRequestContext::get(&core_request).expect("context");
        assert_eq!(context.remote_addr, Some("127.0.0.1:4000".parse().unwrap()));
        assert!(
            core_request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .is_none()
        );
        assert!(core_request.extensions().get::<HttpClient>().is_some());
    }

    #[tokio::test]
    async fn supplied_outbound_client_policy_is_retained_by_the_core_request() {
        let client = HttpClient::with_client(AxumOutboundClient::try_new().expect("client"))
            .with_proxy_marker(false);
        let request = Request::builder()
            .method(Method::GET)
            .uri("/demo")
            .body(AxumBody::empty())
            .expect("request");
        let (parts, body) = request.into_parts();

        let core_request = into_core_request_parts(parts, body, None, client);
        assert!(
            !core_request
                .extensions()
                .get::<HttpClient>()
                .expect("supplied client")
                .proxy_marker_enabled()
        );
    }

    #[tokio::test]
    async fn missing_connect_info_is_handled_gracefully() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/demo")
            .body(AxumBody::empty())
            .expect("request");

        let core_request = into_core_request(request)
            .await
            .expect("request conversion");
        assert!(AxumRequestContext::get(&core_request).is_none());
    }

    #[tokio::test]
    async fn json_content_type_stays_streaming() {
        let json_payload = serde_json::json!({"name": "test"}).to_string();
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/test")
            .header("content-type", "application/json")
            .body(AxumBody::from(json_payload))
            .expect("request");

        let core_request = into_core_request(request)
            .await
            .expect("request conversion");
        assert_eq!(core_request.method(), &Method::POST);

        assert!(matches!(core_request.body(), Body::Stream(_)));
    }

    #[tokio::test]
    async fn non_json_content_type_streams_body() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/upload")
            .header("content-type", "application/octet-stream")
            .body(AxumBody::from("binary data"))
            .expect("request");

        let core_request = into_core_request(request)
            .await
            .expect("request conversion");

        assert!(matches!(core_request.body(), Body::Stream(_)));
    }
}
