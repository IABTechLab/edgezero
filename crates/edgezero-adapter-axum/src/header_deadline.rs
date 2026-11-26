use std::error::Error as StdError;
use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use edgezero_core::context::DEFAULT_INBOUND_JSON_BYTES;
use edgezero_core::http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
use edgezero_core::http::{HeaderMap, HeaderName, HeaderValue};
use hyper::Request;
use hyper::body::Incoming;
use hyper::ext::ReasonPhrase;
use hyper::rt::{Sleep, Timer};
use hyper::service::Service;
use hyper_util::rt::TokioTimer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::{Instant as TokioInstant, Sleep as TokioSleep, sleep_until};

use crate::ingress_config::AxumIngressConfig;
use crate::response::EgressConnection;

/// Largest declared request body a closing HTTP/1 connection drains after an early response.
///
/// A response sent before its request body is read, such as a 413 for a declared
/// `Content-Length` over a reader's cap, closes the connection with the upload unread. The
/// kernel then answers the client's remaining bytes with a reset, which can fail the client's
/// upload before it reads the response. Half-closing and draining first lets a client that writes
/// its whole body before reading still receive the response (RFC 9112 section 9.6). The drain runs
/// only while the upload is active, with bytes waiting or read within [`LINGERING_CLOSE_IDLE`],
/// so an idle or stalled client releases its connection at once. The bound equals the core's default JSON extractor limit,
/// [`DEFAULT_INBOUND_JSON_BYTES`]. A larger declared body, a chunked upload of unknown length and
/// a request without a body close at once as before.
const LINGERING_CLOSE_MAX_BYTES: usize = DEFAULT_INBOUND_JSON_BYTES;
/// Longest time a closing connection spends draining a declared upload.
///
/// This bounds how long a rejected upload keeps its connection task alive. Draining the full
/// [`LINGERING_CLOSE_MAX_BYTES`] within it needs about 34 Mbit/s; a slower client is cut off and
/// closes as before.
const LINGERING_CLOSE_MAX_DURATION: Duration = Duration::from_secs(2);
/// The drain ends once the client sends nothing for this long.
const LINGERING_CLOSE_IDLE: Duration = Duration::from_millis(250);
/// Scratch space for each drained read; drained bytes are discarded.
const LINGERING_CLOSE_READ_BYTES: usize = 8 * 1024;
/// Shares Hyper's head-read phase with the connection's independent timer race.
/// A successful service call disarms it; parser-error writes retain the deadline.
#[derive(Clone)]
pub(crate) struct HeaderDeadline {
    changed: Arc<Notify>,
    deadline: Arc<Mutex<Option<TokioInstant>>>,
    /// Declared body length of the latest request that a closing connection drains, or zero.
    upload: Arc<AtomicUsize>,
}

#[derive(Debug, thiserror::Error)]
#[error("HTTP/1 request head deadline exceeded")]
pub(crate) struct HeaderDeadlineExceeded;

pub(crate) struct HeaderIo {
    deadline: HeaderDeadline,
    /// When Hyper last read request bytes, which shows whether an upload is still active.
    last_read: Option<TokioInstant>,
    linger: Linger,
    stream: TcpStream,
}

/// Shutdown progress of [`HeaderIo`]: half-close, then drain a declared upload under its bounds.
enum Linger {
    Closed,
    Draining {
        idle: Pin<Box<TokioSleep>>,
        remaining: usize,
        until: TokioInstant,
    },
    Open,
}

pub(crate) struct HeaderService<S> {
    deadline: HeaderDeadline,
    limits: AxumIngressConfig,
    responses: EgressConnection,
    service: S,
}

#[derive(Debug, thiserror::Error)]
#[error("HTTP/1 response head exceeds adapter parser storage limits")]
struct ResponseHeadExceeded;

impl HeaderDeadline {
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }

    fn check_io(&self) -> io::Result<()> {
        if self
            .deadline()
            .is_some_and(|limit| TokioInstant::now() >= limit)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                HeaderDeadlineExceeded,
            ));
        }
        Ok(())
    }

    pub(crate) fn deadline(&self) -> Option<TokioInstant> {
        *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn finish_head(&self, now: TokioInstant) -> Result<(), HeaderDeadlineExceeded> {
        let mut deadline = self.deadline.lock().unwrap_or_else(PoisonError::into_inner);
        if deadline.is_some_and(|limit| now >= limit) {
            return Err(HeaderDeadlineExceeded);
        }
        *deadline = None;
        self.changed.notify_one();
        Ok(())
    }

    pub(crate) fn new(timeout: Duration) -> Self {
        let now = TokioInstant::now();
        Self {
            changed: Arc::new(Notify::new()),
            deadline: Arc::new(Mutex::new(Some(now.checked_add(timeout).unwrap_or(now)))),
            upload: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn record_upload(&self, headers: &HeaderMap) {
        self.upload
            .store(lingering_upload(headers), Ordering::Relaxed);
    }
}

impl Timer for HeaderDeadline {
    fn now(&self) -> Instant {
        TokioInstant::now().into_std()
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        *sleep = self.sleep_until(new_deadline);
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        let now = self.now();
        self.sleep_until(now.checked_add(duration).unwrap_or(now))
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        let requested = TokioInstant::from_std(deadline);
        let mut state = self.deadline.lock().unwrap_or_else(PoisonError::into_inner);
        let effective = state.map_or(requested, |existing| existing.min(requested));
        *state = Some(effective);
        self.changed.notify_one();
        TokioTimer::new().sleep_until(effective.into_std())
    }
}

impl HeaderIo {
    pub(crate) fn new(stream: TcpStream, deadline: HeaderDeadline) -> Self {
        Self {
            deadline,
            last_read: None,
            linger: Linger::Open,
            stream,
        }
    }
}

impl AsyncRead for HeaderIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.deadline.check_io()?;
        let before = buf.filled().len();
        let read = Pin::new(&mut this.stream).poll_read(cx, buf);
        if matches!(read, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.last_read = Some(TokioInstant::now());
        }
        read
    }
}

impl AsyncWrite for HeaderIo {
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.deadline.check_io()?;
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.linger {
                Linger::Open => {
                    ready!(Pin::new(&mut this.stream).poll_shutdown(cx))?;
                    this.linger = Linger::Closed;
                    let declared = this.deadline.upload.load(Ordering::Relaxed);
                    if declared == 0 {
                        continue;
                    }
                    // Drain only an active upload: bytes already waiting, or a read within the
                    // idle window. A stalled client releases the connection without a timer.
                    let mut scratch = [0_u8; LINGERING_CLOSE_READ_BYTES];
                    let mut drained = ReadBuf::new(&mut scratch);
                    let probe = Pin::new(&mut this.stream).poll_read(cx, &mut drained);
                    let now = TokioInstant::now();
                    let active_since = match probe {
                        Poll::Ready(Ok(())) if !drained.filled().is_empty() => Some(now),
                        Poll::Pending => this.last_read.filter(|&read| {
                            read.checked_add(LINGERING_CLOSE_IDLE)
                                .is_some_and(|idle| idle > now)
                        }),
                        Poll::Ready(_) => None,
                    };
                    let remaining = declared.saturating_sub(drained.filled().len());
                    if let Some(since) = active_since
                        && remaining > 0
                    {
                        let until = now.checked_add(LINGERING_CLOSE_MAX_DURATION).unwrap_or(now);
                        this.linger = Linger::Draining {
                            idle: Box::pin(sleep_until(lingering_idle_deadline(since, until))),
                            remaining,
                            until,
                        };
                    }
                }
                Linger::Draining {
                    idle,
                    remaining,
                    until,
                } => {
                    let mut scratch = [0_u8; LINGERING_CLOSE_READ_BYTES];
                    let mut drained = ReadBuf::new(&mut scratch);
                    match Pin::new(&mut this.stream).poll_read(cx, &mut drained) {
                        Poll::Ready(Ok(())) if !drained.filled().is_empty() => {
                            *remaining = remaining.saturating_sub(drained.filled().len());
                            let now = TokioInstant::now();
                            if *remaining == 0 || now >= *until {
                                this.linger = Linger::Closed;
                            } else {
                                idle.as_mut().reset(lingering_idle_deadline(now, *until));
                            }
                        }
                        // EOF or a read error ends the drain.
                        Poll::Ready(_) => this.linger = Linger::Closed,
                        Poll::Pending => {
                            if idle.as_mut().poll(cx).is_pending() {
                                return Poll::Pending;
                            }
                            this.linger = Linger::Closed;
                        }
                    }
                }
                Linger::Closed => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.deadline.check_io()?;
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.deadline.check_io()?;
        Pin::new(&mut this.stream).poll_write_vectored(cx, bufs)
    }
}

impl<S> HeaderService<S> {
    pub(crate) fn new(
        service: S,
        deadline: HeaderDeadline,
        limits: AxumIngressConfig,
        responses: EgressConnection,
    ) -> Self {
        Self {
            deadline,
            limits,
            responses,
            service,
        }
    }
}

impl<S, B> Service<Request<Incoming>> for HeaderService<S>
where
    S: Service<Request<Incoming>, Response = hyper::Response<B>>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::Future: 'static,
    B: 'static,
{
    type Error = Box<dyn StdError + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;
    type Response = hyper::Response<B>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        if let Err(error) = self.deadline.finish_head(TokioInstant::now()) {
            let boxed_error: Self::Error = Box::new(error);
            return Box::pin(async move { Err(boxed_error) });
        }
        self.deadline.record_upload(req.headers());
        let future = self.service.call(req);
        let limits = self.limits;
        let responses = self.responses.clone();
        Box::pin(async move {
            let mut response = future.await.map_err(Into::into)?;
            if let Err(error) = bound_response_head(&mut response, limits) {
                responses.signal_transport_error();
                let boxed_error: Self::Error = Box::new(error);
                return Err(boxed_error);
            }
            Ok(response)
        })
    }
}

fn bound_response_head<B>(
    response: &mut hyper::Response<B>,
    limits: AxumIngressConfig,
) -> Result<(), ResponseHeadExceeded> {
    let count = response.headers().len();
    let bytes = response
        .headers()
        .iter()
        .try_fold(0_usize, |charged, (name, value)| {
            charged
                .checked_add(name.as_str().len())?
                .checked_add(value.as_bytes().len())?
                .checked_add(4)
        });
    if count > limits.max_response_header_count()
        || bytes.is_none_or(|total| total > limits.max_response_header_bytes())
    {
        return Err(ResponseHeadExceeded);
    }
    // Discard application-supplied spare capacity before Hyper caches this map.
    let mut compact = HeaderMap::with_capacity(count);
    for (name, value) in response.headers() {
        let copied_name = HeaderName::from_bytes(name.as_str().as_bytes())
            .map_err(|_invalid_name| ResponseHeadExceeded)?;
        let mut copied_value = HeaderValue::from_bytes(value.as_bytes())
            .map_err(|_invalid_value| ResponseHeadExceeded)?;
        copied_value.set_sensitive(value.is_sensitive());
        compact.append(copied_name, copied_value);
    }
    *response.headers_mut() = compact;
    // Only canonical status phrases participate in the bounded encoded head.
    response.extensions_mut().remove::<ReasonPhrase>();
    Ok(())
}

fn lingering_idle_deadline(now: TokioInstant, until: TokioInstant) -> TokioInstant {
    now.checked_add(LINGERING_CLOSE_IDLE)
        .map_or(until, |idle| idle.min(until))
}
/// Returns the declared body length a closing connection drains, or zero to close at once.
///
/// Only a single `Content-Length` from 1 byte to [`LINGERING_CLOSE_MAX_BYTES`] qualifies.
fn lingering_upload(headers: &HeaderMap) -> usize {
    if headers.contains_key(TRANSFER_ENCODING) {
        return 0;
    }
    let mut lengths = headers.get_all(CONTENT_LENGTH).iter();
    let (Some(length), None) = (lengths.next(), lengths.next()) else {
        return 0;
    };
    length
        .to_str()
        .ok()
        .and_then(|text| text.trim().parse::<usize>().ok())
        .filter(|declared| (1..=LINGERING_CLOSE_MAX_BYTES).contains(declared))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use edgezero_core::http::{HeaderMap, HeaderValue};
    use hyper::ext::ReasonPhrase;
    use hyper::rt::Timer as _;
    use tokio::time::{Instant, advance};

    use super::{HeaderDeadline, LINGERING_CLOSE_MAX_BYTES, bound_response_head, lingering_upload};
    use crate::ingress_config::AxumIngressConfig;

    #[test]
    fn response_head_compacts_capacity_preserves_duplicates_and_rejects_overflow() {
        let limits = AxumIngressConfig::default();
        let mut response = hyper::Response::new(());
        *response.headers_mut() = HeaderMap::with_capacity(0x4000);
        response
            .headers_mut()
            .append("set-cookie", HeaderValue::from_static("a=1"));
        let mut sensitive = HeaderValue::from_static("b=2");
        sensitive.set_sensitive(true);
        response.headers_mut().append("set-cookie", sensitive);
        response
            .extensions_mut()
            .insert(ReasonPhrase::from_static(b"custom"));
        bound_response_head(&mut response, limits).unwrap();
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
        assert!(response.headers().capacity() < 100);
        assert!(
            response
                .headers()
                .get_all("set-cookie")
                .iter()
                .nth(1)
                .unwrap()
                .is_sensitive()
        );
        assert!(response.extensions().get::<ReasonPhrase>().is_none());
        let oversized = HeaderValue::from_bytes(&vec![b'a'; limits.max_raw_head_bytes()]).unwrap();
        response.headers_mut().insert("x-large", oversized);
        bound_response_head(&mut response, limits).unwrap_err();
        response.headers_mut().clear();
        for _ in 0..=limits.max_response_header_count() {
            response
                .headers_mut()
                .append("x-dup", HeaderValue::from_static("a"));
        }
        bound_response_head(&mut response, limits).unwrap_err();
    }

    #[tokio::test(start_paused = true)]
    async fn rearmed_header_timer_remains_in_tokio_clock_domain() {
        let deadline = HeaderDeadline::new(Duration::from_secs(10));
        deadline.finish_head(Instant::now()).unwrap();
        advance(Duration::from_secs(20)).await;
        drop(deadline.sleep(Duration::from_secs(10)));
        assert_eq!(
            deadline
                .deadline()
                .unwrap()
                .checked_duration_since(Instant::now()),
            Some(Duration::from_secs(10))
        );
    }

    #[tokio::test]
    async fn first_arm_cannot_extend_accept_deadline_and_next_head_gets_new_budget() {
        let timer = HeaderDeadline::new(Duration::from_millis(10));
        let accepted_deadline = timer.deadline().unwrap();
        let _sleep = timer.sleep_until((Instant::now() + Duration::from_secs(1)).into_std());
        assert_eq!(timer.deadline(), Some(accepted_deadline));
        timer
            .finish_head(accepted_deadline - Duration::from_millis(1))
            .unwrap();
        assert_eq!(timer.deadline(), None);
        let next = Instant::now() + Duration::from_secs(1);
        let _next_sleep = timer.sleep_until(next.into_std());
        assert_eq!(timer.deadline(), Some(next));
        assert!(timer.finish_head(next).is_err());
    }

    #[test]
    fn lingering_close_drains_only_single_declared_lengths_within_the_bound() {
        let at_bound = LINGERING_CLOSE_MAX_BYTES.to_string();
        let over_bound = LINGERING_CLOSE_MAX_BYTES
            .checked_add(1)
            .expect("bound fits usize")
            .to_string();
        let cases: [(&[(&str, &str)], usize); 8] = [
            (&[], 0),
            (&[("content-length", "0")], 0),
            (&[("content-length", "1")], 1),
            (
                &[("content-length", at_bound.as_str())],
                LINGERING_CLOSE_MAX_BYTES,
            ),
            (&[("content-length", over_bound.as_str())], 0),
            (&[("transfer-encoding", "chunked")], 0),
            (&[("content-length", "4"), ("content-length", "4")], 0),
            (
                &[("content-length", "4"), ("transfer-encoding", "chunked")],
                0,
            ),
        ];
        for (fields, expected) in cases {
            let mut headers = HeaderMap::new();
            for &(name, value) in fields {
                headers.append(name, HeaderValue::from_str(value).expect("valid field"));
            }
            assert_eq!(lingering_upload(&headers), expected, "{fields:?}");
        }
    }

    #[tokio::test]
    async fn early_response_lets_a_client_finish_its_declared_upload_and_read_it() {
        use std::convert::Infallible;

        use axum::body::Body;
        use futures::future::join;
        use hyper::service::service_fn;
        use hyper::{Response, StatusCode};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::{TcpListener, TcpStream};
        use tokio::time::timeout;

        use crate::connection::{ConnectionExit, serve_http1};
        use crate::response::EgressConnection;

        // Larger than the loopback socket buffers, so the client blocks mid-upload.
        let declared = 4 * 1024 * 1024;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = async move {
            let (socket, _) = listener.accept().await.unwrap();
            // Answers before polling the body, as a declared-length 413 does.
            let service = service_fn(|_request| async {
                let mut response = Response::new(Body::from("payload too large"));
                *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                Ok::<_, Infallible>(response)
            });
            serve_http1(
                socket,
                service,
                EgressConnection::default(),
                AxumIngressConfig::default(),
            )
            .await
        };
        let client = async {
            let mut stream = TcpStream::connect(address).await.unwrap();
            let head =
                format!("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {declared}\r\n\r\n");
            stream.write_all(head.as_bytes()).await.unwrap();
            // Write the whole declared body before reading anything.
            stream
                .write_all(&vec![b'x'; declared])
                .await
                .expect("upload completes instead of a reset");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .expect("response is readable after the upload");
            String::from_utf8(response).unwrap()
        };
        let (exit, response) = timeout(Duration::from_secs(5), join(server, client))
            .await
            .expect("bounded exchange");
        assert!(response.starts_with("HTTP/1.1 413 "), "{response}");
        assert!(response.ends_with("payload too large"), "{response}");
        assert_eq!(exit.unwrap(), ConnectionExit::Completed);
    }
}
