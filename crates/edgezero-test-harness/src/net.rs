use crate::evidence::{Result, require};
pub mod overflow;
mod readiness;
pub use readiness::ReadinessOrigin;
mod wire;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
pub use wire::{WireOrigins, validate_forward_capture};

pub type Signal = Arc<(Mutex<bool>, Condvar)>;
static CLIENT_EVENTS: Mutex<Vec<Value>> = Mutex::new(Vec::new());
const MAX_HEAD: usize = 16 * 1024;
const MAX_TEXT: usize = 1024 * 1024;

struct BoundedStream {
    stream: TcpStream,
    port: u16,
    deadline: Instant,
}
impl BoundedStream {
    fn budget(&self) -> std::io::Result<Duration> {
        crate::startup::check(self.port).map_err(std::io::Error::other)?;
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "absolute response deadline expired",
            ));
        }
        Ok(remaining.min(Duration::from_millis(100)))
    }
}
impl Read for BoundedStream {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        loop {
            self.stream.set_read_timeout(Some(self.budget()?))?;
            match self.stream.read(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::Interrupted
                    ) => {}
                result => return result,
            }
        }
    }
}
impl Write for BoundedStream {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        loop {
            self.stream.set_write_timeout(Some(self.budget()?))?;
            match self.stream.write(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                            | std::io::ErrorKind::Interrupted
                    ) => {}
                result => return result,
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}

fn bounded_line(
    reader: &mut impl BufRead,
    line: &mut String,
    remaining: &mut usize,
) -> std::io::Result<usize> {
    let bytes = reader
        .take(remaining.saturating_add(1) as u64)
        .read_line(line)?;
    if bytes > *remaining {
        return Err(std::io::Error::other(
            "response headers/chunk metadata exceed 16 KiB",
        ));
    }
    *remaining -= bytes;
    Ok(bytes)
}
pub fn take_client_events() -> Vec<Value> {
    std::mem::take(&mut *CLIENT_EVENTS.lock().unwrap())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseInterruption {
    PrematureEof,
    PeerClosed,
    ReadIo(std::io::ErrorKind),
    InvalidFraming,
    CaptureLimit,
    ObserverRejected,
    MissingPrefix,
}

#[derive(Debug)]
struct CaptureFault {
    cause: ResponseInterruption,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for CaptureFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for CaptureFault {}

fn capture_require(valid: bool, cause: ResponseInterruption, message: &'static str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(Box::new(CaptureFault {
            cause,
            source: message.into(),
        }))
    }
}

fn interruption_cause(error: &(dyn std::error::Error + 'static)) -> ResponseInterruption {
    if let Some(fault) = error.downcast_ref::<CaptureFault>() {
        return fault.cause;
    }
    match error
        .downcast_ref::<std::io::Error>()
        .map(std::io::Error::kind)
    {
        Some(std::io::ErrorKind::UnexpectedEof) => ResponseInterruption::PrematureEof,
        Some(
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe,
        ) => ResponseInterruption::PeerClosed,
        Some(kind) => ResponseInterruption::ReadIo(kind),
        None => ResponseInterruption::InvalidFraming,
    }
}

#[derive(Debug)]
pub struct InterruptedResponse {
    pub status: u16,
    pub headers: Vec<Value>,
    pub body: Vec<u8>,
    pub reason: String,
    pub cause: ResponseInterruption,
}
impl std::fmt::Display for InterruptedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "response {} interrupted after {} bytes: {}",
            self.status,
            self.body.len(),
            self.reason
        )
    }
}
impl std::error::Error for InterruptedResponse {}

#[derive(Debug)]
pub struct ResponseHeadAborted(std::io::ErrorKind);
impl std::fmt::Display for ResponseHeadAborted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "response ended before its status: {:?}", self.0)
    }
}
impl std::error::Error for ResponseHeadAborted {}

fn read_status(reader: &mut impl BufRead) -> Result<u16> {
    let mut line = String::new();
    let mut remaining = MAX_HEAD;
    match bounded_line(reader, &mut line, &mut remaining) {
        Ok(0) => {
            return Err(Box::new(ResponseHeadAborted(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        Err(error)
            if line.is_empty()
                && matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::UnexpectedEof
                ) =>
        {
            return Err(Box::new(ResponseHeadAborted(error.kind())));
        }
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("missing HTTP response status")?
        .parse()?)
}

pub fn release(signal: &Signal) {
    *signal.0.lock().unwrap() = true;
    signal.1.notify_all();
}
pub struct Backend {
    pub port: u16,
    pub receipts: Arc<Mutex<Vec<String>>>,
    pub progress: Arc<Mutex<Vec<Value>>>,
    pub release: Signal,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Backend {
    pub fn start() -> Result<Self> {
        Self::start_with_progress_gate(false)
    }
    pub fn start_gated_progress() -> Result<Self> {
        Self::start_with_progress_gate(true)
    }
    fn start_with_progress_gate(gate_progress: bool) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let receipts = Arc::new(Mutex::new(Vec::new()));
        let progress = Arc::new(Mutex::new(Vec::new()));
        let origin_clock = Instant::now();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let barrier = Arc::new((Mutex::new(0usize), Condvar::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (r, p, s, b, done) = (
            receipts.clone(),
            progress.clone(),
            release.clone(),
            barrier.clone(),
            stop.clone(),
        );
        let worker = thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let (r, p, s, b) = (r.clone(), p.clone(), s.clone(), b.clone());
                        thread::spawn(move || {
                            let _ = serve(stream, r, p, origin_clock, s, b, gate_progress);
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            port,
            receipts,
            progress,
            release,
            stop,
            worker: Some(worker),
        })
    }
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        release(&self.release);
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn marker_response(stream: &mut TcpStream, method: &str, path: &str) -> Result<()> {
    let parts: Vec<_> = path
        .strip_prefix("/marker/")
        .ok_or("invalid marker path")?
        .split('/')
        .collect();
    let [
        expected_method @ ("get" | "head"),
        status @ ("200" | "204" | "304" | "302"),
        shape @ ("absent" | "upstream" | "nominated"),
    ] = parts.as_slice()
    else {
        return Err("invalid marker path".into());
    };
    require(
        method.eq_ignore_ascii_case(expected_method),
        "marker origin method mismatch",
    )?;
    write!(
        stream,
        "HTTP/1.1 {status} Probe\r\nContent-Length: 0\r\nConnection: close\r\n"
    )?;
    if *shape != "absent" {
        stream.write_all(b"x-edgezero-proxy: upstream\r\n")?;
    }
    if *shape == "nominated" {
        stream.write_all(b"Connection: x-edgezero-proxy\r\n")?;
    }
    stream.write_all(b"Location: /not-followed\r\n\r\n")?;
    Ok(())
}

fn serve(
    mut stream: TcpStream,
    receipts: Arc<Mutex<Vec<String>>>,
    progress: Arc<Mutex<Vec<Value>>>,
    origin_clock: Instant,
    signal: Signal,
    barrier: Arc<(Mutex<usize>, Condvar)>,
    gate_progress: bool,
) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let method = line
        .split_whitespace()
        .next()
        .ok_or("missing request method")?
        .to_owned();
    let path = line
        .split_whitespace()
        .nth(1)
        .ok_or("missing request path")?
        .to_owned();
    let mut request_token = None;
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("x-request-token")
        {
            request_token = Some(value.trim().to_owned());
        }
    }
    receipts.lock().unwrap().push(path.clone());
    if let Some(target) = path.strip_prefix("/p5/") {
        return serve_p5(
            &mut stream,
            target,
            &method,
            request_token.as_deref(),
            &signal,
            &progress,
        );
    }
    if path.starts_with("/marker/") {
        return marker_response(&mut stream, &method, &path);
    }
    if let Some(path) = path.strip_prefix("/coding/") {
        let (coding, status) = path.split_once('/').ok_or("invalid coding path")?;
        require(["200", "206"].contains(&status), "invalid coding status")?;
        return coding_response(&mut stream, coding, status.parse()?, &[]);
    }
    if path.starts_with("/g3/") {
        progress
            .lock()
            .unwrap()
            .push(json!({"path":path,"received_ns":origin_clock.elapsed().as_nanos()}));
        if path
            .rsplit('/')
            .next()
            .is_some_and(|s| s.starts_with("slot-"))
        {
            thread::sleep(Duration::from_millis(300));
            if gate_progress {
                let (released, _) = signal
                    .1
                    .wait_timeout_while(
                        signal.0.lock().unwrap(),
                        Duration::from_secs(3),
                        |released| !*released,
                    )
                    .unwrap();
                require(
                    *released,
                    "progress response not released by first client byte",
                )?;
            }
        }
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        return Ok(());
    }
    if path.starts_with("/barrier") {
        let mut count = barrier.0.lock().unwrap();
        *count += 1;
        barrier.1.notify_all();
        let (count, _) = barrier
            .1
            .wait_timeout_while(count, Duration::from_secs(8), |n| *n < 2)
            .map_err(|_| "barrier poisoned")?;
        if *count < 2 {
            stream.write_all(b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n")?;
            return Ok(());
        }
    }
    if path.starts_with("/chunks") {
        stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nfirst\r\n")?;
        stream.flush()?;
        let (ready, _) = signal
            .1
            .wait_timeout_while(signal.0.lock().unwrap(), Duration::from_secs(8), |v| !*v)
            .map_err(|_| "release poisoned")?;
        if *ready {
            if path.contains("large=1") {
                let body = vec![b'z'; 256 * 1024];
                write!(stream, "{:x}\r\n", body.len())?;
                stream.write_all(&body)?;
                stream.write_all(b"\r\n0\r\n\r\n")?;
            } else {
                stream.write_all(b"6\r\nsecond\r\n0\r\n\r\n")?;
            }
        }
    } else if path.starts_with("/abort") {
        stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial",
        )?;
        if path.contains("gated=1") {
            stream.flush()?;
            let (_observed, _) = signal
                .1
                .wait_timeout_while(signal.0.lock().unwrap(), Duration::from_secs(8), |v| !*v)
                .map_err(|_| "release poisoned")?;
        }
    } else {
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")?;
    }
    Ok(())
}

fn serve_p5(
    stream: &mut TcpStream,
    target: &str,
    method: &str,
    request_token: Option<&str>,
    signal: &Signal,
    progress: &Mutex<Vec<Value>>,
) -> Result<()> {
    let mut fields = target.splitn(3, '/');
    let token = fields.next().ok_or("missing P5 token")?;
    let port: u16 = fields.next().ok_or("missing P5 redirect port")?.parse()?;
    let case = fields.next().ok_or("missing P5 origin case")?;
    require(
        method == "GET" && request_token == Some(token) && port != 0,
        "foreign P5 origin request",
    )?;
    let redirect = matches!(case, "redirect" | "pending");
    write!(
        stream,
        "HTTP/1.1 {} Probe\r\nDate: Tue, 27 Oct 2026 00:00:00 GMT\r\nContent-Type: application/octet-stream\r\nCache-Control: no-store\r\nConnection: close\r\n",
        if redirect { 302 } else { 200 }
    )?;
    if redirect {
        write!(
            stream,
            "Location: http://127.0.0.1:{port}/p5/{token}/{port}/final\r\n"
        )?;
        if case == "pending" {
            stream.write_all(b"Content-Encoding: br\r\nContent-Length: 10485760\r\n\r\n")?;
            progress
                .lock()
                .unwrap()
                .push(json!({"event":"p5-pending-head","token":token}));
            stream.flush()?;
            let (released, _) = signal
                .1
                .wait_timeout_while(
                    signal.0.lock().unwrap(),
                    Duration::from_secs(3),
                    |released| !*released,
                )
                .map_err(|_| "P5 gate poisoned")?;
            require(*released, "P5 body gate expired before client completion")?;
        } else {
            stream.write_all(b"Content-Length: 0\r\n\r\n")?;
        }
        return Ok(());
    }
    let (coding, size) = if let Some(parity) = case.strip_prefix("parity/") {
        let fields: Vec<_> = parity.split('/').collect();
        let [
            coding @ ("identity" | "gzip" | "passthrough"),
            "buffered" | "streamed",
            size @ ("ten" | "eleven" | "declared"),
        ] = fields.as_slice()
        else {
            return Err("invalid P5 parity origin".into());
        };
        (*coding, *size)
    } else {
        require(matches!(case, "final" | "image"), "invalid P5 origin case")?;
        ("identity", "ten")
    };
    let body: &[u8] = match (coding, size) {
        ("gzip", "ten") => &[
            31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 51, 48, 52, 50, 54, 49, 53, 51, 183, 176, 4, 0, 198,
            199, 132, 166, 10, 0, 0, 0,
        ],
        ("gzip", _) => &[
            31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 51, 48, 52, 50, 54, 49, 53, 51, 183, 176, 172, 0, 0,
            193, 245, 125, 254, 11, 0, 0, 0,
        ],
        (_, "ten") => b"0123456789",
        _ => b"0123456789x",
    };
    match coding {
        "gzip" => stream.write_all(b"Content-Encoding: gzip\r\n")?,
        "passthrough" => stream.write_all(b"Content-Encoding: x-fixture-opaque\r\n")?,
        _ => {}
    }
    if size == "declared" || !case.starts_with("parity/") {
        write!(stream, "Content-Length: {}\r\n\r\n", body.len())?;
        stream.write_all(body)?;
    } else {
        write!(
            stream,
            "Transfer-Encoding: chunked\r\n\r\n{:x}\r\n",
            body.len()
        )?;
        stream.write_all(body)?;
        stream.write_all(b"\r\n0\r\n\r\n")?;
    }
    Ok(())
}

pub fn port() -> Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn coding_response(
    stream: &mut TcpStream,
    coding: &str,
    status: u16,
    extra: &[(&str, &str)],
) -> Result<()> {
    require([200, 206].contains(&status), "invalid coding status")?;
    let body = coding_body(coding)?;
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Encoding: {coding}\r\nContent-Length: {}\r\nConnection: close\r\n",
        if status == 206 {
            "Partial Content"
        } else {
            "OK"
        },
        body.len()
    )?;
    for (name, value) in coding_representation_headers(coding, status)? {
        write!(stream, "{name}: {value}\r\n")?;
    }
    for (name, value) in extra {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(body)?;
    Ok(())
}

pub fn coding_body(coding: &str) -> Result<&'static [u8]> {
    // Deterministic gzip, zlib and Brotli encodings of b"raw coding probe\n".
    match coding {
        "gzip" => Ok(&[
            31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 43, 74, 44, 87, 72, 206, 79, 201, 204, 75, 87, 40,
            40, 202, 79, 74, 229, 2, 0, 25, 107, 60, 67, 17, 0, 0, 0,
        ]),
        "deflate" => Ok(&[
            120, 156, 43, 74, 44, 87, 72, 206, 79, 201, 204, 75, 87, 40, 40, 202, 79, 74, 229, 2,
            0, 57, 147, 6, 33,
        ]),
        "br" => Ok(&[
            11, 8, 128, 114, 97, 119, 32, 99, 111, 100, 105, 110, 103, 32, 112, 114, 111, 98, 101,
            10, 3,
        ]),
        _ => Err("unknown coding fixture".into()),
    }
}

pub fn coding_representation_headers(
    coding: &str,
    status: u16,
) -> Result<Vec<(&'static str, String)>> {
    let digest = match coding {
        "gzip" => "BkzflHMq10MSCPDB7aAGrb3zFOrrCTOp44/5RdKVon4=",
        "deflate" => "8VYiRs57HGMrKxE4DDr7KClv+uCe8a9YdPsXzEiK3q0=",
        "br" => "nI5tLQUWd+A4M5qx6/4gCIB6dLeWM/EcfwAg0GqQnBQ=",
        _ => return Err("unknown coding fixture".into()),
    };
    let mut fields = vec![
        ("content-type", "application/octet-stream".to_owned()),
        ("etag", "\"coding-probe\"".to_owned()),
        ("last-modified", "Tue, 20 Oct 2026 00:00:00 GMT".to_owned()),
        ("vary", "Accept-Encoding".to_owned()),
        ("accept-ranges", "bytes".to_owned()),
        ("cache-control", "max-age=60".to_owned()),
        ("content-digest", format!("sha-256=:{digest}:")),
    ];
    if status == 206 {
        let length = coding_body(coding)?.len();
        fields.push(("content-range", format!("bytes 0-{}/{length}", length - 1)));
    }
    Ok(fields)
}

pub fn malformed_request(port: u16, token: &str) -> Result<String> {
    let budget = crate::startup::remaining(port, Duration::from_secs(5))?;
    let deadline = Instant::now() + budget;
    let stream =
        TcpStream::connect_timeout(&std::net::SocketAddr::from(([127, 0, 0, 1], port)), budget)?;
    let mut stream = BoundedStream {
        stream,
        port,
        deadline,
    };
    write!(
        stream,
        "POST /probe/malformed HTTP/1.1\r\nHost: localhost\r\nx-request-token: {token}\r\nContent-Length: invalid\r\nConnection: close\r\n\r\n"
    )?;
    let mut line = String::new();
    let mut remaining = MAX_HEAD;
    bounded_line(&mut BufReader::new(stream), &mut line, &mut remaining)?;
    Ok(line.trim().to_owned())
}
pub fn request(
    port: u16,
    path: &str,
    token: &str,
    backend: Option<&str>,
    signal: Option<&Signal>,
) -> Result<Value> {
    request_with_framing(
        port,
        path,
        token,
        backend,
        signal.map(BodyObserver::Signal),
        None,
        Capture::Text,
    )
}

#[derive(Clone, Copy)]
enum Capture<'a> {
    Text,
    Binary,
    Headers(&'a [(&'a str, &'a str)]),
    Forward {
        method: &'a str,
        framing: ForwardFraming,
        origin: &'a str,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum ForwardFraming {
    Absent,
    Zero,
    ChunkedEmpty,
    FiveBytes,
    ThreeBytes,
    Pending,
}

impl ForwardFraming {
    fn wire(self) -> (&'static str, &'static str) {
        match self {
            Self::Absent => ("", ""),
            Self::Zero => ("Content-Length: 0\r\n", ""),
            Self::ChunkedEmpty => ("Transfer-Encoding: chunked\r\n", "0\r\n\r\n"),
            Self::FiveBytes => ("Content-Length: 5\r\n", "abcde"),
            Self::ThreeBytes => ("Transfer-Encoding: chunked\r\n", "3\r\nabc\r\n0\r\n\r\n"),
            Self::Pending => ("Transfer-Encoding: chunked\r\n", ""),
        }
    }
}

pub fn forward_request(
    port: u16,
    case: &str,
    method: &str,
    framing: ForwardFraming,
    token: &str,
    origin: &str,
) -> Result<Value> {
    require(
        matches!(
            method,
            "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "OPTIONS" | "DELETE"
        ) && [case, token].iter().all(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        }) && origin.len() <= 256
            && origin.bytes().all(|byte| byte.is_ascii_graphic()),
        "invalid forwarding request",
    )?;
    request_with_framing(
        port,
        &format!("/progress/forward/{case}"),
        token,
        None,
        None,
        None,
        Capture::Forward {
            method,
            framing,
            origin,
        },
    )
}

pub fn binary_request(port: u16, path: &str, token: &str, backend: Option<&str>) -> Result<Value> {
    request_with_framing(port, path, token, backend, None, None, Capture::Binary)
}

type PrefixNotification<'a> = dyn Fn(u16, &[Value], &[u8]) -> Result<()> + 'a;

#[derive(Clone, Copy)]
pub struct PrefixObserver<'a> {
    pub bytes: usize,
    pub notify: &'a PrefixNotification<'a>,
}

#[derive(Clone, Copy)]
enum BodyObserver<'a> {
    Signal(&'a Signal),
    Prefix(PrefixObserver<'a>),
}

struct BodyObservation<'a>(Option<BodyObserver<'a>>);

impl BodyObserver<'_> {
    fn bytes(self) -> usize {
        match self {
            Self::Signal(_) => 1,
            Self::Prefix(prefix) => prefix.bytes,
        }
    }

    fn notify(self, status: u16, headers: &[Value], bytes: &[u8]) -> Result<()> {
        match self {
            Self::Signal(signal) => {
                release(signal);
                Ok(())
            }
            Self::Prefix(prefix) => (prefix.notify)(status, headers, &bytes[..prefix.bytes]),
        }
    }
}

impl BodyObservation<'_> {
    fn read_limit(&self, bytes: usize, capacity: usize) -> usize {
        self.0.map_or(capacity, |observer| {
            capacity.min(observer.bytes().saturating_sub(bytes))
        })
    }

    fn observe(&mut self, status: u16, headers: &[Value], bytes: &[u8]) -> Result<()> {
        if self
            .0
            .is_some_and(|observer| bytes.len() >= observer.bytes())
        {
            self.0
                .take()
                .unwrap()
                .notify(status, headers, bytes)
                .map_err(|source| {
                    Box::new(CaptureFault {
                        cause: ResponseInterruption::ObserverRejected,
                        source,
                    }) as Box<dyn std::error::Error + Send + Sync>
                })?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        capture_require(
            !matches!(self.0, Some(BodyObserver::Prefix(_))),
            ResponseInterruption::MissingPrefix,
            "response ended before observed prefix",
        )
    }
}

pub fn header_request(
    port: u16,
    path: &str,
    token: &str,
    headers: &[(&str, &str)],
    prefix: Option<PrefixObserver<'_>>,
) -> Result<Value> {
    require(
        prefix.is_none_or(|observer| (1..=512).contains(&observer.bytes)),
        "invalid fixture prefix bound",
    )?;
    require(
        path.starts_with('/')
            && path.len() <= 4096
            && path.bytes().all(|byte| byte.is_ascii_graphic())
            && !token.is_empty()
            && token.len() <= 128
            && token.bytes().all(|byte| byte.is_ascii_graphic()),
        "invalid fixture request path or token",
    )?;
    let mut size = 0usize;
    for (name, value) in headers {
        require(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
                && value.is_ascii()
                && value
                    .bytes()
                    .all(|byte| byte == b'\t' || (32..127).contains(&byte))
                && ![
                    "host",
                    "connection",
                    "content-length",
                    "transfer-encoding",
                    "x-request-token",
                ]
                .iter()
                .any(|reserved| name.eq_ignore_ascii_case(reserved)),
            "invalid fixture request header",
        )?;
        size = size
            .saturating_add(name.len())
            .saturating_add(value.len())
            .saturating_add(4);
        require(size <= 16 * 1024, "fixture request headers exceed limit")?;
    }
    request_with_framing(
        port,
        path,
        token,
        None,
        prefix.map(BodyObserver::Prefix),
        None,
        Capture::Headers(headers),
    )
}

#[derive(Clone, Copy, Debug)]
pub enum Framing {
    Absent,
    LengthZero,
    ChunkedEmpty,
    FixedBody,
    ChunkedBody,
}

impl Framing {
    fn wire(self) -> (&'static str, &'static str) {
        match self {
            Self::Absent => ("", ""),
            Self::LengthZero => ("Content-Length: 0\r\n", ""),
            Self::ChunkedEmpty => ("Transfer-Encoding: chunked\r\n", "0\r\n\r\n"),
            Self::FixedBody => ("Content-Length: 4\r\n", "body"),
            Self::ChunkedBody => ("Transfer-Encoding: chunked\r\n", "4\r\nbody\r\n0\r\n\r\n"),
        }
    }
}

pub fn framed_request(port: u16, framing: Framing) -> Result<Value> {
    request_with_framing(
        port,
        "/progress/empty",
        &crate::token(),
        None,
        None,
        Some(framing),
        Capture::Text,
    )
}

fn request_with_framing(
    port: u16,
    path: &str,
    token: &str,
    backend: Option<&str>,
    observer: Option<BodyObserver<'_>>,
    framing: Option<Framing>,
    capture: Capture<'_>,
) -> Result<Value> {
    let result = exchange(port, path, token, backend, observer, framing, capture);
    if let Err(error) = &result {
        let expected_fault = crate::startup::deferred(port, path)
            && (error.downcast_ref::<ResponseHeadAborted>().is_some()
                || error
                    .downcast_ref::<InterruptedResponse>()
                    .is_some_and(|partial| {
                        matches!(
                            partial.cause,
                            ResponseInterruption::PrematureEof | ResponseInterruption::PeerClosed
                        )
                    }));
        if !expected_fault {
            crate::startup::reject(port, error.to_string());
        }
    }
    result
}

fn exchange(
    port: u16,
    path: &str,
    token: &str,
    backend: Option<&str>,
    observer: Option<BodyObserver<'_>>,
    framing: Option<Framing>,
    capture: Capture<'_>,
) -> Result<Value> {
    let start = Instant::now();
    let budget = crate::startup::remaining(port, Duration::from_secs(12))?;
    let deadline = start + budget;
    let stream =
        TcpStream::connect_timeout(&std::net::SocketAddr::from(([127, 0, 0, 1], port)), budget)?;
    let mut stream = BoundedStream {
        stream,
        port,
        deadline,
    };
    let probe = path.starts_with("/probe/");
    let body = if probe { token } else { "" };
    let framing_header = framing.map_or_else(
        || format!("Content-Length: {}\r\n", body.len()),
        |f| f.wire().0.to_owned(),
    );
    let body = framing.map_or(body, |f| f.wire().1);
    let method = match capture {
        Capture::Forward { method, .. } => method,
        _ if probe => "POST",
        _ => "GET",
    };
    let (framing_header, body) = match capture {
        Capture::Forward { framing, .. } => (framing.wire().0.to_owned(), framing.wire().1),
        _ => (framing_header, body),
    };
    write!(
        stream,
        "{} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nx-request-token: {token}\r\n{framing_header}Connection: close\r\n",
        method,
    )?;
    if let Some(url) = backend {
        write!(stream, "x-fixture-backend: {url}\r\n")?;
    }
    if let Capture::Headers(headers) = capture {
        for (name, value) in headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
    }
    if let Capture::Forward { origin, .. } = capture {
        write!(stream, "x-origin: {origin}\r\n")?;
    }
    write!(stream, "\r\n{body}")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let status = read_status(&mut reader)?;
    let mut line = String::new();
    let mut headers = Vec::new();
    let mut length = None;
    let mut chunked = false;
    let mut head_remaining = MAX_HEAD;
    loop {
        line.clear();
        require(
            bounded_line(&mut reader, &mut line, &mut head_remaining)? > 0,
            "truncated HTTP headers",
        )?;
        if line == "\r\n" {
            break;
        }
        let (name, value) = line
            .trim_end()
            .split_once(':')
            .ok_or("malformed HTTP header")?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            require(length.is_none(), "duplicate response Content-Length")?;
            length = Some(value.parse::<usize>()?);
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            require(
                !chunked && value.eq_ignore_ascii_case("chunked"),
                "unsupported or duplicate response Transfer-Encoding",
            )?;
            chunked = true;
        }
        headers.push(json!([name, value]));
    }
    require(!(chunked && length.is_some()), "ambiguous response framing")?;
    crate::startup::validate_head(port, path, &headers)?;
    let header_ns = start.elapsed().as_nanos() as u64;
    let mut first_byte_ns = None;
    let mut observation = BodyObservation(observer);
    let mut bytes = Vec::new();
    let capture_limit = match capture {
        Capture::Text => MAX_TEXT,
        Capture::Binary => 256,
        Capture::Headers(_) | Capture::Forward { .. } => 64 * 1024,
    };
    let outcome = (|| -> Result<()> {
        let mut read_part = |reader: &mut BufReader<BoundedStream>, count: usize| -> Result<()> {
            capture_require(
                count <= capture_limit.saturating_sub(bytes.len()),
                ResponseInterruption::CaptureLimit,
                "response exceeds fixture capture limit",
            )?;
            if count == 0 {
                return Ok(());
            }
            let mut first = [0];
            reader.read_exact(&mut first)?;
            bytes.push(first[0]);
            if first_byte_ns.is_none() {
                first_byte_ns = Some(start.elapsed().as_nanos() as u64);
            }
            observation.observe(status, &headers, &bytes)?;
            let mut remaining = count - 1;
            let mut buffer = [0u8; 8192];
            while remaining > 0 {
                let size = observation.read_limit(bytes.len(), remaining.min(buffer.len()));
                let read = reader.read(&mut buffer[..size])?;
                capture_require(
                    read > 0,
                    ResponseInterruption::PrematureEof,
                    "truncated HTTP body",
                )?;
                bytes.extend_from_slice(&buffer[..read]);
                remaining -= read;
                observation.observe(status, &headers, &bytes)?;
            }
            Ok(())
        };
        if method == "HEAD" {
            let mut extra = [0];
            require(
                reader.read(&mut extra)? == 0,
                "HEAD response carried body bytes",
            )?;
        } else if chunked {
            loop {
                line.clear();
                capture_require(
                    bounded_line(&mut reader, &mut line, &mut head_remaining)? > 0,
                    ResponseInterruption::PrematureEof,
                    "truncated chunk header",
                )?;
                require(line.ends_with("\r\n"), "invalid chunk header terminator")?;
                let count = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)?;
                if count == 0 {
                    loop {
                        line.clear();
                        capture_require(
                            bounded_line(&mut reader, &mut line, &mut head_remaining)? > 0,
                            ResponseInterruption::PrematureEof,
                            "truncated chunk trailer",
                        )?;
                        require(line.ends_with("\r\n"), "invalid chunk trailer terminator")?;
                        if line == "\r\n" {
                            break;
                        }
                    }
                    break;
                }
                read_part(&mut reader, count)?;
                for expected in *b"\r\n" {
                    let mut byte = [0];
                    reader.read_exact(&mut byte)?;
                    require(byte[0] == expected, "invalid chunk terminator")?;
                }
            }
        } else if let Some(length) = length {
            read_part(&mut reader, length)?;
        } else {
            let mut first = [0];
            if reader.read(&mut first)? != 0 {
                bytes.push(first[0]);
                first_byte_ns = Some(start.elapsed().as_nanos() as u64);
                observation.observe(status, &headers, &bytes)?;
                let mut buffer = [0u8; 8192];
                loop {
                    let capacity = capture_limit
                        .saturating_sub(bytes.len())
                        .saturating_add(1)
                        .min(buffer.len());
                    let capacity = observation.read_limit(bytes.len(), capacity);
                    let read = reader.read(&mut buffer[..capacity])?;
                    if read == 0 {
                        break;
                    }
                    capture_require(
                        read <= capture_limit.saturating_sub(bytes.len()),
                        ResponseInterruption::CaptureLimit,
                        "response exceeds fixture capture limit",
                    )?;
                    bytes.extend_from_slice(&buffer[..read]);
                    observation.observe(status, &headers, &bytes)?;
                }
            }
        }
        observation.finish()
    })();
    if let Err(error) = outcome {
        return Err(Box::new(InterruptedResponse {
            status,
            headers,
            body: bytes,
            reason: error.to_string(),
            cause: interruption_cause(error.as_ref()),
        }));
    }
    require(
        Instant::now() < deadline,
        "absolute response deadline expired",
    )?;
    crate::startup::observe(port, path, &headers)?;
    let mut event = json!({"event":"client_completed","token":token,"status":status,"headers":headers,"header_ns":header_ns,"first_byte_ns":first_byte_ns.unwrap_or(header_ns),"complete_ns":start.elapsed().as_nanos() as u64});
    match capture {
        Capture::Text => event["body"] = json!(String::from_utf8(bytes)?),
        Capture::Binary | Capture::Headers(_) | Capture::Forward { .. } => {
            event["body_bytes"] = json!(bytes)
        }
    }
    crate::startup::annotate_response(port, &mut event);
    CLIENT_EVENTS.lock().unwrap().push(event.clone());
    Ok(event)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarding_client_preserves_original_method_and_explicit_framing() {
        for (method, framing) in [
            ("GET", ForwardFraming::Absent),
            ("HEAD", ForwardFraming::Zero),
            ("GET", ForwardFraming::ChunkedEmpty),
            ("GET", ForwardFraming::FiveBytes),
            ("POST", ForwardFraming::ThreeBytes),
            ("PUT", ForwardFraming::ThreeBytes),
            ("PATCH", ForwardFraming::ThreeBytes),
            ("OPTIONS", ForwardFraming::Absent),
            ("DELETE", ForwardFraming::Absent),
            ("GET", ForwardFraming::Pending),
        ] {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let origin = "http://127.0.0.1:1234/forward/send/wire";
            let expected = format!(
                "{method} /progress/forward/send HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nx-request-token: wire\r\n{}Connection: close\r\nx-origin: {origin}\r\n\r\n{}",
                framing.wire().0,
                framing.wire().1
            );
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut received = vec![0; expected.len()];
                socket.read_exact(&mut received).unwrap();
                assert_eq!(received, expected.as_bytes());
                socket
                    .write_all(
                        b"HTTP/1.1 201 Created\r\nContent-Length: 3\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                if method != "HEAD" {
                    socket.write_all(b"end").unwrap();
                }
            });
            let response = forward_request(port, "send", method, framing, "wire", origin).unwrap();
            server.join().unwrap();
            assert_eq!(response["status"], 201);
            assert_eq!(
                response["body_bytes"],
                json!(if method == "HEAD" {
                    b"".as_slice()
                } else {
                    b"end".as_slice()
                })
            );
        }
    }

    #[test]
    fn forwarding_client_rejects_injection_before_connecting() {
        for (case, method, token, origin) in [
            ("send", "GET\r\nx: injected", "wire", "http://127.0.0.1:1/"),
            ("send", "GET", "wire\r\nx: injected", "http://127.0.0.1:1/"),
            ("send/elsewhere", "GET", "wire", "http://127.0.0.1:1/"),
            ("send", "GET", "wire", "http://127.0.0.1:1/\r\nx: injected"),
        ] {
            assert_eq!(
                forward_request(0, case, method, ForwardFraming::Absent, token, origin)
                    .unwrap_err()
                    .to_string(),
                "invalid forwarding request"
            );
        }
    }

    fn binary_reply(wire: Vec<u8>) -> Result<Value> {
        captured_reply(wire, |port| {
            binary_request(port, "/relay", "binary-test", None)
        })
    }

    fn captured_reply(wire: Vec<u8>, request: impl FnOnce(u16) -> Result<Value>) -> Result<Value> {
        served_reply(move |socket| socket.write_all(&wire).unwrap(), request)
    }

    fn served_reply(
        serve: impl FnOnce(&mut TcpStream) + Send + 'static,
        request: impl FnOnce(u16) -> Result<Value>,
    ) -> Result<Value> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            serve(&mut socket);
        });
        let result = request(port);
        server.join().unwrap();
        result
    }

    #[test]
    fn startup_transport_deadline_does_not_restart_when_the_peer_trickles() {
        let started = Instant::now();
        let result = served_reply(
            |socket| {
                for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" {
                    if socket.write_all(&[*byte]).is_err() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            },
            |port| {
                let stream = TcpStream::connect(("127.0.0.1", port))?;
                let mut stream = BoundedStream {
                    stream,
                    port,
                    deadline: Instant::now() + Duration::from_millis(50),
                };
                stream.write_all(b"GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
                read_status(&mut BufReader::new(stream)).map(|status| json!({"status":status}))
            },
        );
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn startup_transport_rejects_an_oversized_header_section() {
        let wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nx-padding: {}\r\n\r\n",
            "x".repeat(16 * 1024)
        );
        let response = served_reply(
            move |socket| {
                let _ = socket.write_all(wire.as_bytes());
            },
            |port| request(port, "/probe/first", "first-prescribed-request", None, None),
        );
        assert!(
            response.is_err(),
            "startup transport accepted more than 16 KiB of response headers"
        );
    }

    #[test]
    fn startup_transport_rejects_an_oversized_text_body() {
        let body = "x".repeat(1024 * 1024 + 1);
        let wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let response = served_reply(
            move |socket| {
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let _ = socket.write_all(wire.as_bytes());
            },
            |port| request(port, "/probe/first", "first-prescribed-request", None, None),
        );
        assert!(
            response.is_err(),
            "startup transport accepted more than 1 MiB of response text"
        );
    }

    #[test]
    fn header_prefix_observer_receives_the_validated_head_and_exact_prefix_once() {
        use std::cell::Cell;
        let prelude = format!("{}\n", json!({"phase":"pending"}));
        let tail = format!("{}\n", json!({"done":true}));
        for framing in ["fixed", "chunked", "close"] {
            let body = format!("{prelude}{tail}");
            let head = match framing {
                "fixed" => format!("Content-Length: {}\r\n", body.len()),
                "chunked" => "Transfer-Encoding: chunked\r\n".to_owned(),
                _ => String::new(),
            };
            let payload = if framing == "chunked" {
                format!("{:X}\r\n{body}\r\n0\r\n\r\n", body.len())
            } else {
                body.clone()
            };
            let wire = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n{head}\r\n{payload}"
            )
            .into_bytes();
            let observed = Cell::new(0);
            let notify = |status, headers: &[Value], bytes: &[u8]| {
                assert_eq!(status, 200);
                assert!(headers.contains(&json!(["Content-Type", "application/x-ndjson"])));
                assert_eq!(bytes, prelude.as_bytes());
                observed.set(observed.get() + 1);
                Ok(())
            };
            let response = captured_reply(wire, |port| {
                header_request(
                    port,
                    "/p5",
                    "prefix",
                    &[],
                    Some(PrefixObserver {
                        bytes: prelude.len(),
                        notify: &notify,
                    }),
                )
            })
            .unwrap();
            assert_eq!(observed.get(), 1, "missing {framing} prefix witness");
            assert_eq!(response["body_bytes"], json!(body.into_bytes()));
        }
    }

    #[test]
    fn prefix_observation_releases_a_gated_tail_without_waiting_for_the_http_chunk() {
        use std::{cell::Cell, sync::mpsc};
        let prelude = format!("{}\n", json!({"phase":"pending"}));
        let tail = format!("{}\n", json!({"done":true}));
        for framing in ["fixed", "chunked", "split-chunks", "close"] {
            let body = format!("{prelude}{tail}");
            let (notify_tail, wait_for_prefix) = mpsc::sync_channel(1);
            let observed = Cell::new(0);
            let notify = |_, _: &[Value], bytes: &[u8]| {
                assert_eq!(bytes, prelude.as_bytes());
                observed.set(observed.get() + 1);
                notify_tail.send(()).unwrap();
                Ok(())
            };
            let prefix_bytes = prelude.clone();
            let tail_bytes = tail.clone();
            let body_len = body.len();
            let result = served_reply(
                move |socket| {
                    let head = match framing {
                        "fixed" => format!("Content-Length: {body_len}\r\n"),
                        "chunked" | "split-chunks" => "Transfer-Encoding: chunked\r\n".into(),
                        _ => String::new(),
                    };
                    write!(socket, "HTTP/1.1 200 OK\r\n{head}\r\n").unwrap();
                    let split = prefix_bytes.len() / 2;
                    if framing == "chunked" {
                        write!(socket, "{body_len:X}\r\n").unwrap();
                    } else if framing == "split-chunks" {
                        write!(socket, "{split:X}\r\n").unwrap();
                    }
                    socket.write_all(&prefix_bytes.as_bytes()[..split]).unwrap();
                    // The incomplete prefix must not open the gate.
                    assert!(matches!(
                        wait_for_prefix.try_recv(),
                        Err(mpsc::TryRecvError::Empty)
                    ));
                    if framing == "split-chunks" {
                        write!(socket, "\r\n{:X}\r\n", body_len - split).unwrap();
                    }
                    socket.write_all(&prefix_bytes.as_bytes()[split..]).unwrap();
                    wait_for_prefix
                        .recv_timeout(Duration::from_secs(2))
                        .unwrap();
                    socket.write_all(tail_bytes.as_bytes()).unwrap();
                    if matches!(framing, "chunked" | "split-chunks") {
                        socket.write_all(b"\r\n0\r\n\r\n").unwrap();
                    }
                },
                |port| {
                    header_request(
                        port,
                        "/p5",
                        "gated-prefix",
                        &[],
                        Some(PrefixObserver {
                            bytes: prelude.len(),
                            notify: &notify,
                        }),
                    )
                },
            )
            .unwrap();
            assert_eq!(observed.get(), 1, "{framing}");
            assert_eq!(result["body_bytes"], json!(body.into_bytes()));
        }
    }

    #[test]
    fn prefix_bounds_are_checked_before_connect_and_short_bodies_never_notify() {
        use std::cell::Cell;
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let observed = Cell::new(0);
        let notify = |_, _: &[Value], _: &[u8]| {
            observed.set(observed.get() + 1);
            Ok(())
        };
        for bytes in [0, 513] {
            assert!(
                header_request(
                    port,
                    "/p5",
                    "bound",
                    &[],
                    Some(PrefixObserver {
                        bytes,
                        notify: &notify
                    })
                )
                .is_err()
            );
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
        for body in [
            "Content-Length: 0\r\n\r\n",
            "Content-Length: 2\r\n\r\nx",
            "Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n",
            "\r\nx",
        ] {
            let error = captured_reply(format!("HTTP/1.1 200 OK\r\n{body}").into_bytes(), |port| {
                header_request(
                    port,
                    "/p5",
                    "short",
                    &[],
                    Some(PrefixObserver {
                        bytes: 2,
                        notify: &notify,
                    }),
                )
            })
            .unwrap_err();
            assert!(error.downcast_ref::<InterruptedResponse>().is_some());
        }
        assert_eq!(observed.get(), 0);
        let result = captured_reply(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: 512\r\n\r\n{}",
                "x".repeat(512)
            )
            .into_bytes(),
            |port| {
                header_request(
                    port,
                    "/p5",
                    "exact",
                    &[],
                    Some(PrefixObserver {
                        bytes: 512,
                        notify: &notify,
                    }),
                )
            },
        )
        .unwrap();
        assert_eq!(result["body_bytes"].as_array().unwrap().len(), 512);
        assert_eq!(observed.get(), 1);
    }

    #[test]
    fn rejected_prefix_stops_capture_without_reading_the_tail() {
        use std::cell::Cell;
        let observed = Cell::new(0);
        let notify = |_, _: &[Value], bytes: &[u8]| -> Result<()> {
            assert_eq!(bytes, b"abc");
            observed.set(observed.get() + 1);
            Err("rejected prefix".into())
        };
        let error = captured_reply(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef".to_vec(),
            |port| {
                header_request(
                    port,
                    "/p5",
                    "reject",
                    &[],
                    Some(PrefixObserver {
                        bytes: 3,
                        notify: &notify,
                    }),
                )
            },
        )
        .unwrap_err();
        let partial = error.downcast_ref::<InterruptedResponse>().unwrap();
        assert_eq!(partial.body, b"abc");
        assert_eq!(partial.reason, "rejected prefix");
        assert_eq!(partial.cause, ResponseInterruption::ObserverRejected);
        assert_eq!(observed.get(), 1);
    }

    #[test]
    fn ambiguous_response_framing_cannot_notify_the_prefix_observer() {
        use std::cell::Cell;
        let notified = Cell::new(0);
        let notify = |_, _: &[Value], _: &[u8]| {
            notified.set(notified.get() + 1);
            Ok(())
        };
        for framing in [
            "Content-Length: 1\r\nContent-Length: 1",
            "Content-Length: 2\r\nContent-Length: 1",
            "Content-Length: 1\r\nTransfer-Encoding: chunked",
            "Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked",
            "Transfer-Encoding: not-chunked",
            "Transfer-Encoding: gzip, chunked",
        ] {
            let wire =
                format!("HTTP/1.1 200 OK\r\n{framing}\r\n\r\n1\r\nx\r\n0\r\n\r\n").into_bytes();
            assert!(
                captured_reply(wire, |port| header_request(
                    port,
                    "/p5",
                    "ambiguous",
                    &[],
                    Some(PrefixObserver {
                        bytes: 1,
                        notify: &notify
                    }),
                ))
                .is_err(),
                "{framing}"
            );
        }
        assert_eq!(notified.get(), 0);
    }

    #[test]
    fn header_request_preserves_custom_and_duplicate_headers_and_binary_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let worker = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut head = String::new();
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                head.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\n\x00\x80\xff").unwrap();
            head
        });
        let response = header_request(
            port,
            "/p5",
            "header-token",
            &[
                ("x-origin", "http://127.0.0.1:1234/p5/final"),
                ("x-duplicate", "first"),
                ("x-duplicate", "second"),
            ],
            None,
        )
        .unwrap();
        let head = worker.join().unwrap();
        assert!(head.starts_with("GET /p5 HTTP/1.1\r\n"));
        assert!(head.contains("x-request-token: header-token\r\n"));
        assert!(head.contains("x-origin: http://127.0.0.1:1234/p5/final\r\n"));
        assert!(head.contains("x-duplicate: first\r\nx-duplicate: second\r\n"));
        assert_eq!(response["body_bytes"], json!([0, 128, 255]));
    }

    #[test]
    fn header_request_caps_all_response_framings_at_sixty_four_kib() {
        for size in [65_536, 65_537] {
            for framing in ["fixed", "chunked", "close"] {
                let field = match framing {
                    "fixed" => format!("Content-Length: {size}\r\n"),
                    "chunked" => "Transfer-Encoding: chunked\r\n".to_owned(),
                    _ => String::new(),
                };
                let mut wire =
                    format!("HTTP/1.1 200 OK\r\n{field}Connection: close\r\n\r\n").into_bytes();
                if framing == "chunked" {
                    wire.extend(format!("{size:X}\r\n").as_bytes());
                }
                wire.extend(std::iter::repeat_n(255, size));
                if framing == "chunked" {
                    wire.extend(b"\r\n0\r\n\r\n");
                }
                let result =
                    captured_reply(wire, |port| header_request(port, "/p5", "cap", &[], None));
                if size == 65_536 {
                    assert_eq!(result.unwrap()["body_bytes"], json!(vec![255; size]));
                } else {
                    let error = result.unwrap_err();
                    let partial = error.downcast_ref::<InterruptedResponse>().unwrap();
                    assert!(partial.reason.contains("capture limit"));
                    assert_eq!(partial.cause, ResponseInterruption::CaptureLimit);
                    assert!(partial.body.len() <= 65_536);
                }
            }
        }
    }

    #[test]
    fn header_request_rejects_injection_before_connecting() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        for (path, token, headers) in [
            ("/p5\r\nx-injected: yes", "t", vec![]),
            ("/p5", "t\r\nx-injected: yes", vec![]),
            ("/p5", "t", vec![("x-name\r\n", "value")]),
            ("/p5", "t", vec![("x-name", "value\r\nx-injected: yes")]),
            ("/p5", "t", vec![("bad name", "value")]),
            ("/p5", "t", vec![("x-name", "bad\0value")]),
        ] {
            assert!(header_request(port, path, token, &headers, None).is_err());
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }

    #[test]
    fn binary_capture_preserves_encoded_bytes_without_utf8_or_decoding() {
        let backend = Backend::start().unwrap();
        for coding in ["gzip", "deflate", "br"] {
            let response = binary_request(
                backend.port,
                &format!("/coding/{coding}/200"),
                "coding",
                None,
            )
            .unwrap();
            assert_eq!(response["status"], 200);
            assert_eq!(response["body_bytes"], json!(coding_body(coding).unwrap()));
            assert!(response["body"].is_null());
        }
    }

    #[test]
    fn binary_capture_bounds_fixed_chunked_and_close_delimited_payloads() {
        for size in [256, 257] {
            for framing in ["fixed", "chunked", "close"] {
                let header = match framing {
                    "fixed" => format!("Content-Length: {size}\r\n"),
                    "chunked" => "Transfer-Encoding: chunked\r\n".to_owned(),
                    _ => String::new(),
                };
                let mut wire =
                    format!("HTTP/1.1 200 OK\r\n{header}Connection: close\r\n\r\n").into_bytes();
                if framing == "chunked" {
                    wire.extend(format!("{size:x}\r\n").as_bytes());
                }
                wire.extend(std::iter::repeat_n(255, size));
                if framing == "chunked" {
                    wire.extend(b"\r\n0\r\n\r\n");
                }
                let result = binary_reply(wire);
                if size == 256 {
                    assert_eq!(
                        result.unwrap()["body_bytes"],
                        json!(vec![255; size]),
                        "{framing}"
                    );
                } else {
                    let error = result.unwrap_err();
                    let partial = error.downcast_ref::<InterruptedResponse>().unwrap();
                    assert!(
                        partial.reason.contains("capture limit"),
                        "{framing}: {partial}"
                    );
                    assert!(partial.body.len() <= 256);
                }
            }
        }
    }

    #[test]
    fn p5_origin_has_deterministic_body_and_cross_port_redirect() {
        let origin = Backend::start().unwrap();
        let reply =
            binary_request(origin.port, "/p5/p5-origin/8002/final", "p5-origin", None).unwrap();
        assert_eq!(reply["body_bytes"], json!(b"0123456789"));
        let redirect = binary_request(
            origin.port,
            "/p5/p5-origin/8002/redirect",
            "p5-origin",
            None,
        )
        .unwrap();
        assert_eq!(redirect["status"], 302);
        assert!(redirect["headers"].as_array().unwrap().iter().any(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("location"))
                && field[1] == "http://127.0.0.1:8002/p5/p5-origin/8002/final"
        }));
        assert_eq!(origin.receipts.lock().unwrap().len(), 2);
    }

    #[test]
    fn p5_passthrough_origin_uses_an_unsupported_coding_token() {
        let origin = Backend::start().unwrap();
        let reply = binary_request(
            origin.port,
            "/p5/p5-origin/8002/parity/passthrough/streamed/ten",
            "p5-origin",
            None,
        )
        .unwrap();
        assert_eq!(reply["body_bytes"], json!(b"0123456789"));
        assert!(reply["headers"].as_array().unwrap().iter().any(|field| {
            field[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("content-encoding"))
                && field[1] == "x-fixture-opaque"
        }));
    }

    #[test]
    fn binary_capture_preserves_truncation_as_failure() {
        for framing in [
            "Content-Length: 5\r\n\r\nabc",
            "Transfer-Encoding: chunked\r\n\r\n5\r\nabc",
        ] {
            let wire = format!("HTTP/1.1 200 OK\r\n{framing}").into_bytes();
            let error = binary_reply(wire).unwrap_err();
            let partial = error.downcast_ref::<InterruptedResponse>().unwrap();
            assert_eq!(partial.status, 200);
            assert_eq!(partial.body, b"abc");
        }
    }

    #[test]
    fn interrupted_response_distinguishes_peer_end_from_framing_and_read_timeouts() {
        for (wire, expected) in [
            (
                "Content-Length: 5\r\n\r\nabc",
                ResponseInterruption::PrematureEof,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n",
                ResponseInterruption::PrematureEof,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\nz\r\n",
                ResponseInterruption::InvalidFraming,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\nabcX",
                ResponseInterruption::InvalidFraming,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r",
                ResponseInterruption::PrematureEof,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\nabc\r\n",
                ResponseInterruption::InvalidFraming,
            ),
            (
                "Transfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nx\n",
                ResponseInterruption::InvalidFraming,
            ),
        ] {
            let error = binary_reply(
                format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{wire}").into_bytes(),
            )
            .unwrap_err();
            let partial = error.downcast_ref::<InterruptedResponse>().unwrap();
            assert_eq!(partial.cause, expected);
            assert!(
                partial
                    .headers
                    .contains(&json!(["Content-Type", "application/json"]))
            );
        }
        for (kind, expected) in [
            (
                std::io::ErrorKind::UnexpectedEof,
                ResponseInterruption::PrematureEof,
            ),
            (
                std::io::ErrorKind::ConnectionReset,
                ResponseInterruption::PeerClosed,
            ),
            (
                std::io::ErrorKind::TimedOut,
                ResponseInterruption::ReadIo(std::io::ErrorKind::TimedOut),
            ),
            (
                std::io::ErrorKind::WouldBlock,
                ResponseInterruption::ReadIo(std::io::ErrorKind::WouldBlock),
            ),
        ] {
            assert_eq!(interruption_cause(&std::io::Error::from(kind)), expected);
        }
    }

    #[test]
    fn progress_response_gate_waits_for_client_release() {
        let backend = Backend::start_gated_progress().unwrap();
        let mut stream = TcpStream::connect(("127.0.0.1", backend.port)).unwrap();
        stream
            .write_all(
                b"GET /g3/gated/slot-0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        thread::sleep(Duration::from_millis(350));
        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let mut first = [0];
        assert!(stream.read(&mut first).is_err());
        release(&backend.release);
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
    }
    #[test]
    fn empty_body_framing_cases_preserve_wire_distinctions() {
        assert_eq!(Framing::Absent.wire(), ("", ""));
        assert_eq!(Framing::LengthZero.wire(), ("Content-Length: 0\r\n", ""));
        assert_eq!(
            Framing::ChunkedEmpty.wire(),
            ("Transfer-Encoding: chunked\r\n", "0\r\n\r\n")
        );
        assert_eq!(Framing::FixedBody.wire().1, "body");
        assert_eq!(Framing::ChunkedBody.wire().1, "4\r\nbody\r\n0\r\n\r\n");
    }

    #[test]
    fn progress_origin_records_distinct_receipts_before_delayed_completion() {
        let backend = Backend::start().unwrap();
        request(backend.port, "/g3/test/start", "test", None, None).unwrap();
        request(backend.port, "/g3/test/slot-0", "test", None, None).unwrap();
        let receipts = backend.progress.lock().unwrap().clone();
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0]["path"], "/g3/test/start");
        assert_eq!(receipts[1]["path"], "/g3/test/slot-0");
        assert!(
            receipts[0]["received_ns"].as_u64().unwrap()
                <= receipts[1]["received_ns"].as_u64().unwrap()
        );
    }

    #[test]
    fn no_response_head_classification_excludes_timeouts_and_malformed_bytes() {
        struct FailedRead(std::io::ErrorKind);
        impl Read for FailedRead {
            fn read(&mut self, _buffer: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(self.0))
            }
        }
        let eof = read_status(&mut std::io::Cursor::new(b"")).unwrap_err();
        assert!(eof.downcast_ref::<ResponseHeadAborted>().is_some());
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::TimedOut,
        ] {
            let error = read_status(&mut BufReader::new(FailedRead(kind))).unwrap_err();
            assert_eq!(
                error.downcast_ref::<ResponseHeadAborted>().is_some(),
                kind == std::io::ErrorKind::ConnectionReset
            );
        }
        let invalid = read_status(&mut std::io::Cursor::new(b"invalid\r\n")).unwrap_err();
        assert!(invalid.downcast_ref::<ResponseHeadAborted>().is_none());
        assert_eq!(
            read_status(&mut std::io::Cursor::new(b"HTTP/1.1 500 Error\r\n")).unwrap(),
            500
        );
    }

    #[test]
    fn nonblocking_connection_waits_for_request_bytes() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let worker = thread::spawn(move || {
            serve(
                server,
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Mutex::new(Vec::new())),
                Instant::now(),
                Arc::new((Mutex::new(false), Condvar::new())),
                Arc::new((Mutex::new(0), Condvar::new())),
                false,
            )
        });
        thread::sleep(Duration::from_millis(100));
        client
            .write_all(b"GET /ok HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut reply = String::new();
        client.read_to_string(&mut reply).unwrap();
        worker.join().unwrap().unwrap();
        assert!(reply.starts_with("HTTP/1.1 200"));
    }
    #[test]
    fn backend_readiness_paths_have_no_special_response() {
        let backend = Backend::start().unwrap();
        for path in ["/readiness/truncated", "/readiness/truncated/0"] {
            let response = request(backend.port, path, "generic", None, None).unwrap();
            assert_eq!(response["status"], 200);
            assert_eq!(response["body"], "ok");
        }
    }

    #[test]
    fn progressive_body_and_abort() {
        let backend = Backend::start().unwrap();
        let result = request(backend.port, "/chunks", "x", None, Some(&backend.release)).unwrap();
        assert_eq!(result["body"], "firstsecond");
        let failure = request(backend.port, "/abort", "x", None, None).unwrap_err();
        let partial = failure
            .downcast_ref::<InterruptedResponse>()
            .expect("response started");
        assert_eq!(partial.status, 200);
        assert_eq!(partial.body, b"partial");
    }

    #[test]
    fn gated_abort_waits_for_client_observation_before_closing() {
        let backend = Backend::start().unwrap();
        let mut stream = TcpStream::connect(("127.0.0.1", backend.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        stream
            .write_all(b"GET /abort?gated=1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut reader = BufReader::new(stream);
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
        }
        let mut partial = [0; 7];
        reader.read_exact(&mut partial).unwrap();
        assert_eq!(&partial, b"partial");
        let before_release = reader.read(&mut [0]);
        release(&backend.release);
        assert!(matches!(
            before_release,
            Err(error) if matches!(error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
        ));
        assert_eq!(reader.read(&mut [0]).unwrap(), 0);
    }

    #[test]
    fn connection_failure_is_not_an_interrupted_response() {
        let unused = port().unwrap();
        let failure = request(unused, "/abort", "x", None, None).unwrap_err();
        assert!(failure.downcast_ref::<InterruptedResponse>().is_none());
    }
}
