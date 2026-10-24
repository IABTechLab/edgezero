use crate::evidence::{Result, require};
pub mod overflow;
mod readiness;
pub use readiness::ReadinessOrigin;
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

pub type Signal = Arc<(Mutex<bool>, Condvar)>;
static CLIENT_EVENTS: Mutex<Vec<Value>> = Mutex::new(Vec::new());
pub fn take_client_events() -> Vec<Value> {
    std::mem::take(&mut *CLIENT_EVENTS.lock().unwrap())
}

#[derive(Debug)]
pub struct InterruptedResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub reason: String,
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
    match reader.read_line(&mut line) {
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
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
    }
    receipts.lock().unwrap().push(path.clone());
    if let Some(path) = path.strip_prefix("/marker/") {
        let parts: Vec<_> = path.split('/').collect();
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
        return Ok(());
    }
    if let Some(path) = path.strip_prefix("/coding/") {
        let (coding, status) = path.split_once('/').ok_or("invalid coding path")?;
        require(["200", "206"].contains(&status), "invalid coding status")?;
        let body = coding_body(coding)?;
        write!(
            stream,
            "HTTP/1.1 {status} {}\r\nContent-Encoding: {coding}\r\nContent-Length: {}\r\nConnection: close\r\n",
            if status == "206" {
                "Partial Content"
            } else {
                "OK"
            },
            body.len()
        )?;
        for (name, value) in coding_representation_headers(coding, status.parse()?)? {
            write!(stream, "{name}: {value}\r\n")?;
        }
        stream.write_all(b"\r\n")?;
        stream.write_all(body)?;
        return Ok(());
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
pub fn port() -> Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
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
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "POST /probe/malformed HTTP/1.1\r\nHost: localhost\r\nx-request-token: {token}\r\nContent-Length: invalid\r\nConnection: close\r\n\r\n"
    )?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    Ok(line.trim().to_owned())
}
pub fn request(
    port: u16,
    path: &str,
    token: &str,
    backend: Option<&str>,
    signal: Option<&Signal>,
) -> Result<Value> {
    request_with_framing(port, path, token, backend, signal, None, Capture::Text)
}

#[derive(Clone, Copy)]
enum Capture {
    Text,
    Binary,
}

pub fn binary_request(port: u16, path: &str, token: &str, backend: Option<&str>) -> Result<Value> {
    request_with_framing(port, path, token, backend, None, None, Capture::Binary)
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
    signal: Option<&Signal>,
    framing: Option<Framing>,
    capture: Capture,
) -> Result<Value> {
    let start = Instant::now();
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(12)))?;
    stream.set_write_timeout(Some(Duration::from_secs(12)))?;
    let probe = path.starts_with("/probe/");
    let body = if probe { token } else { "" };
    let framing_header = framing.map_or_else(
        || format!("Content-Length: {}\r\n", body.len()),
        |f| f.wire().0.to_owned(),
    );
    let body = framing.map_or(body, |f| f.wire().1);
    write!(
        stream,
        "{} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nx-request-token: {token}\r\n{framing_header}Connection: close\r\n",
        if probe { "POST" } else { "GET" },
    )?;
    if let Some(url) = backend {
        write!(stream, "x-fixture-backend: {url}\r\n")?;
    }
    write!(stream, "\r\n{body}")?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let status = read_status(&mut reader)?;
    let mut line = String::new();
    let mut headers = Vec::new();
    let mut length = None;
    let mut chunked = false;
    loop {
        line.clear();
        require(reader.read_line(&mut line)? > 0, "truncated HTTP headers")?;
        if line == "\r\n" {
            break;
        }
        let (name, value) = line
            .trim_end()
            .split_once(':')
            .ok_or("malformed HTTP header")?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            length = Some(value.parse::<usize>()?);
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value.to_ascii_lowercase().contains("chunked");
        }
        headers.push(json!([name, value]));
    }
    let header_ns = start.elapsed().as_nanos() as u64;
    let mut first_byte_ns = None;
    let mut bytes = Vec::new();
    let capture_limit = match capture {
        Capture::Text => usize::MAX,
        Capture::Binary => 256,
    };
    let outcome = (|| -> Result<()> {
        let mut read_part = |reader: &mut BufReader<TcpStream>, count: usize| -> Result<()> {
            require(
                count <= capture_limit.saturating_sub(bytes.len()),
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
                if let Some(signal) = signal {
                    release(signal);
                }
            }
            let mut remaining = count - 1;
            let mut buffer = [0u8; 8192];
            while remaining > 0 {
                let size = remaining.min(buffer.len());
                let read = reader.read(&mut buffer[..size])?;
                require(read > 0, "truncated HTTP body")?;
                bytes.extend_from_slice(&buffer[..read]);
                remaining -= read;
            }
            Ok(())
        };
        if chunked {
            loop {
                line.clear();
                require(reader.read_line(&mut line)? > 0, "truncated chunk header")?;
                let count = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)?;
                if count == 0 {
                    loop {
                        line.clear();
                        require(reader.read_line(&mut line)? > 0, "truncated chunk trailer")?;
                        if line == "\r\n" {
                            break;
                        }
                    }
                    break;
                }
                read_part(&mut reader, count)?;
                let mut crlf = [0; 2];
                reader.read_exact(&mut crlf)?;
                require(crlf == *b"\r\n", "invalid chunk terminator")?;
            }
        } else if let Some(length) = length {
            read_part(&mut reader, length)?;
        } else {
            let mut first = [0];
            if reader.read(&mut first)? != 0 {
                bytes.push(first[0]);
                first_byte_ns = Some(start.elapsed().as_nanos() as u64);
                if let Some(signal) = signal {
                    release(signal);
                }
                let mut buffer = [0u8; 8192];
                loop {
                    let capacity = capture_limit
                        .saturating_sub(bytes.len())
                        .saturating_add(1)
                        .min(buffer.len());
                    let read = reader.read(&mut buffer[..capacity])?;
                    if read == 0 {
                        break;
                    }
                    require(
                        read <= capture_limit.saturating_sub(bytes.len()),
                        "response exceeds fixture capture limit",
                    )?;
                    bytes.extend_from_slice(&buffer[..read]);
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = outcome {
        return Err(Box::new(InterruptedResponse {
            status,
            body: bytes,
            reason: error.to_string(),
        }));
    }
    let mut event = json!({"event":"client_completed","token":token,"status":status,"headers":headers,"header_ns":header_ns,"first_byte_ns":first_byte_ns.unwrap_or(header_ns),"complete_ns":start.elapsed().as_nanos() as u64});
    match capture {
        Capture::Text => event["body"] = json!(String::from_utf8(bytes)?),
        Capture::Binary => event["body_bytes"] = json!(bytes),
    }
    CLIENT_EVENTS.lock().unwrap().push(event.clone());
    Ok(event)
}
#[cfg(test)]
mod tests {
    use super::*;

    fn binary_reply(wire: Vec<u8>) -> Result<Value> {
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
            socket.write_all(&wire).unwrap();
        });
        let result = binary_request(port, "/relay", "binary-test", None);
        server.join().unwrap();
        result
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
