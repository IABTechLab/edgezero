//! Bounded HTTP/1 evidence collection for synthetic upload probes only.

use crate::evidence::{Result, require};
use serde_json::{Value, json};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const METADATA_BYTES: usize = 16 * 1024;
const ORIGIN_BODY_BYTES: usize = 256;
const SOCKET_BUDGET: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
enum Failure {
    Eof,
    Reset,
    Timeout,
    Io,
    Invalid,
    MetadataLimit,
    CaptureLimit,
}

impl Failure {
    fn name(self) -> &'static str {
        match self {
            Self::Eof => "eof",
            Self::Reset => "reset",
            Self::Timeout => "timeout",
            Self::Io => "io_error",
            Self::Invalid => "invalid_framing",
            Self::MetadataLimit => "metadata_limit",
            Self::CaptureLimit => "capture_limit",
        }
    }
}

impl From<io::Error> for Failure {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::UnexpectedEof => Self::Eof,
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted => Self::Reset,
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => Self::Timeout,
            _ => Self::Io,
        }
    }
}

struct DeadlineSocket {
    socket: TcpStream,
    deadline: Instant,
}
impl Read for DeadlineSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(io::ErrorKind::TimedOut)?;
        self.socket.set_read_timeout(Some(remaining))?;
        self.socket.read(buffer)
    }
}

struct Head {
    first: String,
    fields: Vec<(String, String)>,
}

fn line(reader: &mut impl BufRead, budget: &mut usize) -> std::result::Result<String, Failure> {
    let mut bytes = Vec::new();
    let read = (&mut *reader)
        .take(METADATA_BYTES.saturating_sub(*budget).saturating_add(1) as u64)
        .read_until(b'\n', &mut bytes);
    let count = bytes.len();
    *budget += count;
    if *budget > METADATA_BYTES {
        return Err(Failure::MetadataLimit);
    }
    if let Err(error) = read {
        return Err(if count == 0 {
            error.into()
        } else {
            Failure::Invalid
        });
    }
    if count == 0 {
        return Err(Failure::Eof);
    }
    if !bytes.ends_with(b"\r\n") {
        return Err(Failure::Invalid);
    }
    bytes.truncate(bytes.len() - 2);
    String::from_utf8(bytes).map_err(|_| Failure::Invalid)
}

fn field_parts(field: &str) -> std::result::Result<(&str, &str), Failure> {
    let (name, value) = field.split_once(':').ok_or(Failure::Invalid)?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        || value
            .bytes()
            .any(|byte| (byte < 32 && byte != b'\t') || byte == 127)
    {
        return Err(Failure::Invalid);
    }
    Ok((name, value.trim_matches([' ', '\t'])))
}

fn read_head(reader: &mut impl BufRead, budget: &mut usize) -> std::result::Result<Head, Failure> {
    let first = line(reader, budget)?;
    let mut fields = Vec::new();
    loop {
        let field = line(reader, budget)?;
        if field.is_empty() {
            return Ok(Head { first, fields });
        }
        if fields.len() >= 100 {
            return Err(Failure::MetadataLimit);
        }
        let (name, value) = field_parts(&field)?;
        fields.push((name.to_ascii_lowercase(), value.to_owned()));
    }
}

fn framing(head: &Head) -> std::result::Result<(Option<u64>, bool), Failure> {
    let mut length = None;
    let mut transfers = Vec::new();
    for (name, value) in &head.fields {
        if name == "content-length" {
            for part in value.split(',').map(str::trim) {
                if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(Failure::Invalid);
                }
                let parsed = part.parse::<u64>().map_err(|_| Failure::Invalid)?;
                if length.is_some_and(|previous| previous != parsed) {
                    return Err(Failure::Invalid);
                }
                length = Some(parsed);
            }
        } else if name == "transfer-encoding" {
            transfers.push(value.as_str());
        }
    }
    if !transfers.is_empty()
        && (length.is_some()
            || transfers.len() != 1
            || !transfers[0].eq_ignore_ascii_case("chunked"))
    {
        return Err(Failure::Invalid);
    }
    Ok((length, !transfers.is_empty()))
}

fn amount(
    reader: &mut impl Read,
    mut count: u64,
    body: &mut Vec<u8>,
    cap: usize,
) -> std::result::Result<(), Failure> {
    let mut buffer = [0_u8; 256];
    while count > 0 {
        let room = cap.saturating_add(1).saturating_sub(body.len());
        if room == 0 {
            return Err(Failure::CaptureLimit);
        }
        let size = usize::try_from(count)
            .unwrap_or(usize::MAX)
            .min(buffer.len())
            .min(room);
        let read = reader.read(&mut buffer[..size])?;
        if read == 0 {
            return Err(Failure::Eof);
        }
        body.extend_from_slice(&buffer[..read]);
        if body.len() > cap {
            return Err(Failure::CaptureLimit);
        }
        count -= read as u64;
    }
    Ok(())
}

fn payload(
    reader: &mut impl BufRead,
    head: &Head,
    budget: &mut usize,
    body: &mut Vec<u8>,
    cap: usize,
    response: bool,
) -> std::result::Result<(), Failure> {
    let (length, chunked) = framing(head)?;
    if chunked {
        loop {
            let size = line(reader, budget)?;
            // Extensions are unnecessary for the controlled uploads: fail closed rather than guess.
            if size.is_empty() || !size.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(Failure::Invalid);
            }
            let count = u64::from_str_radix(&size, 16).map_err(|_| Failure::Invalid)?;
            if count == 0 {
                loop {
                    let trailer = line(reader, budget)?;
                    if trailer.is_empty() {
                        return Ok(());
                    }
                    field_parts(&trailer)?;
                }
            }
            amount(reader, count, body, cap)?;
            for expected in b"\r\n" {
                let mut byte = [0];
                reader.read_exact(&mut byte)?;
                if byte[0] != *expected {
                    return Err(Failure::Invalid);
                }
            }
        }
    }
    if let Some(length) = length {
        return amount(reader, length, body, cap);
    }
    if response {
        loop {
            let mut byte = [0];
            if reader.read(&mut byte)? == 0 {
                return Ok(());
            }
            body.push(byte[0]);
            if body.len() > cap {
                return Err(Failure::CaptureLimit);
            }
        }
    }
    Ok(())
}

pub(crate) fn inspect(reader: &mut impl BufRead) -> Value {
    let mut budget = 0;
    let mut body = Vec::new();
    let mut receipt = json!({"head":null,"head_bytes":0,"head_state":"unobserved"});
    let result = (|| {
        let head = match read_head(reader, &mut budget) {
            Ok(head) => head,
            Err(error) => {
                let seen = budget;
                receipt["head_bytes"] = json!(seen);
                receipt["head_state"] = json!(if seen == 0 { "absent" } else { "partial" });
                return Err(error);
            }
        };
        receipt["head_bytes"] = json!(budget);
        receipt["head_state"] = json!("complete");
        let parts: Vec<_> = head.first.split_whitespace().collect();
        if parts.len() != 3 || parts[0] != "POST" || !matches!(parts[2], "HTTP/1.0" | "HTTP/1.1") {
            return Err(Failure::Invalid);
        }
        receipt["head"] =
            json!({"method":parts[0],"target":parts[1],"version":parts[2],"headers":head.fields});
        payload(
            reader,
            &head,
            &mut budget,
            &mut body,
            ORIGIN_BODY_BYTES,
            false,
        )
    })();
    receipt["terminal"] = json!(result.err().map_or("complete", Failure::name));
    receipt["body_bytes"] = json!(body.len());
    receipt["body_prefix"] = json!(body);
    receipt
}

pub struct UploadOrigin {
    pub port: u16,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<Result<Value>>>,
}

impl UploadOrigin {
    pub fn start() -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let observed_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let mut accepted = 0_usize;
            let mut receipts = Vec::new();
            loop {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        accepted += 1;
                        if accepted > 2 {
                            break;
                        }
                        socket.set_nonblocking(false)?;
                        let deadline = Instant::now() + SOCKET_BUDGET;
                        let mut reader = BufReader::new(DeadlineSocket {
                            socket: socket.try_clone()?,
                            deadline,
                        });
                        let receipt = inspect(&mut reader);
                        let complete = receipt["terminal"] == "complete";
                        let upstream_413 = receipt["head"]["target"]
                            .as_str()
                            .is_some_and(|target| target.ends_with("/upstream-413"));
                        receipts.push(receipt);
                        if complete {
                            let wire = if upstream_413 {
                                "HTTP/1.1 413 Payload Too Large\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            } else {
                                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            };
                            // Framing completion is recorded first, even if the guest abandoned the reply.
                            let wrote = deadline
                                .checked_duration_since(Instant::now())
                                .filter(|remaining| !remaining.is_zero())
                                .is_some_and(|remaining| {
                                    socket
                                        .set_write_timeout(Some(remaining))
                                        .and_then(|()| socket.write_all(wire.as_bytes()))
                                        .is_ok()
                                });
                            receipts.last_mut().unwrap()["reply_written"] = json!(wrote);
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        if observed_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(
                json!({"accepted":accepted,"connections":receipts,"overflow":accepted > 2,
                "collection_complete":true,"origin_deadline_ms":SOCKET_BUDGET.as_millis(),"capture_bytes":ORIGIN_BODY_BYTES}),
            )
        });
        Ok(Self {
            port,
            stop,
            worker: Some(worker),
        })
    }

    pub fn url(&self, token: &str, case: &str) -> String {
        format!("http://127.0.0.1:{}/overflow/{token}/{case}", self.port)
    }

    pub fn finish(mut self) -> Result<Value> {
        // This is an explicit bounded observation window, not proof of indefinite host cancellation.
        thread::sleep(Duration::from_millis(100));
        self.stop.store(true, Ordering::SeqCst);
        self.worker
            .take()
            .ok_or("origin collector missing")?
            .join()
            .map_err(|_| "origin collector panicked")?
    }
}

impl Drop for UploadOrigin {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub fn request(
    port: u16,
    case: &str,
    token: &str,
    backend: Option<&str>,
    incoming: Option<usize>,
) -> Result<Value> {
    let started = Instant::now();
    require(
        !token.is_empty()
            && token.len() <= 64
            && token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid overflow token",
    )?;
    require(
        case.len() <= 32
            && case
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
        "invalid overflow case",
    )?;
    require(
        incoming.is_none_or(|length| length <= 2048),
        "unbounded overflow input",
    )?;
    let mut socket = TcpStream::connect(("127.0.0.1", port))?;
    socket.set_write_timeout(Some(SOCKET_BUDGET))?;
    let mut head = format!(
        "POST /progress/overflow/{case} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nx-request-token: {token}\r\nConnection: close\r\n"
    );
    if let Some(backend) = backend {
        require(
            !backend.contains(['\r', '\n']) && backend.len() <= 512,
            "invalid overflow backend",
        )?;
        head.push_str(&format!("x-fixture-backend: {backend}\r\n"));
    }
    if let Some(length) = incoming {
        head.push_str("Transfer-Encoding: chunked\r\n\r\n");
        socket.write_all(head.as_bytes())?;
        write!(socket, "{length:x}\r\n{}\r\n0\r\n\r\n", "x".repeat(length))?;
    } else {
        head.push_str("Content-Length: 0\r\n\r\n");
        socket.write_all(head.as_bytes())?;
    }
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut reader = BufReader::new(DeadlineSocket { socket, deadline });
    let mut metadata = 0;
    let response = read_head(&mut reader, &mut metadata).map_err(|failure| failure.name())?;
    let header_ns = started.elapsed().as_nanos();
    let parts: Vec<_> = response.first.split_whitespace().collect();
    require(
        parts.len() >= 2 && parts[0] == "HTTP/1.1",
        "invalid overflow response status",
    )?;
    let status = parts[1].parse::<u16>()?;
    let mut body = Vec::new();
    payload(&mut reader, &response, &mut metadata, &mut body, 4096, true)
        .map_err(|failure| failure.name())?;
    let event = json!({"event":"client_completed","token":token,"status":status,"headers":response.fields,
        "body":String::from_utf8(body)?,"header_ns":header_ns,"complete_ns":started.elapsed().as_nanos(),
        "first_byte_ns":null,"first_byte_timing":"not-measured"});
    super::CLIENT_EVENTS.lock().unwrap().push(event.clone());
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn observed(wire: &[u8]) -> Value {
        inspect(&mut Cursor::new(wire))
    }

    #[test]
    fn upload_completion_depends_on_framing_not_payload_size_or_socket_eof() {
        for (framing, body, terminal, count) in [
            ("Content-Length: 9\r\n", "xxxx", "eof", 4),
            ("Content-Length: 4\r\n", "xxxx", "complete", 4),
            ("Transfer-Encoding: chunked\r\n", "4\r\nxxxx\r\n", "eof", 4),
            (
                "Transfer-Encoding: chunked\r\n",
                "4\r\nxxxx\r\n0\r\n\r\n",
                "complete",
                4,
            ),
            (
                "Transfer-Encoding: chunked\r\n",
                "8\r\nxxxxxxxx\r\n0\r\n",
                "eof",
                8,
            ),
            ("Content-Length: 9\r\n", "xxxxxxxxx", "complete", 9),
        ] {
            let wire = format!("POST /overflow/test/streamed-9 HTTP/1.1\r\n{framing}\r\n{body}");
            let receipt = observed(wire.as_bytes());
            assert_eq!(receipt["terminal"], terminal);
            assert_eq!(receipt["body_bytes"], count);
        }
    }

    #[test]
    fn malformed_or_unbounded_upload_evidence_is_not_an_abort() {
        for wire in [
            "POST /overflow/t/x HTTP/1.1\r\nContent-Length: 4\r\n",
            "POST /overflow/t/x HTTP/1.1\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nxxxx",
            "POST /overflow/t/x HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n",
            "POST /overflow/t/x HTTP/1.1\r\nTransfer-Encoding: not-chunked\r\n\r\n",
            "POST /overflow/t/x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nxxxxXX0\r\n\r\n",
        ] {
            let receipt = observed(wire.as_bytes());
            assert_ne!(receipt["terminal"], "complete");
        }
        let huge = format!(
            "POST /overflow/t/x HTTP/1.1\r\nX-Large: {}\r\n\r\n",
            "x".repeat(20_000)
        );
        assert_eq!(observed(huge.as_bytes())["terminal"], "metadata_limit");
        let body = format!(
            "POST /overflow/t/x HTTP/1.1\r\nContent-Length: 300\r\n\r\n{}",
            "x".repeat(300)
        );
        let receipt = observed(body.as_bytes());
        assert_eq!(receipt["terminal"], "capture_limit");
        assert!(receipt["body_bytes"].as_u64().unwrap() <= 257);
    }

    #[test]
    fn oversized_head_counts_the_limit_witness() {
        let receipt = observed(&vec![b'x'; METADATA_BYTES + 100]);
        assert_eq!(receipt["terminal"], "metadata_limit");
        assert_eq!(receipt["head_state"], "partial");
        assert_eq!(receipt["head_bytes"], METADATA_BYTES + 1);
    }

    #[test]
    fn partial_head_errors_record_consumed_bytes_not_zero_contact() {
        struct Partial {
            input: Cursor<Vec<u8>>,
            kind: io::ErrorKind,
        }
        impl Read for Partial {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                let read = self.input.read(bytes)?;
                if read == 0 {
                    Err(self.kind.into())
                } else {
                    Ok(read)
                }
            }
        }
        for kind in [io::ErrorKind::ConnectionReset, io::ErrorKind::TimedOut] {
            let receipt = inspect(&mut BufReader::new(Partial {
                input: Cursor::new(b"POST /".to_vec()),
                kind,
            }));
            assert_eq!(receipt["head_bytes"], 6);
            assert_eq!(receipt["head_state"], "partial");
            assert_eq!(receipt["terminal"], "invalid_framing");
        }
    }

    #[test]
    fn third_connection_fails_the_bounded_collector_and_joins_it() {
        let origin = UploadOrigin::start().unwrap();
        let port = origin.port;
        let mut clients = Vec::new();
        for _ in 0..3 {
            let socket = TcpStream::connect(("127.0.0.1", port)).unwrap();
            socket.shutdown(std::net::Shutdown::Write).unwrap();
            clients.push(socket);
        }
        let receipt = origin.finish().unwrap();
        assert_eq!(receipt["accepted"], 3);
        assert_eq!(receipt["connections"].as_array().unwrap().len(), 2);
        assert_eq!(receipt["overflow"], true);
        assert_eq!(receipt["collection_complete"], true);
        assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn joined_origin_retains_completed_incomplete_and_headless_connections() {
        for (wire, terminal, count) in [
            (
                "POST /overflow/test/buffered-8 HTTP/1.1\r\nContent-Length: 8\r\n\r\nxxxxxxxx",
                "complete",
                8,
            ),
            (
                "POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 9\r\n\r\nxxxx",
                "eof",
                4,
            ),
            ("", "eof", 0),
        ] {
            let origin = UploadOrigin::start().unwrap();
            let mut client = TcpStream::connect(("127.0.0.1", origin.port)).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            client.write_all(wire.as_bytes()).unwrap();
            client.shutdown(std::net::Shutdown::Write).unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).unwrap();
            let snapshot = origin.finish().unwrap();
            assert_eq!(snapshot["accepted"], 1);
            assert_eq!(snapshot["connections"][0]["terminal"], terminal);
            assert_eq!(snapshot["connections"][0]["body_bytes"], count);
        }
        let empty = UploadOrigin::start().unwrap().finish().unwrap();
        assert_eq!(empty["accepted"], 0);
        assert_eq!(empty["connections"], json!([]));
        assert_eq!(empty["collection_complete"], true);
    }

    #[test]
    fn stalled_origin_read_is_a_timeout_not_an_abort() {
        let origin = UploadOrigin::start().unwrap();
        let mut client = TcpStream::connect(("127.0.0.1", origin.port)).unwrap();
        client
            .write_all(b"POST /overflow/test/streamed-9 HTTP/1.1\r\nContent-Length: 9\r\n\r\nxxxx")
            .unwrap();
        let started = Instant::now();
        let snapshot = origin.finish().unwrap();
        assert_eq!(snapshot["connections"][0]["terminal"], "timeout");
        assert_eq!(snapshot["connections"][0]["body_bytes"], 4);
        assert!(started.elapsed() < Duration::from_secs(4));
    }
}
