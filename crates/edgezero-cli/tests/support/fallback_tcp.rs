use std::env;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read, Seek as _, SeekFrom, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::str;
use std::thread;
use std::time::{Duration, Instant};

const WIRE_LIMIT: u64 = 8192;
const IO_BUDGET: Duration = Duration::from_secs(8);

struct DeadlineSocket {
    deadline: Instant,
    socket: TcpStream,
}

#[derive(Debug)]
struct Reply {
    body: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    status: u16,
}

struct Server {
    address: SocketAddr,
    child: Child,
    log: File,
}

#[expect(
    clippy::missing_trait_methods,
    reason = "Read defaults delegate to the same absolute-deadline-checked read"
)]
impl Read for DeadlineSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(io::ErrorKind::TimedOut)?;
        self.socket.set_read_timeout(Some(remaining))?;
        self.socket.read(buf)
    }
}

impl Reply {
    fn values(&self, name: &str) -> Vec<&[u8]> {
        self.headers
            .iter()
            .filter(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_slice())
            .collect()
    }
}

impl Server {
    fn assert_running(&mut self) {
        assert!(
            self.child.try_wait().expect("application state").is_none(),
            "application exited during TCP proof: {}",
            self.diagnostics()
        );
    }

    fn diagnostics(&mut self) -> String {
        self.log.seek(SeekFrom::Start(0)).expect("read diagnostics");
        let mut text = String::new();
        (&mut self.log)
            .take(64 * 1024)
            .read_to_string(&mut text)
            .expect("bounded diagnostics");
        text
    }

    fn exchange(&mut self, wire: &[u8]) -> (Vec<Reply>, Duration) {
        self.exchange_after_response(wire, &[])
    }

    fn exchange_after_response(&mut self, wire: &[u8], tail: &[u8]) -> (Vec<Reply>, Duration) {
        self.assert_running();
        let wire_limit = usize::try_from(WIRE_LIMIT).expect("wire bound");
        assert!(wire.len().checked_add(tail.len()).expect("request length") <= wire_limit);
        let started = Instant::now();
        let mut socket = TcpStream::connect_timeout(&self.address, Duration::from_secs(1))
            .expect("connect application");
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("write timeout");
        socket.write_all(wire).expect("send bounded request");
        let mut reader = DeadlineSocket {
            socket,
            deadline: started.checked_add(IO_BUDGET).expect("client deadline"),
        };
        let mut bytes = Vec::new();
        let mut complete_at = None;
        loop {
            let mut buffer = [0_u8; 1024];
            let room = wire_limit
                .saturating_add(1)
                .saturating_sub(bytes.len())
                .min(buffer.len());
            let count = match reader.read(&mut buffer[..room]) {
                Ok(0) => break,
                Ok(count) => count,
                // A reset is permitted only when all retained replies parse completely below.
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => break,
                Err(error) => panic!("bounded response read: {error}"),
            };
            bytes.extend_from_slice(&buffer[..count]);
            assert!(bytes.len() <= wire_limit, "response capture overflow");
            if complete_at.is_none() && parse_replies(&bytes).is_ok() {
                complete_at = Some(started.elapsed());
                if !tail.is_empty() {
                    let remaining = reader
                        .deadline
                        .checked_duration_since(Instant::now())
                        .filter(|remaining| !remaining.is_zero())
                        .expect("deadline before tail write");
                    reader
                        .socket
                        .set_write_timeout(Some(remaining.min(Duration::from_secs(2))))
                        .expect("tail write timeout");
                    if let Err(error) = reader.socket.write_all(tail) {
                        assert!(
                            matches!(
                                error.kind(),
                                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                            ),
                            "deferred tail write: {error}"
                        );
                    }
                }
            }
        }
        self.assert_running();
        (
            parse_replies(&bytes).expect("complete bounded buffered response"),
            complete_at.expect("response completion observed before closure"),
        )
    }

    fn start(mut command: Command) -> Self {
        let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve fallback test port");
        let address = reservation.local_addr().expect("reserved address");
        for (key, _) in env::vars_os() {
            if key.to_string_lossy().starts_with("EDGEZERO__") {
                command.env_remove(key);
            }
        }
        let log = tempfile::tempfile().expect("server diagnostics");
        command
            .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
            .env("EDGEZERO__ADAPTER__PORT", address.port().to_string())
            .env("EDGEZERO__LOGGING__LEVEL", "off")
            .stdout(Stdio::from(log.try_clone().expect("stdout diagnostics")))
            .stderr(Stdio::from(log.try_clone().expect("stderr diagnostics")));
        drop(reservation);
        let mut server = Self {
            child: command.spawn().expect("start application"),
            log,
            address,
        };
        let started = Instant::now();
        loop {
            server.assert_running();
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
                server.assert_running();
                return server;
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "application startup timeout: {}",
                server.diagnostics()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "unpinned test child ownership uses Drop's standard pin forwarding"
)]
impl Drop for Server {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

fn parse_replies(mut wire: &[u8]) -> io::Result<Vec<Reply>> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid bounded response framing",
        )
    };
    let mut replies = Vec::new();
    while !wire.is_empty() {
        if replies.len() == 2 {
            return Err(invalid());
        }
        let mut fields = [httparse::EMPTY_HEADER; 32];
        let mut parsed = httparse::Response::new(&mut fields);
        let httparse::Status::Complete(head_bytes) = parsed
            .parse(wire)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        else {
            return Err(invalid());
        };
        if parsed.version != Some(1) {
            return Err(invalid());
        }
        let mut declared_length = None;
        for field in parsed.headers.iter() {
            if field.name.eq_ignore_ascii_case("transfer-encoding") {
                return Err(invalid());
            }
            if field.name.eq_ignore_ascii_case("content-length") {
                if declared_length.is_some()
                    || field.value.is_empty()
                    || !field.value.iter().all(u8::is_ascii_digit)
                {
                    return Err(invalid());
                }
                declared_length = Some(
                    str::from_utf8(field.value)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
                        .parse::<usize>()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
                );
            }
        }
        let length = declared_length
            .filter(|length| *length <= 1024)
            .ok_or_else(invalid)?;
        let body = wire.get(head_bytes..).ok_or_else(invalid)?;
        let content = body.get(..length).ok_or_else(invalid)?;
        replies.push(Reply {
            status: parsed.code.ok_or_else(invalid)?,
            headers: parsed
                .headers
                .iter()
                .map(|field| (field.name.to_owned(), field.value.to_vec()))
                .collect(),
            body: content.to_vec(),
        });
        wire = &body[length..];
    }
    if replies.is_empty() {
        return Err(invalid());
    }
    Ok(replies)
}

fn post(path: &str, len: usize, chunked: bool, close: bool) -> String {
    let mut wire = format!("POST {path} HTTP/1.1\r\nHost: localhost\r\n");
    if close {
        wire.push_str("Connection: close\r\n");
    }
    if chunked {
        write!(
            wire,
            "Transfer-Encoding: chunked\r\n\r\n{len:x}\r\n{}\r\n0\r\n\r\n",
            "x".repeat(len)
        )
        .expect("bounded chunked fixture");
    } else {
        write!(wire, "Content-Length: {len}\r\n\r\n{}", "x".repeat(len))
            .expect("bounded fixed fixture");
    }
    wire
}

fn assert_plain(reply: &Reply, status: u16, body: &[u8]) {
    assert_eq!(reply.status, status);
    assert_eq!(reply.body, body);
    assert_eq!(
        reply.values("content-type"),
        [b"text/plain; charset=utf-8".as_slice()]
    );
    assert!(
        reply.values("retry-after").is_empty(),
        "fallback gained Retry-After"
    );
}

#[expect(
    clippy::print_stdout,
    reason = "synthetic TCP observations are retained by the explicit qualification command"
)]
fn record(app: &str, case: &str, replies: &[Reply], elapsed: Duration) {
    std::println!(
        "{}",
        serde_json::json!({"event":"fallback-tcp","app":app,"case":case,
        "responses":replies.iter().map(|reply|serde_json::json!({"status":reply.status,
            "body":String::from_utf8_lossy(&reply.body),"allow":reply.values("allow").iter().map(|value|String::from_utf8_lossy(value).into_owned()).collect::<Vec<_>>(),
            "retry_after_count":reply.values("retry-after").len()})).collect::<Vec<_>>(),
        "response_complete_ms":elapsed.as_millis(),"status":"pass","qualification":false})
    );
}

pub fn assert_fallbacks(command: Command, app: &str, expected_health: &[u8]) {
    let mut server = Server::start(command);
    let health = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let (healthy, initial_elapsed) = server.exchange(health);
    assert_eq!(healthy.len(), 1);
    assert_eq!(healthy[0].status, 200);
    assert_eq!(
        healthy[0].body, expected_health,
        "correct application's root response"
    );
    let health_body = healthy[0].body.clone();
    record(app, "initial-health", &healthy, initial_elapsed);
    for (path, fallback) in [("/missing", 404), ("/", 405)] {
        for chunked in [false, true] {
            for len in [4095, 4096, 4097] {
                let (replies, elapsed) = server.exchange(post(path, len, chunked, true).as_bytes());
                assert_eq!(replies.len(), 1);
                if len > 4096 {
                    assert_plain(&replies[0], 413, b"request body too large\n");
                } else {
                    assert_eq!(replies[0].status, fallback);
                    assert!(replies[0].values("retry-after").is_empty());
                    if fallback == 405 {
                        assert_eq!(replies[0].values("allow"), [b"GET".as_slice()]);
                    }
                }
                record(
                    app,
                    &format!("{path}-{}-{len}", if chunked { "chunked" } else { "fixed" }),
                    &replies,
                    elapsed,
                );
            }
        }
    }
    let pending = b"POST /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n";
    let (timed_out, timeout_elapsed) = server.exchange(pending);
    assert_eq!(timed_out.len(), 1);
    assert_plain(&timed_out[0], 408, b"request timeout\n");
    assert!(
        timeout_elapsed >= Duration::from_millis(4500) && timeout_elapsed < IO_BUDGET,
        "five-second fallback response deadline: {timeout_elapsed:?}"
    );
    record(
        app,
        "pending-fallback-deadline",
        &timed_out,
        timeout_elapsed,
    );
    // The remaining payload cannot have been consumed: send it only after the complete 413.
    let prefix = format!(
        "POST /missing HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n1001\r\n{}\r\n",
        "x".repeat(4097)
    );
    let disguised_request = "GET /echo/not-health HTTP/1.1\r\nHost: localhost\r\n\r\n";
    let tail = format!(
        "{:x}\r\n{disguised_request}\r\n0\r\n\r\n{}",
        disguised_request.len(),
        str::from_utf8(health).expect("HTTP fixture")
    );
    let (pipelined, pipeline_elapsed) =
        server.exchange_after_response(prefix.as_bytes(), tail.as_bytes());
    assert_plain(&pipelined[0], 413, b"request body too large\n");
    if let Some(next) = pipelined.get(1) {
        assert_eq!(next.status, 200);
        assert_eq!(next.body, health_body);
    }
    record(
        app,
        "overflow-deferred-tail-close-or-health",
        &pipelined,
        pipeline_elapsed,
    );
    let (recovered, recovery_elapsed) = server.exchange(health);
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, 200);
    assert_eq!(recovered[0].body, health_body);
    record(app, "recovery-health", &recovered, recovery_elapsed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffered_parser_preserves_response_boundaries_and_overflow_body() {
        let wire = b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 23\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nrequest body too large\nHTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\napp";
        let replies = parse_replies(wire).expect("complete pipelined replies");
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].status, 413);
        assert_eq!(replies[0].body, b"request body too large\n");
        assert_eq!(replies[0].headers[0].0, "Content-Length");
        assert_eq!(replies[1].status, 200);
        assert_eq!(replies[1].body, b"app");
    }

    #[test]
    fn buffered_parser_rejects_missing_ambiguous_and_truncated_evidence() {
        for wire in [
            b"".as_slice(),
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 23\r\n\r\nshort",
            b"HTTP/1.1 413 Payload Too Large\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 999999\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: -1\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n\r\nHTTP/1.1 200",
        ] {
            assert!(parse_replies(wire).is_err(), "{wire:?}");
        }
    }
}
