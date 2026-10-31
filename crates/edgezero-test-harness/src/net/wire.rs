use super::overflow::{DeadlineSocket, Profile, capture_wire_body, read_head};
use crate::evidence::{Result, require};
use accept::Acceptor;
use mio::Waker;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    io::{self, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

mod accept;
mod lifetimes;

const GZIP_TEN: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 1, 0, 240, 205, 17, 76, 10, 0, 0, 0,
];
const GZIP_ELEVEN: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0, 0,
];
const IO_BUDGET: Duration = Duration::from_secs(5);
const GATE_BUDGET: Duration = Duration::from_secs(6);
const CONNECTION_LIMIT: usize = 128;
const ACTIVE_LIMIT: usize = 8;

#[derive(Default)]
struct State {
    stop: bool,
    accepted: usize,
    active: usize,
    waiting_collectors: usize,
    accept_wakers: Vec<Arc<Waker>>,
    workers: Vec<thread::JoinHandle<()>>,
    errors: Vec<String>,
    wire: VecDeque<Value>,
    requests: Vec<(u16, String)>,
    gates: HashMap<String, bool>,
    bidders: [bool; 3],
    release_bidders: bool,
    lifetimes: HashMap<String, lifetimes::Lifetime>,
}

pub fn validate_forward_capture(receipt: &Value) -> Result<()> {
    let payload = receipt["payload"]
        .as_str()
        .filter(|value| value.len() <= 8)
        .ok_or("invalid forwarding payload")?;
    let encoded = receipt["encoded_body_hex"]
        .as_str()
        .filter(|value| {
            value.len() <= 256
                && value.len() % 2 == 0
                && value.bytes().all(|b| b.is_ascii_hexdigit())
        })
        .ok_or("invalid forwarding encoded body")?;
    let lengths: Vec<String> = serde_json::from_value(receipt["content_length"].clone())?;
    let encodings: Vec<String> = serde_json::from_value(receipt["transfer_encoding"].clone())?;
    require(
        lengths.len() <= 1
            && encodings.len() <= 1
            && (lengths.is_empty() || encodings.is_empty())
            && lengths.iter().all(|v| v == &payload.len().to_string())
            && encodings.iter().all(|v| v == "chunked")
            && (payload.is_empty() || !lengths.is_empty() || !encodings.is_empty()),
        "invalid forwarding capture framing",
    )?;
    let mut wire = b"POST / HTTP/1.1\r\n".to_vec();
    for (name, values) in [
        ("Content-Length", &lengths),
        ("Transfer-Encoding", &encodings),
    ] {
        for value in values {
            write!(wire, "{name}: {value}\r\n")?;
        }
    }
    wire.extend_from_slice(b"\r\n");
    for index in (0..encoded.len()).step_by(2) {
        wire.push(u8::from_str_radix(&encoded[index..index + 2], 16)?);
    }
    let mut reader = BufReader::new(std::io::Cursor::new(wire));
    let head = read_head(&mut reader, &mut 0, Profile::Wire).map_err(|error| error.name())?;
    let (decoded, _) = capture_wire_body(&mut reader, &head).map_err(|error| error.name())?;
    require(
        decoded == payload.as_bytes() && reader.read(&mut [0])? == 0,
        "forwarding capture bytes disagree with framing",
    )
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn error(&self, error: impl ToString) {
        self.lock().errors.push(error.to_string());
        self.wake.notify_all();
    }

    fn stop(&self, mut wake: impl FnMut(&Waker) -> io::Result<()>) {
        let wakers = {
            let mut state = self.lock();
            state.stop = true;
            state.bidders.fill(true);
            state.release_bidders = true;
            for released in state.gates.values_mut() {
                *released = true;
            }
            self.wake.notify_all();
            std::mem::take(&mut state.accept_wakers)
        };
        for waker in wakers {
            if wake(&waker).is_err() {
                self.error("wire collector wake failed");
            }
        }
    }

    fn release(&self, name: &str) -> Result<()> {
        let mut state = self.lock();
        let gate = state.gates.get_mut(name).ok_or("unarmed P5 gate")?;
        *gate = true;
        self.wake.notify_all();
        Ok(())
    }

    fn wait(&self, budget: Duration, ready: impl Fn(&State) -> bool) -> Result<()> {
        let deadline = Instant::now() + budget;
        let mut state = self.lock();
        while !ready(&state) {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("wire gate timed out")?;
            state = self
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        Ok(())
    }
}

pub struct WireOrigins {
    ports: [u16; 2],
    shared: Arc<Shared>,
    collectors: Vec<thread::JoinHandle<()>>,
}

impl WireOrigins {
    pub fn start() -> Result<Self> {
        let first = TcpListener::bind(("127.0.0.1", 0))?;
        let second = TcpListener::bind(("127.0.0.1", 0))?;
        let ports = [first.local_addr()?.port(), second.local_addr()?.port()];
        let acceptors = [Acceptor::new(first)?, Acceptor::new(second)?];
        let mut origins = Self {
            ports,
            shared: Arc::new(Shared::default()),
            collectors: Vec::new(),
        };
        origins.shared.lock().accept_wakers = acceptors.iter().map(Acceptor::waker).collect();
        for (index, acceptor) in acceptors.into_iter().enumerate() {
            let shared = Arc::clone(&origins.shared);
            origins.collectors.push(
                thread::Builder::new()
                    .name(format!("spin-wire-origin-{index}"))
                    .spawn(move || collect(acceptor, ports[index], ports[1], &shared))?,
            );
        }
        Ok(origins)
    }

    pub fn port(&self, index: usize) -> u16 {
        self.ports[index]
    }

    pub fn register_lifetime(&self, token: &str, case: &str) -> Result<()> {
        lifetimes::register(&self.shared, token, case)
    }

    pub fn mark_lifetime_start(&self, token: &str) -> Result<Instant> {
        lifetimes::mark_start(&self.shared, token)
    }

    pub fn take_lifetime_events(&self, token: &str) -> Result<Value> {
        lifetimes::take_events(&self.shared, token)
    }

    pub fn release_lifetime(&self, token: &str, observed: Instant) -> Result<()> {
        lifetimes::release(&self.shared, token, observed)
    }

    pub fn acknowledge_cleanup_prefix(&self, token: &str, observed: Instant) -> Result<()> {
        lifetimes::acknowledge(&self.shared, token, observed)
    }

    pub fn arm(&self, name: &str) {
        let mut state = self.shared.lock();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            state.errors.push("invalid P5 gate name".to_owned());
        } else if state.gates.contains_key(name) {
            state.errors.push("P5 gate armed twice".to_owned());
        } else if state.gates.len() >= CONNECTION_LIMIT {
            state.errors.push("P5 gate limit".to_owned());
        } else {
            state.gates.insert(name.to_owned(), false);
        }
    }

    pub fn release(&self, name: &str) {
        if let Err(error) = self.shared.release(name) {
            self.shared.error(error);
        }
    }

    pub fn take_wire(&self) -> Result<Value> {
        self.shared.wait(IO_BUDGET, |state| {
            !state.wire.is_empty() || !state.errors.is_empty()
        })?;
        let mut state = self.shared.lock();
        require(state.errors.is_empty(), state.errors.join("; "))?;
        state
            .wire
            .pop_front()
            .ok_or_else(|| "missing wire receipt".into())
    }

    pub fn take_requests(&self) -> Vec<(u16, String)> {
        std::mem::take(&mut self.shared.lock().requests)
    }

    pub fn accepted(&self) -> usize {
        self.shared.lock().accepted
    }

    pub fn finish_counted(self) -> Result<usize> {
        let shared = Arc::clone(&self.shared);
        self.finish()?;
        Ok(shared.lock().accepted)
    }

    pub fn finish(mut self) -> Result<()> {
        self.shutdown();
        let state = self.shared.lock();
        let mut errors = state.errors.clone();
        if !state.wire.is_empty() {
            errors.push(format!("{} undrained wire receipts", state.wire.len()));
        }
        if !state.requests.is_empty() {
            errors.push(format!("{} undrained P5 requests", state.requests.len()));
        }
        for (token, lifetime) in &state.lifetimes {
            if lifetime.undrained() != 0 {
                errors.push(format!(
                    "{} undrained lifetime events for {token}",
                    lifetime.undrained()
                ));
            }
        }
        require(errors.is_empty(), errors.join("; "))
    }

    fn shutdown(&mut self) {
        self.shared.stop(Waker::wake);
        for collector in self.collectors.drain(..) {
            if collector.join().is_err() {
                self.shared.error("wire collector panicked");
            }
        }
        let workers = std::mem::take(&mut self.shared.lock().workers);
        for worker in workers {
            if worker.join().is_err() {
                self.shared.error("wire worker panicked");
            }
        }
    }
}

impl Drop for WireOrigins {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn collect(mut acceptor: Acceptor, port: u16, second: u16, shared: &Arc<Shared>) {
    loop {
        if shared.lock().stop {
            break;
        }
        match acceptor.accept() {
            Ok(socket) => spawn_request(socket, port, second, shared),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                {
                    let mut state = shared.lock();
                    if state.stop {
                        break;
                    }
                    state.waiting_collectors += 1;
                    shared.wake.notify_all();
                }
                let result = acceptor.wait();
                {
                    let mut state = shared.lock();
                    state.waiting_collectors -= 1;
                    shared.wake.notify_all();
                }
                if let Err(error) = result {
                    shared.error(error);
                    shared.stop(Waker::wake);
                    break;
                }
            }
            Err(error) => {
                shared.error(error);
                shared.stop(Waker::wake);
                break;
            }
        }
    }
}

fn spawn_request(socket: TcpStream, port: u16, second: u16, shared: &Arc<Shared>) {
    let mut state = shared.lock();
    if state.stop {
        return;
    }
    state.accepted += 1;
    if state.accepted > CONNECTION_LIMIT || state.active >= ACTIVE_LIMIT {
        state.errors.push("wire origin connection limit".to_owned());
        state.stop = true;
        drop(state);
        shared.stop(Waker::wake);
        return;
    }
    state.active += 1;
    let observed = Arc::clone(shared);
    match thread::Builder::new()
        .name("spin-wire-request".to_owned())
        .spawn(move || {
            let outcome = serve(socket, port, second, &observed);
            let mut state = observed.lock();
            if let Err(error) = outcome {
                state.errors.push(error.to_string());
            }
            state.active -= 1;
            observed.wake.notify_all();
        }) {
        Ok(worker) => state.workers.push(worker),
        Err(error) => {
            state.active -= 1;
            state.errors.push(error.to_string());
        }
    }
    shared.wake.notify_all();
}

fn serve(mut socket: TcpStream, port: u16, second: u16, shared: &Shared) -> Result<()> {
    socket.set_nonblocking(false)?;
    socket.set_write_timeout(Some(IO_BUDGET))?;
    let mut reader = BufReader::new(DeadlineSocket {
        socket: socket.try_clone()?,
        deadline: Instant::now() + IO_BUDGET,
    });
    let head_started = Instant::now();
    let head = read_head(&mut reader, &mut 0, Profile::Wire).map_err(|error| error.name())?;
    let head_received = Instant::now();
    let parts: Vec<_> = head.first.split(' ').collect();
    require(
        head.first.is_ascii() && parts.len() == 3 && matches!(parts[2], "HTTP/1.0" | "HTTP/1.1"),
        "invalid wire request line",
    )?;
    let (method, target) = (parts[0], parts[1]);
    if let Some(tail) = target.strip_prefix("/forward/") {
        let (operation, token) = tail.split_once('/').ok_or("invalid forwarding target")?;
        require(
            matches!(operation, "send" | "begin" | "confirm" | "follow" | "final")
                && !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid forwarding operation or token",
        )?;
        let tokens: Vec<_> = head
            .fields
            .iter()
            .filter(|(name, _)| name == "x-request-token")
            .map(|(_, value)| value.as_str())
            .collect();
        require(tokens == [token], "forwarding token changed")?;
        let (body, encoded) =
            capture_wire_body(&mut reader, &head).map_err(|error| error.name())?;
        reader.get_mut().deadline = Instant::now() + Duration::from_millis(50);
        let mut extra = [0];
        match reader.read(&mut extra) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) => {}
            Ok(_) => return Err("unframed forwarding bytes".into()),
            Err(error) => return Err(error.into()),
        }
        require(
            body.len() <= 8 && body.is_ascii(),
            "forwarding origin body exceeds bound",
        )?;
        let fields = |field| {
            head.fields
                .iter()
                .filter(|(name, _)| name == field)
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>()
        };
        let gate = format!("forward-{token}");
        if operation == "confirm" {
            shared.wait(IO_BUDGET, |state| {
                state
                    .wire
                    .iter()
                    .any(|event| event["token"] == token && event["operation"] == "begin")
            })?;
            shared.release(&gate)?;
        }
        shared.lock().wire.push_back(json!({"port":port,"method":method,"target":target,"token":token,"operation":operation,
            "content_length":fields("content-length"),"transfer_encoding":fields("transfer-encoding"),
            "encoded_body_hex":encoded.iter().map(|b| format!("{b:02x}")).collect::<String>(),"payload":String::from_utf8(body)?}));
        shared.wake.notify_all();
        if operation == "begin" {
            shared.wait(IO_BUDGET, |state| state.gates.get(&gate) == Some(&true))?;
        }
        if operation == "follow" {
            write!(
                socket,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{second}/forward/final/{token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )?;
        } else if operation == "confirm" {
            socket.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")?;
        } else {
            socket.write_all(
                b"HTTP/1.1 201 Created\r\nContent-Length: 3\r\nConnection: close\r\n\r\n",
            )?;
            if method != "HEAD" {
                socket.write_all(b"end")?;
            }
        }
        return Ok(());
    }
    if target.starts_with("/marker/") {
        let tokens: Vec<_> = head
            .fields
            .iter()
            .filter(|(name, _)| name == "x-request-token")
            .map(|(_, value)| value)
            .collect();
        let [token] = tokens.as_slice() else {
            return Err("missing or duplicate marker token".into());
        };
        require(
            matches!(method, "GET" | "HEAD")
                && !token.is_empty()
                && token.len() <= 64
                && token
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && !head
                    .fields
                    .iter()
                    .any(|(name, _)| name == "transfer-encoding")
                && head
                    .fields
                    .iter()
                    .filter(|(name, _)| name == "content-length")
                    .count()
                    <= 1
                && head
                    .fields
                    .iter()
                    .filter(|(name, _)| name == "content-length")
                    .all(|(_, value)| value == "0")
                && reader.buffer().is_empty(),
            "invalid marker method, token or framing",
        )?;
        super::marker_response(&mut socket, method, target)?;
        shared
            .lock()
            .wire
            .push_back(json!({"port":port,"method":method,"target":target,"token":token}));
        shared.wake.notify_all();
        return Ok(());
    }
    if target.starts_with("/lifetimes/") {
        require(
            method == "GET"
                && !head
                    .fields
                    .iter()
                    .any(|(name, _)| name == "transfer-encoding")
                && head
                    .fields
                    .iter()
                    .filter(|(name, _)| name == "content-length")
                    .count()
                    <= 1
                && head
                    .fields
                    .iter()
                    .filter(|(name, _)| name == "content-length")
                    .all(|(_, value)| value == "0")
                && reader.buffer().is_empty(),
            "invalid lifetime method or framing",
        )?;
        return lifetimes::serve(
            &mut socket,
            target,
            &head.fields,
            shared,
            head_started,
            head_received,
        );
    }
    if target.starts_with("/p5/") {
        require(
            method == "GET"
                && !head
                    .fields
                    .iter()
                    .any(|(name, _)| name == "transfer-encoding")
                && head
                    .fields
                    .iter()
                    .filter(|(name, _)| name == "content-length")
                    .all(|(_, value)| value == "0"),
            "P5 request carried framing",
        )?;
        shared.lock().requests.push((port, target.to_owned()));
        return p5(&mut socket, target, second, shared);
    }
    if target.starts_with("/handoff-") {
        require(method == "GET", "handoff request method changed")?;
        return handoff(&mut socket, target, shared);
    }
    require(
        matches!(
            target,
            "/get" | "/head" | "/get-settled" | "/head-settled" | "/post"
        ),
        "unknown wire control route",
    )?;
    let (body, mut encoded) =
        capture_wire_body(&mut reader, &head).map_err(|error| error.name())?;
    reader.get_mut().deadline = Instant::now() + Duration::from_millis(50);
    let mut extra = [0];
    match reader.read(&mut extra) {
        Ok(0) => {}
        Ok(_) => encoded.push(extra[0]),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) => {}
        Err(error) => return Err(error.into()),
    }
    require(body.is_ascii(), "wire payload was not ASCII")?;
    let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
    let fields = |field: &str| {
        head.fields
            .iter()
            .filter(|(name, _)| name == field)
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>()
    };
    shared
        .lock()
        .wire
        .push_back(json!({"method":method,"target":target,
        "content_length":fields("content-length"),"transfer_encoding":fields("transfer-encoding"),
        "encoded_body_hex":hex,"payload":String::from_utf8(body)?}));
    shared.wake.notify_all();
    socket.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")?;
    Ok(())
}

fn handoff(socket: &mut TcpStream, target: &str, shared: &Shared) -> Result<()> {
    let body = match target {
        "/handoff-origin" => {
            shared.wait(IO_BUDGET, |state| {
                state.bidders.iter().all(|started| *started)
            })?;
            b"origin".as_slice()
        }
        "/handoff-release" => {
            shared.lock().release_bidders = true;
            shared.wake.notify_all();
            b"".as_slice()
        }
        "/handoff-bid-A" | "/handoff-bid-B" | "/handoff-bid-C" => {
            let index = usize::from(target.as_bytes()[target.len() - 1] - b'A');
            shared.lock().bidders[index] = true;
            shared.wake.notify_all();
            shared.wait(IO_BUDGET, |state| state.release_bidders)?;
            [b"bid-A", b"bid-B", b"bid-C"][index].as_slice()
        }
        _ => return Err("unpolled retained Send performed provider work".into()),
    };
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )?;
    socket.write_all(body)?;
    Ok(())
}

fn policy_body_result(reject: bool, result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(error)
            if reject
                && matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                ) =>
        {
            Ok(())
        }
        result => result,
    }
}

fn p5(socket: &mut TcpStream, target: &str, second: u16, shared: &Shared) -> Result<()> {
    let parts: Vec<_> = target
        .strip_prefix("/p5/")
        .ok_or("invalid P5 route")?
        .split('/')
        .collect();
    let mut status = 200;
    let mut fields = Vec::<(&str, String)>::new();
    let mut body = b"final".to_vec();
    let mut pending = None;
    let mut blocked = None;
    let mut policy_reject = false;
    match parts.as_slice() {
        [
            "batch",
            token,
            case,
            operation @ ("slot" | "release"),
            index @ ("0" | "1" | "2"),
        ] => {
            require(
                !token.is_empty()
                    && token.len() <= 64
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
                "invalid batch token",
            )?;
            require(
                matches!(
                    *case,
                    "mixed-completed"
                        | "mixed-cutoff"
                        | "mixed-failure"
                        | "fresh-completed"
                        | "fresh-cutoff"
                        | "fresh-failure"
                ),
                "invalid batch case",
            )?;
            let gate = format!("batch-{token}-{case}-{index}");
            if *operation == "release" {
                shared.release(&gate)?;
                body.clear();
            } else {
                require(
                    shared.lock().gates.contains_key(&gate),
                    "unarmed batch gate",
                )?;
                blocked = Some(gate);
                policy_reject = *index == "2" && !case.ends_with("completed");
                body = vec![b'A' + index.parse::<u8>()?];
            }
            fields.push(("Content-Length", body.len().to_string()));
        }
        ["representation", code @ ("200" | "206"), token] => {
            require(
                !token.is_empty() && token.len() <= 128,
                "invalid representation token",
            )?;
            status = code.parse()?;
            body = GZIP_ELEVEN.to_vec();
            for (name, value) in [
                ("Content-Encoding", "gzip"),
                ("Content-Length", "23"),
                ("Content-Range", "bytes 0-22/46"),
                ("ETag", "\"strong\""),
                ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                ("Vary", "Accept-Encoding"),
                ("Accept-Ranges", "bytes"),
                ("Cache-Control", "public, max-age=60"),
                (
                    "Content-Digest",
                    "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
                ),
            ] {
                fields.push((name, value.to_owned()));
            }
        }
        [
            "policy",
            coding @ ("identity" | "gzip"),
            framing @ ("declared" | "observed"),
            size @ ("10" | "11"),
            disposition @ ("reject" | "success"),
            token,
        ] => {
            require(
                !token.is_empty()
                    && token.len() <= 128
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
                "invalid Spin policy token",
            )?;
            let bytes = if *coding == "gzip" {
                if *size == "10" { GZIP_TEN } else { GZIP_ELEVEN }
            } else {
                &[b'a'; 11][..if *size == "10" { 10 } else { 11 }]
            };
            fields.push(("Content-Encoding", (*coding).to_owned()));
            if *framing == "declared" {
                fields.push(("Content-Length", bytes.len().to_string()));
                body = bytes.to_vec();
            } else {
                fields.push(("Transfer-Encoding", "chunked".to_owned()));
                body = format!("{:X}\r\n", bytes.len()).into_bytes();
                body.extend_from_slice(bytes);
                body.extend_from_slice(b"\r\n0\r\n\r\n");
            }
            policy_reject = *disposition == "reject";
        }
        [
            "body",
            coding @ ("identity" | "gzip" | "passthrough"),
            size @ ("10" | "11"),
        ] => {
            let bytes = if *coding == "gzip" {
                if *size == "10" { GZIP_TEN } else { GZIP_ELEVEN }
            } else {
                &[b'a'; 11][..if *size == "10" { 10 } else { 11 }]
            };
            fields.push((
                "Content-Encoding",
                if *coding == "passthrough" {
                    "x-p5-opaque"
                } else {
                    coding
                }
                .to_string(),
            ));
            fields.push(("Transfer-Encoding", "chunked".to_owned()));
            body = format!("{:X}\r\n", bytes.len()).into_bytes();
            body.extend_from_slice(bytes);
            body.extend_from_slice(b"\r\n0\r\n\r\n");
        }
        ["declared", coding @ ("identity" | "gzip" | "passthrough")] => {
            fields.push((
                "Content-Encoding",
                if *coding == "passthrough" {
                    "x-p5-opaque"
                } else {
                    coding
                }
                .to_string(),
            ));
            fields.push(("Content-Length", "11".to_owned()));
            body = vec![b'a'; 11];
        }
        ["cross"] | ["limit", _, _] | ["to-body", _, _] => {
            status = 302;
            body.clear();
            let location = match parts.as_slice() {
                ["cross"] => format!("http://127.0.0.1:{second}/p5/final"),
                ["limit", kind, name] => format!("/p5/redirect/{kind}/{name}"),
                ["to-body", coding, size] => format!("/p5/body/{coding}/{size}"),
                _ => unreachable!(),
            };
            fields.extend([("Location", location), ("Content-Length", "0".to_owned())]);
        }
        ["redirect", kind, name] => {
            status = 302;
            body.clear();
            fields.push(("Location", format!("/p5/next/{name}")));
            match *kind {
                "emptybr" => fields.extend([
                    ("Content-Encoding", "br".to_owned()),
                    ("Content-Length", "0".to_owned()),
                ]),
                "oversized" => {
                    fields.push(("Content-Length", "10485760".to_owned()));
                    pending = Some(*name);
                }
                "pending" => {
                    fields.push(("Transfer-Encoding", "chunked".to_owned()));
                    pending = Some(*name);
                }
                "error" => {
                    fields.push(("Transfer-Encoding", "chunked".to_owned()));
                    body.extend_from_slice(b"3\r\nbad\r\n");
                }
                "clean" => {
                    body = b"found".to_vec();
                    fields.push(("Content-Length", "5".to_owned()));
                }
                _ => return Err("unknown P5 redirect kind".into()),
            }
            if let Some(name) = pending {
                require(shared.lock().gates.contains_key(name), "unarmed P5 gate")?;
            }
        }
        ["next", name] => {
            shared.release(name)?;
            fields.push(("Content-Length", "5".to_owned()));
        }
        ["final"] => fields.push(("Content-Length", "5".to_owned())),
        _ => return Err("unknown P5 origin route".into()),
    }
    write!(
        socket,
        "HTTP/1.1 {status} Fixture\r\nDate: Tue, 27 Oct 2026 00:00:00 GMT\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nX-P5-Fixture: spin-wire\r\n"
    )?;
    for (name, value) in fields {
        write!(socket, "{name}: {value}\r\n")?;
    }
    socket.write_all(b"\r\n")?;
    if let Some(gate) = blocked {
        shared.wait(GATE_BUDGET, |state| state.gates.get(&gate) == Some(&true))?;
    }
    policy_body_result(policy_reject, socket.write_all(&body))?;
    socket.flush()?;
    if let Some(name) = pending {
        shared.wait(GATE_BUDGET, |state| state.gates.get(name) == Some(&true))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{Read, Write},
        net::TcpStream,
        time::Duration,
    };

    fn exchange(origins: &WireOrigins, index: usize, wire: &[u8]) -> Vec<u8> {
        let mut socket = TcpStream::connect(("127.0.0.1", origins.port(index))).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket.write_all(wire).unwrap();
        let mut response = Vec::new();
        socket.read_to_end(&mut response).unwrap();
        response
    }

    #[test]
    fn forwarding_origin_rejects_unframed_chunk_terminator_before_receipt() {
        let origins = WireOrigins::start().unwrap();
        let observed = Arc::clone(&origins.shared);
        let mut socket = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket.write_all(b"GET /forward/send/trailing HTTP/1.1\r\nx-request-token: trailing\r\nContent-Length: 0\r\n\r\n0\r\n\r\n").unwrap();
        let _peer_close = socket.read_to_end(&mut Vec::new());
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("unframed forwarding bytes")
        );
        assert!(observed.lock().wire.is_empty());
    }

    #[test]
    fn raw_capture_preserves_duplicate_fields_chunk_framing_and_extra_byte() {
        let origins = WireOrigins::start().unwrap();
        exchange(
            &origins,
            0,
            b"GET /get HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
        );
        let receipt = origins.take_wire().unwrap();
        assert_eq!(receipt["content_length"], json!(["0", "0"]));
        assert_eq!(receipt["transfer_encoding"], json!([]));
        assert_eq!(receipt["encoded_body_hex"], "");
        exchange(
            &origins,
            0,
            b"GET /get-settled HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n\xff",
        );
        let receipt = origins.take_wire().unwrap();
        assert_eq!(receipt["transfer_encoding"], json!(["chunked"]));
        assert_eq!(receipt["encoded_body_hex"], "300d0a0d0aff");
        assert_eq!(receipt["payload"], "");
        origins.finish().unwrap();
    }

    #[test]
    fn controls_capture_exact_payload_and_bodyless_distinctions() {
        let origins = WireOrigins::start().unwrap();
        for (method, target) in [("GET", "/get"), ("HEAD", "/head-settled")] {
            exchange(
                &origins,
                0,
                format!("{method} {target} HTTP/1.1\r\n\r\n").as_bytes(),
            );
            assert_eq!(
                origins.take_wire().unwrap(),
                json!({"method":method,"target":target,
                "content_length":[],"transfer_encoding":[],"encoded_body_hex":"","payload":""})
            );
        }
        exchange(
            &origins,
            0,
            b"POST /post HTTP/1.1\r\nContent-Length: 14\r\n\r\nupload-control",
        );
        assert_eq!(origins.take_wire().unwrap()["payload"], "upload-control");
        origins.finish().unwrap();
    }

    #[test]
    fn batch_recovery_origin_requires_an_explicit_matching_release() {
        let origins = WireOrigins::start().unwrap();
        origins.arm("batch-owned-mixed-completed-0");
        origins.arm("batch-owned-mixed-completed-1");
        let mut socket = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket
            .write_all(b"GET /p5/batch/owned/mixed-completed/slot/0 HTTP/1.1\r\n\r\n")
            .unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            head.extend(byte);
            assert!(head.len() < 1024);
        }
        exchange(
            &origins,
            0,
            b"GET /p5/batch/owned/mixed-completed/release/1 HTTP/1.1\r\n\r\n",
        );
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let error = socket.read(&mut [0]).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ));
        let release = exchange(
            &origins,
            0,
            b"GET /p5/batch/owned/mixed-completed/release/0 HTTP/1.1\r\n\r\n",
        );
        assert!(release.starts_with(b"HTTP/1.1 200 Fixture\r\n"));
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut body = Vec::new();
        socket.read_to_end(&mut body).unwrap();
        assert_eq!(body, b"A");
        assert_eq!(origins.take_requests().len(), 3);
        origins.finish().unwrap();
    }

    #[test]
    fn marker_origin_preserves_actual_method_and_token_without_body_or_marker() {
        let origins = WireOrigins::start().unwrap();
        for (method, status) in [
            ("GET", 200),
            ("GET", 204),
            ("HEAD", 200),
            ("GET", 304),
            ("GET", 302),
        ] {
            let path = format!("/marker/{}/{status}/absent", method.to_ascii_lowercase());
            let request = format!(
                "{method} {path} HTTP/1.1\r\nx-request-token: marker-control\r\nContent-Length: 0\r\n\r\n"
            );
            let response = exchange(&origins, 0, request.as_bytes());
            assert_eq!(response, format!("HTTP/1.1 {status} Probe\r\nContent-Length: 0\r\nConnection: close\r\nLocation: /not-followed\r\n\r\n").as_bytes());
            assert_eq!(
                origins.take_wire().unwrap(),
                json!({"port":origins.port(0),"method":method,"target":path,"token":"marker-control"})
            );
        }
        assert!(origins.take_requests().is_empty());
        origins.finish().unwrap();
    }

    #[test]
    fn marker_origin_rejects_wrong_method_token_and_framing() {
        for request in [
            "GET /marker/head/200/absent HTTP/1.1\r\nx-request-token: marker\r\n\r\n",
            "POST /marker/get/200/absent HTTP/1.1\r\nx-request-token: marker\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\nx-request-token: bad token\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\nx-request-token: a\r\nx-request-token: b\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\nx-request-token: marker\r\nContent-Length: 1\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\nx-request-token: marker\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            "GET /marker/get/200/absent HTTP/1.1\r\nx-request-token: marker\r\nTransfer-Encoding: chunked\r\n\r\n",
            "GET /marker/get/999/absent HTTP/1.1\r\nx-request-token: marker\r\n\r\n",
        ] {
            let origins = WireOrigins::start().unwrap();
            assert!(exchange(&origins, 0, request.as_bytes()).is_empty());
            assert!(origins.take_wire().is_err());
            assert!(origins.finish().is_err());
        }
        let origins = WireOrigins::start().unwrap();
        let response = exchange(
            &origins,
            0,
            b"HEAD /marker/head/200/absent HTTP/1.1\r\nx-request-token: marker\r\n\r\n",
        );
        assert!(response.starts_with(b"HTTP/1.1 200 Probe\r\n"));
        assert_eq!(origins.take_wire().unwrap()["method"], "HEAD");
        origins.finish().unwrap();
    }

    #[test]
    fn representation_origin_preserves_status_fields_and_encoded_body() {
        let origins = WireOrigins::start().unwrap();
        for status in [200, 206] {
            let request = format!("GET /p5/representation/{status}/control HTTP/1.1\r\n\r\n");
            let response = exchange(&origins, 0, request.as_bytes());
            let split = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .unwrap();
            let head = std::str::from_utf8(&response[..split]).unwrap();
            assert!(head.starts_with(&format!("HTTP/1.1 {status} Fixture\r\n")));
            for field in [
                "Content-Encoding: gzip",
                "Content-Length: 23",
                "ETag: \"strong\"",
                "Content-Range: bytes 0-22/46",
                "Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT",
                "Vary: Accept-Encoding",
                "Accept-Ranges: bytes",
                "Cache-Control: public, max-age=60",
                "Content-Digest: sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:",
            ] {
                assert!(head.contains(field), "missing {field}");
            }
            assert_eq!(&response[split + 4..], GZIP_ELEVEN);
        }
        assert_eq!(origins.take_requests().len(), 2);
        origins.finish().unwrap();
    }

    #[test]
    fn p5_cross_origin_receipts_and_literal_gzip_are_preserved() {
        let origins = WireOrigins::start().unwrap();
        let response = exchange(&origins, 0, b"GET /p5/cross HTTP/1.1\r\n\r\n");
        assert!(String::from_utf8(response).unwrap().contains(&format!(
            "Location: http://127.0.0.1:{}/p5/final\r\n",
            origins.port(1)
        )));
        exchange(&origins, 1, b"GET /p5/final HTTP/1.1\r\n\r\n");
        assert_eq!(
            origins.take_requests(),
            vec![
                (origins.port(0), "/p5/cross".into()),
                (origins.port(1), "/p5/final".into())
            ]
        );
        for size in [10, 11] {
            let response = exchange(
                &origins,
                0,
                format!("GET /p5/body/gzip/{size} HTTP/1.1\r\n\r\n").as_bytes(),
            );
            assert!(
                response.starts_with(
                    b"HTTP/1.1 200 Fixture\r\nDate: Tue, 27 Oct 2026 00:00:00 GMT\r\n"
                )
            );
            let expected = if size == 10 {
                [
                    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 1, 0, 240, 205, 17, 76, 10, 0, 0,
                    0,
                ]
            } else {
                [
                    31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 75, 76, 132, 3, 0, 146, 93, 70, 85, 11, 0, 0,
                    0,
                ]
            };
            let start = response
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .unwrap()
                + 4;
            let mut expected_body = b"17\r\n".to_vec();
            expected_body.extend(expected);
            expected_body.extend(b"\r\n0\r\n\r\n");
            assert_eq!(&response[start..], expected_body);
        }
        assert_eq!(origins.take_requests().len(), 2);
        origins.finish().unwrap();
    }

    #[test]
    fn policy_origin_tcp_preserves_declared_and_observed_body_boundaries() {
        let origins = WireOrigins::start().unwrap();
        for coding in ["identity", "gzip"] {
            for declared in [false, true] {
                for size in [10, 11] {
                    for disposition in ["reject", "success"] {
                        let path = format!(
                            "/p5/policy/{coding}/{}/{size}/{disposition}/unique-{coding}-{declared}-{size}-{disposition}",
                            if declared { "declared" } else { "observed" }
                        );
                        let response = exchange(
                            &origins,
                            0,
                            format!("GET {path} HTTP/1.1\r\n\r\n").as_bytes(),
                        );
                        assert!(
                            response.starts_with(b"HTTP/1.1 200 Fixture\r\n"),
                            "missing policy origin response"
                        );
                        let split = response
                            .windows(4)
                            .position(|bytes| bytes == b"\r\n\r\n")
                            .unwrap()
                            + 4;
                        let head = String::from_utf8(response[..split].to_vec()).unwrap();
                        let bytes = if coding == "gzip" {
                            if size == 10 { GZIP_TEN } else { GZIP_ELEVEN }
                        } else {
                            &[b'a'; 11][..size]
                        };
                        assert!(head.contains(&format!("Content-Encoding: {coding}\r\n")));
                        assert!(head.contains("X-P5-Fixture: spin-wire\r\n"));
                        if declared {
                            assert!(head.contains(&format!("Content-Length: {}\r\n", bytes.len())));
                            assert!(!head.contains("Transfer-Encoding:"));
                            assert_eq!(&response[split..], bytes);
                        } else {
                            assert!(head.contains("Transfer-Encoding: chunked\r\n"));
                            assert!(!head.contains("Content-Length:"));
                            let mut framed = format!("{:X}\r\n", bytes.len()).into_bytes();
                            framed.extend_from_slice(bytes);
                            framed.extend_from_slice(b"\r\n0\r\n\r\n");
                            assert_eq!(&response[split..], framed);
                        }
                        assert_eq!(origins.take_requests(), vec![(origins.port(0), path)]);
                    }
                }
            }
        }
        origins.finish().unwrap();
    }

    #[test]
    fn policy_body_disconnects_are_allowed_only_for_reject_routes() {
        for kind in [
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::TimedOut,
            io::ErrorKind::WriteZero,
        ] {
            assert_eq!(
                policy_body_result(true, Err(kind.into())).is_ok(),
                matches!(
                    kind,
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                )
            );
            assert_eq!(
                policy_body_result(false, Err(kind.into()))
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
        policy_body_result(true, Ok(())).unwrap();
        policy_body_result(false, Ok(())).unwrap();
    }

    #[test]
    fn finish_rejects_undrained_receipts_and_unpolled_dispatch() {
        for wire in [
            &b"GET /get HTTP/1.1\r\n\r\n"[..],
            &b"GET /p5/final HTTP/1.1\r\n\r\n"[..],
            &b"GET /handoff-unused HTTP/1.1\r\n\r\n"[..],
        ] {
            let origins = WireOrigins::start().unwrap();
            exchange(&origins, 0, wire);
            assert!(origins.finish().is_err());
        }
    }

    fn assert_rejected_without_receipt(wire: &[u8], expected: &str) {
        let origins = WireOrigins::start().unwrap();
        let shared = Arc::clone(&origins.shared);
        let response = exchange(&origins, 0, wire);
        let error = origins.finish().unwrap_err().to_string();
        assert_eq!(error, expected);
        assert!(
            response.is_empty(),
            "rejected request received a success response"
        );
        let state = shared.lock();
        assert!(
            state.wire.is_empty(),
            "rejected request produced a capture receipt"
        );
        assert!(
            state.requests.is_empty(),
            "rejected request produced a P5 receipt"
        );
    }

    #[test]
    fn lifetime_routes_reject_unregistered_tokens() {
        assert_rejected_without_receipt(
            b"GET /lifetimes/missing/begin-progress/origin HTTP/1.1\r\nx-fixture-at-us: 2\r\n\r\n",
            "unknown lifetime token",
        );
    }

    #[test]
    fn non_ascii_payloads_are_rejected_without_success_receipts() {
        for wire in [
            &b"POST /post HTTP/1.1\r\nContent-Length: 1\r\n\r\n\xe1"[..],
            &b"POST /post HTTP/1.1\r\nContent-Length: 2\r\n\r\n\xc3\xa9"[..],
            &b"POST /post HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n\xc3\xa9\r\n0\r\n\r\n"
                [..],
        ] {
            assert_rejected_without_receipt(wire, "wire payload was not ASCII");
        }
    }

    #[test]
    fn ambiguous_framing_is_rejected_by_wire_origins_without_success_receipts() {
        for wire in [
            &b"POST /post HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
            &b"POST /post HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n"[..],
            &b"POST /post HTTP/1.1\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n"[..],
        ] {
            assert_rejected_without_receipt(wire, "invalid_framing");
        }
    }

    fn pending(origins: &WireOrigins, name: &str) -> TcpStream {
        use std::io::{BufRead, BufReader};
        origins.arm(name);
        let mut socket = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write!(socket, "GET /p5/redirect/pending/{name} HTTP/1.1\r\n\r\n").unwrap();
        let mut reader = BufReader::new(socket);
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                break;
            }
        }
        reader.into_inner()
    }

    #[test]
    fn p5_next_releases_the_pending_intermediate_without_body_data() {
        let origins = WireOrigins::start().unwrap();
        let mut socket = pending(&origins, "pending-follow");
        let response = exchange(&origins, 0, b"GET /p5/next/pending-follow HTTP/1.1\r\n\r\n");
        assert!(response.ends_with(b"\r\n\r\nfinal"));
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(
            origins.take_requests(),
            vec![
                (
                    origins.port(0),
                    "/p5/redirect/pending/pending-follow".into()
                ),
                (origins.port(0), "/p5/next/pending-follow".into())
            ]
        );
        origins.finish().unwrap();
    }

    #[test]
    fn finish_and_drop_release_pending_gates_join_workers_and_close_both_ports() {
        for finish in [false, true] {
            let origins = WireOrigins::start().unwrap();
            let ports = [origins.port(0), origins.port(1)];
            let mut socket = pending(&origins, "pending-cleanup");
            assert_eq!(origins.take_requests().len(), 1);
            let started = Instant::now();
            if finish {
                origins.finish().unwrap();
            } else {
                drop(origins);
            }
            assert!(started.elapsed() < Duration::from_secs(1));
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).unwrap();
            assert!(bytes.is_empty());
            for port in ports {
                assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
            }
        }
    }

    #[test]
    fn idle_finish_and_drop_wake_both_collectors_without_fake_connections() {
        for finish in [false, true] {
            let origins = WireOrigins::start().unwrap();
            let shared = Arc::clone(&origins.shared);
            let ports = origins.ports;
            if finish {
                origins.finish().unwrap();
            } else {
                drop(origins);
            }
            let state = shared.lock();
            assert!(state.stop);
            assert_eq!(state.accepted, 0);
            assert_eq!(state.active, 0);
            assert_eq!(state.waiting_collectors, 0);
            assert!(state.workers.is_empty());
            assert!(state.errors.is_empty());
            assert!(state.accept_wakers.is_empty());
            for port in ports {
                assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
            }
        }
    }

    #[test]
    fn stop_winning_after_accept_prevents_accounting_and_worker_spawn() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let (socket, _) = listener.accept().unwrap();
        let shared = Arc::new(Shared::default());
        shared.stop(mio::Waker::wake);
        spawn_request(socket, port, port, &shared);
        let state = shared.lock();
        assert_eq!(state.accepted, 0);
        assert_eq!(state.active, 0);
        assert!(state.workers.is_empty());
        assert!(state.errors.is_empty());
        assert_eq!(client.read(&mut [0]).unwrap(), 0);
    }

    #[test]
    fn failed_wakes_are_reported_and_fallback_still_joins_both_collectors() {
        use std::sync::mpsc;
        let origins = WireOrigins::start().unwrap();
        let shared = Arc::clone(&origins.shared);
        let rescue = shared.lock().accept_wakers.clone();
        shared
            .wait(IO_BUDGET, |state| state.waiting_collectors == 2)
            .unwrap();
        let mut attempts = 0;
        shared.stop(|_| {
            attempts += 1;
            Err(io::Error::other("injected wake failure"))
        });
        assert_eq!(attempts, 2);
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            done_tx.send(origins.finish()).unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_secs(2));
        if result.is_err() {
            for wake in rescue {
                let _ = wake.wake();
            }
        }
        worker.join().unwrap();
        let error = result.unwrap().unwrap_err().to_string();
        assert_eq!(error.matches("wire collector wake failed").count(), 2);
        let state = shared.lock();
        assert_eq!(state.accepted, 0);
        assert_eq!(state.active, 0);
        assert!(state.workers.is_empty());
    }

    #[test]
    fn readiness_keeps_the_global_active_connection_limit() {
        let origins = WireOrigins::start().unwrap();
        let clients: Vec<_> = (0..ACTIVE_LIMIT)
            .map(|index| TcpStream::connect(("127.0.0.1", origins.port(index % 2))).unwrap())
            .collect();
        origins
            .shared
            .wait(IO_BUDGET, |state| state.active == ACTIVE_LIMIT)
            .unwrap();
        let ninth = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        origins.shared.wait(IO_BUDGET, |state| state.stop).unwrap();
        assert_eq!(origins.shared.lock().accepted, ACTIVE_LIMIT + 1);
        assert_eq!(origins.shared.lock().workers.len(), ACTIVE_LIMIT);
        drop(ninth);
        drop(clients);
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("wire origin connection limit")
        );
    }

    #[test]
    fn readiness_keeps_the_global_cumulative_connection_limit() {
        let origins = WireOrigins::start().unwrap();
        for index in 0..CONNECTION_LIMIT {
            assert!(
                exchange(&origins, index % 2, b"GET /get HTTP/1.1\r\n\r\n")
                    .starts_with(b"HTTP/1.1 204")
            );
            origins.take_wire().unwrap();
            origins
                .shared
                .wait(IO_BUDGET, |state| state.active == 0)
                .unwrap();
        }
        let extra = TcpStream::connect(("127.0.0.1", origins.port(1))).unwrap();
        origins.shared.wait(IO_BUDGET, |state| state.stop).unwrap();
        assert_eq!(origins.shared.lock().accepted, CONNECTION_LIMIT + 1);
        assert_eq!(origins.shared.lock().active, 0);
        drop(extra);
        assert!(
            origins
                .finish()
                .unwrap_err()
                .to_string()
                .contains("wire origin connection limit")
        );
    }

    #[test]
    fn partial_headers_retain_their_io_deadline_during_shutdown() {
        use std::sync::mpsc;
        let origins = WireOrigins::start().unwrap();
        let shared = Arc::clone(&origins.shared);
        let mut client = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
        client.write_all(b"GET /get HTTP/1.1\r\n").unwrap();
        shared.wait(IO_BUDGET, |state| state.active == 1).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            done_tx.send(origins.finish()).unwrap();
        });
        let result = done_rx.recv_timeout(IO_BUDGET + Duration::from_secs(2));
        drop(client);
        worker.join().unwrap();
        assert!(result.unwrap().is_err());
        assert_eq!(shared.lock().active, 0);
        assert!(shared.lock().workers.is_empty());
    }

    #[test]
    fn finish_waits_for_late_worker_errors_before_validating_drained_queues() {
        use std::sync::mpsc;
        let origins = WireOrigins::start().unwrap();
        exchange(&origins, 0, b"GET /get HTTP/1.1\r\n\r\n");
        origins.take_wire().unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let observed = Arc::clone(&origins.shared);
        // Inject a tracked worker failure after a successful client reply and queue drain.
        origins.shared.lock().workers.push(thread::spawn(move || {
            release_rx.recv().unwrap();
            observed.error("injected late worker failure");
            observed.lock().requests.push((1, "/p5/late".to_owned()));
        }));
        let (finished_tx, finished_rx) = mpsc::channel();
        let finisher = thread::spawn(move || {
            finished_tx.send(origins.finish()).unwrap();
        });
        let before_release = finished_rx.recv_timeout(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        let result = finished_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        finisher.join().unwrap();
        assert!(matches!(
            before_release,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let error = result.unwrap_err().to_string();
        assert!(error.contains("injected late worker failure"));
        assert!(error.contains("undrained P5 requests"));
    }

    #[test]
    fn handoff_waits_for_three_bidders_and_keeps_them_pending_until_release() {
        let origins = WireOrigins::start().unwrap();
        let connect = |path: &str| {
            let mut socket = TcpStream::connect(("127.0.0.1", origins.port(0))).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            write!(socket, "GET {path} HTTP/1.1\r\n\r\n").unwrap();
            socket
        };
        let mut origin = connect("/handoff-origin");
        let mut bidders = ["A", "B", "C"].map(|name| connect(&format!("/handoff-bid-{name}")));
        let mut bytes = Vec::new();
        origin.read_to_end(&mut bytes).unwrap();
        assert!(bytes.ends_with(b"\r\n\r\norigin"));
        for bidder in &mut bidders {
            bidder
                .set_read_timeout(Some(Duration::from_millis(20)))
                .unwrap();
            let error = bidder.read(&mut [0]).unwrap_err();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ));
            bidder
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
        }
        exchange(&origins, 0, b"GET /handoff-release HTTP/1.1\r\n\r\n");
        for (index, bidder) in bidders.iter_mut().enumerate() {
            bytes.clear();
            bidder.read_to_end(&mut bytes).unwrap();
            assert!(bytes.ends_with([b"bid-A", b"bid-B", b"bid-C"][index]));
        }
        origins.finish().unwrap();
    }

    #[test]
    fn bad_gate_operations_are_retained_for_final_validation() {
        let origins = WireOrigins::start().unwrap();
        origins.arm("test");
        origins.release("test");
        origins.release("test");
        origins.arm("test");
        origins.release("unknown");
        let error = origins.finish().unwrap_err().to_string();
        assert!(error.contains("armed twice"));
        assert!(error.contains("unarmed P5 gate"));
    }
}
