use crate::evidence::{Result, require};
use serde_json::{Value, json};
use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

const LIFETIME: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(5);
const HEADER_LIMIT: usize = 4096;
const CASES: &[&str] = &[
    "truncated",
    "silent-head",
    "pending-body",
    "trickle",
    "siblings",
    "exhausted",
    "eof",
    "empty-eof",
    "malformed-head",
    "malformed-body",
    "head-error",
];

struct State {
    stop: bool,
    sockets: Vec<Option<TcpStream>>,
    slots: [bool; 2],
    heads: [bool; 2],
    snapshot: Value,
}

struct Shared {
    case: String,
    token: String,
    started: Instant,
    deadline: Instant,
    max_workers: usize,
    state: Mutex<State>,
    wake: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn error(&self, error: &'static str) {
        self.lock().snapshot["errors"]
            .as_array_mut()
            .unwrap()
            .push(json!(error));
    }

    fn stop(&self, watchdog: bool) {
        let mut state = self.lock();
        if watchdog && !state.stop {
            state.snapshot["watchdog"] = json!(true);
            state.snapshot["errors"]
                .as_array_mut()
                .unwrap()
                .push(json!("origin-watchdog"));
        }
        state.stop = true;
        for socket in state.sockets.iter().flatten() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        self.wake.notify_all();
    }

    fn remaining(&self) -> io::Result<Duration> {
        if self.lock().stop {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        match self.deadline.checked_duration_since(Instant::now()) {
            Some(remaining) if !remaining.is_zero() => Ok(remaining),
            _ => {
                self.stop(true);
                Err(io::ErrorKind::TimedOut.into())
            }
        }
    }

    fn wait_until(&self, at: Instant) -> bool {
        let mut state = self.lock();
        loop {
            if state.stop {
                return false;
            }
            let now = Instant::now();
            if now >= self.deadline {
                drop(state);
                self.stop(true);
                return false;
            }
            if now >= at {
                return true;
            }
            state = self
                .wake
                .wait_timeout(state, at.min(self.deadline) - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    fn stopped_reason(&self) -> Option<&'static str> {
        let state = self.lock();
        state.stop.then_some(if state.snapshot["watchdog"] == true {
            "watchdog"
        } else {
            "harness-stop"
        })
    }
}

pub struct ReadinessOrigin {
    port: u16,
    shared: Arc<Shared>,
    worker: Option<thread::JoinHandle<()>>,
}

impl ReadinessOrigin {
    pub fn start(case: &str, token: &str) -> Result<Self> {
        require(CASES.contains(&case), "unknown-readiness-case")?;
        require(
            !token.is_empty()
                && token.len() <= 128
                && token.bytes().all(|byte| byte.is_ascii_graphic()),
            "invalid-readiness-token",
        )?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let started = Instant::now();
        let max_workers = if case == "siblings" { 2 } else { 1 };
        let shared = Arc::new(Shared {
            case: case.to_owned(),
            token: token.to_owned(),
            started,
            deadline: started + LIFETIME,
            max_workers,
            state: Mutex::new(State {
                stop: false,
                sockets: Vec::new(),
                slots: [false; 2],
                heads: [false; 2],
                snapshot: json!({"case":case,"token":token,"accepted":0,"workers_joined":0,
                    "max_workers":max_workers,"harness_shutdown":false,"watchdog":false,
                    "errors":[],"requests":[],"writes":[],"terminations":[]}),
            }),
            wake: Condvar::new(),
        });
        let observed = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("readiness-origin".to_owned())
            .spawn(move || collect(listener, &observed))?;
        Ok(Self {
            port,
            shared,
            worker: Some(worker),
        })
    }

    pub fn url(&self, slot: usize) -> String {
        format!(
            "http://127.0.0.1:{}/readiness/{}/{slot}",
            self.port, self.shared.case
        )
    }

    pub fn finish(mut self) -> Value {
        self.shutdown();
        self.snapshot()
    }

    fn shutdown(&mut self) {
        self.shared.lock().snapshot["harness_shutdown"] = json!(true);
        self.shared.stop(false);
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            self.shared.error("collector-panicked");
        }
    }

    fn snapshot(&self) -> Value {
        self.shared.lock().snapshot.clone()
    }
}

impl Drop for ReadinessOrigin {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn collect(listener: TcpListener, shared: &Arc<Shared>) {
    thread::scope(|scope| {
        let mut workers = Vec::new();
        while shared.remaining().is_ok() {
            match listener.accept() {
                Ok((socket, _)) => {
                    let mut state = shared.lock();
                    let accepted = state.snapshot["accepted"].as_u64().unwrap() + 1;
                    state.snapshot["accepted"] = json!(accepted);
                    if state.stop || workers.len() == shared.max_workers {
                        state.snapshot["errors"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!("unexpected-connection"));
                        state.snapshot["terminations"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"slot":null,"reason":"io-error"}));
                        let _ = socket.shutdown(Shutdown::Both);
                        drop(state);
                        shared.stop(false);
                        break;
                    }
                    let control = socket
                        .set_nonblocking(false)
                        .and_then(|()| socket.try_clone());
                    let Ok(control) = control else {
                        state.snapshot["terminations"]
                            .as_array_mut()
                            .unwrap()
                            .push(json!({"slot":null,"reason":"io-error"}));
                        drop(state);
                        shared.error("socket-setup-failed");
                        shared.stop(false);
                        break;
                    };
                    let index = state.sockets.len();
                    state.sockets.push(Some(control));
                    drop(state);
                    let shared = Arc::clone(shared);
                    workers.push(scope.spawn(move || serve(socket, index, &shared)));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    shared.wait_until(Instant::now() + POLL);
                }
                Err(_) => {
                    shared.error("origin-accept-failed");
                    shared.stop(false);
                    break;
                }
            }
        }
        for worker in workers {
            let joined = worker.join();
            let mut state = shared.lock();
            let count = state.snapshot["workers_joined"].as_u64().unwrap() + 1;
            state.snapshot["workers_joined"] = json!(count);
            if joined.is_err() {
                state.snapshot["errors"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!("worker-panicked"));
            }
        }
    });
}

struct DeadlineSocket<'a> {
    socket: &'a TcpStream,
    shared: &'a Shared,
}

impl Read for DeadlineSocket<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            self.socket.set_read_timeout(Some(
                self.shared.remaining()?.min(Duration::from_millis(50)),
            ))?;
            match self.socket.read(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) => {}
                outcome => return outcome,
            }
        }
    }
}

impl Write for DeadlineSocket<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.socket
            .set_write_timeout(Some(self.shared.remaining()?))?;
        self.socket.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

fn request(socket: &TcpStream, shared: &Shared) -> std::result::Result<usize, &'static str> {
    let mut reader = DeadlineSocket { socket, shared };
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() == HEADER_LIMIT {
            return Err("request-head-limit");
        }
        let mut byte = [0];
        reader
            .read_exact(&mut byte)
            .map_err(|_| "request-head-incomplete")?;
        bytes.push(byte[0]);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "request-head-invalid")?;
    let mut lines = text.split("\r\n");
    let parts: Vec<_> = lines
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .collect();
    let ["GET", path, "HTTP/1.1"] = parts.as_slice() else {
        return Err("request-head-invalid");
    };
    let prefix = format!("/readiness/{}/", shared.case);
    let slot = path
        .strip_prefix(&prefix)
        .and_then(|slot| slot.parse::<usize>().ok())
        .filter(|slot| *slot < shared.max_workers && *path == format!("{prefix}{slot}"))
        .ok_or("request-path-mismatch")?;
    let mut token = None;
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or("request-head-invalid")?;
        if name.eq_ignore_ascii_case("x-readiness-token") && token.replace(value.trim()).is_some() {
            return Err("request-token-mismatch");
        }
    }
    if token != Some(shared.token.as_str()) {
        return Err("request-token-mismatch");
    }
    let mut state = shared.lock();
    if state.slots[slot] {
        return Err("duplicate-slot");
    }
    state.slots[slot] = true;
    state.snapshot["requests"].as_array_mut().unwrap().push(json!({
        "slot":slot,"path":path,"token":shared.token,"received_us":shared.started.elapsed().as_micros()}));
    Ok(slot)
}

fn send(
    socket: &TcpStream,
    shared: &Shared,
    slot: usize,
    kind: &str,
    bytes: &[u8],
) -> io::Result<()> {
    let mut writer = DeadlineSocket { socket, shared };
    writer.write_all(bytes)?;
    writer.flush()?;
    let mut state = shared.lock();
    state.snapshot["writes"]
        .as_array_mut()
        .unwrap()
        .push(json!({
        "slot":slot,"at_us":shared.started.elapsed().as_micros(),"kind":kind,"bytes":bytes.len()}));
    if kind == "head" {
        state.heads[slot] = true;
        shared.wake.notify_all();
    }
    Ok(())
}

fn hold(socket: &TcpStream, shared: &Shared) -> io::Result<&'static str> {
    socket.set_read_timeout(Some(Duration::from_millis(10)))?;
    while shared.remaining().is_ok() {
        match socket.peek(&mut [0]) {
            Ok(0) => return Ok(shared.stopped_reason().unwrap_or("peer-close")),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                ) =>
            {
                return Ok("peer-close");
            }
            Err(error)
                if !matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                return Err(error);
            }
            _ => {}
        }
        shared.wait_until(Instant::now() + POLL);
    }
    Ok(shared.stopped_reason().unwrap_or("watchdog"))
}

fn respond(socket: &TcpStream, shared: &Shared, slot: usize) -> io::Result<&'static str> {
    let case = shared.case.as_str();
    if case == "silent-head" {
        return hold(socket, shared);
    }
    if case == "head-error" {
        return Ok("complete");
    }
    if case == "malformed-head" {
        send(socket, shared, slot, "head", b"NOT HTTP\r\n\r\n")?;
        return Ok("complete");
    }
    let length = match case {
        "empty-eof" => 0,
        "pending-body" => 1,
        "siblings" if slot == 0 => 1,
        "exhausted" => 2,
        _ => 3,
    };
    let head = if case == "malformed-body" {
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_owned()
    } else {
        format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n")
    };
    send(socket, shared, slot, "head", head.as_bytes())?;
    match case {
        "pending-body" => return hold(socket, shared),
        "exhausted" => {
            send(socket, shared, slot, "body", b"x")?;
            return hold(socket, shared);
        }
        "siblings" if slot == 0 => return hold(socket, shared),
        "siblings" => {
            while !shared.lock().heads.iter().all(|ready| *ready) {
                if !shared.wait_until(Instant::now() + POLL) {
                    return Ok(shared.stopped_reason().unwrap_or("watchdog"));
                }
            }
            send(socket, shared, slot, "body", b"xyz")?;
        }
        "trickle" => {
            let start = Instant::now();
            for (index, byte) in [b"x", b"y", b"z"].into_iter().enumerate() {
                if !shared.wait_until(start + Duration::from_millis(index as u64 * 300)) {
                    return Ok(shared.stopped_reason().unwrap_or("watchdog"));
                }
                send(socket, shared, slot, "body", byte)?;
            }
        }
        "truncated" | "malformed-body" => {
            send(
                socket,
                shared,
                slot,
                "body",
                if case == "truncated" {
                    b"x"
                } else {
                    b"1\r\nx\r\n"
                },
            )?;
            if !shared.wait_until(Instant::now() + Duration::from_millis(50)) {
                return Ok(shared.stopped_reason().unwrap_or("watchdog"));
            }
            if case == "malformed-body" {
                send(socket, shared, slot, "body", b"Z\r\n")?;
            }
        }
        "eof" => send(socket, shared, slot, "body", b"xyz")?,
        _ => {}
    }
    Ok("complete")
}

fn serve(socket: TcpStream, index: usize, shared: &Shared) {
    let mut slot = None;
    let outcome = match request(&socket, shared) {
        Ok(request_slot) => {
            slot = Some(request_slot);
            respond(&socket, shared, request_slot)
        }
        Err(error) => {
            shared.error(error);
            Err(io::ErrorKind::InvalidData.into())
        }
    };
    let reason = shared.stopped_reason().unwrap_or_else(|| match outcome {
        Ok(reason) => reason,
        Err(ref error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionAborted
            ) =>
        {
            "peer-close"
        }
        Err(_) => "io-error",
    });
    if outcome.is_err() && reason == "io-error" && slot.is_some() {
        shared.error("response-io-error");
    }
    let _ = socket.shutdown(Shutdown::Both);
    let mut state = shared.lock();
    state.sockets[index].take();
    state.snapshot["terminations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"slot":slot,"reason":reason}));
}

#[cfg(test)]
mod tests {
    use crate::net::ReadinessOrigin;
    use serde_json::json;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    const TOKEN: &str = "readiness-test";

    fn connect(origin: &ReadinessOrigin, slot: usize, token: &str) -> TcpStream {
        let url = origin.url(slot);
        let authority = url
            .strip_prefix("http://")
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        let mut socket = TcpStream::connect(authority).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let path = url.strip_prefix(&format!("http://{authority}")).unwrap();
        write!(
            socket,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nx-readiness-token: {token}\r\n\r\n"
        )
        .unwrap();
        socket
    }

    fn head(socket: TcpStream) -> BufReader<TcpStream> {
        let mut reader = BufReader::new(socket);
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            if line == "\r\n" {
                return reader;
            }
        }
    }

    fn await_count(origin: &ReadinessOrigin, field: &str, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = origin.snapshot();
            let observed = if field == "accepted" {
                snapshot[field].as_u64().unwrap() as usize
            } else {
                snapshot[field].as_array().unwrap().len()
            };
            if observed >= count {
                return;
            }
            assert!(Instant::now() < deadline, "missing {field}: {snapshot}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn finite_origins_retain_correlated_receipts_and_written_bytes() {
        for (case, suffix, body_bytes) in [
            ("eof", "\r\n\r\nxyz", 3),
            ("empty-eof", "\r\n\r\n", 0),
            ("truncated", "\r\n\r\nx", 1),
        ] {
            let origin = ReadinessOrigin::start(case, TOKEN).unwrap();
            let mut socket = connect(&origin, 0, TOKEN);
            let mut wire = String::new();
            let read = socket.read_to_string(&mut wire);
            let snapshot = origin.finish();
            read.unwrap();
            assert!(wire.ends_with(suffix), "{case}: {wire}");
            assert_eq!(snapshot["case"], case);
            assert_eq!(snapshot["token"], TOKEN);
            assert_eq!(snapshot["accepted"], 1);
            assert_eq!(snapshot["workers_joined"], 1);
            assert_eq!(snapshot["max_workers"], 1);
            assert_eq!(snapshot["harness_shutdown"], true);
            assert_eq!(snapshot["watchdog"], false);
            assert_eq!(snapshot["errors"], json!([]));
            assert_eq!(snapshot["requests"].as_array().unwrap().len(), 1);
            assert_eq!(
                snapshot["requests"][0]["path"],
                format!("/readiness/{case}/0")
            );
            assert_eq!(snapshot["requests"][0]["slot"], 0);
            assert_eq!(snapshot["requests"][0]["token"], TOKEN);
            assert!(snapshot["requests"][0]["received_us"].is_u64());
            let body_written: u64 = snapshot["writes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|write| write["kind"] == "body")
                .map(|write| write["bytes"].as_u64().unwrap())
                .sum();
            assert_eq!(body_written, body_bytes);
            assert_eq!(
                snapshot["terminations"],
                json!([{ "slot":0, "reason":"complete" }])
            );
        }
    }

    #[test]
    fn pending_origin_finish_and_drop_close_sockets_and_join_workers() {
        for case in ["silent-head", "pending-body", "exhausted"] {
            for finish in [false, true] {
                let origin = ReadinessOrigin::start(case, TOKEN).unwrap();
                let mut socket = connect(&origin, 0, TOKEN);
                await_count(&origin, "requests", 1);
                let started = Instant::now();
                if finish {
                    let snapshot = origin.finish();
                    assert_eq!(snapshot["workers_joined"], 1);
                    assert_eq!(snapshot["terminations"][0]["reason"], "harness-stop");
                    assert_eq!(snapshot["errors"], json!([]));
                } else {
                    drop(origin);
                }
                assert!(started.elapsed() < Duration::from_secs(1));
                let mut remaining = Vec::new();
                socket.read_to_end(&mut remaining).unwrap();
            }
        }
    }

    #[test]
    fn siblings_flush_both_heads_before_only_slot_one_completes() {
        let origin = ReadinessOrigin::start("siblings", TOKEN).unwrap();
        let ready = connect(&origin, 1, TOKEN);
        let mut ready = head(ready);
        ready
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(30)))
            .unwrap();
        assert!(ready.read(&mut [0]).is_err());
        ready
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut pending = head(connect(&origin, 0, TOKEN));
        let mut body = String::new();
        ready.read_to_string(&mut body).unwrap();
        assert_eq!(body, "xyz");
        let snapshot = origin.finish();
        assert_eq!(snapshot["accepted"], 2);
        assert_eq!(snapshot["workers_joined"], 2);
        assert_eq!(snapshot["max_workers"], 2);
        assert_eq!(snapshot["errors"], json!([]));
        let writes = snapshot["writes"].as_array().unwrap();
        assert_eq!(writes[0]["kind"], "head");
        assert_eq!(writes[1]["kind"], "head");
        assert_eq!(writes[2]["slot"], 1);
        assert_eq!(writes[2]["kind"], "body");
        assert_eq!(pending.read(&mut [0]).unwrap(), 0);
    }

    #[test]
    fn malformed_and_head_error_origins_keep_distinct_wire_shapes() {
        for (case, expected) in [
            ("head-error", ""),
            ("malformed-head", "NOT HTTP\r\n\r\n"),
            (
                "malformed-body",
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\nx\r\nZ\r\n",
            ),
        ] {
            let origin = ReadinessOrigin::start(case, TOKEN).unwrap();
            let mut socket = connect(&origin, 0, TOKEN);
            let mut wire = String::new();
            let read = socket.read_to_string(&mut wire);
            let snapshot = origin.finish();
            read.unwrap();
            assert_eq!(wire, expected);
            assert_eq!(snapshot["errors"], json!([]));
            assert_eq!(snapshot["terminations"][0]["reason"], "complete");
        }
    }

    #[test]
    fn trickle_uses_absolute_interruptible_three_hundred_ms_intervals() {
        let origin = ReadinessOrigin::start("trickle", TOKEN).unwrap();
        let mut reader = head(connect(&origin, 0, TOKEN));
        let mut body = String::new();
        reader.read_to_string(&mut body).unwrap();
        let snapshot = origin.finish();
        assert_eq!(body, "xyz");
        let writes: Vec<_> = snapshot["writes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|write| write["kind"] == "body")
            .collect();
        assert_eq!(writes.len(), 3);
        let first = writes[0]["at_us"].as_u64().unwrap();
        assert!(writes[1]["at_us"].as_u64().unwrap() >= first + 295_000);
        assert!(writes[2]["at_us"].as_u64().unwrap() >= first + 595_000);
        assert_eq!(snapshot["errors"], json!([]));
    }

    #[test]
    fn trickle_shutdown_interrupts_the_next_scheduled_write() {
        let origin = ReadinessOrigin::start("trickle", TOKEN).unwrap();
        let mut reader = head(connect(&origin, 0, TOKEN));
        let mut first = [0];
        reader.read_exact(&mut first).unwrap();
        assert_eq!(&first, b"x");
        let started = Instant::now();
        let snapshot = origin.finish();
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(snapshot["workers_joined"], 1);
        assert_eq!(snapshot["terminations"][0]["reason"], "harness-stop");
        assert_eq!(
            snapshot["writes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|write| write["kind"] == "body")
                .count(),
            1
        );
        assert_eq!(reader.read(&mut [0]).unwrap(), 0);
    }

    #[test]
    fn watchdog_expiry_is_retained_as_failure_and_joins_the_pending_worker() {
        let started = Instant::now();
        let origin = ReadinessOrigin::start("pending-body", TOKEN).unwrap();
        let mut reader = head(connect(&origin, 0, TOKEN));
        reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let end = reader.read(&mut [0]);
        let snapshot = origin.finish();
        assert_eq!(end.unwrap(), 0);
        assert!(started.elapsed() >= Duration::from_secs(3));
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(snapshot["watchdog"], true);
        assert_eq!(snapshot["harness_shutdown"], true);
        assert_eq!(snapshot["workers_joined"], 1);
        assert_eq!(snapshot["terminations"][0]["reason"], "watchdog");
        assert!(
            snapshot["errors"]
                .as_array()
                .unwrap()
                .contains(&json!("origin-watchdog"))
        );
    }

    #[test]
    fn held_origin_records_peer_close_separately_from_harness_shutdown() {
        let origin = ReadinessOrigin::start("pending-body", TOKEN).unwrap();
        let reader = head(connect(&origin, 0, TOKEN));
        reader.get_ref().shutdown(Shutdown::Both).unwrap();
        await_count(&origin, "terminations", 1);
        let snapshot = origin.finish();
        assert_eq!(snapshot["terminations"][0]["reason"], "peer-close");
        assert_eq!(snapshot["workers_joined"], 1);
        assert_eq!(snapshot["watchdog"], false);
        assert_eq!(snapshot["errors"], json!([]));
    }

    #[test]
    fn unexpected_connection_is_retained_without_spawning_an_extra_worker() {
        let origin = ReadinessOrigin::start("eof", TOKEN).unwrap();
        connect(&origin, 0, TOKEN)
            .read_to_end(&mut Vec::new())
            .unwrap();
        let _extra = connect(&origin, 0, TOKEN);
        await_count(&origin, "accepted", 2);
        let snapshot = origin.finish();
        assert_eq!(snapshot["accepted"], 2);
        assert_eq!(snapshot["workers_joined"], 1);
        assert!(
            snapshot["errors"]
                .as_array()
                .unwrap()
                .contains(&json!("unexpected-connection"))
        );
        assert_eq!(snapshot["terminations"].as_array().unwrap().len(), 2);
        assert!(
            snapshot["terminations"]
                .as_array()
                .unwrap()
                .contains(&json!({"slot":null,"reason":"io-error"}))
        );
    }

    #[test]
    fn partial_and_oversized_heads_are_bounded_and_do_not_leak_headers() {
        for oversized in [false, true] {
            let origin = ReadinessOrigin::start("eof", TOKEN).unwrap();
            let url = origin.url(0);
            let authority = url
                .strip_prefix("http://")
                .unwrap()
                .split('/')
                .next()
                .unwrap();
            let mut socket = TcpStream::connect(authority).unwrap();
            socket
                .write_all(b"GET /readiness/eof/0 HTTP/1.1\r\nx-private: private-secret\r\n")
                .unwrap();
            if oversized {
                socket.write_all(&[b'x'; 4096]).unwrap();
                await_count(&origin, "terminations", 1);
            } else {
                await_count(&origin, "accepted", 1);
            }
            let started = Instant::now();
            let snapshot = origin.finish();
            assert!(started.elapsed() < Duration::from_secs(1));
            assert_eq!(snapshot["accepted"], 1);
            assert_eq!(snapshot["workers_joined"], 1);
            assert!(!snapshot["errors"].as_array().unwrap().is_empty());
            assert_eq!(snapshot["requests"], json!([]));
            assert!(!snapshot.to_string().contains("private-secret"));
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    #[test]
    fn foreign_tokens_and_duplicate_sibling_slots_fail_without_echoing_input() {
        let origin = ReadinessOrigin::start("eof", TOKEN).unwrap();
        let _socket = connect(&origin, 0, "private-secret");
        await_count(&origin, "terminations", 1);
        let snapshot = origin.finish();
        assert_eq!(snapshot["requests"], json!([]));
        assert!(
            snapshot["errors"]
                .as_array()
                .unwrap()
                .contains(&json!("request-token-mismatch"))
        );
        assert!(!snapshot.to_string().contains("private-secret"));

        let origin = ReadinessOrigin::start("siblings", TOKEN).unwrap();
        let _first = head(connect(&origin, 0, TOKEN));
        let _second = connect(&origin, 0, TOKEN);
        await_count(&origin, "terminations", 1);
        let snapshot = origin.finish();
        assert_eq!(snapshot["workers_joined"], 2);
        assert_eq!(snapshot["requests"].as_array().unwrap().len(), 1);
        assert!(
            snapshot["errors"]
                .as_array()
                .unwrap()
                .contains(&json!("duplicate-slot"))
        );
    }
}
