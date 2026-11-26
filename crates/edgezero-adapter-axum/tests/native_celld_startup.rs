//! Deterministic loopback HTTP fixtures through the public native runner.
//! This is not real celld, the store service, TLS, fleet, persistence or
//! generated-consumer acceptance.

#![cfg(feature = "celld-kv")]
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "fixture helpers precede startup scenarios"
)]

#[cfg(test)]
#[path = "fixtures/blocking_accept.rs"]
mod blocking_accept;

#[cfg(test)]
mod tests {
    use crate::blocking_accept::accept_blocking;
    use std::fs;
    use std::io::{ErrorKind, Read, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use serde_json::json;

    const WAIT: Duration = Duration::from_secs(12);

    struct ScopedFixture {
        address: SocketAddr,
        value: Arc<Mutex<(u16, Vec<u8>)>>,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl ScopedFixture {
        fn new(initial_status: u16, initial_value: Vec<u8>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("loopback fixture");
            listener.set_nonblocking(true).expect("nonblocking fixture");
            let address = listener.local_addr().expect("fixture address");
            let value = Arc::new(Mutex::new((initial_status, initial_value)));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let worker = {
                let worker_value = Arc::clone(&value);
                let worker_requests = Arc::clone(&requests);
                let worker_stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let deadline = Instant::now().checked_add(WAIT).expect("fixture deadline");
                    while !worker_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                        match accept_blocking(&listener) {
                            Ok((mut stream, peer)) => {
                                assert!(peer.ip().is_loopback());
                                stream
                                    .set_read_timeout(Some(Duration::from_secs(2)))
                                    .expect("bounded read");
                                stream
                                    .set_write_timeout(Some(Duration::from_secs(2)))
                                    .expect("bounded write");
                                let request = read_request(&mut stream);
                                assert!(!request.to_ascii_lowercase().contains("authorization:"));
                                let line = request.lines().next().expect("request line").to_owned();
                                worker_requests
                                    .lock()
                                    .expect("request log")
                                    .push(line.clone());
                                assert!(
                                    line.starts_with(
                                        "GET /v1/stores/state-fixture/value?key=value "
                                    ),
                                    "unexpected store request"
                                );
                                let (status, body) = worker_value.lock().expect("value").clone();
                                let content_type = if status == 200 {
                                    "application/octet-stream"
                                } else {
                                    "application/json"
                                };
                                // Failure tests may close early after checking Content-Length.
                                let _sent = write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nx-edgezero-store-version: 1\r\nx-edgezero-store-instance: AAAAAAAAAAAAAAAAAAAAAA\r\nConnection: close\r\n\r\n", body.len())
                                .and_then(|()| stream.write_all(&body));
                            }
                            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(5));
                            }
                            Err(error) => panic!("fixture accept failed: {error}"),
                        }
                    }
                })
            };
            Self {
                address,
                value,
                requests,
                stop,
                worker: Some(worker),
            }
        }

        fn endpoint(&self) -> String {
            format!("http://{}", self.address)
        }

        fn count(&self) -> usize {
            self.requests.lock().expect("request log").len()
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for ScopedFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(worker) = self.worker.take() {
                worker.join().expect("fixture thread");
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1_024];
        while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut buffer).expect("request read");
            assert_ne!(count, 0, "complete request headers");
            bytes.extend_from_slice(buffer.get(..count).expect("read fits buffer"));
            assert!(bytes.len() <= 0x4000, "bounded request");
        }
        String::from_utf8(bytes).expect("HTTP fixture request")
    }

    struct Host {
        child: Child,
        address: SocketAddr,
        readers: Vec<JoinHandle<Vec<u8>>>,
    }

    impl Host {
        fn spawn(root: &Path, bindings: &Path, initializer_gate: bool, supply_state: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("reserve app address");
            let address = listener.local_addr().expect("app address");
            drop(listener);
            let mut command = Command::new(env!("CARGO_BIN_EXE_edgezero-native-celld-host"));
            command
                .env_clear()
                .current_dir(root)
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env("EDGEZERO__STORE_BINDINGS_FILE", bindings)
                .env("EDGEZERO__ADAPTER__HOST", "127.0.0.1")
                .env("EDGEZERO__ADAPTER__PORT", address.port().to_string())
                .env("EDGEZERO__LOGGING__LEVEL", "off")
                .env(
                    "FIXTURE_INITIALIZER_ENTERED",
                    root.join("initializer-entered"),
                )
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if initializer_gate {
                command.env("FIXTURE_INITIALIZER_GATE", root.join("initializer-release"));
            }
            if supply_state {
                command.env("FIXTURE_DEFERRED_MARKER", root.join("deferred-polled"));
            }
            let mut child = command.spawn().expect("public runner child");
            let streams: [Box<dyn Read + Send>; 2] = [
                Box::new(child.stdout.take().expect("stdout")),
                Box::new(child.stderr.take().expect("stderr")),
            ];
            let readers = streams
                .into_iter()
                .map(|mut stream| {
                    thread::spawn(move || {
                        let mut bytes = Vec::new();
                        stream.read_to_end(&mut bytes).expect("child output");
                        bytes
                    })
                })
                .collect();
            Self {
                child,
                address,
                readers,
            }
        }

        fn no_listener(&self) {
            TcpStream::connect_timeout(&self.address, Duration::from_millis(40))
                .expect_err("no listener before readiness");
        }

        fn get(&mut self, path: &str) -> String {
            let deadline = Instant::now()
                .checked_add(WAIT)
                .expect("readiness deadline");
            loop {
                assert!(
                    self.child.try_wait().expect("child status").is_none(),
                    "host exited before readiness"
                );
                if let Ok(mut stream) =
                    TcpStream::connect_timeout(&self.address, Duration::from_millis(40))
                {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("bounded app read");
                    write!(
                        stream,
                        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
                    )
                    .expect("app request");
                    let mut response = String::new();
                    stream.read_to_string(&mut response).expect("app response");
                    assert!(
                        response.starts_with("HTTP/1.1 200"),
                        "successful fixture response"
                    );
                    return response
                        .split_once("\r\n\r\n")
                        .expect("response headers")
                        .1
                        .to_owned();
                }
                assert!(Instant::now() < deadline, "host readiness timeout");
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn failure(&mut self) -> String {
            let deadline = Instant::now().checked_add(WAIT).expect("failure deadline");
            loop {
                if let Some(status) = self.child.try_wait().expect("child status") {
                    assert!(!status.success(), "expected startup failure");
                    self.child.wait().expect("reap failed host");
                    let mut bytes = Vec::new();
                    for reader in self.readers.drain(..) {
                        bytes.extend(reader.join().expect("output reader"));
                    }
                    return String::from_utf8_lossy(&bytes).into_owned();
                }
                assert!(Instant::now() < deadline, "startup failure timeout");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "`Drop::pin_drop` is unstable (`pin_ergonomics`) and conflicts with `drop`"
    )]
    impl Drop for Host {
        fn drop(&mut self) {
            if self.child.try_wait().ok().flatten().is_none() {
                let _killed = self.child.kill();
            }
            let _reaped = self.child.wait();
            for reader in self.readers.drain(..) {
                let _joined = reader.join();
            }
        }
    }

    fn wait_until<Ready>(ready: Ready, label: &str)
    where
        Ready: Fn() -> bool,
    {
        let deadline = Instant::now().checked_add(WAIT).expect("fixture deadline");
        while !ready() {
            assert!(Instant::now() < deadline, "timeout waiting for {label}");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn bindings(endpoint: &str) -> String {
        format!(
            "version=1
[kv.state]
provider='celld-kv'
endpoint='{endpoint}'
store='state-fixture'
[kv.state.access]
mode='trusted-network'
[kv.other]
provider='celld-kv'
endpoint='{endpoint}'
store='other-fixture'
[kv.other.access]
mode='trusted-network'
"
        )
    }

    #[test]
    fn public_runner_prepares_without_probe_and_initializer_reads_live_kv() {
        let root = tempfile::tempdir().expect("isolated child root");
        let fixture = ScopedFixture::new(200, b"first-value".to_vec());
        let path = root.path().join("bindings.toml");
        fs::write(&path, bindings(&fixture.endpoint())).expect("bindings");
        let mut host = Host::spawn(root.path(), &path, true, false);
        wait_until(
            || root.path().join("initializer-entered").is_file(),
            "initializer entry",
        );
        // File settings were captured before the initializer; later edits cannot change sources.
        fs::write(&path, "invalid later file contents").expect("replace captured file");
        host.no_listener();
        assert_eq!(fixture.count(), 0, "KV setup performs no remote probe");
        fs::write(root.path().join("initializer-release"), b"ready").expect("release initializer");
        assert_eq!(host.get("/startup"), "first-value");
        assert_eq!(fixture.count(), 1);
        *fixture.value.lock().expect("edit remote value") = (200, b"second-value".to_vec());
        assert_eq!(host.get("/kv"), "second-value");
        assert_eq!(host.get("/startup"), "first-value");
        assert_eq!(
            fixture.count(),
            2,
            "KV reads are live; state is the app's own"
        );
    }

    #[test]
    fn failed_initializer_read_never_listens_or_falls_back() {
        let root = tempfile::tempdir().expect("root");
        let fixture = ScopedFixture::new(
            503,
            json!({"error": "unavailable"}).to_string().into_bytes(),
        );
        let path = root.path().join("bindings.toml");
        fs::write(&path, bindings(&fixture.endpoint())).expect("bindings");
        let mut host = Host::spawn(root.path(), &path, false, false);
        let error = host.failure();
        host.no_listener();
        assert_eq!(fixture.count(), 1);
        assert!(!error.contains("state-fixture"));
        assert!(!error.contains(&fixture.endpoint()));
    }

    #[test]
    fn supplied_remote_source_conflict_never_polls_or_fetches() {
        let root = tempfile::tempdir().expect("root");
        let fixture = ScopedFixture::new(200, b"unused".to_vec());
        let path = root.path().join("bindings.toml");
        fs::write(&path, bindings(&fixture.endpoint())).expect("bindings");
        let mut host = Host::spawn(root.path(), &path, false, true);
        let error = host.failure();
        host.no_listener();
        assert!(error.contains("duplicate"));
        assert_eq!(fixture.count(), 0);
        assert!(!root.path().join("initializer-entered").exists());
        assert!(!root.path().join("deferred-polled").exists());
    }

    #[test]
    fn invalid_later_kv_credentials_fail_before_any_request() {
        let root = tempfile::tempdir().expect("root");
        let fixture = ScopedFixture::new(200, b"unused".to_vec());
        let path = root.path().join("bindings.toml");
        let input = bindings(&fixture.endpoint()).replace(
            &format!(
                "endpoint='{}'\nstore='other-fixture'\n[kv.other.access]\nmode='trusted-network'",
                fixture.endpoint()
            ),
            "endpoint='https://unreachable.invalid'\nstore='other-fixture'\n[kv.other.access]\nmode='authenticated'\ntoken_env='ABSENT_TOKEN'",
        );
        assert!(input.contains("ABSENT_TOKEN"));
        fs::write(&path, input).expect("bindings");
        let mut host = Host::spawn(root.path(), &path, false, false);
        let error = host.failure();
        host.no_listener();
        assert!(error.contains("celld token"));
        assert!(!root.path().join("initializer-entered").exists());
        assert_eq!(fixture.count(), 0);
        assert!(!error.contains("ABSENT_TOKEN"));
        assert!(!error.contains("unreachable.invalid"));
    }
}
