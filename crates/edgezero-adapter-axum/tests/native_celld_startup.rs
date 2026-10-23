//! Deterministic loopback HTTP fixtures through the public native runner.
//! This is not real celld, TLS, fleet, persistence or generated-consumer acceptance.

#![cfg(all(feature = "celld-config", feature = "celld-kv"))]
#![expect(
    clippy::arbitrary_source_item_ordering,
    reason = "fixture helpers precede startup scenarios"
)]

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{ErrorKind, Read, Write as _};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use edgezero_core::blob_envelope::BlobEnvelope;
    use serde_json::json;

    const WAIT: Duration = Duration::from_secs(12);

    struct ScopedFixture {
        address: SocketAddr,
        config: Arc<Mutex<(u16, Vec<u8>)>>,
        gate: Arc<AtomicBool>,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl ScopedFixture {
        fn new(config_status: u16, document: Vec<u8>, gated: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("loopback fixture");
            listener.set_nonblocking(true).expect("nonblocking fixture");
            let address = listener.local_addr().expect("fixture address");
            let config = Arc::new(Mutex::new((config_status, document)));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let gate = Arc::new(AtomicBool::new(!gated));
            let stop = Arc::new(AtomicBool::new(false));
            let worker = {
                let worker_config = Arc::clone(&config);
                let worker_requests = Arc::clone(&requests);
                let worker_gate = Arc::clone(&gate);
                let worker_stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let deadline = Instant::now().checked_add(WAIT).expect("fixture deadline");
                    while !worker_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                        match listener.accept() {
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
                                let (status, body) = if line.starts_with(
                                    "GET /v1/stores/config-fixture/value?key=settings ",
                                ) {
                                    while !worker_gate.load(Ordering::Relaxed)
                                        && !worker_stop.load(Ordering::Relaxed)
                                        && Instant::now() < deadline
                                    {
                                        thread::sleep(Duration::from_millis(5));
                                    }
                                    worker_config.lock().expect("document").clone()
                                } else {
                                    assert!(
                                        line.starts_with(
                                            "GET /v1/stores/state-fixture/value?key=value "
                                        ),
                                        "unexpected scoped request"
                                    );
                                    (200, b"live-value".to_vec())
                                };
                                let content_type = if status == 200 {
                                    "application/octet-stream"
                                } else {
                                    "application/json"
                                };
                                // Failure tests may close early after checking Content-Length.
                                let _sent = write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nContent-Type: {content_type}\r\nx-celld-store-version: 1\r\nConnection: close\r\n\r\n", body.len())
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
                config,
                gate,
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

        fn wait_request(&self) {
            wait_until(|| self.count() != 0, "scoped request");
        }
    }

    #[expect(
        clippy::missing_trait_methods,
        reason = "Drop::pin_drop is unstable and conflicts with drop"
    )]
    impl Drop for ScopedFixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.gate.store(true, Ordering::Relaxed);
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
        reason = "Drop::pin_drop is unstable and conflicts with drop"
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

    fn envelope(greeting: &str) -> Vec<u8> {
        serde_json::to_vec(&BlobEnvelope::new(
            json!({"greeting": greeting}),
            "2026-10-15T12:00:00Z".to_owned(),
        ))
        .expect("typed config envelope")
    }

    fn bindings(endpoint: &str) -> String {
        format!(
            "version=1
[config.app]
provider='celld-config'
endpoint='{endpoint}'
default_key='settings'
[config.app.access]
mode='trusted-network'
[config.app.documents.settings]
store='config-fixture'
key='settings'
[kv.state]
provider='celld-kv'
endpoint='{endpoint}'
store='state-fixture'
[kv.state.access]
mode='trusted-network'
"
        )
    }

    #[test]
    fn public_runner_gates_preparation_and_initializer_then_keeps_snapshot_and_live_kv() {
        let root = tempfile::tempdir().expect("isolated child root");
        let fixture = ScopedFixture::new(200, envelope("first-greeting"), true);
        let path = root.path().join("bindings.toml");
        fs::write(&path, bindings(&fixture.endpoint())).expect("bindings");
        let mut host = Host::spawn(root.path(), &path, true, false);
        fixture.wait_request();
        host.no_listener();
        assert!(!root.path().join("initializer-entered").exists());
        // File settings were captured before the gated fetch; later edits cannot change sources/defaults.
        fs::write(&path, "invalid later file contents").expect("replace captured file");
        fixture.gate.store(true, Ordering::Relaxed);
        wait_until(
            || root.path().join("initializer-entered").is_file(),
            "initializer entry",
        );
        host.no_listener();
        assert_eq!(fixture.count(), 1, "KV setup performs no remote probe");
        fs::write(root.path().join("initializer-release"), b"ready").expect("release initializer");
        assert_eq!(host.get("/snapshot"), "first-greeting");
        *fixture.config.lock().expect("edit remote document") = (200, envelope("second-greeting"));
        assert_eq!(host.get("/snapshot"), "first-greeting");
        assert_eq!(fixture.count(), 1, "snapshot reads never fetch again");
        assert_eq!(host.get("/kv"), "live-value");
        assert_eq!(host.get("/kv"), "live-value");
        assert_eq!(fixture.count(), 3);
        drop(host);
        fs::write(&path, bindings(&fixture.endpoint())).expect("fresh process bindings");
        let mut restarted = Host::spawn(root.path(), &path, false, false);
        assert_eq!(restarted.get("/snapshot"), "second-greeting");
        assert_eq!(fixture.count(), 4, "fresh preparation adopts remote edits");
    }

    #[test]
    fn config_failures_never_initialize_listen_or_fall_back() {
        for (status, document, policy, category) in [
            (
                404,
                json!({"error": "not_found"}).to_string().into_bytes(),
                "",
                "missing",
            ),
            (200, vec![0xff], "", "malformed"),
            (
                200,
                envelope("document-too-large"),
                "\n[limits.celld]\nmax_config_document_bytes=4\n",
                "size limit",
            ),
        ] {
            let root = tempfile::tempdir().expect("root");
            let fixture = ScopedFixture::new(status, document, false);
            let path = root.path().join("bindings.toml");
            fs::write(&path, format!("{}{policy}", bindings(&fixture.endpoint())))
                .expect("bindings");
            let mut host = Host::spawn(root.path(), &path, false, false);
            let error = host.failure();
            host.no_listener();
            assert!(!root.path().join("initializer-entered").exists());
            assert_eq!(fixture.count(), 1);
            assert!(error.contains(category), "closed failure category");
            assert!(!error.contains("config-fixture"));
            assert!(!error.contains(&fixture.endpoint()));
        }
    }

    #[test]
    fn supplied_remote_source_conflict_never_polls_or_fetches() {
        let root = tempfile::tempdir().expect("root");
        let fixture = ScopedFixture::new(200, envelope("unused-greeting"), false);
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
    fn invalid_later_kv_credentials_fail_before_any_config_request() {
        let root = tempfile::tempdir().expect("root");
        let fixture = ScopedFixture::new(200, envelope("unused-greeting"), false);
        let path = root.path().join("bindings.toml");
        let input = bindings(&fixture.endpoint())
            .replace(
                &format!(
                    "[kv.state]\nprovider='celld-kv'\nendpoint='{}'",
                    fixture.endpoint()
                ),
                "[kv.state]\nprovider='celld-kv'\nendpoint='https://unreachable.invalid'",
            )
            .replace(
                "[kv.state.access]\nmode='trusted-network'",
                "[kv.state.access]\nmode='authenticated'\ntoken_env='ABSENT_TOKEN'",
            );
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
