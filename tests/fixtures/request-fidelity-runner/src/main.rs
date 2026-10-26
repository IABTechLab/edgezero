//! Fixed, loopback-only HTTP ingress probes for the three WASM adapters.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::Seek as _;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tokio::signal::unix::{SignalKind, signal};

const RESPONSE_LIMIT: usize = 1024 * 1024;
const HEADER_LIMIT: usize = 64 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
type Cancelled = Arc<AtomicBool>;

struct Options {
    platforms: Vec<&'static str>,
    skip_build: bool,
    readiness_timeout: Duration,
    record_observations: bool,
    root: PathBuf,
}

impl Options {
    fn parse() -> io::Result<Self> {
        let mut args = env::args().skip(1);
        let first = args.next();
        let (root, platform) = if first.as_deref() == Some("--repo-root") {
            let root = PathBuf::from(
                args.next()
                    .ok_or_else(|| io::Error::other("missing repository root"))?,
            )
            .canonicalize()?;
            (root, args.next())
        } else {
            (
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../..")
                    .canonicalize()?,
                first,
            )
        };
        let platforms = match platform.as_deref() {
            Some("all") => vec!["fastly", "cloudflare", "spin"],
            Some("fastly") => vec!["fastly"],
            Some("cloudflare") => vec!["cloudflare"],
            Some("spin") => vec!["spin"],
            _ => {
                return Err(io::Error::other(
                    "usage: request-fidelity-runner <all|fastly|cloudflare|spin> [--skip-build] [--readiness-timeout SECONDS] [--record-observations]",
                ));
            }
        };
        let mut options = Self {
            platforms,
            skip_build: false,
            readiness_timeout: Duration::from_secs(40),
            record_observations: false,
            root,
        };
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--repo-root" => {
                    options.root = PathBuf::from(
                        args.next()
                            .ok_or_else(|| io::Error::other("missing repository root"))?,
                    )
                    .canonicalize()?;
                }
                "--skip-build" => options.skip_build = true,
                "--record-observations" => options.record_observations = true,
                "--readiness-timeout" => {
                    let seconds = args
                        .next()
                        .ok_or_else(|| io::Error::other("missing readiness timeout"))?
                        .parse::<f64>()
                        .map_err(io::Error::other)?;
                    options.readiness_timeout =
                        Duration::try_from_secs_f64(seconds).map_err(io::Error::other)?;
                    if options.readiness_timeout.is_zero() {
                        return Err(io::Error::other("readiness timeout must be positive"));
                    }
                }
                _ => return Err(io::Error::other(format!("unknown argument: {argument}"))),
            }
        }
        Ok(options)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _result = writeln!(io::stderr(), "request fidelity: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute() -> io::Result<()> {
    let options = Options::parse()?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    let mut worker = tokio::task::spawn_blocking(move || run(options, &worker_cancelled));
    tokio::select! {
        result = &mut worker => result.map_err(io::Error::other)?,
        _ = interrupt.recv() => {
            cancelled.store(true, Ordering::SeqCst);
            let _result = worker.await;
            Err(io::Error::new(io::ErrorKind::Interrupted, "SIGINT: owned children stopped"))
        }
        _ = terminate.recv() => {
            cancelled.store(true, Ordering::SeqCst);
            let _result = worker.await;
            Err(io::Error::new(io::ErrorKind::Interrupted, "SIGTERM: owned children stopped"))
        }
        _ = hangup.recv() => {
            cancelled.store(true, Ordering::SeqCst);
            let _result = worker.await;
            Err(io::Error::new(io::ErrorKind::Interrupted, "SIGHUP: owned children stopped"))
        }
    }
}

fn check_cancelled(cancelled: &Cancelled) -> io::Result<()> {
    if cancelled.load(Ordering::SeqCst) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "interrupted"))
    } else {
        Ok(())
    }
}

struct OwnedProcess {
    child: Child,
    group: u32,
}

impl OwnedProcess {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        command.process_group(0).stdin(Stdio::null());
        let child = command.spawn().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("{}: {error}", command.get_program().to_string_lossy()),
            )
        })?;
        let group = child.id();
        Ok(Self { child, group })
    }

    fn wait(&mut self, cancelled: &Cancelled) -> io::Result<()> {
        loop {
            check_cancelled(cancelled)?;
            if let Some(status) = self.child.try_wait()? {
                return if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "fixture command exits with {status}"
                    )))
                };
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

fn signal_group(group: u32, name: &str) {
    let _result = Command::new("/bin/kill")
        .args([name, "--", &format!("-{group}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        // Signal the owned group even if its direct wrapper already exited.
        signal_group(self.group, "-TERM");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.child.try_wait().is_ok_and(|status| status.is_some())
                || Instant::now() >= deadline
            {
                break;
            }
            thread::sleep(POLL_INTERVAL);
        }
        signal_group(self.group, "-KILL");
        // Also stop the direct child if the external group signal failed.
        let _result = self.child.kill();
        let _result = self.child.wait();
    }
}

fn command(fixture: &Path, program: &str, arguments: &[&str]) -> Command {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(fixture)
        .env("WRANGLER_SEND_METRICS", "false");
    command
}

fn configure_worker_build(
    command: &mut Command,
    fixture: &Path,
    bindgen: &Path,
    cancelled: &Cancelled,
) -> io::Result<()> {
    let lock: toml::Value = toml::from_str(&fs::read_to_string(fixture.join("Cargo.lock"))?)
        .map_err(io::Error::other)?;
    let versions: Vec<_> = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|package| package.get("name").and_then(toml::Value::as_str) == Some("wasm-bindgen"))
        .filter_map(|package| package.get("version").and_then(toml::Value::as_str))
        .collect();
    let [version] = versions.as_slice() else {
        return Err(io::Error::other(
            "fixture lock requires one wasm-bindgen version",
        ));
    };
    let bindgen = bindgen.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("wasm-bindgen {}: {error}", bindgen.display()),
        )
    })?;
    verify_version(fixture, &bindgen.to_string_lossy(), version, cancelled)?;
    let esbuild = fixture.join("node_modules/.bin/esbuild");
    let esbuild = esbuild.canonicalize().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "npm-locked esbuild {}: {error}; run npm ci",
                esbuild.display()
            ),
        )
    })?;
    let npm_lock: Value = serde_json::from_slice(&fs::read(fixture.join("package-lock.json"))?)
        .map_err(io::Error::other)?;
    let version = npm_lock
        .pointer("/packages/node_modules~1esbuild/version")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("missing npm-locked esbuild version"))?;
    let actual = run_command(
        fixture,
        &esbuild.to_string_lossy(),
        &["--version"],
        cancelled,
        true,
    )?;
    if actual.trim() != version {
        return Err(io::Error::other(format!(
            "esbuild requires version {version}; found {:?}",
            actual.trim()
        )));
    }
    command
        .env("WASM_BINDGEN_BIN", bindgen)
        .env("ESBUILD_BIN", esbuild);
    Ok(())
}

fn run_command(
    fixture: &Path,
    program: &str,
    arguments: &[&str],
    cancelled: &Cancelled,
    capture: bool,
) -> io::Result<String> {
    let mut command = command(fixture, program, arguments);
    run_prepared_command(&mut command, cancelled, capture)
}

fn run_prepared_command(
    command: &mut Command,
    cancelled: &Cancelled,
    capture: bool,
) -> io::Result<String> {
    let description = std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|value| value.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    let program_prefix = format!("{}: ", command.get_program().to_string_lossy());
    let describe = |error: io::Error| {
        let message = error.to_string();
        let message = message.strip_prefix(&program_prefix).unwrap_or(&message);
        io::Error::new(error.kind(), format!("{description}: {message}"))
    };
    check_cancelled(cancelled).map_err(describe)?;
    if !capture {
        use std::os::fd::AsFd as _;
        command.stdout(Stdio::from(
            io::stderr()
                .as_fd()
                .try_clone_to_owned()
                .map_err(describe)?,
        ));
        command.stderr(Stdio::inherit());
        return OwnedProcess::spawn(command)
            .map_err(describe)?
            .wait(cancelled)
            .map(|()| String::new())
            .map_err(describe);
    }
    // A file avoids pipe backpressure while waiting interruptibly for build tools.
    let mut output = tempfile::tempfile().map_err(describe)?;
    let mut stderr = tempfile::tempfile().map_err(describe)?;
    command.stdout(output.try_clone().map_err(describe)?);
    command.stderr(Stdio::from(stderr.try_clone().map_err(describe)?));
    let result = OwnedProcess::spawn(command)
        .map_err(describe)?
        .wait(cancelled);
    output.rewind().map_err(describe)?;
    let mut bytes = Vec::new();
    output.read_to_end(&mut bytes).map_err(describe)?;
    stderr.rewind().map_err(describe)?;
    let mut diagnostics = Vec::new();
    stderr.read_to_end(&mut diagnostics).map_err(describe)?;
    result.map_err(|error| {
        describe(io::Error::new(
            error.kind(),
            format!(
                "{error}; stdout: {}; stderr: {}",
                String::from_utf8_lossy(&bytes),
                String::from_utf8_lossy(&diagnostics)
            ),
        ))
    })?;
    eprint!("{}", String::from_utf8_lossy(&diagnostics));
    String::from_utf8(bytes).map_err(|error| {
        describe(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{error}; output: {}",
                String::from_utf8_lossy(error.as_bytes())
            ),
        ))
    })
}

fn verify_version(
    fixture: &Path,
    program: &str,
    expected: &str,
    cancelled: &Cancelled,
) -> io::Result<()> {
    let output = run_command(fixture, program, &["--version"], cancelled, true)?;
    let actual = if program == "worker-build" {
        output.trim()
    } else {
        output.split_whitespace().nth(1).unwrap_or("")
    };
    if actual != expected {
        return Err(io::Error::other(format!(
            "{program} requires version {expected}; found {actual:?} (output: {:?})",
            output.trim()
        )));
    }
    Ok(())
}

fn fixture_tool_pin(fixture: &Path, tool: &str) -> io::Result<String> {
    let manifest: toml::Value = toml::from_str(&fs::read_to_string(fixture.join("Cargo.toml"))?)
        .map_err(io::Error::other)?;
    manifest
        .get("package")
        .and_then(|package| package.get("metadata"))
        .and_then(|metadata| metadata.get("request-fidelity"))
        .and_then(|pins| pins.get(tool))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other(format!("missing fixture tool pin: {tool}")))
}

fn resolve_wasm_bindgen(
    override_path: Option<std::ffi::OsString>,
    path: &std::ffi::OsStr,
) -> io::Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt as _;
    let executable =
        |metadata: &fs::Metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0;
    if let Some(path) = override_path {
        let path = PathBuf::from(path);
        let metadata = path.metadata().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("WASM_BINDGEN_BIN {}: {error}", path.display()),
            )
        })?;
        if !executable(&metadata) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "WASM_BINDGEN_BIN {} must be an executable file",
                    path.display()
                ),
            ));
        }
        return Ok(path);
    }
    env::split_paths(path)
        .map(|directory| directory.join("wasm-bindgen"))
        .find(|path| path.metadata().is_ok_and(|metadata| executable(&metadata)))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
            "wasm-bindgen not found on PATH; install the fixture-locked CLI or set WASM_BINDGEN_BIN"))
}

fn build(platform: &str, fixture: &Path, cancelled: &Cancelled) -> io::Result<()> {
    if platform == "cloudflare" {
        let pin = fixture_tool_pin(fixture, "worker-build")?;
        verify_version(fixture, "worker-build", &pin, cancelled)?;
        run_command(fixture, "npm", &["ci"], cancelled, false)?;
        let lock = fs::read(fixture.join("Cargo.lock"))?;
        let mut command = command(
            fixture,
            "worker-build",
            &[
                ".",
                "--release",
                "--no-opt",
                "--out-dir",
                "build/cloudflare",
                "--",
                "--features",
                "cloudflare",
                "--locked",
                "--target-dir",
                "target/cloudflare",
            ],
        );
        let bindgen = resolve_wasm_bindgen(
            env::var_os("WASM_BINDGEN_BIN"),
            &env::var_os("PATH").unwrap_or_default(),
        )?;
        configure_worker_build(&mut command, fixture, &bindgen, cancelled)?;
        run_prepared_command(&mut command, cancelled, false)?;
        if fs::read(fixture.join("Cargo.lock"))? != lock {
            return Err(io::Error::other(
                "worker-build changes the locked fixture dependencies",
            ));
        }
    } else {
        let mut arguments = vec![
            "build",
            "--locked",
            "--release",
            "--features",
            platform,
            "--target-dir",
        ];
        arguments.extend(if platform == "fastly" {
            [
                "target/fastly",
                "--target",
                "wasm32-wasip1",
                "--bin",
                "request-fidelity-fastly",
            ]
            .as_slice()
        } else {
            ["target/spin", "--target", "wasm32-wasip2", "--lib"].as_slice()
        });
        run_command(fixture, "cargo", &arguments, cancelled, false)?;
    }
    Ok(())
}

fn with_runtime_log<T>(
    result: io::Result<T>,
    log_path: &Path,
    output: &mut impl Write,
) -> io::Result<T> {
    if result.is_err()
        && let Ok(log) = fs::read(log_path)
    {
        let log = String::from_utf8_lossy(&log);
        let _result = writeln!(output, "{log}");
    }
    result
}

fn report_observation_diff(
    output: &mut impl Write,
    platform: &str,
    expected: &Value,
    observed: &Value,
) -> io::Result<()> {
    let cases: BTreeSet<_> = expected
        .as_object()
        .into_iter()
        .flat_map(|cases| cases.keys())
        .chain(
            observed
                .as_object()
                .into_iter()
                .flat_map(|cases| cases.keys()),
        )
        .collect();
    for case in cases {
        if expected.get(case) != observed.get(case) {
            writeln!(
                output,
                "{platform}/{case}: expected={} observed={}",
                expected.get(case).unwrap_or(&Value::Null),
                observed.get(case).unwrap_or(&Value::Null)
            )?;
        }
    }
    Ok(())
}

fn run(options: Options, cancelled: &Cancelled) -> io::Result<()> {
    let fixture = options.root.join("tests/fixtures/request-fidelity");
    let mut observations = json!({});
    for platform in &options.platforms {
        check_cancelled(cancelled)?;
        if *platform == "spin" {
            let pin = fixture_tool_pin(&fixture, "spin-cli")?;
            verify_version(&fixture, "spin", &pin, cancelled)?;
        } else if *platform == "fastly" {
            let versions = fs::read_to_string(options.root.join(".tool-versions"))?;
            let pin = versions
                .lines()
                .find_map(|line| line.strip_prefix("viceroy "))
                .ok_or_else(|| io::Error::other("missing Viceroy pin"))?;
            verify_version(&fixture, "viceroy", pin.trim(), cancelled)?;
        }
        if !options.skip_build {
            build(platform, &fixture, cancelled)?;
        }
        let scratch = tempfile::tempdir()?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        drop(listener);
        writeln!(io::stderr(), "{platform} loopback fixture uses port {port}")?;
        let address = format!("127.0.0.1:{port}");
        let port_string = port.to_string();
        let scratch_string = scratch.path().to_string_lossy();
        let log_dir = scratch.path().join("logs");
        let log_dir_string = log_dir.to_string_lossy();
        let mut runtime = match *platform {
            "fastly" => command(
                &fixture,
                "viceroy",
                &[
                    "serve",
                    "-C",
                    "fastly.toml",
                    "--addr",
                    &address,
                    "target/fastly/wasm32-wasip1/release/request-fidelity-fastly.wasm",
                ],
            ),
            "spin" => command(
                &fixture,
                "spin",
                &[
                    "up",
                    "--from",
                    "spin.toml",
                    "--listen",
                    &address,
                    "--state-dir",
                    &scratch_string,
                    "--log-dir",
                    &log_dir_string,
                ],
            ),
            _ => command(
                &fixture,
                "node_modules/.bin/wrangler",
                &[
                    "dev",
                    "--local",
                    "--ip",
                    "127.0.0.1",
                    "--port",
                    &port_string,
                ],
            ),
        };
        let log_path = scratch.path().join("runtime.log");
        let log = File::create(&log_path)?;
        runtime.stdout(log.try_clone()?).stderr(log);
        let mut process = OwnedProcess::spawn(&mut runtime)?;
        let result = (|| {
            let deadline = Instant::now() + options.readiness_timeout;
            loop {
                check_cancelled(cancelled)?;
                if process.child.try_wait()?.is_some() {
                    return Err(io::Error::other("runtime exits before readiness"));
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other("runtime readiness deadline exceeded"));
                }
                if exchange(port, &ready_case(), cancelled, deadline)
                    .is_ok_and(|(status, _)| status == 200)
                {
                    break;
                }
                thread::sleep(POLL_INTERVAL);
            }
            let mut observed = probes(platform, port, cancelled)?;
            if *platform == "cloudflare" {
                let routing =
                    cloudflare_routing_probes(&fixture, cancelled, options.readiness_timeout)?;
                observed
                    .as_object_mut()
                    .expect("probe results are objects")
                    .extend(routing);
            }
            if !options.record_observations {
                let expected: Value =
                    serde_json::from_slice(&fs::read(fixture.join("observations.json"))?)
                        .map_err(io::Error::other)?;
                if expected.get(*platform) != Some(&observed) {
                    report_observation_diff(
                        &mut io::stderr(),
                        platform,
                        &expected[*platform],
                        &observed,
                    )?;
                    return Err(io::Error::other(format!(
                        "{platform}: full runtime observations changed; inspect before updating baseline"
                    )));
                }
            }
            Ok(observed)
        })();
        drop(process);
        observations[*platform] = with_runtime_log(result, &log_path, &mut io::stderr())?;
    }
    serde_json::to_writer_pretty(io::stdout(), &observations).map_err(io::Error::other)?;
    writeln!(io::stdout())
}

fn request_packet(case: &Case) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(case.method);
    packet.push(b' ');
    packet.extend_from_slice(case.target);
    packet.extend_from_slice(b" HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\nX-Fixture: ");
    packet.extend_from_slice(case.name.as_bytes());
    packet.extend_from_slice(b"\r\n");
    for (name, value) in &case.headers {
        packet.extend_from_slice(name);
        packet.extend_from_slice(b": ");
        packet.extend_from_slice(value);
        packet.extend_from_slice(b"\r\n");
    }
    packet.extend_from_slice(b"\r\n");
    packet.extend_from_slice(case.body);
    packet
}

struct TimedSocket<'a> {
    socket: TcpStream,
    cancelled: &'a Cancelled,
    deadline: Instant,
}

impl Read for TimedSocket<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            // Buffered/exact reads retry Interrupted, so cancellation must be terminal.
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(io::Error::other("cancelled"));
            }
            if Instant::now() >= self.deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP response deadline exceeded",
                ));
            }
            match self.socket.read(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) => {}
                result => return result,
            }
        }
    }
}

fn exchange(
    port: u16,
    case: &Case,
    cancelled: &Cancelled,
    deadline: Instant,
) -> io::Result<(u16, Value)> {
    check_cancelled(cancelled)?;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(5));
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP connection deadline exceeded",
        ));
    }
    let mut socket = TcpStream::connect_timeout(&address, timeout)?;
    socket.set_write_timeout(Some(timeout))?;
    socket.set_read_timeout(Some(POLL_INTERVAL))?;
    socket.write_all(&request_packet(case))?;
    let mut reader = BufReader::new(TimedSocket {
        socket,
        cancelled,
        deadline,
    });
    let (status, payload) = read_response(&mut reader)?;
    Ok((
        status,
        serde_json::from_slice(&payload).unwrap_or(Value::Null),
    ))
}

fn http_line(reader: &mut impl BufRead, remaining: &mut usize) -> io::Result<String> {
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(*remaining).map_err(io::Error::other)?)
        .read_until(b'\n', &mut bytes)?;
    *remaining -= bytes.len();
    if !bytes.ends_with(b"\r\n") {
        return Err(io::Error::other("missing CRLF or oversized HTTP header"));
    }
    bytes.truncate(bytes.len() - 2);
    String::from_utf8(bytes).map_err(io::Error::other)
}

fn read_response(reader: &mut impl BufRead) -> io::Result<(u16, Vec<u8>)> {
    let mut remaining = HEADER_LIMIT;
    loop {
        let status_line = http_line(reader, &mut remaining)?;
        let mut fields = status_line.split_whitespace();
        if !matches!(fields.next(), Some("HTTP/1.1" | "HTTP/1.0")) {
            return Err(io::Error::other("invalid HTTP response status line"));
        }
        let status = fields
            .next()
            .ok_or_else(|| io::Error::other("missing response status"))?
            .parse::<u16>()
            .map_err(io::Error::other)?;
        let mut length = None;
        let mut chunked = false;
        loop {
            let line = http_line(reader, &mut remaining)?;
            if line.is_empty() {
                break;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| io::Error::other("invalid HTTP response header"))?;
            if name.eq_ignore_ascii_case("content-length") {
                let next = value.trim().parse::<usize>().map_err(io::Error::other)?;
                if length.is_some_and(|previous| previous != next) {
                    return Err(io::Error::other("conflicting response lengths"));
                }
                length = Some(next);
            }
            if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value
                    .split(',')
                    .any(|item| item.trim().eq_ignore_ascii_case("chunked"));
            }
        }
        if (100..200).contains(&status) {
            continue;
        }
        let mut body = Vec::new();
        if chunked {
            loop {
                let line = http_line(reader, &mut remaining)?;
                let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
                    .map_err(io::Error::other)?;
                if size == 0 {
                    while !http_line(reader, &mut remaining)?.is_empty() {}
                    break;
                }
                append_body(reader, &mut body, size)?;
                let mut delimiter = [0; 2];
                reader.read_exact(&mut delimiter)?;
                if delimiter != *b"\r\n" {
                    return Err(io::Error::other("invalid chunk delimiter"));
                }
            }
        } else if let Some(length) = length {
            append_body(reader, &mut body, length)?;
        } else {
            reader
                .take(u64::try_from(RESPONSE_LIMIT + 1).map_err(io::Error::other)?)
                .read_to_end(&mut body)?;
            if body.len() > RESPONSE_LIMIT {
                return Err(io::Error::other("oversized HTTP response body"));
            }
        }
        return Ok((status, body));
    }
}

fn append_body(reader: &mut impl Read, body: &mut Vec<u8>, length: usize) -> io::Result<()> {
    let end = body
        .len()
        .checked_add(length)
        .filter(|end| *end <= RESPONSE_LIMIT)
        .ok_or_else(|| io::Error::other("oversized HTTP response body"))?;
    let start = body.len();
    body.resize(end, 0);
    reader.read_exact(&mut body[start..])
}

fn ready_case() -> Case {
    Case {
        name: "ready",
        method: b"GET",
        target: b"/reserved",
        headers: Vec::new(),
        body: b"",
    }
}

fn cases() -> Vec<Case> {
    let mut cases: Vec<_> = [
        "extension",
        "dot",
        "encoded-dot",
        "slashes",
        "absolute",
        "high-byte",
        "utf8",
        "unicode",
        "repeat",
        "compound-invalid",
        "compound-valid",
        "spoof",
        "length-identical",
        "length-conflict",
        "te-cl",
        "chunked",
        "continue",
    ]
    .into_iter()
    .map(|name| Case {
        name,
        ..ready_case()
    })
    .collect();
    for case in &mut cases {
        match case.name {
            "extension" => case.method = b"EXAMPLE-METHOD",
            "dot" => case.target = b"/reserved/../ordinary",
            "encoded-dot" => case.target = b"/reserved/%2e%2e/ordinary",
            "slashes" => case.target = b"/reserved//%2F?q=a%2Fb",
            "absolute" => case.target = b"http://example.com/reserved?fixed=1",
            "high-byte" => case.headers = vec![(b"Cookie", b"a=\xff")],
            "utf8" => case.headers = vec![(b"Cookie", b"a=\xc3\xa9")],
            "unicode" => case.headers = vec![(b"Cookie", b"a=\xc4\x80")],
            "repeat" => {
                case.headers = vec![
                    (b"Cookie", b"a=1"),
                    (b"Origin", b"https://example.com"),
                    (b"X-Control", b"first"),
                    (b"COOKIE", b"b=2"),
                    (b"Origin", b"https://other.example.com"),
                    (b"x-control", b"second"),
                ]
            }
            "compound-invalid" | "compound-valid" => {
                case.headers = vec![
                    (b"Cookie", b"diagnostics=1"),
                    (
                        b"Cookie",
                        if case.name == "compound-invalid" {
                            b"a=\xff"
                        } else {
                            b"a=\xef\xbf\xbd"
                        },
                    ),
                ];
            }
            "spoof" => {
                case.headers = vec![
                    (b"Origin", b"https://spoof.example.com"),
                    (b"Forwarded", b"proto=https;host=spoof.example.com"),
                    (b"spin-full-url", b"https://spoof.example.com/reserved"),
                    (b"spin-client-addr", b"192.0.2.1:1"),
                ]
            }
            "length-identical" | "length-conflict" => {
                case.method = b"POST";
                case.headers = vec![
                    (b"Content-Length", b"0"),
                    (
                        b"Content-Length",
                        if case.name == "length-identical" {
                            b"0"
                        } else {
                            b"1"
                        },
                    ),
                ];
            }
            "te-cl" | "chunked" => {
                case.method = b"POST";
                case.headers = vec![(b"Transfer-Encoding", b"chunked")];
                if case.name == "te-cl" {
                    case.headers.push((b"Content-Length", b"0"));
                }
                case.body = b"0\r\n\r\n";
            }
            "continue" => {
                case.method = b"GET";
                case.target = b"/ordinary";
            }
            _ => return Vec::new(),
        }
    }
    cases
}

fn require(condition: bool, platform: &str, case: &str, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(format!("{platform}/{case}: {message}")))
    }
}

fn verify_observation(platform: &str, case: &str, status: u16, verdict: &Value) -> io::Result<()> {
    if status == 200 {
        if case == "continue" {
            return require(
                *verdict == json!({"ordinary": true}),
                platform,
                case,
                "continuation must run ordinary handler",
            );
        }
        require(
            verdict["hook"] == true && verdict["ordinary"] == false,
            platform,
            case,
            "hook must intercept before ordinary lifecycle",
        )?;
        if platform == "fastly" {
            require(
                verdict["native_client_preserved"] == true,
                platform,
                case,
                "native provenance lost",
            )?;
        }
        require(
            verdict["global_order_unavailable"] == (platform != "fastly")
                && verdict["spoofed_origin_ignored"] == true
                && verdict["origin_available"] == true
                && verdict["origin_matches_expected"] == true,
            platform,
            case,
            "order capability and origin provenance must remain explicit",
        )?;
        require(
            verdict["target_available"] == (platform != "fastly"),
            platform,
            case,
            "unexpected target availability",
        )?;
        if platform != "fastly" {
            require(
                verdict["target_matches_runtime"] == true,
                platform,
                case,
                "target must match runtime",
            )?;
        }
        match case {
            "extension" => require(
                verdict["extension_method"] == true,
                platform,
                case,
                "extension token lost",
            )?,
            "high-byte" if platform == "cloudflare" => {
                require(
                    verdict["cookie_replacement"] == true && verdict["cookie_octets"] == false,
                    platform,
                    case,
                    "expected documented UTF-8 replacement, not original FF preservation",
                )?;
                writeln!(
                    io::stderr(),
                    "UNSUPPORTED local workerd replaces invalid UTF-8 FF before Rust; hook sees UTF-8 replacement bytes."
                )?;
            }
            "high-byte" => require(
                verdict["cookie_octets"] == true,
                platform,
                case,
                "original cookie FF lost",
            )?,
            "utf8" => require(
                verdict["cookie_utf8"] == true && verdict["cookie_latin1"] == false,
                platform,
                case,
                "valid UTF-8 C3 A9 must remain exact",
            )?,
            "unicode" => require(
                verdict["cookie_unicode"] == true,
                platform,
                case,
                "valid UTF-8 C4 80 must remain exact",
            )?,
            "repeat" if platform != "cloudflare" => require(
                verdict["cookie_count"] == 2
                    && verdict["same_name_order"] == true
                    && verdict["origin_count"] == 2
                    && verdict["control_count"] == 2,
                platform,
                case,
                "ordinary repeated fields or same-name order lost",
            )?,
            "repeat" => require(
                verdict["cookie_count"] == 1
                    && verdict["cookie_repeat_comma_joined"] == true
                    && verdict["cookie_repeat_semicolon_joined"] == false,
                platform,
                case,
                "must establish exact comma joining rather than infer cookie semantics from counts",
            )?,
            "compound-invalid" | "compound-valid" if platform == "cloudflare" => require(
                verdict["cookie_count"] == 1 && verdict["cookie_compound_comma_joined"] == true,
                platform,
                case,
                "invalid FF and original UTF-8 replacement must have the same documented runtime representation",
            )?,
            "compound-invalid" => require(
                verdict["cookie_count"] == 2 && verdict["cookie_compound_original"] == true,
                platform,
                case,
                "must retain valid-looking diagnostics alongside unrelated original FF",
            )?,
            "compound-valid" => require(
                verdict["cookie_count"] == 2 && verdict["cookie_compound_runtime_utf8"] == true,
                platform,
                case,
                "must retain the client's original valid UTF-8 replacement bytes",
            )?,
            "te-cl" | "chunked" => {
                let count = verdict["transfer_encoding_count"].as_u64();
                let chunked = verdict["transfer_encoding_chunked"].as_bool();
                require(
                    matches!(count, Some(0 | 1)) && chunked == count.map(|count| count == 1),
                    platform,
                    case,
                    "must record whether chunked Transfer-Encoding survives conversion",
                )?;
            }
            _ => {}
        }
        return Ok(());
    }
    match (platform, case, status) {
        ("cloudflare", "length-identical" | "length-conflict", 500) => writeln!(
            io::stderr(),
            "UNSUPPORTED local Wrangler duplicate Content-Length ingress (500); no hook verdict."
        )?,
        ("cloudflare", "absolute", 500) => writeln!(
            io::stderr(),
            "UNSUPPORTED local Wrangler absolute-form ingress (500); no hook verdict."
        )?,
        ("cloudflare", "extension", 501) => writeln!(
            io::stderr(),
            "PENDING local workerd rejects extension methods before Worker invocation (501)."
        )?,
        ("spin", "high-byte" | "compound-invalid", 500) => writeln!(
            io::stderr(),
            "PENDING Spin rejects non-UTF-8 header bytes before component invocation (500)."
        )?,
        (_, "length-identical" | "length-conflict" | "te-cl" | "absolute", 400) => {}
        _ => {
            return Err(io::Error::other(format!(
                "{platform}/{case}: unexpected HTTP status {status}"
            )));
        }
    }
    require(
        verdict.is_null(),
        platform,
        case,
        "transport rejection must have no hook verdict",
    )
}

fn cloudflare_routing_probes(
    fixture: &Path,
    cancelled: &Cancelled,
    readiness_timeout: Duration,
) -> io::Result<serde_json::Map<String, Value>> {
    let scratch = tempfile::tempdir()?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let address = format!("http=127.0.0.1:{port}");
    let mut runtime = command(
        fixture,
        "node_modules/.bin/workerd",
        &["serve", "workerd.capnp", "--socket-addr", &address],
    );
    let log_path = scratch.path().join("workerd.log");
    let log = File::create(&log_path)?;
    runtime.stdout(log.try_clone()?).stderr(log);
    let mut process = OwnedProcess::spawn(&mut runtime)?;
    let result = (|| {
        let deadline = Instant::now() + readiness_timeout;
        loop {
            check_cancelled(cancelled)?;
            if process.child.try_wait()?.is_some() || Instant::now() >= deadline {
                return Err(io::Error::other("direct workerd readiness failed"));
            }
            if exchange(port, &ready_case(), cancelled, deadline)
                .is_ok_and(|(status, _)| status == 200)
            {
                break;
            }
            thread::sleep(POLL_INTERVAL);
        }
        let mut observed = serde_json::Map::new();
        let mut failures = Vec::new();
        for (name, target) in [
            ("route-unicode", b"/caf\xc3\xa9".as_slice()),
            ("route-backslash", b"/a\\b".as_slice()),
        ] {
            let case = Case {
                name,
                target,
                ..ready_case()
            };
            let (status, verdict) = exchange(
                port,
                &case,
                cancelled,
                Instant::now() + Duration::from_secs(5),
            )?;
            if status != 200
                || verdict
                    != json!({
                        "ordinary": true,
                        "primary_uri_normalized": true,
                        "raw_target_preserved": true,
                    })
            {
                failures.push(format!("{name}: HTTP {status}, verdict {verdict}"));
            }
            observed.insert(
                name.to_owned(),
                json!({"status": status, "verdict": verdict}),
            );
        }
        require(
            failures.is_empty(),
            "cloudflare",
            "no-hook routing",
            &failures.join("; "),
        )?;
        Ok(observed)
    })();
    drop(process);
    with_runtime_log(result, &log_path, &mut io::stderr())
}

fn probes(platform: &str, port: u16, cancelled: &Cancelled) -> io::Result<Value> {
    let mut observed = json!({});
    let cases = cases();
    require(
        cases.len() == 17,
        platform,
        "fixture",
        "must execute all 17 fixed ingress cases",
    )?;
    for case in cases {
        let (status, verdict) = exchange(
            port,
            &case,
            cancelled,
            Instant::now() + Duration::from_secs(5),
        )?;
        verify_observation(platform, case.name, status, &verdict)?;
        observed[case.name] = json!({ "status": status, "verdict": verdict });
    }
    if platform == "fastly" {
        let case = Case {
            name: "snapshot-origin",
            ..ready_case()
        };
        let (status, verdict) = exchange(
            port,
            &case,
            cancelled,
            Instant::now() + Duration::from_secs(5),
        )?;
        verify_observation(platform, case.name, status, &verdict)?;
        observed[case.name] = json!({"status": status, "verdict": verdict});
        for (name, target) in [
            ("native", b"/native".as_slice()),
            ("native-dot", b"/reserved/../native".as_slice()),
        ] {
            let case = Case {
                name,
                target,
                ..ready_case()
            };
            let (status, verdict) = exchange(
                port,
                &case,
                cancelled,
                Instant::now() + Duration::from_secs(5),
            )?;
            require(
                status == 200 && verdict == json!({"native": true, "raw_target_supported": false}),
                platform,
                name,
                "native shortcut must report unavailable raw-target support",
            )?;
            observed[name] = json!({"status": status, "verdict": verdict});
        }
        writeln!(
            io::stderr(),
            "PENDING Fastly original-target and native-shortcut exclusion: safe SDK accessor required."
        )?;
    }
    Ok(observed)
}

struct Case {
    name: &'static str,
    method: &'static [u8],
    target: &'static [u8],
    headers: Vec<(&'static [u8], &'static [u8])>,
    body: &'static [u8],
}

#[cfg(test)]
mod tests {
    use super::{
        Case, HEADER_LIMIT, OwnedProcess, TimedSocket, cases, cloudflare_routing_probes,
        read_response, request_packet, verify_observation,
    };
    use serde_json::json;
    use std::io::{self, BufRead as _, BufReader, Cursor, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn missing_program_diagnostic_names_the_executable() {
        let program = "/missing-edgezero-runtime";
        let error = OwnedProcess::spawn(&mut Command::new(program))
            .err()
            .expect("missing program must fail");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains(program));
        let error = super::run_command(
            Path::new("/tmp"),
            program,
            &["--version"],
            &Arc::new(AtomicBool::new(false)),
            true,
        )
        .expect_err("missing command must fail");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(error.to_string().matches(program).count(), 1);
        assert!(error.to_string().contains("--version"));
    }

    #[test]
    fn command_failures_include_arguments_and_captured_diagnostics() {
        let directory = tempfile::tempdir().expect("command directory");
        let cancelled = Arc::new(AtomicBool::new(false));
        for (script, kind, diagnostics) in [
            (
                "printf '%s %s' probe out; printf '%s %s' probe err >&2; exit 3",
                io::ErrorKind::Other,
                "stdout: probe out",
            ),
            ("printf '\\377'", io::ErrorKind::InvalidData, "�"),
        ] {
            let error = super::run_command(
                directory.path(),
                "/bin/sh",
                &["-c", script],
                &cancelled,
                true,
            )
            .expect_err("command must fail");
            assert_eq!(error.kind(), kind);
            let message = error.to_string();
            assert!(message.contains("/bin/sh -c"), "{message}");
            assert!(message.contains(diagnostics), "{message}");
            if kind == io::ErrorKind::Other {
                assert!(message.contains("stderr: probe err"), "{message}");
                assert!(message.contains("exit status: 3"), "{message}");
            }
        }
    }

    #[test]
    fn noncaptured_command_streams_in_order_and_preserves_failure() {
        const CHILD: &str = "EDGEZERO_TEST_NONCAPTURE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let error = super::run_command(
                Path::new("/tmp"),
                "/bin/sh",
                &["-c", "printf 'out-first\\n'; printf 'err-middle\\n' >&2; printf 'out-last\\n'; exit 7"],
                &Arc::new(AtomicBool::new(false)),
                false,
            ).expect_err("noncapture must preserve command failure");
            assert!(error.to_string().contains("exit status: 7"), "{error}");
            return;
        }
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "tests::noncaptured_command_streams_in_order_and_preserves_failure",
            ])
            .env(CHILD, "1")
            .output()
            .expect("child test");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            "out-first\nerr-middle\nout-last\n"
        );
    }

    #[test]
    fn captured_success_keeps_stdout_separate_from_stderr() {
        let directory = tempfile::tempdir().expect("command directory");
        let output = super::run_command(
            directory.path(),
            "/bin/sh",
            &["-c", "printf 'tool 1.2.3'; printf 'tool warning' >&2"],
            &Arc::new(AtomicBool::new(false)),
            true,
        )
        .expect("successful command");
        assert_eq!(output, "tool 1.2.3");
    }

    #[test]
    fn bindgen_resolution_skips_nonexecutables_and_honors_overrides() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("tool directories");
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        std::fs::create_dir_all(&first).expect("first directory");
        std::fs::create_dir_all(&second).expect("second directory");
        let shadow = first.join("wasm-bindgen");
        let executable = second.join("wasm-bindgen");
        for (file, mode) in [(&shadow, 0o644), (&executable, 0o755)] {
            std::fs::write(file, "#!/bin/sh\nexit 0\n").expect("tool");
            std::fs::set_permissions(file, std::fs::Permissions::from_mode(mode))
                .expect("permissions");
        }
        let path = std::env::join_paths([&first, &second]).expect("PATH");
        assert_eq!(
            super::resolve_wasm_bindgen(None, &path).expect("executable lookup"),
            executable
        );
        let override_tool = directory.path().join("override-wasm-bindgen");
        std::fs::copy(&executable, &override_tool).expect("distinct override outside PATH");
        assert_eq!(
            super::resolve_wasm_bindgen(Some(override_tool.clone().into_os_string()), &path)
                .expect("override"),
            override_tool
        );
        for invalid in [shadow, directory.path().join("missing"), first] {
            let error = super::resolve_wasm_bindgen(Some(invalid.into_os_string()), &path)
                .expect_err("invalid override must not fall back to PATH");
            assert!(error.to_string().contains("WASM_BINDGEN_BIN"), "{error}");
        }
        std::fs::remove_file(&executable).expect("remove executable");
        let error = super::resolve_wasm_bindgen(None, &path).expect_err("no executable");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn version_diagnostic_includes_expected_and_actual() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("tool directory");
        let tool = directory.path().join("fixture-tool");
        std::fs::write(&tool, "#!/bin/sh\necho 'fixture-tool 9.9.9'\n").expect("binary");
        std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let cancelled = Arc::new(AtomicBool::new(false));
        let error = super::verify_version(
            directory.path(),
            &tool.to_string_lossy(),
            "expected-pin",
            &cancelled,
        )
        .expect_err("wrong tool version");
        assert!(error.to_string().contains("expected-pin"));
        assert!(error.to_string().contains("9.9.9"));
    }

    #[test]
    fn missing_tool_pin_names_the_tool() {
        let directory = tempfile::tempdir().expect("temporary fixture");
        std::fs::write(
            directory.path().join("Cargo.toml"),
            "[package]\nname = 'fixture'\n",
        )
        .expect("manifest");
        let error =
            super::fixture_tool_pin(directory.path(), "worker-build").expect_err("missing pin");
        assert!(error.to_string().contains("worker-build"));
    }

    #[test]
    fn non_utf8_runtime_logs_are_printed_lossily() {
        let directory = tempfile::tempdir().expect("temporary log");
        let path = directory.path().join("runtime.log");
        std::fs::write(&path, b"before\xffafter").expect("log bytes");
        let mut output = Vec::new();
        let error = super::with_runtime_log(
            Err::<(), _>(io::Error::other("original failure")),
            &path,
            &mut output,
        )
        .expect_err("original error");
        assert_eq!(error.to_string(), "original failure");
        assert_eq!(
            String::from_utf8(output).expect("lossy log"),
            "before\u{fffd}after\n"
        );
    }

    #[test]
    fn worker_build_requires_locked_local_tools() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("fixture directory");
        let fixture = directory.path();
        std::fs::write(
            fixture.join("Cargo.lock"),
            "[[package]]\nname = 'wasm-bindgen'\nversion = '0.2.128'\n",
        )
        .expect("lock");
        std::fs::write(
            fixture.join("package-lock.json"),
            r#"{"packages":{"node_modules/esbuild":{"version":"0.27.3"}}}"#,
        )
        .expect("npm lock");
        let bindgen = fixture.join("wasm-bindgen");
        std::fs::write(&bindgen, "#!/bin/sh\necho 'wasm-bindgen 0.2.128'\n").expect("binary");
        std::fs::set_permissions(&bindgen, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut command = Command::new("worker-build");
        let error = super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
            .expect_err("missing npm-locked esbuild must not trigger a download");
        assert!(error.to_string().contains("esbuild"));
        let esbuild = fixture.join("node_modules/.bin/esbuild");
        std::fs::create_dir_all(esbuild.parent().expect("parent")).expect("bin directory");
        std::fs::write(&esbuild, "#!/bin/sh\necho '0.27.3'\n").expect("binary");
        std::fs::set_permissions(&esbuild, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
            .expect("pinned tools");
        let envs: std::collections::BTreeMap<_, _> = command.get_envs().collect();
        assert_eq!(
            envs[std::ffi::OsStr::new("WASM_BINDGEN_BIN")],
            Some(
                bindgen
                    .canonicalize()
                    .expect("canonical binary")
                    .as_os_str()
            )
        );
        assert_eq!(
            envs[std::ffi::OsStr::new("ESBUILD_BIN")],
            Some(
                esbuild
                    .canonicalize()
                    .expect("canonical binary")
                    .as_os_str()
            )
        );
        std::fs::write(&esbuild, "#!/bin/sh\necho '0.28.1'\n").expect("wrong esbuild");
        let error = super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
            .expect_err("reject unlocked esbuild");
        assert!(error.to_string().contains("0.28.1"));
        std::fs::write(&esbuild, "#!/bin/sh\necho '0.27.3'\n").expect("restore esbuild");
        std::fs::write(&bindgen, "#!/bin/sh\necho 'wasm-bindgen 0.2.127'\n").expect("wrong tool");
        let error = super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
            .expect_err("reject wrong bindgen");
        assert!(error.to_string().contains("0.2.127"));
        std::fs::remove_file(&bindgen).expect("remove tool");
        let error = super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
            .expect_err("missing configured tool must fail instead of downloading");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains("wasm-bindgen"));
    }

    #[test]
    fn diagnostic_failures_preserve_the_original_runtime_error() {
        let directory = tempfile::tempdir().expect("should create log directory");
        for (name, contents) in [
            ("missing", None),
            ("invalid-utf8", Some(b"\xff".as_slice())),
            ("failed-output", Some(b"runtime diagnostics".as_slice())),
        ] {
            let path = directory.path().join(name);
            if let Some(contents) = contents {
                std::fs::write(&path, contents).expect("should create diagnostic log");
            }
            let mut output = Cursor::new([].as_mut_slice());
            let result = Err::<(), _>(io::Error::new(
                io::ErrorKind::TimedOut,
                "original readiness failure",
            ));
            let error = super::with_runtime_log(result, &path, &mut output)
                .expect_err("should preserve runtime failure");
            assert_eq!(error.kind(), io::ErrorKind::TimedOut, "{name}");
            assert_eq!(error.to_string(), "original readiness failure", "{name}");
        }
    }

    #[test]
    #[ignore = "requires a freshly built Cloudflare fixture and pinned workerd"]
    fn cloudflare_primary_uri_preserves_no_hook_routing() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("runner has a fixture parent")
            .join("request-fidelity");
        let cancelled = Arc::new(AtomicBool::new(false));
        cloudflare_routing_probes(&fixture, &cancelled, Duration::from_secs(60))
            .expect("primary URI normalization and raw ingress capture must remain independent");
    }

    #[test]
    fn cancelled_response_reads_do_not_retry_until_deadline() {
        for prefix in [
            b"".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n".as_slice(),
        ] {
            let listener =
                TcpListener::bind("127.0.0.1:0").expect("should bind a silent loopback server");
            let socket = TcpStream::connect(listener.local_addr().expect("should get port"))
                .expect("should connect to silent server");
            let (mut server, _) = listener.accept().expect("should accept test client");
            server
                .write_all(prefix)
                .expect("should write partial response");
            socket
                .set_read_timeout(Some(Duration::from_millis(10)))
                .expect("should bound individual reads");
            let cancelled = Arc::new(AtomicBool::new(false));
            let mut reader = BufReader::new(TimedSocket {
                socket,
                cancelled: &cancelled,
                deadline: Instant::now() + Duration::from_millis(150),
            });
            if !prefix.is_empty() {
                let _buffer = reader.fill_buf().expect("should buffer response headers");
            }
            cancelled.store(true, Ordering::SeqCst);
            // Bound the old retry loop as well, so a regression fails rather than hangs.
            let fallback_cancelled = Arc::clone(&cancelled);
            let fallback = thread::spawn(move || {
                thread::sleep(Duration::from_millis(200));
                fallback_cancelled.store(false, Ordering::SeqCst);
            });
            let error = read_response(&mut reader)
                .expect_err("should cancel both header and exact-body reads");
            fallback.join().expect("should join retry-loop fallback");
            assert_eq!(
                error.kind(),
                io::ErrorKind::Other,
                "should return a terminal cancellation error instead of a retryable interrupt"
            );
            assert_eq!(
                error.to_string(),
                "cancelled",
                "should preserve cancellation reason"
            );
        }
    }

    #[test]
    fn baseline_diff_reports_changed_added_and_removed_cases_once() {
        let expected = json!({"same": {"hook": true}, "changed": {"count": 2}, "removed": true});
        let observed = json!({"same": {"hook": true}, "changed": {"count": 1}, "added": false});
        let mut output = Vec::new();
        super::report_observation_diff(&mut output, "spin", &expected, &observed)
            .expect("should report changes");
        let output = String::from_utf8(output).expect("should write UTF-8");
        assert_eq!(output.lines().count(), 3);
        assert!(output.contains("spin/changed: expected={\"count\":2} observed={\"count\":1}"));
        assert!(output.contains("spin/added: expected=null observed=false"));
        assert!(output.contains("spin/removed: expected=true observed=null"));
        assert!(!output.contains("spin/same"));
    }

    #[test]
    fn fixed_registry_contains_unicode_and_standalone_chunked_probes() {
        let cases = cases();
        assert_eq!(cases.len(), 17, "should include every fixed ingress case");
        let unicode = cases
            .iter()
            .find(|case| case.name == "unicode")
            .expect("should include unicode case");
        assert_eq!(
            unicode.headers,
            [(b"Cookie".as_slice(), b"a=\xc4\x80".as_slice())],
            "should send exact UTF-8 C4 80"
        );
        let chunked = cases
            .iter()
            .find(|case| case.name == "chunked")
            .expect("should include chunked case");
        assert_eq!(
            chunked.headers,
            [(b"Transfer-Encoding".as_slice(), b"chunked".as_slice())],
            "should probe Transfer-Encoding without Content-Length"
        );
        assert_eq!(
            chunked.body, b"0\r\n\r\n",
            "should terminate the empty chunked body"
        );
    }

    #[test]
    fn observation_verification_rejects_utf8_corruption_even_during_collection() {
        let mut verdict = json!({"hook": true, "ordinary": false, "global_order_unavailable": true, "spoofed_origin_ignored": true, "origin_available": true, "origin_matches_expected": true, "target_available": true, "target_matches_runtime": true, "cookie_utf8": false, "cookie_latin1": true});
        let error = verify_observation("cloudflare", "utf8", 200, &verdict)
            .expect_err("should reject the former C3 A9 to E9 defect");
        assert!(
            error.to_string().contains("C3 A9"),
            "should reach the UTF-8 check: {error}"
        );
        verdict["cookie_utf8"] = json!(true);
        verdict["cookie_latin1"] = json!(false);
        verify_observation("cloudflare", "utf8", 200, &verdict).expect("should accept exact UTF-8");
        let _error = verify_observation("cloudflare", "unicode", 500, &serde_json::Value::Null)
            .expect_err("should reject the former C4 80 conversion error");
    }

    #[test]
    fn cancellation_stops_and_reaps_the_owned_process_group() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut command = Command::new("/bin/sleep");
        command
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut process = OwnedProcess::spawn(&mut command).expect("should spawn owned group");
        let group = process.group;
        let worker_cancelled = Arc::clone(&cancelled);
        let stopper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            worker_cancelled.store(true, Ordering::SeqCst);
        });
        let start = Instant::now();
        let error = process
            .wait(&cancelled)
            .expect_err("should stop waiting on interruption");
        assert_eq!(
            error.kind(),
            io::ErrorKind::Interrupted,
            "should preserve interruption reason"
        );
        drop(process);
        stopper.join().expect("should join cancellation trigger");
        assert!(
            start.elapsed() < Duration::from_secs(6),
            "should bound cleanup even when interrupted"
        );
        let status = Command::new("/bin/kill")
            .args(["-0", "--", &format!("-{group}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("should inspect owned group");
        assert!(
            !status.success(),
            "should leave no live owned process group"
        );
    }

    #[test]
    fn packet_preserves_case_order_high_bytes_and_chunk_body() {
        let case = Case {
            name: "fixed",
            method: b"EXAMPLE-METHOD",
            target: b"/reserved//%2F?q=a%2Fb",
            headers: vec![(b"Cookie", b"a=\xff"), (b"COOKIE", b"b=2")],
            body: b"0\r\n\r\n",
        };
        assert_eq!(
            request_packet(&case),
            b"EXAMPLE-METHOD /reserved//%2F?q=a%2Fb HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\nX-Fixture: fixed\r\nCookie: a=\xff\r\nCOOKIE: b=2\r\n\r\n0\r\n\r\n",
            "should preserve the exact raw request bytes"
        );
    }

    #[test]
    fn response_decodes_chunks_extensions_and_trailers() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;fixed=yes\r\n{\"a\"\r\n3\r\n:1}\r\n0\r\nX-Fixture: fixed\r\n\r\n";
        let (status, body) = read_response(&mut Cursor::new(raw)).expect("should parse chunks");
        assert_eq!(status, 200, "should preserve status");
        assert_eq!(body, b"{\"a\":1}", "should decode the chunk framing");
    }

    #[test]
    fn response_handles_interim_status_and_content_length() {
        let raw =
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 418 Fixed\r\nContent-Length: 2\r\n\r\n{}extra";
        let (status, body) = read_response(&mut Cursor::new(raw)).expect("should parse response");
        assert_eq!(status, 418, "should use final response status");
        assert_eq!(body, b"{}", "should read exactly Content-Length");
    }

    #[test]
    fn response_rejects_truncation_and_oversized_body() {
        for raw in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}XX".as_slice(),
        ] {
            let _error = read_response(&mut Cursor::new(raw))
                .expect_err("should reject incomplete or oversized responses");
        }
    }

    #[test]
    fn response_reads_eof_framing_and_rejects_excessive_headers() {
        let (status, body) = read_response(&mut Cursor::new(b"HTTP/1.0 200 OK\r\n\r\n{}"))
            .expect("should parse EOF-framed response");
        assert_eq!(status, 200, "should retain status");
        assert_eq!(body, b"{}", "should read the EOF-framed body");
        let oversized = format!(
            "HTTP/1.1 200 OK\r\nX-Fixture: {}\r\n\r\n",
            "a".repeat(HEADER_LIMIT)
        );
        let _error = read_response(&mut Cursor::new(oversized))
            .expect_err("should enforce a total header budget");
    }
}
