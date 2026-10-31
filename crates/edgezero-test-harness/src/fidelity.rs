//! Fixed, loopback-only HTTP ingress probes for the three WASM adapters.

use crate::{Args, Process};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const RESPONSE_LIMIT: usize = 1024 * 1024;
const HEADER_LIMIT: usize = 64 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
type Cancelled = AtomicBool;

fn io_error(error: Box<dyn std::error::Error + Send + Sync>) -> io::Error {
    match error.downcast::<io::Error>() {
        Ok(error) => *error,
        Err(error) => io::Error::other(error),
    }
}

fn check_cancelled(cancelled: &Cancelled) -> io::Result<()> {
    crate::check_cancelled(cancelled)
}

fn command(fixture: &Path, program: &str, arguments: &[&str]) -> Command {
    let program = match program {
        "spin" => env::var_os("SPIN_BIN").unwrap_or_else(|| program.into()),
        "viceroy" => env::var_os("VICEROY_BIN").unwrap_or_else(|| program.into()),
        _ => program.into(),
    };
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
    let scratch = tempfile::tempdir().map_err(describe)?;
    let log_path = scratch.path().join("command.log");
    let stdout_path = scratch.path().join("stdout.log");
    let mut process = if capture {
        Process::spawn_with_stdout(command, &log_path, &stdout_path)
    } else {
        Process::spawn(command, &log_path)
    }
    .map_err(io_error)
    .map_err(describe)?;
    let result = process.wait_success_cancelled(cancelled).map_err(io_error);
    // Never write to a potentially blocked terminal while command work is owned.
    let diagnostics = fs::read(&log_path).unwrap_or_default();
    let output = if capture {
        fs::read(&stdout_path).and_then(|bytes| {
            String::from_utf8(bytes).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{error}; output: {}",
                        String::from_utf8_lossy(error.as_bytes())
                    ),
                )
            })
        })
    } else {
        Ok(String::new())
    };
    if result
        .as_ref()
        .err()
        .is_none_or(|error| error.kind() != io::ErrorKind::Interrupted)
    {
        let _ = io::stderr().write_all(&diagnostics);
    }
    let failure = |error: io::Error, stdout: &str| {
        let log = scratch.keep().join("command.log");
        describe(io::Error::new(
            error.kind(),
            format!(
                "{error}; stdout: {}; stderr: {}; log: {}",
                stdout,
                String::from_utf8_lossy(&diagnostics),
                log.display()
            ),
        ))
    };
    if let Err(error) = result {
        return Err(failure(error, output.as_deref().unwrap_or_default()));
    }
    output.map_err(|error| failure(error, ""))
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
                "--offline",
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
            "--offline",
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

pub fn run(args: &Args, output: &Path) -> crate::Result<()> {
    let repository = crate::repository_root();
    let fixture = repository.join("crates/edgezero-test-harness/fixtures/request-fidelity");
    let cancelled = &crate::CANCELLED;
    let platforms = if args.adapter == "all" {
        vec!["fastly", "cloudflare", "spin"]
    } else {
        vec![args.adapter.as_str()]
    };
    let mut observations = json!({});
    for platform in &platforms {
        check_cancelled(cancelled)?;
        if *platform == "spin" {
            let pin = fixture_tool_pin(&fixture, "spin-cli")?;
            verify_version(&fixture, "spin", &pin, cancelled)?;
        } else if *platform == "fastly" {
            let versions = fs::read_to_string(repository.join(".tool-versions"))?;
            let pin = versions
                .lines()
                .find_map(|line| line.strip_prefix("viceroy "))
                .ok_or_else(|| io::Error::other("missing Viceroy pin"))?;
            verify_version(&fixture, "viceroy", pin.trim(), cancelled)?;
        }
        build(platform, &fixture, cancelled)?;
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
        let log_path = output.join(format!("{platform}-runtime.log"));
        let mut process = Process::spawn(&mut runtime, &log_path).map_err(io_error)?;
        let result = (|| {
            let deadline = Instant::now() + args.readiness_timeout;
            loop {
                check_cancelled(cancelled)?;
                process.check_running().map_err(io_error)?;
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
            process.check_running().map_err(io_error)?;
            let mut observed = probes(platform, port, cancelled)?;
            if *platform == "cloudflare" {
                let routing =
                    cloudflare_routing_probes(&fixture, cancelled, args.readiness_timeout)?;
                observed
                    .as_object_mut()
                    .expect("probe results are objects")
                    .extend(routing);
            }
            process.check_running().map_err(io_error)?;
            if !args.record_observations {
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
        let result = complete_runtime(&mut process, result);
        observations[*platform] = with_runtime_log(result, &log_path, &mut io::stderr())?;
    }
    fs::write(
        output.join("observations.json"),
        serde_json::to_vec_pretty(&observations)?,
    )?;
    serde_json::to_writer_pretty(io::stdout(), &observations)?;
    writeln!(io::stdout())?;
    Ok(())
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

fn complete_runtime<T>(process: &mut Process, result: io::Result<T>) -> io::Result<T> {
    match (result, process.finish().map_err(io_error)) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(io::Error::new(
            error.kind(),
            format!("{error}; cleanup: {cleanup}"),
        )),
    }
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
    let mut process = Process::spawn(&mut runtime, &log_path).map_err(io_error)?;
    let result = (|| {
        let deadline = Instant::now() + readiness_timeout;
        loop {
            check_cancelled(cancelled)?;
            process.check_running().map_err(io_error)?;
            if Instant::now() >= deadline {
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
        process.check_running().map_err(io_error)?;
        Ok(observed)
    })();
    let result = complete_runtime(&mut process, result);
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
mod tests;
