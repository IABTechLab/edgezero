use super::{
    Case, HEADER_LIMIT, TimedSocket, cases, cloudflare_routing_probes, read_response,
    request_packet, verify_observation,
};
use crate::Process;
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
fn runtime_overrides_use_the_ci_selected_executables() {
    const CHILD: &str = "EDGEZERO_FIDELITY_TOOL_OVERRIDE_TEST";
    if std::env::var_os(CHILD).is_some() {
        for (program, variable) in [("spin", "SPIN_BIN"), ("viceroy", "VICEROY_BIN")] {
            let command = super::command(Path::new("/tmp"), program, &["--version"]);
            assert_eq!(command.get_program(), std::env::var_os(variable).unwrap());
        }
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "fidelity::tests::runtime_overrides_use_the_ci_selected_executables",
        ])
        .env(CHILD, "1")
        .env("SPIN_BIN", "/explicit/ci/spin")
        .env("VICEROY_BIN", "/explicit/ci/viceroy");
    let mut process = Process::spawn(&mut child, &directory.path().join("tools.log")).unwrap();
    process.wait_success().unwrap();
}

#[test]
fn baseline_retains_all_56_raw_observations() {
    let baseline: serde_json::Value = serde_json::from_slice(
        &std::fs::read(crate::fixture_root().join("../request-fidelity/observations.json"))
            .unwrap(),
    )
    .unwrap();
    let common: std::collections::BTreeSet<_> = cases().into_iter().map(|case| case.name).collect();
    assert_eq!(common.len(), 17);
    for (platform, extra) in [
        ("spin", vec![]),
        ("fastly", vec!["snapshot-origin", "native", "native-dot"]),
        ("cloudflare", vec!["route-unicode", "route-backslash"]),
    ] {
        let mut expected = common.clone();
        expected.extend(extra);
        let actual: std::collections::BTreeSet<_> = baseline[platform]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(actual, expected, "{platform}");
    }
    assert_eq!(
        baseline
            .as_object()
            .unwrap()
            .values()
            .map(|cases| cases.as_object().unwrap().len())
            .sum::<usize>(),
        56
    );
}

#[test]
fn missing_program_diagnostic_names_the_executable() {
    let program = "/missing-edgezero-runtime";
    let directory = tempfile::tempdir().expect("process log directory");
    let error = Process::spawn(
        &mut Command::new(program),
        &directory.path().join("runtime.log"),
    )
    .err()
    .map(super::io_error)
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
            &[
                "-c",
                "printf 'out-first\\n'; printf 'err-middle\\n' >&2; printf 'out-last\\n'; exit 7",
            ],
            &Arc::new(AtomicBool::new(false)),
            false,
        )
        .expect_err("noncapture must preserve command failure");
        assert!(error.to_string().contains("exit status: 7"), "{error}");
        return;
    }
    let output = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "fidelity::tests::noncaptured_command_streams_in_order_and_preserves_failure",
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
fn failed_commands_retain_complete_stdout_and_diagnostic_logs() {
    let directory = tempfile::tempdir().unwrap();
    for capture in [false, true] {
        let error = super::run_command(
            directory.path(),
            "/bin/sh",
            &["-c", "printf 'out'; printf '\\377err' >&2; exit 23"],
            &AtomicBool::new(false),
            capture,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let message = error.to_string();
        assert!(message.contains("23") && message.contains("out") && message.contains("err"));
        let (_, log) = message
            .rsplit_once("; log: ")
            .expect("retained command log");
        assert_eq!(
            std::fs::read(log).unwrap(),
            if capture {
                b"\xfferr".as_slice()
            } else {
                b"out\xfferr".as_slice()
            }
        );
        let logs = Path::new(log).parent().unwrap();
        if capture {
            assert_eq!(std::fs::read(logs.join("stdout.log")).unwrap(), b"out");
        }
        std::fs::remove_dir_all(logs).unwrap();
    }
}

#[test]
fn pending_command_cancellation_reaps_before_blocked_output_is_drained() {
    use std::os::fd::OwnedFd;
    use std::os::unix::{net::UnixStream, process::CommandExt as _};

    const CHILD: &str = "EDGEZERO_TEST_BLOCKED_COMMAND_OUTPUT";
    if let Some(directory) = std::env::var_os(CHILD) {
        crate::CANCELLED.store(false, Ordering::SeqCst);
        ctrlc::set_handler(crate::request_cancellation).unwrap();
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                "echo $$ > \"$COMMAND_TEST_DIR/pid\"; printf 'pending diagnostics\\n'; touch \"$COMMAND_TEST_DIR/ready\"; exec /bin/sleep 30",
            ])
            .env("COMMAND_TEST_DIR", directory);
        let error = super::run_prepared_command(&mut command, &AtomicBool::new(false), false)
            .expect_err("pending command must be cancelled");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted, "{error}");
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let (reader, mut writer) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    loop {
        match writer.write(&[b'x'; 8192]) {
            Ok(length) => assert!(length > 0),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("fill diagnostic output: {error}"),
        }
    }
    writer.set_nonblocking(false).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "fidelity::tests::pending_command_cancellation_reaps_before_blocked_output_is_drained",
            "--nocapture",
        ])
        .env(CHILD, directory.path())
        .stdout(Stdio::null())
        .stderr(Stdio::from(OwnedFd::from(writer)))
        .process_group(0)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !directory.path().join("ready").is_file() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let ready = directory.path().join("ready").is_file();
    let pid = std::fs::read_to_string(directory.path().join("pid")).unwrap_or_default();
    // Give the old live-forwarding poll a chance to block on the already-full socket.
    thread::sleep(Duration::from_millis(200));
    crate::signal_group_command(
        Command::new("kill").args(["-TERM", "--", &child.id().to_string()]),
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    let reaped = || {
        !pid.trim().is_empty()
            && !Command::new("kill")
                .args(["-0", "--", pid.trim()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    while !reaped() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let cleaned_before_drain = reaped();
    drop(reader);
    if !pid.trim().is_empty() {
        let _ = crate::signal_group(
            pid.trim().parse().unwrap(),
            "-KILL",
            Instant::now() + Duration::from_secs(2),
        );
    }
    let status = crate::wait_for_exit(&mut child, Instant::now() + Duration::from_secs(3)).unwrap();
    if status.is_none() {
        let _ = crate::signal_group(child.id(), "-KILL", Instant::now() + Duration::from_secs(2));
        crate::wait_for_exit(&mut child, Instant::now() + Duration::from_secs(2)).unwrap();
    }
    assert!(ready, "pending command did not start");
    assert!(
        cleaned_before_drain,
        "blocked diagnostic output prevented cancellation and child reaping"
    );
    assert!(status.is_some_and(|status| status.success()));
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
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(mode)).expect("permissions");
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
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let cancelled = Arc::new(AtomicBool::new(false));
    let error = super::verify_version(
        directory.path(),
        &tool.to_string_lossy(),
        "expected-pin",
        &cancelled,
    )
    .expect_err("wrong tool version");
    assert!(error.to_string().contains("expected-pin"), "{error:?}");
    assert!(error.to_string().contains("9.9.9"), "{error:?}");
}

#[test]
fn missing_tool_pin_names_the_tool() {
    let directory = tempfile::tempdir().expect("temporary fixture");
    std::fs::write(
        directory.path().join("Cargo.toml"),
        "[package]\nname = 'fixture'\n",
    )
    .expect("manifest");
    let error = super::fixture_tool_pin(directory.path(), "worker-build").expect_err("missing pin");
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
    std::fs::set_permissions(&bindgen, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut command = Command::new("worker-build");
    let error = super::configure_worker_build(&mut command, fixture, &bindgen, &cancelled)
        .expect_err("missing npm-locked esbuild must not trigger a download");
    assert!(error.to_string().contains("esbuild"));
    let esbuild = fixture.join("node_modules/.bin/esbuild");
    std::fs::create_dir_all(esbuild.parent().expect("parent")).expect("bin directory");
    std::fs::write(&esbuild, "#!/bin/sh\necho '0.27.3'\n").expect("binary");
    std::fs::set_permissions(&esbuild, std::fs::Permissions::from_mode(0o755)).expect("executable");
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
    let fixture = crate::fixture_root().join("../request-fidelity");
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
        let error =
            read_response(&mut reader).expect_err("should cancel both header and exact-body reads");
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
    let directory = tempfile::tempdir().expect("process log directory");
    let mut process = Process::spawn(&mut command, &directory.path().join("runtime.log"))
        .expect("should spawn owned group");
    let group = process.0.id();
    let worker_cancelled = Arc::clone(&cancelled);
    let stopper = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        worker_cancelled.store(true, Ordering::SeqCst);
    });
    let start = Instant::now();
    let error = process
        .wait_success_cancelled(&cancelled)
        .map_err(super::io_error)
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
