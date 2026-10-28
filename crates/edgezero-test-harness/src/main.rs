mod evidence;
mod net;
mod runners;
use evidence::{Result, require};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub struct Args {
    adapter: String,
    suite: String,
    output: Option<PathBuf>,
    requests: usize,
    repetitions: usize,
    seed: u64,
    construction_rounds: u64,
    max_requests: usize,
}
impl Args {
    fn parse() -> Result<Self> {
        Self::parse_from(std::env::args().skip(1))
    }

    fn parse_from<I>(arguments: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut a = Self {
            adapter: "fastly".into(),
            suite: "smoke".into(),
            output: None,
            requests: 100,
            repetitions: 3,
            seed: 856,
            construction_rounds: 0,
            max_requests: 10,
        };
        let mut adapter_selected = false;
        let mut args = arguments.into_iter();
        while let Some(key) = args.next() {
            if key == "--help" || key == "-h" {
                println!(
                    "edgezero-test-harness [--adapter fastly|cloudflare|spin|axum|all] [--suite smoke|benchmark|assembly|outbound-readiness|outbound-client-progress|outbound-progress|outbound-overflow|outbound-wire|outbound-lifetimes|response-framing|synthesized-headers] [--output EMPTY_DIR] [--requests 100] [--repetitions 3] [--seed 856] [--construction-rounds 0] [--max-requests 10]"
                );
                std::process::exit(0);
            }
            let value = args.next().ok_or(format!("missing value for {key}"))?;
            match key.as_str() {
                "--adapter" => {
                    a.adapter = value;
                    adapter_selected = true;
                }
                "--suite" => a.suite = value,
                "--output" => a.output = Some(value.into()),
                "--requests" => a.requests = value.parse()?,
                "--repetitions" => a.repetitions = value.parse()?,
                "--seed" => a.seed = value.parse()?,
                "--construction-rounds" => a.construction_rounds = value.parse()?,
                "--max-requests" => a.max_requests = value.parse()?,
                _ => return Err(format!("unknown option {key}").into()),
            }
        }
        require(
            ["fastly", "cloudflare", "spin", "axum", "all"].contains(&a.adapter.as_str()),
            "unknown adapter",
        )?;
        require(
            [
                "smoke",
                "benchmark",
                "assembly",
                "outbound-readiness",
                "outbound-client-progress",
                "outbound-progress",
                "outbound-overflow",
                "outbound-wire",
                "outbound-lifetimes",
                "response-framing",
                "synthesized-headers",
            ]
            .contains(&a.suite.as_str()),
            "unknown suite",
        )?;
        require(
            !matches!(
                a.suite.as_str(),
                "outbound-readiness" | "outbound-client-progress"
            ) || a.adapter == "fastly",
            "Fastly receive suites require --adapter fastly",
        )?;
        require(
            !matches!(a.suite.as_str(), "outbound-wire" | "outbound-lifetimes")
                || (adapter_selected && a.adapter != "all"),
            "wire and lifetime suites require one explicit adapter",
        )?;
        require(
            a.requests > 0 && a.repetitions > 0,
            "requests and repetitions must be positive",
        )?;
        require(
            (1..=1000).contains(&a.max_requests),
            "max-requests must be 1..1000",
        )?;
        Ok(a)
    }
}
pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/reusable-app")
        .canonicalize()
        .expect("fixture workspace root")
}
pub fn token() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{:x}-{:x}-{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
pub struct Records {
    path: PathBuf,
    values: Vec<Value>,
}
impl Records {
    fn collect_clients(&mut self, fields: Value) -> Result<()> {
        for event in net::take_client_events() {
            if !self.values.iter().any(|existing| {
                existing["event"] == "client_completed" && existing["token"] == event["token"]
            }) {
                self.push(annotate(event, fields.clone()))?;
            }
        }
        Ok(())
    }
    fn push(&mut self, v: Value) -> Result<()> {
        if matches!(
            v["status"].as_str(),
            Some("fail" | "unverified" | "unsupported")
        ) {
            eprintln!("edgezero-test-harness: {v}");
        }
        writeln!(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
            "{v}"
        )?;
        self.values.push(v);
        Ok(())
    }
}
pub fn logs(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| l.starts_with('{'))
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}
pub fn await_evidence(mut check: impl FnMut() -> Result<()>) -> Result<()> {
    let start = Instant::now();
    loop {
        match check() {
            Ok(()) => return Ok(()),
            Err(e) if start.elapsed() >= Duration::from_secs(5) => return Err(e),
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }
}
pub fn executable(variable: &str, default: &str) -> Option<PathBuf> {
    let value = std::env::var(variable).unwrap_or_else(|_| default.into());
    let path = PathBuf::from(&value);
    if path.components().count() > 1 {
        return path.canonicalize().ok();
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|p| p.join(&value))
        .find(|p| p.is_file())
        .and_then(|p| p.canonicalize().ok())
}
pub fn checked(command: &mut Command) -> Result<()> {
    let status = command.status()?;
    require(
        status.success(),
        format!("command {command:?} failed: {status}"),
    )
}
/// Process groups of live runtimes, killed by the Ctrl-C handler because a
/// terminal SIGINT reaches only the harness's own foreground group.
static LIVE_GROUPS: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalOutcome {
    Sent,
    Absent,
}

fn requested_termination(status: ExitStatus, term_sent: bool) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal() == Some(15) || (term_sent && status.code() == Some(128 + 15))
    }
    #[cfg(not(unix))]
    {
        let _ = (status, term_sent);
        false
    }
}
fn wait_for_exit(child: &mut Child, deadline: Instant) -> Result<Option<ExitStatus>> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}
fn signal_group(id: u32, signal: &str, deadline: Instant) -> Result<SignalOutcome> {
    #[cfg(unix)]
    let target = format!("-{id}");
    #[cfg(not(unix))]
    let target = id.to_string();
    signal_group_command(
        Command::new("kill")
            .args([signal, "--", &target])
            .env("LC_ALL", "C"),
        deadline,
    )
}
fn signal_group_command(command: &mut Command, deadline: Instant) -> Result<SignalOutcome> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    require(
        !remaining.is_zero(),
        "process-group signal deadline expired",
    )?;
    let execution_deadline = deadline - (remaining / 2).min(Duration::from_secs(1));
    let path = std::env::temp_dir().join(format!("fixture-signal-{}.log", token()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .ok();
    let captured = file.is_some();
    let outcome = (|| {
        let mut helper = command
            .stdout(Stdio::null())
            .stderr(file.map_or_else(Stdio::null, Stdio::from))
            .spawn()?;
        let status = match wait_for_exit(&mut helper, execution_deadline) {
            Ok(Some(status)) => status,
            outcome => {
                let mut failure = outcome.err().map_or_else(
                    || "process-group signal helper timed out".to_owned(),
                    |error| error.to_string(),
                );
                if let Err(error) = helper.kill() {
                    failure.push_str(&format!("; kill signal helper: {error}"));
                }
                match wait_for_exit(&mut helper, deadline) {
                    Ok(Some(_)) => {}
                    Ok(None) => failure.push_str("; signal helper was not reaped before deadline"),
                    Err(error) => failure.push_str(&format!("; reap signal helper: {error}")),
                }
                return Err(failure.into());
            }
        };
        if status.success() {
            return Ok(SignalOutcome::Sent);
        }
        require(
            captured,
            format!("process-group signal failed: {status}; diagnostics unavailable"),
        )?;
        let mut diagnostic = String::new();
        // A descendant may retain stderr; a regular-file read never waits for its EOF.
        fs::File::open(&path)?
            .take(4096)
            .read_to_string(&mut diagnostic)?;
        require(
            diagnostic.to_ascii_lowercase().contains("no such process"),
            format!("process-group signal failed: {status}: {diagnostic}"),
        )?;
        Ok(SignalOutcome::Absent)
    })();
    if captured {
        // Diagnostics must not determine whether a successfully sent signal is confirmed.
        let _ = fs::remove_file(&path);
    }
    outcome
}
fn kill_live_groups_and_exit() {
    for id in LIVE_GROUPS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
    {
        let _ = signal_group(*id, "-KILL", Instant::now() + Duration::from_secs(5));
    }
    std::process::exit(130);
}
pub struct Process(Child, ProcessState);
struct ProcessState {
    log: PathBuf,
    finished: Option<std::result::Result<(), String>>,
    shutdown_started: bool,
    failures: Vec<String>,
}
impl Process {
    pub fn spawn(command: &mut Command, log: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let file = fs::File::create(log)?;
        let p = Self(
            command
                .stdout(Stdio::from(file.try_clone()?))
                .stderr(Stdio::from(file))
                .spawn()?,
            ProcessState {
                log: log.to_owned(),
                finished: None,
                shutdown_started: false,
                failures: Vec::new(),
            },
        );
        LIVE_GROUPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(p.0.id());
        Ok(p)
    }
    pub fn start(command: &mut Command, log: &Path, port: u16) -> Result<Self> {
        let mut p = Self::spawn(command, log)?;
        let start = Instant::now();
        loop {
            p.check_running()?;
            if std::net::TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                Duration::from_millis(200),
            )
            .is_ok()
            {
                p.check_running()?;
                return Ok(p);
            }
            require(
                start.elapsed() < Duration::from_secs(45),
                "runtime did not listen",
            )?;
            thread::sleep(Duration::from_millis(50));
        }
    }
    pub fn check_running(&mut self) -> Result<()> {
        if let Some(outcome) = &self.1.finished {
            return Err(outcome
                .as_ref()
                .err()
                .cloned()
                .unwrap_or_else(|| {
                    format!("runtime already finished; see {}", self.1.log.display())
                })
                .into());
        }
        require(!self.1.shutdown_started, "runtime cleanup is still pending")?;
        match self.0.try_wait()? {
            None => Ok(()),
            Some(status) => Err(format!(
                "runtime exited before shutdown: {status}; see {}",
                self.1.log.display()
            )
            .into()),
        }
    }
    pub fn finish(&mut self) -> Result<()> {
        if let Some(outcome) = &self.1.finished {
            return outcome.clone().map_err(Into::into);
        }
        let mut failures = self.1.failures.clone();
        if !self.1.shutdown_started {
            if let Err(error) = self.check_running() {
                failures.push(error.to_string());
            }
            self.1.shutdown_started = true;
        }
        let id = self.0.id();
        let term_deadline = Instant::now() + Duration::from_secs(5);
        let term_sent = match signal_group(id, "-TERM", term_deadline) {
            Ok(outcome) => outcome == SignalOutcome::Sent,
            Err(error) => {
                failures.push(error.to_string());
                false
            }
        };
        let mut status = match wait_for_exit(&mut self.0, term_deadline) {
            Ok(status) => status,
            Err(error) => {
                failures.push(error.to_string());
                None
            }
        };
        if status.is_none() {
            failures.push("runtime required SIGKILL after its five-second TERM grace".to_owned());
        }
        // The parent may have exited while a descendant still holds sockets.
        let kill_deadline = Instant::now() + Duration::from_secs(5);
        #[cfg(unix)]
        let group_cleaned = match signal_group(id, "-KILL", kill_deadline) {
            Ok(_) => true,
            Err(error) => {
                failures.push(error.to_string());
                false
            }
        };
        #[cfg(not(unix))]
        let group_cleaned = true;
        if status.is_none()
            && (cfg!(not(unix)) || !group_cleaned)
            && let Err(error) = self.0.kill()
        {
            failures.push(error.to_string());
        }
        if status.is_none() {
            match wait_for_exit(&mut self.0, kill_deadline) {
                Ok(observed) => status = observed,
                Err(error) => failures.push(error.to_string()),
            }
        }
        match status {
            Some(status) => {
                let terminated = requested_termination(status, term_sent);
                if !status.success() && !terminated {
                    failures.push(format!(
                        "runtime exited unexpectedly during shutdown: {status}"
                    ));
                }
            }
            None => {
                failures.push("runtime did not exit within its five-second KILL grace".to_owned())
            }
        }
        let confirmed = status.is_some() && group_cleaned;
        if confirmed {
            LIVE_GROUPS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|live| *live != id);
        }
        self.1.failures = failures.clone();
        let outcome = if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{}; see {}",
                failures.join("; "),
                self.1.log.display()
            ))
        };
        if confirmed {
            self.1.finished = Some(outcome.clone());
        }
        outcome.map_err(Into::into)
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
pub fn req(port: u16, path: &str) -> Result<Value> {
    net::request(port, path, &token(), None, None)
}
pub fn guest(reply: &Value) -> Result<Value> {
    require(
        reply["status"] == 200,
        format!("unexpected response: {reply}"),
    )?;
    Ok(serde_json::from_str(
        reply["body"].as_str().ok_or("missing body")?,
    )?)
}
pub fn annotate(mut value: Value, fields: Value) -> Value {
    value
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    value
}
pub fn artifact(path: &Path) -> Result<Value> {
    let bytes = fs::read(path)?;
    let fingerprint = bytes.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    Ok(
        json!({"path":path,"bytes":bytes.len(),"fnv1a64":format!("{fingerprint:016x}"),"purpose":"non-security build identity"}),
    )
}
/// The compiler that `$CARGO` builds with: `$RUSTC` when set, else the
/// `rustc` beside `$CARGO`, else whichever `rustc` is on `PATH`.
fn rustc() -> PathBuf {
    if let Some(rustc) = std::env::var_os("RUSTC") {
        return rustc.into();
    }
    std::env::var_os("CARGO")
        .map(|cargo| {
            Path::new(&cargo).with_file_name(format!("rustc{}", std::env::consts::EXE_SUFFIX))
        })
        .filter(|rustc| rustc.is_file())
        .unwrap_or_else(|| "rustc".into())
}
fn run() -> Result<i32> {
    let args = Args::parse()?;
    ctrlc::set_handler(kill_live_groups_and_exit)?;
    let path = args
        .output
        .clone()
        .unwrap_or_else(|| root().join(".runs").join(token()));
    if path.exists() {
        require(
            fs::read_dir(&path)?.next().is_none(),
            "output directory must be empty; refusing to overwrite evidence",
        )?;
    }
    fs::create_dir_all(&path)?;
    let path = path.canonicalize()?;
    fs::copy(root().join("Cargo.lock"), path.join("Cargo.lock"))?;
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
        path.join("harness-Cargo.lock"),
    )?;
    let mut records = Records {
        path: path.join("events.jsonl"),
        values: Vec::new(),
    };
    records.push(json!({"event":"run","seed":args.seed,"suite":args.suite,"construction_rounds":args.construction_rounds,"max_requests":args.max_requests}))?;
    records.push(json!({"event":"build_environment","rustc":String::from_utf8_lossy(&Command::new(rustc()).arg("--version").output()?.stdout).trim(),"host_os":std::env::consts::OS,"host_arch":std::env::consts::ARCH,"lockfile":artifact(&path.join("Cargo.lock"))?,"harness_lockfile":artifact(&path.join("harness-Cargo.lock"))?}))?;
    let adapters = if args.adapter == "all" {
        vec!["fastly", "cloudflare", "spin", "axum"]
    } else {
        vec![args.adapter.as_str()]
    };
    for adapter in adapters {
        let recorded = records.values.len();
        let result = if adapter == "fastly" {
            runners::fastly(&args, &path, &mut records)
        } else {
            runners::provider(adapter, &args, &path, &mut records)
        };
        if let Err(error) = result {
            // Runners record their own contextual failure for this error; add one only
            // when they did not (an earlier non-fatal fail record must not hide it).
            let error = error.to_string();
            if !records.values[recorded..]
                .iter()
                .any(|r| r["status"] == "fail" && r["error"] == error.as_str())
            {
                records.push(json!({"status":"fail","adapter":adapter,"error":error}))?;
            }
        }
    }
    let summary = evidence::summarize(&records.values);
    if summary["build_identity"]["status"] == "invalid" {
        let builds = records
            .values
            .iter()
            .filter(|r| r["event"] == "build")
            .map(|r| json!({"variant":r["variant"],"adapter":r["adapter"],"repetition":r["repetition"],"fnv1a64":r["artifact"]["fnv1a64"]}))
            .collect::<Vec<_>>();
        records
            .push(json!({"status":"fail","reason":"mixed build fingerprints","builds":builds}))?;
    }
    let code = if records.values.iter().any(|r| r["status"] == "fail") {
        1
    } else if records
        .values
        .iter()
        .any(|r| r["status"] == "unverified" || r["status"] == "unsupported")
    {
        2
    } else {
        0
    };
    fs::write(
        path.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    for (key, value) in summary["variants"].as_object().unwrap() {
        println!(
            "{key}: {} samples, completion p50 {:.3} ms",
            value["samples"],
            value["completion_ns"]["p50"].as_f64().unwrap_or(0.) / 1_000_000.
        );
    }
    println!("{}", json!({"exit_code":code,"evidence":path}));
    Ok(code)
}
fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("edgezero-test-harness: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    fn metadata(manifest: &Path) -> Value {
        let output = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args([
                "metadata",
                "--offline",
                "--locked",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(manifest)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[test]
    fn runner_workspace_is_separate_from_runtime_and_fixture_graphs() {
        let runner = Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .unwrap();
        assert_eq!(runner.file_name().unwrap(), "edgezero-test-harness");
        let repository = root().join("../../..").canonicalize().unwrap();
        assert_eq!(runner, repository.join("crates/edgezero-test-harness"));
        let own = metadata(&runner.join("Cargo.toml"));
        assert_eq!(
            Path::new(own["workspace_root"].as_str().unwrap())
                .canonicalize()
                .unwrap(),
            runner
        );
        assert_eq!(own["packages"].as_array().unwrap().len(), 1);
        assert_eq!(own["packages"][0]["name"], "edgezero-test-harness");
        assert_eq!(own["packages"][0]["publish"], json!([]));
        assert!(
            own["packages"][0]["dependencies"]
                .as_array()
                .unwrap()
                .iter()
                .all(|dependency| {
                    !dependency["name"]
                        .as_str()
                        .unwrap()
                        .starts_with("edgezero-")
                })
        );
        for manifest in [repository.join("Cargo.toml"), root().join("Cargo.toml")] {
            let other = metadata(&manifest);
            assert!(other["packages"].as_array().unwrap().iter().all(|package| {
                package["name"] != "edgezero-test-harness" && package["name"] != "fixture-harness"
            }));
        }
        assert!(!root().join("crates/fixture-harness/Cargo.toml").exists());
        let launcher =
            fs::read_to_string(repository.join("scripts/smoke_test_reusable_app.sh")).unwrap();
        assert!(launcher.contains("crates/edgezero-test-harness/Cargo.toml"));
        assert!(launcher.find("cd \"$REPO_DIR\"").unwrap() < launcher.find("exec ").unwrap());
    }
}

#[cfg(test)]
mod argument_tests {
    use super::Args;

    #[test]
    fn outbound_lifetimes_selects_exactly_one_explicit_provider() {
        for adapter in ["fastly", "axum", "cloudflare", "spin", "all"] {
            let result = Args::parse_from(
                ["--adapter", adapter, "--suite", "outbound-lifetimes"].map(str::to_owned),
            );
            assert_eq!(result.is_ok(), adapter != "all", "{adapter}");
        }
        assert!(Args::parse_from(["--suite", "outbound-lifetimes"].map(str::to_owned)).is_err());
    }

    #[test]
    fn outbound_wire_suite_is_explicit_and_selects_one_supported_provider() {
        for adapter in ["fastly", "axum", "cloudflare", "spin", "all"] {
            let result = Args::parse_from(
                ["--adapter", adapter, "--suite", "outbound-wire"].map(str::to_owned),
            );
            assert_eq!(result.is_ok(), adapter != "all", "{adapter}");
        }
        assert!(Args::parse_from(["--suite", "outbound-wire"].map(str::to_owned)).is_err());
    }

    #[test]
    fn client_progress_suite_is_explicit_and_fastly_only() {
        for adapter in ["fastly", "axum", "cloudflare", "spin", "all"] {
            let result = Args::parse_from(
                ["--adapter", adapter, "--suite", "outbound-client-progress"].map(str::to_owned),
            );
            assert_eq!(result.is_ok(), adapter == "fastly", "{adapter}");
        }
    }

    #[test]
    fn full_readiness_suite_remains_explicit_and_fastly_only() {
        for adapter in ["fastly", "axum", "cloudflare", "spin", "all"] {
            let result = Args::parse_from(
                ["--adapter", adapter, "--suite", "outbound-readiness"].map(str::to_owned),
            );
            assert_eq!(result.is_ok(), adapter == "fastly", "{adapter}");
        }
    }
}

#[cfg(all(test, unix))]
mod process_tests {
    use super::*;
    use std::{net::TcpListener, os::unix::process::ExitStatusExt};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("fixture-process-{}", token()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn command(&self, mode: &str) -> Command {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "process_tests::outbound_wire_process_helper",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("EDGEZERO_PROCESS_TEST_MODE", mode)
                .env("EDGEZERO_PROCESS_TEST_DIR", &self.0);
            command
        }

        fn spawn(&self, mode: &str) -> Process {
            Process::spawn(&mut self.command(mode), &self.0.join("runtime.log")).unwrap()
        }

        fn ready(&self) {
            until(|| self.0.join("ready").is_file());
        }

        fn descendant_port(&self) -> u16 {
            fs::read_to_string(self.0.join("descendant"))
                .unwrap()
                .parse()
                .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn until(mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(
                Instant::now() < deadline,
                "subprocess observation timed out"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    #[ignore = "invoked only as a controlled Rust subprocess"]
    fn outbound_wire_process_helper() {
        let Ok(mode) = std::env::var("EDGEZERO_PROCESS_TEST_MODE") else {
            return;
        };
        let path = PathBuf::from(std::env::var_os("EDGEZERO_PROCESS_TEST_DIR").unwrap());
        if mode != "term-signal" {
            let ignore = matches!(mode.as_str(), "ignore-term" | "descendant-worker");
            let code = match mode.as_str() {
                "bad-shutdown" => 7,
                "term-status" => 143,
                _ => 0,
            };
            ctrlc::set_handler(move || {
                if !ignore {
                    std::process::exit(code);
                }
            })
            .unwrap();
        }
        if mode == "cleanup-tmp" || mode == "cleanup-retry" {
            let fixture = Fixture(path.clone());
            let kill = std::env::var_os("EDGEZERO_PROCESS_TEST_KILL").unwrap();
            let mut runtime = fixture.spawn(if mode == "cleanup-tmp" {
                "graceful"
            } else {
                "descendant"
            });
            fixture.ready();
            let outcome = (|| -> Result<()> {
                let _ = runtime.finish();
                require(runtime.0.try_wait()?.is_some(), "runtime survived finish")?;
                if mode == "cleanup-retry" {
                    require(
                        runtime.1.finished.is_none(),
                        "unconfirmed cleanup was cached",
                    )?;
                    require(
                        LIVE_GROUPS.lock().unwrap().contains(&runtime.0.id()),
                        "unconfirmed group cleanup lost ownership",
                    )?;
                    std::os::unix::fs::symlink(&kill, path.join("commands").join("kill"))?;
                } else {
                    fs::create_dir(std::env::temp_dir())?;
                }
                let _ = runtime.finish();
                require(
                    runtime.1.finished.is_some(),
                    "retry did not confirm cleanup",
                )?;
                require(
                    !LIVE_GROUPS.lock().unwrap().contains(&runtime.0.id()),
                    "confirmed cleanup retained the group",
                )?;
                Ok(())
            })();
            // Clean up the nested group even when exercising the unfixed implementation.
            if outcome.is_err() {
                let _ = Command::new(kill)
                    .args(["-KILL", "--", &format!("-{}", runtime.0.id())])
                    .status();
                let _ = runtime.0.kill();
                let _ = wait_for_exit(&mut runtime.0, Instant::now() + Duration::from_secs(5));
            }
            std::mem::forget(fixture);
            outcome.unwrap();
            fs::write(path.join("cleanup-checked"), b"complete").unwrap();
        }
        if mode == "signal-capture" {
            let fixture = Fixture(path.clone());
            assert_eq!(
                signal_group_command(
                    &mut fixture.command("signal-orphan"),
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap(),
                SignalOutcome::Absent
            );
            std::mem::forget(fixture);
            fs::write(path.join("signal-captured"), b"complete").unwrap();
        }
        if mode == "descendant-worker" {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            fs::write(
                path.join("descendant"),
                listener.local_addr().unwrap().port().to_string(),
            )
            .unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
                std::hint::black_box(&listener);
            }
        }
        if matches!(mode.as_str(), "descendant" | "orphan" | "signal-orphan") {
            let fixture = Fixture(path.clone());
            let mut descendant = fixture.command("descendant-worker").spawn().unwrap();
            // The child intentionally inherits our process group and log descriptors.
            std::mem::forget(fixture);
            until(|| path.join("descendant").is_file());
            assert!(descendant.try_wait().unwrap().is_none());
        }
        println!("helper-stdout");
        eprintln!("helper-stderr");
        std::io::stdout().flush().unwrap();
        std::io::stderr().flush().unwrap();
        fs::write(path.join("ready"), b"ready").unwrap();
        if mode == "signal-timeout" {
            fs::write(path.join("signal-pid"), std::process::id().to_string()).unwrap();
        }
        match mode.as_str() {
            "exit-zero" | "orphan" => std::process::exit(0),
            "signal-orphan" => {
                eprintln!("No such process");
                std::process::exit(1);
            }
            "exit-nonzero" => std::process::exit(7),
            "exit-term-status" => std::process::exit(143),
            _ => loop {
                thread::sleep(Duration::from_secs(1));
            },
        }
    }

    #[test]
    fn outbound_wire_process_rejects_premature_zero_and_nonzero_exits() {
        for mode in ["exit-zero", "exit-nonzero", "exit-term-status"] {
            let fixture = Fixture::new();
            let mut process = fixture.spawn(mode);
            fixture.ready();
            until(|| process.0.try_wait().unwrap().is_some());
            let error = process.check_running().unwrap_err().to_string();
            assert!(error.contains("runtime exited before shutdown"), "{error}");
            assert!(error.contains("runtime.log"), "{error}");
            assert!(process.finish().unwrap_err().to_string().contains(&error));
            let started = Instant::now();
            assert!(process.finish().unwrap_err().to_string().contains(&error));
            assert!(started.elapsed() < Duration::from_millis(200));
        }
    }

    #[test]
    fn outbound_wire_process_finish_detects_exit_without_a_prior_health_check() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("exit-zero");
        fixture.ready();
        until(|| process.0.try_wait().unwrap().is_some());
        assert!(
            process
                .finish()
                .unwrap_err()
                .to_string()
                .contains("runtime exited before shutdown")
        );
    }

    #[test]
    fn outbound_wire_process_signal_diagnostics_do_not_wait_for_descendant_eof() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("signal-capture");
        until(|| fixture.0.join("signal-captured").is_file());
        process.check_running().unwrap();
        let port = fixture.descendant_port();
        process.finish().unwrap();
        until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn outbound_wire_process_cleanup_survives_missing_diagnostics_and_retries_owned_groups() {
        for mode in ["cleanup-tmp", "cleanup-retry"] {
            let fixture = Fixture::new();
            let mut command = fixture.command(mode);
            command.env(
                "EDGEZERO_PROCESS_TEST_KILL",
                executable("KILL_BIN", "kill").unwrap(),
            );
            if mode == "cleanup-tmp" {
                command.env("TMPDIR", fixture.0.join("missing-temp"));
            } else {
                let commands = fixture.0.join("commands");
                fs::create_dir(&commands).unwrap();
                command.env("PATH", commands);
            }
            let mut process = Process::spawn(&mut command, &fixture.0.join("runtime.log")).unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            while !fixture.0.join("cleanup-checked").is_file() {
                let health = process.check_running();
                assert!(
                    health.is_ok(),
                    "{mode}: {health:?}: {}",
                    fs::read_to_string(fixture.0.join("runtime.log")).unwrap()
                );
                assert!(Instant::now() < deadline, "{mode}: cleanup fixture stalled");
                thread::sleep(Duration::from_millis(10));
            }
            process.finish().unwrap();
        }
    }

    #[test]
    fn outbound_wire_process_signal_timeout_reaps_the_helper_before_returning() {
        let fixture = Fixture::new();
        let started = Instant::now();
        let error = signal_group_command(
            &mut fixture.command("signal-timeout"),
            Instant::now() + Duration::from_millis(300),
        )
        .unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
        let pid = fs::read_to_string(fixture.0.join("signal-pid")).unwrap();
        assert!(
            !Command::new("ps")
                .args(["-p", &pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success(),
            "signal helper {pid} was not reaped"
        );
    }

    #[test]
    fn outbound_wire_process_accepts_requested_zero_or_sigterm_and_keeps_file_logs() {
        for mode in ["graceful", "term-signal", "term-status"] {
            let fixture = Fixture::new();
            let mut process = fixture.spawn(mode);
            fixture.ready();
            process.check_running().unwrap();
            process.finish().unwrap();
            process.finish().unwrap();
            let status = process.0.try_wait().unwrap().unwrap();
            assert!(status.success() || status.signal() == Some(15) || status.code() == Some(143));
            let output = fs::read_to_string(fixture.0.join("runtime.log")).unwrap();
            assert!(output.contains("helper-stdout") && output.contains("helper-stderr"));
        }
    }

    #[test]
    fn shell_term_exit_requires_a_confirmed_sent_signal() {
        let status = ExitStatus::from_raw(143 << 8);
        assert!(requested_termination(status, true));
        assert!(!requested_termination(status, false));
    }

    #[test]
    fn outbound_wire_process_rejects_unexpected_shutdown_status() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("bad-shutdown");
        fixture.ready();
        let error = process.finish().unwrap_err().to_string();
        assert!(error.contains("unexpectedly during shutdown"), "{error}");
        assert_eq!(process.0.try_wait().unwrap().unwrap().code(), Some(7));
    }

    #[test]
    fn outbound_wire_process_escalates_ignored_term_within_two_deadlines() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("ignore-term");
        fixture.ready();
        let started = Instant::now();
        let error = process.finish().unwrap_err().to_string();
        assert!(error.contains("SIGKILL"), "{error}");
        assert!(started.elapsed() >= Duration::from_secs(5));
        assert!(started.elapsed() < Duration::from_secs(11));
        assert_eq!(process.0.try_wait().unwrap().unwrap().signal(), Some(9));
        let started = Instant::now();
        assert_eq!(process.finish().unwrap_err().to_string(), error);
        assert!(started.elapsed() < Duration::from_millis(200));
    }

    #[test]
    fn outbound_wire_process_cleans_descendants_after_graceful_or_premature_parent_exit() {
        for mode in ["descendant", "orphan"] {
            let fixture = Fixture::new();
            let mut process = fixture.spawn(mode);
            fixture.ready();
            let port = fixture.descendant_port();
            assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
            if mode == "orphan" {
                until(|| process.0.try_wait().unwrap().is_some());
            }
            assert_eq!(process.finish().is_ok(), mode == "descendant");
            until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
            assert!(
                !LIVE_GROUPS
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&process.0.id())
            );
        }
    }

    #[test]
    fn outbound_wire_process_drop_uses_the_same_group_cleanup() {
        let fixture = Fixture::new();
        let process = fixture.spawn("descendant");
        fixture.ready();
        let port = fixture.descendant_port();
        drop(process);
        until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn outbound_wire_process_start_reports_exit_and_spawn_failure() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let started = Instant::now();
        let error = Process::start(
            &mut fixture.command("exit-zero"),
            &log,
            net::port().unwrap(),
        )
        .err()
        .expect("premature exit must fail startup")
        .to_string();
        assert!(error.contains("runtime exited before shutdown"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            Process::spawn(&mut Command::new(fixture.0.join("missing-runtime")), &log).is_err()
        );
    }
}
