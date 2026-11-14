mod checks;
mod evidence;
mod fidelity;
mod net;
mod runners;
mod startup;
use evidence::{Result, require};
use nix::{
    errno::Errno,
    sys::signal::{self, Signal},
    unistd::Pid,
};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    readiness_timeout: Duration,
    record_observations: bool,
}
impl Args {
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
            readiness_timeout: Duration::from_secs(40),
            record_observations: false,
        };
        let mut adapter_selected = false;
        let mut args = arguments.into_iter();
        while let Some(key) = args.next() {
            if key == "--help" || key == "-h" {
                println!(
                    "edgezero-test-harness [--adapter fastly|cloudflare|spin|axum|all] [--suite smoke|benchmark|assembly|outbound-readiness|outbound-client-progress|outbound-progress|outbound-overflow|outbound-wire|outbound-lifetimes|response-framing|synthesized-headers|ingress-fidelity|ci] [--output EMPTY_DIR] [--requests 100] [--repetitions 3] [--seed 856] [--construction-rounds 0] [--max-requests 10] [--readiness-timeout 40] [--record-observations]"
                );
                std::process::exit(0);
            }
            if key == "--record-observations" {
                a.record_observations = true;
                continue;
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
                "--readiness-timeout" => {
                    a.readiness_timeout = Duration::try_from_secs_f64(value.parse()?)?
                }
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
                "ingress-fidelity",
                "ci",
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
            a.suite != "ingress-fidelity" || (adapter_selected && a.adapter != "axum"),
            "ingress fidelity requires --adapter spin|fastly|cloudflare|all",
        )?;
        require(
            a.suite != "ci" || (adapter_selected && a.adapter != "all"),
            "ci requires one explicit adapter",
        )?;
        require(
            !a.readiness_timeout.is_zero(),
            "readiness timeout must be positive",
        )?;
        require(
            !a.record_observations || a.suite == "ingress-fidelity",
            "record-observations requires --suite ingress-fidelity",
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
pub fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}
pub fn acceptance_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("acceptance")
}
pub fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/reusable-app")
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
    fn push(&mut self, mut v: Value) -> Result<()> {
        startup::annotate(&mut v);
        if v["startup_identity"] == "unverified/no-credit" && v["status"] == "pass" {
            v["status"] = json!("pending");
        }
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
    parse_logs(&fs::read_to_string(path).unwrap_or_default())
}
pub fn parse_logs(text: &str) -> Vec<Value> {
    text.lines()
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
    let directory = tempfile::tempdir()?;
    let log = directory.path().join("command.log");
    let result = Process::spawn(command, &log).and_then(|mut process| process.wait_success());
    match result {
        Ok(_) => Ok(()),
        Err(error) => {
            let kind = error
                .downcast_ref::<std::io::Error>()
                .map_or(std::io::ErrorKind::Other, std::io::Error::kind);
            let mut diagnostics = Vec::new();
            if let Ok(file) = fs::File::open(&log) {
                let _ = file.take(64 * 1024).read_to_end(&mut diagnostics);
            }
            let retained = directory.keep().join("command.log");
            Err(std::io::Error::new(
                kind,
                format!(
                    "command {} failed: {error}; diagnostics: {}; log: {}",
                    command.get_program().to_string_lossy(),
                    String::from_utf8_lossy(&diagnostics),
                    retained.display()
                ),
            )
            .into())
        }
    }
}
/// Owned command/runtime groups, retained until bounded teardown is confirmed.
static LIVE_GROUPS: Mutex<std::collections::BTreeMap<u32, bool>> =
    Mutex::new(std::collections::BTreeMap::new());
static CANCELLED: AtomicBool = AtomicBool::new(false);

fn request_cancellation() {
    CANCELLED.store(true, Ordering::SeqCst);
    let groups = LIVE_GROUPS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    for (id, nested_harness) in groups {
        // A child harness must cascade to its own groups before its owner reaps it.
        let signal = if nested_harness {
            Signal::SIGTERM
        } else {
            Signal::SIGKILL
        };
        let _ = signal_target(
            SignalTarget::Group(id),
            Some(signal),
            Instant::now() + Duration::from_secs(2),
        );
    }
}

pub fn check_cancelled(cancelled: &AtomicBool) -> std::io::Result<()> {
    if cancelled.load(Ordering::SeqCst) || CANCELLED.load(Ordering::SeqCst) {
        Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "interrupted",
        ))
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalOutcome {
    Sent,
    Absent,
}

#[derive(Clone, Copy, Debug)]
enum SignalTarget {
    Group(u32),
    Pid(u32),
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
fn signal_target(
    target: SignalTarget,
    signal: Option<Signal>,
    deadline: Instant,
) -> Result<SignalOutcome> {
    let (SignalTarget::Group(id) | SignalTarget::Pid(id)) = target;
    let id = i32::try_from(id).ok().filter(|id| *id > 1).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid signal target ID")
    })?;
    require(Instant::now() < deadline, "process signal deadline expired")?;
    let id = Pid::from_raw(id);
    let result = match target {
        SignalTarget::Group(_) => signal::killpg(id, signal),
        SignalTarget::Pid(_) => signal::kill(id, signal),
    };
    require(Instant::now() < deadline, "process signal deadline expired")?;
    signal_outcome(result)
}

fn signal_outcome(result: nix::Result<()>) -> Result<SignalOutcome> {
    match result {
        Ok(()) => Ok(SignalOutcome::Sent),
        Err(Errno::ESRCH) => Ok(SignalOutcome::Absent),
        Err(error) => Err(error.into()),
    }
}
pub struct Process(startup::ChildHandle, ProcessState);
struct ProcessState {
    log: PathBuf,
    command_status: Option<ExitStatus>,
    expected_exit_code: i32,
    finished: Option<std::result::Result<(), String>>,
    shutdown_started: bool,
    failures: Vec<String>,
    launch: Option<std::sync::Arc<Mutex<startup::Launch>>>,
}
impl Process {
    pub fn spawn(command: &mut Command, log: &Path) -> Result<Self> {
        Self::spawn_logged(command, log, None)
    }
    pub fn spawn_with_stdout(command: &mut Command, log: &Path, stdout: &Path) -> Result<Self> {
        Self::spawn_logged(command, log, Some(stdout))
    }
    fn spawn_logged(command: &mut Command, log: &Path, stdout: Option<&Path>) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let file = fs::File::create(log)?;
        let output = match stdout {
            Some(path) => fs::File::create(path)?,
            None => file.try_clone()?,
        };
        let p = Self(
            startup::ChildHandle::new(
                command
                    .stdin(Stdio::null())
                    .stdout(Stdio::from(output))
                    .stderr(Stdio::from(file))
                    .spawn()
                    .map_err(|error| {
                        std::io::Error::new(
                            error.kind(),
                            format!("{}: {error}", command.get_program().to_string_lossy()),
                        )
                    })?,
            ),
            ProcessState {
                log: log.to_owned(),
                command_status: None,
                expected_exit_code: 0,
                finished: None,
                shutdown_started: false,
                failures: Vec::new(),
                launch: None,
            },
        );
        LIVE_GROUPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                p.0.id(),
                std::env::current_exe().is_ok_and(|own| command.get_program() == own.as_os_str()),
            );
        Ok(p)
    }
    pub fn start(command: &mut Command, log: &Path, port: u16) -> Result<Self> {
        Self::start_logged(command, log, None, port)
    }
    pub fn start_with_stdout(
        command: &mut Command,
        log: &Path,
        stdout: &Path,
        port: u16,
    ) -> Result<Self> {
        Self::start_logged(command, log, Some(stdout), port)
    }
    fn start_logged(
        command: &mut Command,
        log: &Path,
        stdout: Option<&Path>,
        port: u16,
    ) -> Result<Self> {
        let deadline = Instant::now() + Duration::from_secs(45);
        startup::require_configured(command)?;
        let mut p = Self::spawn_logged(command, log, stdout)?;
        p.1.launch = Some(startup::register(command, p.0.clone(), log, port, deadline));
        loop {
            p.check_running()?;
            if std::net::TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(200)),
            )
            .is_ok()
            {
                p.check_running()?;
                startup::check(port)?;
                return Ok(p);
            }
            require(Instant::now() < deadline, "runtime did not listen")?;
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            );
        }
    }
    pub fn check_running(&mut self) -> Result<()> {
        check_cancelled(&CANCELLED)?;
        self.ensure_running()
    }
    fn ensure_running(&mut self) -> Result<()> {
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
    pub fn wait_success(&mut self) -> Result<ExitStatus> {
        self.wait_success_cancelled(&CANCELLED)
    }
    pub fn wait_exit_code(&mut self, expected: i32) -> Result<ExitStatus> {
        self.wait_expected_cancelled(expected, &CANCELLED)
    }
    pub fn child_status(&self) -> std::io::Result<Option<ExitStatus>> {
        self.0.try_wait()
    }
    pub fn wait_success_cancelled(&mut self, cancelled: &AtomicBool) -> Result<ExitStatus> {
        self.wait_expected_cancelled(0, cancelled)
    }
    fn wait_expected_cancelled(
        &mut self,
        expected: i32,
        cancelled: &AtomicBool,
    ) -> Result<ExitStatus> {
        require(
            self.1.launch.is_none(),
            "cannot wait for command completion on a registered server",
        )?;
        if self.1.finished.is_some() {
            self.finish()?;
            let status = self.1.command_status.ok_or("process already shut down")?;
            require(
                status.code() == Some(expected),
                "command exit code differs from expected",
            )?;
            return Ok(status);
        }
        self.1.expected_exit_code = expected;
        loop {
            self.check_cancellation(cancelled)?;
            if let Some(status) = self.0.try_wait()? {
                return self.complete_command(status, expected, cancelled);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn check_cancellation(&mut self, cancelled: &AtomicBool) -> Result<()> {
        if let Err(error) = check_cancelled(cancelled) {
            let cleanup = self.finish();
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                cleanup.err().map_or_else(
                    || error.to_string(),
                    |cleanup| format!("{error}; cleanup: {cleanup}"),
                ),
            )
            .into());
        }
        Ok(())
    }

    fn complete_command(
        &mut self,
        status: ExitStatus,
        expected: i32,
        cancelled: &AtomicBool,
    ) -> Result<ExitStatus> {
        self.1.command_status = Some(status);
        self.check_cancellation(cancelled)?;
        if status.code() != Some(expected) {
            self.1.failures.push(format!("command exited: {status}"));
        }
        self.finish()?;
        Ok(status)
    }
    pub fn finish(&mut self) -> Result<()> {
        if let Some(outcome) = &self.1.finished {
            return outcome.clone().map_err(Into::into);
        }
        let mut failures = self.1.failures.clone();
        if !self.1.shutdown_started {
            if let Some(launch) = &self.1.launch
                && let Err(error) = launch.lock().unwrap().close()
            {
                failures.push(error.to_string());
            }
            if self.1.command_status.is_none()
                && let Err(error) = self.ensure_running()
            {
                failures.push(error.to_string());
            }
            self.1.shutdown_started = true;
        }
        let id = self.0.id();
        let term_deadline = Instant::now() + Duration::from_secs(5);
        let term_sent = match signal_target(
            SignalTarget::Group(id),
            Some(Signal::SIGTERM),
            term_deadline,
        ) {
            Ok(outcome) => outcome == SignalOutcome::Sent,
            Err(error) => {
                failures.push(error.to_string());
                false
            }
        };
        let mut status = match self.0.wait_until(term_deadline) {
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
        let group_cleaned = match signal_target(
            SignalTarget::Group(id),
            Some(Signal::SIGKILL),
            kill_deadline,
        ) {
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
            && let Err(error) =
                signal_target(SignalTarget::Pid(id), Some(Signal::SIGKILL), kill_deadline)
        {
            failures.push(error.to_string());
            if let Err(error) = self.0.kill() {
                failures.push(error.to_string());
            }
        }
        if status.is_none() {
            match self.0.wait_until(kill_deadline) {
                Ok(observed) => status = observed,
                Err(error) => failures.push(error.to_string()),
            }
        }
        match status {
            Some(status) => {
                let terminated = requested_termination(status, term_sent);
                if status.code() != Some(self.1.expected_exit_code) && !terminated {
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
                .remove(&id);
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
        if let (Some(launch), Err(error)) = (&self.1.launch, &outcome) {
            launch.lock().unwrap().teardown_failed(error);
        }
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
fn run(arguments: Vec<String>) -> Result<i32> {
    if arguments
        .first()
        .is_some_and(|argument| argument == "response-egress")
    {
        return evidence::response_egress::run(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "acceptance")
    {
        ctrlc::set_handler(request_cancellation)?;
        return evidence::cutover::run(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "check")
    {
        ctrlc::set_handler(request_cancellation)?;
        checks::run(&arguments[1..])?;
        return Ok(0);
    }
    let args = Args::parse_from(arguments)?;
    ctrlc::set_handler(request_cancellation)?;
    let path = args
        .output
        .clone()
        .unwrap_or_else(|| fixture_root().join(".runs").join(token()));
    if path.exists() {
        require(
            fs::read_dir(&path)?.next().is_none(),
            "output directory must be empty; refusing to overwrite evidence",
        )?;
    }
    fs::create_dir_all(&path)?;
    let path = path.canonicalize()?;
    let _output_directory = startup::OutputDirectory::enter(&path)?;
    if args.suite == "ingress-fidelity" {
        fidelity::run(&args, &path)?;
        return Ok(0);
    }
    fs::copy(fixture_root().join("Cargo.lock"), path.join("Cargo.lock"))?;
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
        path.join("harness-Cargo.lock"),
    )?;
    let mut records = Records {
        path: path.join("events.jsonl"),
        values: Vec::new(),
    };
    records.push(json!({"event":"run","seed":args.seed,"suite":args.suite,"construction_rounds":args.construction_rounds,"max_requests":args.max_requests,"fixture_build_id":startup::build_id()?}))?;
    records.push(json!({"event":"build_environment","rustc":String::from_utf8_lossy(&Command::new(rustc()).arg("--version").output()?.stdout).trim(),"host_os":std::env::consts::OS,"host_arch":std::env::consts::ARCH,"lockfile":artifact(&path.join("Cargo.lock"))?,"harness_lockfile":artifact(&path.join("harness-Cargo.lock"))?}))?;
    let adapters = if args.adapter == "all" {
        vec!["fastly", "cloudflare", "spin", "axum"]
    } else {
        vec![args.adapter.as_str()]
    };
    for adapter in adapters {
        let recorded = records.values.len();
        let result = if args.suite == "ci" {
            runners::ci::run(&args, &path, &mut records)
        } else if adapter == "fastly" {
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
    exit_result(run(std::env::args().skip(1).collect()));
}

fn exit_result(result: Result<i32>) -> ! {
    match result {
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
        let repository = repository_root();
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
        for manifest in [
            repository.join("Cargo.toml"),
            fixture_root().join("Cargo.toml"),
        ] {
            let other = metadata(&manifest);
            assert!(other["packages"].as_array().unwrap().iter().all(|package| {
                package["name"] != "edgezero-test-harness" && package["name"] != "fixture-harness"
            }));
        }
        assert!(
            !fixture_root()
                .join("crates/fixture-harness/Cargo.toml")
                .exists()
        );
        assert!(!repository.join("tests/fixtures/reusable-app").exists());
        assert!(!repository.join("tests/fixtures/request-fidelity").exists());
        assert!(
            acceptance_root()
                .join("outbound-cutover-cases.json")
                .is_file()
        );
        let launcher = fs::read_to_string(repository.join("scripts/test_integration.sh")).unwrap();
        assert!(launcher.contains("crates/edgezero-test-harness/Cargo.toml"));
        assert!(launcher.find("cd \"$REPO_DIR\"").unwrap() < launcher.find("exec ").unwrap());
    }
}

#[cfg(test)]
mod command_exit_tests {
    use super::*;

    #[test]
    fn explicit_unqualified_exit_never_accepts_success_failure_or_a_signal() {
        for (script, accepted) in [
            ("exit 2", true),
            ("exit 0", false),
            ("exit 23", false),
            ("kill -TERM $$", false),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut process = Process::spawn(
                Command::new("sh").args(["-c", script]),
                &directory.path().join("child.log"),
            )
            .unwrap();
            assert_eq!(process.wait_exit_code(2).is_ok(), accepted, "{script}");
            assert!(
                process.wait_success().is_err(),
                "unqualified is never a successful command"
            );
        }
    }
}

#[cfg(test)]
mod argument_tests {
    use super::Args;

    #[test]
    fn fidelity_and_ci_require_their_supported_explicit_adapters() {
        for adapter in ["fastly", "cloudflare", "spin", "axum", "all"] {
            for suite in ["ingress-fidelity", "ci"] {
                let result =
                    Args::parse_from(["--adapter", adapter, "--suite", suite].map(str::to_owned));
                assert_eq!(
                    result.is_ok(),
                    if suite == "ci" {
                        adapter != "all"
                    } else {
                        adapter != "axum"
                    }
                );
            }
        }
        for suite in ["ingress-fidelity", "ci"] {
            assert!(Args::parse_from(["--suite", suite].map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn fidelity_options_preserve_recording_and_finite_positive_readiness() {
        let args = Args::parse_from(
            [
                "--adapter",
                "spin",
                "--suite",
                "ingress-fidelity",
                "--record-observations",
                "--readiness-timeout",
                "0.25",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert!(args.record_observations);
        assert_eq!(
            args.readiness_timeout,
            std::time::Duration::from_millis(250)
        );
        for timeout in ["0", "-1", "NaN", "inf", "1e100"] {
            assert!(
                Args::parse_from(
                    [
                        "--adapter",
                        "spin",
                        "--suite",
                        "ingress-fidelity",
                        "--readiness-timeout",
                        timeout
                    ]
                    .map(str::to_owned)
                )
                .is_err(),
                "{timeout}"
            );
        }
        assert!(Args::parse_from(["--record-observations"].map(str::to_owned)).is_err());
        assert!(
            Args::parse_from(
                [
                    "--adapter",
                    "spin",
                    "--suite",
                    "ingress-fidelity",
                    "--skip-build"
                ]
                .map(str::to_owned)
            )
            .is_err()
        );
    }

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
    // Supervisor fixtures do not build guest code or probe the real compiler.
    const TEST_BUILD_ID: &str = "process-supervisor-fixture";

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
            startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
            command
        }

        fn spawn(&self, mode: &str) -> Process {
            Process::spawn(&mut self.command(mode), &self.0.join("runtime.log")).unwrap()
        }

        fn start_responder(&self, command: &mut Command) -> Result<(Process, u16)> {
            let started = Instant::now();
            let deadline = started + Duration::from_secs(45);
            startup::require_configured(command)?;
            let nonce = command
                .get_envs()
                .find_map(|(name, value)| (name == startup::NONCE).then_some(value).flatten())
                .and_then(|value| value.to_str())
                .ok_or("missing fixture launch nonce")?
                .to_owned();
            let log = self.0.join("runtime.log");
            let mut process = Process::spawn(command, &log)?;
            let port = self.responder_port(
                &mut process,
                &nonce,
                (started + Duration::from_secs(5)).min(deadline),
            )?;
            process.1.launch = Some(startup::register(
                command,
                process.0.clone(),
                &log,
                port,
                deadline,
            ));
            startup::check(port)?;
            Ok((process, port))
        }

        fn responder_port(
            &self,
            process: &mut Process,
            nonce: &str,
            deadline: Instant,
        ) -> Result<u16> {
            loop {
                process.check_running()?;
                require(
                    Instant::now() < deadline,
                    "fixture endpoint deadline expired",
                )?;
                match fs::File::open(self.0.join("endpoint.json")) {
                    Ok(file) => {
                        let mut bytes = Vec::new();
                        file.take(1025).read_to_end(&mut bytes)?;
                        require(bytes.len() <= 1024, "fixture endpoint exceeds limit")?;
                        let value: Value = serde_json::from_slice(&bytes)
                            .map_err(|_| "invalid fixture endpoint JSON")?;
                        require(
                            value.as_object().is_some_and(|fields| fields.len() == 3)
                                && value["pid"].as_u64() == Some(u64::from(process.0.id()))
                                && value["nonce"].as_str() == Some(nonce),
                            "fixture endpoint identity mismatch",
                        )?;
                        let port = value["port"]
                            .as_u64()
                            .and_then(|port| u16::try_from(port).ok())
                            .filter(|port| *port != 0)
                            .ok_or("invalid fixture endpoint port")?;
                        process.check_running()?;
                        require(
                            Instant::now() < deadline,
                            "fixture endpoint deadline expired",
                        )?;
                        return Ok(port);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                thread::sleep(
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(5)),
                );
            }
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
            if thread::panicking() {
                eprintln!("preserved failed process fixture: {}", self.0.display());
                return;
            }
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct GroupCleanup(Option<u32>);

    impl GroupCleanup {
        fn finish(&mut self) -> Result<()> {
            if let Some(id) = self.0 {
                signal_target(
                    SignalTarget::Group(id),
                    Some(Signal::SIGKILL),
                    Instant::now() + Duration::from_secs(2),
                )?;
                self.0 = None;
            }
            Ok(())
        }
    }

    impl Drop for GroupCleanup {
        fn drop(&mut self) {
            let _ = self.finish();
        }
    }

    fn static_check_command(fixture: &Fixture, selection: &str) -> Command {
        let mut command = fixture.command("static-check");
        command
            .current_dir(&fixture.0)
            .env(
                "EDGEZERO_STATIC_CHECK_ARGS",
                json!(["check", selection]).to_string(),
            )
            .env("FASTLY_CLI", "/missing/fastly")
            .env("SPIN_CLI", "/missing/spin");
        command
    }

    fn fixture_runs() -> std::collections::BTreeSet<PathBuf> {
        match fs::read_dir(fixture_root().join(".runs")) {
            Ok(entries) => entries.map(|entry| entry.unwrap().path()).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(error) => panic!("cannot observe fixture runs: {error}"),
        }
    }

    #[test]
    fn static_check_help_runs_without_provider_tools_or_fixture_output() {
        let fixture = Fixture::new();
        let before = fixture_runs();
        let output = checks::output_until(
            &mut static_check_command(&fixture, "--help"),
            64 * 1024,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
        assert!(output.contains("check [all|dependencies|forwarding|image-optimizer]"));
        assert_eq!(fixture_runs(), before);
        assert!(fs::read_dir(&fixture.0).unwrap().next().is_none());
    }

    #[test]
    fn acceptance_cli_runs_without_provider_tools_or_fixture_output() {
        let fixture = Fixture::new();
        let before = fixture_runs();
        for mode in ["--help", "--manifest", "--coverage"] {
            let mut command = static_check_command(&fixture, "--help");
            command.env(
                "EDGEZERO_STATIC_CHECK_ARGS",
                json!(["acceptance", mode]).to_string(),
            );
            let output = checks::output_until(
                &mut command,
                1024 * 1024,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();
            if mode == "--help" {
                assert!(output.contains("qualified remains false"));
            } else {
                let report = output
                    .lines()
                    .find_map(|line| {
                        serde_json::from_str::<Value>(
                            line.strip_prefix(
                                "test process_tests::outbound_wire_process_helper ... ",
                            )
                            .unwrap_or(line),
                        )
                        .ok()
                    })
                    .expect("CLI JSON report");
                assert_eq!(report["qualified"], false);
            }
        }
        let mut command = static_check_command(&fixture, "--help");
        command.env(
            "EDGEZERO_STATIC_CHECK_ARGS",
            json!(["acceptance", "--unknown"]).to_string(),
        );
        let error = checks::output_until(
            &mut command,
            64 * 1024,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("choose --manifest"), "{error}");
        assert_eq!(fixture_runs(), before);
        assert!(fs::read_dir(&fixture.0).unwrap().next().is_none());
    }

    #[test]
    fn acceptance_cancellation_reaps_blocked_git_without_exporting() {
        use std::os::unix::fs::PermissionsExt;
        for signal in [Signal::SIGINT, Signal::SIGTERM] {
            let fixture = Fixture::new();
            let wrapper = fixture.0.join("git");
            fs::write(&wrapper,"#!/bin/sh\nexport EDGEZERO_PROCESS_TEST_MODE=acceptance-blocked-git\nexec \"$EDGEZERO_TEST_HELPER\" --ignored --exact process_tests::outbound_wire_process_helper --nocapture --test-threads=1\n").unwrap();
            fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
            let input = fixture.0.join("observations.json");
            fs::write(
                &input,
                serde_json::to_vec(&json!({"stage":"frozen-capture","source_state":"clean"}))
                    .unwrap(),
            )
            .unwrap();
            let output = fixture.0.join("record.json");
            let args = json!([
                "acceptance",
                "--export-frozen",
                "--candidate",
                "a".repeat(40),
                "--input",
                input,
                "--artifacts",
                fixture.0,
                "--output",
                output,
                "--candidate-root",
                fixture.0,
                "--provenance",
                "provenance.json",
                "--provenance-sha256",
                "b".repeat(64)
            ]);
            let mut command = static_check_command(&fixture, "--help");
            command
                .env("EDGEZERO_STATIC_CHECK_ARGS", args.to_string())
                .env("EDGEZERO_TEST_HELPER", std::env::current_exe().unwrap())
                .env("PATH", &fixture.0);
            let mut controller =
                Process::spawn(&mut command, &fixture.0.join("controller.log")).unwrap();
            until(|| fixture.0.join("git-started").is_file());
            let git = fs::read_to_string(fixture.0.join("git-started"))
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let mut cleanup = GroupCleanup(Some(git));
            signal_target(
                SignalTarget::Pid(controller.0.id()),
                Some(signal),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
            let result = controller.wait_exit_code(1);
            let observed = signal_target(
                SignalTarget::Pid(git),
                None,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
            cleanup.finish().unwrap();
            assert!(result.is_ok(), "{signal}: {result:?}");
            assert_eq!(
                observed,
                SignalOutcome::Absent,
                "{signal}: Git survived cancellation"
            );
            assert!(!output.exists());
        }
    }

    #[test]
    fn unknown_static_check_fails_without_starting_a_runtime() {
        let fixture = Fixture::new();
        let before = fixture_runs();
        let error = checks::output_until(
            &mut static_check_command(&fixture, "unknown"),
            64 * 1024,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown static check"), "{error}");
        assert_eq!(fixture_runs(), before);
        assert!(fs::read_dir(&fixture.0).unwrap().next().is_none());
    }

    #[test]
    fn dependency_checks_share_one_metadata_snapshot_per_workspace() {
        use std::os::unix::fs::PermissionsExt;
        let fixture = Fixture::new();
        let wrapper = fixture.0.join("cargo-wrapper");
        let calls = fixture.0.join("calls.log");
        fs::write(&wrapper, "#!/bin/sh\nprintf '%s\\t%s\\n' \"$PWD\" \"$*\" >> \"$CHECK_CALLS\"\nexec \"$REAL_CARGO\" \"$@\"\n").unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700)).unwrap();
        let mut command = static_check_command(&fixture, "dependencies");
        command
            .env("CARGO", &wrapper)
            .env(
                "REAL_CARGO",
                std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()),
            )
            .env("CHECK_CALLS", &calls);
        checks::output_until(
            &mut command,
            64 * 1024,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
        let calls = fs::read_to_string(calls).unwrap();
        let metadata = calls
            .lines()
            .filter(|line| line.contains("\tmetadata "))
            .collect::<Vec<_>>();
        assert_eq!(metadata.len(), 3, "{calls}");
        let roots = metadata
            .iter()
            .map(|line| line.split_once('\t').unwrap().0)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(roots.len(), 3, "{calls}");
        for line in metadata {
            assert!(
                line.ends_with("metadata --locked --offline --format-version 1 --all-features"),
                "{line}"
            );
        }
        assert_eq!(calls.lines().count(), 4, "{calls}");
        assert_eq!(calls.lines().filter(|line| line.ends_with("tree --locked --offline --package edgezero-cli --no-default-features --edges features,no-dev --invert serde_json")).count(), 1, "{calls}");
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
        if mode == "static-check" {
            let arguments =
                serde_json::from_str(&std::env::var("EDGEZERO_STATIC_CHECK_ARGS").unwrap())
                    .unwrap();
            exit_result(run(arguments));
        }
        if mode == "acceptance-blocked-git" {
            fs::write(path.join("git-started"), std::process::id().to_string()).unwrap();
            thread::sleep(Duration::from_secs(30));
            return;
        }
        if mode == "checked-controller" {
            ctrlc::set_handler(request_cancellation).unwrap();
            let fixture = Fixture(path.clone());
            let runtime = fixture.command("descendant");
            let mut command = Command::new("/bin/sh");
            command
                .args([
                    "-c",
                    "echo $$ > \"$EDGEZERO_PROCESS_TEST_DIR/checked-child\"; exec \"$@\"",
                    "owned-checked",
                ])
                .arg(runtime.get_program())
                .args(runtime.get_args())
                .envs(runtime.get_envs().map(|(key, value)| (key, value.unwrap())));
            let error = checked(&mut command).unwrap_err();
            assert!(error.to_string().contains("interrupted"), "{error}");
            assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .map(std::io::Error::kind),
                Some(std::io::ErrorKind::Interrupted)
            );
            let id = fs::read_to_string(path.join("checked-child")).unwrap();
            let id = id.trim();
            assert!(
                !LIVE_GROUPS
                    .lock()
                    .unwrap()
                    .contains_key(&id.parse().unwrap())
            );
            assert_eq!(
                signal_target(
                    SignalTarget::Pid(id.parse().unwrap()),
                    None,
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap(),
                SignalOutcome::Absent,
            );
            fs::write(path.join("checked-reaped"), b"complete").unwrap();
            std::mem::forget(fixture);
            return;
        }
        if mode == "cascade-controller" {
            ctrlc::set_handler(request_cancellation).unwrap();
            let fixture = Fixture(path.clone());
            let mut child = fixture.spawn("blocked-controller");
            assert_eq!(LIVE_GROUPS.lock().unwrap().get(&child.0.id()), Some(&true));
            let error = child.wait_success().unwrap_err();
            assert!(error.to_string().contains("interrupted"), "{error}");
            assert!(child.0.try_wait().unwrap().is_some());
            fs::write(path.join("cascade-reaped"), b"complete").unwrap();
            std::mem::forget(fixture);
            return;
        }
        if matches!(mode.as_str(), "command-controller" | "blocked-controller") {
            ctrlc::set_handler(request_cancellation).unwrap();
            let fixture = Fixture(path.clone());
            let runtime = fixture.command("descendant");
            let mut command = Command::new("/bin/sh");
            command
                .args(["-c", "exec \"$@\"", "owned-runtime"])
                .arg(runtime.get_program())
                .args(runtime.get_args())
                .envs(runtime.get_envs().map(|(key, value)| (key, value.unwrap())));
            let mut child = Process::spawn(&mut command, &path.join("runtime.log")).unwrap();
            assert_eq!(LIVE_GROUPS.lock().unwrap().get(&child.0.id()), Some(&false));
            if mode == "blocked-controller" {
                fixture.ready();
                fs::write(path.join("nested-runtime"), child.0.id().to_string()).unwrap();
                fs::write(path.join("controller-blocked"), b"network wait").unwrap();
                thread::sleep(Duration::from_secs(30));
            }
            let error = child.wait_success().unwrap_err();
            assert!(error.to_string().contains("interrupted"), "{error}");
            assert!(child.0.try_wait().unwrap().is_some());
            assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&child.0.id()));
            fs::write(path.join("cancellation-reaped"), b"complete").unwrap();
            std::mem::forget(fixture);
            return;
        }
        if mode == "startup-responder" {
            use std::io::{BufRead, BufReader};
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let identity = std::env::var("EDGEZERO_PROCESS_TEST_IDENTITY").unwrap();
            let later_identity = std::env::var("EDGEZERO_PROCESS_TEST_LATER_IDENTITY").ok();
            let skip_identity = std::env::var("EDGEZERO_PROCESS_TEST_SKIP_IDENTITY")
                .map(|count| count.parse::<usize>().unwrap())
                .unwrap_or(0);
            let endpoint = json!({
                "port": listener.local_addr().unwrap().port(),
                "pid": std::process::id(),
                "nonce": std::env::var(startup::NONCE).unwrap(),
            });
            fs::write(
                path.join("endpoint.tmp"),
                serde_json::to_vec(&endpoint).unwrap(),
            )
            .unwrap();
            fs::rename(path.join("endpoint.tmp"), path.join("endpoint.json")).unwrap();
            let mut ordinal = 0;
            for socket in listener.incoming() {
                let mut socket = socket.unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(socket.try_clone().unwrap());
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap() == 0 {
                        break;
                    }
                    request.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                }
                if request.is_empty() {
                    continue;
                }
                let mut body_length = None;
                for line in request.lines().skip(1) {
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        assert!(body_length.is_none(), "duplicate fixture request length");
                        let length = value.trim().parse::<usize>().unwrap();
                        assert!(length <= 16 * 1024, "fixture request body exceeds 16 KiB");
                        body_length = Some(length);
                    }
                }
                let mut body_bytes = vec![0; body_length.unwrap_or(0)];
                reader.read_exact(&mut body_bytes).unwrap();
                fs::write(path.join("request-body"), body_bytes).unwrap();
                ordinal += 1;
                fs::write(path.join("request"), &request).unwrap();
                fs::write(path.join("ordinal"), ordinal.to_string()).unwrap();
                let body = json!({"ordinal":ordinal}).to_string();
                let identity = if ordinal > 1 {
                    later_identity.as_deref().unwrap_or(&identity)
                } else {
                    &identity
                };
                let skip = identity.is_empty() || ordinal <= skip_identity;
                let identity = if skip {
                    String::new()
                } else {
                    format!("{}: {identity}\r\n", startup::HEADER)
                };
                write!(socket, "HTTP/1.1 200 OK\r\n{identity}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
            return;
        }
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
        if mode == "cleanup-no-tools" {
            let fixture = Fixture(path.clone());
            let mut runtime = fixture.spawn("descendant");
            fixture.ready();
            runtime.finish().unwrap();
            assert!(runtime.0.try_wait().unwrap().is_some());
            assert!(runtime.1.finished.is_some());
            assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&runtime.0.id()));
            std::mem::forget(fixture);
            fs::write(path.join("cleanup-checked"), b"complete").unwrap();
        }
        if mode == "descendant-worker" {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            fs::write(path.join("descendant-pid"), std::process::id().to_string()).unwrap();
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
        if matches!(mode.as_str(), "descendant" | "orphan") {
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
        match mode.as_str() {
            "exit-zero" | "orphan" => std::process::exit(0),
            "exit-nonzero" => std::process::exit(7),
            "exit-term-status" => std::process::exit(143),
            _ => loop {
                thread::sleep(Duration::from_secs(1));
            },
        }
    }

    #[test]
    fn nested_harness_cancellation_stops_runtime_while_network_wait_is_blocked() {
        fn alive(id: u32) -> bool {
            let output = Command::new("ps")
                .args(["-o", "stat=", "-p", &id.to_string()])
                .output()
                .unwrap();
            assert!(
                output.stderr.is_empty(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            output.status.success()
                && !String::from_utf8_lossy(&output.stdout)
                    .trim_start()
                    .starts_with('Z')
        }
        for mode in ["blocked-controller", "cascade-controller"] {
            let fixture = Fixture::new();
            let mut controller = fixture.spawn(mode);
            until(|| fixture.0.join("controller-blocked").is_file());
            let port = fixture.descendant_port();
            let runtime = fs::read_to_string(fixture.0.join("nested-runtime"))
                .unwrap()
                .parse::<u32>()
                .unwrap();
            let mut nested_cleanup = GroupCleanup(Some(runtime));
            let descendant = fs::read_to_string(fixture.0.join("descendant-pid"))
                .unwrap()
                .parse::<u32>()
                .unwrap();
            assert!(alive(runtime) && alive(descendant));
            let started = Instant::now();
            signal_target(
                SignalTarget::Group(controller.0.id()),
                Some(Signal::SIGTERM),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
            // Inspect before killing the blocked owner. Always clean the nested group on red.
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline && (alive(runtime) || alive(descendant)) {
                thread::sleep(Duration::from_millis(10));
            }
            let stopped = !alive(runtime) && !alive(descendant);
            let elapsed = started.elapsed();
            if stopped {
                nested_cleanup.0 = None;
            }
            assert!(
                controller.0.try_wait().unwrap().is_none(),
                "owner must still be blocked"
            );
            nested_cleanup.finish().unwrap();
            if mode == "cascade-controller" {
                controller.wait_success().unwrap();
                assert!(fixture.0.join("cascade-reaped").is_file());
            } else {
                let _ = signal_target(
                    SignalTarget::Group(controller.0.id()),
                    Some(Signal::SIGKILL),
                    Instant::now() + Duration::from_secs(2),
                );
                let _ = controller.finish();
            }
            assert!(
                stopped,
                "flag-only cancellation orphaned the nested runtime while its owner was blocked"
            );
            assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
            assert!(elapsed < Duration::from_secs(3));
        }
    }

    #[test]
    fn root_signal_cancellation_reaps_commands_and_descendants() {
        for signal in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGHUP] {
            let fixture = Fixture::new();
            let mut controller = fixture.spawn("command-controller");
            fixture.ready();
            let port = fixture.descendant_port();
            signal_target(
                SignalTarget::Group(controller.0.id()),
                Some(signal),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
            controller.wait_success().unwrap();
            assert!(fixture.0.join("cancellation-reaped").is_file(), "{signal}");
            until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        }
    }

    #[test]
    fn checked_preserves_failure_status_diagnostics_and_log() {
        checked(Command::new("/bin/sh").args(["-c", "exit 0"])).unwrap();
        let error = checked(Command::new("/bin/sh").args([
            "-c",
            "printf 'checked-out'; printf '\\377checked-err' >&2; exit 23",
        ]))
        .unwrap_err()
        .to_string();
        assert!(error.contains("23") && error.contains("/bin/sh"), "{error}");
        assert!(
            error.contains("checked-out") && error.contains("checked-err"),
            "{error}"
        );
        assert!(error.contains('\u{fffd}'), "{error}");
        let (_, log) = error.rsplit_once("; log: ").expect("retained command log");
        assert_eq!(fs::read(log).unwrap(), b"checked-out\xffchecked-err");
        fs::remove_dir_all(Path::new(log).parent().unwrap()).unwrap();
    }

    #[test]
    fn checked_cancellation_reaps_the_command_and_descendants() {
        for signal in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGHUP] {
            let fixture = Fixture::new();
            let mut controller = fixture.spawn("checked-controller");
            fixture.ready();
            let port = fixture.descendant_port();
            signal_target(
                SignalTarget::Pid(controller.0.id()),
                Some(signal),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            while controller.child_status().unwrap().is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            let completed = controller.child_status().unwrap().is_some();
            let outcome = if completed {
                controller.wait_success().map(|_| ())
            } else {
                controller.finish()
            };
            assert!(completed, "checked() ignored direct {signal} cancellation");
            outcome.unwrap();
            assert!(fixture.0.join("checked-reaped").is_file(), "{signal}");
            until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        }
    }

    fn assert_progress_cargo_cancellation(adapter: &str) {
        use std::os::unix::fs::PermissionsExt;

        let fixture = Fixture::new();
        let cargo = fixture.0.join("cargo");
        fs::write(&cargo, r#"#!/bin/sh
case "$1" in
  --version) printf 'cargo process-fixture\n'; exit 0 ;;
  build|metadata)
    printf '%s' "$$" > "$EDGEZERO_PROCESS_TEST_DIR/cargo-started"
    export EDGEZERO_PROCESS_TEST_MODE=descendant
    exec "$EDGEZERO_TEST_HELPER" --ignored --exact process_tests::outbound_wire_process_helper --nocapture --test-threads=1
    ;;
  *) exit 64 ;;
esac
"#).unwrap();
        fs::set_permissions(&cargo, fs::Permissions::from_mode(0o700)).unwrap();
        let output = fixture.0.join("evidence");
        let mut command = static_check_command(&fixture, "--help");
        command
            .env(
                "EDGEZERO_STATIC_CHECK_ARGS",
                json!([
                    "--adapter",
                    adapter,
                    "--suite",
                    "outbound-progress",
                    "--output",
                    output,
                ])
                .to_string(),
            )
            .env("EDGEZERO_TEST_HELPER", std::env::current_exe().unwrap())
            .env("CARGO", &cargo)
            .env("RUSTC", executable("RUSTC", "rustc").unwrap())
            .env("WRANGLER_BIN", &cargo)
            .env("WORKER_BUILD_BIN", &cargo);
        let mut controller =
            Process::spawn(&mut command, &fixture.0.join("controller.log")).unwrap();
        fixture.ready();
        let cargo_id = fs::read_to_string(fixture.0.join("cargo-started"))
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let mut cleanup = GroupCleanup(Some(cargo_id));
        let port = fixture.descendant_port();
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
        signal_target(
            SignalTarget::Pid(controller.0.id()),
            Some(Signal::SIGTERM),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while controller.child_status().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let completed = controller.child_status().unwrap().is_some();
        let child = signal_target(
            SignalTarget::Pid(cargo_id),
            None,
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        let descendant_closed = loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::sleep(Duration::from_millis(10));
        };
        let outcome = if completed {
            controller.wait_exit_code(1).map(|_| ())
        } else {
            controller.finish()
        };
        cleanup.finish().unwrap();
        assert!(
            completed,
            "{adapter}: direct cancellation did not stop Cargo"
        );
        outcome.unwrap();
        assert_eq!(
            child,
            SignalOutcome::Absent,
            "{adapter}: Cargo was not reaped"
        );
        assert!(
            descendant_closed,
            "{adapter}: Cargo descendant retained its socket"
        );
    }

    #[test]
    fn progress_build_cancellation_reaps_cargo_and_descendants() {
        assert_progress_cargo_cancellation("axum");
    }

    #[test]
    fn progress_metadata_cancellation_reaps_cargo_and_descendants() {
        assert_progress_cargo_cancellation("cloudflare");
    }

    #[test]
    fn failed_process_fixture_preserves_diagnostics_until_explicit_cleanup() {
        let fixture = Fixture::new();
        let path = fixture.0.clone();
        fs::write(path.join("runtime.log"), b"failure evidence").unwrap();
        let outcome = std::panic::catch_unwind(move || {
            let _retained = fixture;
            panic!("controlled fixture failure");
        });
        assert!(outcome.is_err());
        assert_eq!(
            fs::read(path.join("runtime.log")).unwrap(),
            b"failure evidence"
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn cancellation_wins_when_a_child_exit_is_observed_after_the_signal() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("exit-nonzero");
        until(|| process.child_status().unwrap().is_some());
        let status = process.child_status().unwrap().unwrap();
        let cancelled = AtomicBool::new(true);
        let error = process.complete_command(status, 0, &cancelled).unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::Interrupted)
        );
        assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&process.0.id()));
    }

    #[test]
    fn command_completion_accepts_zero_and_cleans_exited_parents_descendants() {
        for mode in ["exit-zero", "orphan"] {
            let fixture = Fixture::new();
            let mut process = fixture.spawn(mode);
            fixture.ready();
            let status = process.wait_success().unwrap();
            assert!(status.success());
            process.finish().unwrap();
            assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&process.0.id()));
            if mode == "orphan" {
                let port = fixture.descendant_port();
                until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
            }
        }
    }

    #[test]
    fn command_completion_preserves_failure_status_and_logs() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("exit-nonzero");
        let error = process.wait_success().unwrap_err().to_string();
        assert!(
            error.contains("command exited") && error.contains("7"),
            "{error}"
        );
        assert!(error.contains("runtime.log"), "{error}");
        assert_eq!(process.child_status().unwrap().unwrap().code(), Some(7));
        assert!(process.finish().is_err());
        assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&process.0.id()));
    }

    #[test]
    fn command_cancellation_reaps_before_returning() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("descendant");
        fixture.ready();
        let port = fixture.descendant_port();
        let started = Instant::now();
        let error = process
            .wait_success_cancelled(&AtomicBool::new(true))
            .unwrap_err();
        assert!(error.to_string().contains("interrupted"), "{error}");
        assert!(process.0.try_wait().unwrap().is_some());
        assert!(!LIVE_GROUPS.lock().unwrap().contains_key(&process.0.id()));
        until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
        assert!(started.elapsed() < Duration::from_secs(6));
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
    fn outbound_wire_process_signals_distinguish_pid_from_group() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("descendant");
        fixture.ready();
        process.check_running().unwrap();
        let port = fixture.descendant_port();
        let descendant = fs::read_to_string(fixture.0.join("descendant-pid"))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            signal_target(
                SignalTarget::Pid(process.0.id()),
                Some(Signal::SIGTERM),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap(),
            SignalOutcome::Sent
        );
        let status = process
            .0
            .wait_until(Instant::now() + Duration::from_secs(2))
            .unwrap()
            .expect("owned parent did not stop");
        assert!(status.success());
        assert_eq!(
            signal_target(
                SignalTarget::Pid(process.0.id()),
                None,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap(),
            SignalOutcome::Absent
        );
        assert_eq!(
            signal_target(
                SignalTarget::Pid(descendant),
                None,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap(),
            SignalOutcome::Sent
        );
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
        process.wait_success().unwrap();
        until(|| std::net::TcpStream::connect(("127.0.0.1", port)).is_err());
    }

    #[test]
    fn outbound_wire_process_cleanup_needs_neither_path_nor_temp_diagnostics() {
        let fixture = Fixture::new();
        let mut command = fixture.command("cleanup-no-tools");
        command
            .env("TMPDIR", fixture.0.join("missing-temp"))
            .env("PATH", fixture.0.join("missing-commands"));
        let mut process = Process::spawn(&mut command, &fixture.0.join("runtime.log")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !fixture.0.join("cleanup-checked").is_file() {
            let health = process.check_running();
            assert!(
                health.is_ok(),
                "{health:?}: {}",
                fs::read_to_string(fixture.0.join("runtime.log")).unwrap()
            );
            assert!(Instant::now() < deadline, "cleanup fixture stalled");
            thread::sleep(Duration::from_millis(10));
        }
        process.finish().unwrap();
    }

    #[test]
    fn outbound_wire_process_signal_deadline_prevents_owned_child_termination() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("graceful");
        fixture.ready();
        let deadline = Instant::now();
        for target in [
            SignalTarget::Pid(process.0.id()),
            SignalTarget::Group(process.0.id()),
        ] {
            let error = signal_target(target, Some(Signal::SIGKILL), deadline).unwrap_err();
            assert!(error.to_string().contains("deadline expired"), "{error}");
            process.check_running().unwrap();
        }
        process.finish().unwrap();
    }

    #[test]
    fn outbound_wire_process_signal_rejects_zero_one_and_overflow_ids() {
        for id in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            for target in [SignalTarget::Pid(id), SignalTarget::Group(id)] {
                let error = signal_target(
                    target,
                    Some(Signal::SIGKILL),
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::InvalidInput
                );
            }
        }
    }

    #[test]
    fn outbound_wire_process_signal_only_esrch_means_absent() {
        assert_eq!(signal_outcome(Ok(())).unwrap(), SignalOutcome::Sent);
        assert_eq!(
            signal_outcome(Err(Errno::ESRCH)).unwrap(),
            SignalOutcome::Absent
        );
        for errno in [Errno::EPERM, Errno::EINVAL, Errno::EINTR] {
            let error = signal_outcome(Err(errno)).unwrap_err();
            assert_eq!(error.downcast_ref::<Errno>(), Some(&errno));
        }
    }

    #[test]
    fn outbound_wire_process_nested_group_cleanup_runs_before_signal_error_returns() {
        let fixture = Fixture::new();
        let mut process = fixture.spawn("descendant");
        fixture.ready();
        let port = fixture.descendant_port();
        let failure = (|| -> Result<()> {
            let _cleanup = GroupCleanup(Some(process.0.id()));
            signal_target(
                SignalTarget::Pid(process.0.id()),
                Some(Signal::SIGTERM),
                Instant::now(),
            )?;
            Ok(())
        })();
        assert!(
            failure
                .unwrap_err()
                .to_string()
                .contains("deadline expired")
        );
        let status = process
            .0
            .wait_until(Instant::now() + Duration::from_secs(2))
            .unwrap()
            .expect("red-path cleanup did not reap the owned child");
        assert_eq!(status.signal(), Some(9));
        until(|| TcpListener::bind(("127.0.0.1", port)).is_ok());
        assert!(
            process.finish().is_err(),
            "SIGKILL must not become successful completion"
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
                    .contains_key(&process.0.id())
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
    fn startup_responder_rejects_invalid_endpoint_publication() {
        let fixture = Fixture::new();
        let mut command = fixture.command("graceful");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let mut process = Process::spawn(&mut command, &fixture.0.join("runtime.log")).unwrap();
        fixture.ready();
        let endpoint = json!({"pid":process.0.id(), "nonce":nonce, "port":1234});
        let mut invalid = vec![json!(null), json!([]), json!("not an endpoint")];
        for (field, value) in [
            ("pid", json!(u64::from(process.0.id()) + 1)),
            ("nonce", json!("private-foreign-nonce")),
            ("port", json!(0)),
            ("port", json!(65_536)),
            ("port", json!(-1)),
            ("port", json!("1234")),
            ("extra", json!(true)),
        ] {
            let mut value_with_fault = endpoint.clone();
            value_with_fault[field] = value;
            invalid.push(value_with_fault);
        }
        let mut missing = endpoint.clone();
        missing.as_object_mut().unwrap().remove("nonce");
        invalid.push(missing);
        let mut bytes: Vec<_> = invalid
            .iter()
            .map(|value| serde_json::to_vec(value).unwrap())
            .collect();
        bytes.push(b"{malformed".to_vec());
        // Valid JSON with excess whitespace must still hit the byte limit.
        let mut oversized = serde_json::to_vec(&endpoint).unwrap();
        oversized.resize(1025, b' ');
        bytes.push(oversized);
        for bytes in bytes {
            fs::write(fixture.0.join("endpoint.json"), bytes).unwrap();
            let error = fixture
                .responder_port(
                    &mut process,
                    &nonce,
                    Instant::now() + Duration::from_secs(5),
                )
                .unwrap_err()
                .to_string();
            assert!(error.contains("fixture endpoint"), "{error}");
            assert!(!error.contains("private-foreign-nonce"), "{error}");
            assert!(!fixture.0.join("ordinal").exists());
        }
        let mut exact = serde_json::to_vec(&endpoint).unwrap();
        exact.resize(1024, b' ');
        fs::write(fixture.0.join("endpoint.json"), exact).unwrap();
        assert_eq!(
            fixture
                .responder_port(
                    &mut process,
                    &nonce,
                    Instant::now() + Duration::from_secs(5)
                )
                .unwrap(),
            1234
        );
        process.finish().unwrap();
    }

    #[test]
    fn startup_responder_publication_preserves_deadline_and_child_liveness() {
        let fixture = Fixture::new();
        let mut command = fixture.command("graceful");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let mut process = Process::spawn(&mut command, &fixture.0.join("runtime.log")).unwrap();
        fixture.ready();
        let error = fixture
            .responder_port(
                &mut process,
                &nonce,
                Instant::now() + Duration::from_millis(20),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("endpoint deadline expired"),
            "{error}"
        );
        let endpoint = json!({"pid":process.0.id(), "nonce":nonce, "port":1234});
        fs::write(
            fixture.0.join("endpoint.json"),
            serde_json::to_vec(&endpoint).unwrap(),
        )
        .unwrap();
        let error = fixture
            .responder_port(&mut process, &nonce, Instant::now())
            .unwrap_err();
        assert!(
            error.to_string().contains("endpoint deadline expired"),
            "{error}"
        );
        process.finish().unwrap();

        let exited_fixture = Fixture::new();
        let mut command = exited_fixture.command("exit-zero");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let mut process =
            Process::spawn(&mut command, &exited_fixture.0.join("runtime.log")).unwrap();
        until(|| process.child_status().unwrap().is_some());
        let endpoint = json!({"pid":process.0.id(), "nonce":nonce, "port":1234});
        fs::write(
            exited_fixture.0.join("endpoint.json"),
            serde_json::to_vec(&endpoint).unwrap(),
        )
        .unwrap();
        let error = exited_fixture
            .responder_port(
                &mut process,
                &nonce,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("runtime exited before shutdown"),
            "{error}"
        );
        process.wait_success().unwrap();
    }

    #[test]
    fn startup_responder_owns_endpoint_before_the_first_prescribed_request() {
        let fixture = Fixture::new();
        let foreign = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let foreign_port = foreign.local_addr().unwrap().port();
        let log = fixture.0.join("runtime.log");
        let mut command = fixture.command("startup-responder");
        startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        command
            .env("EDGEZERO_PROCESS_TEST_PORT", foreign_port.to_string())
            .env("EDGEZERO_PROCESS_TEST_IDENTITY", "foreign");
        let (mut process, port) = fixture.start_responder(&mut command).unwrap();
        assert_ne!(port, foreign_port);
        assert!(TcpListener::bind(("127.0.0.1", port)).is_err());
        assert!(!fixture.0.join("ordinal").exists());
        assert!(startup::check(port).unwrap().is_some());
        let error = net::request(port, "/first", "request-token", None, None).unwrap_err();
        assert!(error.to_string().contains("mismatched"), "{error}");
        assert!(net::request(port, "/second", "request-token", None, None).is_err());
        assert_eq!(fs::read_to_string(fixture.0.join("ordinal")).unwrap(), "1");
        foreign.set_nonblocking(true).unwrap();
        assert_eq!(
            foreign.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(process.finish().is_err());
        assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        assert!(
            std::net::TcpStream::connect_timeout(
                &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                Duration::from_millis(200),
            )
            .is_err()
        );
    }

    #[test]
    fn startup_attests_the_first_prescribed_response_without_an_extra_http_request() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let mut command = fixture.command("startup-responder");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let identity = format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID);
        command.env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity);
        let (mut process, port) = fixture.start_responder(&mut command).unwrap();
        let response = net::request(port, "/first", "request-token", None, None).unwrap();
        assert_eq!(guest(&response).unwrap()["ordinal"], 1);
        assert!(
            !fs::read_to_string(fixture.0.join("request"))
                .unwrap()
                .contains(&nonce)
        );
        process.finish().unwrap();
        assert_eq!(startup::settle(&log).unwrap()["status"], "pass");
    }

    #[test]
    fn startup_rejects_mismatched_nonce_build_and_mode_without_retrying() {
        for index in 1..=5 {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let mut command = fixture.command("startup-responder");
            let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
            let identity = format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID);
            let mut parts: Vec<_> = identity.split(';').collect();
            let wrong = match index {
                1..=3 => {
                    parts[index] = "foreign";
                    parts.join(";")
                }
                4 => format!("{identity}\r\n{}: {identity}", startup::HEADER),
                _ => String::new(),
            };
            command.env("EDGEZERO_PROCESS_TEST_IDENTITY", wrong);
            let (mut process, port) = fixture.start_responder(&mut command).unwrap();
            let error = net::request(port, "/first", "request-token", None, None).unwrap_err();
            let diagnostic = if index == 5 {
                "missing launch identity"
            } else {
                "mismatched"
            };
            assert!(error.to_string().contains(diagnostic), "{error}");
            assert!(net::request(port, "/first", "request-token", None, None).is_err());
            assert_eq!(fs::read_to_string(fixture.0.join("ordinal")).unwrap(), "1");
            assert!(process.finish().is_err());
            assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        }
    }

    #[test]
    fn startup_fault_observations_have_no_credit_until_the_prescribed_recovery_response() {
        for correct in [false, true] {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let mut command = fixture.command("startup-responder");
            let nonce = startup::command(&mut command, "fixture-fastly-faults", TEST_BUILD_ID);
            let identity = format!("v1;{nonce};{};fixture-fastly-faults", TEST_BUILD_ID);
            command
                .env(
                    "EDGEZERO_PROCESS_TEST_IDENTITY",
                    if correct { &identity } else { "foreign" },
                )
                .env("EDGEZERO_PROCESS_TEST_SKIP_IDENTITY", "1");
            let (mut process, port) = fixture.start_responder(&mut command).unwrap();
            let first = net::request(port, "/constructor-panic", "fault", None, None).unwrap();
            assert_eq!(first["startup_identity"], "pending/no-credit");
            let mut records = Records {
                path: fixture.0.join("events.jsonl"),
                values: Vec::new(),
            };
            records
                .push(json!({"status":"pass","assertion":"earlier-parser-observation"}))
                .unwrap();
            assert_eq!(records.values[0]["status"], "pending");
            let recovered = net::request(port, "/probe/recovered", "recovery", None, None);
            assert_eq!(
                fs::read(fixture.0.join("request-body")).expect("helper captured the request body"),
                b"recovery"
            );
            assert_eq!(recovered.is_ok(), correct, "{recovered:?}");
            assert_eq!(process.finish().is_ok(), correct);
            assert_eq!(
                startup::settle(&log).unwrap()["status"],
                if correct { "pass" } else { "unverified" }
            );
            assert_eq!(records.values[0]["status"], "pending");
        }
    }

    #[test]
    fn startup_review_observe_rejects_present_invalid_identity_after_attestation() {
        for index in 1..=4 {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let mut command = fixture.command("startup-responder");
            let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
            let identity = format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID);
            command.env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity);
            let (mut process, port) = fixture.start_responder(&mut command).unwrap();
            net::request(port, "/first", "first", None, None).unwrap();
            let mut headers = vec![json!([startup::HEADER, &identity])];
            if index == 4 {
                headers.push(json!([startup::HEADER, &identity]));
            } else {
                let mut parts: Vec<_> = identity.split(';').collect();
                parts[index] = "foreign";
                headers[0][1] = json!(parts.join(";"));
            }
            assert!(
                startup::observe(port, "/later", &headers).is_err(),
                "later identity was accepted: {headers:?}"
            );
            assert!(
                startup::observe(port, "/later", &[json!([startup::HEADER, &identity])]).is_err()
            );
            assert!(process.finish().is_err());
            assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        }
    }

    #[test]
    fn startup_review_wire_rejects_later_identity_and_cannot_recover() {
        for duplicate in [false, true] {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let mut command = fixture.command("startup-responder");
            let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
            let identity = format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID);
            let later = if duplicate {
                format!("{identity}\r\n{}: {identity}", startup::HEADER)
            } else {
                "foreign".to_owned()
            };
            command
                .env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity)
                .env("EDGEZERO_PROCESS_TEST_LATER_IDENTITY", later);
            let (mut process, port) = fixture.start_responder(&mut command).unwrap();
            net::request(port, "/first", "first", None, None).unwrap();
            assert!(net::request(port, "/second", "second", None, None).is_err());
            assert!(net::request(port, "/bindings", "later", None, None).is_err());
            assert_eq!(fs::read_to_string(fixture.0.join("ordinal")).unwrap(), "2");
            assert!(process.finish().is_err());
            assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        }
    }

    #[test]
    fn startup_review_recovery_cannot_defer_its_identity_to_bindings() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let mut command = fixture.command("startup-responder");
        let nonce = startup::command(&mut command, "fixture-fastly-faults", TEST_BUILD_ID);
        let identity = format!("v1;{nonce};{};fixture-fastly-faults", TEST_BUILD_ID);
        command
            .env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity)
            .env("EDGEZERO_PROCESS_TEST_SKIP_IDENTITY", "2");
        let (mut process, port) = fixture.start_responder(&mut command).unwrap();
        let fault = net::request(port, "/constructor-panic", "fault", None, None).unwrap();
        assert_eq!(fault["startup_identity"], "pending/no-credit");
        assert!(
            net::request(port, "/probe/recovered", "recovery", None, None).is_err(),
            "missing recovery identity was deferred"
        );
        assert!(net::request(port, "/bindings", "later", None, None).is_err());
        assert_eq!(fs::read_to_string(fixture.0.join("ordinal")).unwrap(), "2");
        assert!(process.finish().is_err());
        assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
    }

    #[test]
    fn startup_review_fault_mode_does_not_exempt_unprescribed_paths() {
        for path in [
            "/bindings",
            "/probe/malformed",
            "/constructor-panic?extra",
            "/constructor-panic/",
        ] {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let mut command = fixture.command("startup-responder");
            let nonce = startup::command(&mut command, "fixture-fastly-faults", TEST_BUILD_ID);
            let identity = format!("v1;{nonce};{};fixture-fastly-faults", TEST_BUILD_ID);
            command
                .env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity)
                .env("EDGEZERO_PROCESS_TEST_SKIP_IDENTITY", "1");
            let (mut process, port) = fixture.start_responder(&mut command).unwrap();
            assert!(
                net::request(port, path, "request", None, None).is_err(),
                "unprescribed path was exempted: {path}"
            );
            assert!(process.finish().is_err());
            assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        }
    }

    #[test]
    fn startup_review_fault_identity_cannot_credit_a_case_before_recovery() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let mut command = fixture.command("startup-responder");
        let nonce = startup::command(&mut command, "fixture-fastly-faults", TEST_BUILD_ID);
        let identity = format!("v1;{nonce};{};fixture-fastly-faults", TEST_BUILD_ID);
        command.env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity);
        let (mut process, port) = fixture.start_responder(&mut command).unwrap();
        let fault = net::request(port, "/inbound-error", "fault", None, None).unwrap();
        assert_eq!(fault["startup_identity"], "pending/no-credit");
        assert!(startup::check(port).unwrap().is_some());
        assert!(process.finish().is_err());
        assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
    }

    #[test]
    fn startup_review_missing_later_identity_is_explicitly_unattested() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let mut command = fixture.command("startup-responder");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let identity = format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID);
        command
            .env("EDGEZERO_PROCESS_TEST_IDENTITY", &identity)
            .env("EDGEZERO_PROCESS_TEST_LATER_IDENTITY", "");
        let (mut process, port) = fixture.start_responder(&mut command).unwrap();
        net::request(port, "/first", "first", None, None).unwrap();
        let later = net::request(port, "/canonical-error", "later", None, None).unwrap();
        assert_eq!(later["response_identity"], "unattested");
        process.finish().unwrap();
        assert_eq!(startup::settle(&log).unwrap()["status"], "pass");
    }

    #[test]
    fn startup_absolute_deadline_expires_even_with_an_owned_live_child() {
        let fixture = Fixture::new();
        let log = fixture.0.join("runtime.log");
        let port = net::port().unwrap();
        let mut command = fixture.command("graceful");
        let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
        let mut process = Process::spawn(&mut command, &log).unwrap();
        process.1.launch = Some(startup::register(
            &command,
            process.0.clone(),
            &log,
            port,
            Instant::now(),
        ));
        fixture.ready();
        let headers = vec![json!([
            startup::HEADER,
            format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID)
        ])];
        let error = startup::observe(port, "/first", &headers).unwrap_err();
        assert!(error.to_string().contains("absolute startup deadline"));
        assert!(process.finish().is_err());
        assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
    }

    #[test]
    fn startup_matching_identity_cannot_attest_a_child_that_already_exited() {
        for mode in ["exit-zero", "exit-nonzero"] {
            let fixture = Fixture::new();
            let log = fixture.0.join("runtime.log");
            let port = net::port().unwrap();
            let mut command = fixture.command(mode);
            let nonce = startup::command(&mut command, "fixture-test", TEST_BUILD_ID);
            let mut process = Process::spawn(&mut command, &log).unwrap();
            process.1.launch = Some(startup::register(
                &command,
                process.0.clone(),
                &log,
                port,
                Instant::now() + Duration::from_secs(5),
            ));
            fixture.ready();
            until(|| process.0.try_wait().unwrap().is_some());
            let headers = vec![json!([
                startup::HEADER,
                format!("v1;{nonce};{};fixture-test", TEST_BUILD_ID)
            ])];
            assert!(
                startup::observe(port, "/first", &headers)
                    .unwrap_err()
                    .to_string()
                    .contains("owned child exited")
            );
            assert!(process.finish().is_err());
            assert_eq!(startup::settle(&log).unwrap()["status"], "unverified");
        }
    }

    #[test]
    fn startup_foreign_listener_with_a_live_child_cannot_complete_successfully() {
        let fixture = Fixture::new();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let outcome = Process::start(
            &mut fixture.command("graceful"),
            &fixture.0.join("runtime.log"),
            port,
        );
        if let Ok(mut process) = outcome {
            fixture.ready();
            process.check_running().unwrap();
            let completion = process.finish();
            assert!(
                completion.is_err(),
                "a foreign TCP listener plus a live owned child is not responder identity"
            );
        }
        assert!(
            listener.local_addr().is_ok(),
            "startup cleanup must not close the foreign listener"
        );
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
