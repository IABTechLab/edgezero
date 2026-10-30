use crate::{Result, require};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

pub const HEADER: &str = "x-edgezero-fixture-launch";
pub const NONCE: &str = "EDGEZERO_FIXTURE_LAUNCH_NONCE";
pub const MODE: &str = "EDGEZERO_FIXTURE_LAUNCH_MODE";
const EXPECTED: &str = "EDGEZERO_FIXTURE_EXPECTED_ID";
const FAULT_RECOVERY: &str = "/probe/recovered";
const UNATTESTED_FAULT_PATHS: &[&str] = &[
    "/constructor-panic",
    "/collect-error",
    "/inbound-error",
    "/terminal-store-error",
    "/recoverable-store-error",
];
static LAUNCHES: Mutex<Vec<Arc<Mutex<Launch>>>> = Mutex::new(Vec::new());
thread_local! {
    static CURRENT: RefCell<Option<Arc<Mutex<Launch>>>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct ChildHandle(Arc<Mutex<Child>>);
impl ChildHandle {
    pub fn new(child: Child) -> Self {
        Self(Arc::new(Mutex::new(child)))
    }
    pub fn id(&self) -> u32 {
        self.0.lock().unwrap().id()
    }
    pub fn try_wait(&self) -> std::io::Result<Option<ExitStatus>> {
        self.0.lock().unwrap().try_wait()
    }
    pub fn kill(&self) -> std::io::Result<()> {
        self.0.lock().unwrap().kill()
    }
    pub fn wait_until(&self, deadline: Instant) -> Result<Option<ExitStatus>> {
        crate::wait_for_exit(&mut self.0.lock().unwrap(), deadline)
    }
}

pub struct Launch {
    pub port: u16,
    log: PathBuf,
    child: ChildHandle,
    expected: Option<String>,
    deadline: Instant,
    deferred: bool,
    verified: bool,
    closed: bool,
    failure: Option<String>,
}

pub fn build_id() -> &'static str {
    static BUILD: OnceLock<String> = OnceLock::new();
    BUILD.get_or_init(crate::token)
}

pub fn command(command: &mut Command, mode: &str) -> String {
    let nonce = crate::token();
    command
        .env(NONCE, &nonce)
        .env(MODE, mode)
        .env(EXPECTED, format!("v1;{nonce};{};{mode}", build_id()));
    nonce
}

pub fn require_configured(command: &Command) -> Result<()> {
    require(
        command
            .get_envs()
            .any(|(key, value)| key == EXPECTED && value.is_some()),
        "startup identity unverified/open: launch metadata is absent",
    )
}

pub fn register(
    command: &Command,
    child: ChildHandle,
    log: &Path,
    port: u16,
    deadline: Instant,
) -> Arc<Mutex<Launch>> {
    let expected = command
        .get_envs()
        .find(|(key, _)| *key == EXPECTED)
        .and_then(|(_, value)| value)
        .map(|value| value.to_string_lossy().into_owned());
    let deferred = expected
        .as_ref()
        .is_some_and(|id| id.ends_with(";fixture-fastly-faults"));
    let launch = Arc::new(Mutex::new(Launch {
        port,
        log: log.to_owned(),
        child,
        expected,
        deadline,
        deferred,
        verified: false,
        closed: false,
        failure: None,
    }));
    LAUNCHES.lock().unwrap().push(Arc::clone(&launch));
    CURRENT.with(|current| *current.borrow_mut() = Some(Arc::clone(&launch)));
    launch
}

fn find(port: u16) -> Option<Arc<Mutex<Launch>>> {
    LAUNCHES
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|launch| {
            let launch = launch.lock().unwrap();
            launch.port == port && !launch.closed
        })
        .cloned()
}

impl Launch {
    fn fail(&mut self, reason: impl Into<String>) -> Result<()> {
        let reason = self.failure.get_or_insert_with(|| reason.into());
        Err(format!("startup identity unverified/open: {reason}").into())
    }
    fn check(&mut self) -> Result<()> {
        if let Some(reason) = &self.failure {
            return Err(format!("startup identity unverified/open: {reason}").into());
        }
        if self.closed {
            return self.fail("owned launch is closed");
        }
        if !self.verified && Instant::now() >= self.deadline {
            return self.fail("absolute startup deadline expired");
        }
        if self.child.try_wait()?.is_some() {
            return self.fail("owned child exited before responder attestation/teardown");
        }
        Ok(())
    }
    pub fn close(&mut self) -> Result<()> {
        let result = self.check().and_then(|()| {
            if self.verified {
                Ok(())
            } else {
                self.fail("no identity-bearing prescribed response; this mode remains open")
            }
        });
        self.closed = true;
        result
    }
    pub fn teardown_failed(&mut self, reason: &str) {
        self.failure.get_or_insert_with(|| reason.to_owned());
    }
    fn evidence(&self) -> Value {
        json!({"event":"startup-identity", "status":if self.verified && self.failure.is_none(){"pass"}else{"unverified"},
            "port":self.port,"owned_child":self.child.id(),"launch_identity":self.expected,
            "reason":self.failure,"qualification":false,"atomic_port_reservation":false})
    }
}

pub fn check(port: u16) -> Result<Option<Instant>> {
    let Some(launch) = find(port) else {
        return Ok(None);
    };
    let mut launch = launch.lock().unwrap();
    launch.check()?;
    Ok((!launch.verified).then_some(launch.deadline))
}

fn verify_head(launch: &mut Launch, path: &str, headers: &[Value]) -> Result<bool> {
    launch.check()?;
    let identities: Vec<_> = headers
        .iter()
        .filter(|pair| {
            pair[0]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case(HEADER))
        })
        .collect();
    if !identities.is_empty()
        && (launch.expected.is_none()
            || identities.len() != 1
            || identities[0][1].as_str() != launch.expected.as_deref())
    {
        launch.fail("missing, duplicate, or mismatched launch/build/mode identity")?;
    }
    let unqualified_fault = launch.deferred && UNATTESTED_FAULT_PATHS.contains(&path);
    if launch.deferred && !launch.verified && !unqualified_fault && path != FAULT_RECOVERY {
        launch.fail("unprescribed response before required /probe/recovered attestation")?;
    }
    if identities.is_empty() && !launch.verified && !unqualified_fault {
        launch.fail("missing launch identity on required prescribed response")?;
    }
    Ok(!identities.is_empty())
}

pub fn validate_head(port: u16, path: &str, headers: &[Value]) -> Result<()> {
    let Some(launch) = find(port) else {
        return Ok(());
    };
    let mut launch = launch.lock().unwrap();
    verify_head(&mut launch, path, headers)?;
    Ok(())
}

pub fn observe(port: u16, path: &str, headers: &[Value]) -> Result<()> {
    let Some(launch) = find(port) else {
        return Ok(());
    };
    let mut launch = launch.lock().unwrap();
    if !verify_head(&mut launch, path, headers)? {
        return Ok(());
    }
    launch.check()?;
    if !launch.deferred || path == FAULT_RECOVERY {
        launch.verified = true;
    }
    Ok(())
}

pub fn annotate_response(port: u16, value: &mut Value) {
    if let Some(launch) = find(port) {
        let launch = launch.lock().unwrap();
        let present = value["headers"].as_array().is_some_and(|headers| {
            headers.iter().any(|pair| {
                pair[0]
                    .as_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case(HEADER))
            })
        });
        value["response_identity"] = json!(if present {
            "matched-launch"
        } else {
            "unattested"
        });
        if !launch.verified || launch.failure.is_some() {
            value["startup_identity"] = json!("pending/no-credit");
            value["qualification"] = json!(false);
        }
    }
}

pub fn reject(port: u16, reason: impl Into<String>) {
    if let Some(launch) = find(port) {
        let mut launch = launch.lock().unwrap();
        if !launch.verified {
            let _ = launch.fail(reason);
        }
    }
}

pub fn deferred(port: u16, path: &str) -> bool {
    UNATTESTED_FAULT_PATHS.contains(&path)
        && find(port).is_some_and(|launch| launch.lock().unwrap().deferred)
}

pub fn annotate(value: &mut Value) {
    CURRENT.with(|current| {
        if let Some(launch) = current.borrow().as_ref() {
            let launch = launch.lock().unwrap();
            if !launch.verified || launch.failure.is_some() {
                value["startup_identity"] = json!("pending/no-credit");
                value["qualification"] = json!(false);
                if value["status"] == "pass" {
                    value["status"] = json!("pending");
                }
            }
        }
    });
}

pub fn settle(log: &Path) -> Option<Value> {
    let mut launches = LAUNCHES.lock().unwrap();
    let index = launches
        .iter()
        .position(|launch| launch.lock().unwrap().log == log)?;
    let launch = launches.remove(index);
    CURRENT.with(|current| {
        if current
            .borrow()
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &launch))
        {
            current.borrow_mut().take();
        }
    });
    let evidence = launch.lock().unwrap().evidence();
    Some(evidence)
}

pub fn remaining(port: u16, budget: Duration) -> Result<Duration> {
    let deadline = check(port)?.unwrap_or_else(|| Instant::now() + budget);
    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .min(budget);
    require(!remaining.is_zero(), "absolute startup deadline expired")?;
    Ok(remaining)
}
