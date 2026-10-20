use fastly::handle::BodyHandle;
use fastly::{Backend, Request, Response};
use serde_json::{Value, json};
use std::io::Read;
use std::time::{Duration, Instant};

// Only this opt-in host-primitive experiment calls the raw readiness ABI.
#[allow(
    unsafe_code,
    reason = "fixture probes the pinned Fastly readiness ABI with live borrowed handles"
)]
fn select(handles: &[u32], timeout_ms: u32) -> (String, Option<u32>) {
    let mut selected = u32::MAX;
    // SAFETY: the input slice and output remain live for the synchronous call;
    // handles stay owned by the caller. A zero/unbounded timeout is never used.
    let status = unsafe {
        fastly_sys::fastly_async_io::select(
            handles.as_ptr(),
            handles.len(),
            timeout_ms.max(1),
            &mut selected,
        )
    };
    (
        format!("{status:?}"),
        (selected != u32::MAX).then_some(selected),
    )
}

#[allow(
    unsafe_code,
    reason = "borrow body identity without transferring or closing its ownership"
)]
fn body_id(body: &BodyHandle) -> u32 {
    // SAFETY: only readiness observes this borrowed identity; body retains ownership.
    unsafe { body.as_u32() }
}

fn main() -> Result<(), fastly::Error> {
    fastly::init();
    let request = Request::from_client();
    let target = request
        .get_header_str("x-fixture-backend")
        .ok_or_else(|| std::io::Error::other("missing loopback probe target"))?;
    let mode = request.get_path().trim_start_matches('/');
    if !["direct", "selected"].contains(&mode) || !target.starts_with("http://127.0.0.1:") {
        return Err(std::io::Error::other("unsupported probe mode or target").into());
    }
    let authority = target
        .trim_start_matches("http://")
        .split('/')
        .next()
        .ok_or_else(|| std::io::Error::other("missing loopback authority"))?;
    if authority
        .strip_prefix("127.0.0.1:")
        .and_then(|p| p.parse::<u16>().ok())
        .is_none_or(|p| p == 0)
        || !target.ends_with("/readiness/truncated")
    {
        return Err(
            std::io::Error::other("probe target must be the controlled loopback origin").into(),
        );
    }
    let backend = Backend::builder("readiness-origin", authority)
        .connect_timeout(Duration::from_secs(2))
        .first_byte_timeout(Duration::from_secs(2))
        .between_bytes_timeout(Duration::from_secs(2))
        .finish()?;
    let (head, body) = Request::get(target).into_handles();
    let start = Instant::now();
    let pending = head.send_async(body.unwrap_or_else(BodyHandle::new), backend.name())?;
    let before = start.elapsed().as_micros();
    let (status, selected) = select(&[pending.as_u32()], 500);
    let mut samples = vec![json!({"operation":"head-select", "start_us":before,
        "end_us":start.elapsed().as_micros(), "timeout_ms":500, "status":status, "selected":selected})];
    if selected.is_none() {
        return Err(std::io::Error::other("probe head did not become ready").into());
    }
    let (_head, mut body) = pending.wait()?;
    for _ in 0..32 {
        let remaining = Duration::from_millis(500).saturating_sub(start.elapsed());
        if remaining.is_zero() {
            break;
        }
        if mode == "selected" {
            let timeout_ms = u32::try_from(remaining.as_millis()).unwrap_or(500).max(1);
            let before = start.elapsed().as_micros();
            let (status, selected) = select(&[body_id(&body)], timeout_ms);
            samples.push(json!({"operation":"body-select", "start_us":before,
                "end_us":start.elapsed().as_micros(), "timeout_ms":timeout_ms, "status":status, "selected":selected}));
            if selected.is_none() {
                break;
            }
        }
        let before = start.elapsed().as_micros();
        println!(
            "{}",
            json!({"event":"read-enter", "mode":mode, "start_us":before})
        );
        let outcome = body.read(&mut [0; 1]);
        let mut sample =
            json!({"operation":"read", "start_us":before, "end_us":start.elapsed().as_micros()});
        let terminal = match outcome {
            Ok(bytes) => {
                sample["bytes"] = json!(bytes);
                bytes == 0
            }
            Err(error) => {
                sample["error"] = json!(format!("{:?}", error.kind()));
                true
            }
        };
        samples.push(sample);
        if terminal {
            break;
        }
    }
    let probe: Value = json!({"case":"truncated", "mode":mode,
        "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"),
        "source_state":"working-tree-characterization", "read_limit":1,
        "deadline_us":500_000, "samples":samples, "qualification":false});
    println!("{probe}");
    Response::from_body(serde_json::to_string(&probe)?).send_to_client();
    Ok(())
}
