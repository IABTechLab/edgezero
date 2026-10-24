use fastly::handle::{BodyHandle, PendingRequestHandle, PollHandleResult, ResponseHandle};
use fastly::{Backend, Request, Response};
use serde_json::{Value, json};
use std::io::Read;
use std::time::{Duration, Instant};

const DEADLINE_US: u64 = 500_000;

// Only this opt-in host-primitive experiment calls the raw readiness ABI.
#[allow(
    unsafe_code,
    reason = "probe the pinned ABI with live borrowed handles"
)]
fn select(handles: &[u32], timeout_ms: u32) -> (String, Option<usize>, bool) {
    let mut selected = u32::MAX;
    // SAFETY: slices remain live, and the caller retains ownership of every handle.
    let status = unsafe {
        fastly_sys::fastly_async_io::select(
            handles.as_ptr(),
            handles.len(),
            timeout_ms,
            &mut selected,
        )
    };
    let index = (selected != u32::MAX).then_some(selected as usize);
    let valid = status.is_ok() && index.is_none_or(|index| index < handles.len());
    (format!("{status:?}"), index, valid)
}

#[allow(
    unsafe_code,
    reason = "borrow identity without transferring body ownership"
)]
fn body_id(body: &BodyHandle) -> u32 {
    // SAFETY: readiness observes identity only; the body remains owned by its slot.
    unsafe { body.as_u32() }
}

enum Handle {
    Head(PendingRequestHandle),
    Body {
        _head: ResponseHandle,
        body: BodyHandle,
    },
}

struct Slot {
    handle: Option<Handle>,
    phase: &'static str,
    result: Option<Value>,
    bytes: usize,
    checksum: u64,
}

struct Exchange {
    identity: Value,
    start: Instant,
    selected: bool,
    read_limit: usize,
    siblings: bool,
    samples: Vec<Value>,
    slots: Vec<Slot>,
}

impl Exchange {
    fn now(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    fn record(&mut self, sample: Value) -> Result<(), fastly::Error> {
        if self.samples.len() >= 512 {
            return Err(std::io::Error::other("readiness trace capacity exhausted").into());
        }
        println!(
            "{}",
            json!({"event":"readiness-sample", "identity":self.identity, "sample":sample})
        );
        self.samples.push(sample);
        Ok(())
    }

    fn finish(
        &mut self,
        slot: usize,
        outcome: &str,
        error: Option<String>,
    ) -> Result<(), fastly::Error> {
        let start = self.now();
        let state = &mut self.slots[slot];
        // Close guest-owned resources before recording the terminal boundary.
        state.handle = None;
        let phase = state.phase;
        let end = self.now();
        let mut sample = json!({"operation":"finish", "slot":slot, "start_us":start,
            "end_us":end, "outcome":outcome, "phase":phase});
        let mut result = json!({"outcome":outcome, "phase":phase, "at_us":end,
            "bytes":self.slots[slot].bytes,
            "checksum":format!("{:016x}", self.slots[slot].checksum)});
        if let Some(error) = error {
            sample["error"] = json!(error);
            result["error"] = json!(error);
        }
        self.slots[slot].result = Some(result);
        self.record(sample)
    }

    // A pull performs at most one raw body read per fresh matching readiness result.
    fn next_chunk(&mut self) -> Result<Option<(usize, Vec<u8>)>, fastly::Error> {
        loop {
            if self.slots.iter().all(|slot| slot.result.is_some()) {
                return Ok(None);
            }
            if self.now() >= DEADLINE_US {
                for slot in 0..self.slots.len() {
                    if self.slots[slot].result.is_none() {
                        self.finish(slot, "timeout", None)?;
                    }
                }
                continue;
            }
            let heads_only = self.siblings
                && self
                    .slots
                    .iter()
                    .any(|slot| matches!(slot.handle, Some(Handle::Head(_))));
            let live: Vec<_> = self
                .slots
                .iter()
                .enumerate()
                .filter_map(|(index, slot)| match &slot.handle {
                    Some(Handle::Head(handle)) => Some((index, "head", handle.as_u32())),
                    Some(Handle::Body { body, .. }) if !heads_only => {
                        Some((index, "body", body_id(body)))
                    }
                    _ => None,
                })
                .collect();
            let slot = if self.selected || live.iter().any(|(_, phase, _)| *phase == "head") {
                let handles: Vec<_> = live.iter().map(|(_, _, id)| *id).collect();
                let start = self.now();
                if start >= DEADLINE_US {
                    continue;
                }
                let timeout_ms = ((DEADLINE_US - start) / 1000).max(1) as u32;
                let (status, index, valid) = select(&handles, timeout_ms);
                let end = self.now();
                self.record(json!({"operation":"select", "start_us":start, "end_us":end,
                    "slots":live.iter().map(|(slot, _, _)| *slot).collect::<Vec<_>>(),
                    "phases":live.iter().map(|(_, phase, _)| *phase).collect::<Vec<_>>(),
                    "timeout_ms":timeout_ms, "status":status, "selected":index}))?;
                if !valid {
                    return Err(std::io::Error::other("invalid readiness status or index").into());
                }
                if end >= DEADLINE_US {
                    continue;
                }
                let Some(index) = index else {
                    continue;
                };
                live[index].0
            } else {
                live[0].0
            };
            let before = self.now();
            if before >= DEADLINE_US {
                continue;
            }
            if matches!(self.slots[slot].handle, Some(Handle::Head(_))) {
                let Some(Handle::Head(pending)) = self.slots[slot].handle.take() else {
                    return Err(std::io::Error::other("missing selected head").into());
                };
                let outcome = pending.poll();
                let end = self.now();
                match outcome {
                    PollHandleResult::Pending(pending) => {
                        self.slots[slot].handle = Some(Handle::Head(pending));
                        self.record(
                            json!({"operation":"head-poll", "slot":slot, "start_us":before,
                            "end_us":end, "outcome":"pending"}),
                        )?;
                        return Err(std::io::Error::other("selected head remained pending").into());
                    }
                    PollHandleResult::Done(Ok((head, body))) => {
                        self.record(
                            json!({"operation":"head-poll", "slot":slot, "start_us":before,
                            "end_us":end, "outcome":"ready"}),
                        )?;
                        if end < DEADLINE_US {
                            self.slots[slot].phase = "body";
                            self.slots[slot].handle = Some(Handle::Body { _head: head, body });
                        } else {
                            drop((head, body));
                        }
                    }
                    PollHandleResult::Done(Err(error)) => {
                        let error = format!("{error:?}");
                        self.record(
                            json!({"operation":"head-poll", "slot":slot, "start_us":before,
                            "end_us":end, "outcome":"error", "error":error}),
                        )?;
                        if end < DEADLINE_US {
                            self.finish(slot, "error", Some(error))?;
                        }
                    }
                }
                continue;
            }
            let mut chunk = vec![0; self.read_limit];
            println!(
                "{}",
                json!({"event":"read-enter", "identity":self.identity, "slot":slot, "start_us":before})
            );
            let before = self.now();
            if before >= DEADLINE_US {
                continue;
            }
            let Some(Handle::Body { body, .. }) = &mut self.slots[slot].handle else {
                return Err(std::io::Error::other("missing selected body").into());
            };
            let outcome = body.read(&mut chunk);
            let end = self.now();
            let mut sample = json!({"operation":"read", "slot":slot, "start_us":before,
                "end_us":end, "capacity":self.read_limit});
            let (bytes, error) = match outcome {
                Ok(bytes) => {
                    sample["bytes"] = json!(bytes);
                    (bytes, None)
                }
                Err(error) => {
                    let error = format!("{:?}", error.kind());
                    sample["error"] = json!(error);
                    (0, Some(error))
                }
            };
            self.record(sample)?;
            if end >= DEADLINE_US {
                continue;
            }
            if let Some(error) = error {
                self.finish(slot, "error", Some(error))?;
            } else if bytes == 0 {
                self.finish(slot, "eof", None)?;
            } else {
                chunk.truncate(bytes);
                let state = &mut self.slots[slot];
                state.bytes += bytes;
                if state.bytes > 65_536 {
                    return Err(std::io::Error::other("diagnostic byte cap exceeded").into());
                }
                for byte in &chunk {
                    state.checksum =
                        (state.checksum ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
                }
                return Ok(Some((slot, chunk)));
            }
        }
    }
}

// Ownership crosses the pull boundary before any body read, without resetting time.
struct PullBody(Exchange);
impl PullBody {
    fn new(mut exchange: Exchange) -> Result<Self, fastly::Error> {
        let now = exchange.now();
        exchange.record(json!({"operation":"handoff", "start_us":now, "end_us":now,
            "body_reads":0, "deadline_us":DEADLINE_US}))?;
        Ok(Self(exchange))
    }
    fn next(&mut self) -> Result<Option<(usize, Vec<u8>)>, fastly::Error> {
        self.0.next_chunk()
    }
}

fn main() -> Result<(), fastly::Error> {
    fastly::init();
    let request = Request::from_client();
    let segments: Vec<_> = request
        .get_path()
        .trim_start_matches('/')
        .split('/')
        .collect();
    let [case, mode, consumption, limit] = segments.as_slice() else {
        return Err(std::io::Error::other("invalid readiness route").into());
    };
    if ![
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
    ]
    .contains(case)
        || !["buffered", "streamed"].contains(consumption)
        || !["selected", "direct"].contains(mode)
        || (*mode == "direct"
            && (*case != "truncated" || *consumption != "buffered" || *limit != "1"))
    {
        return Err(std::io::Error::other("unsupported readiness leg").into());
    }
    let read_limit: usize = limit.parse()?;
    if ![1, 16_384].contains(&read_limit) {
        return Err(std::io::Error::other("unsupported diagnostic read capacity").into());
    }
    let target: edgezero_core::http::Uri = request
        .get_header_str("x-fixture-backend")
        .ok_or_else(|| std::io::Error::other("missing controlled origin"))?
        .parse()?;
    let authority = target
        .authority()
        .ok_or_else(|| std::io::Error::other("missing authority"))?;
    if target.scheme_str() != Some("http")
        || target.host() != Some("127.0.0.1")
        || target.port_u16().is_none_or(|port| port == 0)
        || authority.as_str().contains('@')
        || target.query().is_some()
        || target.path() != format!("/readiness/{case}/0")
    {
        return Err(std::io::Error::other("origin must be controlled loopback endpoint").into());
    }
    let token = request
        .get_header_str("x-request-token")
        .ok_or_else(|| std::io::Error::other("missing token"))?;
    if token.is_empty()
        || token.len() > 128
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(std::io::Error::other("invalid token").into());
    }
    let backend = Backend::builder("readiness-origin", authority.as_str())
        .connect_timeout(Duration::from_secs(2))
        .first_byte_timeout(Duration::from_secs(2))
        .between_bytes_timeout(Duration::from_secs(2))
        .finish()?;
    let mut exchange = Exchange {
        identity: json!({"case":case,"mode":mode,"consumption":consumption,
        "read_limit":read_limit,"token":token}),
        start: Instant::now(),
        selected: *mode == "selected",
        read_limit,
        siblings: *case == "siblings",
        samples: Vec::new(),
        slots: Vec::new(),
    };
    for slot in 0..if exchange.siblings { 2 } else { 1 } {
        let (head, body) = Request::get(format!("http://{authority}/readiness/{case}/{slot}"))
            .with_header("x-readiness-token", token)
            .with_pass(true)
            .into_handles();
        let before = exchange.now();
        let pending = head.send_async(body.unwrap_or_else(BodyHandle::new), backend.name());
        let end = exchange.now();
        let pending = match pending {
            Ok(pending) => pending,
            Err(error) => {
                exchange.record(
                    json!({"operation":"dispatch", "slot":slot, "start_us":before,
                    "end_us":end, "outcome":"error", "error":format!("{error:?}")}),
                )?;
                return Err(std::io::Error::other("diagnostic dispatch failed").into());
            }
        };
        exchange.slots.push(Slot {
            handle: Some(Handle::Head(pending)),
            phase: "head",
            result: None,
            bytes: 0,
            checksum: 0xcbf29ce484222325,
        });
        exchange.record(
            json!({"operation":"dispatch", "slot":slot, "start_us":before,
            "end_us":end, "outcome":"pending"}),
        )?;
    }
    let mut buffered = vec![Vec::new(); exchange.slots.len()];
    if *consumption == "buffered" {
        while let Some((slot, chunk)) = exchange.next_chunk()? {
            buffered[slot].extend_from_slice(&chunk);
        }
    } else {
        let mut stream = PullBody::new(exchange)?;
        while let Some((_slot, chunk)) = stream.next()? {
            drop(chunk);
        }
        exchange = stream.0;
    }
    let completed = exchange.now();
    let slots: Vec<_> = exchange
        .slots
        .into_iter()
        .enumerate()
        .map(|(slot, state)| {
            let mut result = state.result.unwrap_or(Value::Null);
            result["buffered_bytes"] = json!(buffered[slot].len());
            result
        })
        .collect();
    let probe = json!({"schema":2, "case":case, "mode":mode, "consumption":consumption,
        "read_limit":read_limit, "token":token,
        "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"),
        "source_state":"working-tree-characterization", "qualification":false,
        "deadline_us":DEADLINE_US, "tolerance_us":5000, "samples":exchange.samples,
        "slots":slots, "completed_us":completed});
    println!("{probe}");
    Response::from_body(serde_json::to_string(&probe)?).send_to_client();
    Ok(())
}
