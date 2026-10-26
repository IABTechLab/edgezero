use fastly::handle::{BodyHandle, PendingRequestHandle, PollHandleResult, ResponseHandle};
use fastly::{Backend, Request, Response};
use serde_json::{Value, json};
use std::io::Read;
use std::time::{Duration, Instant};

const DEADLINE_US: u64 = 500_000;

fn client_failure(error: &edgezero_core::EdgeError) -> Value {
    use edgezero_core::{FastlyFailureCause, ProviderFailureDetail, ProviderFailurePhase};
    let source_eof = matches!(
        error.provider_failure_detail(),
        Some(ProviderFailureDetail::Fastly {
            phase: ProviderFailurePhase::ResponseBodyRead,
            cause: FastlyFailureCause::IoError {
                kind: std::io::ErrorKind::UnexpectedEof
            },
        })
    );
    json!({"category":error.kind(),"source_unexpected_eof":source_eof})
}

fn client_probe(
    request: &Request,
    case: &str,
    consumption: &str,
    authority: &str,
) -> Result<(), fastly::Error> {
    use edgezero_adapter_fastly::outbound::FastlyOutboundClient;
    use edgezero_core::{Deadline, HttpClient, OutboundRequest};
    let token = request_token(request)?;
    let client = HttpClient::with_client(FastlyOutboundClient::new());
    let make = |slot| {
        OutboundRequest::get(format!("http://{authority}/readiness/{case}/{slot}"))
            .and_then(|request| request.header("x-readiness-token", token))
            .map(|request| {
                request
                    .timeout(Duration::from_secs(2))
                    .max_response_bytes(65_536)
            })
    };
    let start = Instant::now();
    let result = futures::executor::block_on(async {
        if case == "full-body-error" {
            let mut outbound = make(0)?;
            if consumption == "streamed" {
                outbound = outbound.stream_response();
            }
            let mut bytes = 0usize;
            let mut checksum = 0xcbf29ce484222325u64;
            let result = match client.send(outbound).await {
                Ok(response) if consumption == "streamed" => {
                    use futures::StreamExt;
                    let mut source = response.into_body().into_stream().ok_or_else(|| {
                        edgezero_core::EdgeError::internal(std::io::Error::other(
                            "expected streamed response",
                        ))
                    })?;
                    let mut result = Ok(());
                    while let Some(chunk) = source.next().await {
                        match chunk {
                            Ok(chunk) => {
                                bytes += chunk.len();
                                if bytes > 65_536 {
                                    return Err(edgezero_core::EdgeError::internal(
                                        std::io::Error::other("probe response limit exceeded"),
                                    ));
                                }
                                for byte in chunk {
                                    checksum =
                                        (checksum ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                            }
                            Err(error) => {
                                result = Err(error);
                                break;
                            }
                        }
                    }
                    result
                }
                Ok(response) => response.into_bytes_bounded(65_536).await.map(|_| ()),
                Err(error) => Err(error),
            };
            return Ok::<_, edgezero_core::EdgeError>(
                json!({"kind":if result.is_err(){"error"}else{"eof"},
                "bytes":bytes,"checksum":format!("{checksum:016x}"),"failure":result.err().map(|e|client_failure(&e))}),
            );
        }
        let results = client
            .send_all_until(
                vec![make(0)?, make(1)?],
                Deadline::after(Duration::from_micros(DEADLINE_US)),
            )
            .await
            .map_err(|failure| failure.error)?;
        let mut slots = Vec::new();
        for slot in results.slots {
            slots.push(match slot {
                None => json!({"kind":"pending"}),
                Some(slot) => match slot.outcome {
                    Ok(response) => {
                        let bytes = response.into_bytes_bounded(65_536).await?;
                        let checksum = bytes.iter().fold(0xcbf29ce484222325u64, |sum, byte| (sum ^ u64::from(*byte)).wrapping_mul(0x100000001b3));
                        json!({"kind":"ok","bytes":bytes.len(),"checksum":format!("{checksum:016x}"),"elapsed_us":slot.elapsed.as_micros()})
                    }
                    Err(error) => json!({"kind":"error","failure":client_failure(&error)}),
                },
            });
        }
        Ok(json!({"termination":format!("{:?}",results.termination),"slots":slots}))
    });
    let probe = json!({"event":"fastly-client-progress","case":case,"consumption":consumption,
        "token":token,"revision":env!("EDGEZERO_QUALIFICATION_REVISION"),"result":result.map_err(|e|client_failure(&e)),"completed_us":start.elapsed().as_micros(),"qualification":false});
    println!("{probe}");
    Response::from_body(serde_json::to_string(&probe)?).send_to_client();
    Ok(())
}

struct Selection {
    status: String,
    index: Option<usize>,
    valid: bool,
    entered: Instant,
    returned: Instant,
}

// Only this opt-in host-primitive experiment calls the raw readiness ABI.
#[allow(
    unsafe_code,
    reason = "probe the pinned ABI with live borrowed handles"
)]
fn select(handles: &[u32], timeout_ms: u32) -> Selection {
    let mut selected = u32::MAX;
    let entered = Instant::now();
    // SAFETY: slices remain live, and the caller retains ownership of every handle.
    let status = unsafe {
        fastly_sys::fastly_async_io::select(
            handles.as_ptr(),
            handles.len(),
            timeout_ms,
            &mut selected,
        )
    };
    let returned = Instant::now();
    let index = (selected != u32::MAX).then_some(selected as usize);
    let valid = status.is_ok() && index.is_none_or(|index| index < handles.len());
    Selection {
        status: format!("{status:?}"),
        index,
        valid,
        entered,
        returned,
    }
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
    heads_first: bool,
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
            let heads_only = self.heads_first
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
                let selection = select(&handles, timeout_ms);
                let end = self.now();
                self.record(json!({"operation":"select", "start_us":start, "end_us":end,
                    "slots":live.iter().map(|(slot, _, _)| *slot).collect::<Vec<_>>(),
                    "phases":live.iter().map(|(_, phase, _)| *phase).collect::<Vec<_>>(),
                    "timeout_ms":timeout_ms, "status":selection.status, "selected":selection.index,
                    "abi_enter_us":selection.entered.duration_since(self.start).as_micros(),
                    "abi_return_us":selection.returned.duration_since(self.start).as_micros()}))?;
                if !selection.valid {
                    return Err(std::io::Error::other("invalid readiness status or index").into());
                }
                if end >= DEADLINE_US {
                    continue;
                }
                let Some(index) = selection.index else {
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

fn request_token(request: &Request) -> Result<&str, fastly::Error> {
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
    Ok(token)
}

fn timer_control(request: &Request, source: &str, budget: &str) -> Result<(), fastly::Error> {
    let budget_ms: u32 = budget.parse()?;
    if ![1, 100, 500].contains(&budget_ms) || !["empty-select", "wasi-sleep"].contains(&source) {
        return Err(std::io::Error::other("invalid timer control").into());
    }
    let token = request_token(request)?;
    let start = Instant::now();
    let selection = if source == "empty-select" {
        Some(select(&[], budget_ms))
    } else {
        std::thread::sleep(Duration::from_millis(u64::from(budget_ms)));
        None
    };
    let elapsed_us = match &selection {
        Some(selection) => selection.returned.duration_since(start),
        None => start.elapsed(),
    }
    .as_micros() as u64;
    let budget_us = u64::from(budget_ms) * 1000;
    let mut result = json!({"schema":1, "event":"readiness-timer-control", "source":source,
        "budget_ms":budget_ms, "token":token, "elapsed_us":elapsed_us,
        "overrun_us":elapsed_us.saturating_sub(budget_us),
        "within_five_ms":elapsed_us <= budget_us + 5000, "qualification":false,
        "revision":option_env!("EDGEZERO_QUALIFICATION_REVISION").unwrap_or("unrecorded"),
        "source_state":"working-tree-characterization", "measurement_end":"primitive-return"});
    if let Some(selection) = selection {
        result["abi_status"] = json!(selection.status);
        result["selected"] = json!(selection.index);
        result["abi_enter_us"] = json!(selection.entered.duration_since(start).as_micros());
        result["abi_return_us"] = json!(selection.returned.duration_since(start).as_micros());
    }
    println!("{result}");
    Response::from_body(serde_json::to_string(&result)?).send_to_client();
    Ok(())
}

fn main() -> Result<(), fastly::Error> {
    fastly::init();
    let request = Request::from_client();
    let segments: Vec<_> = request
        .get_path()
        .trim_start_matches('/')
        .split('/')
        .collect();
    if let ["timer-control", source, budget] = segments.as_slice() {
        return timer_control(&request, source, budget);
    }
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
        "mixed-head-body",
        "mixed-body-head",
        "full-body-error",
    ]
    .contains(case)
        || !["buffered", "streamed"].contains(consumption)
        || !["selected", "direct", "client"].contains(mode)
        || (*mode == "direct"
            && (*case != "truncated" || *consumption != "buffered" || *limit != "1"))
    {
        return Err(std::io::Error::other("unsupported readiness leg").into());
    }
    let read_limit: usize = limit.parse()?;
    if ![1, 16_384].contains(&read_limit) {
        return Err(std::io::Error::other("unsupported diagnostic read capacity").into());
    }
    if matches!(
        *case,
        "mixed-head-body" | "mixed-body-head" | "full-body-error"
    ) && read_limit != 16_384
    {
        return Err(
            std::io::Error::other("progress probe requires full diagnostic capacity").into(),
        );
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
    let token = request_token(&request)?;
    if *mode == "client" {
        if !matches!(
            *case,
            "mixed-head-body" | "mixed-body-head" | "full-body-error"
        ) {
            return Err(std::io::Error::other("unsupported client probe").into());
        }
        return client_probe(&request, case, consumption, authority.as_str());
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
        heads_first: *case == "siblings",
        samples: Vec::new(),
        slots: Vec::new(),
    };
    let count = if matches!(*case, "siblings" | "mixed-head-body" | "mixed-body-head") {
        2
    } else {
        1
    };
    for slot in 0..count {
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
