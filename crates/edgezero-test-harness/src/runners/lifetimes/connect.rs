use super::*;
use crate::net::connect_fault::{self, Fault};
use std::sync::atomic::{AtomicBool, Ordering};

fn validate(
    report: &Value,
    token: &str,
    target: std::net::SocketAddr,
    healthy: std::net::SocketAddr,
) -> Result<()> {
    require(
        report["adapter"] == "spin"
            && report["case"] == "connect-timeout"
            && report["token"] == token
            && report["qualification"] == false
            && report["steps"]
                .as_array()
                .is_some_and(|steps| steps.len() == 3)
            && report["activity"]["dispatches"] == 3
            && report["activity"]["begins"] == 0
            && report["activity"]["batches"] == 0,
        "invalid native connect/recovery identity",
    )?;
    let fault = &report["steps"][1];
    require(
        fault["step"] == "connect-timeout"
            && fault["uri"] == format!("http://{target}/transport/plain/{token}")
            && fault["settings"] == json!({"timeout_ms":2000,"connect_ms":200})
            && fault["outcome"]["error"]["kind"] == "gateway_timeout"
            && fault["outcome"]["error"]["status"] == 504,
        "connect fixture returned an unreachable, early, or unrelated error",
    )?;
    let elapsed = number(fault, "elapsed_us")?;
    match fault["outcome"]["error"]["cause"].as_str() {
        Some("ConnectTimeout") => require(
            (200_000..2_000_000).contains(&elapsed),
            "invalid phase timeout interval",
        )?,
        Some("PerCallTimeout") => require(
            (2_000_000..=2_005_000).contains(&elapsed),
            "aggregate timeout changed or exceeded qualification window",
        )?,
        _ => return Err("connect timeout lost its governing source".into()),
    }
    for (index, native) in report["steps"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, v)| (i, &v["native"]))
    {
        require(
            native["invalid"] == false
                && native["attachments"] == 1
                && native["preparations"] == 1
                && native["operation_acquisitions"] == 1
                && native["operation_releases"] == 1
                && number(native, "operation_polls")? > 0
                && native["heads"] == u64::from(index != 1)
                && native["prepared"]["mode"] == "Send"
                && native["prepared"]["target"] == report["steps"][index]["uri"]
                && number(native, "source_acquisitions")? <= 1
                && native["source_releases"] == native["source_acquisitions"]
                && (index != 1 || native["source_acquisitions"] == 0)
                && (index != 2 || native["source_acquisitions"] == 1),
            "connect/recovery lacks the actual SDK operation and release proof",
        )?;
    }
    let initial = &report["steps"][0];
    require(
        initial["step"] == "connect-start"
            && initial["uri"]
                == format!("http://{healthy}/lifetimes/{token}/connect-timeout/start")
            && initial["outcome"]["status"] == 204
            && initial["outcome"]["bytes"] == json!([]),
        "invalid causal start control",
    )?;
    let recovered = &report["steps"][2];
    require(
        recovered["step"] == "recovery"
            && recovered["uri"]
                == format!("http://{healthy}/lifetimes/{token}/connect-timeout/done")
            && recovered["outcome"]["status"] == 200
            && recovered["outcome"]["bytes"] == json!(b"ok".as_slice()),
        "same-client recovery failed",
    )
}

fn fault_bounds(source: &Value, token: &str, elapsed: u64) -> Result<[u64; 2]> {
    let events = source
        .as_array()
        .filter(|events| events.len() == 9)
        .ok_or("missing causal control source events")?;
    require(
        events.iter().all(|v| {
            v["token"] == token && v["case"] == "connect-timeout" && v.get("error").is_none()
        }),
        "foreign or failed causal control event",
    )?;
    let get = |role: &str, op: &str| -> Result<&Value> {
        let mut found = events.iter().filter(|v| v["role"] == role && v["op"] == op);
        let value = found.next().ok_or("missing causal boundary")?;
        require(found.next().is_none(), "duplicate causal boundary")?;
        Ok(value)
    };
    for role in ["start", "done"] {
        let mut previous = 0;
        for op in ["request", "write-started", "head"]
            .into_iter()
            .chain((role == "done").then_some("chunk"))
            .chain(["closed"])
        {
            let event = get(role, op)?;
            let at = number(event, "at_us")?;
            require(at >= previous, "reordered causal source boundary")?;
            previous = at;
        }
    }
    require(
        get("done", "chunk")?["bytes"] == json!(b"ok"),
        "recovery source payload changed",
    )?;
    let start = number(get("start", "write-started")?, "at_us")?;
    let done = number(get("done", "request")?, "at_us")?;
    require(
        number(get("start", "closed")?, "at_us")? <= done,
        "recovery preceded the completed start control",
    )?;
    let lower = done
        .checked_sub(elapsed)
        .and_then(|at| at.checked_add(2))
        .ok_or("fault duration exceeds its causal window")?;
    let upper = start.checked_add(elapsed).ok_or("causal bound overflow")?;
    require(
        lower < upper,
        "causal clock bounds cannot prove an active fault interval",
    )?;
    Ok([lower, upper])
}

fn validate_samples(
    samples: &[Value],
    pid: u32,
    ticks: u64,
    target: std::net::SocketAddr,
    cause: &str,
    bounds: [u64; 2],
) -> Result<()> {
    require(
        !samples.is_empty() && samples.len() <= 512,
        "missing or unbounded native socket observations",
    )?;
    let mut connection = None;
    let mut first_pending = None;
    let mut last_pending = 0;
    let mut previous = 0;
    for sample in samples {
        let socket = &sample["socket"];
        let at = number(sample, "at_us")?;
        let read_started = number(sample, "read_started_us")?;
        let inode = number(socket, "inode")?;
        let local = socket["local"]
            .as_str()
            .ok_or("missing native source endpoint")?;
        let key = (inode, local);
        require(
            sample["pid"] == pid
                && sample["start_ticks"] == ticks
                && socket["remote"] == target.to_string()
                && inode > 0
                && local.parse::<std::net::SocketAddr>()?.ip().is_loopback()
                && at >= previous
                && read_started <= at
                && connection.get_or_insert(key) == &key,
            "socket ownership, destination, identity or observation ordering changed",
        )?;
        previous = at;
        match number(socket, "state")? {
            2 => {
                if read_started >= bounds[0] && at < bounds[1] {
                    first_pending.get_or_insert(at);
                    last_pending = read_started;
                }
            }
            7 => {}
            _ => return Err("controlled socket established or acquired a non-client state".into()),
        }
    }
    let first = first_pending.ok_or("no actual Spin SYN_SENT observation")?;
    match cause {
        "ConnectTimeout" => Ok(()),
        "PerCallTimeout" => require(
            last_pending.saturating_sub(first) >= 200_000
                && last_pending >= bounds[1].saturating_sub(5_000),
            "aggregate outcome lacks SYN_SENT evidence near its original terminal bound",
        ),
        _ => Err("unrecognized native timeout cause".into()),
    }
}

pub(super) fn check(port: u16, origins: &WireOrigins, records: &mut Records) -> Result<()> {
    let fault = match Fault::start() {
        Ok(fault) => fault,
        Err(error) => {
            records.push(json!({"event":"connect-timeout-case","adapter":"spin",
                "status":"unverified","error":error.to_string(),"qualification":false}))?;
            return Err(error);
        }
    };
    let target = fault.target();
    let group = crate::startup::owned_process_group(port)?;
    let owner = connect_fault::resolve_listener(group, ([127, 0, 0, 1], port).into())?;
    crate::startup::check(port)?;
    let pid = owner.pid;
    let before = connect_fault::observe_pinned(&owner, target)?;
    require(
        before.rows.is_empty(),
        "runtime already owns the fresh fault destination",
    )?;
    let token = token();
    let first = format!("http://{target}/");
    let second = format!("http://127.0.0.1:{}/", origins.port(0));
    origins.register_lifetime(&token, "connect-timeout")?;
    let done = AtomicBool::new(false);
    let started = origins.mark_lifetime_start(&token)?;
    let (reply, samples) = thread::scope(|scope| -> Result<_> {
        let waiting = scope.spawn(|| {
            let result = net::header_request(
                port,
                "/progress/transport/connect-timeout",
                &token,
                &[("x-origin", &first), ("x-origin-second", &second)],
                None,
            );
            done.store(true, Ordering::SeqCst);
            result
        });
        let observed = (|| -> Result<Vec<Value>> {
            let mut samples = Vec::new();
            for _ in 0..512 {
                let read_started = started.elapsed().as_micros();
                let sample = connect_fault::observe_pinned(&owner, target)?;
                require(
                    sample.pid == owner.pid
                        && sample.start_ticks == owner.start_ticks
                        && sample.process_group == group
                        && sample.rows.len() <= 1,
                    "runtime identity or connection cardinality changed",
                )?;
                for row in sample.rows {
                    samples.push(json!({"pid":sample.pid,"start_ticks":sample.start_ticks,"process_group":sample.process_group,
                        "leader_start_ticks":owner.leader_start_ticks,"listener_inode":owner.listener_inode,
                        "read_started_us":read_started,"at_us":started.elapsed().as_micros(),"socket":row.json()}));
                }
                if done.load(Ordering::SeqCst) {
                    return Ok(samples);
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err("connect observation exhausted its finite sampling window".into())
        })();
        let reply = waiting
            .join()
            .map_err(|_| "connect request reader panicked")?;
        Ok((reply, observed))
    })?;
    let mut report = Value::Null;
    let source = origins.take_lifetime_events(&token);
    let mut bounds = None;
    let checked = (|| -> Result<()> {
        let samples = samples.as_ref().map_err(ToString::to_string)?;
        require(
            !samples.is_empty(),
            "no Spin-owned SYN_SENT evidence during the real SDK request",
        )?;
        let reply = reply.as_ref().map_err(ToString::to_string)?;
        require(
            reply["status"] == 200,
            "connect guest returned an error response",
        )?;
        report = serde_json::from_slice(&serde_json::from_value::<Vec<u8>>(
            reply["body_bytes"].clone(),
        )?)?;
        validate(
            &report,
            &token,
            target,
            format!("127.0.0.1:{}", origins.port(0)).parse()?,
        )?;
        let interval = fault_bounds(
            source.as_ref().map_err(ToString::to_string)?,
            &token,
            number(&report["steps"][1], "elapsed_us")?,
        )?;
        bounds = Some(interval);
        validate_samples(
            samples,
            pid,
            before.start_ticks,
            target,
            report["steps"][1]["outcome"]["error"]["cause"]
                .as_str()
                .ok_or("missing timeout cause")?,
            interval,
        )?;
        crate::startup::check(port).map(|_| ())
    })();
    records.push(json!({"event":"connect-timeout-case","adapter":"spin","sentinel":"ez_4_acceptance_06_spin",
        "status":if checked.is_ok(){"pass"}else{"fail"},"error":checked.as_ref().err().map(ToString::to_string),
        "fault":fault.policy(),"pid":pid,"start_ticks":before.start_ticks,"process_group":group,
        "leader_start_ticks":owner.leader_start_ticks,"listener_inode":owner.listener_inode,
        "samples":samples.as_ref().ok(),"sample_error":samples.as_ref().err().map(ToString::to_string),
        "report":report,"client_error":reply.as_ref().err().map(ToString::to_string),
        "source":source.as_ref().ok(),"source_error":source.as_ref().err().map(ToString::to_string),
        "certain_fault_interval_us":bounds,"same_rate_assumed":true,"clock_sync_proven":false,"qualification":false}))?;
    checked?;
    println!("ez_4_acceptance_06_spin: passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_report() -> Value {
        let uris = [
            "http://127.0.0.1:4661/lifetimes/control/connect-timeout/start",
            "http://127.0.0.1:4660/transport/plain/control",
            "http://127.0.0.1:4661/lifetimes/control/connect-timeout/done",
        ];
        let native = |index: usize| {
            json!({"invalid":false,"attachments":1,"preparations":1,
            "operation_acquisitions":1,"operation_releases":1,"operation_polls":1,
            "heads":u64::from(index!=1),"source_acquisitions":u64::from(index==2),
            "source_releases":u64::from(index==2),"prepared":{"mode":"Send","target":uris[index]}})
        };
        json!({"adapter":"spin","case":"connect-timeout","token":"control","qualification":false,
            "activity":{"dispatches":3,"begins":0,"batches":0},"steps":[
            {"step":"connect-start","uri":uris[0],"outcome":{"status":204,"bytes":[]},"native":native(0)},
            {"step":"connect-timeout","uri":uris[1],"settings":{"timeout_ms":2000,"connect_ms":200},
            "elapsed_us":2_000_000,"outcome":{"error":{"kind":"gateway_timeout","status":504,"cause":"PerCallTimeout"}},"native":native(1)},
            {"step":"recovery","uri":uris[2],"outcome":{"status":200,"bytes":b"ok"},"native":native(2)}]})
    }

    #[test]
    fn every_prepared_mode_and_target_must_match_its_actual_step() {
        let target = "127.0.0.1:4660".parse().unwrap();
        let healthy = "127.0.0.1:4661".parse().unwrap();
        let original = model_report();
        validate(&original, "control", target, healthy).unwrap();
        for index in 0..3 {
            for (field, value) in [
                ("mode", json!("Begin")),
                ("target", json!("http://127.0.0.1:9/foreign")),
            ] {
                let mut changed = original.clone();
                changed["steps"][index]["native"]["prepared"][field] = value;
                assert!(validate(&changed, "control", target, healthy).is_err());
            }
            let mut missing = original.clone();
            missing["steps"][index]["native"]
                .as_object_mut()
                .unwrap()
                .remove("prepared");
            assert!(validate(&missing, "control", target, healthy).is_err());
        }
        let mut wrong = original;
        wrong["steps"][2]["uri"] = json!("http://127.0.0.1:9/foreign");
        assert!(validate(&wrong, "control", target, healthy).is_err());
    }

    fn controls() -> Value {
        let mut events = Vec::new();
        for (role, steps) in [
            (
                "start",
                vec![
                    ("request", 0),
                    ("write-started", 100),
                    ("head", 200),
                    ("closed", 300),
                ],
            ),
            (
                "done",
                vec![
                    ("request", 2_000_500),
                    ("write-started", 2_000_510),
                    ("head", 2_000_520),
                    ("chunk", 2_000_530),
                    ("closed", 2_000_540),
                ],
            ),
        ] {
            for (op, at) in steps {
                let mut event = json!({"token":"control","case":"connect-timeout","role":role,"op":op,"at_us":at});
                if op == "chunk" {
                    event["bytes"] = json!(b"ok");
                }
                events.push(event);
            }
        }
        json!(events)
    }

    #[test]
    fn causal_control_bounds_do_not_invent_a_clock_alignment() {
        let original = controls();
        assert_eq!(
            fault_bounds(&original, "control", 2_000_000).unwrap(),
            [502, 2_000_100]
        );
        for index in 0..9 {
            let mut missing = original.clone();
            missing.as_array_mut().unwrap().remove(index);
            assert!(fault_bounds(&missing, "control", 2_000_000).is_err());
            let mut foreign = original.clone();
            foreign[index]["token"] = json!("another");
            assert!(fault_bounds(&foreign, "control", 2_000_000).is_err());
        }
        let mut delayed = original.clone();
        delayed[4]["at_us"] = json!(5_000_000);
        assert!(fault_bounds(&delayed, "control", 2_000_000).is_err());
    }

    fn validate_samples(
        samples: &[Value],
        pid: u32,
        ticks: u64,
        target: std::net::SocketAddr,
        cause: &str,
    ) -> Result<()> {
        super::validate_samples(samples, pid, ticks, target, cause, [500, 2_000_000])
    }

    fn socket_sample(at: u64, state: u64) -> Value {
        json!({"pid":73,"start_ticks":99,"at_us":at,"read_started_us":at.saturating_sub(1),
            "socket":{"inode":17,"local":"127.0.0.1:40000","remote":"127.0.0.1:4660","state":state}})
    }

    #[test]
    fn aggregate_cannot_credit_a_socket_closed_long_before_its_reported_deadline() {
        let samples = [
            socket_sample(1000, 2),
            socket_sample(201000, 2),
            socket_sample(202000, 7),
        ];
        assert!(
            validate_samples(
                &samples,
                73,
                99,
                "127.0.0.1:4660".parse().unwrap(),
                "PerCallTimeout"
            )
            .is_err()
        );
    }

    #[test]
    fn delayed_inspection_cannot_turn_an_early_snapshot_into_terminal_evidence() {
        let mut delayed = socket_sample(1_999_000, 2);
        delayed["read_started_us"] = json!(200_000);
        let samples = [socket_sample(1000, 2), delayed];
        assert!(
            validate_samples(
                &samples,
                73,
                99,
                "127.0.0.1:4660".parse().unwrap(),
                "PerCallTimeout"
            )
            .is_err()
        );
    }

    #[test]
    fn phase_cannot_credit_a_socket_observed_only_during_later_recovery() {
        let samples = [socket_sample(2_100_000, 2)];
        assert!(
            validate_samples(
                &samples,
                73,
                99,
                "127.0.0.1:4660".parse().unwrap(),
                "ConnectTimeout"
            )
            .is_err()
        );
    }

    #[test]
    fn actual_spin_connection_cannot_be_replaced_by_a_foreign_or_established_socket() {
        let target = "127.0.0.1:4660".parse().unwrap();
        let sample = |at: u64, state| {
            json!({"pid":73,"start_ticks":99,"at_us":at,"read_started_us":at.saturating_sub(1),
            "socket":{"inode":17,"local":"127.0.0.1:40000","remote":"127.0.0.1:4660","state":state}})
        };
        let valid = vec![sample(1000, 2), sample(1_999_000, 2), sample(2_000_000, 7)];
        validate_samples(&valid, 73, 99, target, "PerCallTimeout").unwrap();
        validate_samples(&valid, 73, 99, target, "ConnectTimeout").unwrap();
        for field in ["pid", "start_ticks"] {
            let mut changed = valid.clone();
            changed[0][field] = json!(74);
            assert!(validate_samples(&changed, 73, 99, target, "PerCallTimeout").is_err());
        }
        for (field, value) in [
            ("state", json!(1)),
            ("inode", json!(18)),
            ("remote", json!("127.0.0.1:4661")),
        ] {
            let mut changed = valid.clone();
            changed[1]["socket"][field] = value;
            assert!(validate_samples(&changed, 73, 99, target, "PerCallTimeout").is_err());
        }
        assert!(validate_samples(&valid[..1], 73, 99, target, "PerCallTimeout").is_err());
        assert!(validate_samples(&[], 73, 99, target, "ConnectTimeout").is_err());
    }
}
