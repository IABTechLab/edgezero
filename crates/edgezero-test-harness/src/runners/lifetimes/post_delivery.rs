use super::*;

const PREFIX: &[u8] = b"edgezero-lifetime-cleanup ";

pub(super) fn check(
    port: u16,
    origins: &WireOrigins,
    log: &Path,
    cursor: &mut protocol::LogCursor,
    records: &mut Records,
) -> Result<()> {
    let token = token();
    // Reuse the bounded three-role source, with 20/40/60 ms receipt-relative replies.
    origins.register_lifetime(&token, "batch-progress")?;
    origins.mark_lifetime_start(&token)?;
    let backend = format!(
        "http://127.0.0.1:{}/lifetimes/{token}/batch-progress",
        origins.port(0)
    );
    let reply = net::header_request(
        port,
        "/progress/lifetimes/post-delivery-batch",
        &token,
        &[("x-fixture-backend", &backend)],
        None,
    );
    let mut evidence = Vec::new();
    let outcome = (|| -> Result<()> {
        let reply = reply.as_ref().map_err(ToString::to_string)?;
        let types = reply["headers"]
            .as_array()
            .ok_or("missing post-delivery headers")?
            .iter()
            .filter(|field| {
                field[0]
                    .as_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case("content-type"))
            })
            .collect::<Vec<_>>();
        let mut expected = serde_json::to_vec(&json!({"schema":1,"case":"post-delivery-batch",
            "token":token,"qualification":false}))?;
        expected.push(b'\n');
        require(
            reply["status"] == 200
                && types.len() == 1
                && types[0][1] == "application/json"
                && reply["body_bytes"] == json!(expected),
            "invalid post-delivery response",
        )?;
        let (all, end) = protocol::wait_validated(log, cursor, PREFIX, 1, 1, &|_, record| {
            validate(record, &token)
        })?;
        evidence = all;
        cursor.offset = end;
        Ok(())
    })();
    let source = origins.take_lifetime_events(&token);
    let origin = source.as_ref().ok();
    let outcome = outcome.and_then(|()| {
        let origin = source.as_ref().map_err(ToString::to_string)?;
        validate_origin(origin, &token)?;
        validate_timing(
            evidence.first().ok_or("missing post-delivery record")?,
            origin,
        )
    });
    records.push(json!({"event":"lifetime-case","adapter":"fastly","case":"post-delivery-batch","token":token,
        "status":if outcome.is_ok(){"pass"}else{"fail"},"error":outcome.as_ref().err().map(ToString::to_string),
        "protocol":evidence,"origin":origin,"source_error":source.as_ref().err().map(ToString::to_string),
        "client":reply.as_ref().ok(),"client_error":reply.as_ref().err().map(ToString::to_string),"qualification":false}))?;
    outcome
}

fn validate_origin(origin: &Value, token: &str) -> Result<()> {
    let events = origin
        .as_array()
        .ok_or("missing post-delivery source evidence")?;
    require(
        events.len() == 12
            && events.iter().all(|event| {
                event["token"] == token
                    && event["case"] == "batch-progress"
                    && event.get("error").is_none()
            }),
        "invalid post-delivery source identity or cardinality",
    )?;
    for (index, role) in ["a", "b", "c"].into_iter().enumerate() {
        let phases = events
            .iter()
            .filter(|event| event["role"] == role)
            .collect::<Vec<_>>();
        require(
            phases.len() == 4
                && phases
                    .iter()
                    .zip(["request", "head", "chunk", "closed"])
                    .all(|(event, op)| event["op"] == op),
            "post-delivery source phase order changed",
        )?;
        let requested = number(phases[0], "at_us")?;
        let scheduled = requested
            .checked_add([20000, 40000, 60000][index])
            .ok_or("invalid post-delivery schedule")?;
        let latest = scheduled
            .checked_add(5000)
            .ok_or("invalid post-delivery schedule bound")?;
        require(
            number(phases[0], "guest_at_us")? >= 34000
                && phases[3]["reason"] == "complete"
                && phases[2]["bytes"] == json!([b'A' + index as u8]),
            "post-delivery source started early or failed",
        )?;
        for phase in [&phases[1], &phases[2]] {
            require(
                number(phase, "scheduled_us")? == scheduled
                    && (scheduled..=latest).contains(&number(phase, "write_started_us")?)
                    && number(phase, "write_started_us")? <= number(phase, "at_us")?,
                "post-delivery source missed its unchanged schedule",
            )?;
        }
        require(
            number(phases[1], "at_us")? <= number(phases[2], "write_started_us")?
                && number(phases[2], "at_us")? <= number(phases[3], "at_us")?,
            "post-delivery source timestamps are reordered",
        )?;
    }
    Ok(())
}

fn validate_timing(record: &Value, origin: &Value) -> Result<()> {
    let evidence = &record["evidence"];
    let events = origin
        .as_array()
        .ok_or("missing post-delivery source evidence")?;
    let slots = evidence["slots"]
        .as_array()
        .ok_or("missing post-delivery slots")?;
    for (index, role) in ["a", "b", "c"].into_iter().enumerate() {
        let request = events
            .iter()
            .find(|event| event["role"] == role && event["op"] == "request")
            .ok_or("missing post-delivery request receipt")?;
        let chunk = events
            .iter()
            .find(|event| event["role"] == role && event["op"] == "chunk")
            .ok_or("missing post-delivery source write")?;
        let slot = slots.get(index).ok_or("missing post-delivery slot")?;
        let sent = number(request, "guest_at_us")?;
        let released = number(slot, "release_at_us")?;
        let source_delay = number(chunk, "write_started_us")?
            .checked_sub(number(request, "at_us")?)
            .ok_or("post-delivery source wrote before receiving request")?;
        // Same-rate causal durations need no synchronization between host and guest epochs.
        require(
            sent <= number(evidence, "batch_at_us")?
                && released
                    .checked_sub(sent)
                    .and_then(|elapsed| elapsed.checked_add(1000))
                    .is_some_and(|elapsed| elapsed >= source_delay),
            "post-delivery completion preceded its matching source write",
        )?;
    }
    Ok(())
}

fn validate(record: &Value, token: &str) -> Result<()> {
    require(
        record["schema"] == 1
            && record["event"] == "post-delivery-batch"
            && record["token"] == token
            && record["qualification"] == false
            && record["callback_scope_closed"] == true
            && record["terminal_reports"] == 1
            && record["released_after_return"] == json!([1, 1, 1])
            && record["result"] == "Ok",
        "invalid post-delivery callback identity or terminal evidence",
    )?;
    let evidence = &record["evidence"];
    let handoff = number(evidence, "handoff_at_us")?;
    let started = number(evidence, "batch_at_us")?;
    let collecting = number(evidence, "collect_started_us")?;
    let collected = number(evidence, "collected_at_us")?;
    require(
        handoff <= started
            && (34000..=39000).contains(&started)
            && number(evidence, "cutoff_at_us")? == 534000
            && (450000..=455000).contains(&collecting)
            && collecting <= collected
            && collected < 534000
            && evidence["termination"] == "Completed"
            && evidence["callback_scope_live"] == true
            && evidence["same_client"] == true,
        "post-delivery batch changed its clock, budget or ownership",
    )?;
    let slots = evidence["slots"]
        .as_array()
        .ok_or("missing post-delivery slots")?;
    require(slots.len() == 3, "post-delivery batch needs three results")?;
    for (index, slot) in slots.iter().enumerate() {
        let released = number(slot, "release_at_us")?;
        let elapsed = number(slot, "elapsed_us")?;
        require(
            slot["index"] == index
                && slot["bytes"] == json!([b'A' + index as u8])
                && slot["heads"] == 1
                && slot["acquired"] == 1
                && slot["released"] == 1
                && started <= released
                && released < collecting
                && (released - started).abs_diff(elapsed) <= 5000,
            "post-delivery slot was missing, retained or stamped at collection",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Value {
        json!({"schema":1,"event":"post-delivery-batch","token":"post","qualification":false,
            "callback_scope_closed":true,"terminal_reports":1,"result":"Ok","released_after_return":[1,1,1],
            "evidence":{"handoff_at_us":1000,"batch_at_us":34000,"cutoff_at_us":534000,
                "collect_started_us":450000,"collected_at_us":451000,"termination":"Completed",
                "callback_scope_live":true,"same_client":true,"slots":[
                    {"index":0,"elapsed_us":21000,"bytes":[65],"heads":1,"acquired":1,"released":1,"release_at_us":55000},
                    {"index":1,"elapsed_us":41000,"bytes":[66],"heads":1,"acquired":1,"released":1,"release_at_us":75000},
                    {"index":2,"elapsed_us":61000,"bytes":[67],"heads":1,"acquired":1,"released":1,"release_at_us":95000}]}})
    }

    #[test]
    fn requires_original_arrival_times_and_three_releases_before_late_collection() {
        validate(&report(), "post").unwrap();
        let mut late = report();
        late["evidence"]["slots"][0]["elapsed_us"] = json!(416000);
        assert!(validate(&late, "post").is_err());
        let mut retained = report();
        retained["evidence"]["slots"][2]["released"] = json!(0);
        assert!(validate(&retained, "post").is_err());
        let mut duplicate = report();
        duplicate["evidence"]["slots"][1]["released"] = json!(2);
        assert!(validate(&duplicate, "post").is_err());
    }

    #[test]
    fn rejects_wrong_lifetime_cutoff_order_identity_and_cardinality() {
        for field in ["callback_scope_closed", "qualification"] {
            let mut invalid = report();
            invalid[field] = json!(field == "qualification");
            assert!(validate(&invalid, "post").is_err(), "{field}");
        }
        for (field, value) in [
            ("handoff_at_us", json!(35000)),
            ("batch_at_us", json!(40000)),
            ("cutoff_at_us", json!(535000)),
            ("collect_started_us", json!(449999)),
            ("collected_at_us", json!(534000)),
            ("termination", json!("Cutoff")),
            ("same_client", json!(false)),
            ("callback_scope_live", json!(false)),
        ] {
            let mut invalid = report();
            invalid["evidence"][field] = value;
            assert!(validate(&invalid, "post").is_err(), "{field}");
        }
        let mut short = report();
        short["evidence"]["slots"].as_array_mut().unwrap().pop();
        assert!(validate(&short, "post").is_err());
        let mut reused = report();
        reused["evidence"]["slots"][1]["index"] = json!(0);
        assert!(validate(&reused, "post").is_err());
        let mut late_release = report();
        late_release["evidence"]["slots"][0]["release_at_us"] = json!(450000);
        assert!(validate(&late_release, "post").is_err());
        assert!(validate(&report(), "foreign").is_err());
        let mut repeated = report();
        repeated["released_after_return"] = json!([1, 2, 1]);
        assert!(validate(&repeated, "post").is_err());
    }

    fn source() -> Value {
        let mut events = Vec::new();
        for (index, role) in ["a", "b", "c"].into_iter().enumerate() {
            let request = 50000;
            let scheduled = request + [20000, 40000, 60000][index];
            events.extend([
                json!({"op":"request","role":role,"case":"batch-progress","token":"post","at_us":request,"guest_at_us":34000}),
                json!({"op":"head","role":role,"case":"batch-progress","token":"post","at_us":scheduled+1,"scheduled_us":scheduled,"write_started_us":scheduled}),
                json!({"op":"chunk","role":role,"case":"batch-progress","token":"post","at_us":scheduled+3,"scheduled_us":scheduled,"write_started_us":scheduled+2,"bytes":[b'A'+index as u8]}),
                json!({"op":"closed","role":role,"case":"batch-progress","token":"post","at_us":scheduled+4,"reason":"complete"}),
            ]);
        }
        json!(events)
    }

    #[test]
    fn source_evidence_requires_exact_roles_clean_framing_and_original_schedule() {
        validate_origin(&source(), "post").unwrap();
        for (index, field, value) in [
            (0, "guest_at_us", json!(0)),
            (1, "scheduled_us", json!(80000)),
            (1, "write_started_us", json!(76000)),
            (2, "bytes", json!([66])),
            (3, "reason", json!("fixture_released")),
            (4, "role", json!("a")),
        ] {
            let mut invalid = source();
            invalid[index][field] = value;
            assert!(
                validate_origin(&invalid, "post").is_err(),
                "{index}:{field}"
            );
        }
        assert!(validate_origin(&source(), "foreign").is_err());
    }

    #[test]
    fn matching_elapsed_and_release_cannot_precede_the_sources_actual_write() {
        validate_timing(&report(), &source()).unwrap();
        let mut early = report();
        early["evidence"]["slots"][0]["release_at_us"] = json!(35000);
        early["evidence"]["slots"][0]["elapsed_us"] = json!(1000);
        validate(&early, "post").unwrap();
        assert!(validate_timing(&early, &source()).is_err());
    }
}
