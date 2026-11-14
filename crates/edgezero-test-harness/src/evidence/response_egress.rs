use super::{Result, require};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs::File, io::Read, path::Path};

const MAX_INPUT: u64 = 4 * 1024 * 1024;
const MAX_TRANSFER: u64 = 64 * 1024 * 1024;
const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;
const SCHEDULE: &str = include_str!("../../acceptance/response-egress-schedule.json");

fn token(value: &Value) -> bool {
    value.as_str().is_some_and(|text| {
        (1..=64).contains(&text.len())
            && text
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

fn hex(value: &Value, length: usize) -> bool {
    value.as_str().is_some_and(|text| {
        text.len() == length
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn bounded_text(value: &Value, maximum: usize) -> bool {
    value
        .as_str()
        .is_some_and(|text| !text.is_empty() && text.encode_utf16().count() <= maximum)
}

fn valid_identity(spec: &Value) -> bool {
    token(&spec["run"])
        && hex(&spec["revision"], 40)
        && hex(&spec["artifact"], 64)
        && hex(&spec["binary_sha256"], 64)
        && matches!(
            spec["adapter"].as_str(),
            Some("axum" | "cloudflare" | "fastly" | "spin")
        )
        && bounded_text(&spec["sdk"], 79)
        && spec["runtime"].is_object()
        && (bounded_text(&spec["runtime"]["revision"], 100)
            || matches!(
                spec["runtime"]["unknown"].as_str(),
                Some("provider-unpublished" | "operator-unavailable")
            ))
        && spec["compatibility"].is_object()
        && spec["compatibility"]["flags"]
            .as_array()
            .is_some_and(|flags| {
                flags.len() <= 32
                    && flags.iter().all(|flag| {
                        flag.as_str().is_some_and(|text| {
                            (1..=80).contains(&text.len())
                                && text.bytes().all(|byte| {
                                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
                                })
                        })
                    })
            })
        && (spec["adapter"] != "cloudflare" || valid_date(&spec["compatibility"]["date"]))
        && bounded_text(&spec["deployment"], 200)
}

fn valid_date(value: &Value) -> bool {
    value.as_str().is_some_and(|text| {
        text.len() == 10
            && text.bytes().enumerate().all(|(index, byte)| {
                if index == 4 || index == 7 {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit()
                }
            })
    })
}

fn integer(value: &Value) -> Option<u64> {
    let number = value.as_f64()?;
    (number.is_finite()
        && number >= 0.0
        && number <= MAX_SAFE_INTEGER as f64
        && number.fract() == 0.0)
        .then_some(number as u64)
}

fn number_is(value: &Value, expected: u64) -> bool {
    integer(value) == Some(expected)
}

fn is_null(value: &Value, field: &str) -> bool {
    value.get(field).is_some_and(Value::is_null)
}

fn action(name: &str) -> &str {
    match name {
        "nonread" | "disconnect" | "cancel" | "slow" => name,
        _ => "read",
    }
}

fn validate(manifest: &Value, clients: &Value, events: &Value) -> Result<usize> {
    require(
        number_is(&manifest["schema"], 1) && valid_identity(manifest),
        "Revision-pinned deployment metadata is incomplete",
    )?;
    require(
        manifest["telemetry_complete"] == true,
        "Complete unsampled telemetry export is required",
    )?;
    let schedule: Value = serde_json::from_str(SCHEDULE)?;
    require(
        manifest.get("expected") == Some(&schedule),
        "The complete canonical 26-probe schedule is required",
    )?;
    let expected = schedule
        .as_array()
        .ok_or("invalid embedded probe schedule")?;
    let clients = clients.as_array().ok_or("Invalid client observations")?;
    let events = events.as_array().ok_or("Invalid telemetry events")?;
    require(
        clients.len() == expected.len(),
        "Missing or extra client observations",
    )?;
    require(
        events.len() == 4 * expected.len(),
        "Missing, extra or duplicate telemetry",
    )?;
    let names = expected
        .iter()
        .map(|probe| {
            Ok((
                probe["probe"].as_str().ok_or("invalid embedded probe")?,
                probe["case"].as_str().ok_or("invalid embedded case")?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut observations = BTreeMap::new();
    for client in clients {
        let probe = client["probe"].as_str().ok_or("Invalid client probe")?;
        let name = client["case"].as_str().ok_or("Invalid client case")?;
        require(
            names.get(probe) == Some(&name)
                && !observations.contains_key(probe)
                && client["action"] == action(name)
                && integer(&client["bytes"]).is_some_and(|bytes| bytes <= MAX_TRANSFER),
            "Invalid client observation",
        )?;
        observations.insert(probe, client);
    }
    for event in events {
        let probe = event["probe"].as_str().ok_or("Missing telemetry probe")?;
        require(
            number_is(&event["schema"], 1)
                && event["run"] == manifest["run"]
                && event["revision"] == manifest["revision"]
                && event["artifact"] == manifest["artifact"]
                && event["case"]
                    .as_str()
                    .is_some_and(|name| names.get(probe) == Some(&name))
                && token(&event["instance"]),
            "Telemetry identity mismatch",
        )?;
    }
    for (probe, name) in names {
        let mut trace = events
            .iter()
            .filter(|event| event["probe"] == probe)
            .collect::<Vec<_>>();
        trace.sort_by_key(|event| integer(&event["sequence"]).unwrap_or(u64::MAX));
        require(
            trace.len() == 4
                && trace.iter().enumerate().all(|(index, event)| {
                    integer(&event["sequence"]) == u64::try_from(index).ok()
                        && event["instance"] == trace[0]["instance"]
                }),
            "Incomplete or conflicting trace sequence",
        )?;
        let one = |kind: &str| -> Result<&Value> {
            let selected = trace
                .iter()
                .filter(|event| event["event"] == kind)
                .collect::<Vec<_>>();
            require(selected.len() == 1, "Missing or duplicate lifecycle event")?;
            Ok(*selected[0])
        };
        one("source-drop")?;
        let terminal = one("terminal")?;
        let closed = one("trace-closed")?;
        let resource = one("resource-drop")?;
        require(
            number_is(&closed["sequence"], 3)
                && number_is(&closed["terminals"], 1)
                && number_is(&closed["source_drops"], 1)
                && number_is(&closed["resource_drops"], 1)
                && number_is(&closed["polls_after_terminal"], 0)
                && integer(&closed["polls"]).is_some()
                && integer(&terminal["sequence"]) < integer(&resource["sequence"]),
            "Ownership counters or release ordering disagree",
        )?;
        require(
            integer(&terminal["bytes_written"]).is_some_and(|bytes| bytes <= MAX_TRANSFER)
                && matches!(
                    terminal["body_kind"].as_str(),
                    Some("Application" | "Fallback")
                ),
            "Invalid terminal byte accounting",
        )?;
        require(
            matches!(
                terminal["outcome"].as_str(),
                Some(
                    "Completed"
                        | "HostHandoff"
                        | "ClientDisconnected"
                        | "ConversionError"
                        | "DeadlineExceeded"
                        | "RequestCancelled"
                        | "SourceError"
                        | "TransportError"
                )
            ),
            "Unknown terminal outcome",
        )?;
        let client = observations
            .get(probe)
            .ok_or("Missing client observation")?;
        let bodyless = matches!(name, "head" | "204" | "205" | "304");
        if name == "normal" || bodyless {
            let status = match name {
                "204" => 204,
                "205" => 205,
                "304" => 304,
                _ => 200,
            };
            require(
                matches!(
                    terminal["outcome"].as_str(),
                    Some("Completed" | "HostHandoff")
                ) && terminal["body_kind"] == "Application"
                    && is_null(terminal, "fallback")
                    && client["ended"] == true
                    && number_is(&client["bytes"], if bodyless { 0 } else { 3072 })
                    && number_is(&client["status"], status),
                "Normal/bodyless client observation or terminal outcome disagrees",
            )?;
        }
        if bodyless {
            require(
                number_is(&closed["polls"], 0) && number_is(&terminal["bytes_written"], 0),
                "Suppressed body was polled or written",
            )?;
        }
        if name == "normal" {
            require(
                integer(&closed["polls"]).is_some_and(|polls| polls >= 4)
                    && number_is(&terminal["bytes_written"], 3072),
                "Normal response lacks source/accepted-byte evidence",
            )?;
        }
        if name == "pending" {
            require(
                integer(&closed["polls"]).is_some_and(|polls| polls >= 1),
                "Pending producer was never polled",
            )?;
        }
        if name == "expired" {
            require(
                number_is(&closed["polls"], 0)
                    && terminal["body_kind"] == "Fallback"
                    && (is_null(client, "status") || number_is(&client["status"], 504)),
                "Expired precommit response was not suppressed",
            )?;
        }
        if matches!(name, "nonread" | "disconnect" | "cancel") {
            require(
                client["ended"] == false
                    && client["result"]
                        == if name == "disconnect" {
                            "client-close"
                        } else {
                            "client-deadline"
                        },
                "Client did not exercise the scheduled interruption",
            )?;
        }
        if name == "slow" {
            require(
                integer(&client["pauses"]).is_some_and(|pauses| pauses > 0),
                "Slow-client backpressure was not exercised",
            )?;
        }
        if matches!(name, "expired" | "pending") {
            require(
                terminal["outcome"] == "DeadlineExceeded",
                "Deadline was not observed",
            )?;
        }
        if name == "source-error" {
            require(
                terminal["outcome"] == "SourceError",
                "Source failure was misclassified",
            )?;
        }
        if name == "conversion-error" {
            require(
                terminal["outcome"] == "ConversionError",
                "Conversion failure was misclassified",
            )?;
        }
        if terminal["body_kind"] == "Fallback" {
            require(
                matches!(terminal["fallback"].as_str(), Some("Completed" | "Aborted"))
                    && matches!(
                        terminal["outcome"].as_str(),
                        Some("DeadlineExceeded" | "ConversionError" | "SourceError")
                    )
                    && integer(&terminal["bytes_written"]).is_some_and(|bytes| bytes <= 1024),
                "Fallback cause or bounded disposition is invalid",
            )?;
        } else {
            require(
                is_null(terminal, "fallback"),
                "Application report cannot have fallback disposition",
            )?;
        }
    }
    Ok(expected.len())
}

fn report(manifest: &Value, clients: &Value, events: &Value) -> Value {
    match validate(manifest, clients, events) {
        Ok(probes) => {
            json!({"status":"qualified-ownership","scope":"scheduled probes only; not confirmed client delivery or provider allocation","probes":probes,"capabilities_changed":false})
        }
        Err(error) => json!({"status":"unverified","reason":error.to_string()}),
    }
}

fn read_json(path: &Path) -> Result<Value> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_INPUT + 1)
        .read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 <= MAX_INPUT,
        "Evidence input exceeds 4 MiB",
    )?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn run(arguments: &[String]) -> Result<i32> {
    let [mode, clients, events] = arguments else {
        return Err("Use response-egress --validate CLIENTS.json EVENTS.json".into());
    };
    require(
        mode == "--validate",
        "Use response-egress --validate CLIENTS.json EVENTS.json",
    )?;
    let bundle = read_json(Path::new(clients))?;
    let events = read_json(Path::new(events))?;
    let result = report(&bundle["manifest"], &bundle["clients"], &events);
    println!("{result}");
    Ok(if result["status"] == "qualified-ownership" {
        0
    } else {
        2
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Value, Value, Value) {
        let schedule: Value = serde_json::from_str(SCHEDULE).unwrap();
        let manifest = json!({"schema":1,"run":"run-1","revision":"a".repeat(40),"artifact":"b".repeat(64),"binary_sha256":"c".repeat(64),"adapter":"cloudflare","sdk":"worker 0.8.5","runtime":{"unknown":"provider-unpublished"},"compatibility":{"date":"2026-10-07","flags":[]},"deployment":"isolated-test-deployment","telemetry_complete":true,"expected":schedule});
        let mut clients = Vec::new();
        let mut events = Vec::new();
        for probe in manifest["expected"].as_array().unwrap() {
            let name = probe["case"].as_str().unwrap();
            let bodyless = matches!(name, "head" | "204" | "205" | "304");
            let fallback = matches!(name, "expired" | "conversion-error");
            let normal = name == "normal" || name == "slow";
            let outcome = match name {
                "pending" | "expired" => "DeadlineExceeded",
                "source-error" => "SourceError",
                "conversion-error" => "ConversionError",
                "nonread" | "cancel" => "RequestCancelled",
                "disconnect" => "ClientDisconnected",
                _ => "HostHandoff",
            };
            let bytes = if normal {
                3072
            } else if name == "source-error" {
                1024
            } else {
                0
            };
            let polls = if normal {
                4
            } else if bodyless || fallback {
                0
            } else {
                2
            };
            let status = match name {
                "head" | "normal" | "slow" => json!(200),
                "204" => json!(204),
                "205" => json!(205),
                "304" => json!(304),
                "expired" => json!(504),
                _ => Value::Null,
            };
            clients.push(json!({"probe":probe["probe"],"case":name,"action":action(name),"ended":normal || bodyless,"bytes":if normal {3072} else {0},"status":status,"pauses":1,"result":if name == "disconnect" {"client-close"} else {"client-deadline"}}));
            for (sequence, kind) in ["source-drop", "terminal", "resource-drop", "trace-closed"]
                .into_iter()
                .enumerate()
            {
                let mut event = json!({"schema":1,"run":manifest["run"],"revision":manifest["revision"],"artifact":manifest["artifact"],"probe":probe["probe"],"case":name,"instance":"instance-1","sequence":sequence,"event":kind});
                if kind == "terminal" {
                    event.as_object_mut().unwrap().extend(json!({"outcome":outcome,"body_kind":if fallback {"Fallback"} else {"Application"},"bytes_written":bytes,"fallback":if fallback {json!("Completed")} else {Value::Null}}).as_object().unwrap().clone());
                }
                if kind == "trace-closed" {
                    event.as_object_mut().unwrap().extend(json!({"source_drops":1,"resource_drops":1,"terminals":1,"polls":polls,"polls_after_terminal":0}).as_object().unwrap().clone());
                }
                events.push(event);
            }
        }
        (manifest, Value::Array(clients), Value::Array(events))
    }

    #[test]
    fn canonical_schedule_preserves_each_original_case_and_recovery_identity() {
        let original = [
            "normal",
            "head",
            "204",
            "205",
            "304",
            "slow",
            "nonread",
            "disconnect",
            "cancel",
            "pending",
            "expired",
            "source-error",
            "conversion-error",
        ];
        let expected = original
            .iter()
            .enumerate()
            .flat_map(|(index, name)| {
                [
                    json!({"probe":format!("case-{index}"),"case":name}),
                    json!({"probe":format!("recovery-{index}"),"case":"normal"}),
                ]
            })
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::from_str::<Value>(SCHEDULE).unwrap(),
            json!(expected)
        );
        let (_, clients, events) = fixture();
        assert_eq!(clients.as_array().unwrap().len(), 26);
        assert_eq!(events.as_array().unwrap().len(), 104);
    }

    #[test]
    fn json_integer_spellings_and_utf16_text_lengths_preserve_validator_semantics() {
        for spelling in ["4.0", "4e0", "-0.0"] {
            assert!(integer(&serde_json::from_str(spelling).unwrap()).is_some());
        }
        for invalid in [
            json!(1.5),
            json!(-1),
            json!(MAX_SAFE_INTEGER + 1),
            json!(true),
        ] {
            assert!(integer(&invalid).is_none());
        }
        let (mut manifest, clients, events) = fixture();
        manifest["deployment"] = json!("\u{e9}".repeat(200));
        assert!(validate(&manifest, &clients, &events).is_ok());
        manifest["deployment"] = json!("\u{e9}".repeat(201));
        assert!(validate(&manifest, &clients, &events).is_err());
        manifest["deployment"] = json!("\u{1f600}".repeat(100));
        assert!(validate(&manifest, &clients, &events).is_ok());
        fn floating(value: &mut Value) {
            match value {
                Value::Number(number) => *value = json!(number.as_f64().unwrap()),
                Value::Array(values) => values.iter_mut().for_each(floating),
                Value::Object(fields) => fields.values_mut().for_each(floating),
                _ => {}
            }
        }
        let (mut manifest, mut clients, mut events) = fixture();
        floating(&mut manifest);
        floating(&mut clients);
        floating(&mut events);
        assert!(validate(&manifest, &clients, &events).is_ok());
    }

    #[test]
    fn complete_correlated_schedule_is_accepted_without_delivery_promotion() {
        let (manifest, clients, mut events) = fixture();
        assert_eq!(validate(&manifest, &clients, &events).unwrap(), 26);
        events.as_array_mut().unwrap().reverse();
        assert_eq!(validate(&manifest, &clients, &events).unwrap(), 26);
        assert_eq!(
            report(&manifest, &clients, &events)["capabilities_changed"],
            false
        );
    }

    #[test]
    fn coherent_subsets_and_changed_schedule_never_qualify() {
        for size in [0, 1, 2, 24, 25] {
            let (mut manifest, mut clients, mut events) = fixture();
            manifest["expected"].as_array_mut().unwrap().truncate(size);
            clients.as_array_mut().unwrap().truncate(size);
            events.as_array_mut().unwrap().truncate(4 * size);
            assert!(validate(&manifest, &clients, &events).is_err());
        }
        let (mut manifest, clients, events) = fixture();
        manifest["expected"][1]["probe"] = json!("renamed-recovery");
        assert!(validate(&manifest, &clients, &events).is_err());
    }

    #[test]
    fn missing_duplicated_truncated_and_mismatched_evidence_fails_closed() {
        type Mutation = fn(&mut Value, &mut Value, &mut Value);
        let mutations: &[Mutation] = &[
            |_, _, events| {
                events.as_array_mut().unwrap().remove(0);
            },
            |_, _, events| {
                let duplicate = events[1].clone();
                events.as_array_mut().unwrap().push(duplicate);
            },
            |_, _, events| {
                events.as_array_mut().unwrap().pop();
            },
            |_, _, events| {
                events[1]["revision"] = json!("c".repeat(40));
            },
            |_, _, events| {
                events[3]["polls_after_terminal"] = json!(1);
            },
            |_, _, events| {
                events[3]["source_drops"] = json!(2);
            },
            |manifest, _, _| {
                manifest["telemetry_complete"] = json!(false);
            },
            |_, clients, _| {
                clients.as_array_mut().unwrap().pop();
            },
            |_, clients, _| {
                clients[0]["action"] = json!("nonread");
            },
            |manifest, _, _| {
                manifest["adapter"] = json!("invented-target");
            },
            |manifest, _, _| {
                manifest.as_object_mut().unwrap().remove("binary_sha256");
            },
            |_, _, events| {
                events[1]["body_kind"] = json!("Fallback");
            },
            |_, _, events| {
                events[1]["bytes_written"] = json!(0);
            },
            |_, _, events| {
                events[3]["polls"] = json!(0);
            },
            |manifest, _, _| {
                manifest["runtime"] = json!(true);
            },
            |manifest, _, _| {
                manifest["compatibility"] = json!(true);
            },
            |manifest, _, _| {
                manifest["binary_sha256"] = json!([manifest["binary_sha256"]]);
            },
            |manifest, _, events| {
                manifest.as_object_mut().unwrap().remove("run");
                for event in events.as_array_mut().unwrap() {
                    event.as_object_mut().unwrap().remove("run");
                }
            },
            |manifest, _, _| {
                manifest["expected"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"probe":"extra-probe","case":"normal"}));
            },
            |manifest, clients, events| {
                for probe in manifest["expected"].as_array_mut().unwrap() {
                    probe.as_object_mut().unwrap().remove("probe");
                }
                for client in clients.as_array_mut().unwrap() {
                    client.as_object_mut().unwrap().remove("probe");
                }
                for event in events.as_array_mut().unwrap() {
                    event.as_object_mut().unwrap().remove("probe");
                }
            },
            |_, clients, _| {
                clients[1] = clients[0].clone();
            },
            |_, _, events| {
                events[0]["instance"] = json!("another-instance");
            },
            |_, _, events| {
                events[3]["polls"] = json!(MAX_SAFE_INTEGER + 1);
            },
            |_, _, events| {
                events[1].as_object_mut().unwrap().remove("fallback");
            },
        ];
        for (index, mutate) in mutations.iter().enumerate() {
            let (mut manifest, mut clients, mut events) = fixture();
            mutate(&mut manifest, &mut clients, &mut events);
            assert!(
                validate(&manifest, &clients, &events).is_err(),
                "mutation {index}"
            );
        }
    }

    #[test]
    fn each_case_retains_activity_classification_and_interruption_requirements() {
        let (manifest, mut clients, events) = fixture();
        clients[20]["status"] = Value::Null;
        assert!(validate(&manifest, &clients, &events).is_ok());
        for probe in 0..26 {
            for field in ["outcome", "body_kind", "bytes_written"] {
                let (manifest, clients, mut events) = fixture();
                events[4 * probe + 1][field] = json!("invalid");
                assert!(validate(&manifest, &clients, &events).is_err());
            }
        }
        for (probe, field, value) in [
            (6, "ended", json!(true)),
            (7, "result", json!("end")),
            (8, "result", json!("end")),
            (5, "pauses", json!(0)),
        ] {
            let (manifest, mut clients, events) = fixture();
            clients[2 * probe][field] = value;
            assert!(validate(&manifest, &clients, &events).is_err());
        }
    }

    #[test]
    fn input_limits_and_command_shape_are_enforced_without_runtime_setup() {
        assert!(run(&[]).is_err());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("oversized.json");
        std::fs::write(&path, vec![b' '; MAX_INPUT as usize + 1]).unwrap();
        assert!(read_json(&path).unwrap_err().to_string().contains("4 MiB"));
    }
}
