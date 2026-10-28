use super::{Result, require};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug)]
pub struct TimerLeg {
    pub source: &'static str,
    pub budget_ms: u64,
    pub repetition: usize,
}

impl TimerLeg {
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.source, self.budget_ms, self.repetition)
    }
}

pub fn timer_legs() -> Vec<TimerLeg> {
    let mut legs = Vec::with_capacity(72);
    for repetition in 0..8 {
        for budget_ms in [1, 100, 500] {
            for source in ["native-sleep", "empty-select", "wasi-sleep"] {
                legs.push(TimerLeg {
                    source,
                    budget_ms,
                    repetition,
                });
            }
        }
    }
    legs
}

pub fn validate_timer_controls(probes: &[Value], revision: &str) -> Result<()> {
    require(
        revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid expected timer revision",
    )?;
    let legs = timer_legs();
    require(
        probes.len() == legs.len(),
        "missing timer control inventory",
    )?;
    let mut identities = HashSet::new();
    let mut tokens = HashSet::new();
    for probe in probes {
        let leg = legs
            .iter()
            .find(|leg| {
                probe["source"] == leg.source
                    && probe["budget_ms"].as_u64() == Some(leg.budget_ms)
                    && probe["repetition"].as_u64() == Some(leg.repetition as u64)
            })
            .ok_or("unknown timer control leg")?;
        let token = probe["token"]
            .as_str()
            .ok_or("missing timer control token")?;
        require(
            probe["schema"].as_u64() == Some(1)
                && probe["event"] == "readiness-timer-control"
                && probe["revision"] == revision
                && probe["qualification"].as_bool() == Some(false)
                && !token.is_empty()
                && token.len() <= 128
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && identities.insert(leg.key())
                && tokens.insert(token),
            "invalid/duplicate timer control identity",
        )?;
        let elapsed = probe["elapsed_us"]
            .as_u64()
            .ok_or("missing timer elapsed")?;
        let budget = leg.budget_ms * 1000;
        require(
            (budget..=12_000_000).contains(&elapsed),
            "early or unbounded timer control",
        )?;
        require(
            probe["overrun_us"].as_u64() == Some(elapsed - budget)
                && probe["within_five_ms"].as_bool() == Some(elapsed <= budget + 5000),
            "incorrect timer overrun or lateness classification",
        )?;
        let primitive_end = if let Some(endpoint) = probe.get("measurement_end") {
            require(
                endpoint == "primitive-return",
                "unknown timer measurement endpoint",
            )?;
            true
        } else {
            false
        };
        if leg.source == "empty-select" {
            let entered = probe["abi_enter_us"]
                .as_u64()
                .ok_or("missing timer ABI entry")?;
            let returned = probe["abi_return_us"]
                .as_u64()
                .ok_or("missing timer ABI return")?;
            require(
                entered <= returned
                    && returned <= elapsed
                    && returned - entered >= budget
                    && (!primitive_end || returned == elapsed)
                    && probe["abi_status"] == "FastlyStatus::OK"
                    && probe.get("selected").is_some_and(Value::is_null),
                "invalid timer ABI timestamps, status or selection",
            )?;
        } else {
            require(
                ["abi_enter_us", "abi_return_us", "abi_status", "selected"]
                    .iter()
                    .all(|field| probe.get(field).is_none()),
                "sleep control contains ABI observations",
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::TimerLeg;
    use crate::evidence::{timer_legs, validate_timer_controls};
    use serde_json::{Value, json};
    use std::collections::HashSet;

    const REVISION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn matrix() -> Vec<Value> {
        let mut probes = Vec::new();
        for repetition in 0..8 {
            for budget_ms in [1, 100, 500] {
                for source in ["native-sleep", "empty-select", "wasi-sleep"] {
                    let elapsed = budget_ms * 1000;
                    let mut probe = json!({
                        "schema":1, "event":"readiness-timer-control", "source":source,
                        "budget_ms":budget_ms, "repetition":repetition,
                        "token":format!("{source}-{budget_ms}-{repetition}"),
                        "revision":REVISION, "qualification":false,
                        "elapsed_us":elapsed, "overrun_us":0, "within_five_ms":true,
                    });
                    if source == "empty-select" {
                        probe["abi_enter_us"] = json!(0);
                        probe["abi_return_us"] = json!(elapsed);
                        probe["abi_status"] = json!("FastlyStatus::OK");
                        probe["selected"] = Value::Null;
                    }
                    probes.push(probe);
                }
            }
        }
        probes
    }

    fn elapsed(probe: &mut Value, us: u64) {
        let budget = probe["budget_ms"].as_u64().unwrap() * 1000;
        probe["elapsed_us"] = json!(us);
        probe["overrun_us"] = json!(us - budget);
        probe["within_five_ms"] = json!(us <= budget + 5000);
        if probe["source"] == "empty-select" {
            probe["abi_return_us"] = json!(us);
        }
    }

    #[test]
    fn timer_inventory_is_exactly_seventy_two_unique_legs() {
        let legs = timer_legs();
        assert_eq!(legs.len(), 72);
        let actual: HashSet<_> = legs.iter().map(TimerLeg::key).collect();
        let expected: HashSet<_> = matrix()
            .iter()
            .map(|probe| {
                format!(
                    "{}/{}/{}",
                    probe["source"].as_str().unwrap(),
                    probe["budget_ms"],
                    probe["repetition"]
                )
            })
            .collect();
        assert_eq!(actual, expected);
        for leg in legs {
            assert_eq!(
                leg.key(),
                format!("{}/{}/{}", leg.source, leg.budget_ms, leg.repetition)
            );
        }
    }

    #[test]
    fn timer_accepts_complete_inventory() {
        validate_timer_controls(&matrix(), REVISION).unwrap();
    }

    #[test]
    fn timer_replays_retained_runtime_controls_without_hiding_lateness() {
        use std::{
            collections::BTreeMap,
            fs::File,
            io::{BufRead, BufReader},
        };
        let path = crate::root()
            .join("../../../docs/audits/evidence/2026-10-23-fastly-timer-controls-probes.jsonl");
        let mut runs: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for line in BufReader::new(File::open(path).unwrap()).lines() {
            let row: Value = serde_json::from_str(&line.unwrap()).unwrap();
            if row["kind"] == "timer-control" {
                runs.entry(row["run"].as_str().unwrap().to_owned())
                    .or_default()
                    .push(row["probe"].clone());
            }
        }
        assert_eq!(runs.len(), 4);
        for (name, late, late_source) in [
            ("repaired", 5, "native-sleep"),
            ("stock", 7, "native-sleep"),
            ("repaired-reviewed", 0, "native-sleep"),
            ("stock-reviewed", 1, "wasi-sleep"),
        ] {
            let probes = &runs[name];
            validate_timer_controls(probes, "77819f712ad971ada5494c563dd86a3b64a2afa6").unwrap();
            assert_eq!(probes.len(), 72);
            let retained_late: Vec<_> = probes
                .iter()
                .filter(|p| p["within_five_ms"] == false)
                .collect();
            assert_eq!(retained_late.len(), late);
            assert!(retained_late.iter().all(|p| p["source"] == late_source));
            assert!(probes.iter().all(|p| p["qualification"] == false));
            if name.ends_with("-reviewed") {
                assert!(
                    probes
                        .iter()
                        .all(|p| p["measurement_end"] == "primitive-return")
                );
                for probe in probes.iter().filter(|p| p["source"] == "empty-select") {
                    assert_eq!(probe["elapsed_us"], probe["abi_return_us"]);
                }
            }
        }
    }

    #[test]
    fn timer_accepts_late_observations_without_relabelling_them() {
        let mut probes = matrix();
        for probe in &mut probes {
            let budget = probe["budget_ms"].as_u64().unwrap() * 1000;
            elapsed(probe, budget + 5001);
        }
        let retained = probes.clone();
        validate_timer_controls(&probes, REVISION).unwrap();
        assert_eq!(probes, retained);
        assert!(probes.iter().all(|probe| probe["within_five_ms"] == false));
    }

    #[test]
    fn timer_accepts_exact_tolerance_and_containment_boundaries() {
        for overrun in [0, 5000, 5001] {
            let mut probes = matrix();
            for probe in &mut probes {
                let budget = probe["budget_ms"].as_u64().unwrap() * 1000;
                elapsed(probe, budget + overrun);
            }
            validate_timer_controls(&probes, REVISION).unwrap();
        }
        let mut probes = matrix();
        for probe in &mut probes {
            elapsed(probe, 12_000_000);
        }
        probes[1]["abi_enter_us"] = json!(12_000_000 - 1000);
        probes[0]["token"] = json!("a".repeat(128));
        validate_timer_controls(&probes, REVISION).unwrap();
    }

    #[test]
    fn timer_rejects_missing_extra_or_duplicate_inventory() {
        assert!(validate_timer_controls(&[], REVISION).is_err());
        let baseline = matrix();
        for missing in 0..baseline.len() {
            let mut probes = baseline.clone();
            probes.remove(missing);
            assert!(validate_timer_controls(&probes, REVISION).is_err());
        }
        let mut probes = baseline.clone();
        probes.push(probes[0].clone());
        assert!(validate_timer_controls(&probes, REVISION).is_err());
        let mut probes = baseline;
        probes[1] = probes[0].clone();
        probes[1]["token"] = json!("unique-duplicate-leg-token");
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_invalid_or_foreign_identity() {
        let mutations = [
            ("schema", json!(2)),
            ("event", json!("readiness-sample")),
            ("source", json!("unknown")),
            ("budget_ms", json!(0)),
            ("budget_ms", json!(493)),
            ("budget_ms", json!("1")),
            ("budget_ms", json!(1.0)),
            ("repetition", json!(8)),
            ("repetition", json!(-1)),
            ("repetition", json!(0.0)),
            ("revision", json!("b".repeat(40))),
            ("revision", json!("a".repeat(39))),
            ("revision", json!("g".repeat(40))),
            ("qualification", json!(true)),
            ("qualification", json!("false")),
            ("token", json!("")),
            ("token", json!("bad/token")),
            ("token", json!("bad_token")),
            ("token", json!("bad token")),
            ("token", json!("bad\n")),
            ("token", json!("a".repeat(129))),
            ("token", json!(123)),
            ("token", json!("native-sleep-100-0")),
        ];
        for (field, value) in mutations {
            let mut probes = matrix();
            probes[0][field] = value.clone();
            assert!(
                validate_timer_controls(&probes, REVISION).is_err(),
                "accepted {field}={value}"
            );
        }
        let mut probes = matrix();
        probes[0]["token"] = json!("\u{e9}");
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_invalid_expected_revision() {
        for revision in ["", &"a".repeat(39), &"g".repeat(40), &"a".repeat(41)] {
            assert!(validate_timer_controls(&matrix(), revision).is_err());
        }
    }

    #[test]
    fn timer_rejects_missing_required_metadata() {
        for field in [
            "schema",
            "event",
            "source",
            "budget_ms",
            "repetition",
            "token",
            "revision",
            "qualification",
            "elapsed_us",
            "overrun_us",
            "within_five_ms",
        ] {
            let mut probes = matrix();
            probes[0].as_object_mut().unwrap().remove(field);
            assert!(
                validate_timer_controls(&probes, REVISION).is_err(),
                "accepted missing {field}"
            );
        }
    }

    #[test]
    fn timer_rejects_forged_or_unbounded_elapsed_accounting() {
        let mutations = [
            ("elapsed_us", json!(999)),
            ("elapsed_us", json!(12_000_001)),
            ("elapsed_us", json!(-1)),
            ("elapsed_us", json!(1000.0)),
            ("elapsed_us", json!("1000")),
            ("overrun_us", json!(1)),
            ("overrun_us", json!(-1)),
            ("overrun_us", json!(0.0)),
            ("within_five_ms", json!(false)),
            ("within_five_ms", json!("true")),
        ];
        for (field, value) in mutations {
            let mut probes = matrix();
            probes[0][field] = value.clone();
            assert!(
                validate_timer_controls(&probes, REVISION).is_err(),
                "accepted {field}={value}"
            );
        }
        let mut probes = matrix();
        elapsed(&mut probes[0], 6001);
        probes[0]["within_five_ms"] = json!(true);
        assert!(validate_timer_controls(&probes, REVISION).is_err());
        probes[0]["within_five_ms"] = json!(false);
        probes[0]["overrun_us"] = json!(5000);
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_invalid_or_missing_empty_select_abi_observations() {
        let mutations = [
            ("abi_enter_us", json!(-1)),
            ("abi_enter_us", json!(1001)),
            ("abi_enter_us", json!(0.0)),
            ("abi_return_us", json!(1001)),
            ("abi_return_us", json!("1000")),
            ("abi_status", json!("FastlyStatus::INVAL")),
            ("abi_status", Value::Null),
            ("selected", json!(0)),
            ("selected", json!(u32::MAX)),
            ("selected", json!("null")),
        ];
        for (field, value) in mutations {
            let mut probes = matrix();
            probes[1][field] = value.clone();
            assert!(
                validate_timer_controls(&probes, REVISION).is_err(),
                "accepted {field}={value}"
            );
        }
        for field in ["abi_enter_us", "abi_return_us", "abi_status", "selected"] {
            let mut probes = matrix();
            probes[1].as_object_mut().unwrap().remove(field);
            assert!(
                validate_timer_controls(&probes, REVISION).is_err(),
                "accepted missing {field}"
            );
        }
        let mut probes = matrix();
        probes[1]["abi_enter_us"] = json!(999);
        probes[1]["abi_return_us"] = json!(998);
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_a_declared_primitive_end_that_includes_later_work() {
        let mut probes = matrix();
        elapsed(&mut probes[1], 1100);
        probes[1]["abi_return_us"] = json!(1000);
        probes[1]["measurement_end"] = json!("primitive-return");
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_an_abi_wait_shorter_than_its_positive_timeout() {
        let mut probes = matrix();
        probes[1]["abi_return_us"] = json!(999);
        assert!(validate_timer_controls(&probes, REVISION).is_err());
    }

    #[test]
    fn timer_rejects_abi_fields_on_native_and_wasi_sleep() {
        for index in [0, 2] {
            for field in ["abi_enter_us", "abi_return_us", "abi_status", "selected"] {
                let mut probes = matrix();
                probes[index][field] = Value::Null;
                assert!(
                    validate_timer_controls(&probes, REVISION).is_err(),
                    "accepted {field} on {}",
                    probes[index]["source"]
                );
            }
        }
    }
}
