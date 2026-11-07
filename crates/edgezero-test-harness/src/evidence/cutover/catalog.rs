use super::*;
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) const UNPROMOTED: &str = "Follow-up not promoted or confirmed present";
const FASTLY_UNIT: &str = "scripts/run_test_nonzero.sh --inventory unit/fastly cargo test --offline --locked -p edgezero-adapter-fastly --no-default-features --features fastly,test-utils --target wasm32-wasip1 --lib";
const FASTLY_CI: &str = "scripts/test_integration.sh --adapter fastly --suite ci --output crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/redirect/fastly";
const ISOLATION: &[&str] = &[
    "response::tests::body_created_batches_end_at_egress_without_closing_callback_work",
    "response::tests::pending_body_write_deadline_releases_only_response_work",
    "outbound::scope::tests::response_scope_closes_only_response_created_work",
    "lifecycle::tests::callback_scope_exit_releases_unpolled_send_and_begin_before_cross_scope_polling",
    "lifecycle::tests::callback_scope_exit_releases_retained_batch_driver",
];
const CALLBACK: &[&str] = &[
    "callback_cleanup::tests::native_operation_cleanup_requires_real_pending_ownership_and_release",
    "callback_cleanup::tests::native_operation_evidence_remains_bounded_with_all_five_roles",
    "callback_cleanup::tests::callback_mode_accepts_only_exact_case_tokens",
];
const FOCUS: &[&str] = &[
    "outbound::image_optimizer::tests::image_optimizer_quality_accepts_exactly_zero_through_one_hundred",
    "outbound::image_optimizer::tests::image_optimizer_dimensions_and_crop_reject_zero_without_invented_maxima",
    "outbound::image_optimizer::tests::image_optimizer_crop_checks_both_public_percent_payloads",
    "image_optimizer_focus_valid",
    "image_optimizer_focus_x_only",
    "image_optimizer_focus_y_only",
    "image_optimizer_focus_smart_offsets",
];

fn section_name(line: &str) -> Option<&str> {
    let name = line.strip_prefix("### ")?.split_once('.')?.0;
    let suffix = name.strip_prefix("EZ-")?;
    (suffix == "Q1" || (!suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())))
        .then_some(name)
}

fn listed(line: &str) -> Option<&str> {
    if let Some(body) = line.strip_prefix("- ") {
        return Some(body);
    }
    let (number, body) = line.split_once(". ")?;
    (!number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())).then_some(body)
}

fn add_criterion(criteria: &mut Vec<Value>, section: &str, id: &str, body: &str) {
    let body = body.trim();
    criteria.push(json!({"section":section,"case_id":format!("{section}/acceptance/{id}"),"text":body,"source_digest":hash(body)}));
}

pub(super) fn acceptance_criteria(source: &str) -> Result<Value> {
    let mut criteria = Vec::new();
    let mut sections = Vec::new();
    let mut current = None;
    for (offset, line) in source.split_inclusive('\n').scan(0, |offset, line| {
        let start = *offset;
        *offset += line.len();
        Some((start, line))
    }) {
        if let Some(name) = section_name(line) {
            if let Some((name, start)) = current.take() {
                sections.push((name, &source[start..offset]));
            }
            current = Some((name, offset));
        }
    }
    if let Some((name, start)) = current {
        sections.push((name, &source[start..]));
    }
    for (section, contents) in sections {
        let mut body = String::new();
        let mut collecting = false;
        for line in contents.split_inclusive('\n') {
            if line.trim_end() == "#### Acceptance criteria" {
                collecting = true;
                continue;
            }
            if collecting
                && ["## ", "### ", "#### "]
                    .iter()
                    .any(|prefix| line.starts_with(prefix))
            {
                break;
            }
            if collecting {
                body.push_str(line);
            }
        }
        if !collecting {
            continue;
        }
        let (entries, end_to_end) = body
            .split_once("\n\nThe Trusted Server end-to-end check ")
            .map_or((body.as_str(), None), |(entries, end)| (entries, Some(end)));
        let mut entry = None::<String>;
        let mut index = 0;
        for line in entries.split_inclusive('\n') {
            if let Some(first) = listed(line) {
                if let Some(previous) = entry.take() {
                    index += 1;
                    add_criterion(&mut criteria, section, &format!("{index:02}"), &previous);
                }
                entry = Some(first.to_owned());
            } else if let Some(value) = &mut entry {
                value.push_str(line);
            }
        }
        if let Some(previous) = entry {
            index += 1;
            add_criterion(&mut criteria, section, &format!("{index:02}"), &previous);
        }
        if let Some(end) = end_to_end {
            add_criterion(
                &mut criteria,
                section,
                "ts-e2e",
                &format!("The Trusted Server end-to-end check {}", end.trim()),
            );
        }
    }
    require(!criteria.is_empty(), "source has no acceptance criteria")?;
    Ok(Value::Array(criteria))
}

fn requirement_ids(source: &str) -> HashSet<&str> {
    source
        .split("**")
        .filter(|part| {
            let Some((section, number)) = part
                .strip_prefix("EZ-")
                .and_then(|part| part.split_once('.'))
            else {
                return false;
            };
            (section == "Q1"
                || (!section.is_empty() && section.bytes().all(|byte| byte.is_ascii_digit())))
                && !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
        })
        .collect()
}

fn validate_approval(decision: &Value) -> Result<()> {
    require(
        nonempty(&decision["id"]),
        "approval needs a nonempty string identity",
    )?;
    require(
        matches!(
            text(&decision["status"]),
            "pending" | "approved" | "rejected"
        ),
        "invalid approval status",
    )?;
    let scope = decision["scope"]
        .as_array()
        .ok_or("approval needs explicit leg scope")?;
    require(
        !scope.is_empty()
            && scope
                .iter()
                .map(Value::to_string)
                .collect::<HashSet<_>>()
                .len()
                == scope.len(),
        "approval needs explicit leg scope",
    )?;
    if decision["status"] != "approved" {
        return Ok(());
    }
    let date = text(&decision["date"]);
    let valid_date = date.len() == 10
        && date.bytes().enumerate().all(|(index, byte)| {
            if [4, 7].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        });
    require(
        matches!(
            text(&decision["action"]),
            "promote" | "present" | "defer" | "requirement-change"
        ) && [
            "edgezero_approver",
            "consumer_approver",
            "evidence",
            "owner",
            "ci_owner",
        ]
        .iter()
        .all(|field| nonempty(&decision[field]))
            && valid_date,
        "approval needs attributed decision, date, owners and evidence",
    )?;
    if decision["action"] == "present" {
        require(
            nonempty(&decision["changelog"]),
            "present approval needs CHANGELOG confirmation",
        )?;
    }
    Ok(())
}

fn baseline(row: &Value) -> &str {
    let mut source = text(&row["source_id"]).split('/');
    let section = source.next().unwrap_or("");
    let number = source.nth(1).unwrap_or("");
    if section == "EZ-Q1" {
        return "gate";
    }
    if (section == "EZ-2"
        && number
            .parse::<u32>()
            .is_ok_and(|number| (16..=21).contains(&number)))
        || (section == "EZ-6" && number == "10")
    {
        return "follow-up";
    }
    if [
        "EZ-5/acceptance/12",
        "EZ-2/acceptance/10b",
        "EZ-2/acceptance/13b",
        "EZ-5/acceptance/09b",
        "EZ-5/acceptance/11b",
        "EZ-6/acceptance/23b",
        "EZ-8/acceptance/06b",
        "EZ-9/acceptance/16b",
    ]
    .contains(&text(&row["case_id"]))
        || (row["source_id"] == "EZ-1/acceptance/02"
            && text(&row["environment"]).ends_with("/http3"))
    {
        return "conditional";
    }
    if section
        .strip_prefix("EZ-")
        .and_then(|section| section.parse::<u32>().ok())
        .is_some_and(|number| number <= 9)
    {
        "blocking"
    } else {
        "follow-up"
    }
}

fn exact_proofs(row: &Value, expected: &[&str]) -> bool {
    let proofs = std::iter::once(text(&row["sentinel"]))
        .chain(
            row["additional_sentinels"]
                .as_array()
                .into_iter()
                .flatten()
                .map(text),
        )
        .collect::<Vec<_>>();
    proofs.len() == expected.len() && expected.iter().all(|name| proofs.contains(name))
}

fn validate_owner(row: &Value, cases: &HashMap<String, &Value>, id: &str) -> Result<()> {
    if row["implementation"] != "ready" {
        return Ok(());
    }
    let case = text(&row["case_id"]);
    let environment = text(&row["environment"]);
    if case == "EZ-2/acceptance/03" && environment == "viceroy" {
        let readback = cases
            .get("EZ-2/acceptance/22@unit/fastly")
            .copied()
            .unwrap_or(&Value::Null);
        require(row["sentinel"] == "fastly-budget-inventory" && row["command"] == "scripts/test_integration.sh --adapter fastly --suite outbound-lifetimes --output crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/lifetimes/viceroy"
            && row["artifact_path"] == "outbound/lifetimes/viceroy/events.jsonl" && row["companion_case"] == "EZ-2/acceptance/22@unit/fastly" && readback["command"] == FASTLY_UNIT
            && readback["artifact_path"] == "outbound/unit/fastly/raw.log" && readback["additional_sentinels"].as_array().is_some_and(|names| names.iter().any(|name| name == "outbound::fastly_impl::tests::b31_registered_transport_preserves_origin_and_wire_authority")), format!("{id}: baseline configuration coverage requires its native SDK TLS readback"))?;
    }
    if case == "EZ-6/acceptance/21" && environment == "unit/fastly" {
        require(
            row["command"] == FASTLY_UNIT
                && row["artifact_path"] == "outbound/redirect/fastly/runtime-unit-tests.log"
                && row.get("companion_case").is_none()
                && exact_proofs(row, ISOLATION),
            format!(
                "{id}: Fastly response-isolation proofs must retain all five tests in their runtime-unit owner"
            ),
        )?;
    }
    if case == "EZ-6/acceptance/23" && environment == "unit/fastly/both-wrappers" {
        require(
            row["companion_case"] == "EZ-6/acceptance/21@unit/fastly",
            format!("{id}: callback cleanup requires its Fastly response-isolation companion"),
        )?;
        require(
            row["command"] == FASTLY_CI
                && row["artifact_path"] == "outbound/redirect/fastly/fixture-check.log"
                && exact_proofs(row, CALLBACK),
            format!(
                "{id}: callback cleanup must retain all three fixture counter tests in their CI owner"
            ),
        )?;
    }
    if case == "EZ-6/acceptance/23" && environment == "viceroy" {
        require(
            row.get("companion_case").is_none(),
            format!("{id}: callback runtime cleanup must remain an independent proof"),
        )?;
    }
    if case == "EZ-6/acceptance/22" && ["axum", "viceroy", "workerd", "spin"].contains(&environment)
    {
        require(
            row["companion_case"] == "EZ-6/acceptance/22@unit/core",
            format!("{id}: private identity companion required"),
        )?;
    }
    if case == "EZ-4/acceptance/01"
        && ["unit/axum", "unit/fastly", "unit/cloudflare", "unit/spin"].contains(&environment)
    {
        require(
            row["companion_case"] == "EZ-4/acceptance/01@unit/core",
            format!("{id}: connect mapping requires its core companion"),
        )?;
    }
    if case == "EZ-3/acceptance/01" && environment == "unit/fastly" {
        require(
            row["command"] == FASTLY_UNIT
                && row["sentinel"]
                    == "outbound::fastly_impl::tests::get_head_empty_settlement_after_26ms_reaches_sdk_dispatch"
                && row["additional_sentinels"]
                    == json!([
                        "outbound::fastly_impl::tests::dispatch_accepts_live_budget_after_preparation",
                        "outbound::fastly_impl::tests::dispatch_budget_rejects_backwards_clock"
                    ])
                && row["artifact_path"] == "outbound/unit/fastly/raw.log",
            format!(
                "{id}: Fastly dispatch-retirement proofs must retain SDK dispatch, expiry and clock guards"
            ),
        )?;
    }
    if case == "EZ-8/acceptance/01" && environment == "unit/core" {
        require(
            row["command"] == "CARGO_NET_OFFLINE=true scripts/test_integration.sh check"
                && row["sentinel"] == "ez_8_acceptance_01_unit_core"
                && row["artifact_path"] == "outbound/unit/core/image-optimizer.log"
                && row["additional_sentinels"] == json!(FOCUS)
                && row.get("companion_case").is_none(),
            format!(
                "{id}: focus compiler owner must retain numeric, positive and all three negative proofs"
            ),
        )?;
    }
    if case == "EZ-8/acceptance/03" && environment == "unit/fastly" {
        require(
            row["companion_case"] == "EZ-8/acceptance/03@unit/core",
            format!("{id}: optimizer mapping requires its core companion"),
        )?;
    }
    Ok(())
}

pub(super) fn validate_manifest(
    manifest: &Value,
    source: &str,
    reviewed_legs: &str,
) -> Result<Value> {
    require(
        manifest["schema"].as_f64() == Some(1.0)
            && manifest["cases"]
                .as_array()
                .is_some_and(|cases| !cases.is_empty())
            && manifest["decisions"].is_array(),
        "invalid or empty inventory",
    )?;
    require(
        manifest["source_digest"] == hash(source),
        "source document changed; review acceptance preambles and inventory digests",
    )?;
    let criteria_value = acceptance_criteria(source)?;
    let criteria = unique(
        criteria_value.as_array().ok_or("invalid criteria")?,
        |value| text(&value["case_id"]).to_owned(),
    )?;
    let requirements = requirement_ids(source);
    let decision_rows = manifest["decisions"]
        .as_array()
        .ok_or("invalid decisions")?;
    let decisions = unique(decision_rows, |value| text(&value["id"]).to_owned())?;
    for decision in decision_rows {
        validate_approval(decision)?;
    }
    let rows = manifest["cases"].as_array().ok_or("invalid cases")?;
    let cases = unique(rows, key)?;
    let sorted = cases.keys().collect::<BTreeSet<_>>();
    require(
        hash(serde_json::to_vec(&sorted)?) == reviewed_legs,
        "reviewed leg set changed; split/environment policy review required",
    )?;
    let mut covered = HashSet::new();
    for row in rows {
        let id = key(row);
        let supplemental = row["source_kind"] == "normative"
            && row["source_id"] == "EZ-24.12"
            && row["case_id"] == "EZ-24/requirements/12";
        let normative = source
            .lines()
            .find_map(|line| line.strip_prefix("- **EZ-24.12** "));
        let extra = normative.map(|value| json!({"text":value,"source_digest":hash(value)}));
        let origin = if supplemental {
            extra.as_ref()
        } else {
            criteria.get(text(&row["source_id"])).copied()
        };
        let trimmed =
            text(&row["case_id"]).strip_suffix(|character: char| character.is_ascii_lowercase());
        require(
            origin.is_some()
                && (supplemental
                    || row["case_id"] == row["source_id"]
                    || trimmed == row["source_id"].as_str()),
            format!("{id}: unknown source criterion"),
        )?;
        let origin = origin.ok_or("missing criterion")?;
        require(
            row["source_digest"] == origin["source_digest"],
            format!("{id}: source digest changed; review required"),
        )?;
        require(
            row["expected"] == origin["text"],
            format!("{id}: expected assertion differs from source"),
        )?;
        require(
            row["requirement_ids"].as_array().is_some_and(|ids| {
                !ids.is_empty() && ids.iter().all(|id| requirements.contains(text(id)))
            }),
            format!("{id}: unknown requirement IDs"),
        )?;
        require(
            matches!(
                text(&row["applicability"]),
                "blocking" | "follow-up" | "conditional" | "gate"
            ),
            format!("{id}: invalid applicability"),
        )?;
        let baseline = baseline(row);
        require(
            !["blocking", "gate"].contains(&baseline) || row["applicability"] == baseline,
            format!("{id}: cannot downgrade blocking applicability"),
        )?;
        require(
            matches!(text(&row["implementation"]), "planned" | "ready"),
            format!("{id}: invalid implementation state"),
        )?;
        for field in [
            "environment",
            "owner",
            "task",
            "substep",
            "sentinel",
            "command",
            "artifact_path",
        ] {
            require(nonempty(&row[field]), format!("{id}: missing {field}"))?;
        }
        if let Some(additional) = row.get("additional_sentinels") {
            require(
                additional.as_array().is_some_and(|names| {
                    !names.is_empty()
                        && names
                            .iter()
                            .all(|name| name.as_str().is_some_and(rust_name))
                        && std::iter::once(text(&row["sentinel"]))
                            .chain(names.iter().map(text))
                            .collect::<HashSet<_>>()
                            .len()
                            == names.len() + 1
                }),
                format!("{id}: invalid additional_sentinels"),
            )?;
        }
        if let Some(companion) = row.get("companion_case") {
            let other = cases.get(text(companion)).copied();
            require(
                nonempty(companion)
                    && companion != &id
                    && other.is_some_and(|other| {
                        other["implementation"] == "ready"
                            && ["blocking", "gate"].contains(&text(&other["applicability"]))
                            && other.get("companion_case").is_none()
                    }),
                format!(
                    "{id}: companion must be a distinct ready mandatory proof without another companion"
                ),
            )?;
        }
        validate_owner(row, &cases, &id)?;
        require(
            safe_path(text(&row["artifact_path"])),
            format!("{id}: unsafe artifact path"),
        )?;
        if row["applicability"] == "conditional" {
            require(
                nonempty(&row["condition"]),
                format!("{id}: explicit condition required"),
            )?;
        }
        if row.get("approval") != Some(&Value::Null) {
            require(
                decisions
                    .get(text(&row["approval"]))
                    .is_some_and(|decision| {
                        decision["scope"]
                            .as_array()
                            .is_some_and(|scope| scope.iter().any(|value| value == &id))
                    }),
                format!("{id}: missing or incorrectly scoped approval"),
            )?;
        }
        covered.insert(text(&row["source_id"]));
    }
    for id in criteria.keys() {
        require(
            covered.contains(id.as_str()),
            format!("missing criterion {id}"),
        )?;
    }
    for decision in decision_rows {
        for scope in decision["scope"].as_array().ok_or("invalid scope")? {
            require(
                cases.contains_key(text(scope)),
                format!("approval references unknown leg {}", text(scope)),
            )?;
        }
    }
    Ok(
        json!({"criteria":criteria.len(),"legs":cases.len(),"planned":rows.iter().filter(|row| row["implementation"] == "planned").count()}),
    )
}

pub(super) fn safe_path(path: &str) -> bool {
    !std::path::Path::new(path).is_absolute() && !path.split(['/', '\\']).any(|part| part == "..")
}

pub(super) fn decision_for<'a>(
    row: &Value,
    decisions: &HashMap<String, &'a Value>,
) -> Option<&'a Value> {
    row.get("approval")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .and_then(|id| decisions.get(id).copied())
}

pub(super) fn disposition(
    row: &Value,
    decisions: &HashMap<String, &Value>,
) -> Result<&'static str> {
    if let Some(decision) = decision_for(row, decisions)
        && decision["status"] == "approved"
    {
        if ["promote", "present"].contains(&text(&decision["action"])) {
            return Ok("required");
        }
        require(
            !["blocking", "gate"].contains(&text(&row["applicability"]))
                || decision["action"] == "requirement-change",
            format!(
                "{}: blocking applicability requires an approved requirement change",
                key(row)
            ),
        )?;
        return Ok("deferred");
    }
    Ok(
        if ["blocking", "gate"].contains(&text(&row["applicability"])) {
            "required"
        } else {
            "deferred"
        },
    )
}

pub(super) fn coverage(manifest: &Value, source: &str, reviewed_legs: &str) -> Result<Value> {
    let validated = validate_manifest(manifest, source, reviewed_legs)?;
    let rows = manifest["cases"].as_array().ok_or("invalid cases")?;
    let decisions_rows = manifest["decisions"]
        .as_array()
        .ok_or("invalid decisions")?;
    let decisions = unique(decisions_rows, |value| text(&value["id"]).to_owned())?;
    let mut summary = json!({"required-local":{"ready":0,"planned":0},"required-external":{"ready":0,"planned":0},"conditional-pending":{"ready":0,"planned":0},"follow-up-not-promoted":{"ready":0,"planned":0},"approved-deferred":{"ready":0,"planned":0}});
    let mut legs = Vec::new();
    let mut pending = Vec::new();
    for row in rows {
        let decision = decision_for(row, &decisions);
        let external = ["hosted-", "consumer-"]
            .iter()
            .any(|prefix| text(&row["environment"]).starts_with(prefix));
        let required = disposition(row, &decisions)? == "required";
        let category = if required {
            if external {
                "required-external"
            } else {
                "required-local"
            }
        } else if decision.is_some_and(|decision| decision["status"] == "approved") {
            "approved-deferred"
        } else if row["applicability"] == "conditional" {
            "conditional-pending"
        } else {
            "follow-up-not-promoted"
        };
        let state = text(&row["implementation"]);
        summary[category][state] = json!(
            summary[category][state]
                .as_u64()
                .ok_or("invalid implementation count")?
                + 1
        );
        let id = key(row);
        if category == "required-local" && state == "planned" {
            pending.push(id.clone());
        }
        let condition = row
            .get("condition")
            .filter(|condition| !condition.is_null())
            .cloned()
            .unwrap_or_else(|| {
                if category == "follow-up-not-promoted" {
                    json!(UNPROMOTED)
                } else {
                    Value::Null
                }
            });
        legs.push(json!({"id":id,"category":category,"implementation":state,"location":if external {"external"} else {"local"},"approval":row["approval"],"approval_status":decision.map(|decision| decision["status"].clone()),"condition":condition}));
    }
    Ok(
        json!({"status":"inventory-coverage","qualified":false,"local_bindings_complete":pending.is_empty(),"criteria":validated["criteria"],"total_legs":legs.len(),"summary":summary,"local_pending":pending,"pending_decisions":decisions_rows.iter().filter(|decision| decision["status"] == "pending").map(|decision|decision["id"].clone()).collect::<Vec<_>>(),"legs":legs}),
    )
}

pub(super) fn sentinels(
    manifest: &Value,
    environment: &str,
    command: Option<&str>,
) -> Result<Value> {
    require(
        command.is_none_or(|command| !command.trim().is_empty()),
        "sentinel command must be nonempty",
    )?;
    let environments = environment.split(',').collect::<Vec<_>>();
    require(
        environments
            .iter()
            .all(|environment| !environment.is_empty())
            && environments.iter().collect::<HashSet<_>>().len() == environments.len(),
        "invalid or duplicate sentinel environment",
    )?;
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for environment in environments {
        let mut count = 0;
        for row in manifest["cases"]
            .as_array()
            .ok_or("invalid cases")?
            .iter()
            .filter(|row| {
                row["environment"] == environment
                    && row["implementation"] == "ready"
                    && command.is_none_or(|command| row["command"] == command)
            })
        {
            for name in std::iter::once(&row["sentinel"])
                .chain(row["additional_sentinels"].as_array().into_iter().flatten())
            {
                let value = text(name);
                require(
                    rust_name(value),
                    "ready test sentinels must be Rust test names",
                )?;
                count += 1;
                if seen.insert(value) {
                    names.push(name.clone());
                }
            }
        }
        require(
            count > 0,
            "no ready test sentinels for selected environment",
        )?;
    }
    Ok(json!(names))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_or_nonstring_approval_ids_cannot_waive_unrelated_blocking_legs() {
        let fixture = super::super::fixtures::Fixture::new();
        for id in [json!(""), json!(" "), Value::Null, json!(false), json!(42)] {
            let mut manifest = fixture.manifest.clone();
            manifest["decisions"] = json!([{"id":id,"status":"approved","action":"requirement-change","scope":[key(&manifest["cases"][0])],"edgezero_approver":"fixture","consumer_approver":"fixture","date":"2026-11-06","evidence":"fixture","owner":"fixture","ci_owner":"fixture"}]);
            assert!(
                validate_manifest(
                    &manifest,
                    &fixture.context.source,
                    &fixture.context.reviewed_legs
                )
                .is_err(),
                "{id}"
            );
        }
        assert_eq!(
            disposition(&fixture.manifest["cases"][1], &HashMap::new()).unwrap(),
            "required"
        );
    }

    #[test]
    fn decimal_schema_numbers_have_the_same_json_meaning() {
        let fixture = super::super::fixtures::Fixture::new();
        let mut manifest = fixture.manifest.clone();
        manifest["schema"] = json!(1.0);
        assert!(
            validate_manifest(
                &manifest,
                &fixture.context.source,
                &fixture.context.reviewed_legs
            )
            .is_ok()
        );
    }

    #[test]
    fn criteria_preserve_complete_multiline_text_and_numbering() {
        let source = "### EZ-9. Redirects\n#### Acceptance criteria\n\n- Unit. First.\n  A second line.\n- Unit. Second.\n\n#### Out of scope\n";
        let criteria = acceptance_criteria(source).unwrap();
        assert_eq!(
            criteria,
            json!([
                {"section":"EZ-9","case_id":"EZ-9/acceptance/01","text":"Unit. First.\n  A second line.","source_digest":hash("Unit. First.\n  A second line.")},
                {"section":"EZ-9","case_id":"EZ-9/acceptance/02","text":"Unit. Second.","source_digest":hash("Unit. Second.")}
            ])
        );
    }
}
