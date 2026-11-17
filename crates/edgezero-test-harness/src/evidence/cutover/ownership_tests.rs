use super::*;
use quote::ToTokens;
use std::{collections::HashMap, fs};
use syn::{Expr, Lit, visit::Visit};

#[derive(Default)]
struct Functions {
    definitions: HashMap<String, Vec<String>>,
    owners: Vec<String>,
}

impl<'ast> Visit<'ast> for Functions {
    fn visit_item_fn(&mut self, function: &'ast syn::ItemFn) {
        self.definitions
            .entry(function.sig.ident.to_string())
            .or_default()
            .push(format!(
                "{} {}",
                self.owners.join(" "),
                function.to_token_stream()
            ));
        syn::visit::visit_item_fn(self, function);
    }
    fn visit_impl_item_fn(&mut self, function: &'ast syn::ImplItemFn) {
        self.definitions
            .entry(function.sig.ident.to_string())
            .or_default()
            .push(format!(
                "{} {}",
                self.owners.join(" "),
                function.to_token_stream()
            ));
        syn::visit::visit_impl_item_fn(self, function);
    }
    fn visit_item_mod(&mut self, module: &'ast syn::ItemMod) {
        self.owners.push(format!(
            "{} {}",
            module
                .attrs
                .iter()
                .map(ToTokens::to_token_stream)
                .map(|tokens| tokens.to_string())
                .collect::<Vec<_>>()
                .join(" "),
            module.ident
        ));
        syn::visit::visit_item_mod(self, module);
        self.owners.pop();
    }
    fn visit_item_impl(&mut self, implementation: &'ast syn::ItemImpl) {
        self.owners.push(format!(
            "{} {}",
            implementation
                .attrs
                .iter()
                .map(ToTokens::to_token_stream)
                .map(|tokens| tokens.to_string())
                .collect::<Vec<_>>()
                .join(" "),
            implementation.self_ty.to_token_stream()
        ));
        syn::visit::visit_item_impl(self, implementation);
        self.owners.pop();
    }
}

fn function(source: &str, name: &str) -> Result<String> {
    let mut functions = Functions::default();
    functions.visit_file(&syn::parse_file(source)?);
    let found = functions
        .definitions
        .get(name)
        .filter(|found| found.len() == 1)
        .ok_or("missing or ambiguous executable function")?;
    Ok(found[0].clone())
}

fn strings(expression: &Expr) -> Option<Vec<String>> {
    let expression = match expression {
        Expr::Reference(reference) => &*reference.expr,
        value => value,
    };
    let Expr::Array(array) = expression else {
        return None;
    };
    array
        .elems
        .iter()
        .map(|element| match element {
            Expr::Lit(value) => match &value.lit {
                Lit::Str(value) => Some(value.value()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

#[derive(Default)]
struct Guards(Vec<(Vec<String>, Vec<String>)>);

impl<'ast> Visit<'ast> for Guards {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Expr::Path(path) = &*call.func
            && path.path.is_ident("guarded")
            && call.args.len() == 2
            && let (Some(options), Some(arguments)) =
                (strings(&call.args[0]), strings(&call.args[1]))
        {
            self.0.push((options, arguments));
        }
        syn::visit::visit_expr_call(self, call);
    }
}

fn guarded_commands(source: &str) -> Result<Vec<String>> {
    let mut guards = Guards::default();
    guards.visit_file(&syn::parse_file(source)?);
    Ok(guards
        .0
        .into_iter()
        .map(|(options, arguments)| {
            format!(
                "scripts/run_test_nonzero.sh {} cargo {}",
                options.join(" "),
                arguments.join(" ")
            )
        })
        .collect())
}

pub(super) const AXUM_NATIVE: &str = "scripts/run_test_nonzero.sh --inventory axum/native-tcp,unit/axum,axum/http1,axum cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --all-targets";
pub(super) const AXUM_QUALIFICATION: &str = "scripts/run_test_nonzero.sh --include-ignored --inventory axum/http1-chunked cargo test --offline --locked -p edgezero-adapter-axum --no-default-features --features axum,test-utils --lib connection::tests::";
pub(super) const AXUM_CORRECTNESS: &str =
    "connection::tests::pending_chunked_forward_correctness_over_tcp";

fn explicit_qualification_owner(row: &Value) -> bool {
    row["case_id"] == "EZ-1/acceptance/06"
        && row["environment"] == "axum/http1-chunked"
        && row["command"] == AXUM_QUALIFICATION
        && row["artifact_path"] == "outbound/redirect/axum/qualification-tests.log"
        && row["sentinel"]
            == "connection::tests::pending_chunked_forward_preserves_original_deadlines_over_tcp"
}

#[test]
fn axum_explicit_qualification_owner_rejects_other_commands_and_bindings() {
    let root = crate::repository_root();
    let inventory = io::read_json(&root.join(INVENTORY)).unwrap();
    let row = inventory["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| key(row) == "EZ-1/acceptance/06@axum/http1-chunked")
        .unwrap();
    assert!(explicit_qualification_owner(row));
    for command in [
        AXUM_NATIVE.to_owned(),
        AXUM_QUALIFICATION.replace("--include-ignored ", ""),
        AXUM_QUALIFICATION.replace("connection::tests::", AXUM_CORRECTNESS),
        format!("{AXUM_QUALIFICATION} -- --skip strict"),
    ] {
        let mut changed = row.clone();
        changed["command"] = json!(command);
        assert!(!explicit_qualification_owner(&changed));
    }
    for field in ["case_id", "environment", "artifact_path", "sentinel"] {
        let mut changed = row.clone();
        changed[field] = json!("other");
        assert!(!explicit_qualification_owner(&changed));
    }
}

#[test]
fn axum_timing_qualification_is_separate_from_required_native_correctness() {
    let root = crate::repository_root();
    let inventory = io::read_json(&root.join(INVENTORY)).unwrap();
    let row = inventory["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| key(row) == "EZ-1/acceptance/06@axum/http1-chunked")
        .unwrap();
    assert_eq!(row["command"], AXUM_QUALIFICATION);
    assert_eq!(
        row["artifact_path"],
        "outbound/redirect/axum/qualification-tests.log"
    );
    assert_eq!(row["source_id"], "EZ-1/acceptance/06");
    assert_eq!(
        row["source_digest"],
        "c5cb36d9fd126f56051dad7a2ab8a87c64d728db1782bb9ba8ad31b6b8bddac8"
    );
    assert_eq!(row["applicability"], "blocking");
    assert!(text(&row["expected"]).contains("within 205 ms"));
    assert!(text(&row["expected"]).contains("within 105 ms"));
    let qualification =
        catalog::sentinels(&inventory, "axum/http1-chunked", Some(AXUM_QUALIFICATION)).unwrap();
    assert_eq!(
        qualification,
        json!([
            "connection::tests::pending_chunked_forward_preserves_original_deadlines_over_tcp",
            "connection::tests::forwarding_pending_witness_does_not_count_ready_source_errors",
            "connection::tests::timed_error_reader_rejects_oversized_heads_and_trailing_bytes"
        ])
    );
    let native = catalog::sentinels(
        &inventory,
        "axum/native-tcp,unit/axum,axum/http1,axum",
        Some(AXUM_NATIVE),
    )
    .unwrap();
    assert!(
        native
            .as_array()
            .unwrap()
            .contains(&json!(AXUM_CORRECTNESS))
    );
    assert!(!native.as_array().unwrap().contains(&row["sentinel"]));
    let source =
        fs::read_to_string(root.join("crates/edgezero-test-harness/src/runners/ci/platforms.rs"))
            .unwrap();
    let commands = guarded_commands(&source).unwrap();
    assert!(commands.iter().any(|command| command == AXUM_NATIVE));
    assert!(!commands.iter().any(|command| command == AXUM_QUALIFICATION));
    let source =
        fs::read_to_string(root.join("crates/edgezero-adapter-axum/src/connection.rs")).unwrap();
    let correctness = function(&source, "pending_chunked_forward_correctness_over_tcp").unwrap();
    assert!(!correctness.contains("ignore"));
    assert!(
        correctness.contains("pending_chunked_forward_over_tcp (Duration :: from_millis (50))")
    );
    let strict = function(
        &source,
        "pending_chunked_forward_preserves_original_deadlines_over_tcp",
    )
    .unwrap();
    assert!(strict.contains("ignore = \"explicit timing qualification\""));
    assert!(strict.contains("pending_chunked_forward_over_tcp (Duration :: from_millis (5))"));
}

#[test]
fn axum_qualification_guard_rejects_correctness_only_and_missing_strict_passes() {
    let root = crate::repository_root();
    let inventory = io::read_json(&root.join(INVENTORY)).unwrap();
    let names =
        catalog::sentinels(&inventory, "axum/http1-chunked", Some(AXUM_QUALIFICATION)).unwrap();
    let names = names
        .as_array()
        .unwrap()
        .iter()
        .map(|name| text(name).to_owned())
        .collect::<Vec<_>>();
    let listed = names
        .iter()
        .map(|name| format!("{name}: test"))
        .collect::<Vec<_>>()
        .join("\n");
    let strict = &names[0];
    let all_passes = names
        .iter()
        .map(|name| format!("test {name} ... ok\n"))
        .collect::<String>();
    for (passes, success) in [
        (all_passes.clone(), true),
        (format!("test {AXUM_CORRECTNESS} ... ok\n"), false),
        (
            all_passes.replace(&format!("test {strict} ... ok\n"), ""),
            false,
        ),
        (
            all_passes.replace(
                &format!("test {strict} ... ok"),
                &format!("test {strict} ... ignored"),
            ),
            false,
        ),
    ] {
        let output = std::process::Command::new("bash")
            .current_dir(&root)
            .arg("scripts/run_test_nonzero.sh")
            .arg("--include-ignored")
            .arg(names.join("\n"))
            .args(["bash", "-c", "if [[ \"$*\" == *--list* ]]; then printf '%s\\n' \"$LISTED_TESTS\"; else printf '%s\\n' \"$TEST_OUTPUT\"; fi", "synthetic-tests"])
            .env("LISTED_TESTS", &listed)
            .env("TEST_OUTPUT", format!("{passes}test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s"))
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if !success {
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains(&format!("required sentinel test not passed: {strict}"))
            );
        }
    }
}

fn workflow(source: &str) -> Result<Value> {
    let parsed: serde_yaml_ng::Value = serde_yaml_ng::from_str(source)?;
    let value = serde_json::to_value(parsed)?;
    require(value["jobs"].is_object(), "workflow jobs must be a mapping")?;
    Ok(value)
}

fn automatic_ci(value: &Value, adapter: &str) -> bool {
    let (job, dimension, selector) = if adapter == "axum" {
        ("host-tests", "suite", "axum")
    } else {
        ("adapter-wasm-tests", "include", adapter)
    };
    let job = &value["jobs"][job];
    if job.get("if").is_some() || job.get("continue-on-error").is_some() {
        return false;
    }
    let matrix = &job["strategy"]["matrix"][dimension];
    let member = matrix.as_array().is_some_and(|rows| {
        rows.iter().any(|row| {
            if adapter == "axum" {
                row == selector
            } else {
                row["adapter"] == selector
            }
        })
    });
    let selected = if adapter == "axum" {
        "axum"
    } else {
        "${{ matrix.adapter }}"
    };
    let expected = format!(
        "run=\"$GITHUB_WORKSPACE/crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/redirect/{selected}\"\nCARGO_NET_OFFLINE=true ./scripts/test_integration.sh --adapter {selected} --suite ci --output \"$run\""
    );
    member
        && job["steps"].as_array().is_some_and(|steps| {
            steps
                .iter()
                .filter(|step| {
                    step.get("continue-on-error").is_none()
                        && (if adapter == "axum" {
                            step["if"] == "matrix.suite == 'axum'"
                        } else {
                            step.get("if").is_none()
                        })
                        && text(&step["run"]).trim() == expected
                })
                .count()
                == 1
        })
}

fn strict_budget(row: &Value) -> bool {
    row["environment"] == "viceroy"
        && [
            "EZ-2/acceptance/02",
            "EZ-2/acceptance/03",
            "EZ-2/acceptance/04",
            "EZ-2/acceptance/05",
            "EZ-2/acceptance/06",
            "EZ-3/acceptance/05",
        ]
        .contains(&text(&row["case_id"]))
}

fn runtime_owner(row: &Value) -> Option<(&str, &str)> {
    let args = text(&row["command"]).split(' ').collect::<Vec<_>>();
    if args.len() != 7
        || args[0] != "scripts/test_integration.sh"
        || args[1] != "--adapter"
        || args[3] != "--suite"
        || args[5] != "--output"
        || !["axum", "fastly", "cloudflare", "spin"].contains(&args[2])
    {
        return None;
    }
    let environment = match args[2] {
        "fastly" => "viceroy",
        "cloudflare" => "workerd",
        value => value,
    };
    let suite = args[4];
    let directory = match suite {
        "ci" | "outbound-wire" => "redirect",
        "outbound-lifetimes" => "lifetimes",
        "outbound-overflow" => "overflow",
        "response-framing" | "synthesized-headers" | "smoke" | "outbound-progress" | "assembly" => {
            suite
        }
        _ => return None,
    };
    let expected = if suite == "ci" {
        format!(
            "crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/redirect/{}",
            args[2]
        )
    } else {
        format!(
            "crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/{directory}/{environment}"
        )
    };
    if args[6] != expected {
        return None;
    }
    if suite != "ci"
        && (text(&row["artifact_path"])
            != format!("outbound/{directory}/{environment}/events.jsonl")
            || text(&row["environment"]).split('/').next() != Some(environment))
    {
        return None;
    }
    Some((args[2], suite))
}

fn registry_fingerprints() -> Value {
    let root = crate::repository_root();
    let mut result = serde_json::Map::new();
    for (path, names) in [
        (
            "src/main.rs",
            &[
                "parse_from",
                "run",
                "outbound_lifetimes_selects_exactly_one_explicit_provider",
                "outbound_wire_suite_is_explicit_and_selects_one_supported_provider",
            ][..],
        ),
        ("src/runners.rs", &["fastly", "provider"][..]),
        (
            "src/runners/lifetimes.rs",
            &["run", "checks", "combined_count"][..],
        ),
        (
            "src/runners/wire.rs",
            &[
                "run",
                "progress_wire",
                "progress_suite",
                "finish_wire",
                "qualify_budgets",
            ][..],
        ),
        (
            "src/runners/ci.rs",
            &["run", "validate_markers", "phase_with_exit"][..],
        ),
    ] {
        let source =
            fs::read_to_string(root.join("crates/edgezero-test-harness").join(path)).unwrap();
        for name in names {
            result.insert(
                format!("{path}:{name}"),
                json!(hash(function(&source, name).unwrap())),
            );
        }
    }
    Value::Object(result)
}

#[test]
fn registered_executable_selectors_dispatch_and_controllers_retain_reviewed_ownership() {
    let expected: Value = serde_json::from_str(include_str!("reviewed_owners.json")).unwrap();
    let actual = registry_fingerprints();
    assert_eq!(
        actual, expected,
        "executable owner changes need explicit review; comments and strings cannot substitute for control flow"
    );
}

#[test]
fn source_inspection_uses_ast_and_pairs_each_guard_call_without_comment_or_string_false_positives()
{
    let original = "fn proof() { first(); second(); }";
    assert_eq!(
        function(original, "proof").unwrap(),
        function(
            "// fn proof() { bogus(); }\nfn proof() { first( /* trivia */ ); second(); }",
            "proof"
        )
        .unwrap()
    );
    assert_ne!(
        function("#[test] fn proof() {}", "proof").unwrap(),
        function("fn proof() {}", "proof").unwrap()
    );
    assert_ne!(
        function("#[cfg(test)] mod tests { #[test] fn proof() {} }", "proof").unwrap(),
        function("mod tests { #[test] fn proof() {} }", "proof").unwrap()
    );
    assert!(function("fn proof() {} mod nested { fn proof() {} }", "proof").is_err());
    assert!(function("const NOT_CODE: &str = \"fn proof() {}\";", "proof").is_err());
    assert_ne!(
        function(original, "proof").unwrap(),
        function("fn proof() { second(); first(); }", "proof").unwrap()
    );
    let source = r#"fn real() { guarded(&["--inventory", "env"], &["test", "--lib",]); } // guarded(&["fake"], &["test"])
const TEXT: &str = "guarded(&[fake], &[test])";"#;
    assert_eq!(
        guarded_commands(source).unwrap(),
        vec!["scripts/run_test_nonzero.sh --inventory env cargo test --lib"]
    );
    let malformed = format!("fn real() {{ guarded(&[{}", " ".repeat(100_000));
    assert!(guarded_commands(&malformed).is_err());
}

#[test]
fn executable_mutations_cannot_be_replaced_by_comments_or_constant_fragments() {
    let root = crate::repository_root().join("crates/edgezero-test-harness/src");
    for (path, name, before, after) in [
        (
            "main.rs",
            "parse_from",
            "|| (adapter_selected && a.adapter != \"all\")",
            "|| true",
        ),
        (
            "main.rs",
            "parse_from",
            "                \"outbound-lifetimes\",\n",
            "",
        ),
        (
            "main.rs",
            "outbound_lifetimes_selects_exactly_one_explicit_provider",
            "fn outbound_lifetimes_selects_exactly_one_explicit_provider()",
            "fn obsolete_selector()",
        ),
        (
            "runners.rs",
            "fastly",
            "return lifetimes::run(\"fastly\", run, records);",
            "return wire::run(\"fastly\", run, records);",
        ),
        (
            "runners.rs",
            "provider",
            "return wire::run(name, run, records);",
            "return smoke::run(name, run, records);",
        ),
        (
            "runners/lifetimes.rs",
            "run",
            "let outcome = wire::progress_suite(",
            "let outcome = unused_suite(",
        ),
        (
            "runners/lifetimes.rs",
            "run",
            "let count = combined_count(outcome, tail, callbacks);",
            "let count = Some(1);",
        ),
        (
            "runners/lifetimes.rs",
            "run",
            "let count = count?;\n    diagnostic?;",
            "diagnostic?;\n    let count = count?;",
        ),
        (
            "runners/lifetimes.rs",
            "run",
            "let count = count?;\n    diagnostic?;",
            "let count = Some(1);\n    diagnostic?;",
        ),
        (
            "runners/lifetimes.rs",
            "checks",
            "require(failures.is_empty(), failures.join(\"; \"))?;",
            "let _ = failures;",
        ),
        (
            "runners/lifetimes.rs",
            "checks",
            "Err(error) => failures.push(format!(\"budget-qualification: {error}\"))",
            "Err(_error) => {}",
        ),
        (
            "runners/wire.rs",
            "qualify_budgets",
            "budgets::run(adapter, port, records)",
            "Ok(6)",
        ),
        (
            "runners/wire.rs",
            "progress_suite",
            "let result = check(port, origins, records)?;",
            "let result = Ok(());",
        ),
        (
            "runners/wire.rs",
            "progress_suite",
            "let outcome = finish_wire(name, &mut process, origins, &state, records, outcome);",
            "let outcome = outcome;",
        ),
    ] {
        let original = fs::read_to_string(root.join(path)).unwrap();
        assert!(
            original.contains(before),
            "{path}:{name}: mutation must apply"
        );
        let at = original.find(&format!("fn {name}")).unwrap();
        let changed = format!(
            "{}{}\n/* {before} */\n",
            &original[..at],
            original[at..].replacen(before, after, 1)
        );
        assert_ne!(
            function(&changed, name).ok(),
            function(&original, name).ok(),
            "{path}:{name}"
        );
    }
}

#[test]
fn canonical_ci_and_qualification_owners_cover_every_ready_command_without_scope_promotion() {
    let root = crate::repository_root();
    let workflow_text = fs::read_to_string(root.join(".github/workflows/test.yml")).unwrap();
    let yaml = workflow(&workflow_text).unwrap();
    let inventory = io::read_json(&root.join(INVENTORY)).unwrap();
    let source =
        fs::read_to_string(root.join("crates/edgezero-test-harness/src/runners/ci/platforms.rs"))
            .unwrap();
    let guarded = guarded_commands(&source).unwrap();
    let steps = yaml["jobs"]["host-tests"]["steps"].as_array().unwrap();
    let direct = steps
        .iter()
        .filter_map(|step| step["run"].as_str())
        .flat_map(str::lines)
        .map(str::trim)
        .collect::<Vec<_>>();
    for adapter in ["axum", "fastly", "cloudflare", "spin"] {
        assert!(automatic_ci(&yaml, adapter), "{adapter}");
    }
    for row in inventory["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["implementation"] == "ready")
    {
        let command = text(&row["command"]);
        if command.starts_with("scripts/test_integration.sh ") {
            let (adapter, suite) = runtime_owner(row)
                .unwrap_or_else(|| panic!("unregistered {}: {command}", key(row)));
            assert!(automatic_ci(&yaml, adapter));
            assert!(!strict_budget(row) || suite == "outbound-lifetimes");
        } else {
            assert!(!strict_budget(row));
            let macro_proof = row["case_id"] == "EZ-22/acceptance/08"
                && command
                    == "scripts/run_test_nonzero.sh fallible_macro_state_skips_configuration_and_retries_without_caching_failure cargo test --offline --locked -p edgezero-macros --test app_macro"
                && direct.contains(&"cargo test --workspace --all-targets")
                && function(
                    &fs::read_to_string(root.join("crates/edgezero-macros/tests/app_macro.rs"))
                        .unwrap(),
                    text(&row["sentinel"]).rsplit("::").next().unwrap(),
                )
                .is_ok_and(|definition| definition.contains("# [test]"));
            assert!(
                direct.contains(&command)
                    || guarded.iter().any(|value| value == command)
                    || macro_proof
                    || explicit_qualification_owner(row),
                "unowned {}: {command}",
                key(row)
            );
        }
    }
    let host = &yaml["jobs"]["host-tests"];
    assert!(host.get("if").is_none());
    assert!(
        host["strategy"]["matrix"]["suite"]
            .as_array()
            .unwrap()
            .contains(&json!("workspace"))
    );
    let harness_tests =
        "cargo test --offline --locked --manifest-path crates/edgezero-test-harness/Cargo.toml";
    assert!(
        steps
            .iter()
            .any(|step| step["if"] == "matrix.suite == 'workspace'"
                && text(&step["run"]).lines().any(|line| line == harness_tests))
    );
    let launcher = fs::read_to_string(root.join("scripts/test_integration.sh")).unwrap();
    assert_eq!(
        launcher.trim_end().lines().last().unwrap(),
        "exec \"${CARGO:-cargo}\" run --quiet --locked --manifest-path \"$REPO_DIR/crates/edgezero-test-harness/Cargo.toml\" -- \"$@\""
    );
}

#[test]
fn ci_and_runtime_ownership_reject_noncanonical_mutations_and_duplicate_yaml() {
    for source in ["jobs: {}\njobs: {}", "jobs: []", "jobs: [", "[]"] {
        assert!(workflow(source).is_err());
    }
    let root = crate::repository_root();
    let yaml =
        workflow(&fs::read_to_string(root.join(".github/workflows/test.yml")).unwrap()).unwrap();
    for adapter in ["axum", "fastly", "cloudflare", "spin"] {
        let environment = match adapter {
            "fastly" => "viceroy",
            "cloudflare" => "workerd",
            value => value,
        };
        let mut row = json!({"command":format!("scripts/test_integration.sh --adapter {adapter} --suite outbound-lifetimes --output crates/edgezero-test-harness/fixtures/reusable-app/.runs/outbound/lifetimes/{environment}"),"environment":environment,"artifact_path":format!("outbound/lifetimes/{environment}/events.jsonl")});
        assert_eq!(runtime_owner(&row), Some((adapter, "outbound-lifetimes")));
        for suffix in [" --skip-build", " --adapter fastly", " && true", "; true"] {
            let mut changed = row.clone();
            changed["command"] = json!(format!("{}{suffix}", text(&row["command"])));
            assert!(runtime_owner(&changed).is_none());
        }
        for field in ["environment", "artifact_path"] {
            let mut changed = row.clone();
            changed[field] = json!("wrong");
            assert!(runtime_owner(&changed).is_none());
        }
        row["command"] = json!(text(&row["command"]).replace("--adapter", "--other"));
        assert!(runtime_owner(&row).is_none());
        let job = if adapter == "axum" {
            "host-tests"
        } else {
            "adapter-wasm-tests"
        };
        let mut changed = yaml.clone();
        changed["jobs"][job]["if"] = json!(false);
        assert!(!automatic_ci(&changed, adapter));
        changed = yaml.clone();
        changed["jobs"][job]["steps"] = json!([]);
        assert!(!automatic_ci(&changed, adapter));
        changed = yaml.clone();
        changed["jobs"][job]["continue-on-error"] = json!(true);
        assert!(!automatic_ci(&changed, adapter));
        changed = yaml.clone();
        for step in changed["jobs"][job]["steps"].as_array_mut().unwrap() {
            step["continue-on-error"] = json!(true);
        }
        assert!(!automatic_ci(&changed, adapter));
        changed = yaml.clone();
        changed["jobs"][job]["strategy"]["matrix"] = json!({});
        assert!(!automatic_ci(&changed, adapter));
        changed = yaml.clone();
        let duplicate = changed["jobs"][job]["steps"].as_array().unwrap().clone();
        changed["jobs"][job]["steps"]
            .as_array_mut()
            .unwrap()
            .extend(duplicate);
        assert!(!automatic_ci(&changed, adapter));
    }
}
