#[cfg(test)]
use super::compiler::{Artifact, core_artifact, require_one_doctest, require_one_pass};
use super::compiler::{build_core, fence, run_doctest};
use crate::evidence::{Result, require};
use std::{fs, path::Path};

fn extract(markdown: &str) -> Result<String> {
    extract_section(markdown, "Forward An Inbound Request", 2)
}

fn extract_section(markdown: &str, wanted: &str, wanted_level: usize) -> Result<String> {
    let lines = markdown.split('\n').collect::<Vec<_>>();
    let mut active = None;
    let mut selected = false;
    let mut sections = 0;
    let mut examples = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some((marker, length, start, language, chosen)) = active {
            if fence(line).is_some_and(|(end, count, info)| {
                end == marker && count >= length && info.is_empty()
            }) {
                if chosen {
                    require(
                        language == "rust",
                        "forwarding example must use a plain rust fence",
                    )?;
                    examples.push(format!("{}\n", lines[start + 1..index].join("\n")));
                }
                active = None;
            }
            continue;
        }
        if let Some((marker, length, language)) = fence(line) {
            active = Some((marker, length, index, language, selected));
            continue;
        }
        let level = line
            .chars()
            .take_while(|character| *character == '#')
            .count();
        if (1..=wanted_level).contains(&level) && line[level..].starts_with(char::is_whitespace) {
            let heading = line[level..].trim().trim_end_matches('#').trim_end();
            selected = level == wanted_level && heading == wanted;
            sections += usize::from(selected);
        }
    }
    require(
        !active.is_some_and(|(_, _, _, _, chosen)| chosen)
            && sections == 1
            && examples.len() == 1
            && !examples[0].trim().is_empty(),
        "forwarding section must contain exactly one closed, nonempty Rust fence",
    )?;
    Ok(examples.remove(0))
}

pub(super) fn run(root: &Path) -> Result<()> {
    let code = extract(&fs::read_to_string(root.join("docs/guide/proxying.md"))?)?;
    let artifact = build_core(root)?;
    run_doctest(&code, &artifact, root)?;
    let handlers = fs::read_to_string(root.join("docs/guide/handlers.md"))?;
    for (section, level) in [("Request Context", 3), ("Custom Extractors", 2)] {
        run_doctest(
            &extract_section(&handlers, section, level)?,
            &artifact,
            root,
        )?;
    }
    println!("forwarding example: one doctest passed; network handler not invoked");
    println!("handler examples: two doctests passed; handlers not invoked");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(code: &str, language: &str) -> String {
        format!("## Forward An Inbound Request\n\n```{language}\n{code}\n```\n")
    }

    #[test]
    fn both_context_handler_examples_are_selected_from_the_actual_guide() {
        let guide =
            fs::read_to_string(crate::repository_root().join("docs/guide/handlers.md")).unwrap();
        let context = extract_section(&guide, "Request Context", 3).unwrap();
        let custom = extract_section(&guide, "Custom Extractors", 2).unwrap();
        assert!(context.contains("ctx.method()"));
        assert!(custom.contains("ctx.headers()"));
        assert!(!context.contains("ctx.request()"));
        assert!(!custom.contains("ctx.request()"));
    }

    #[test]
    fn extracts_the_actual_forwarding_example_unchanged() {
        let guide =
            std::fs::read_to_string(crate::repository_root().join("docs/guide/proxying.md"))
                .unwrap();
        let code = extract(&guide).unwrap();
        assert!(code.contains("ctx.into_request()?"));
        assert!(code.contains("ResponseDecoding::Original"));
        assert!(guide.contains(&code));
    }

    #[test]
    fn refuses_missing_duplicate_empty_unclosed_or_weakened_fences() {
        for markdown in [
            "## Other\n".into(),
            section("fn example() {}", "rust").repeat(2),
            "## Forward An Inbound Request\nNo example.\n".into(),
            format!(
                "{}\n```rust\nfn second() {{}}\n```\n",
                section("fn example() {}", "rust")
            ),
            "## Forward An Inbound Request\n```rust\nfn example() {}\n".into(),
            section("", "rust"),
        ] {
            assert!(extract(&markdown).is_err(), "{markdown}");
        }
        for language in [
            "rust,ignore",
            "rust,ignore-x86_64",
            "rust,no_run",
            "rust,compile_fail",
            "rust,should_panic",
            "rust,edition2018",
            "rs",
            "",
        ] {
            assert!(
                extract(&section("fn example() {}", language)).is_err(),
                "{language}"
            );
        }
    }

    #[test]
    fn fenced_headings_and_other_sections_do_not_change_selection() {
        let markdown = format!(
            "## Before\n```text\n## Forward An Inbound Request\n```\n{}## After\n```rust\nfn unrelated() {{}}\n```\n",
            section("fn example() {}\n// ## Other", "rust")
        );
        assert_eq!(
            extract(&markdown).unwrap(),
            "fn example() {}\n// ## Other\n"
        );
    }

    fn records() -> Vec<serde_json::Value> {
        use serde_json::json;
        vec![
            json!({"reason":"compiler-artifact", "manifest_path":crate::repository_root().join("crates/edgezero-core/Cargo.toml"),
                "target":{"name":"edgezero_core","kind":["lib"],"edition":"2024"},
                "filenames":["/custom target/debug/libedgezero_core.rlib"], "fresh":true}),
            json!({"reason":"build-finished", "success":true}),
        ]
    }

    fn cargo_output(records: &[serde_json::Value]) -> String {
        records
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn documentation_uses_the_async_trait_artifact_from_the_same_cargo_graph() {
        let mut records = records();
        let macro_record = serde_json::json!({"reason":"compiler-artifact", "target":{"name":"async_trait","kind":["proc-macro"]},"filenames":["/custom target/debug/deps/libasync_trait.dylib"]});
        records.push(macro_record.clone());
        assert_eq!(
            core_artifact(&cargo_output(&records), &crate::repository_root())
                .unwrap()
                .async_trait,
            Some("/custom target/debug/deps/libasync_trait.dylib".into())
        );
        records.push(macro_record);
        assert!(core_artifact(&cargo_output(&records), &crate::repository_root()).is_err());
    }

    #[test]
    fn cargo_artifacts_support_fresh_builds_and_custom_target_directories() {
        let root = crate::repository_root();
        let mut records = records();
        assert_eq!(
            core_artifact(&cargo_output(&records), &root).unwrap(),
            Artifact {
                async_trait: None,
                library: "/custom target/debug/libedgezero_core.rlib".into(),
                dependencies: "/custom target/debug/deps".into(),
                edition: "2024".into(),
            }
        );
        records[0]["filenames"] = serde_json::json!(["/elsewhere/deps/libedgezero_core-123.rlib"]);
        assert_eq!(
            core_artifact(&cargo_output(&records), &root)
                .unwrap()
                .dependencies,
            std::path::Path::new("/elsewhere/deps")
        );
    }

    #[test]
    fn refuses_failed_missing_ambiguous_or_malformed_cargo_records() {
        use serde_json::json;
        let root = crate::repository_root();
        assert!(core_artifact("not JSON", &root).is_err());
        for index in 0..9 {
            let mut records = records();
            match index {
                0 => {
                    records.pop();
                }
                1 => records[1]["success"] = json!(false),
                2 => records[0]["manifest_path"] = json!("/other/Cargo.toml"),
                3 => records[0]["filenames"] = json!([]),
                4 => {
                    records[0]["target"]
                        .as_object_mut()
                        .unwrap()
                        .remove("edition");
                }
                5 => records.push(records[0].clone()),
                6 => records[0]["filenames"] = json!(["relative.rlib"]),
                7 => records[0]["filenames"] = json!(["/one.rlib", "/two.rlib"]),
                8 => records.push(records[1].clone()),
                _ => unreachable!(),
            }
            assert!(
                core_artifact(&cargo_output(&records), &root).is_err(),
                "mutation {index}"
            );
        }
    }

    #[test]
    fn requires_exactly_one_listed_and_passed_nonignored_doctest() {
        require_one_doctest("example.md - (line 2): test\n\n1 test, 0 benchmarks\n").unwrap();
        require_one_pass("running 1 test\ntest example.md - (line 2) ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").unwrap();
        for listing in [
            "0 tests, 0 benchmarks\n",
            "a: test\nb: test\n\n2 tests, 0 benchmarks\n",
            "a: test\n\n1 test, 0 benchmarks\nignored",
            "a: test\n",
        ] {
            assert!(require_one_doctest(listing).is_err(), "{listing}");
        }
        for result in [
            "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
            "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0s",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s",
        ] {
            assert!(require_one_pass(result).is_err(), "{result}");
        }
    }

    #[test]
    fn actual_rustdoc_compiles_forwarding_and_rejects_weakened_or_executable_examples() {
        let root = crate::repository_root();
        let artifact = build_core(&root).unwrap();
        let guide = std::fs::read_to_string(root.join("docs/guide/proxying.md")).unwrap();
        let code = extract(&guide).unwrap();
        run_doctest(&code, &artifact, &root).unwrap();
        let broken = code.replace("ctx.into_request()?", "ctx.into_request()");
        assert_ne!(broken, code);
        assert!(
            run_doctest(&broken, &artifact, &root)
                .unwrap_err()
                .to_string()
                .contains("E0308")
        );
        assert!(run_doctest(&format!("{code}\nfn broken( {{}}"), &artifact, &root).is_err());
        run_doctest(
            &format!("{code}\nfn not_called() {{ panic!(\"do not execute\"); }}"),
            &artifact,
            &root,
        )
        .unwrap();
        let payload = "fn main() {}\n```\n```text\nthis is not valid Rust";
        let extracted = extract(&format!(
            "## Forward An Inbound Request\n~~~rust\n{payload}\n~~~\n"
        ))
        .unwrap();
        assert_eq!(extracted, format!("{payload}\n"));
        assert!(run_doctest(&extracted, &artifact, &root).is_err());
        let payload = "fn handler() { panic!(\"HANDLER_WAS_CALLED\"); }\nfn main() { handler(); }\n```\n```text\n";
        let extracted = extract(&format!(
            "## Forward An Inbound Request\n````rust\n{payload}\n````\n"
        ))
        .unwrap();
        let failure = run_doctest(&extracted, &artifact, &root)
            .unwrap_err()
            .to_string();
        assert!(failure.contains("Couldn't compile the test"), "{failure}");
        assert!(!failure.contains("Test executable failed"), "{failure}");
        assert!(
            run_doctest(
                &format!("{code}\npanic!(\"do not execute\");"),
                &artifact,
                &root
            )
            .is_err()
        );
    }
}
