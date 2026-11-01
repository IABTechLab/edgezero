use crate::evidence::{Result, require};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn fence(line: &str) -> Option<(char, usize, &str)> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    let content = line.trim_start_matches(' ');
    if line.len() - content.len() > 3 {
        return None;
    }
    let marker = content.chars().next()?;
    if !matches!(marker, '`' | '~') {
        return None;
    }
    let length = content
        .chars()
        .take_while(|character| *character == marker)
        .count();
    (length >= 3).then(|| (marker, length, content[length..].trim()))
}

fn extract(markdown: &str) -> Result<String> {
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
        if (1..=2).contains(&level) && line[level..].starts_with(char::is_whitespace) {
            let heading = line[level..].trim().trim_end_matches('#').trim_end();
            selected = level == 2 && heading == "Forward An Inbound Request";
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
    run_doctest(&code, &build_core(root)?, root)?;
    println!("forwarding example: one doctest passed; network handler not invoked");
    Ok(())
}

#[derive(Debug, PartialEq)]
struct Artifact {
    library: PathBuf,
    dependencies: PathBuf,
    edition: String,
}

fn core_artifact(output: &str, root: &Path) -> Result<Artifact> {
    let records = output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<std::result::Result<Vec<Value>, _>>()?;
    let finished = records
        .iter()
        .filter(|record| record["reason"] == "build-finished")
        .collect::<Vec<_>>();
    require(
        finished.len() == 1 && finished[0]["success"] == true,
        "Cargo build did not finish successfully",
    )?;
    let manifest = root.join("crates/edgezero-core/Cargo.toml");
    let artifacts = records
        .iter()
        .filter(|record| {
            record["reason"] == "compiler-artifact"
                && record["manifest_path"].as_str() == manifest.to_str()
                && record["target"]["name"] == "edgezero_core"
                && record["target"]["kind"]
                    .as_array()
                    .is_some_and(|kinds| kinds.contains(&Value::from("lib")))
        })
        .collect::<Vec<_>>();
    require(
        artifacts.len() == 1,
        "Cargo must emit exactly one core library artifact",
    )?;
    let artifact = artifacts[0];
    let filenames = artifact["filenames"]
        .as_array()
        .ok_or("Cargo artifact is missing filenames")?;
    let libraries = filenames
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| name.ends_with(".rlib"))
        .collect::<Vec<_>>();
    require(
        libraries.len() == 1 && Path::new(libraries[0]).is_absolute(),
        "Cargo artifact must identify exactly one absolute rlib path",
    )?;
    let edition = artifact["target"]["edition"]
        .as_str()
        .ok_or("Cargo artifact is missing edition")?;
    require(
        ["2015", "2018", "2021", "2024"].contains(&edition),
        "Cargo artifact has unsupported edition",
    )?;
    let library = PathBuf::from(libraries[0]);
    let directory = library
        .parent()
        .ok_or("Cargo artifact is missing its parent")?;
    let dependencies = if directory.file_name().is_some_and(|name| name == "deps") {
        directory.to_owned()
    } else {
        directory.join("deps")
    };
    Ok(Artifact {
        library,
        dependencies,
        edition: edition.into(),
    })
}

fn require_one_doctest(listing: &str) -> Result<()> {
    require(
        listing
            .lines()
            .filter(|line| line.ends_with(": test"))
            .count()
            == 1
            && listing
                .lines()
                .any(|line| line.trim_end() == "1 test, 0 benchmarks")
            && !listing.to_ascii_lowercase().contains("ignore"),
        "expected exactly one non-ignored listed forwarding doctest",
    )
}

fn require_one_pass(result: &str) -> Result<()> {
    let summaries = result
        .lines()
        .filter(|line| line.starts_with("test result:"))
        .collect::<Vec<_>>();
    require(
        summaries.len() == 1
            && summaries[0].starts_with(
                "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;",
            ),
        "expected exactly one passed, non-ignored forwarding doctest",
    )
}

fn build_core(root: &Path) -> Result<Artifact> {
    let output = super::output(
        Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .current_dir(root)
            .args([
                "build",
                "--offline",
                "--locked",
                "-p",
                "edgezero-core",
                "--lib",
                "--message-format=json",
            ]),
        32 * 1024 * 1024,
    )?;
    core_artifact(&output, root)
}

fn run_doctest(code: &str, artifact: &Artifact, root: &Path) -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let markdown = temporary.path().join("forwarding.md");
    let longest = code
        .split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    let delimiter = "`".repeat(3.max(longest + 1));
    fs::write(
        &markdown,
        format!("{delimiter}rust\n{code}\n# fn main() {{}}\n{delimiter}\n"),
    )?;
    let rustdoc = std::env::var_os("RUSTDOC")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            crate::rustc().with_file_name(format!("rustdoc{}", std::env::consts::EXE_SUFFIX))
        });
    let command = || {
        let mut command = Command::new(&rustdoc);
        command
            .current_dir(root)
            .arg("--test")
            .arg(&markdown)
            .arg("--edition")
            .arg(&artifact.edition)
            .arg("--extern")
            .arg(format!("edgezero_core={}", artifact.library.display()))
            .arg("-L")
            .arg(format!("dependency={}", artifact.dependencies.display()))
            .arg("--test-args=--color=never");
        command
    };
    require_one_doctest(&super::output(
        command().arg("--test-args=--list"),
        32 * 1024 * 1024,
    )?)?;
    require_one_pass(&super::output(&mut command(), 32 * 1024 * 1024)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(code: &str, language: &str) -> String {
        format!("## Forward An Inbound Request\n\n```{language}\n{code}\n```\n")
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
    fn cargo_artifacts_support_fresh_builds_and_custom_target_directories() {
        let root = crate::repository_root();
        let mut records = records();
        assert_eq!(
            core_artifact(&cargo_output(&records), &root).unwrap(),
            Artifact {
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
