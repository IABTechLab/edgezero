use crate::evidence::{Result, require};
use std::{fs, path::Path, process::Command};

const SHAPES: [(&str, &str); 3] = [
    (
        "image_optimizer_focus_x_only",
        "let _ = OutboundCropFocus::Percent { x: 25 };",
    ),
    (
        "image_optimizer_focus_y_only",
        "let _ = OutboundCropFocus::Percent { y: 75 };",
    ),
    (
        "image_optimizer_focus_smart_offsets",
        "let _ = OutboundCropFocus::Smart { x: 25, y: 75 };",
    ),
];
const VALID: &str = "image_optimizer_focus_valid";
const NUMERIC: [&str; 3] = [
    "outbound::image_optimizer::tests::image_optimizer_quality_accepts_exactly_zero_through_one_hundred",
    "outbound::image_optimizer::tests::image_optimizer_dimensions_and_crop_reject_zero_without_invented_maxima",
    "outbound::image_optimizer::tests::image_optimizer_crop_checks_both_public_percent_payloads",
];
const SENTINEL: &str = "ez_8_acceptance_01_unit_core";

fn canonical(code: &str) -> String {
    code.lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn examples(source: &str) -> Result<Vec<(&'static str, String)>> {
    let title =
        "/// Crop focus with paired percentage coordinates, validated by the options builder.\n";
    require(
        source.matches(title).count() == 1,
        "missing or duplicate focus documentation",
    )?;
    let docs = source
        .split_once(title)
        .ok_or("missing focus documentation")?
        .1;
    let mut active = None;
    let mut code = String::new();
    let mut blocks = Vec::new();
    for line in docs.lines().take_while(|line| line.starts_with("///")) {
        let line = line.strip_prefix("///").ok_or("invalid doc line")?;
        let line = line.strip_prefix(' ').unwrap_or(line);
        if let Some((marker, length)) = active {
            if super::compiler::fence(line).is_some_and(|(end, count, info)| {
                end == marker && count >= length && info.is_empty()
            }) {
                blocks.push(std::mem::take(&mut code));
                active = None;
            } else {
                code.push_str(line);
                code.push('\n');
            }
        } else if let Some((marker, length, language)) = super::compiler::fence(line) {
            require(
                language == "compile_fail",
                "focus example must be compile_fail without weakening annotations",
            )?;
            active = Some((marker, length));
        }
    }
    require(
        active.is_none() && blocks.len() == 3,
        "expected three closed focus examples",
    )?;
    SHAPES
        .iter()
        .map(|(id, statement)| {
            let expected = canonical(&format!(
                "use edgezero_core::OutboundCropFocus;\n{statement}"
            ));
            let matches = blocks
                .iter()
                .filter(|code| canonical(code) == expected)
                .collect::<Vec<_>>();
            require(
                matches.len() == 1,
                format!("missing, duplicate or changed focus shape: {id}"),
            )?;
            Ok((*id, matches[0].clone()))
        })
        .collect()
}

fn document(examples: &[(&str, String)]) -> String {
    let mut markdown = format!(
        "# {VALID}\n\n```rust\nuse edgezero_core::OutboundCropFocus;\nlet _ = [OutboundCropFocus::Default, OutboundCropFocus::Smart, OutboundCropFocus::Percent {{ x: 0, y: 100 }}];\n```\n"
    );
    for (id, code) in examples {
        markdown.push_str(&format!("\n# {id}\n\n```compile_fail\n{code}```\n"));
    }
    markdown
}

fn listed(text: &str) -> Result<Vec<(String, bool)>> {
    let mut tests = Vec::new();
    let ids = std::iter::once(VALID)
        .chain(SHAPES.iter().map(|(id, _)| *id))
        .collect::<Vec<_>>();
    let mut seen = std::collections::HashSet::new();
    for name in text.lines().filter_map(|line| line.strip_suffix(": test")) {
        let (_, heading) = name
            .rsplit_once(" - ")
            .ok_or("missing focus test heading")?;
        let (id, line) = heading
            .split_once(" (line ")
            .ok_or("missing focus test line")?;
        require(
            ids.contains(&id) && seen.insert(id),
            "missing, duplicate or unknown focus test",
        )?;
        require(
            line.strip_suffix(')')
                .and_then(|line| line.parse::<usize>().ok())
                .is_some_and(|line| line > 0),
            "invalid focus test line",
        )?;
        tests.push((name.to_owned(), id != VALID));
    }
    require(
        tests.len() == 4 && !text.to_ascii_lowercase().contains("ignore"),
        "expected four non-ignored focus tests",
    )?;
    Ok(tests)
}

fn passed(listing: &str, result: &str) -> Result<()> {
    let tests = listed(listing)?;
    for (name, negative) in tests {
        let expected = format!(
            "test {name}{} ... ok",
            if negative { " - compile fail" } else { "" }
        );
        require(
            result.lines().filter(|line| *line == expected).count() == 1,
            format!("focus test not passed exactly once: {name}"),
        )?;
    }
    require(
        result
            .lines()
            .filter(|line| line.starts_with("test ") && !line.starts_with("test result:"))
            .count()
            == 4,
        "unexpected or duplicate focus result",
    )?;
    let mut total = 0;
    for summary in result
        .lines()
        .filter(|line| line.starts_with("test result:"))
    {
        let (count, rest) = summary
            .strip_prefix("test result: ok. ")
            .and_then(|line| line.split_once(" passed; "))
            .ok_or("unsuccessful focus summary")?;
        require(
            rest.starts_with("0 failed; 0 ignored; 0 measured; 0 filtered out;"),
            "skipped or failed focus test",
        )?;
        total += count.parse::<usize>()?;
    }
    require(total == 4, "focus summaries did not pass four tests")
}

fn numeric_command(root: &Path) -> Command {
    let mut command = Command::new("bash");
    command
        .current_dir(root)
        .arg("scripts/run_test_nonzero.sh")
        .arg(NUMERIC.join("\n"))
        .arg(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args([
            "test",
            "--offline",
            "--locked",
            "-p",
            "edgezero-core",
            "--features",
            "test-utils",
            "--lib",
            "outbound::image_optimizer::tests::",
        ]);
    command
}

fn compile(markdown: &str, artifact: &super::compiler::Artifact, root: &Path) -> Result<String> {
    let mut listing = String::new();
    let result = super::compiler::run_markdown(markdown, artifact, root, |text| {
        listed(text)?;
        listing = text.to_owned();
        Ok(())
    })?;
    passed(&listing, &result)?;
    Ok(result)
}

fn require_contributors(text: &str) -> Result<()> {
    for id in NUMERIC
        .into_iter()
        .chain(std::iter::once(VALID))
        .chain(SHAPES.iter().map(|(id, _)| *id))
    {
        require(
            text.lines()
                .filter(|line| *line == format!("test {id} ... ok"))
                .count()
                == 1,
            format!("missing or duplicate retained focus proof: {id}"),
        )?;
    }
    Ok(())
}

pub(super) fn artifact_path(root: &Path) -> std::path::PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target"));
    let target = if target.is_absolute() {
        target
    } else {
        root.join(target)
    };
    target.join("outbound/unit/core/image-optimizer.log")
}

pub(super) fn invalidate(path: &Path) -> Result<()> {
    fs::create_dir_all(path.parent().ok_or("missing artifact parent")?)?;
    // A failed rerun must not leave an earlier success artifact in place.
    fs::write(path, "")?;
    Ok(())
}

pub(super) fn run(root: &Path, path: &Path) -> Result<()> {
    let examples = examples(&fs::read_to_string(
        root.join("crates/edgezero-core/src/outbound.rs"),
    )?)?;
    let numeric = super::output(&mut numeric_command(root), 8 * 1024 * 1024)?;
    let compiler = compile(
        &document(&examples),
        &super::compiler::build_core(root)?,
        root,
    )?;
    let mut retained = format!("{numeric}\n{compiler}");
    for id in std::iter::once(VALID).chain(SHAPES.iter().map(|(id, _)| *id)) {
        retained.push_str(&format!("test {id} ... ok\n"));
    }
    require_contributors(&retained)?;
    retained.push_str(&format!("test {SENTINEL} ... ok\n"));
    require(
        retained.len() <= 16 * 1024 * 1024,
        "oversized focus evidence",
    )?;
    fs::write(path, &retained)?;
    require(
        fs::read_to_string(path)? == retained,
        "persisted focus evidence differs",
    )?;
    print!("{retained}");
    println!(
        "image optimizer evidence: {}; qualification=false",
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> String {
        std::fs::read_to_string(
            crate::repository_root().join("crates/edgezero-core/src/outbound.rs"),
        )
        .unwrap()
    }

    #[test]
    fn extracts_each_actual_focus_shape_once_without_changing_its_code() {
        let source = source();
        let examples = examples(&source).unwrap();
        assert_eq!(examples.len(), 3);
        for ((id, code), (expected, statement)) in examples.iter().zip(SHAPES) {
            assert_eq!(*id, expected);
            assert!(code.contains(statement));
            assert!(code.contains("use edgezero_core::OutboundCropFocus;"));
        }
    }

    #[test]
    fn missing_duplicate_weakened_or_unrelated_examples_cannot_receive_credit() {
        let source = source();
        for mutated in [
            source.replace(SHAPES[0].1, ""),
            source.replace(SHAPES[1].1, SHAPES[0].1),
            source.replace("/// ```compile_fail", "/// ```ignore"),
            source.replace("/// ```compile_fail", "/// ```no_run"),
            source.replace(SHAPES[2].1, "unknown_symbol();"),
            source.replace(
                "use edgezero_core::OutboundCropFocus;",
                "useedgezero_core::OutboundCropFocus;",
            ),
            source.replace(
                "/// Crop focus with paired percentage coordinates",
                "/// Other documentation",
            ),
            source.replace("/// ```\n", ""),
        ] {
            assert!(examples(&mutated).is_err(), "{mutated}");
        }
    }

    fn listing() -> String {
        let mut text = format!("examples.md - {VALID} (line 3): test\n1 test, 0 benchmarks\n");
        for (index, (id, _)) in SHAPES.iter().enumerate() {
            text.push_str(&format!(
                "examples.md - {id} (line {}): test\n",
                10 + index * 6
            ));
        }
        text.push_str("3 tests, 0 benchmarks\n");
        text
    }

    fn result() -> String {
        let mut text = format!(
            "test examples.md - {VALID} (line 3) ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n"
        );
        for (index, (id, _)) in SHAPES.iter().enumerate() {
            text.push_str(&format!(
                "test examples.md - {id} (line {}) - compile fail ... ok\n",
                10 + index * 6
            ));
        }
        text.push_str("test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n");
        text
    }

    #[test]
    fn discovery_and_execution_require_every_distinct_positive_and_negative_proof() {
        let listing = listing();
        let result = result();
        assert_eq!(listed(&listing).unwrap().len(), 4);
        passed(&listing, &result).unwrap();
        for text in [
            listing.replace(SHAPES[1].0, SHAPES[0].0),
            listing.replace(VALID, "other"),
            String::new(),
        ] {
            assert!(listed(&text).is_err(), "{text}");
        }
        for text in [
            result.replace(" - compile fail", ""),
            result.replace(" ... ok", " ... ignored"),
            result.replace("0 failed", "1 failed"),
            result.replace("0 ignored", "1 ignored"),
            result.replace(VALID, "other"),
            format!("{result}{result}"),
            String::new(),
        ] {
            assert!(passed(&listing, &text).is_err(), "{text}");
        }
    }

    #[test]
    fn retained_contributors_and_numeric_discovery_are_complete() {
        let command = numeric_command(&crate::repository_root());
        let arguments = command
            .get_args()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        assert_eq!(arguments[1], NUMERIC.join("\n"));
        assert!(
            arguments
                .iter()
                .any(|value| value == "outbound::image_optimizer::tests::")
        );
        let mut evidence = String::new();
        for name in NUMERIC
            .into_iter()
            .chain(std::iter::once(VALID))
            .chain(SHAPES.iter().map(|(id, _)| *id))
        {
            evidence.push_str(&format!("test {name} ... ok\n"));
        }
        require_contributors(&evidence).unwrap();
        for text in [
            evidence.replace(NUMERIC[2], "unrelated"),
            evidence.replace(VALID, "unrelated"),
            evidence.replace(" ... ok", " ... ignored"),
            format!("{evidence}{evidence}"),
        ] {
            assert!(require_contributors(&text).is_err(), "{text}");
        }
    }

    #[test]
    fn actual_compiler_rejects_an_unexpectedly_valid_negative_and_a_broken_positive() {
        let root = crate::repository_root();
        let artifact = super::super::compiler::build_core(&root).unwrap();
        let markdown = document(&examples(&source()).unwrap());
        compile(&markdown, &artifact, &root).unwrap();
        let valid_negative = markdown.replace(
            SHAPES[0].1,
            "let _ = OutboundCropFocus::Percent { x: 25, y: 75 };",
        );
        let error = compile(&valid_negative, &artifact, &root)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Test compiled successfully"), "{error}");
        let broken_positive = markdown.replace("let _ = [", "let _: MissingPositiveControl = [");
        let error = compile(&broken_positive, &artifact, &root)
            .unwrap_err()
            .to_string();
        assert!(error.contains("MissingPositiveControl"), "{error}");
    }
}
