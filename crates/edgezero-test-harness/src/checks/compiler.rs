use crate::evidence::{Result, require};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub(super) fn fence(line: &str) -> Option<(char, usize, &str)> {
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

#[derive(Debug, PartialEq)]
pub(super) struct Artifact {
    pub(super) library: PathBuf,
    pub(super) dependencies: PathBuf,
    pub(super) edition: String,
}

pub(super) fn core_artifact(output: &str, root: &Path) -> Result<Artifact> {
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

pub(super) fn require_one_doctest(listing: &str) -> Result<()> {
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

pub(super) fn require_one_pass(result: &str) -> Result<()> {
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

pub(super) fn build_core(root: &Path) -> Result<Artifact> {
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

pub(super) fn run_doctest(code: &str, artifact: &Artifact, root: &Path) -> Result<()> {
    let longest = code
        .split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or(0);
    let delimiter = "`".repeat(3.max(longest + 1));
    let markdown = format!("{delimiter}rust\n{code}\n# fn main() {{}}\n{delimiter}\n");
    require_one_pass(&run_markdown(
        &markdown,
        artifact,
        root,
        require_one_doctest,
    )?)
}

pub(super) fn run_markdown(
    markdown: &str,
    artifact: &Artifact,
    root: &Path,
    validate_listing: impl FnOnce(&str) -> Result<()>,
) -> Result<String> {
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("examples.md");
    fs::write(&path, markdown)?;
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
            .arg(&path)
            .arg("--edition")
            .arg(&artifact.edition)
            .arg("--extern")
            .arg(format!("edgezero_core={}", artifact.library.display()))
            .arg("-L")
            .arg(format!("dependency={}", artifact.dependencies.display()))
            .arg("--test-args=--color=never");
        command
    };
    let listing = super::output(command().arg("--test-args=--list"), 32 * 1024 * 1024)?;
    validate_listing(&listing)?;
    let result = super::output(&mut command(), 32 * 1024 * 1024)?;
    Ok(format!("{listing}\n{result}"))
}
