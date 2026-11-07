use super::{Result, require};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod catalog;
mod cli;
#[cfg(test)]
mod fixtures;
mod io;
#[cfg(test)]
mod ownership_tests;
mod record;
#[cfg(test)]
mod record_tests;
#[cfg(test)]
mod reviewed_tests;

const SPEC: &str = "docs/superpowers/specs/2026-10-17-edgezero-pr275-requirements.md";
const INVENTORY: &str = "crates/edgezero-test-harness/acceptance/outbound-cutover-cases.json";
const REVIEWED_LEGS: &str = "5ad5ad13cb3a0ea9d96307f1ec150649a21357490b4b9b26ae59027e0e539ec9";

pub fn run(arguments: &[String]) -> Result<i32> {
    let (exit, output) = cli::execute(arguments, &crate::repository_root())?;
    print!("{output}");
    Ok(exit)
}

#[cfg(test)]
pub(super) fn assembly_contract() -> Result<()> {
    let mut fixture = fixtures::Fixture::actual();
    fixture.input["source_state"] = json!("working-tree");
    fixture.input["qualification"] = json!(false);
    fixture.input["results"]
        .as_array_mut()
        .ok_or("missing results")?
        .truncate(2);
    for (index, status) in ["failed", "pending"].iter().enumerate() {
        fixture.input["results"][index]["status"] = json!(status);
        fixture.input["results"][index]["samples"] = json!(if index == 0 { 3 } else { 0 });
        fixture.input["results"][index]["assertions"][0]["passed"] = json!(false);
    }
    let draft = record::assemble_record(&fixture.manifest, &fixture.input, &fixture.context)?;
    require(
        draft["stage"] == "pre-freeze"
            && draft["qualification"] == false
            && draft["qualified"] == false
            && draft["source_state"] == "working-tree",
        "assembly promoted development evidence",
    )?;
    require(
        draft["assembly"]
            == json!({"observed_legs":2,"passed":0,"failed":1,"pending":1,"samples":3}),
        "assembly lost outcome or sample counts",
    )?;
    require(
        draft["results"] == fixture.input["results"] && draft["builds"] == fixture.input["builds"],
        "assembly changed counts, provenance or producer hashes",
    )?;
    let path = fixture.artifacts.path().join("draft.json");
    io::write_new(&path, &draft)?;
    let original = std::fs::read(&path)?;
    require(
        io::write_new(&path, &draft).is_err() && std::fs::read(&path)? == original,
        "assembly overwrote existing evidence",
    )?;
    let report = record::inspect_release(&fixture.manifest, &draft, &fixture.context)?;
    require(
        report["status"] == "blocked"
            && report["qualified"] == false
            && report["passed"] == 0
            && report["findings"]
                .as_array()
                .is_some_and(|findings| findings.len() > 2),
        "report lost independent release blockers",
    )?;
    require(
        record::validate_release(&fixture.manifest, &draft, &fixture.context).is_err(),
        "pre-freeze draft passed release validation",
    )
}

fn hash(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn text(value: &Value) -> &str {
    value.as_str().unwrap_or("")
}

fn nonempty(value: &Value) -> bool {
    value.as_str().is_some_and(|value| !value.trim().is_empty())
}

fn json_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, left)| right.get(key).is_some_and(|right| json_equal(left, right)))
        }
        _ => left == right,
    }
}

fn key(value: &Value) -> String {
    format!(
        "{}@{}",
        text(&value["case_id"]),
        text(&value["environment"])
    )
}

fn hex(value: &Value, size: usize) -> bool {
    value.as_str().is_some_and(|value| {
        value.len() == size
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn safe_integer(value: &Value) -> Option<u64> {
    value
        .as_f64()
        .filter(|value| {
            value.is_finite()
                && *value >= 0.0
                && *value <= 9_007_199_254_740_991.0
                && value.fract() == 0.0
        })
        .map(|value| value as u64)
}

#[derive(Clone)]
struct Context {
    candidate_sha: String,
    source: String,
    reviewed_legs: String,
    artifacts_root: Option<std::path::PathBuf>,
    candidate_root: Option<std::path::PathBuf>,
    provenance_path: Option<String>,
    provenance_sha256: Option<String>,
}

fn rust_name(value: &str) -> bool {
    !value.is_empty()
        && value.split("::").all(|part| {
            let mut chars = part.bytes();
            chars
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                && chars.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

fn unique(
    values: &[Value],
    identify: impl Fn(&Value) -> String,
) -> Result<std::collections::HashMap<String, &Value>> {
    let mut map = std::collections::HashMap::new();
    for value in values {
        let id = identify(value);
        require(
            map.insert(id.clone(), value).is_none(),
            format!("duplicate {id}"),
        )?;
    }
    Ok(map)
}
