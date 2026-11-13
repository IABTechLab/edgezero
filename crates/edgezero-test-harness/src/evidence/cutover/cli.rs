use super::*;
use std::{collections::HashMap, path::Path};

const HELP: &str = "edgezero-test-harness acceptance
  --manifest [--inventory INVENTORY.json]
  --coverage [--inventory INVENTORY.json]
  --sentinels ENVIRONMENT [--command EXACT_COMMAND] [--inventory INVENTORY.json]
  --assemble --candidate SHA --input OBSERVATIONS.json --artifacts DIRECTORY --output NEW_DRAFT.json
  --export-frozen --candidate SHA --input OBSERVATIONS.json --artifacts DIRECTORY --output NEW_RECORD.json --candidate-root CLEAN_CHECKOUT --provenance RECEIPT.json --provenance-sha256 REVIEWED_SHA256
  --report|--release --candidate SHA --record RECORD.json --artifacts DIRECTORY --candidate-root CLEAN_CHECKOUT --provenance RECEIPT.json --provenance-sha256 REVIEWED_SHA256
Record modes also accept --inventory INVENTORY.json.
Coverage exits nonzero for unbound required-local legs. Binding completeness is not passing execution or qualification.
Assembly preserves observations in an unqualified pre-freeze draft. Frozen export verifies source/build/capture provenance against an independently reviewed receipt hash; it does not create a freeze or authenticate telemetry.
Both refuse overwrites and exit nonzero for open release gates. Manifest and sentinel modes permit planned/pending legs. Approvals and raw measurements require independent review; qualified remains false.
";

fn encoded(value: &Value) -> Result<String> {
    Ok(format!("{}\n", serde_json::to_string(value)?))
}

pub(super) fn execute(arguments: &[String], root: &Path) -> Result<(i32, String)> {
    if arguments == ["--help"] {
        return Ok((0, HELP.into()));
    }
    let mode = arguments.first().map(String::as_str).unwrap_or("");
    require(
        [
            "--manifest",
            "--coverage",
            "--sentinels",
            "--assemble",
            "--export-frozen",
            "--report",
            "--release",
        ]
        .contains(&mode),
        "choose --manifest, --coverage, --sentinels, --assemble, --export-frozen, --report or --release; see --help",
    )?;
    let mut args = arguments[1..].iter();
    let environment = if mode == "--sentinels" {
        args.next().map(String::as_str)
    } else {
        None
    };
    let mut options = HashMap::new();
    while let Some(name) = args.next() {
        let value = args
            .next()
            .ok_or("unknown, duplicate or incomplete argument")?;
        require(
            [
                "--candidate",
                "--command",
                "--record",
                "--artifacts",
                "--inventory",
                "--input",
                "--output",
                "--candidate-root",
                "--provenance",
                "--provenance-sha256",
            ]
            .contains(&name.as_str())
                && !value.trim().is_empty()
                && !value.starts_with("--")
                && options.insert(name.as_str(), value.as_str()).is_none(),
            "unknown, duplicate or incomplete argument",
        )?;
    }
    let allowed: &[&str] = match mode {
        "--sentinels" => &["--inventory", "--command"],
        "--manifest" | "--coverage" => &["--inventory"],
        "--assemble" => &[
            "--inventory",
            "--candidate",
            "--input",
            "--artifacts",
            "--output",
        ],
        "--export-frozen" => &[
            "--inventory",
            "--candidate",
            "--input",
            "--artifacts",
            "--output",
            "--candidate-root",
            "--provenance",
            "--provenance-sha256",
        ],
        _ => &[
            "--inventory",
            "--candidate",
            "--record",
            "--artifacts",
            "--candidate-root",
            "--provenance",
            "--provenance-sha256",
        ],
    };
    require(
        options.keys().all(|name| allowed.contains(name)),
        "arguments do not match selected mode",
    )?;
    let inventory_path = options
        .get("--inventory")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join(INVENTORY));
    let manifest = io::read_json(&inventory_path)?;
    let source = std::fs::read_to_string(root.join(SPEC))?;
    let validation = catalog::validate_manifest(&manifest, &source, REVIEWED_LEGS)?;
    let criteria = catalog::acceptance_criteria(&source)?;
    require(
        criteria
            .as_array()
            .ok_or("invalid criteria")?
            .iter()
            .map(|row| text(&row["section"]))
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 26,
        "source must contain EZ-1 through EZ-25 and EZ-Q1",
    )?;
    if mode == "--sentinels" {
        let environment = environment
            .filter(|value| !value.trim().is_empty() && !value.starts_with("--"))
            .ok_or("sentinel listing needs an environment")?;
        let names = catalog::sentinels(&manifest, environment, options.get("--command").copied())?;
        return Ok((
            0,
            format!(
                "{}\n",
                names
                    .as_array()
                    .ok_or("invalid sentinels")?
                    .iter()
                    .map(text)
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
        ));
    }
    if mode == "--manifest" {
        let mut result = validation;
        result["status"] = json!("valid-inventory");
        result["qualified"] = json!(false);
        return Ok((0, encoded(&result)?));
    }
    if mode == "--coverage" {
        let mut coverage = catalog::coverage(&manifest, &source, REVIEWED_LEGS)?;
        let exit = i32::from(coverage["local_bindings_complete"] != true);
        coverage
            .as_object_mut()
            .ok_or("invalid coverage")?
            .remove("legs");
        return Ok((exit, encoded(&coverage)?));
    }
    let required = |name: &str| -> Result<&str> {
        options
            .get(name)
            .copied()
            .ok_or_else(|| format!("missing {name}").into())
    };
    let context = Context {
        candidate_sha: required("--candidate")?.into(),
        source,
        reviewed_legs: REVIEWED_LEGS.into(),
        artifacts_root: Some(required("--artifacts")?.into()),
        candidate_root: options
            .get("--candidate-root")
            .map(std::path::PathBuf::from),
        provenance_path: options.get("--provenance").map(|value| (*value).into()),
        provenance_sha256: options
            .get("--provenance-sha256")
            .map(|value| (*value).into()),
    };
    if mode == "--assemble" || mode == "--export-frozen" {
        let input = io::read_json(Path::new(required("--input")?))?;
        let output = Path::new(required("--output")?);
        let assembled = if mode == "--assemble" {
            record::assemble_record(&manifest, &input, &context)?
        } else {
            record::export_frozen_record(&manifest, &input, &context)?
        };
        let mut report = record::inspect_release(&manifest, &assembled, &context)?;
        io::write_new(output, &assembled)?;
        report["record_output"] = json!(output.canonicalize()?);
        return Ok((i32::from(report["status"] != "passed"), encoded(&report)?));
    }
    let input = io::read_json(Path::new(required("--record")?))?;
    let report = if mode == "--report" {
        record::inspect_release(&manifest, &input, &context)?
    } else {
        record::validate_release(&manifest, &input, &context)?
    };
    Ok((i32::from(report["status"] != "passed"), encoded(&report)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_needs_no_provider_or_inventory_and_modes_reject_ambiguous_arguments() {
        let missing = Path::new("/no-acceptance-checkout");
        let args = |values: &[&str]| {
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        };
        let (exit, output) = execute(&args(&["--help"]), missing).unwrap();
        assert_eq!(exit, 0);
        assert!(output.contains("qualified remains false"));
        for values in [
            vec![],
            vec!["--unknown"],
            vec!["--manifest", "--candidate", "a"],
            vec!["--coverage", "--inventory"],
            vec!["--coverage", "--inventory", "x", "--inventory", "y"],
            vec!["--assemble", "--record", "x"],
        ] {
            assert!(execute(&args(&values), missing).is_err(), "{values:?}");
        }
    }
}
