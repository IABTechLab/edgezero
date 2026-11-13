use crate::evidence::{Result, require};
use serde_json::Value;
use std::{collections::BTreeMap, fs, path::Path, process::Command};

fn audit(lock: &str, graph: &Value, expected: &[[String; 3]]) -> Result<()> {
    let lock: toml::Value = toml::from_str(lock)?;
    let locked = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or("missing locked packages")?;
    let packages = graph["packages"]
        .as_array()
        .ok_or("missing metadata packages")?;
    let nodes = graph["resolve"]["nodes"]
        .as_array()
        .ok_or("missing metadata resolve nodes")?;
    let core = packages
        .iter()
        .filter(|package| package["name"] == "edgezero-core")
        .collect::<Vec<_>>();
    require(core.len() == 1, "expected one edgezero-core package")?;
    let core = core[0];
    let core_nodes = nodes
        .iter()
        .filter(|node| node["id"] == core["id"])
        .collect::<Vec<_>>();
    require(
        core_nodes.len() == 1,
        "expected one edgezero-core resolve node",
    )?;
    let declarations = core["dependencies"]
        .as_array()
        .ok_or("missing core dependencies")?;
    let edges = core_nodes[0]["dependencies"]
        .as_array()
        .ok_or("missing core dependency edges")?;
    require(!expected.is_empty(), "empty audited dependency contract")?;
    for [name, version, checksum] in expected {
        let matches = locked
            .iter()
            .filter(|package| package.get("name").and_then(toml::Value::as_str) == Some(name))
            .collect::<Vec<_>>();
        require(
            matches.len() == 1,
            format!("expected one locked {name} package"),
        )?;
        for (field, expected) in [
            ("version", version.as_str()),
            (
                "source",
                "registry+https://github.com/rust-lang/crates.io-index",
            ),
            ("checksum", checksum.as_str()),
        ] {
            require(
                matches[0].get(field).and_then(toml::Value::as_str) == Some(expected),
                format!("locked {name} {field} differs from audited contract"),
            )?;
        }
        let matches = packages
            .iter()
            .filter(|package| package["name"] == name.as_str())
            .collect::<Vec<_>>();
        require(
            matches.len() == 1 && matches[0]["version"] == version.as_str(),
            format!("expected one resolved {name} {version} package"),
        )?;
        let normal = declarations
            .iter()
            .filter(|dependency| {
                dependency["name"] == name.as_str() && dependency.get("kind") == Some(&Value::Null)
            })
            .collect::<Vec<_>>();
        require(
            normal.len() == 1 && normal[0]["req"] == format!("={version}"),
            format!("edgezero-core must declare one normal {name} dependency pinned as ={version}"),
        )?;
        require(
            edges.contains(&matches[0]["id"]),
            format!("edgezero-core is missing its direct {name} dependency"),
        )?;
    }
    Ok(())
}

fn audit_map_features(tree: &str) -> Result<()> {
    for feature in ["indexmap", "preserve_order"] {
        require(
            !tree.contains(&format!("serde_json feature \"{feature}\"")),
            format!("no-CLI graph enables serde_json/{feature}"),
        )?;
    }
    Ok(())
}

pub(super) fn run(root: &Path) -> Result<()> {
    let mut expected = BTreeMap::new();
    for path in [
        "crates/edgezero-test-harness/acceptance/dependencies/brotli.json",
        "crates/edgezero-test-harness/acceptance/dependencies/flate.json",
    ] {
        let contract: Vec<[String; 3]> = serde_json::from_slice(&fs::read(root.join(path))?)?;
        require(!contract.is_empty(), "empty audited dependency contract")?;
        for row in contract {
            if let Some(previous) = expected.insert(row[0].clone(), row.clone()) {
                require(previous == row, "conflicting audited dependency contracts")?;
            }
        }
    }
    let expected = expected.into_values().collect::<Vec<_>>();
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    for workspace in [
        ".",
        "examples/app-demo",
        "crates/edgezero-test-harness/fixtures/reusable-app",
    ] {
        let path = root.join(workspace);
        let result = (|| {
            let graph = super::output(
                Command::new(&cargo).current_dir(&path).args([
                    "metadata",
                    "--locked",
                    "--offline",
                    "--format-version",
                    "1",
                    "--all-features",
                ]),
                16 * 1024 * 1024,
            )?;
            audit(
                &fs::read_to_string(path.join("Cargo.lock"))?,
                &serde_json::from_str(&graph)?,
                &expected,
            )
        })();
        result.map_err(|error| format!("dependency contract ({workspace}): {error}"))?;
    }
    let tree = super::output(
        Command::new(&cargo).current_dir(root).args([
            "tree",
            "--locked",
            "--offline",
            "--package",
            "edgezero-cli",
            "--no-default-features",
            "--edges",
            "features,no-dev",
            "--invert",
            "serde_json",
        ]),
        16 * 1024 * 1024,
    )?;
    audit_map_features(&tree)?;
    println!("dependency contracts: Brotli, flate and no-CLI JSON map passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> (String, Value, Vec<[String; 3]>) {
        let source = "registry+https://github.com/rust-lang/crates.io-index";
        let lock = format!(
            "version = 4\n[[package]]\nname = \"codec\"\nversion = \"1.2.3\"\nsource = \"{source}\"\nchecksum = \"audited-checksum\"\n"
        );
        let graph = json!({"packages": [
            {"id":"codec-id", "name":"codec", "version":"1.2.3"},
            {"id":"core-id", "name":"edgezero-core", "dependencies":[
                {"name":"codec", "kind":null, "req":"=1.2.3"}
            ]}
        ], "resolve":{"nodes":[
            {"id":"core-id", "dependencies":["codec-id"]}
        ]}});
        let expected = vec![["codec".into(), "1.2.3".into(), "audited-checksum".into()]];
        (lock, graph, expected)
    }

    #[test]
    fn exact_normal_pins_match_the_lock_and_resolved_graph() {
        let (lock, graph, expected) = fixture();
        audit(&lock, &graph, &expected).unwrap();
    }

    #[test]
    fn dependency_audit_rejects_missing_duplicate_and_drifted_lock_packages() {
        let (lock, graph, expected) = fixture();
        for bad in [
            "not valid TOML = [".into(),
            "version = 4".into(),
            format!(
                "{lock}\n[[package]]{}",
                lock.split_once("[[package]]").unwrap().1
            ),
            lock.replace("1.2.3", "1.2.4"),
            lock.replace(
                "registry+https://github.com/rust-lang/crates.io-index",
                "git+elsewhere",
            ),
            lock.replace("audited-checksum", "different-checksum"),
        ] {
            assert!(audit(&bad, &graph, &expected).is_err(), "{bad}");
        }
    }

    #[test]
    fn dependency_audit_rejects_missing_duplicate_and_drifted_graph_packages() {
        let (lock, graph, expected) = fixture();
        for index in 0..8 {
            let mut bad = graph.clone();
            match index {
                0 => bad["packages"].as_array_mut().unwrap().clear(),
                1 => bad["packages"]
                    .as_array_mut()
                    .unwrap()
                    .push(graph["packages"][0].clone()),
                2 => bad["packages"][0]["version"] = json!("1.2.4"),
                3 => bad["packages"][1]["dependencies"][0]["kind"] = json!("dev"),
                4 => bad["packages"][1]["dependencies"][0]["req"] = json!("^1.2.3"),
                5 => bad["resolve"]["nodes"].as_array_mut().unwrap().clear(),
                6 => bad["resolve"]["nodes"][0]["dependencies"]
                    .as_array_mut()
                    .unwrap()
                    .clear(),
                7 => bad["packages"][1]["dependencies"]
                    .as_array_mut()
                    .unwrap()
                    .push(graph["packages"][1]["dependencies"][0].clone()),
                _ => unreachable!(),
            }
            assert!(audit(&lock, &bad, &expected).is_err(), "mutation {index}");
        }
    }

    #[test]
    fn no_cli_graph_keeps_the_btree_json_map() {
        audit_map_features("serde_json v1\nnormal consumer\n").unwrap();
        for feature in ["indexmap", "preserve_order"] {
            assert!(audit_map_features(&format!("serde_json feature \"{feature}\"\n")).is_err());
        }
    }
}
