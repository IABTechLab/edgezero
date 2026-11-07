use super::*;
use std::{fs, path::Path, process::Command};

pub(super) struct Fixture {
    pub artifacts: tempfile::TempDir,
    pub candidate: tempfile::TempDir,
    pub manifest: Value,
    pub input: Value,
    pub receipt: Value,
    pub context: Context,
}

pub(super) fn write(path: &Path, value: &Value) -> Vec<u8> {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes = serde_json::to_vec(value).unwrap();
    fs::write(path, &bytes).unwrap();
    bytes
}

impl Fixture {
    pub fn new() -> Self {
        let source = "### EZ-1. Forwarding\n- **EZ-1.1** bounded\n#### Acceptance criteria\n\n1. A proof.\n\n#### Out of scope\n";
        let criterion = catalog::acceptance_criteria(source).unwrap()[0].clone();
        let rows = ["axum", "workerd"].into_iter().map(|environment| json!({"case_id":criterion["case_id"],"source_id":criterion["case_id"],"source_digest":criterion["source_digest"],"requirement_ids":["EZ-1.1"],"environment":environment,"applicability":"blocking","approval":null,"condition":null,"owner":"EdgeZero","task":"P2","substep":"fixture","sentinel":"proof","command":"fixture proof","expected":criterion["text"],"artifact_path":format!("raw/{environment}.json"),"implementation":"ready"})).collect::<Vec<_>>();
        let manifest = json!({"schema":1,"source_digest":hash(source),"decisions":[],"cases":rows});
        Self::with_catalog(source.into(), manifest)
    }

    pub fn actual() -> Self {
        let root = crate::repository_root();
        Self::with_catalog(
            fs::read_to_string(root.join(SPEC)).unwrap(),
            io::read_json(&root.join(INVENTORY)).unwrap(),
        )
    }

    fn with_catalog(source: String, manifest: Value) -> Self {
        let artifacts = tempfile::tempdir().unwrap();
        let candidate = tempfile::tempdir().unwrap();
        fs::create_dir_all(candidate.path().join(SPEC).parent().unwrap()).unwrap();
        fs::write(candidate.path().join(SPEC), &source).unwrap();
        let inventory_bytes = write(&candidate.path().join(INVENTORY), &manifest);
        fs::write(candidate.path().join("source.rs"), "fn fixture() {}\n").unwrap();
        let mut fixture = Self {
            artifacts,
            candidate,
            manifest,
            input: Value::Null,
            receipt: Value::Null,
            context: Context {
                candidate_sha: String::new(),
                source,
                reviewed_legs: String::new(),
                artifacts_root: None,
                candidate_root: None,
                provenance_path: Some("provenance.json".into()),
                provenance_sha256: None,
            },
        };
        fixture.git(&["init", "--quiet"]);
        fixture.git(&["add", "."]);
        fixture.git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "--quiet",
            "-m",
            "synthetic frozen-source fixture",
        ]);
        let sha = fixture.git(&["rev-parse", "HEAD"]);
        let tree = fixture.git(&["rev-parse", "HEAD^{tree}"]);
        fixture.context.candidate_sha = sha.clone();
        fixture.context.artifacts_root = Some(fixture.artifacts.path().into());
        fixture.context.candidate_root = Some(fixture.candidate.path().into());
        let rows = fixture.manifest["cases"].as_array().unwrap();
        let mut ids = rows.iter().map(key).collect::<Vec<_>>();
        ids.sort();
        fixture.context.reviewed_legs = hash(serde_json::to_vec(&ids).unwrap());
        fs::create_dir_all(fixture.artifacts.path().join("build")).unwrap();
        fs::write(fixture.artifacts.path().join("build/app.bin"), b"abc").unwrap();
        let mut results = Vec::new();
        let mut applicability = Vec::new();
        let decisions = unique(fixture.manifest["decisions"].as_array().unwrap(), |row| {
            text(&row["id"]).into()
        })
        .unwrap();
        for row in rows {
            let disposition = catalog::disposition(row, &decisions).unwrap();
            applicability.push(json!({"case_id":row["case_id"],"environment":row["environment"],"disposition":disposition,"condition":row.get("condition").filter(|value| !value.is_null()).cloned().unwrap_or_else(|| json!(catalog::UNPROMOTED))}));
            let raw = write(
                &fixture.artifacts.path().join(text(&row["artifact_path"])),
                &json!({"candidate_sha":sha,"fixture":true}),
            );
            results.push(json!({"case_id":row["case_id"],"environment":row["environment"],"candidate_sha":sha,"build_id":hash(b"abc"),"source_digest":row["source_digest"],"status":"passed","command":row["command"],"sentinel":row["sentinel"],"artifact_path":row["artifact_path"],"artifact_sha256":hash(raw),"samples":1,"assertions":[{"name":"fixture proof","passed":true}],"measurements":[]}));
        }
        fixture.input = json!({"schema":1,"stage":"frozen-capture","source_state":"clean","candidate_sha":sha,"builds":[{"build_id":hash(b"abc"),"candidate_sha":sha,"artifact_path":"build/app.bin"}],"runner_image":"qualification@sha256:fixture","toolchain":{"rust":"1.98.1","fastly_sdk":"0.13.1","viceroy":"0.21.0","spin":"4.1.0","workerd":"fixture"},"retention":{"kind":"release-asset","uri":"https://example.invalid/release/evidence","expires_at":null},"comparisons":null,"applicability":applicability,"results":results});
        let recipe = write(
            &fixture.artifacts.path().join("build/recipe.json"),
            &json!({"schema":1,"candidate_sha":sha,"source_tree":tree,"argv":["synthetic-build-fixture"],"env":{}}),
        );
        fixture.receipt = json!({"schema":1,"freeze_ref":"synthetic-Q3-only","capture_ref":"synthetic-capture-only","candidate_sha":sha,"candidate_tree":tree,"source_digest":hash(&fixture.context.source),"inventory_sha256":hash(inventory_bytes),"observations":{"artifact_path":"observations.json","artifact_sha256":null},"builds":[{"build_id":hash(b"abc"),"source_tree":tree,"recipe_path":"build/recipe.json","recipe_sha256":hash(recipe)}]});
        fixture.reseal();
        fixture
    }

    pub fn git(&self, args: &[&str]) -> String {
        let mut command = Command::new("git");
        for (name, _) in
            std::env::vars_os().filter(|(name, _)| name.to_string_lossy().starts_with("GIT_"))
        {
            command.env_remove(name);
        }
        let output = command
            .args(["-C"])
            .arg(self.candidate.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    pub fn reseal(&mut self) {
        self.receipt["observations"]["artifact_sha256"] = json!(hash(write(
            &self.artifacts.path().join("observations.json"),
            &self.input
        )));
        self.context.provenance_sha256 = Some(hash(write(
            &self.artifacts.path().join("provenance.json"),
            &self.receipt,
        )));
    }

    pub fn export(&self) -> Result<Value> {
        record::export_frozen_record(&self.manifest, &self.input, &self.context)
    }
    pub fn report(&self, record: &Value) -> Value {
        record::inspect_release(&self.manifest, record, &self.context).unwrap()
    }
}
