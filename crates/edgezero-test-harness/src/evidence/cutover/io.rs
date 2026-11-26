use super::*;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

const JSON_LIMIT: u64 = 16 * 1024 * 1024;
const ARTIFACT_LIMIT: u64 = 256 * 1024 * 1024;

fn open(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
    }
    Ok(options.open(path)?)
}

fn artifact_path(root: &Path, name: &str) -> Result<PathBuf> {
    require(
        !name.trim().is_empty() && catalog::safe_path(name),
        "unsafe artifact path",
    )?;
    let root = root.canonicalize()?;
    let path = root.join(name).canonicalize()?;
    require(
        path != root && path.starts_with(root),
        "artifact escapes evidence directory",
    )?;
    Ok(path)
}

fn bounded_bytes(path: &Path) -> Result<Vec<u8>> {
    let file = open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && metadata.len() <= JSON_LIMIT,
        "JSON input must be a regular file at most 16 MiB",
    )?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(JSON_LIMIT + 1).read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 == metadata.len() && bytes.len() as u64 <= JSON_LIMIT,
        "JSON artifact changed while reading",
    )?;
    Ok(bytes)
}

pub(super) fn read_json(path: &Path) -> Result<Value> {
    let bytes = bounded_bytes(&path.canonicalize()?)?;
    serde_json::from_slice(&bytes).map_err(|_| "JSON input is not valid JSON".into())
}

pub(super) fn artifact_hash(root: &Path, name: &str) -> Result<String> {
    let mut file = open(&artifact_path(root, name)?)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && metadata.len() <= ARTIFACT_LIMIT,
        "artifact must be a regular file at most 256 MiB",
    )?;
    let mut digest = Sha256::new();
    let mut bytes = [0_u8; 64 * 1024];
    let mut total = 0;
    loop {
        let read = file.read(&mut bytes)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        require(
            total <= ARTIFACT_LIMIT,
            "artifact exceeds 256 MiB while reading",
        )?;
        digest.update(&bytes[..read]);
    }
    require(total == metadata.len(), "artifact changed while reading")?;
    Ok(format!("{:x}", digest.finalize()))
}

pub(super) fn verified_bytes(root: &Path, reference: &Value, name: &str) -> Result<Vec<u8>> {
    require(
        hex(&reference["artifact_sha256"], 64),
        format!("{name} artifact hash required"),
    )?;
    let bytes = bounded_bytes(&artifact_path(root, text(&reference["artifact_path"]))?)?;
    require(
        reference["artifact_sha256"] == hash(&bytes),
        format!("{name} artifact hash mismatch"),
    )?;
    Ok(bytes)
}

pub(super) fn verified_json(root: &Path, reference: &Value, name: &str) -> Result<Value> {
    let bytes = verified_bytes(root, reference, name)?;
    serde_json::from_slice(&bytes).map_err(|_| format!("{name} artifact is not valid JSON").into())
}

fn git(root: &Path, args: &[&str]) -> Result<String> {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os().filter(|(key, _)| key.to_string_lossy().starts_with("GIT_"))
    {
        command.env_remove(key);
    }
    command
        .args(["--no-optional-locks", "-c", "core.fsmonitor=false", "-C"])
        .arg(root)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0");
    crate::checks::output_until(
        &mut command,
        1024 * 1024,
        Instant::now() + Duration::from_secs(5),
    )
    .map(|output| output.trim().to_owned())
    .map_err(|_| "cannot verify candidate checkout".into())
}

pub(super) fn verified_candidate(manifest: &Value, context: &Context) -> Result<Value> {
    let root = context
        .candidate_root
        .as_ref()
        .ok_or("explicit candidate checkout required")?
        .canonicalize()?;
    require(
        Path::new(&git(&root, &["rev-parse", "--show-toplevel"])?).canonicalize()? == root,
        "candidate checkout must be a repository root",
    )?;
    require(
        git(&root, &["rev-parse", "HEAD"])? == context.candidate_sha,
        "candidate checkout revision mismatch",
    )?;
    require(
        git(&root, &["ls-files", "-v", "-z"])?
            .split('\0')
            .filter(|entry| !entry.is_empty())
            .all(|entry| {
                entry
                    .as_bytes()
                    .first()
                    .is_some_and(|byte| *byte != b'S' && !byte.is_ascii_lowercase())
            }),
        "candidate index flags can hide source modifications",
    )?;
    require(
        git(
            &root,
            &[
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ],
        )?
        .is_empty(),
        "candidate checkout must be clean",
    )?;
    git(
        &root,
        &["ls-files", "--error-unmatch", "--", SPEC, INVENTORY],
    )?;
    let source = verified_bytes(
        &root,
        &json!({"artifact_path":SPEC,"artifact_sha256":hash(&context.source)}),
        "candidate requirements",
    )?;
    require(
        source == context.source.as_bytes(),
        "candidate requirements mismatch",
    )?;
    let inventory = bounded_bytes(&artifact_path(&root, INVENTORY)?)?;
    let decoded: Value =
        serde_json::from_slice(&inventory).map_err(|_| "candidate inventory is not valid JSON")?;
    require(
        json_equal(&decoded, manifest),
        "candidate inventory mismatch",
    )?;
    Ok(json!({"tree":git(&root, &["rev-parse", "HEAD^{tree}"])?,"inventorySha256":hash(inventory)}))
}

pub(super) fn write_new(path: &Path, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    require(
        bytes.len() as u64 <= JSON_LIMIT,
        "assembled JSON exceeds 16 MiB",
    )?;
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.write_all(&bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn bounded_json_and_streamed_artifacts_enforce_limits_and_hash_all_buffers() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("raw");
        for length in [0, 64 * 1024 - 1, 64 * 1024, 128 * 1024 + 7] {
            let bytes = vec![0x5a; length];
            fs::write(&file, &bytes).unwrap();
            assert_eq!(artifact_hash(root.path(), "raw").unwrap(), hash(&bytes));
        }
        let sparse = OpenOptions::new().write(true).open(&file).unwrap();
        sparse.set_len(ARTIFACT_LIMIT + 1).unwrap();
        assert!(
            artifact_hash(root.path(), "raw")
                .unwrap_err()
                .to_string()
                .contains("256 MiB")
        );
        sparse.set_len(JSON_LIMIT + 1).unwrap();
        assert!(read_json(&file).unwrap_err().to_string().contains("16 MiB"));
        let mut bytes = vec![b' '; JSON_LIMIT as usize];
        bytes[..2].copy_from_slice(b"{}");
        fs::write(&file, &bytes).unwrap();
        assert_eq!(read_json(&file).unwrap(), json!({}));
        fs::write(&file, b"not json").unwrap();
        assert!(read_json(&file).is_err());
        fs::write(&file, b"").unwrap();
        assert!(read_json(&file).is_err());
    }

    #[test]
    fn authenticated_json_parses_exactly_the_buffer_whose_hash_was_checked() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("raw.json");
        let bytes = serde_json::to_vec(&json!({"builds":["attested"]})).unwrap();
        fs::write(&file, &bytes).unwrap();
        let reference = json!({"artifact_path":"raw.json","artifact_sha256":hash(&bytes)});
        let authenticated = verified_bytes(root.path(), &reference, "capture").unwrap();
        fs::write(&file, serde_json::to_vec(&json!({"builds":[]})).unwrap()).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&authenticated).unwrap(),
            json!({"builds":["attested"]})
        );
        assert!(verified_json(root.path(), &reference, "capture").is_err());
    }

    #[test]
    fn traversal_directories_symlink_escapes_and_nonregular_artifacts_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("raw"), b"private").unwrap();
        fs::create_dir(root.path().join("directory")).unwrap();
        for path in [
            "../raw",
            "/raw",
            "directory",
            "",
            "nested/../raw",
            "nested\\..\\raw",
        ] {
            assert!(artifact_hash(root.path(), path).is_err(), "{path}");
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path().join("raw"), root.path().join("escaped"))
                .unwrap();
            assert!(
                artifact_hash(root.path(), "escaped")
                    .unwrap_err()
                    .to_string()
                    .contains("escapes")
            );
            let fifo = root.path().join("pending");
            let mut create = Command::new("mkfifo");
            create.arg(&fifo);
            crate::checks::output_until(&mut create, 1024, Instant::now() + Duration::from_secs(2))
                .unwrap();
            let start = Instant::now();
            assert!(
                artifact_hash(root.path(), "pending")
                    .unwrap_err()
                    .to_string()
                    .contains("regular file")
            );
            assert!(
                read_json(&fifo)
                    .unwrap_err()
                    .to_string()
                    .contains("regular file")
            );
            assert!(start.elapsed() < Duration::from_secs(1));
        }
    }

    #[test]
    fn exclusive_writes_refuse_overwrites_without_truncating_evidence() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("record.json");
        write_new(&file, &json!({"qualified":false})).unwrap();
        let original = fs::read(&file).unwrap();
        assert!(write_new(&file, &json!({"qualified":true})).is_err());
        assert_eq!(fs::read(file).unwrap(), original);
    }
}
