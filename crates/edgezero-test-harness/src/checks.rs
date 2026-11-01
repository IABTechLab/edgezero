use crate::{
    CANCELLED, Process, check_cancelled,
    evidence::{Result, require},
};
use std::{
    fs,
    io::Read,
    process::Command,
    thread,
    time::{Duration, Instant},
};

mod dependencies;
mod forwarding;

#[cfg(test)]
mod tests {
    #[test]
    fn static_checks_have_one_explicit_selection_without_runtime_arguments() {
        assert!(super::select(&[]).is_ok());
        for name in ["dependencies", "forwarding", "all"] {
            assert!(super::select(&[name.into()]).is_ok(), "{name}");
        }
        for args in [vec!["unknown".into()], vec!["all".into(), "extra".into()]] {
            assert!(super::select(&args).is_err());
        }
    }

    #[test]
    fn command_capture_preserves_stdout_diagnostics_and_output_limits() {
        use std::process::Command;
        assert_eq!(
            super::output(
                Command::new("sh").args(["-c", "printf out; printf err >&2"]),
                32
            )
            .unwrap(),
            "out"
        );
        let error = super::output(
            Command::new("sh").args(["-c", "printf diagnostic >&2; exit 7"]),
            32,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("diagnostic"), "{error}");
        let error = super::output(Command::new("sh").args(["-c", "printf too-long"]), 2)
            .unwrap_err()
            .to_string();
        assert!(error.contains("output exceeded"), "{error}");
    }

    #[test]
    fn command_deadline_interrupts_a_pending_process() {
        let start = std::time::Instant::now();
        let error = super::output_until(
            std::process::Command::new("sh").args(["-c", "sleep 30"]),
            32,
            start + std::time::Duration::from_millis(50),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("exceeded its deadline"), "{error}");
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn completed_command_rechecks_stderr_after_the_last_live_size_poll() {
        let directory = tempfile::tempdir().unwrap();
        let stdout = directory.path().join("stdout");
        let stderr = directory.path().join("stderr");
        std::fs::write(&stdout, b"ok").unwrap();
        std::fs::write(&stderr, b"late stderr exceeds cap").unwrap();
        assert!(super::read_output(&stdout, &stderr, 4).is_err());
    }
}

#[derive(Clone, Copy)]
enum Selection {
    All,
    Dependencies,
    Forwarding,
}

fn select(args: &[String]) -> Result<Selection> {
    match args {
        [] => Ok(Selection::All),
        [name] => match name.as_str() {
            "all" => Ok(Selection::All),
            "dependencies" => Ok(Selection::Dependencies),
            "forwarding" => Ok(Selection::Forwarding),
            _ => Err("unknown static check; use check [all|dependencies|forwarding]".into()),
        },
        _ => Err("use check [all|dependencies|forwarding]".into()),
    }
}

pub fn run(args: &[String]) -> Result<()> {
    if args == ["--help"] {
        println!(
            "edgezero-test-harness check [all|dependencies|forwarding]\nStatic repository checks; no provider runtime or fixture output."
        );
        return Ok(());
    }
    let selection = select(args)?;
    let root = crate::repository_root();
    if matches!(selection, Selection::All | Selection::Dependencies) {
        dependencies::run(&root)?;
    }
    if matches!(selection, Selection::All | Selection::Forwarding) {
        forwarding::run(&root)?;
    }
    Ok(())
}

pub(crate) fn output(command: &mut Command, limit: u64) -> Result<String> {
    output_until(command, limit, Instant::now() + Duration::from_secs(600))
}

pub(crate) fn output_until(command: &mut Command, limit: u64, deadline: Instant) -> Result<String> {
    let directory = tempfile::tempdir()?;
    let log = directory.path().join("stderr.log");
    let stdout = directory.path().join("stdout.log");
    let mut process = Process::spawn_with_stdout(command, &log, &stdout)?;
    let result: Result<String> = (|| {
        loop {
            check_cancelled(&CANCELLED)?;
            require(
                Instant::now() < deadline,
                "check command exceeded its deadline",
            )?;
            for path in [&log, &stdout] {
                require(
                    fs::metadata(path)?.len() <= limit,
                    "check command output exceeded its limit",
                )?;
            }
            if process.child_status()?.is_some() {
                process.wait_success()?;
                return read_output(&stdout, &log, limit);
            }
            thread::sleep(Duration::from_millis(10));
        }
    })();
    match result {
        Ok(text) => Ok(text),
        Err(error) => {
            let cleanup = process.finish();
            let mut diagnostics = Vec::new();
            for path in [&stdout, &log] {
                fs::File::open(path)?
                    .take(32 * 1024)
                    .read_to_end(&mut diagnostics)?;
            }
            let retained = directory.keep();
            Err(format!(
                "check command {} failed: {error}; cleanup: {cleanup:?}; diagnostics: {}; logs: {}",
                command.get_program().to_string_lossy(),
                String::from_utf8_lossy(&diagnostics),
                retained.display()
            )
            .into())
        }
    }
}

fn read_output(stdout: &std::path::Path, stderr: &std::path::Path, limit: u64) -> Result<String> {
    for path in [stdout, stderr] {
        require(
            fs::metadata(path)?.len() <= limit,
            "check command output exceeded its limit",
        )?;
    }
    let mut bytes = Vec::new();
    fs::File::open(stdout)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 <= limit,
        "check command output exceeded its limit",
    )?;
    Ok(String::from_utf8(bytes)?)
}
