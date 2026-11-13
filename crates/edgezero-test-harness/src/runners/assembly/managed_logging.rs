use super::*;
use std::io::Read as _;

const LEVELS: [&str; 5] = ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"];

fn bounded_log(path: &Path) -> Result<String> {
    let mut text = String::new();
    fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_string(&mut text)?;
    require(
        text.len() <= 1024 * 1024,
        "logging evidence exceeded one MiB",
    )?;
    Ok(text)
}

fn validate_records(text: &str, token: &str, ceiling: usize, owned: bool) -> Result<()> {
    for (index, level) in LEVELS.iter().enumerate() {
        for (kind, count) in [("record", ceiling), ("boot", 2)] {
            let marker = format!("managed-logging-{kind} {token} {level}");
            require(
                text.lines().filter(|line| line.contains(&marker)).count()
                    == usize::from(index < count),
                "managed logger lost, duplicated or leaked a filtered record",
            )?;
        }
    }
    require(
        text.contains("\"event\":\"fixture_log\"") == owned,
        "application logger ownership changed",
    )
}

fn validate_owner(path: &Path, owned: bool, expected_max: &str) -> Result<()> {
    let text = bounded_log(path)?;
    let events: Vec<_> = text
        .lines()
        .filter_map(|line| {
            let start = line.find('{')?;
            serde_json::from_str::<Value>(&line[start..]).ok()
        })
        .filter(|event| event["event"] == "managed-logging-owner")
        .collect();
    let [event] = events.as_slice() else {
        return Err("missing or duplicate logger ownership witness".into());
    };
    require(
        event["owned"] == owned
            && event["after"] == expected_max
            && event["unchanged"] == true
            && event["repeated"] == true
            && event["conflict"] == true
            && (!owned || event["before"] == event["after"]),
        "logger was reinstalled, reconfigured or changed application-owned state",
    )
}

fn command(
    name: &str,
    level: Option<&str>,
    exe: &Path,
    staged: &Path,
    package: &Path,
    run: &Path,
    port: u16,
) -> Result<Command> {
    if name == "spin" {
        let mut command = provider_command(name, "per-request", exe, staged, package, run, port)?;
        if let Some(level) = level {
            command.args(["--env", &format!("EDGEZERO__LOGGING__LEVEL={level}")]);
        }
        return Ok(command);
    }
    // Logging qualification uses the supported Wrangler launcher, not the native
    // boundary launcher used to isolate upload/egress transport assertions.
    let config = run.join("managed-logging-wrangler.toml");
    fs::write(
        &config,
        wrangler_runtime_config(package, &staged.join("index.js"))?,
    )?;
    let mut command = Command::new(exe);
    let nonce = startup::command(
        &mut command,
        "fixture-cloudflare-per-request",
        &startup::build_id()?,
    );
    command
        .current_dir(package)
        .args(["dev", "--local", "--no-bundle", "--config"])
        .arg(config)
        .args([
            "--ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--log-level",
            "debug",
            "--var",
            "FIXTURE_MODE:per-request",
            "--var",
            &format!("{}:{nonce}", startup::NONCE),
        ])
        .env("WRANGLER_SEND_METRICS", "false");
    if let Some(level) = level {
        command.args(["--var", &format!("EDGEZERO__LOGGING__LEVEL:{level}")]);
    }
    Ok(command)
}

pub(super) fn run(
    name: &str,
    exe: &Path,
    staged: &Path,
    package: &Path,
    run: &Path,
    records: &mut Records,
) -> Result<()> {
    let mut failed = 0;
    for (case, level, ceiling, owned) in [
        ("default", None, 3, false),
        ("off", Some("off"), 0, false),
        ("error", Some("error"), 1, false),
        ("warn", Some("warn"), 2, false),
        ("info", Some("info"), 3, false),
        ("debug", Some("debug"), 4, false),
        ("trace", Some("trace"), 5, false),
        ("mixed", Some("DeBuG"), 4, false),
        ("invalid", Some("not-a-level"), 3, false),
        ("owned", Some("off"), 2, true),
    ] {
        let log = run.join(format!("managed-logging-{name}-{case}.log"));
        let stdout = run.join(format!("managed-logging-{name}-{case}.stdout.log"));
        let outcome = (|| -> Result<()> {
            let port = net::port()?;
            let mut cmd = command(name, level, exe, staged, package, run, port)?;
            let mut process = if name == "spin" {
                Process::start_with_stdout(&mut cmd, &log, &stdout, port)?
            } else {
                Process::start(&mut cmd, &log, port)?
            };
            let token = token();
            if name == "cloudflare" && case == "off" {
                let failure = net::request(port, "/managed-logging/failure", &token, None, None)?;
                require(
                    failure["status"] == 503,
                    "cold startup failure was not rendered",
                )?;
                await_evidence(|| {
                    require(
                        bounded_log(&log)?.contains("managed-logging-startup-failure"),
                        "cold boot failure was suppressed at ordinary Off level",
                    )
                })?;
            }
            let reply = net::request(
                port,
                if owned {
                    "/managed-logging/owned"
                } else {
                    "/managed-logging"
                },
                &token,
                None,
                None,
            )?;
            require(reply["status"] == 200, "logging handler failed")?;
            let expected_max = if owned {
                "INFO"
            } else {
                LEVELS[ceiling.max(2) - 1]
            };
            await_evidence(|| {
                let text = bounded_log(&log)?;
                validate_records(&text, &token, ceiling, owned)?;
                validate_records(&text, &format!("after-{token}"), ceiling, owned)?;
                validate_owner(&log, owned, expected_max)
            })?;
            if name == "spin" && case == "off" {
                let failure = net::request(port, "/managed-logging/failure", &token, None, None)?;
                require(
                    failure["status"] == 503,
                    "logging startup failure was not rendered",
                )?;
                await_evidence(|| {
                    require(
                        bounded_log(&log)?.contains("managed-logging-startup-failure"),
                        "boot failure was suppressed at ordinary Off level",
                    )
                })?;
            }
            process.finish()?;
            validate_records(&bounded_log(&log)?, &token, ceiling, owned)?;
            validate_records(
                &bounded_log(&log)?,
                &format!("after-{token}"),
                ceiling,
                owned,
            )?;
            records.push(
                json!({"assertion":"managed-logging","adapter":name,"case":case,
                "level":level,"ordinary_records":ceiling,"boot_records":2,
                "sink":if name == "spin" {"stderr"} else {"Wrangler console"},
                "status":"pass","qualification":false}),
            )
        })();
        let result = settle(
            records,
            &log,
            json!({"adapter":name,"case":case}),
            json!({"phase":"managed-logging"}),
            outcome,
        );
        failed += usize::from(result.is_err());
    }
    require(failed == 0, "one or more managed logging cases failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_validator_rejects_missing_duplicate_and_filtered_output() {
        let token = "probe";
        let text = "ERROR app managed-logging-record probe ERROR\nWARN app managed-logging-record probe WARN\nERROR edgezero::boot managed-logging-boot probe ERROR\nWARN edgezero::boot managed-logging-boot probe WARN\n";
        validate_records(text, token, 2, false).unwrap();
        assert!(
            validate_records(
                &text.replace("WARN app managed-logging-record probe WARN\n", ""),
                token,
                2,
                false
            )
            .is_err()
        );
        assert!(validate_records(&format!("{text}{text}"), token, 2, false).is_err());
        assert!(
            validate_records(
                &format!("{text}INFO app managed-logging-record probe INFO\n"),
                token,
                2,
                false
            )
            .is_err()
        );
    }
}
