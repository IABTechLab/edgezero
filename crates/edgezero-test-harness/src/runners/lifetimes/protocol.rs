use super::*;

pub(super) const LOG_LIMIT: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct LogCursor {
    pub(super) offset: u64,
    observed: std::cell::Cell<u64>,
}

#[cfg(test)]
pub(super) fn parse(prefix: &[u8], bytes: &[u8], expected: &[Value]) -> Result<Vec<Value>> {
    parse_validated(prefix, bytes, expected.len(), &|index, record| {
        require(
            record == &expected[index],
            "invalid or reordered canonical cleanup record",
        )
    })
}

fn parse_validated(
    prefix: &[u8],
    bytes: &[u8],
    count: usize,
    validate: &dyn Fn(usize, &Value) -> Result<()>,
) -> Result<Vec<Value>> {
    require(
        bytes.len() <= LOG_LIMIT,
        "cleanup runtime log exceeds bound",
    )?;
    let mut records = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.starts_with(prefix) {
            require(
                line.ends_with(b"\n") || records.len() < count || !prefix.starts_with(line),
                "incomplete extra cleanup protocol prefix",
            )?;
            continue;
        }
        require(
            line.len() <= prefix.len() + 2049,
            "cleanup protocol record exceeds bound",
        )?;
        if !line.ends_with(b"\n") {
            require(
                records.len() < count,
                "incomplete extra cleanup protocol record",
            )?;
            break;
        }
        require(records.len() < count, "extra cleanup protocol record")?;
        let record: Value = serde_json::from_slice(&line[prefix.len()..line.len() - 1])?;
        require(
            line[prefix.len()..line.len() - 1] == serde_json::to_vec(&record)?,
            "invalid or reordered canonical cleanup record",
        )?;
        validate(records.len(), &record)?;
        records.push(record);
    }
    Ok(records)
}

pub(super) fn finish(log: &Path, cursor: &LogCursor, prefix: &[u8]) -> Result<()> {
    let bytes = read_bytes(log, cursor)?;
    require(
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .all(|line| !line.starts_with(prefix) && !prefix.starts_with(line)),
        "unexpected cleanup protocol in final runtime log tail",
    )
}

fn read_bytes(log: &Path, cursor: &LogCursor) -> Result<Vec<u8>> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let mut file = fs::File::open(log)?;
    let length = file.metadata()?.len();
    require(
        length <= LOG_LIMIT as u64 && length >= cursor.observed.get() && length >= cursor.offset,
        "cleanup log exceeded its total bound or shrank",
    )?;
    file.seek(SeekFrom::Start(cursor.offset))?;
    let remaining = LOG_LIMIT as u64 - cursor.offset;
    let mut bytes = Vec::new();
    file.take(remaining + 1).read_to_end(&mut bytes)?;
    require(
        bytes.len() as u64 <= remaining && cursor.offset + bytes.len() as u64 >= length,
        "cleanup runtime log exceeds total bound or shrank during read",
    )?;
    cursor.observed.set(cursor.offset + bytes.len() as u64);
    Ok(bytes)
}

#[cfg(test)]
pub(super) fn read(
    log: &Path,
    cursor: &LogCursor,
    prefix: &[u8],
    expected: &[Value],
) -> Result<(Vec<Value>, u64)> {
    let bytes = read_bytes(log, cursor)?;
    let records = parse(prefix, &bytes, expected)?;
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    Ok((records, cursor.offset + complete as u64))
}

pub(super) fn wait_validated(
    log: &Path,
    cursor: &LogCursor,
    prefix: &[u8],
    maximum: usize,
    count: usize,
    deadline: Instant,
    validate: &dyn Fn(usize, &Value) -> Result<()>,
) -> Result<(Vec<Value>, u64)> {
    require(count <= maximum, "invalid cleanup protocol count")?;
    loop {
        require(
            Instant::now() < deadline,
            "cleanup terminal evidence timed out",
        )?;
        let bytes = read_bytes(log, cursor)?;
        let records = parse_validated(prefix, &bytes, maximum, validate)?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or("cleanup terminal evidence timed out")?;
        if records.len() >= count {
            let complete = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |index| index + 1);
            return Ok((records, cursor.offset + complete as u64));
        }
        thread::sleep(remaining.min(Duration::from_millis(5)));
    }
}

pub(super) fn diagnose_until(
    log: &Path,
    cursor: &LogCursor,
    prefix: &[u8],
    maximum: usize,
    deadline: Instant,
) -> Value {
    let started = Instant::now();
    let remaining = deadline.saturating_duration_since(started);
    let mut diagnostic = LogCursor {
        offset: cursor.offset,
        observed: std::cell::Cell::new(cursor.observed.get()),
    };
    let mut records = Vec::new();
    let mut malformed = false;
    let mut truncated = false;
    let mut incomplete = false;
    let mut read_failed = false;
    let mut late_read = false;
    // This cursor and its records are diagnostic only; strict validation remains failed.
    while Instant::now() < deadline {
        let bytes = match read_bytes(log, &diagnostic) {
            Ok(bytes) => bytes,
            Err(_) => {
                read_failed = true;
                break;
            }
        };
        if Instant::now() >= deadline {
            late_read = true;
            break;
        }
        incomplete = false;
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            if !line.ends_with(b"\n") {
                incomplete = line.starts_with(prefix) || prefix.starts_with(line);
                malformed |= incomplete && line.len() > prefix.len() + 2049;
                break;
            }
            diagnostic.offset += line.len() as u64;
            match parse_validated(prefix, line, 1, &|_, _| Ok(())) {
                Ok(values) => {
                    for value in values {
                        if records.len() < maximum {
                            records.push(value);
                        } else {
                            truncated = true;
                        }
                    }
                }
                Err(_) => malformed = true,
            }
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        thread::sleep(remaining.min(Duration::from_millis(5)));
    }
    json!({"records":records,"malformed":malformed,"truncated":truncated,
        "incomplete":incomplete,"read_failed":read_failed,"late_read":late_read,
        "started_with_remaining_us":remaining.as_micros(),"observed_for_us":started.elapsed().as_micros(),
        "deadline_reached":Instant::now()>=deadline,"qualification":false})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_diagnostics_retain_later_records_without_accepting_or_advancing() {
        use std::io::Write as _;
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("cleanup.log");
        let prefix = b"record ";
        let first = json!({"at_us":1_000_000,"stage":"survivor-failed"});
        let late = json!({"at_us":1_200_000,"stage":"victim-closed"});
        let encode = |record: &Value| {
            let mut bytes = prefix.to_vec();
            bytes.extend(serde_json::to_vec(record).unwrap());
            bytes.push(b'\n');
            bytes
        };
        fs::write(&log, encode(&first)).unwrap();
        let cursor = LogCursor::default();
        let deadline = Instant::now() + Duration::from_millis(100);
        let failure = wait_validated(&log, &cursor, prefix, 3, 2, deadline, &|_, _| {
            Err("original strict order failure".into())
        });
        let observed = cursor.observed.get();
        let diagnostics = thread::scope(|scope| {
            let later = encode(&late);
            let output = &log;
            scope.spawn(move || {
                thread::sleep(Duration::from_millis(20));
                fs::OpenOptions::new()
                    .append(true)
                    .open(output)
                    .unwrap()
                    .write_all(&later)
                    .unwrap();
            });
            diagnose_until(&log, &cursor, prefix, 3, deadline)
        });
        assert_eq!(diagnostics["records"], json!([first, late]));
        assert_eq!(diagnostics["deadline_reached"], true);
        assert_eq!(diagnostics["qualification"], false);
        assert_eq!(
            failure.unwrap_err().to_string(),
            "original strict order failure"
        );
        assert_eq!(cursor.offset, 0);
        assert_eq!(cursor.observed.get(), observed);
        assert!(Instant::now() >= deadline);
    }

    fn diagnose(bytes: &[u8], maximum: usize) -> Value {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("cleanup.log");
        fs::write(&log, bytes).unwrap();
        diagnose_until(
            &log,
            &LogCursor::default(),
            b"record ",
            maximum,
            Instant::now() + Duration::from_millis(100),
        )
    }

    fn encode(value: &Value) -> Vec<u8> {
        let mut bytes = b"record ".to_vec();
        bytes.extend(serde_json::to_vec(value).unwrap());
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn diagnostics_skip_malformed_lines_but_retain_foreign_and_duplicate_rows() {
        let foreign = json!({"stage":"foreign"});
        let victim = json!({"stage":"victim"});
        let mut bytes =
            b"ordinary runtime log\nrecord invalid-json\nrecord {\"stage\": \"noncanonical\"}\n"
                .to_vec();
        bytes.extend(encode(&foreign));
        bytes.extend(encode(&foreign));
        bytes.extend(encode(&victim));
        let value = diagnose(&bytes, 3);
        assert_eq!(value["records"], json!([foreign, foreign, victim]));
        assert_eq!(value["malformed"], true);
        assert_eq!(value["truncated"], false);
        assert_eq!(value["incomplete"], false);
        assert_eq!(value["qualification"], false);
    }

    #[test]
    fn diagnostics_keep_the_existing_global_row_limit_without_early_success() {
        let row = json!({"stage":"duplicate"});
        let bytes = encode(&row).repeat(3);
        let value = diagnose(&bytes, 2);
        assert_eq!(value["records"], json!([row, row]));
        assert_eq!(value["truncated"], true);
        assert_eq!(value["deadline_reached"], true);
        assert_eq!(value["qualification"], false);
    }

    #[test]
    fn diagnostics_flag_incomplete_matching_prefixes_and_oversize_records() {
        for partial in [b"rec".as_slice(), b"record {\"stage\":"] {
            let value = diagnose(partial, 1);
            assert_eq!(value["records"], json!([]));
            assert_eq!(value["incomplete"], true);
            assert_eq!(value["malformed"], false);
        }
        let mut oversized = b"record ".to_vec();
        oversized.extend(vec![b'x'; 2050]);
        let value = diagnose(&oversized, 1);
        assert_eq!(value["incomplete"], true);
        assert_eq!(value["malformed"], true);
        oversized.push(b'\n');
        oversized.extend(encode(&json!({"stage":"later"})));
        let value = diagnose(&oversized, 1);
        assert_eq!(value["records"], json!([{"stage":"later"}]));
        assert_eq!(value["incomplete"], false);
        assert_eq!(value["malformed"], true);
    }

    #[test]
    fn diagnostic_partial_lines_are_decoded_once_after_completion() {
        use std::io::Write as _;
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("cleanup.log");
        let value = json!({"stage":"closed"});
        let bytes = encode(&value);
        fs::write(&log, &bytes[..3]).unwrap();
        let captured = thread::scope(|scope| {
            let output = &log;
            let tail = &bytes[3..];
            scope.spawn(move || {
                thread::sleep(Duration::from_millis(20));
                fs::OpenOptions::new()
                    .append(true)
                    .open(output)
                    .unwrap()
                    .write_all(tail)
                    .unwrap();
            });
            diagnose_until(
                &log,
                &LogCursor::default(),
                b"record ",
                1,
                Instant::now() + Duration::from_millis(100),
            )
        });
        assert_eq!(captured["records"], json!([value]));
        assert_eq!(captured["malformed"], false);
        assert_eq!(captured["incomplete"], false);
        assert_eq!(captured["truncated"], false);
    }

    #[test]
    fn diagnostic_expiry_does_not_read_or_reset_the_original_cursor() {
        let cursor = LogCursor {
            offset: 17,
            observed: std::cell::Cell::new(100),
        };
        let value = diagnose_until(
            Path::new("does-not-exist"),
            &cursor,
            b"record ",
            1,
            Instant::now() - Duration::from_millis(1),
        );
        assert_eq!(value["records"], json!([]));
        assert_eq!(value["read_failed"], false);
        assert_eq!(value["started_with_remaining_us"], 0);
        assert_eq!(value["deadline_reached"], true);
        assert_eq!(cursor.offset, 17);
        assert_eq!(cursor.observed.get(), 100);
    }

    #[test]
    fn diagnostic_log_shrink_or_total_overflow_stops_without_resetting_strict_state() {
        for bytes in [vec![b'x'], vec![b'x'; LOG_LIMIT + 1]] {
            let directory = tempfile::tempdir().unwrap();
            let log = directory.path().join("cleanup.log");
            fs::write(&log, bytes).unwrap();
            let cursor = LogCursor {
                offset: 0,
                observed: std::cell::Cell::new(2),
            };
            let value = diagnose_until(
                &log,
                &cursor,
                b"record ",
                1,
                Instant::now() + Duration::from_secs(1),
            );
            assert_eq!(value["read_failed"], true);
            assert_eq!(value["records"], json!([]));
            assert_eq!(cursor.offset, 0);
            assert_eq!(cursor.observed.get(), 2);
        }
    }

    #[test]
    fn dynamic_validation_preserves_canonical_bounds_and_duplicate_tail_rejection() {
        let prefix = b"record ";
        let encode = |value: &Value| {
            let mut bytes = prefix.to_vec();
            bytes.extend(serde_json::to_vec(value).unwrap());
            bytes.push(b'\n');
            bytes
        };
        let valid = encode(&json!({"measured":100}));
        let validate = |index, record: &Value| {
            require(
                index == 0 && record["measured"] == 100,
                "invalid measurement",
            )
        };
        assert_eq!(
            parse_validated(prefix, &valid, 1, &validate).unwrap().len(),
            1
        );
        assert!(parse_validated(prefix, &encode(&json!({"measured":200})), 1, &validate).is_err());
        let mut duplicate = valid.clone();
        duplicate.extend(&valid);
        assert!(parse_validated(prefix, &duplicate, 1, &validate).is_err());
        for extra in [b"rec".as_slice(), b"record {", b"record {}\n"] {
            let mut tail = valid.clone();
            tail.extend(extra);
            assert!(parse_validated(prefix, &tail, 1, &validate).is_err());
        }
        assert!(parse_validated(prefix, &vec![b'x'; LOG_LIMIT + 1], 1, &validate).is_err());
        let oversized = encode(&json!({"measured":"x".repeat(2048)}));
        assert!(parse_validated(prefix, &oversized, 1, &|_, _| Ok(())).is_err());
        assert!(parse_validated(prefix, b"record {\"measured\": 100}\n", 1, &validate).is_err());
    }
}
