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

pub(super) fn wait(
    log: &Path,
    cursor: &LogCursor,
    prefix: &[u8],
    expected: &[Value],
    count: usize,
) -> Result<(Vec<Value>, u64)> {
    wait_validated(
        log,
        cursor,
        prefix,
        expected.len(),
        count,
        &|index, record| {
            require(
                record == &expected[index],
                "invalid or reordered canonical cleanup record",
            )
        },
    )
}

pub(super) fn wait_validated(
    log: &Path,
    cursor: &LogCursor,
    prefix: &[u8],
    maximum: usize,
    count: usize,
    validate: &dyn Fn(usize, &Value) -> Result<()>,
) -> Result<(Vec<Value>, u64)> {
    require(count <= maximum, "invalid cleanup protocol count")?;
    let started = Instant::now();
    loop {
        let bytes = read_bytes(log, cursor)?;
        let records = parse_validated(prefix, &bytes, maximum, validate)?;
        if records.len() >= count {
            let complete = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |index| index + 1);
            return Ok((records, cursor.offset + complete as u64));
        }
        require(
            started.elapsed() < Duration::from_secs(2),
            "cleanup terminal evidence timed out",
        )?;
        thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
