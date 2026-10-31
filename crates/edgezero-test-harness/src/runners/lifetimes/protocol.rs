use super::*;

pub(super) const LOG_LIMIT: usize = 1024 * 1024;

#[derive(Default)]
pub(super) struct LogCursor {
    pub(super) offset: u64,
    observed: std::cell::Cell<u64>,
}

pub(super) fn parse(prefix: &[u8], bytes: &[u8], expected: &[Value]) -> Result<Vec<Value>> {
    require(
        bytes.len() <= LOG_LIMIT,
        "cleanup runtime log exceeds bound",
    )?;
    let mut records = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.starts_with(prefix) {
            require(
                line.ends_with(b"\n")
                    || records.len() < expected.len()
                    || !prefix.starts_with(line),
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
                records.len() < expected.len(),
                "incomplete extra cleanup protocol record",
            )?;
            break;
        }
        let record = expected
            .get(records.len())
            .ok_or("extra cleanup protocol record")?;
        require(
            line[prefix.len()..line.len() - 1] == serde_json::to_vec(record)?,
            "invalid or reordered canonical cleanup record",
        )?;
        records.push(record.clone());
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
    let started = Instant::now();
    loop {
        let (records, end) = read(log, cursor, prefix, expected)?;
        if records.len() >= count {
            return Ok((records, end));
        }
        require(
            started.elapsed() < Duration::from_secs(2),
            "cleanup terminal evidence timed out",
        )?;
        thread::sleep(Duration::from_millis(5));
    }
}
