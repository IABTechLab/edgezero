//! Lossless dotenv records shared by readers, writers, and preview redaction.
use regex::Regex;
use std::sync::LazyLock;

// Match dotenv's quoted alternatives as complete values, including its greedy
// escaped-quote behavior. Independently searching for the first quote corrupts
// valid values whose literal contents contain assignments or comment lines.
#[expect(
    clippy::expect_used,
    reason = "the constant grammar is compiled and exercised by unit tests"
)]
static ASSIGNMENT: LazyLock<Regex> = LazyLock::new(|| {
    let pattern = concat!(
        r"(?m)\A[\s\u{feff}]*(?:export[\s\u{feff}]+)?(?P<key>[A-Za-z0-9_.-]+)",
        r"(?P<delimiter>[\s\u{feff}]*=[\s\u{feff}]*?|:[\s\u{feff}]+?)",
        r#"(?P<value>(?P<quoted>[\s\u{feff}]*'(?:\\'|[^'])*'|[\s\u{feff}]*"(?:\\"|[^"])*"|[\s\u{feff}]*`(?:\\`|[^`])*`)|[^#\r\n]+)?"#,
        r"[\s\u{feff}]*(?:#[^\r\n]*)?(?:\r\n|\r|\n|\z)",
    ).replace(r"[\s\u{feff}]", r"[\t\n\x0B\x0C\r \u{00A0}\u{1680}\u{2000}-\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}\u{FEFF}]");
    Regex::new(&pattern).expect("constant dotenv record expression is valid")
});

/// One logical record, including its original line endings.
#[derive(Clone, Copy, Debug)]
pub struct EnvRecord<'record> {
    pub raw: &'record str,
    pub line: usize,
    pub assignment: Option<EnvAssignment<'record>>,
}

/// Parsed spans retain the consumer's complete quoted value, rather than
/// interpreting physical lines inside it as assignments.
#[derive(Clone, Copy, Debug)]
pub struct EnvAssignment<'record> {
    pub key: &'record str,
    pub prefix: &'record str,
    pub value: &'record str,
    pub commented: bool,
    pub terminated: bool,
    pub quoted: bool,
}

/// Scan logical records without changing bytes or evaluating shell syntax.
#[must_use]
#[inline]
pub fn records(body: &str) -> Vec<EnvRecord<'_>> {
    scan(body, false)
}

/// Also hide stashed assignments and ambiguous quoted tails in previews.
#[must_use]
#[inline]
pub fn redaction_records(body: &str) -> Vec<EnvRecord<'_>> {
    scan(body, true)
}

#[expect(
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    reason = "all offsets are bounded regex captures, input lengths, or ASCII delimiters"
)]
fn scan(body: &str, redact: bool) -> Vec<EnvRecord<'_>> {
    let mut result = Vec::new();
    let mut offset = 0;
    let mut line = 1;
    while offset < body.len() {
        let rest = &body[offset..];
        let first_end = physical_end(rest);
        let first = &rest[..first_end];
        let trimmed = first.trim_start_matches(dotenv_whitespace);
        let commented = trimmed.starts_with('#');
        let candidate_offset = if commented {
            first.len() - trimmed.len() + 1
        } else {
            0
        };
        let candidate = if commented && !redact {
            &first[candidate_offset..]
        } else {
            &rest[candidate_offset..]
        };
        let mut end = first_end;
        let mut assignment = None;
        if !trimmed.is_empty()
            && let Some(captures) = ASSIGNMENT.captures(candidate)
            && let Some(key) = captures.name("key")
        {
            let captured_value = captures.name("value");
            let quoted = captures.name("quoted").is_some();
            let value_start = captured_value.map_or_else(
                || {
                    captures
                        .name("delimiter")
                        .map_or(key.end(), |delimiter| delimiter.end())
                },
                |value| value.start(),
            );
            let raw_value = captured_value.map_or("", |value| value.as_str());
            let leading = raw_value.trim_start_matches(dotenv_whitespace);
            let opener = leading
                .chars()
                .next()
                .filter(|ch| matches!(ch, '\'' | '"' | '`'));
            let terminated = quoted
                || opener
                    .is_none_or(|quote| leading.get(1..).is_some_and(|tail| tail.contains(quote)));
            let value_end =
                captured_value.map_or(value_start, |value| value.end()) + candidate_offset;
            end = value_end + physical_end(&rest[value_end..]);
            if redact && !quoted && opener.is_some() {
                end = rest.len();
            }
            assignment = Some(EnvAssignment {
                key: key.as_str(),
                prefix: &rest[..candidate_offset + value_start],
                value: raw_value,
                commented,
                terminated: terminated || commented,
                quoted,
            });
        } else if redact && let Some((key, value)) = first.split_once('=') {
            // A malformed/commented key can still stash a real secret. Hide it
            // without tightening the active reader's accepted key grammar.
            let prefix_end = key.len() + 1;
            if rest[prefix_end..]
                .trim_start_matches(dotenv_whitespace)
                .starts_with(['\'', '"', '`'])
            {
                end = rest.len();
            }
            assignment = Some(EnvAssignment {
                key,
                prefix: &rest[..prefix_end],
                value,
                commented,
                terminated: true,
                quoted: false,
            });
        } else {
            // Keep standalone comments and blank records byte-for-byte.
        }
        let raw = &rest[..end];
        result.push(EnvRecord {
            raw,
            line,
            assignment,
        });
        offset += end;
        line += raw.matches('\n').count() + raw.matches('\r').count() - raw.matches("\r\n").count();
    }
    result
}

fn dotenv_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\t' | '\n' | '\x0b' | '\x0c' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn physical_end(body: &str) -> usize {
    body.find(['\r', '\n']).map_or(body.len(), |index| {
        let tail = body.get(index..).unwrap_or_default();
        index.saturating_add(if tail.starts_with("\r\n") { 2 } else { 1 })
    })
}

/// Decode the captured literal value, matching dotenv's newline handling.
/// Variable references and command substitutions remain literal text.
///
/// # Errors
/// Rejects NUL bytes before values reach a child process.
#[inline]
pub fn decode_value(assignment: EnvAssignment<'_>) -> Result<String, String> {
    let normalized = assignment.value.replace("\r\n", "\n").replace('\r', "\n");
    let raw = normalized.trim_matches(dotenv_whitespace);
    let quote = raw
        .chars()
        .next()
        .filter(|ch| matches!(ch, '\'' | '"' | '`'));
    let inner = if quote.is_some_and(|delimiter| raw.len() >= 2 && raw.ends_with(delimiter)) {
        raw.get(1..raw.len().saturating_sub(1)).unwrap_or_default()
    } else {
        raw
    };
    let value = if quote == Some('"') {
        inner.replace("\\n", "\n").replace("\\r", "\r")
    } else {
        inner.to_owned()
    };
    if assignment.key.contains('\0') || value.contains('\0') {
        return Err("env-file key or value contains a NUL byte".to_owned());
    }
    Ok(value)
}

/// Serialize one literal value without introducing dotenv interpolation or
/// changing backslashes. `always_quote` retains a provider's quoted style.
///
/// # Errors
/// Rejects control separators and values which dotenv cannot quote losslessly.
#[inline]
pub fn encode_value(value: &str, always_quote: bool) -> Result<String, String> {
    if value.contains(['\r', '\n', '\0']) {
        return Err("env-file value contains an embedded newline or NUL byte".to_owned());
    }
    let needs_quote = always_quote
        || value.trim_matches(dotenv_whitespace) != value
        || value.contains('#')
        || value.starts_with(['\'', '"', '`']);
    if !needs_quote {
        return Ok(value.to_owned());
    }
    if !value.ends_with('\\') {
        for quote in ['"', '\'', '`'] {
            if !value.contains(quote)
                && (quote != '"' || (!value.contains("\\n") && !value.contains("\\r")))
            {
                return Ok(format!("{quote}{value}{quote}"));
            }
        }
    }
    Err("env-file value cannot be represented losslessly in a single dotenv assignment".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_grammar_keeps_quoted_assignments_and_comments_in_one_record() {
        for input in [
            "TOKEN=\"first\\\\\"\n# SECRET_SENTINEL\nMANAGED=inside\nlast\"\nMANAGED=old\n",
            "TOKEN: \"first\n# SECRET_SENTINEL\nMANAGED=inside\nlast\"\nMANAGED=old\n",
            "TOKEN=\n\"first\n# SECRET_SENTINEL\nMANAGED=inside\nlast\"\nMANAGED=old\n",
            "TOKEN\n= \"first\n# SECRET_SENTINEL\nMANAGED=inside\nlast\"\nMANAGED=old\n",
            "export\tTOKEN=\u{feff}\"first\n# SECRET_SENTINEL\nMANAGED=inside\nlast\"\nMANAGED=old\n",
        ] {
            let parsed = records(input);
            assert_eq!(parsed.len(), 2, "{input:?}: {parsed:?}");
            assert!(parsed.first().unwrap().raw.contains("SECRET_SENTINEL"));
            assert_eq!(parsed.last().unwrap().assignment.unwrap().value, "old");
        }
    }

    #[test]
    fn empty_colon_values_and_non_dotenv_whitespace_keep_following_assignments() {
        let parsed = records("EMPTY: # note\nNEXT=ok\n");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.last().unwrap().assignment.unwrap().key, "NEXT");
        let nel_records = records("TOKEN=\u{0085}\"first\nMANAGED=inside\nlast\"\nMANAGED=old\n");
        assert_eq!(
            nel_records
                .iter()
                .filter_map(|record| record.assignment)
                .filter(|entry| entry.key == "MANAGED")
                .count(),
            2
        );
        let cr_records = records("FIRST=one\rSECOND=two\r");
        assert_eq!(cr_records.len(), 2);
    }

    #[test]
    fn decoding_uses_the_full_value_capture_and_normalizes_newlines() {
        for (input, expected) in [
            ("A=\"hello\"suffix\n", "\"hello\"suffix"),
            ("A=\"a\"b\"c\"\n", "a\"b\"c"),
            ("A=\"first\r\nlast\"\n", "first\nlast"),
        ] {
            let entry = records(input).first().unwrap().assignment.unwrap();
            assert_eq!(decode_value(entry).unwrap(), expected, "{input:?}");
        }
    }

    #[test]
    fn encoded_values_round_trip_without_absorbing_following_records() {
        for value in [
            "cache#blue",
            " spaced ",
            "'quoted'",
            r"literal\n",
            "has'and\"quotes#",
            "ordinary\\",
        ] {
            let encoded = encode_value(value, false).unwrap();
            let body = format!("NAME={encoded}\n# operator note '\nNEXT=ok\n");
            let parsed = records(&body);
            assert_eq!(
                decode_value(parsed.first().unwrap().assignment.unwrap()).unwrap(),
                value
            );
            assert_eq!(parsed.last().unwrap().assignment.unwrap().key, "NEXT");
        }
        for value in ["cache#blue\\", "all'\"`#quotes"] {
            encode_value(value, false).unwrap_err();
        }
    }
}
