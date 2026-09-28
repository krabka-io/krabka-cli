//! `LineMessageReader`, the default `--line-reader` of
//! `kafka-console-producer`, as a pure function from a line to a record.

use krabka_client_producer::{Header, ProducerRecord};

use crate::{
    connection::Properties,
    console::{is_true, raw},
};

/// How lines become records, as `LineMessageReader.configure` sets it up.
#[derive(Debug, Clone)]
pub(crate) struct LineReader {
    pub topic: String,
    pub parse_key: bool,
    pub key_separator: String,
    pub parse_headers: bool,
    pub headers_delimiter: String,
    pub headers_separator: String,
    pub headers_key_separator: String,
    pub ignore_error: bool,
    pub null_marker: Option<String>,
    headers_separator_pattern: regex::Regex,
    /// The number of lines read so far, which the error messages name.
    line_number: u64,
}

impl PartialEq for LineReader {
    fn eq(&self, other: &Self) -> bool {
        self.settings() == other.settings()
            && self.headers_separator_pattern.as_str() == other.headers_separator_pattern.as_str()
            && self.line_number == other.line_number
    }
}

impl Eq for LineReader {}

impl LineReader {
    /// Reads the reader properties as `LineMessageReader.configure` does, and
    /// refuses the same combinations with the same messages.
    ///
    /// `default_topic` is `--topic`, used when the properties set no `topic`.
    pub(crate) fn configure(properties: &Properties, default_topic: &str) -> Result<Self, String> {
        let text = |key: &str, default: &str| raw(properties, key).unwrap_or(default).to_owned();
        let flag = |key: &str| raw(properties, key).is_some_and(is_true);
        let headers_separator = text("headers.separator", ",");
        let headers_separator_pattern = regex::Regex::new(&headers_separator)
            .map_err(|error| format!("headers.separator {headers_separator}: {error}"))?;
        let reader = Self {
            topic: text("topic", default_topic),
            parse_key: flag("parse.key"),
            key_separator: text("key.separator", "\t"),
            parse_headers: flag("parse.headers"),
            headers_delimiter: text("headers.delimiter", "\t"),
            headers_separator,
            headers_key_separator: text("headers.key.separator", ":"),
            ignore_error: flag("ignore.error"),
            null_marker: raw(properties, "null.marker").map(str::to_owned),
            headers_separator_pattern,
            line_number: 0,
        };
        let marker = reader.null_marker.as_deref();
        for (equal, message) in [
            (
                reader.headers_delimiter == reader.headers_separator,
                "headers.delimiter and headers.separator may not be equal",
            ),
            (
                reader.headers_delimiter == reader.headers_key_separator,
                "headers.delimiter and headers.key.separator may not be equal",
            ),
            (
                reader.headers_separator == reader.headers_key_separator,
                "headers.separator and headers.key.separator may not be equal",
            ),
            (
                marker == Some(reader.key_separator.as_str()),
                "null.marker and key.separator may not be equal",
            ),
            (
                marker == Some(reader.headers_separator.as_str()),
                "null.marker and headers.separator may not be equal",
            ),
            (
                marker == Some(reader.headers_delimiter.as_str()),
                "null.marker and headers.delimiter may not be equal",
            ),
            (
                marker == Some(reader.headers_key_separator.as_str()),
                "null.marker and headers.key.separator may not be equal",
            ),
        ] {
            if equal {
                return Err(message.to_owned());
            }
        }
        Ok(reader)
    }

    /// The settings that decide how a line splits, for comparison.
    fn settings(&self) -> (&str, bool, &str, bool, &str, &str, &str, bool, Option<&str>) {
        (
            &self.topic,
            self.parse_key,
            &self.key_separator,
            self.parse_headers,
            &self.headers_delimiter,
            &self.headers_separator,
            &self.headers_key_separator,
            self.ignore_error,
            self.null_marker.as_deref(),
        )
    }

    /// The record for the next `line`, which carries no line terminator.
    pub(crate) fn record(&mut self, line: &str) -> Result<ProducerRecord, String> {
        self.line_number += 1;
        let headers = self.parse(
            self.parse_headers,
            line,
            0,
            &self.headers_delimiter,
            "headers delimiter",
        )?;
        let header_offset =
            headers.map_or(0, |headers| headers.len() + self.headers_delimiter.len());
        let key = self.parse(
            self.parse_key,
            line,
            header_offset,
            &self.key_separator,
            "key separator",
        )?;
        let key_offset = key.map_or(0, |key| key.len() + self.key_separator.len());
        let value = &line[header_offset + key_offset..];
        let headers = match headers {
            Some(headers) if Some(headers) != self.null_marker.as_deref() => {
                self.split_headers(headers)?
            }
            _ => Vec::new(),
        };
        Ok(ProducerRecord {
            topic: self.topic.clone(),
            partition: None,
            key: key.and_then(|key| self.bytes(key)),
            value: self.bytes(value),
            headers,
            timestamp_ms: None,
        })
    }

    /// The field before `demarcation`, from `start`, when `enabled`.
    fn parse<'a>(
        &self,
        enabled: bool,
        line: &'a str,
        start: usize,
        demarcation: &str,
        name: &str,
    ) -> Result<Option<&'a str>, String> {
        if !enabled {
            return Ok(None);
        }
        match line[start..].find(demarcation) {
            Some(index) => Ok(Some(&line[start..start + index])),
            None if self.ignore_error => Ok(None),
            None => Err(format!(
                "No {name} found on line number {}: '{line}'",
                self.line_number
            )),
        }
    }

    /// A field that equals the null marker is null.
    fn bytes<T: From<Vec<u8>>>(&self, field: &str) -> Option<T> {
        (Some(field) != self.null_marker.as_deref()).then(|| field.as_bytes().to_vec().into())
    }

    fn split_headers(&self, headers: &str) -> Result<Vec<Header>, String> {
        java_split(&self.headers_separator_pattern, headers)
            .into_iter()
            .map(|pair| {
                let Some(index) = pair.find(&self.headers_key_separator) else {
                    if self.ignore_error {
                        return Ok(Header {
                            key: pair.to_owned(),
                            value: None,
                        });
                    }
                    return Err(format!(
                        "No header key separator found in pair '{pair}' on line number {}",
                        self.line_number
                    ));
                };
                let key = &pair[..index];
                if let Some(marker) = self.null_marker.as_deref().filter(|marker| *marker == key) {
                    return Err(format!(
                        "Header keys should not be equal to the null marker '{marker}' as they can't be null"
                    ));
                }
                Ok(Header {
                    key: key.to_owned(),
                    value: self.bytes(&pair[index + self.headers_key_separator.len()..]),
                })
            })
            .collect()
    }
}

/// `Pattern.split(input)`: the whole input when nothing matches, otherwise
/// the pieces between matches with trailing empty pieces dropped, and no
/// leading empty piece for a zero-width match at the start.
fn java_split<'a>(pattern: &regex::Regex, input: &'a str) -> Vec<&'a str> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut matched = false;
    for found in pattern.find_iter(input) {
        matched = true;
        if found.end() == 0 {
            continue;
        }
        pieces.push(&input[start..found.start()]);
        start = found.end();
    }
    if !matched {
        return vec![input];
    }
    pieces.push(&input[start..]);
    while pieces.last().is_some_and(|piece| piece.is_empty()) {
        pieces.pop();
    }
    pieces
}

/// The lines of `chunk` as `BufferedReader.readLine` splits them: on `\n`,
/// `\r` or `\r\n`, without the terminator. A terminator at the end does not
/// start another line.
pub(crate) fn split_lines(chunk: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < chunk.len() {
        match chunk[index] {
            b'\n' => {
                lines.push(&chunk[start..index]);
                start = index + 1;
            }
            b'\r' => {
                lines.push(&chunk[start..index]);
                if chunk.get(index + 1) == Some(&b'\n') {
                    index += 1;
                }
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    if start < chunk.len() {
        lines.push(&chunk[start..]);
    }
    lines
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn properties(pairs: &[(&str, &str)]) -> Properties {
        let mut properties = Properties::default();
        for (key, value) in pairs {
            properties.insert(*key, *value);
        }
        properties
    }

    fn reader(pairs: &[(&str, &str)]) -> LineReader {
        LineReader::configure(&properties(pairs), "orders").expect("valid reader properties")
    }

    fn record(
        key: Option<&str>,
        value: Option<&str>,
        headers: &[(&str, Option<&str>)],
    ) -> ProducerRecord {
        let bytes = |text: &str| text.as_bytes().to_vec().into();
        ProducerRecord {
            topic: "orders".into(),
            partition: None,
            key: key.map(bytes),
            value: value.map(bytes),
            headers: headers
                .iter()
                .map(|(key, value)| Header {
                    key: (*key).to_owned(),
                    value: value.map(bytes),
                })
                .collect(),
            timestamp_ms: None,
        }
    }

    /// The key split of the issue's table: no separator, a leading one, a
    /// trailing one, a multi-character one, an empty key, and a line that
    /// holds the separator twice.
    #[test]
    fn parse_key_splits_on_the_first_separator() {
        let cases: [(&str, &str, Result<ProducerRecord, String>); 7] = [
            (":", "a:b", Ok(record(Some("a"), Some("b"), &[]))),
            (
                ":",
                "nosep",
                Err("No key separator found on line number 1: 'nosep'".into()),
            ),
            (":", ":b", Ok(record(Some(""), Some("b"), &[]))),
            (":", "a:", Ok(record(Some("a"), Some(""), &[]))),
            ("::", "a::b:c", Ok(record(Some("a"), Some("b:c"), &[]))),
            (":", ":", Ok(record(Some(""), Some(""), &[]))),
            (":", "a:b:c", Ok(record(Some("a"), Some("b:c"), &[]))),
        ];
        for (separator, line, expected) in cases {
            let mut reader = reader(&[("parse.key", "true"), ("key.separator", separator)]);
            assert!(reader.record(line) == expected, "{separator:?} {line:?}");
        }
    }

    #[test]
    fn lines_split_into_headers_key_and_value() {
        type Case<'a> = (
            &'a [(&'a str, &'a str)],
            &'a str,
            Result<ProducerRecord, String>,
        );
        let cases: [Case<'_>; 10] = [
            (
                &[],
                "plain value",
                Ok(record(None, Some("plain value"), &[])),
            ),
            (&[], "", Ok(record(None, Some(""), &[]))),
            (
                &[("parse.key", "TRUE ")],
                "k\tv",
                Ok(record(Some("k"), Some("v"), &[])),
            ),
            (
                &[("parse.headers", "true"), ("parse.key", "true")],
                "h1:v1,h2:v2\tk\tv",
                Ok(record(
                    Some("k"),
                    Some("v"),
                    &[("h1", Some("v1")), ("h2", Some("v2"))],
                )),
            ),
            (
                &[("parse.headers", "true")],
                "h1:\tv",
                Ok(record(None, Some("v"), &[("h1", Some(""))])),
            ),
            (
                &[("parse.headers", "true")],
                "bad\tv",
                Err("No header key separator found in pair 'bad' on line number 1".into()),
            ),
            (
                &[("parse.headers", "true"), ("ignore.error", "true")],
                "bad\tv",
                Ok(record(None, Some("v"), &[("bad", None)])),
            ),
            (
                &[("parse.key", "true"), ("ignore.error", "true")],
                "nosep",
                Ok(record(None, Some("nosep"), &[])),
            ),
            (
                &[
                    ("parse.headers", "true"),
                    ("parse.key", "true"),
                    ("null.marker", "~"),
                ],
                "~\t~\t~",
                Ok(record(None, None, &[])),
            ),
            (
                &[("parse.headers", "true"), ("null.marker", "~")],
                "h:~,~:x\tv",
                Err(
                    "Header keys should not be equal to the null marker '~' as they can't be null"
                        .into(),
                ),
            ),
        ];
        for (pairs, line, expected) in cases {
            assert!(reader(pairs).record(line) == expected, "{pairs:?} {line:?}");
        }
    }

    #[test]
    fn the_headers_separator_is_a_java_regular_expression() {
        let mut reader = reader(&[("parse.headers", "true"), ("headers.separator", "[;|]")]);
        assert!(
            reader.record("a:1;b:2|c:3;;\tv")
                == Ok(record(
                    None,
                    Some("v"),
                    &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]
                ))
        );
    }

    #[test]
    fn line_numbers_count_every_line_read() {
        let mut reader = reader(&[("parse.key", "true")]);
        assert!(reader.record("a\tb").is_ok());
        assert!(reader.record("c\td").is_ok());
        assert!(
            reader.record("bad") == Err("No key separator found on line number 3: 'bad'".into())
        );
    }

    #[test]
    fn a_topic_property_overrides_the_topic() {
        let mut reader = reader(&[("topic", "other")]);
        assert!(reader.record("v").map(|record| record.topic) == Ok("other".to_owned()));
    }

    #[test]
    fn conflicting_separators_are_refused_as_the_jvm_reader_refuses_them() {
        let cases: [(&[(&str, &str)], &str); 7] = [
            (
                &[("headers.separator", "\t")],
                "headers.delimiter and headers.separator may not be equal",
            ),
            (
                &[("headers.key.separator", "\t")],
                "headers.delimiter and headers.key.separator may not be equal",
            ),
            (
                &[("headers.key.separator", ",")],
                "headers.separator and headers.key.separator may not be equal",
            ),
            (
                &[("null.marker", "\t"), ("headers.delimiter", "|")],
                "null.marker and key.separator may not be equal",
            ),
            (
                &[("null.marker", ",")],
                "null.marker and headers.separator may not be equal",
            ),
            (
                &[("null.marker", "|"), ("headers.delimiter", "|")],
                "null.marker and headers.delimiter may not be equal",
            ),
            (
                &[("null.marker", ":")],
                "null.marker and headers.key.separator may not be equal",
            ),
        ];
        for (pairs, expected) in cases {
            assert!(
                LineReader::configure(&properties(pairs), "orders").map(|_| ())
                    == Err(expected.to_owned()),
                "{pairs:?}"
            );
        }
    }

    #[test]
    fn java_split_drops_trailing_empty_pieces() {
        let comma = regex::Regex::new(",").unwrap();
        let cases: [(&str, &[&str]); 6] = [
            ("a,b", &["a", "b"]),
            ("", &[""]),
            ("a", &["a"]),
            ("a,,b,,", &["a", "", "b"]),
            (",a", &["", "a"]),
            (",,", &[]),
        ];
        for (input, expected) in cases {
            assert!(java_split(&comma, input) == expected, "{input:?}");
        }
    }

    #[test]
    fn lines_split_as_buffered_reader_read_line_splits_them() {
        let cases: [(&[u8], &[&[u8]]); 8] = [
            (b"a\n", &[b"a"]),
            (b"a", &[b"a"]),
            (b"\n", &[b""]),
            (b"a\r\n", &[b"a"]),
            (b"a\rb\n", &[b"a", b"b"]),
            (b"a\r", &[b"a"]),
            (b"a\r\r\n", &[b"a", b""]),
            (b"", &[]),
        ];
        for (chunk, expected) in cases {
            assert!(split_lines(chunk) == expected, "{chunk:?}");
        }
    }
}
