//! The `--formatter` classes of `kafka-console-consumer`, as pure functions
//! from a record and the `--formatter-property` settings to the bytes that the
//! JVM tool writes.

use std::{
    collections::BTreeSet,
    fmt::{self, Write as _},
    ops::Bound::{Excluded, Unbounded},
};

use crate::{
    connection::Properties,
    console::{is_true, raw},
};

/// The formatter class that the JVM tool uses by default.
pub(crate) const DEFAULT_FORMATTER: &str =
    "org.apache.kafka.tools.consumer.DefaultMessageFormatter";
const LOGGING_FORMATTER: &str = "org.apache.kafka.tools.consumer.LoggingMessageFormatter";
const NO_OP_FORMATTER: &str = "org.apache.kafka.tools.consumer.NoOpMessageFormatter";
/// The formatters that decode Kafka's internal topics. They are Kafka
/// tooling for `__consumer_offsets` and friends, which krabka does not decode.
const INTERNAL_FORMATTERS: [&str; 5] = [
    "OffsetsMessageFormatter",
    "GroupMetadataMessageFormatter",
    "TransactionLogMessageFormatter",
    "ShareGroupMessageFormatter",
    "CoordinatorRecordMessageFormatter",
];
const SERIALIZATION: &str = "org.apache.kafka.common.serialization.";

/// One consumed record, as the formatter sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// The record timestamp. The pinned client does not report the timestamp
    /// type, so it prints as `CreateTime`.
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// A `--formatter` class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Formatter {
    /// `DefaultMessageFormatter`.
    Default(DefaultFormatter),
    /// `LoggingMessageFormatter`: the default output, and an INFO log line.
    Logging(DefaultFormatter),
    /// `NoOpMessageFormatter`: nothing.
    NoOp,
}

impl Formatter {
    /// The formatter that `--formatter class` names, configured from the
    /// formatter properties.
    ///
    /// A class may be named in full or by its simple name. The second list
    /// returned is the warnings that the JVM tool logs while it configures the
    /// formatter.
    pub(crate) fn new(class: &str, properties: &Properties) -> Result<(Self, Vec<String>), String> {
        let simple = class.rsplit('.').next().unwrap_or(class);
        let named = |full: &str| class == full || class == simple_name(full);
        if named(DEFAULT_FORMATTER) {
            let (formatter, warnings) = DefaultFormatter::configure(properties);
            return Ok((Self::Default(formatter), warnings));
        }
        if named(LOGGING_FORMATTER) {
            let (formatter, warnings) = DefaultFormatter::configure(properties);
            return Ok((Self::Logging(formatter), warnings));
        }
        if named(NO_OP_FORMATTER) {
            return Ok((Self::NoOp, Vec::new()));
        }
        if INTERNAL_FORMATTERS.contains(&simple) {
            return Err(format!(
                "{class}: decoding Kafka's internal topics is not supported by krabka console-consumer"
            ));
        }
        Err(format!(
            "{class}: no such formatter; krabka builds in {}, {} and {}",
            simple_name(DEFAULT_FORMATTER),
            simple_name(LOGGING_FORMATTER),
            simple_name(NO_OP_FORMATTER),
        ))
    }

    /// The bytes written to stdout for `record`, and for `LoggingMessageFormatter`
    /// the line that it logs.
    pub(crate) fn format(&self, record: &Record) -> Result<(Vec<u8>, Option<String>), String> {
        match self {
            Self::Default(formatter) => Ok((formatter.format(record)?, None)),
            Self::Logging(formatter) => Ok((formatter.format(record)?, Some(log_line(record)))),
            Self::NoOp => Ok((Vec::new(), None)),
        }
    }

    /// The deserializer that renders a record's value in JSON output, and the
    /// literal that stands for a null field.
    pub(crate) fn render_value(
        &self,
        bytes: Option<&[u8]>,
        field: Field,
    ) -> Result<Option<String>, String> {
        let (Self::Default(formatter) | Self::Logging(formatter)) = self else {
            return Ok(bytes.map(|bytes| String::from_utf8_lossy(bytes).into_owned()));
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let deserializer = match field {
            Field::Key => formatter.key_deserializer,
            Field::Value => formatter.value_deserializer,
            Field::Header => formatter.headers_deserializer,
        };
        let rendered = deserializer.map_or_else(|| Ok(bytes.to_vec()), |d| d.deserialize(bytes))?;
        Ok(Some(String::from_utf8_lossy(&rendered).into_owned()))
    }
}

/// Which deserializer of a formatter applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Field {
    Key,
    Value,
    Header,
}

fn simple_name(class: &str) -> &str {
    class.rsplit('.').next().unwrap_or(class)
}

/// The line that `LoggingMessageFormatter` logs at INFO.
fn log_line(record: &Record) -> String {
    let key = record.key.as_deref().map_or_else(
        || "null ".to_owned(),
        |key| format!("{}, ", String::from_utf8_lossy(key)),
    );
    let value = record.value.as_deref().map_or_else(
        || "null".to_owned(),
        |value| String::from_utf8_lossy(value).into_owned(),
    );
    format!("CreateTime:{}, key:{key}value:{value}", record.timestamp)
}

/// A field that `DefaultMessageFormatter` can print, in the order it prints
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Column {
    Timestamp,
    Partition,
    Offset,
    Delivery,
    Epoch,
    Headers,
    Key,
    Value,
}

impl Column {
    /// The `print.*` property that turns the column on or off.
    const fn property(self) -> &'static str {
        match self {
            Self::Timestamp => "print.timestamp",
            Self::Partition => "print.partition",
            Self::Offset => "print.offset",
            Self::Delivery => "print.delivery",
            Self::Epoch => "print.epoch",
            Self::Headers => "print.headers",
            Self::Key => "print.key",
            Self::Value => "print.value",
        }
    }

    const ALL: [Self; 8] = [
        Self::Timestamp,
        Self::Partition,
        Self::Offset,
        Self::Delivery,
        Self::Epoch,
        Self::Headers,
        Self::Key,
        Self::Value,
    ];
}

/// `DefaultMessageFormatter` and the settings that `configure` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DefaultFormatter {
    /// The columns that print. Only the value prints by default.
    pub columns: BTreeSet<Column>,
    pub key_separator: Vec<u8>,
    pub line_separator: Vec<u8>,
    pub headers_separator: Vec<u8>,
    pub null_literal: Vec<u8>,
    pub key_deserializer: Option<Deserializer>,
    pub value_deserializer: Option<Deserializer>,
    pub headers_deserializer: Option<Deserializer>,
}

impl Default for DefaultFormatter {
    fn default() -> Self {
        Self {
            columns: BTreeSet::from([Column::Value]),
            key_separator: b"\t".to_vec(),
            line_separator: b"\n".to_vec(),
            headers_separator: b",".to_vec(),
            null_literal: b"null".to_vec(),
            key_deserializer: None,
            value_deserializer: None,
            headers_deserializer: None,
        }
    }
}

impl DefaultFormatter {
    /// Reads the formatter properties as `DefaultMessageFormatter.configure`
    /// does. An unknown deserializer class is logged and ignored, as the JVM
    /// formatter does.
    pub(crate) fn configure(properties: &Properties) -> (Self, Vec<String>) {
        let mut formatter = Self::default();
        let mut warnings = Vec::new();
        for column in Column::ALL {
            if let Some(value) = raw(properties, column.property()) {
                if is_true(value) {
                    formatter.columns.insert(column);
                } else {
                    formatter.columns.remove(&column);
                }
            }
        }
        for (key, bytes) in [
            ("key.separator", &mut formatter.key_separator),
            ("line.separator", &mut formatter.line_separator),
            ("headers.separator", &mut formatter.headers_separator),
            ("null.literal", &mut formatter.null_literal),
        ] {
            if let Some(value) = raw(properties, key) {
                *bytes = value.as_bytes().to_vec();
            }
        }
        for (key, slot) in [
            ("key.deserializer", &mut formatter.key_deserializer),
            ("value.deserializer", &mut formatter.value_deserializer),
            ("headers.deserializer", &mut formatter.headers_deserializer),
        ] {
            if let Some(name) = raw(properties, key) {
                match Deserializer::from_class(name) {
                    Some(deserializer) => *slot = Some(deserializer),
                    None => {
                        warnings.push(format!("Unable to instantiate a deserializer from {name}"));
                    }
                }
            }
        }
        (formatter, warnings)
    }

    /// The bytes that `DefaultMessageFormatter.writeTo` writes for `record`.
    pub(crate) fn format(&self, record: &Record) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for &column in &self.columns {
            match column {
                Column::Timestamp => {
                    out.extend_from_slice(format!("CreateTime:{}", record.timestamp).as_bytes());
                }
                Column::Partition => {
                    out.extend_from_slice(format!("Partition:{}", record.partition).as_bytes());
                }
                Column::Offset => {
                    out.extend_from_slice(format!("Offset:{}", record.offset).as_bytes());
                }
                // A classic consumer has no delivery count.
                Column::Delivery => out.extend_from_slice(b"Delivery:NOT_PRESENT"),
                // ConsoleConsumer hands the formatter a copy of the record
                // without its leader epoch, so the JVM tool always prints
                // NOT_PRESENT.
                Column::Epoch => out.extend_from_slice(b"Epoch:NOT_PRESENT"),
                Column::Headers => {
                    if record.headers.is_empty() {
                        out.extend_from_slice(b"NO_HEADERS");
                    }
                    for (index, (key, value)) in record.headers.iter().enumerate() {
                        if index > 0 {
                            out.extend_from_slice(&self.headers_separator);
                        }
                        out.extend_from_slice(key.as_bytes());
                        out.push(b':');
                        out.extend(self.deserialize(self.headers_deserializer, value.as_deref())?);
                    }
                }
                Column::Key => {
                    out.extend(self.deserialize(self.key_deserializer, record.key.as_deref())?);
                }
                Column::Value => {
                    out.extend(self.deserialize(self.value_deserializer, record.value.as_deref())?);
                }
            }
            // A column is followed by the key separator when a later column
            // prints, and by the line separator when it is the last. The value
            // is always last.
            let later = self
                .columns
                .range((Excluded(column), Unbounded))
                .next()
                .is_some();
            out.extend_from_slice(if later {
                &self.key_separator
            } else {
                &self.line_separator
            });
        }
        Ok(out)
    }

    /// A null field is the null literal, and the deserializer then reads
    /// those bytes, as the JVM formatter does.
    fn deserialize(
        &self,
        deserializer: Option<Deserializer>,
        bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        let bytes = bytes.unwrap_or(&self.null_literal);
        deserializer.map_or_else(|| Ok(bytes.to_vec()), |d| d.deserialize(bytes))
    }
}

/// The `org.apache.kafka.common.serialization` deserializers that the
/// formatter can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deserializer {
    String,
    ByteArray,
    Bytes,
    ByteBuffer,
    Short,
    Integer,
    Long,
    Float,
    Double,
    Uuid,
    Boolean,
    Void,
}

impl Deserializer {
    fn from_class(name: &str) -> Option<Self> {
        let simple = name.strip_prefix(SERIALIZATION).unwrap_or(name);
        Some(match simple {
            "StringDeserializer" => Self::String,
            "ByteArrayDeserializer" => Self::ByteArray,
            "BytesDeserializer" => Self::Bytes,
            "ByteBufferDeserializer" => Self::ByteBuffer,
            "ShortDeserializer" => Self::Short,
            "IntegerDeserializer" => Self::Integer,
            "LongDeserializer" => Self::Long,
            "FloatDeserializer" => Self::Float,
            "DoubleDeserializer" => Self::Double,
            "UUIDDeserializer" => Self::Uuid,
            "BooleanDeserializer" => Self::Boolean,
            "VoidDeserializer" => Self::Void,
            _ => return None,
        })
    }

    /// The UTF-8 bytes of `deserialize(bytes).toString()`, or the message of
    /// the exception that the JVM deserializer throws.
    pub(crate) fn deserialize(self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        let text = match self {
            Self::String => String::from_utf8_lossy(bytes).into_owned(),
            // `byte[].toString()` is an identity hash; the raw bytes are the
            // only useful rendering.
            Self::ByteArray => return Ok(bytes.to_vec()),
            Self::Bytes => printable(bytes),
            Self::ByteBuffer => format!(
                "java.nio.HeapByteBuffer[pos=0 lim={0} cap={0}]",
                bytes.len()
            ),
            Self::Short => i16::from_be_bytes(sized(bytes, "ShortDeserializer")?).to_string(),
            Self::Integer => i32::from_be_bytes(sized(bytes, "IntegerDeserializer")?).to_string(),
            Self::Long => i64::from_be_bytes(sized(bytes, "LongDeserializer")?).to_string(),
            Self::Float => java_decimal(f32::from_be_bytes(sized(bytes, "Deserializer")?)),
            Self::Double => java_decimal(f64::from_be_bytes(sized(bytes, "Deserializer")?)),
            Self::Uuid => uuid(&String::from_utf8_lossy(bytes))
                .ok_or_else(|| "Error parsing data into UUID".to_owned())?,
            Self::Boolean => match bytes {
                [1] => "true".to_owned(),
                [0] => "false".to_owned(),
                [other] => {
                    return Err(format!(
                        "Unexpected byte received by BooleanDeserializer: {}",
                        other.cast_signed()
                    ));
                }
                _ => return Err("Size of data received by BooleanDeserializer is not 1".into()),
            },
            // The formatter never passes null, so VoidDeserializer always
            // refuses, as it does in the JVM tool.
            Self::Void => return Err("Data should be null for a VoidDeserializer.".into()),
        };
        Ok(text.into_bytes())
    }
}

fn sized<const N: usize>(bytes: &[u8], class: &str) -> Result<[u8; N], String> {
    bytes
        .try_into()
        .map_err(|_| format!("Size of data received by {class} is not {N}"))
}

/// `org.apache.kafka.common.utils.Bytes.toString`: printable ASCII other than
/// the backslash as itself, every other byte as `\xNN`.
fn printable(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &byte in bytes {
        if (b' '..=b'~').contains(&byte) && byte != b'\\' {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "\\x{byte:02X}");
        }
    }
    out
}

/// `UUID.fromString(text).toString()`: five hex fields, each no wider than
/// its canonical width, rendered canonical and lower-case.
fn uuid(text: &str) -> Option<String> {
    let fields = text.split('-').collect::<Vec<_>>();
    let widths = [8, 4, 4, 4, 12];
    if fields.len() != widths.len() {
        return None;
    }
    let mut out = String::new();
    for (index, (field, width)) in fields.iter().zip(widths).enumerate() {
        if field.is_empty() || field.len() > width {
            return None;
        }
        let value = u64::from_str_radix(field, 16).ok()?;
        if index > 0 {
            out.push('-');
        }
        let _ = write!(out, "{value:0width$x}");
    }
    Some(out)
}

/// `Double.toString` and `Float.toString`.
///
/// Both the JVM and Rust print the shortest decimal that reads back as the
/// same value. The JVM prints plain notation, with at least one fraction
/// digit, for magnitudes in `[1e-3, 1e7)`, and `d.dddE±n` otherwise.
pub(crate) fn java_decimal<T>(value: T) -> String
where
    T: fmt::Display + fmt::LowerExp + Into<f64> + Copy,
{
    let wide: f64 = value.into();
    if wide.is_nan() {
        return "NaN".into();
    }
    let sign = if wide.is_sign_negative() { "-" } else { "" };
    if wide.is_infinite() {
        return format!("{sign}Infinity");
    }
    if wide == 0.0 {
        return format!("{sign}0.0");
    }
    if (1e-3..1e7).contains(&wide.abs()) {
        let plain = value.to_string();
        return if plain.contains('.') {
            plain
        } else {
            format!("{plain}.0")
        };
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .unwrap_or((scientific.as_str(), "0"));
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
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

    fn record() -> Record {
        Record {
            topic: "orders".into(),
            partition: 2,
            offset: 41,
            timestamp: 1_790_600_093_698,
            key: Some(b"k1".to_vec()),
            value: Some(b"v1".to_vec()),
            headers: vec![("h1".into(), Some(b"x".to_vec())), ("h2".into(), None)],
        }
    }

    fn formatted(pairs: &[(&str, &str)], record: &Record) -> String {
        let (formatter, warnings) = DefaultFormatter::configure(&properties(pairs));
        assert!(warnings.is_empty());
        String::from_utf8(formatter.format(record).expect("formats")).unwrap()
    }

    type FormatCase<'a> = (&'a [(&'a str, &'a str)], &'a Record, &'a str);

    /// Every `print.*` flag, with the separators the JVM formatter puts
    /// between fields. The first row is the line that
    /// `kafka-console-consumer` 4.3.1 printed for the same flags.
    #[test]
    fn the_default_formatter_prints_the_fields_that_are_asked_for() {
        let all = [
            ("print.timestamp", "true"),
            ("print.partition", "true"),
            ("print.offset", "true"),
            ("print.epoch", "true"),
            ("print.headers", "true"),
            ("print.key", "true"),
        ];
        let no_headers = Record {
            headers: Vec::new(),
            ..record()
        };
        let cases: [FormatCase<'_>; 9] = [
            (
                &all,
                &no_headers,
                "CreateTime:1790600093698\tPartition:2\tOffset:41\tEpoch:NOT_PRESENT\tNO_HEADERS\tk1\tv1\n",
            ),
            (&[], &record(), "v1\n"),
            (&[("print.key", "true")], &record(), "k1\tv1\n"),
            (
                &[
                    ("print.key", "true"),
                    ("key.separator", ":"),
                    ("line.separator", "|"),
                ],
                &record(),
                "k1:v1|",
            ),
            (
                &[("print.value", "false"), ("print.key", "true")],
                &record(),
                "k1\n",
            ),
            (&[("print.value", "false")], &record(), ""),
            (
                &[("print.headers", "true"), ("headers.separator", ";")],
                &record(),
                "h1:x;h2:null\tv1\n",
            ),
            (
                &[("print.delivery", " TRUE "), ("print.value", "no")],
                &record(),
                "Delivery:NOT_PRESENT\n",
            ),
            (
                &[("print.key", "true"), ("null.literal", "<nil>")],
                &Record {
                    key: None,
                    value: None,
                    ..record()
                },
                "<nil>\t<nil>\n",
            ),
        ];
        for (pairs, record, expected) in cases {
            assert!(formatted(pairs, record) == expected, "{pairs:?}");
        }
    }

    #[test]
    fn a_deserializer_reads_the_field_and_a_null_reads_the_null_literal() {
        let record = Record {
            key: Some(42_i32.to_be_bytes().to_vec()),
            value: None,
            ..record()
        };
        let pairs = [
            ("print.key", "true"),
            (
                "key.deserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer",
            ),
            (
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
            ),
        ];
        assert!(formatted(&pairs, &record) == "42\tnull\n");
    }

    #[test]
    fn a_failing_deserializer_fails_the_record() {
        let (formatter, _) = DefaultFormatter::configure(&properties(&[(
            "value.deserializer",
            "org.apache.kafka.common.serialization.LongDeserializer",
        )]));
        assert!(
            formatter.format(&record())
                == Err("Size of data received by LongDeserializer is not 8".into())
        );
    }

    #[test]
    fn an_unknown_deserializer_is_logged_and_ignored() {
        let (formatter, warnings) =
            DefaultFormatter::configure(&properties(&[("value.deserializer", "com.example.Avro")]));
        assert!(
            (formatter, warnings)
                == (
                    DefaultFormatter::default(),
                    vec!["Unable to instantiate a deserializer from com.example.Avro".to_owned()]
                )
        );
    }

    #[test]
    fn deserializers_render_as_the_jvm_to_string_does() {
        let cases: [(Deserializer, &[u8], Result<&str, &str>); 20] = [
            (Deserializer::String, b"caf\xc3\xa9", Ok("caf\u{e9}")),
            (Deserializer::ByteArray, b"\x00\x01", Ok("\u{0}\u{1}")),
            (
                Deserializer::Bytes,
                b"a\\\x00~\x7f",
                Ok("a\\x5C\\x00~\\x7F"),
            ),
            (
                Deserializer::ByteBuffer,
                b"abc",
                Ok("java.nio.HeapByteBuffer[pos=0 lim=3 cap=3]"),
            ),
            (Deserializer::Short, &[0xff, 0xfe], Ok("-2")),
            (Deserializer::Integer, &[0, 0, 1, 0], Ok("256")),
            (
                Deserializer::Integer,
                b"abc",
                Err("Size of data received by IntegerDeserializer is not 4"),
            ),
            (Deserializer::Long, &[0, 0, 0, 0, 0, 0, 0, 7], Ok("7")),
            (Deserializer::Float, &1.5_f32.to_be_bytes(), Ok("1.5")),
            (
                Deserializer::Float,
                b"ab",
                Err("Size of data received by Deserializer is not 4"),
            ),
            (Deserializer::Double, &1e7_f64.to_be_bytes(), Ok("1.0E7")),
            (
                Deserializer::Uuid,
                b"123E4567-E89B-12D3-A456-426614174000",
                Ok("123e4567-e89b-12d3-a456-426614174000"),
            ),
            (
                Deserializer::Uuid,
                b"1-2-3-4-5",
                Ok("00000001-0002-0003-0004-000000000005"),
            ),
            (
                Deserializer::Uuid,
                b"not-a-uuid",
                Err("Error parsing data into UUID"),
            ),
            (Deserializer::Boolean, &[1], Ok("true")),
            (Deserializer::Boolean, &[0], Ok("false")),
            (
                Deserializer::Boolean,
                &[0xff],
                Err("Unexpected byte received by BooleanDeserializer: -1"),
            ),
            (
                Deserializer::Boolean,
                &[1, 1],
                Err("Size of data received by BooleanDeserializer is not 1"),
            ),
            (
                Deserializer::Void,
                b"",
                Err("Data should be null for a VoidDeserializer."),
            ),
            (
                Deserializer::Short,
                &[1],
                Err("Size of data received by ShortDeserializer is not 2"),
            ),
        ];
        for (deserializer, bytes, expected) in cases {
            let actual = deserializer.deserialize(bytes);
            let expected = expected
                .map(|text| text.as_bytes().to_vec())
                .map_err(str::to_owned);
            assert!(actual == expected, "{deserializer:?} {bytes:?}");
        }
    }

    #[test]
    fn decimals_render_as_java_to_string_does() {
        let doubles = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (0.001, "0.001"),
            (0.000_1, "1.0E-4"),
            (1.234_5e-5, "1.2345E-5"),
            (9_999_999.0, "9999999.0"),
            (1e7, "1.0E7"),
            (1.5e300, "1.5E300"),
            (-2.5, "-2.5"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (value, expected) in doubles {
            assert!(java_decimal(value) == expected, "{value}");
        }
        for (value, expected) in [(0.1_f32, "0.1"), (3.0e10_f32, "3.0E10"), (-0.0_f32, "-0.0")] {
            assert!(java_decimal(value) == expected, "{value}");
        }
    }

    #[test]
    fn formatter_classes_resolve_by_full_or_simple_name() {
        let empty = Properties::default();
        let cases = [
            (DEFAULT_FORMATTER, Ok(Formatter::Default(DefaultFormatter::default()))),
            ("DefaultMessageFormatter", Ok(Formatter::Default(DefaultFormatter::default()))),
            (LOGGING_FORMATTER, Ok(Formatter::Logging(DefaultFormatter::default()))),
            ("NoOpMessageFormatter", Ok(Formatter::NoOp)),
            (
                "org.apache.kafka.tools.consumer.OffsetsMessageFormatter",
                Err("org.apache.kafka.tools.consumer.OffsetsMessageFormatter: decoding Kafka's internal topics is not supported by krabka console-consumer".to_owned()),
            ),
            (
                "com.example.Mine",
                Err("com.example.Mine: no such formatter; krabka builds in DefaultMessageFormatter, LoggingMessageFormatter and NoOpMessageFormatter".to_owned()),
            ),
        ];
        for (class, expected) in cases {
            let actual = Formatter::new(class, &empty).map(|(formatter, _)| formatter);
            assert!(actual == expected, "{class}");
        }
    }

    #[test]
    fn the_logging_formatter_prints_the_default_output_and_logs_a_line() {
        let (formatter, _) = Formatter::new(LOGGING_FORMATTER, &Properties::default()).unwrap();
        let (bytes, line) = formatter.format(&record()).unwrap();
        assert!(
            (bytes, line)
                == (
                    b"v1\n".to_vec(),
                    Some("CreateTime:1790600093698, key:k1, value:v1".to_owned())
                )
        );
        let no_key = Record {
            key: None,
            value: None,
            ..record()
        };
        assert!(log_line(&no_key) == "CreateTime:1790600093698, key:null value:null");
    }

    #[test]
    fn the_no_op_formatter_prints_nothing() {
        assert!(Formatter::NoOp.format(&record()) == Ok((Vec::new(), None)));
    }
}
