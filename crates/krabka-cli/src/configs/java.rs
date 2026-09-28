//! The JVM behaviours that `kafka-configs` output depends on: the iteration
//! order of a `java.util.HashMap`, `Double.toString`, `Double.parseDouble`,
//! `String.trim`, `Integer.parseInt` failures, and the `toString` of the
//! Scala collections that its error messages print.

/// A `HashMap` bin longer than this is treeified, or the table grows when it
/// is smaller than [`MIN_TREEIFY_CAPACITY`].
const TREEIFY_THRESHOLD: usize = 8;
const MIN_TREEIFY_CAPACITY: usize = 64;
const DEFAULT_CAPACITY: usize = 16;

/// The largest Scala immutable collection that keeps insertion order. A
/// larger one is a `HashMap` or `HashSet`.
const SCALA_SMALL_COLLECTION: usize = 4;

/// `String.hashCode`, over the UTF-16 code units.
fn string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// `HashMap.hash`: the high half of the hash folded into the low half.
fn spread(value: &str) -> usize {
    let hash = string_hash(value).cast_unsigned();
    usize::try_from(hash ^ (hash >> 16)).expect("a u32 fits in usize")
}

/// The order in which a `java.util.HashMap` iterates `keys`, inserted in the
/// given order, as JDK 21 does.
///
/// `initial_capacity` is the argument of `new HashMap<>(n)`, or `None` for
/// `new HashMap<>()`. The keys must be distinct. A bin that the JDK would
/// treeify, which needs nine colliding keys in a table of at least 64 bins,
/// keeps its insertion order here.
pub fn hash_map_order<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    initial_capacity: Option<usize>,
) -> Vec<&'a str> {
    let mut capacity = initial_capacity.map_or(DEFAULT_CAPACITY, |n| n.max(1).next_power_of_two());
    let mut bins: Vec<Vec<&str>> = vec![Vec::new(); capacity];
    let mut size = 0;
    for key in keys {
        let bin = spread(key) & (capacity - 1);
        bins[bin].push(key);
        if bins[bin].len() > TREEIFY_THRESHOLD && capacity < MIN_TREEIFY_CAPACITY {
            capacity *= 2;
            bins = rehash(&bins, capacity);
        }
        size += 1;
        if size > capacity * 3 / 4 {
            capacity *= 2;
            bins = rehash(&bins, capacity);
        }
    }
    bins.into_iter().flatten().collect()
}

/// A resize: each bin splits into two and keeps the relative order of its
/// keys.
fn rehash<'a>(bins: &[Vec<&'a str>], capacity: usize) -> Vec<Vec<&'a str>> {
    let mut resized = vec![Vec::new(); capacity];
    for key in bins.iter().flatten() {
        resized[spread(key) & (capacity - 1)].push(*key);
    }
    resized
}

/// `Double.toString`: the shortest digits that round-trip, in plain notation
/// from 10^-3 up to but not including 10^7 and in `d.dddE<n>` notation
/// outside that range.
pub fn double_to_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    if value == 0.0 {
        return format!("{sign}0.0");
    }
    let magnitude = value.abs();
    let scientific = format!("{magnitude:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("`{:e}` always writes an exponent");
    let exponent: i32 = exponent.parse().expect("`{:e}` writes a decimal exponent");
    let digits = mantissa.replace('.', "");
    if (1e-3..1e7).contains(&magnitude) {
        if let Ok(point) = usize::try_from(exponent) {
            let integer_len = point + 1;
            let padded = format!("{digits:0<integer_len$}");
            let (integer, fraction) = padded.split_at(integer_len);
            let fraction = if fraction.is_empty() { "0" } else { fraction };
            format!("{sign}{integer}.{fraction}")
        } else {
            let zeros = usize::try_from(-exponent - 1).expect("the exponent is negative");
            format!("{sign}0.{}{digits}", "0".repeat(zeros))
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() { "0" } else { rest };
        format!("{sign}{first}.{rest}E{exponent}")
    }
}

/// `Double.parseDouble` for decimal input: surrounding control characters and
/// spaces are ignored, `NaN` and `Infinity` are spelled as Java spells them,
/// and one trailing `f`, `F`, `d` or `D` is allowed. Hexadecimal input is not
/// accepted.
pub fn parse_double(value: &str) -> Option<f64> {
    let value = trim(value);
    let (sign, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (-1.0, &value[1..]),
        Some(b'+') => (1.0, &value[1..]),
        _ => (1.0, value),
    };
    match unsigned {
        "NaN" => return Some(f64::NAN),
        "Infinity" => return Some(sign * f64::INFINITY),
        _ => {}
    }
    let decimal = value.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(value);
    // Rust also reads `inf`, `infinity` and `nan`, which Java refuses.
    if decimal
        .chars()
        .any(|c| c.is_ascii_alphabetic() && !matches!(c, 'e' | 'E'))
    {
        return None;
    }
    decimal.parse().ok()
}

/// `Integer.parseInt`, with the message of the `NumberFormatException` it
/// throws.
pub fn parse_int(value: &str) -> Result<i32, String> {
    let digits = value.strip_prefix(['-', '+']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("For input string: \"{value}\""));
    }
    value
        .parse()
        .map_err(|_| format!("For input string: \"{value}\""))
}

/// `String.trim`: strips every leading and trailing character at or below
/// U+0020.
pub fn trim(value: &str) -> &str {
    value.trim_matches(|c: char| c <= ' ')
}

/// The `toString` of a Scala immutable `Set`, `HashSet` above four elements.
pub fn scala_set<S: AsRef<str>>(items: &[S]) -> String {
    let name = if items.len() > SCALA_SMALL_COLLECTION {
        "HashSet"
    } else {
        "Set"
    };
    format!("{name}({})", join(items))
}

/// The `toString` of a Scala `ArrayBuffer`.
pub fn scala_buffer<S: AsRef<str>>(items: &[S]) -> String {
    format!("ArrayBuffer({})", join(items))
}

fn join<S: AsRef<str>>(items: &[S]) -> String {
    items
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    const QUOTAS: [&str; 4] = [
        "consumer_byte_rate",
        "producer_byte_rate",
        "request_percentage",
        "controller_mutation_rate",
    ];
    const TOPIC_CONFIGS: [&str; 6] = [
        "retention.ms",
        "cleanup.policy",
        "segment.bytes",
        "max.message.bytes",
        "min.insync.replicas",
        "flush.ms",
    ];
    const TOPICS: [&str; 14] = [
        "t1",
        "orders",
        "__consumer_offsets",
        "payments",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
    ];

    // Every expected order below is what JDK 21 printed for the same keys.
    #[test]
    fn hash_map_order_matches_the_jdk() {
        let cases: [(&[&str], Option<usize>, &[&str]); 8] = [
            (
                &QUOTAS,
                Some(4),
                &[
                    "producer_byte_rate",
                    "consumer_byte_rate",
                    "controller_mutation_rate",
                    "request_percentage",
                ],
            ),
            (
                &QUOTAS,
                None,
                &[
                    "request_percentage",
                    "producer_byte_rate",
                    "consumer_byte_rate",
                    "controller_mutation_rate",
                ],
            ),
            (
                &["request_percentage", "producer_byte_rate"],
                Some(2),
                &["request_percentage", "producer_byte_rate"],
            ),
            (&["foo", "bar"], None, &["bar", "foo"]),
            (
                &TOPIC_CONFIGS,
                None,
                &[
                    "cleanup.policy",
                    "flush.ms",
                    "max.message.bytes",
                    "min.insync.replicas",
                    "retention.ms",
                    "segment.bytes",
                ],
            ),
            (
                &TOPIC_CONFIGS,
                Some(6),
                &[
                    "cleanup.policy",
                    "min.insync.replicas",
                    "retention.ms",
                    "segment.bytes",
                    "flush.ms",
                    "max.message.bytes",
                ],
            ),
            (
                &TOPICS,
                None,
                &[
                    "a",
                    "b",
                    "c",
                    "d",
                    "e",
                    "f",
                    "payments",
                    "g",
                    "h",
                    "i",
                    "j",
                    "orders",
                    "t1",
                    "__consumer_offsets",
                ],
            ),
            (&[], Some(0), &[]),
        ];
        for (keys, capacity, expected) in cases {
            check!(
                hash_map_order(keys.iter().copied(), capacity) == expected,
                "{keys:?} {capacity:?}"
            );
        }
    }

    #[test]
    fn double_to_string_matches_the_jdk() {
        let cases = [
            (1024.0, "1024.0"),
            (2.0e7, "2.0E7"),
            (12.5, "12.5"),
            (3.0, "3.0"),
            (1.0e7, "1.0E7"),
            (9_999_999.0, "9999999.0"),
            (0.001, "0.001"),
            (0.0001, "1.0E-4"),
            (1.5e-5, "1.5E-5"),
            (123_456_789.0, "1.23456789E8"),
            (1.0e21, "1.0E21"),
            (-2.5e8, "-2.5E8"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (100.0, "100.0"),
            (0.012_5, "0.0125"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (value, expected) in cases {
            check!(double_to_string(value) == expected, "{value}");
        }
    }

    #[test]
    fn parse_double_accepts_what_java_accepts() {
        let cases = [
            ("1024", Some(1024.0)),
            (" 12.5 ", Some(12.5)),
            ("1e3", Some(1000.0)),
            ("+2.5E-1", Some(0.25)),
            ("7d", Some(7.0)),
            ("7F", Some(7.0)),
            (".5", Some(0.5)),
            ("-Infinity", Some(f64::NEG_INFINITY)),
            ("abc", None),
            ("", None),
            ("inf", None),
            ("infinity", None),
            ("nan", None),
            ("1,000", None),
        ];
        for (input, expected) in cases {
            check!(parse_double(input) == expected, "{input:?}");
        }
        check!(parse_double("NaN").is_some_and(f64::is_nan));
    }

    #[test]
    fn parse_int_fails_with_the_java_message() {
        let cases = [
            ("8192", Ok(8192)),
            ("-0", Ok(0)),
            ("+5", Ok(5)),
            ("0008192", Ok(8192)),
            ("", Err("For input string: \"\"")),
            ("-", Err("For input string: \"-\"")),
            ("99999999999", Err("For input string: \"99999999999\"")),
            ("1a", Err("For input string: \"1a\"")),
        ];
        for (input, expected) in cases {
            check!(
                parse_int(input) == expected.map_err(str::to_owned),
                "{input:?}"
            );
        }
    }

    #[test]
    fn trim_strips_control_characters_as_java_does() {
        check!(trim("\u{1} a b\t\n") == "a b");
        check!(trim("\u{a0}a\u{a0}") == "\u{a0}a\u{a0}");
    }

    #[test]
    fn scala_collections_render_as_their_to_string() {
        check!(scala_set(&["bar", "foo"]) == "Set(bar, foo)");
        check!(scala_set(&["a", "b", "c", "d", "e"]) == "HashSet(a, b, c, d, e)");
        check!(scala_buffer(&["foo", "bar"]) == "ArrayBuffer(foo, bar)");
    }
}
