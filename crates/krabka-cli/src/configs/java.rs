//! The JVM behaviours that `kafka-configs` output depends on: the iteration
//! order of a `java.util.HashMap`, `Double.toString`, `Double.parseDouble`,
//! `String.trim`, `Integer.parseInt` failures, and the `toString` of the
//! Scala collections that its error messages print.

/// The largest Scala immutable collection that keeps insertion order. A
/// larger one is a `HashMap` or `HashSet`.
const SCALA_SMALL_COLLECTION: usize = 4;

pub use crate::jvm::parse_int;
use crate::jvm::{Table, hash_order, string_hash};

/// The order in which a `java.util.HashMap` iterates `keys`, inserted in the
/// given order. `initial_capacity` is the argument of `new HashMap<>(n)`, or
/// `None` for `new HashMap<>()`.
pub fn hash_map_order<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    initial_capacity: Option<usize>,
) -> Vec<&'a str> {
    let table = initial_capacity.map_or(Table::Default, Table::WithCapacity);
    hash_order(keys.into_iter().collect(), table, |key| string_hash(key))
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

/// `String.trim`: strips every leading and trailing character at or below
/// U+0020.
pub fn trim(value: &str) -> &str {
    value.trim_matches(|c: char| c <= ' ')
}

/// The `toString` of a Scala immutable `Set` built from the distinct
/// `items` in this order: a `Set` in insertion order up to four elements,
/// and a `HashSet` in its own order above that.
pub fn scala_set<S: AsRef<str>>(items: &[S]) -> String {
    let ordered = scala_set_order(items.iter().map(AsRef::as_ref).collect());
    let name = if ordered.len() > SCALA_SMALL_COLLECTION {
        "HashSet"
    } else {
        "Set"
    };
    format!("{name}({})", join(&ordered))
}

/// The iteration order of a Scala immutable `Set` that was given `items`
/// in this order: each once, in insertion order up to four elements, and in
/// the order of a `HashSet` above that.
pub fn scala_set_order<T: AsRef<str> + PartialEq>(items: Vec<T>) -> Vec<T> {
    let mut unique = Vec::<T>::new();
    for item in items {
        if !unique.contains(&item) {
            unique.push(item);
        }
    }
    if unique.len() <= SCALA_SMALL_COLLECTION {
        return unique;
    }
    let hashed = unique
        .into_iter()
        .map(|item| (improve(string_hash(item.as_ref())).cast_unsigned(), item))
        .collect();
    champ_order(hashed, 0)
}

/// `scala.collection.Hashing.improve`, the hash that a Scala `HashSet`
/// places an element by.
const fn improve(hash: i32) -> i32 {
    let hash = hash.cast_unsigned();
    let mut h = hash.wrapping_add(!(hash << 9));
    h ^= h >> 14;
    h = h.wrapping_add(h << 4);
    (h ^ (h >> 10)).cast_signed()
}

/// The number of hash bits that one level of a Scala `HashSet` uses.
const CHAMP_BITS: u32 = 5;

/// The order in which a Scala `HashSet` (a CHAMP trie) iterates the
/// elements of one node at `shift`: the elements alone in their slot, by
/// slot, then each sub-node by slot, depth first. Elements whose whole hash
/// collides keep insertion order.
fn champ_order<T>(items: Vec<(u32, T)>, shift: u32) -> Vec<T> {
    if shift >= u32::BITS {
        return items.into_iter().map(|(_, item)| item).collect();
    }
    let mut slots: Vec<Vec<(u32, T)>> = (0..1 << CHAMP_BITS).map(|_| Vec::new()).collect();
    for (hash, item) in items {
        slots[usize::try_from((hash >> shift) & 31).expect("a slot fits in usize")]
            .push((hash, item));
    }
    let mut payload = Vec::new();
    let mut nodes = Vec::new();
    for slot in slots {
        if slot.len() == 1 {
            payload.extend(slot.into_iter().map(|(_, item)| item));
        } else if !slot.is_empty() {
            nodes.push(slot);
        }
    }
    for node in nodes {
        payload.extend(champ_order(node, shift + CHAMP_BITS));
    }
    payload
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
    fn scala_sets_iterate_in_the_order_of_scala_2_13() {
        // The orders that scala-library 2.13.18, the one of the
        // apache/kafka:4.3.1 image, prints.
        let letters = [
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y", "z", "aa", "ab", "ac", "ad", "ae", "af", "ag",
            "ah",
        ];
        let cases: [(Vec<&str>, Vec<&str>); 4] = [
            (vec!["b", "a", "b"], vec!["b", "a"]),
            (
                vec!["g1", "g2", "g3", "g4", "g5"],
                vec!["g2", "g1", "g5", "g3", "g4"],
            ),
            (
                vec![
                    "orders", "payments", "audit", "billing", "shipping", "alpha",
                ],
                vec![
                    "alpha", "payments", "orders", "audit", "billing", "shipping",
                ],
            ),
            (
                letters.to_vec(),
                vec![
                    "e", "n", "t", "a", "ab", "m", "af", "i", "ah", "v", "ag", "p", "ad", "w", "k",
                    "s", "ae", "x", "j", "y", "u", "f", "q", "ac", "b", "g", "l", "c", "h", "aa",
                    "r", "o", "z", "d",
                ],
            ),
        ];
        for (items, expected) in cases {
            check!(scala_set_order(items.clone()) == expected, "{items:?}");
        }
    }

    #[test]
    fn scala_collections_render_as_their_to_string() {
        check!(scala_set(&["bar", "foo"]) == "Set(bar, foo)");
        check!(scala_set(&["g1", "g2", "g3", "g4", "g5"]) == "HashSet(g2, g1, g5, g3, g4)");
        check!(scala_buffer(&["foo", "bar"]) == "ArrayBuffer(foo, bar)");
    }
}
