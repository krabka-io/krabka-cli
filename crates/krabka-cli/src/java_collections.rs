//! The iteration order of Java's `HashMap` and `HashSet`.
//!
//! Kafka's tools print some sets as they iterate them, for example the
//! duplicate entries of a reassignment file, and Kafka's replica placer
//! draws random numbers in `HashMap` order. These functions give that order,
//! so krabka prints and places what the JVM tool prints and places.

/// Java's `String.hashCode`.
#[must_use]
pub fn string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// The iteration order of a `HashMap` or `HashSet` that received keys with
/// these hash codes in this order, as indexes into `hashes`.
///
/// The table iterates its buckets in index order and each bucket in
/// insertion order. The bucket of a key is its spread hash modulo the table
/// size, which starts at 16 and doubles once the table holds more than three
/// quarters of it.
#[must_use]
pub fn hash_order(hashes: &[i32]) -> Vec<usize> {
    let mut capacity = 16_usize;
    while hashes.len() * 4 > capacity * 3 {
        capacity *= 2;
    }
    let mut order = (0..hashes.len()).collect::<Vec<_>>();
    order.sort_by_key(|&index| {
        let hash = u32::from_ne_bytes(hashes[index].to_ne_bytes());
        let spread = hash ^ (hash >> 16);
        (usize::try_from(spread).unwrap_or(0) & (capacity - 1), index)
    });
    order
}

/// `values` in the iteration order of a `HashSet` that received them in this
/// order, each once.
pub fn hash_set_order<T: PartialEq>(values: Vec<T>, hash: impl Fn(&T) -> i32) -> Vec<T> {
    let mut unique = Vec::<T>::new();
    for value in values {
        if !unique.contains(&value) {
            unique.push(value);
        }
    }
    let order = hash_order(&unique.iter().map(&hash).collect::<Vec<_>>());
    let mut slots = unique.into_iter().map(Some).collect::<Vec<_>>();
    order
        .into_iter()
        .filter_map(|index| slots[index].take())
        .collect()
}

/// Java's `AbstractCollection.toString`: `[a, b, c]`.
pub fn to_string<T: std::fmt::Display>(values: &[T]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn hashes_and_orders_match_the_jvm() {
        check!(string_hash("") == 0);
        check!(string_hash("foo") == 101_574);
        check!(string_hash("bar") == 97_299);
        check!(string_hash("__consumer_offsets") == -970_371_369);
        // A `HashSet` of "foo" then "bar" iterates "bar" first: bucket 2
        // before bucket 7.
        check!(hash_set_order(vec!["foo", "bar"], |s| string_hash(s)) == vec!["bar", "foo"]);
        // Integers are their own hash: 16 shares bucket 0 with 0 and keeps its
        // insertion place.
        check!(hash_set_order(vec![16, 1, 0, 1], |i| *i) == vec![16, 0, 1]);
        // More than twelve keys double the table to 32 buckets.
        let keys = (0..13).rev().chain([16]).collect::<Vec<i32>>();
        check!(hash_set_order(keys, |i| *i) == vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16]);
        check!(to_string(&[1, 2]) == "[1, 2]");
        check!(to_string::<i32>(&[]) == "[]");
    }
}
