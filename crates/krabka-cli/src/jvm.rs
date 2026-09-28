//! The JVM behaviours that the Kafka tools' output depends on: Java's hash
//! codes, the iteration order of a `java.util.HashMap` or `HashSet`, and the
//! parsing of `Integer.parseInt`.
//!
//! The Kafka tools print many collections in the order a `HashMap` yields
//! them. Operators diff and grep those lines, so krabka computes the same
//! order rather than sorting. The model is JDK 21's, the JVM of the
//! apache/kafka:4.3.1 image.

/// The largest table that a `HashMap` allocates, `1 << 30`.
const MAXIMUM_CAPACITY: usize = 1 << 30;

/// A bin longer than this is treeified, or the table grows while it is
/// smaller than [`MIN_TREEIFY_CAPACITY`].
const TREEIFY_THRESHOLD: usize = 8;
const MIN_TREEIFY_CAPACITY: usize = 64;

/// `String.hashCode`: the UTF-16 code units folded with 31, in wrapping
/// 32-bit arithmetic.
#[must_use]
pub fn string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// `Integer.hashCode`: the value itself.
#[must_use]
pub const fn integer_hash(value: i32) -> i32 {
    value
}

/// `TopicPartition.hashCode`.
#[must_use]
pub fn topic_partition_hash(topic: &str, partition: i32) -> i32 {
    31_i32
        .wrapping_mul(31_i32.wrapping_add(partition))
        .wrapping_add(string_hash(topic))
}

/// How the `HashMap` or `HashSet` whose order is wanted was built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    /// `new HashMap<>()`, `new HashSet<>()`, and the maps and sets of
    /// `Collectors.toMap` and `Collectors.toSet`.
    Default,
    /// `new HashMap<>(initialCapacity)`.
    WithCapacity(usize),
    /// `new HashMap<>(map)` of a map with the same keys, and `putAll` into a
    /// `HashMap` that has no table yet.
    Copy,
    /// `new HashSet<>(collection)` of a collection with the same elements,
    /// which JDK 19 and later size for at least 12.
    HashSetCopy,
}

/// `HashMap.tableSizeFor`: the smallest power of two at or above `wanted`,
/// and at least 1.
fn table_size_for(wanted: usize) -> usize {
    wanted.max(1).next_power_of_two().min(MAXIMUM_CAPACITY)
}

/// `(int) Math.ceil(entries / 0.75)`: the table that JDK 19 and later size
/// to hold `entries` without resizing.
fn capacity_for(entries: usize) -> usize {
    (entries * 4).div_ceil(3)
}

/// The table size that the first insertion allocates.
fn initial_capacity(table: Table, entries: usize) -> usize {
    match table {
        Table::Default => 16,
        Table::WithCapacity(initial) => table_size_for(initial),
        Table::Copy => table_size_for(capacity_for(entries)),
        Table::HashSetCopy => table_size_for(capacity_for(entries.max(12))),
    }
}

/// `HashMap.hash`: the high half of the hash folded into the low half.
fn spread(hash: i32) -> usize {
    let hash = hash.cast_unsigned();
    usize::try_from(hash ^ (hash >> 16)).expect("a u32 fits in usize")
}

/// The iteration order of a `HashMap` or `HashSet` built as `table` and
/// given the distinct `keys` in this order, whose hash codes `hash` gives.
///
/// The table iterates its bins in index order and each bin in insertion
/// order, and a resize splits each bin keeping that order. A bin that grows
/// past eight keys resizes a table smaller than 64, as `treeifyBin` does. A
/// bin that the JDK does turn into a tree keeps insertion order here, which
/// is where the two can differ.
#[must_use]
pub fn hash_order<T>(keys: Vec<T>, table: Table, hash: impl Fn(&T) -> i32) -> Vec<T> {
    let spreads = keys.iter().map(|key| spread(hash(key))).collect::<Vec<_>>();
    let mut capacity = initial_capacity(table, keys.len());
    let mut bins: Vec<Vec<usize>> = vec![Vec::new(); capacity];
    let resize = |bins: &[Vec<usize>], capacity: usize| {
        let mut resized = vec![Vec::new(); capacity];
        for &index in bins.iter().flatten() {
            resized[spreads[index] & (capacity - 1)].push(index);
        }
        resized
    };
    for (index, spread) in spreads.iter().enumerate() {
        let bin = spread & (capacity - 1);
        bins[bin].push(index);
        if bins[bin].len() > TREEIFY_THRESHOLD && capacity < MIN_TREEIFY_CAPACITY {
            capacity *= 2;
            bins = resize(&bins, capacity);
        }
        // `threshold = (int) (capacity * 0.75f)`.
        if index + 1 > capacity / 4 * 3 + capacity % 4 * 3 / 4 && capacity < MAXIMUM_CAPACITY {
            capacity *= 2;
            bins = resize(&bins, capacity);
        }
    }
    let mut slots = keys.into_iter().map(Some).collect::<Vec<_>>();
    bins.into_iter()
        .flatten()
        .filter_map(|index| slots[index].take())
        .collect()
}

/// The order of `keys` in a table that has grown to `capacity` bins: by bin,
/// and in the given order within a bin. For a map that holds more keys than
/// the ones whose order is wanted, such as the overrides among all the
/// configs of a topic.
#[must_use]
pub fn order_in_table<T>(mut keys: Vec<T>, capacity: usize, hash: impl Fn(&T) -> i32) -> Vec<T> {
    keys.sort_by_key(|key| spread(hash(key)) & (capacity - 1));
    keys
}

/// The number of bins of a `new HashMap<>()` after `entries` insertions of
/// keys that do not collide.
#[must_use]
pub fn default_capacity(entries: usize) -> usize {
    let mut capacity = 16;
    while entries > capacity / 4 * 3 && capacity < MAXIMUM_CAPACITY {
        capacity *= 2;
    }
    capacity
}

/// `values` in the iteration order of a `HashSet` that was given them in
/// this order, each once.
pub fn hash_set_order<T: PartialEq>(values: Vec<T>, hash: impl Fn(&T) -> i32) -> Vec<T> {
    let mut unique = Vec::<T>::new();
    for value in values {
        if !unique.contains(&value) {
            unique.push(value);
        }
    }
    hash_order(unique, Table::Default, hash)
}

/// Java's `AbstractCollection.toString`: `[a, b, c]`.
pub fn collection_to_string<T: std::fmt::Display>(values: &[T]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// `Integer.parseInt`, with the message of the `NumberFormatException` it
/// throws: an optional sign and decimal digits only, in the `int` range.
///
/// # Errors
/// Returns `For input string: "<value>"` for anything else.
pub fn parse_int(value: &str) -> Result<i32, String> {
    let digits = value.strip_prefix(['-', '+']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("For input string: \"{value}\""));
    }
    value
        .parse()
        .map_err(|_| format!("For input string: \"{value}\""))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn hashes_match_the_jvm() {
        let cases = [
            (string_hash(""), 0),
            (string_hash("foo"), 101_574),
            (string_hash("bar"), 97_299),
            (string_hash("orders"), -1_008_770_331),
            (string_hash("__consumer_offsets"), -970_371_369),
            (string_hash("\u{e9}t\u{e9}"), 227_742),
            (topic_partition_hash("orders", 0), -1_008_769_370),
            (topic_partition_hash("orders", 7), -1_008_769_153),
        ];
        for (actual, expected) in cases {
            check!(actual == expected);
        }
    }

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
    fn string_key_orders_match_the_jdk() {
        let cases: [(&[&str], Table, &[&str]); 9] = [
            (
                &QUOTAS,
                Table::WithCapacity(4),
                &[
                    "producer_byte_rate",
                    "consumer_byte_rate",
                    "controller_mutation_rate",
                    "request_percentage",
                ],
            ),
            (
                &QUOTAS,
                Table::Default,
                &[
                    "request_percentage",
                    "producer_byte_rate",
                    "consumer_byte_rate",
                    "controller_mutation_rate",
                ],
            ),
            (
                &["request_percentage", "producer_byte_rate"],
                Table::WithCapacity(2),
                &["request_percentage", "producer_byte_rate"],
            ),
            (&["foo", "bar"], Table::Default, &["bar", "foo"]),
            (
                &TOPIC_CONFIGS,
                Table::Default,
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
                Table::WithCapacity(6),
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
                Table::Default,
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
            (&[], Table::WithCapacity(0), &[]),
            (&[], Table::Copy, &[]),
        ];
        for (keys, table, expected) in cases {
            check!(
                hash_order(keys.to_vec(), table, |key| string_hash(key)) == expected,
                "{keys:?} {table:?}"
            );
        }
    }

    #[test]
    fn integer_key_orders_follow_table_growth() {
        let cases: [(Vec<i32>, Table, Vec<i32>); 6] = [
            // 16 shares bin 0 with 0 and keeps its insertion place.
            (vec![16, 1, 0], Table::Default, vec![16, 0, 1]),
            // A thirteenth key doubles the table to 32 bins.
            (
                (0..13).rev().chain([16]).collect(),
                Table::Default,
                vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16],
            ),
            // Two bins, then four: 3 lands in bin 3 after the resize.
            (vec![3, 1, 2], Table::WithCapacity(2), vec![1, 2, 3]),
            // A copy of three keys has four bins, so 4 shares bin 0 with 0.
            (vec![4, 0, 1], Table::Copy, vec![4, 0, 1]),
            // A copy of four has eight bins.
            (vec![8, 4, 0, 1], Table::Copy, vec![8, 0, 1, 4]),
            // A HashSet copy has at least 16 bins.
            (vec![16, 4, 0, 1], Table::HashSetCopy, vec![16, 0, 1, 4]),
        ];
        for (keys, table, expected) in cases {
            check!(
                hash_order(keys.clone(), table, |key| integer_hash(*key)) == expected,
                "{keys:?} {table:?}"
            );
        }
    }

    // Nine keys in one bin of a 16-bin table resize it, as `treeifyBin`
    // does, so the keys spread over 32 bins.
    #[test]
    fn a_long_bin_resizes_a_small_table() {
        let keys = (0..9).map(|n| n * 16).collect::<Vec<i32>>();
        check!(
            hash_order(keys, Table::Default, |key| integer_hash(*key))
                == vec![0, 32, 64, 96, 128, 16, 48, 80, 112]
        );
    }

    // The order `kafka-log-dirs --topic-list events,orders` printed on a
    // Kafka 4.3.1 broker holding events-0..19 and orders-0..1.
    #[test]
    fn topic_partitions_order_as_a_kafka_log_dirs_listing() {
        let keys = (0..20)
            .map(|partition| ("events", partition))
            .chain([("orders", 0), ("orders", 1)])
            .collect::<Vec<_>>();
        let order = hash_order(keys, Table::Default, |(topic, partition)| {
            topic_partition_hash(topic, *partition)
        })
        .into_iter()
        .map(|(topic, partition)| format!("{topic}-{partition}"))
        .collect::<Vec<_>>();
        check!(
            order
                == [
                    "events-19",
                    "events-11",
                    "events-12",
                    "events-13",
                    "events-14",
                    "events-15",
                    "events-16",
                    "events-17",
                    "events-18",
                    "events-3",
                    "events-4",
                    "events-5",
                    "events-6",
                    "events-7",
                    "events-8",
                    "events-9",
                    "events-10",
                    "orders-0",
                    "orders-1",
                    "events-0",
                    "events-1",
                    "events-2",
                ]
        );
    }

    #[test]
    fn sets_drop_repeats_and_print_as_java_collections() {
        check!(hash_set_order(vec![16, 1, 0, 1], |i| *i) == vec![16, 0, 1]);
        check!(hash_set_order(vec!["foo", "bar"], |s| string_hash(s)) == vec!["bar", "foo"]);
        check!(collection_to_string(&[1, 2]) == "[1, 2]");
        check!(collection_to_string::<i32>(&[]) == "[]");
        check!(default_capacity(12) == 16);
        check!(default_capacity(13) == 32);
        check!(order_in_table(vec![17, 1, 16], 16, |i| *i) == vec![16, 17, 1]);
    }

    #[test]
    fn parse_int_accepts_what_java_accepts() {
        let cases = [
            ("42", Ok(42)),
            ("-7", Ok(-7)),
            ("+3", Ok(3)),
            ("2147483647", Ok(i32::MAX)),
            ("2147483648", Err("For input string: \"2147483648\"")),
            ("", Err("For input string: \"\"")),
            ("-", Err("For input string: \"-\"")),
            (" 1", Err("For input string: \" 1\"")),
            ("1_0", Err("For input string: \"1_0\"")),
        ];
        for (value, expected) in cases {
            check!(
                parse_int(value) == expected.map_err(str::to_owned),
                "{value}"
            );
        }
    }
}
