//! The JVM behaviours that `kafka-topics` output depends on.
//!
//! `kafka-topics` prints topics and topic configs in the iteration order of
//! `java.util.HashMap`, splits its arguments with `String.split`, prints topic
//! IDs as Kafka's `Uuid.toString` does, and matches `--topic` as a
//! `java.util.regex` pattern. Byte-level agreement with its stdout needs each
//! of these reproduced.

use regex::Regex;

/// `String.hashCode`: `s[0]*31^(n-1) + ... + s[n-1]` over UTF-16 code units,
/// in wrapping 32-bit arithmetic.
fn string_hash(value: &str) -> u32 {
    value.encode_utf16().fold(0_u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    })
}

/// `HashMap.hash`: the high 16 bits folded into the low 16.
fn spread(value: &str) -> u32 {
    let hash = string_hash(value);
    hash ^ (hash >> 16)
}

/// `HashMap.tableSizeFor`: the smallest power of two at or above `capacity`.
fn table_size_for(capacity: usize) -> usize {
    capacity.max(1).next_power_of_two()
}

/// How a `java.util.HashMap` was constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Construction {
    /// `new HashMap<>()`.
    Default,
    /// `new HashMap<>(initialCapacity)`.
    WithCapacity(usize),
    /// `new HashMap<>(otherMap)`, from a map of this many entries.
    CopyOf(usize),
}

/// The table capacity of a map built as `construction` after `len` puts.
///
/// The first put allocates the table at the initial threshold, and each put
/// that takes the size past three quarters of the capacity doubles it.
fn final_capacity(construction: Construction, len: usize) -> usize {
    let mut capacity = match construction {
        Construction::Default => 16,
        Construction::WithCapacity(initial) => table_size_for(initial),
        // `putMapEntries`: `(int) (size / 0.75f + 1.0f)`.
        Construction::CopyOf(size) => table_size_for(4 * size / 3 + 1),
    };
    for size in 1..=len {
        // `threshold = (int) (capacity * 0.75f)`.
        if size > capacity * 3 / 4 {
            capacity *= 2;
        }
    }
    capacity
}

/// The iteration order of a `java.util.HashMap` built as `construction` and
/// filled with `keys` in order.
///
/// A key's bucket is its spread hash masked by the final capacity, and a
/// bucket keeps insertion order through every resize, so the iteration order
/// is a stable sort of the insertion order by bucket. A bucket of eight or
/// more keys in a table of 64 or more becomes a tree bin, whose order this
/// does not model.
pub fn hash_map_order<S: AsRef<str>>(keys: Vec<S>, construction: Construction) -> Vec<S> {
    let mask = bucket_mask(final_capacity(construction, keys.len()));
    let mut keys = keys;
    keys.sort_by_key(|key| spread(key.as_ref()) & mask);
    keys
}

fn bucket_mask(capacity: usize) -> u32 {
    u32::try_from(capacity - 1).unwrap_or(u32::MAX)
}

/// The order in which `kafka-topics --describe` prints the topics it
/// resolved by name, given them in sorted order.
///
/// `KafkaAdminClient.describeTopics` puts the names into a
/// `HashMap(names.size())`, returns a copy of it, and
/// `DescribeTopicsResult.allTopicNames` collects that copy into a third
/// `HashMap(size)`. `TopicCommand` prints the values of the third.
pub fn describe_order(sorted: Vec<String>) -> Vec<String> {
    let len = sorted.len();
    let first = hash_map_order(sorted, Construction::WithCapacity(len));
    let copy = hash_map_order(first, Construction::CopyOf(len));
    hash_map_order(copy, Construction::WithCapacity(len))
}

/// How many entries a Kafka 4.3.1 broker returns for one topic's
/// `DescribeConfigs`, which fixes the table capacity of the `Config` map
/// that `kafka-topics` iterates.
const TOPIC_CONFIG_ENTRIES: usize = 33;

/// The order in which `kafka-topics --describe` prints topic config
/// overrides after `Configs:`.
///
/// `Config` keeps every entry of the `DescribeConfigs` answer in a default
/// `HashMap`, and the overrides print in its iteration order. The broker
/// answers every topic config, so the table capacity is that of 33 entries.
/// Two overrides that share a bucket print in the broker's answer order,
/// which this approximates with name order.
pub fn config_order(sorted_overrides: Vec<(String, String)>) -> Vec<(String, String)> {
    let mask = bucket_mask(final_capacity(Construction::Default, TOPIC_CONFIG_ENTRIES));
    let mut overrides = sorted_overrides;
    overrides.sort_by_key(|(name, _)| spread(name) & mask);
    overrides
}

/// `String.split(regex)` with Java's rule that trailing empty strings are
/// removed. A separator match at the very start still yields a leading empty
/// string, because only a zero-width match suppresses it.
pub fn split(value: &str, separator: &Regex) -> Vec<String> {
    let mut parts = separator
        .split(value)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    // `"".split(x)` is `[""]`: with no match there is nothing to remove.
    if parts.len() > 1 {
        while parts.last().is_some_and(String::is_empty) {
            parts.pop();
        }
    }
    parts
}

/// `Integer.parseInt`, with the message of its `NumberFormatException`.
pub fn parse_int(value: &str) -> Result<i32, String> {
    value
        .parse()
        .map_err(|_| format!("For input string: \"{value}\""))
}

const BASE64_URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Kafka's `Uuid.toString`: URL-safe base64 of the 16 bytes, unpadded.
pub fn uuid_to_string(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(22);
    for chunk in bytes.chunks(3) {
        let word = chunk.iter().enumerate().fold(0_u32, |word, (index, byte)| {
            word | u32::from(*byte) << (16 - 8 * index)
        });
        for index in 0..=chunk.len() {
            let digit = usize::try_from(word >> (18 - 6 * index) & 63).unwrap_or_default();
            out.push(char::from(BASE64_URL[digit]));
        }
    }
    out
}

/// Kafka's `Uuid.fromString`, with the messages of the exceptions it and
/// `Base64.getUrlDecoder` throw.
pub fn uuid_from_string(value: &str) -> Result<[u8; 16], String> {
    if value.chars().count() > 24 {
        let prefix = value.chars().take(24).collect::<String>();
        return Err(format!(
            "Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID"
        ));
    }
    let unpadded = value.trim_end_matches('=');
    let mut bits = 0_u32;
    let mut held = 0;
    let mut bytes = Vec::with_capacity(18);
    for c in unpadded.chars() {
        let digit = BASE64_URL
            .iter()
            .position(|&symbol| char::from(symbol) == c)
            .ok_or_else(|| format!("Illegal base64 character {:x}", u32::from(c)))?;
        bits = bits << 6 | u32::try_from(digit).unwrap_or_default();
        held += 6;
        if held >= 8 {
            held -= 8;
            bytes.push(u8::try_from(bits >> held & 0xff).unwrap_or_default());
        }
    }
    if unpadded.len() % 4 == 1 {
        return Err("Last unit does not have enough valid bits".into());
    }
    <[u8; 16]>::try_from(bytes.as_slice()).map_err(|_| {
        format!(
            "Input string `{value}` decoded as {} bytes, which is not equal to the expected 16 \
             bytes of a base64-encoded UUID",
            bytes.len()
        )
    })
}

/// Kafka's `TopicFilter.IncludeList`: the `--topic` value as a whole-name
/// regular expression.
#[derive(Debug)]
pub struct IncludeList(Regex);

impl IncludeList {
    /// Builds the filter as `TopicFilter` does: trimmed, `,` read as `|`,
    /// spaces removed, and surrounding quotes stripped.
    ///
    /// Rust's `regex` has no look-around or back-references, so a pattern
    /// that uses them is refused here although `java.util.regex` accepts it.
    pub fn new(raw: &str) -> Result<Self, String> {
        let pattern = raw
            .trim()
            .replace(',', "|")
            .replace(' ', "")
            .trim_start_matches(['"', '\''])
            .trim_end_matches(['"', '\''])
            .to_owned();
        Regex::new(&format!("^(?:{pattern})$"))
            .map(Self)
            .map_err(|_| format!("{pattern} is an invalid regex."))
    }

    /// Whether the whole of `topic` matches, as `String.matches` does.
    pub fn matches(&self, topic: &str) -> bool {
        self.0.is_match(topic)
    }
}

/// Kafka's `Topic.isInternal`.
pub fn is_internal(topic: &str) -> bool {
    matches!(
        topic,
        "__consumer_offsets" | "__transaction_state" | "__share_group_state"
    )
}

/// Kafka's `Topic.hasCollisionChars`.
pub fn has_collision_chars(topic: &str) -> bool {
    topic.contains(['.', '_'])
}
