//! The JVM behaviours that `kafka-topics` output depends on.
//!
//! `kafka-topics` prints topics and topic configs in the iteration order of
//! `java.util.HashMap`, splits its arguments with `String.split`, prints topic
//! IDs as Kafka's `Uuid.toString` does, and matches `--topic` as a
//! `java.util.regex` pattern. Byte-level agreement with its stdout needs each
//! of these reproduced.

use krabka_client_admin::Config;
use krabka_ids::KafkaUuid;
use regex::Regex;

pub use crate::jvm::parse_int;
use crate::jvm::{Table, default_capacity, hash_order, order_in_table, string_hash};

/// The order in which `kafka-topics --describe` prints the topics it
/// resolved by name, given them in sorted order.
///
/// `KafkaAdminClient.describeTopics` puts the names into a
/// `HashMap(names.size())`, returns a copy of it, and
/// `DescribeTopicsResult.allTopicNames` collects that copy into a third
/// `HashMap(size)`. `TopicCommand` prints the values of the third.
pub fn describe_order(sorted: Vec<String>) -> Vec<String> {
    let len = sorted.len();
    let hash = |name: &String| string_hash(name);
    let first = hash_order(sorted, Table::WithCapacity(len), hash);
    let copy = hash_order(first, Table::Copy, hash);
    hash_order(copy, Table::WithCapacity(len), hash)
}

/// The names in the order of the default `HashMap` that holds them.
pub fn default_order(names: Vec<String>) -> Vec<String> {
    hash_order(names, Table::Default, |name| string_hash(name))
}

/// The non-default configs of a topic, as `kafka-topics --describe` prints
/// them after `Configs:`: each entry whose source is not the built-in
/// default, with Java's `null` for a withheld value.
///
/// `Config` keeps every entry of the `DescribeConfigs` answer in a default
/// `HashMap`, and the entries print in its iteration order, so the table has
/// grown for all of them. Two entries that share a bucket print in the
/// broker's answer order, which this approximates with name order.
pub fn non_default_configs(config: &Config) -> Vec<(String, String)> {
    let non_default = config
        .entries
        .values()
        .filter(|entry| !entry.is_default())
        .map(|entry| {
            let value = entry.value.clone().unwrap_or_else(|| "null".to_owned());
            (entry.name.clone(), value)
        })
        .collect();
    order_in_table(
        non_default,
        default_capacity(config.entries.len()),
        |(name, _)| string_hash(name),
    )
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

/// Kafka's `Uuid.toString`: URL-safe base64 of the 16 bytes, unpadded.
pub fn uuid_to_string(bytes: &[u8; 16]) -> String {
    KafkaUuid(uuid::Uuid::from_bytes(*bytes)).to_string()
}

/// Kafka's `Uuid.fromString`, with the messages of the exceptions it and
/// `Base64.getUrlDecoder` throw.
pub fn uuid_from_string(value: &str) -> Result<[u8; 16], String> {
    value
        .parse::<KafkaUuid>()
        .map(|id| *id.0.as_bytes())
        .map_err(|error| error.to_string())
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
