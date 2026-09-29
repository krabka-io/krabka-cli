//! `--config` and `--replica-assignment`, parsed and checked as
//! `kafka-topics` does before it sends a request.

use std::{collections::BTreeMap, sync::LazyLock};

use regex::Regex;

use super::java::{default_order, parse_int, split};

/// The type of a topic config value, as Kafka's `ConfigDef.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Type {
    Int,
    Long,
    Double,
    Boolean,
    String,
    List,
}

impl Type {
    const fn name(self) -> &'static str {
        match self {
            Self::Int => "INT",
            Self::Long => "LONG",
            Self::Double => "DOUBLE",
            Self::Boolean => "BOOLEAN",
            Self::String => "STRING",
            Self::List => "LIST",
        }
    }
}

/// Every topic config that Kafka 4.3.1's `LogConfig` defines, internal ones
/// included, in definition order. `ConfigDef.parse` checks values in this
/// order, so the first bad value in it is the one Kafka reports.
const TOPIC_CONFIGS: &[(&str, Type)] = &[
    ("segment.bytes", Type::Int),
    ("segment.ms", Type::Long),
    ("segment.jitter.ms", Type::Long),
    ("segment.index.bytes", Type::Int),
    ("flush.messages", Type::Long),
    ("flush.ms", Type::Long),
    ("retention.bytes", Type::Long),
    ("retention.ms", Type::Long),
    ("max.message.bytes", Type::Int),
    ("index.interval.bytes", Type::Int),
    ("delete.retention.ms", Type::Long),
    ("min.compaction.lag.ms", Type::Long),
    ("max.compaction.lag.ms", Type::Long),
    ("file.delete.delay.ms", Type::Long),
    ("min.cleanable.dirty.ratio", Type::Double),
    ("cleanup.policy", Type::List),
    ("unclean.leader.election.enable", Type::Boolean),
    ("min.insync.replicas", Type::Int),
    ("compression.type", Type::String),
    ("compression.gzip.level", Type::Int),
    ("compression.lz4.level", Type::Int),
    ("compression.zstd.level", Type::Int),
    ("preallocate", Type::Boolean),
    ("message.timestamp.type", Type::String),
    ("message.timestamp.before.max.ms", Type::Long),
    ("message.timestamp.after.max.ms", Type::Long),
    ("leader.replication.throttled.replicas", Type::List),
    ("follower.replication.throttled.replicas", Type::List),
    ("remote.storage.enable", Type::Boolean),
    ("local.retention.ms", Type::Long),
    ("local.retention.bytes", Type::Long),
    ("remote.log.copy.disable", Type::Boolean),
    ("remote.log.delete.on.disable", Type::Boolean),
    ("internal.segment.bytes", Type::Int),
];

static CONFIG_SEPARATOR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s*=\s*").expect("the separator is a valid regex"));

/// Whether `value` parses as `kind`, as `ConfigDef.parseType` reads a string.
fn parses_as(value: &str, kind: Type) -> bool {
    let value = value.trim();
    match kind {
        Type::Int => value.parse::<i32>().is_ok(),
        Type::Long => value.parse::<i64>().is_ok(),
        // `Double.parseDouble` also takes a `d` or `f` suffix.
        Type::Double => value
            .strip_suffix(['d', 'D', 'f', 'F'])
            .unwrap_or(value)
            .parse::<f64>()
            .is_ok(),
        Type::Boolean => value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false"),
        Type::String | Type::List => true,
    }
}

/// Parses the `--config` values as `TopicCommand.parseTopicConfigsToBeAdded`
/// does, then checks the names and the value types as `LogConfig.validate`
/// does.
///
/// Each value splits on `=` with the whitespace around it, and must give
/// exactly a key and a value. Kafka's value range checks run on the broker,
/// which answers a range error in the `CreateTopics` response.
pub fn parse_topic_configs(values: &[String]) -> Result<BTreeMap<String, String>, String> {
    let pairs = values
        .iter()
        .map(|value| split(value, &CONFIG_SEPARATOR))
        .collect::<Vec<_>>();
    if !pairs.iter().all(|pair| pair.len() == 2) {
        return Err(
            "requirement failed: Invalid topic config: all configs to be added must be in \
                    the format \"key=val\"."
                .into(),
        );
    }
    let mut configs = BTreeMap::new();
    let mut names = Vec::new();
    for pair in pairs {
        let (key, value) = (pair[0].trim().to_owned(), pair[1].trim().to_owned());
        if !names.contains(&key) {
            names.push(key.clone());
        }
        configs.insert(key, value);
    }
    // `validateNames` walks the keys of the `HashMap` that holds them.
    for name in default_order(names) {
        if !TOPIC_CONFIGS.iter().any(|(known, _)| *known == name) {
            return Err(format!("Unknown topic config name: {name}"));
        }
    }
    for (name, kind) in TOPIC_CONFIGS {
        if let Some(value) = configs.get(*name)
            && !parses_as(value, *kind)
        {
            let reason = match kind {
                Type::Boolean => "Expected value to be either true or false".to_owned(),
                other => format!("Not a number of type {}", other.name()),
            };
            return Err(format!(
                "Invalid value {value} for configuration {name}: {reason}"
            ));
        }
    }
    Ok(configs)
}

/// Parses `--replica-assignment` as `TopicCommand.parseReplicaAssignment`
/// does: partitions split on `,`, replicas on `:`, each broker id trimmed.
///
/// A partition that names a broker twice, or that has a different replica
/// count from partition 0, is refused with Kafka's message.
pub fn parse_replica_assignment(value: &str) -> Result<Vec<Vec<i32>>, String> {
    static COMMA: LazyLock<Regex> = LazyLock::new(|| Regex::new(",").expect("valid regex"));
    static COLON: LazyLock<Regex> = LazyLock::new(|| Regex::new(":").expect("valid regex"));
    let mut assignment: Vec<Vec<i32>> = Vec::new();
    for (index, partition) in split(value, &COMMA).iter().enumerate() {
        let brokers = split(partition, &COLON)
            .iter()
            .map(|broker| parse_int(broker.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = Vec::new();
        let mut duplicates = Vec::new();
        for broker in &brokers {
            if seen.contains(broker) {
                if !duplicates.contains(broker) {
                    duplicates.push(*broker);
                }
            } else {
                seen.push(*broker);
            }
        }
        if !duplicates.is_empty() {
            // `ToolsUtils.duplicates` returns a `HashSet<Integer>`, which
            // iterates small ids in ascending order.
            duplicates.sort_unstable();
            let listed = duplicates
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            return Err(format!(
                "Partition replica lists may not contain duplicate entries: {listed}"
            ));
        }
        if assignment
            .first()
            .is_some_and(|first| first.len() != brokers.len())
        {
            let listed = brokers
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "Partition {index} has different replication factor: [{listed}]"
            ));
        }
        assignment.push(brokers);
    }
    Ok(assignment)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    type ParsedConfigs = Result<&'static [(&'static str, &'static str)], &'static str>;
    type ParsedAssignment = Result<Vec<Vec<i32>>, &'static str>;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn topic_configs_parse_or_fail_with_the_kafka_message() {
        let format = "requirement failed: Invalid topic config: all configs to be added must be \
                      in the format \"key=val\".";
        let cases: &[(&[&str], ParsedConfigs)] = &[
            (&[], Ok(&[])),
            (&["retention.ms=1000"], Ok(&[("retention.ms", "1000")])),
            (
                &[" cleanup.policy = compact ", "retention.ms=5"],
                Ok(&[("cleanup.policy", "compact"), ("retention.ms", "5")]),
            ),
            // Java's `split` drops the trailing empty string.
            (&["retention.ms=5="], Ok(&[("retention.ms", "5")])),
            (
                &["retention.ms=1", "retention.ms=2"],
                Ok(&[("retention.ms", "2")]),
            ),
            (
                &["min.cleanable.dirty.ratio=0.5d"],
                Ok(&[("min.cleanable.dirty.ratio", "0.5d")]),
            ),
            (&["preallocate=TRUE"], Ok(&[("preallocate", "TRUE")])),
            (&["retention.ms"], Err(format)),
            (&["a=b=c"], Err(format)),
            (&["="], Err(format)),
            // A separator at the start leaves an empty key, as in Java.
            (&["=5"], Err("Unknown topic config name: ")),
            (&["foo=bar"], Err("Unknown topic config name: foo")),
            (
                &["retention.ms=abc"],
                Err("Invalid value abc for configuration retention.ms: Not a number of type LONG"),
            ),
            (
                &["segment.bytes=1.5"],
                Err("Invalid value 1.5 for configuration segment.bytes: Not a number of type INT"),
            ),
            (
                &["preallocate=yes"],
                Err(
                    "Invalid value yes for configuration preallocate: Expected value to be \
                     either true or false",
                ),
            ),
            // `ConfigDef.parse` reports the first bad value in definition order.
            (
                &["retention.ms=x", "segment.ms=y"],
                Err("Invalid value y for configuration segment.ms: Not a number of type LONG"),
            ),
        ];
        for (values, expected) in cases {
            let expected = expected
                .map(|pairs| {
                    pairs
                        .iter()
                        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                        .collect::<BTreeMap<_, _>>()
                })
                .map_err(str::to_owned);
            assert!(
                parse_topic_configs(&strings(values)) == expected,
                "{values:?}"
            );
        }
    }

    #[test]
    fn replica_assignments_parse_or_fail_with_the_kafka_message() {
        let cases: &[(&str, ParsedAssignment)] = &[
            ("1", Ok(vec![vec![1]])),
            ("1:2,2:3", Ok(vec![vec![1, 2], vec![2, 3]])),
            (" 1 : 2 , 3:4", Ok(vec![vec![1, 2], vec![3, 4]])),
            ("1,1", Ok(vec![vec![1], vec![1]])),
            ("1:", Ok(vec![vec![1]])),
            (
                "1:1",
                Err("Partition replica lists may not contain duplicate entries: 1"),
            ),
            (
                "3:2:3:2",
                Err("Partition replica lists may not contain duplicate entries: 2,3"),
            ),
            (
                "1,1:2",
                Err("Partition 1 has different replication factor: [1, 2]"),
            ),
            ("a", Err("For input string: \"a\"")),
            (":1", Err("For input string: \"\"")),
            ("1,,2", Err("For input string: \"\"")),
            ("99999999999", Err("For input string: \"99999999999\"")),
        ];
        for (value, expected) in cases {
            let expected = expected.clone().map_err(str::to_owned);
            assert!(parse_replica_assignment(value) == expected, "{value}");
        }
    }
}
