//! The two JSON files of `kafka-reassign-partitions`, read and written as
//! Kafka reads and writes them.
//!
//! The reassignment file is `{"version":1,"partitions":[{"topic":"foo",
//! "partition":0,"replicas":[1,2],"log_dirs":["any","any"]}]}`. The
//! topics-to-move file is `{"version":1,"topics":[{"topic":"foo"}]}`.
//! Operators keep these files in source control and feed them to either tool,
//! so the writer produces Jackson's bytes: the same key order, no spaces.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    jvm::{collection_to_string as to_string, hash_set_order, string_hash},
    kafka_json,
    topic_partition::TopicPartition,
};

/// The `log_dirs` entry that leaves the replica in whatever log dir it has.
pub const ANY_LOG_DIR: &str = "any";

/// One entry of a reassignment file, in file order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTarget {
    pub partition: TopicPartition,
    pub replicas: Vec<i32>,
    /// The log dir of each replica, `any` for none.
    pub log_dirs: Vec<String>,
}

/// A parsed reassignment file. Duplicates are kept, so the caller can refuse
/// them as Kafka does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReassignmentFile {
    pub partitions: Vec<PartitionTarget>,
}

/// One replica of a partition on one broker, as Kafka's
/// `TopicPartitionReplica`. It renders as `topic-partition-broker`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Replica {
    pub broker: i32,
    pub topic: String,
    pub partition: i32,
}

impl std::fmt::Display for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.topic, self.partition, self.broker)
    }
}

impl ReassignmentFile {
    /// The target replicas of each entry, in file order.
    pub fn targets(&self) -> Vec<(TopicPartition, Vec<i32>)> {
        self.partitions
            .iter()
            .map(|target| (target.partition.clone(), target.replicas.clone()))
            .collect()
    }

    /// The replicas that the file moves to a named log dir, as Kafka's
    /// `replicaAssignment` map.
    pub fn log_dir_moves(&self) -> BTreeMap<Replica, String> {
        let mut moves = BTreeMap::new();
        for target in &self.partitions {
            for (broker, log_dir) in target.replicas.iter().zip(&target.log_dirs) {
                if log_dir != ANY_LOG_DIR {
                    moves.insert(
                        Replica {
                            broker: *broker,
                            topic: target.partition.topic.clone(),
                            partition: target.partition.partition,
                        },
                        log_dir.clone(),
                    );
                }
            }
        }
        moves
    }
}

/// `ReassignPartitionsCommand.parsePartitionReassignmentData`.
///
/// # Errors
/// Returns the message that `kafka-reassign-partitions` prints for the same
/// file.
pub fn parse_reassignment(text: &str) -> Result<ReassignmentFile, String> {
    let document = kafka_json::try_parse_full(text)?;
    let object = kafka_json::document_object(document.as_ref())?;
    let version = object.get("version").map_or(Ok(1), kafka_json::int)?;
    if version != 1 {
        return Err(format!("Not supported version field value {version}"));
    }
    let mut file = ReassignmentFile::default();
    let Some(partitions) = object.get("partitions") else {
        return Ok(file);
    };
    for entry in kafka_json::array(partitions)? {
        let entry = kafka_json::object(entry)?;
        let topic = kafka_json::string(kafka_json::field(entry, "topic")?)?;
        let partition = kafka_json::int(kafka_json::field(entry, "partition")?)?;
        let replicas = kafka_json::int_list(kafka_json::field(entry, "replicas")?)?;
        let log_dirs = match entry.get("log_dirs") {
            Some(log_dirs) => kafka_json::string_list(log_dirs)?,
            None => vec![ANY_LOG_DIR.to_owned(); replicas.len()],
        };
        let partition = TopicPartition::new(topic, partition);
        if replicas.len() != log_dirs.len() {
            return Err(format!(
                "Size of replicas list {} is different from size of log dirs list {} for \
                 partition {partition}",
                to_string(&replicas),
                to_string(&log_dirs)
            ));
        }
        file.partitions.push(PartitionTarget {
            partition,
            replicas,
            log_dirs,
        });
    }
    Ok(file)
}

/// The duplicates of `values`, each once, in the iteration order of Kafka's
/// `ToolsUtils.duplicates` result, a `HashSet`.
pub fn duplicates<T: PartialEq + Clone>(values: &[T], hash: impl Fn(&T) -> i32) -> Vec<T> {
    let found = values
        .iter()
        .enumerate()
        .filter(|(index, value)| values[..*index].contains(value))
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    hash_set_order(found, hash)
}

/// `ReassignPartitionsCommand.parseExecuteAssignmentArgs`: the refusals of
/// `--execute` for a parsed file.
///
/// # Errors
/// Returns the message that `kafka-reassign-partitions --execute` prints.
pub fn check_execute(file: &ReassignmentFile) -> Result<(), String> {
    if file.partitions.is_empty() {
        return Err("Partition reassignment list cannot be empty".into());
    }
    if file
        .partitions
        .iter()
        .any(|target| target.replicas.is_empty())
    {
        return Err("Partition replica list cannot be empty".into());
    }
    let partitions = file
        .partitions
        .iter()
        .map(|target| target.partition.clone())
        .collect::<Vec<_>>();
    let repeated = duplicates(&partitions, TopicPartition::java_hash);
    if !repeated.is_empty() {
        return Err(format!(
            "Partition reassignment contains duplicate topic partitions: {}",
            repeated
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    let repeated_replicas = file
        .partitions
        .iter()
        .filter_map(|target| {
            let repeated = duplicates(&target.replicas, |id| *id);
            (!repeated.is_empty()).then(|| {
                format!(
                    "{} contains multiple entries for {}",
                    target.partition,
                    repeated
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
        })
        .collect::<Vec<_>>();
    if !repeated_replicas.is_empty() {
        return Err(format!(
            "Partition replica lists may not contain duplicate entries: {}",
            repeated_replicas.join(". ")
        ));
    }
    Ok(())
}

/// `ReassignPartitionsCommand.parseTopicsData`.
///
/// # Errors
/// Returns the message that `kafka-reassign-partitions --generate` prints.
pub fn parse_topics(text: &str) -> Result<Vec<String>, String> {
    let document = kafka_json::parse_full(text)
        .ok_or_else(|| "The input string is not a valid JSON".to_owned())?;
    let object = kafka_json::document_object(document.as_ref())?;
    let version = object.get("version").map_or(Ok(1), kafka_json::int)?;
    if version != 1 {
        return Err(format!("Not supported version field value {version}"));
    }
    let Some(topics) = object.get("topics") else {
        return Ok(Vec::new());
    };
    kafka_json::array(topics)?
        .iter()
        .map(|entry| kafka_json::string(kafka_json::field(kafka_json::object(entry)?, "topic")?))
        .collect()
}

/// `ReassignPartitionsCommand.parseGenerateAssignmentArgs`: the brokers of
/// `--broker-list` and the topics of the topics-to-move file.
///
/// # Errors
/// Returns the message that `kafka-reassign-partitions --generate` prints.
pub fn parse_generate(
    topics_json: &str,
    broker_list: &str,
) -> Result<(Vec<i32>, Vec<String>), String> {
    let brokers = java_split(broker_list, ',')
        .into_iter()
        .map(|id| {
            id.parse::<i32>()
                .map_err(|_| format!("For input string: \"{id}\""))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let repeated = duplicates(&brokers, |id| *id);
    if !repeated.is_empty() {
        return Err(format!(
            "Broker list contains duplicate entries: {}",
            to_string(&repeated)
        ));
    }
    let topics = parse_topics(topics_json)?;
    let repeated = duplicates(&topics, |topic| string_hash(topic));
    if !repeated.is_empty() {
        return Err(format!(
            "List of topics to reassign contains duplicate entries: {}",
            to_string(&repeated)
        ));
    }
    Ok((brokers, topics))
}

/// Java's `String.split` with a single-character pattern: trailing empty
/// strings are dropped, but an empty input gives one empty string.
fn java_split(value: &str, separator: char) -> Vec<&str> {
    let mut parts = value.split(separator).collect::<Vec<_>>();
    if value.is_empty() {
        return parts;
    }
    while parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}

#[derive(Serialize)]
struct FileJson<'a> {
    version: i32,
    partitions: Vec<EntryJson<'a>>,
}

#[derive(Serialize)]
struct EntryJson<'a> {
    topic: &'a str,
    partition: i32,
    replicas: &'a [i32],
    log_dirs: Vec<&'a str>,
}

/// `ReassignPartitionsCommand.formatAsReassignmentJson`: the file of
/// `assignment`, sorted by topic and partition, with each replica's log dir
/// from `log_dirs` or `any`.
pub fn format_reassignment(
    assignment: &BTreeMap<TopicPartition, Vec<i32>>,
    log_dirs: &BTreeMap<Replica, String>,
) -> String {
    let partitions = assignment
        .iter()
        .map(|(partition, replicas)| EntryJson {
            topic: &partition.topic,
            partition: partition.partition,
            replicas,
            log_dirs: replicas
                .iter()
                .map(|broker| {
                    log_dirs
                        .get(&Replica {
                            broker: *broker,
                            topic: partition.topic.clone(),
                            partition: partition.partition,
                        })
                        .map_or(ANY_LOG_DIR, String::as_str)
                })
                .collect(),
        })
        .collect();
    serde_json::to_string(&FileJson {
        version: 1,
        partitions,
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod tests;
