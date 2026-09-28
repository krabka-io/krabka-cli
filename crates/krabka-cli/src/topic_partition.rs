//! Kafka's `TopicPartition`: a partition named by its topic and index.

use std::fmt;

use serde::Serialize;

use crate::jvm::topic_partition_hash;

/// One partition of one topic.
///
/// The order is by topic name, then by partition index, as Kafka's tools
/// sort their output. `Display` renders `topic-partition`, as Kafka's
/// `TopicPartition.toString` does.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct TopicPartition {
    /// The topic name.
    pub topic: String,
    /// The partition index.
    pub partition: i32,
}

impl TopicPartition {
    /// The partition `partition` of `topic`.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl TopicPartition {
    /// Java's `TopicPartition.hashCode`, which decides the order in which
    /// Kafka's tools print a set of partitions.
    #[must_use]
    pub fn java_hash(&self) -> i32 {
        topic_partition_hash(&self.topic, self.partition)
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

/// Joins the partitions with `separator`, as Kafka joins them in its output.
pub fn join<'a>(
    partitions: impl IntoIterator<Item = &'a TopicPartition>,
    separator: &str,
) -> String {
    partitions
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(separator)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn partitions_sort_by_topic_then_index_and_render_as_kafka_does() {
        let mut partitions = vec![
            TopicPartition::new("foo", 10),
            TopicPartition::new("bar", 2),
            TopicPartition::new("foo", 9),
        ];
        partitions.sort();
        check!(join(&partitions, ",") == "bar-2,foo-9,foo-10");
        // 31 * (31 + 9) + "foo".hashCode()
        check!(TopicPartition::new("foo", 9).java_hash() == 102_814);
    }
}
