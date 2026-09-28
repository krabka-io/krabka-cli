//! `kafka-topics --describe`: the per-topic summary line, the per-partition
//! lines, and the five incident-triage selectors that filter them.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use serde_json::{Value, json};

use super::java::{config_order, uuid_to_string};

/// One partition as `kafka-topics` sees it: Kafka's `TopicPartitionInfo`,
/// with node ids for nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub index: i32,
    /// The leader, or `None` when the partition has no leader or the leader
    /// is not a live broker. `kafka-topics` prints `none` for it.
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    /// The eligible leader replicas (KIP-966), or `None` when the source of
    /// the description does not carry them. `kafka-topics` prints `N/A` for
    /// `None`.
    pub elr: Option<Vec<i32>>,
    pub last_known_elr: Option<Vec<i32>>,
}

/// One topic as `kafka-topics` sees it: Kafka's `TopicDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    /// The topic ID, or `None` for the zero ID, which `kafka-topics` does not
    /// print.
    pub id: Option<[u8; 16]>,
    /// The partitions, in partition order.
    pub partitions: Vec<Partition>,
}

/// An ongoing reassignment of one partition, as Kafka's
/// `PartitionReassignment`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reassignment {
    pub replicas: Vec<i32>,
    pub adding: Vec<i32>,
    pub removing: Vec<i32>,
}

/// Kafka's `TopicCommand.isReassignmentInProgress`: a replica of the
/// partition is still being added or removed.
fn reassignment_in_progress(partition: &Partition, reassignment: Option<&Reassignment>) -> bool {
    reassignment.is_some_and(|reassignment| {
        partition.replicas.iter().any(|replica| {
            reassignment.adding.contains(replica) || reassignment.removing.contains(replica)
        })
    })
}

/// Kafka's `TopicCommand.getReplicationFactor`: during a reassignment the
/// target replica count without the replicas being added.
pub fn replication_factor(partition: &Partition, reassignment: Option<&Reassignment>) -> usize {
    match reassignment {
        Some(reassignment) if reassignment_in_progress(partition, Some(reassignment)) => {
            reassignment
                .replicas
                .len()
                .saturating_sub(reassignment.adding.len())
        }
        _ => partition.replicas.len(),
    }
}

/// `--under-replicated-partitions`: fewer in-sync replicas than the
/// replication factor.
pub fn is_under_replicated(partition: &Partition, reassignment: Option<&Reassignment>) -> bool {
    replication_factor(partition, reassignment) > partition.isr.len()
}

/// `--unavailable-partitions`: no leader, or a leader that is not a live
/// broker.
pub fn is_unavailable(partition: &Partition, live_brokers: &BTreeSet<i32>) -> bool {
    partition
        .leader
        .is_none_or(|leader| !live_brokers.contains(&leader))
}

/// `--under-min-isr-partitions`: no leader, or fewer in-sync replicas than
/// `min.insync.replicas`. As in Kafka, the minimum is read only for a
/// partition that has a leader.
pub fn is_under_min_isr(
    partition: &Partition,
    min_isr: impl FnOnce() -> Result<usize, String>,
) -> Result<bool, String> {
    if partition.leader.is_none() {
        return Ok(true);
    }
    Ok(partition.isr.len() < min_isr()?)
}

/// `--at-min-isr-partitions`: exactly `min.insync.replicas` in-sync
/// replicas.
pub fn is_at_min_isr(
    partition: &Partition,
    min_isr: impl FnOnce() -> Result<usize, String>,
) -> Result<bool, String> {
    Ok(min_isr()? == partition.isr.len())
}

/// One of `kafka-topics --describe`'s filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Selector {
    /// `--under-replicated-partitions`.
    UnderReplicated,
    /// `--unavailable-partitions`.
    Unavailable,
    /// `--under-min-isr-partitions`.
    UnderMinIsr,
    /// `--at-min-isr-partitions`.
    AtMinIsr,
    /// `--topics-with-overrides`.
    TopicsWithOverrides,
}

/// The filters that `--describe` applies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selectors(pub BTreeSet<Selector>);

impl Selectors {
    pub fn has(&self, selector: Selector) -> bool {
        self.0.contains(&selector)
    }

    /// Kafka's `DescribeOptions.describeConfigs`: the summary line prints
    /// unless a partition selector is set.
    fn describe_configs(&self) -> bool {
        !(self.has(Selector::Unavailable)
            || self.has(Selector::UnderReplicated)
            || self.has(Selector::UnderMinIsr)
            || self.has(Selector::AtMinIsr))
    }

    /// Kafka's `DescribeOptions.describePartitions`.
    fn describe_partitions(&self) -> bool {
        !self.has(Selector::TopicsWithOverrides)
    }
}

/// Everything `--describe` needs about one topic.
pub struct TopicReport<'a> {
    pub topic: &'a Topic,
    /// The topic's non-default configs, by name.
    pub configs: &'a BTreeMap<String, String>,
    /// The effective `min.insync.replicas`, or why it is not known.
    pub min_isr: Result<usize, String>,
    /// The ongoing reassignments, by partition.
    pub reassignments: &'a BTreeMap<i32, Reassignment>,
}

fn joined(ids: &[i32]) -> String {
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn elr_field(ids: Option<&Vec<i32>>) -> String {
    ids.map_or_else(|| "N/A".to_owned(), |ids| joined(ids))
}

/// Kafka's `TopicDescription.printDescription`.
fn summary_line(topic: &Topic, replication_factor: usize, configs: &[(String, String)]) -> String {
    let mut line = format!("Topic: {}", topic.name);
    if let Some(id) = &topic.id {
        let _ = write!(line, "\tTopicId: {}", uuid_to_string(id));
    }
    let configs = configs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    let _ = write!(
        line,
        "\tPartitionCount: {}\tReplicationFactor: {replication_factor}\tConfigs: {configs}",
        topic.partitions.len()
    );
    line
}

/// Kafka's `PartitionDescription.printDescription`.
fn partition_line(
    topic: &str,
    partition: &Partition,
    reassignment: Option<&Reassignment>,
) -> String {
    let leader = partition
        .leader
        .map_or_else(|| "none".to_owned(), |leader| leader.to_string());
    let mut line = format!(
        "\tTopic: {topic}\tPartition: {}\tLeader: {leader}\tReplicas: {}\tIsr: {}",
        partition.index,
        joined(&partition.replicas),
        joined(&partition.isr),
    );
    if let Some(reassignment) = reassignment {
        let _ = write!(
            line,
            "\tAdding Replicas: {}\tRemoving Replicas: {}",
            joined(&reassignment.adding),
            joined(&reassignment.removing),
        );
    }
    let _ = write!(
        line,
        "\tElr: {}\tLastKnownElr: {}",
        elr_field(partition.elr.as_ref()),
        elr_field(partition.last_known_elr.as_ref()),
    );
    line
}

fn partition_json(partition: &Partition, reassignment: Option<&Reassignment>) -> Value {
    json!({
        "partition": partition.index,
        "leader": partition.leader,
        "replicas": partition.replicas,
        "isr": partition.isr,
        "adding_replicas": reassignment.map(|r| &r.adding),
        "removing_replicas": reassignment.map(|r| &r.removing),
        "elr": partition.elr,
        "last_known_elr": partition.last_known_elr,
    })
}

/// The human lines and the JSON value of one described topic, or `None`
/// when the selectors print nothing of it.
///
/// This is Kafka's `printDescribeConfig` followed by
/// `printPartitionDescription`.
pub fn describe_topic(
    report: &TopicReport<'_>,
    selectors: &Selectors,
    live_brokers: &BTreeSet<i32>,
) -> Result<Option<(Vec<String>, Value)>, String> {
    let topic = report.topic;
    let mut lines = Vec::new();
    let first = topic.partitions.first();
    let replication_factor = first.map_or(0, |partition| {
        replication_factor(partition, report.reassignments.get(&partition.index))
    });
    let configs = config_order(
        report
            .configs
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    );
    if selectors.describe_configs()
        && (!selectors.has(Selector::TopicsWithOverrides) || !configs.is_empty())
    {
        lines.push(summary_line(topic, replication_factor, &configs));
    }
    let mut partitions = Vec::new();
    if selectors.describe_partitions() {
        for partition in &topic.partitions {
            let reassignment = report.reassignments.get(&partition.index);
            if should_print(partition, reassignment, report, selectors, live_brokers)? {
                lines.push(partition_line(&topic.name, partition, reassignment));
                partitions.push(partition_json(partition, reassignment));
            }
        }
    }
    if lines.is_empty() {
        return Ok(None);
    }
    let value = json!({
        "topic": topic.name,
        "topic_id": topic.id.as_ref().map(uuid_to_string),
        "partition_count": topic.partitions.len(),
        "replication_factor": replication_factor,
        "configs": report.configs,
        "partitions": partitions,
    });
    Ok(Some((lines, value)))
}

/// Kafka's `DescribeOptions.shouldPrintTopicPartition`, with its
/// short-circuit order.
fn should_print(
    partition: &Partition,
    reassignment: Option<&Reassignment>,
    report: &TopicReport<'_>,
    selectors: &Selectors,
    live_brokers: &BTreeSet<i32>,
) -> Result<bool, String> {
    let min_isr = || report.min_isr.clone();
    Ok(selectors.describe_configs()
        || (selectors.has(Selector::UnderReplicated)
            && is_under_replicated(partition, reassignment))
        || (selectors.has(Selector::Unavailable) && is_unavailable(partition, live_brokers))
        || (selectors.has(Selector::UnderMinIsr) && is_under_min_isr(partition, min_isr)?)
        || (selectors.has(Selector::AtMinIsr) && is_at_min_isr(partition, min_isr)?))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // Under replicated, unavailable, under min ISR, at min ISR.
    type Predicates = (bool, bool, Result<bool, String>, Result<bool, String>);
    type Rendered = Result<Option<String>, String>;

    fn partition(index: i32, leader: Option<i32>, replicas: &[i32], isr: &[i32]) -> Partition {
        Partition {
            index,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            elr: None,
            last_known_elr: None,
        }
    }

    #[test]
    fn the_replication_factor_discounts_replicas_being_added() {
        let moving = Reassignment {
            replicas: vec![1, 2, 3, 4],
            adding: vec![3, 4],
            removing: vec![],
        };
        let finished = Reassignment {
            replicas: vec![5, 6],
            adding: vec![7],
            removing: vec![8],
        };
        let cases = [
            (partition(0, Some(1), &[1, 2, 3, 4], &[1, 2]), None, 4),
            (
                partition(0, Some(1), &[1, 2, 3, 4], &[1, 2]),
                Some(&moving),
                2,
            ),
            // A reassignment that no longer touches the replicas is ignored.
            (partition(0, Some(1), &[1, 2], &[1, 2]), Some(&finished), 2),
        ];
        for (partition, reassignment, expected) in cases {
            assert!(
                replication_factor(&partition, reassignment) == expected,
                "{partition:?}"
            );
        }
    }

    #[test]
    fn each_selector_predicate_matches_exactly_its_partitions() {
        let live = BTreeSet::from([1, 2, 3]);
        let healthy = partition(0, Some(1), &[1, 2, 3], &[1, 2, 3]);
        let lagging = partition(1, Some(1), &[1, 2, 3], &[1, 2]);
        let leaderless = partition(2, None, &[1, 2, 3], &[]);
        let dead_leader = partition(3, Some(9), &[9, 2], &[2]);
        let at_one = partition(4, Some(1), &[1], &[1]);
        let two = || Ok(2);
        let unknown = || Err("unknown".to_owned());
        let cases: &[(&str, &Partition, Predicates)] = &[
            ("healthy", &healthy, (false, false, Ok(false), Ok(false))),
            ("lagging", &lagging, (true, false, Ok(false), Ok(true))),
            ("leaderless", &leaderless, (true, true, Ok(true), Ok(false))),
            (
                "dead leader",
                &dead_leader,
                (true, true, Ok(true), Ok(false)),
            ),
            ("at one", &at_one, (false, false, Ok(true), Ok(false))),
        ];
        for (name, partition, expected) in cases {
            assert!(
                (
                    is_under_replicated(partition, None),
                    is_unavailable(partition, &live),
                    is_under_min_isr(partition, two),
                    is_at_min_isr(partition, two),
                ) == *expected,
                "{name}"
            );
        }
        // A leaderless partition is under min ISR without the minimum being
        // read; every other case needs it.
        assert!(is_under_min_isr(&leaderless, unknown) == Ok(true));
        assert!(is_under_min_isr(&healthy, unknown) == Err("unknown".into()));
        assert!(is_at_min_isr(&leaderless, unknown) == Err("unknown".into()));
    }

    fn orders() -> Topic {
        Topic {
            name: "orders".into(),
            id: Some([
                0xe0, 0x88, 0x08, 0x30, 0x48, 0x19, 0x41, 0x84, 0xb9, 0x21, 0xc9, 0xd2, 0xce, 0x31,
                0xc0, 0xab,
            ]),
            partitions: vec![
                partition(0, Some(1), &[1, 2], &[1, 2]),
                Partition {
                    elr: Some(vec![]),
                    last_known_elr: Some(vec![2]),
                    ..partition(1, None, &[2, 1], &[])
                },
                partition(2, Some(2), &[2, 1, 3], &[2]),
            ],
        }
    }

    fn render(
        topic: &Topic,
        configs: &BTreeMap<String, String>,
        min_isr: Result<usize, String>,
        reassignments: &BTreeMap<i32, Reassignment>,
        selectors: &[Selector],
    ) -> Result<Option<String>, String> {
        let report = TopicReport {
            topic,
            configs,
            min_isr,
            reassignments,
        };
        let selectors = Selectors(selectors.iter().copied().collect());
        Ok(
            describe_topic(&report, &selectors, &BTreeSet::from([1, 2, 3]))?
                .map(|(lines, _)| lines.join("\n") + "\n"),
        )
    }

    #[test]
    fn describe_renders_the_jvm_tab_delimited_shape() {
        let topic = orders();
        let configs = BTreeMap::from([
            ("retention.ms".to_owned(), "1000".to_owned()),
            ("min.insync.replicas".to_owned(), "2".to_owned()),
        ]);
        let reassignments = BTreeMap::from([(
            2,
            Reassignment {
                replicas: vec![2, 1, 3],
                adding: vec![3],
                removing: vec![],
            },
        )]);
        let summary = "Topic: orders\tTopicId: 4IgIMEgZQYS5IcnSzjHAqw\tPartitionCount: 3\t\
                       ReplicationFactor: 2\tConfigs: min.insync.replicas=2,retention.ms=1000\n";
        let p0 = "\tTopic: orders\tPartition: 0\tLeader: 1\tReplicas: 1,2\tIsr: 1,2\tElr: N/A\t\
                  LastKnownElr: N/A\n";
        let p1 = "\tTopic: orders\tPartition: 1\tLeader: none\tReplicas: 2,1\tIsr: \tElr: \t\
                  LastKnownElr: 2\n";
        let p2 = "\tTopic: orders\tPartition: 2\tLeader: 2\tReplicas: 2,1,3\tIsr: 2\tAdding \
                  Replicas: 3\tRemoving Replicas: \tElr: N/A\tLastKnownElr: N/A\n";
        let cases: &[(&str, &[Selector], Option<String>)] = &[
            ("plain", &[], Some(format!("{summary}{p0}{p1}{p2}"))),
            (
                "under replicated",
                &[Selector::UnderReplicated],
                Some(format!("{p1}{p2}")),
            ),
            ("unavailable", &[Selector::Unavailable], Some(p1.to_owned())),
            (
                "under min isr",
                &[Selector::UnderMinIsr],
                Some(format!("{p1}{p2}")),
            ),
            ("at min isr", &[Selector::AtMinIsr], Some(p0.to_owned())),
            (
                "two selectors print the union",
                &[Selector::Unavailable, Selector::AtMinIsr],
                Some(format!("{p0}{p1}")),
            ),
            (
                "overrides",
                &[Selector::TopicsWithOverrides],
                Some(summary.to_owned()),
            ),
        ];
        for (name, selectors, expected) in cases {
            assert!(
                render(&topic, &configs, Ok(2), &reassignments, selectors) == Ok(expected.clone()),
                "{name}"
            );
        }
    }

    #[test]
    fn a_topic_prints_nothing_when_no_selected_partition_matches() {
        let topic = Topic {
            name: "quiet".into(),
            id: None,
            partitions: vec![partition(0, Some(1), &[1], &[1])],
        };
        let empty = BTreeMap::new();
        let cases: [(&[Selector], Rendered); 4] = [
            (&[Selector::TopicsWithOverrides], Ok(None)),
            (&[Selector::UnderReplicated], Ok(None)),
            (
                &[],
                Ok(Some(
                    "Topic: quiet\tPartitionCount: 1\tReplicationFactor: 1\tConfigs: \n\t\
                     Topic: quiet\tPartition: 0\tLeader: 1\tReplicas: 1\tIsr: 1\tElr: N/A\t\
                     LastKnownElr: N/A\n"
                        .to_owned(),
                )),
            ),
            // The minimum is needed and unknown, so the selector fails.
            (&[Selector::AtMinIsr], Err("unknown".to_owned())),
        ];
        for (selectors, expected) in cases {
            assert!(
                render(
                    &topic,
                    &empty,
                    Err("unknown".into()),
                    &BTreeMap::new(),
                    selectors
                ) == expected,
                "{selectors:?}"
            );
        }
    }

    #[test]
    fn the_json_rendering_carries_the_printed_partitions() {
        let topic = orders();
        let configs = BTreeMap::from([("retention.ms".to_owned(), "1000".to_owned())]);
        let reassignments = BTreeMap::new();
        let report = TopicReport {
            topic: &topic,
            configs: &configs,
            min_isr: Ok(1),
            reassignments: &reassignments,
        };
        let selectors = Selectors(BTreeSet::from([Selector::Unavailable]));
        let (_, value) = describe_topic(&report, &selectors, &BTreeSet::from([1, 2]))
            .unwrap()
            .unwrap();
        assert!(
            value
                == json!({
                    "topic": "orders",
                    "topic_id": "4IgIMEgZQYS5IcnSzjHAqw",
                    "partition_count": 3,
                    "replication_factor": 2,
                    "configs": {"retention.ms": "1000"},
                    "partitions": [{
                        "partition": 1,
                        "leader": null,
                        "replicas": [2, 1],
                        "isr": [],
                        "adding_replicas": null,
                        "removing_replicas": null,
                        "elr": [],
                        "last_known_elr": [2],
                    }],
                })
        );
    }
}
