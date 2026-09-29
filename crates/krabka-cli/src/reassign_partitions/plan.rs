//! The pure parts of `kafka-reassign-partitions`: partition states, moves,
//! throttle values, and the lines that the tool prints.

use std::collections::{BTreeMap, BTreeSet};

use krabka_client_admin::{ClusterNode, KafkaError, PartitionAssignment, ReplicaLogDirInfo};
use serde_json::{Value, json};

use super::files::Replica;
use crate::{
    common::java_message,
    replica_placer::UsableBroker,
    topic_partition::{TopicPartition, join},
};

/// `leader.replication.throttled.rate` and the other throttle configs.
pub const LEADER_RATE: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_RATE: &str = "follower.replication.throttled.rate";
pub const LOG_DIR_RATE: &str = "replica.alter.log.dirs.io.max.bytes.per.second";
pub const LEADER_REPLICAS: &str = "leader.replication.throttled.replicas";
pub const FOLLOWER_REPLICAS: &str = "follower.replication.throttled.replicas";

/// Broker IDs joined with commas, as Kafka prints a replica list.
pub fn ids(ids: &[i32]) -> String {
    ids.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// `ReassignPartitionsCommand.curReassignmentsToString`.
pub fn list_lines(active: &BTreeMap<TopicPartition, PartitionAssignment>) -> Vec<String> {
    if active.is_empty() {
        return vec!["No partition reassignments found.".into()];
    }
    std::iter::once("Current partition reassignments:".to_owned())
        .chain(active.iter().map(|(partition, assignment)| {
            let mut line = format!("{partition}: replicas: {}.", ids(&assignment.replicas));
            for (label, replicas) in [
                (" adding: ", &assignment.adding_replicas),
                (" removing: ", &assignment.removing_replicas),
            ] {
                if !replicas.is_empty() {
                    line.push_str(label);
                    line.push_str(&ids(replicas));
                    line.push('.');
                }
            }
            line
        }))
        .collect()
}

/// Kafka's `PartitionReassignmentState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionState {
    pub current: Vec<i32>,
    pub target: Vec<i32>,
    /// No reassignment of the partition is active.
    pub done: bool,
}

/// `ReassignPartitionsCommand.findPartitionReassignmentStates`: an active
/// reassignment is in progress; otherwise the partition's current replicas,
/// or none for a partition that does not exist, against the target.
pub fn partition_states(
    targets: &[(TopicPartition, Vec<i32>)],
    active: &BTreeMap<TopicPartition, PartitionAssignment>,
    described: &BTreeMap<TopicPartition, Vec<i32>>,
) -> BTreeMap<TopicPartition, PartitionState> {
    targets
        .iter()
        .map(|(partition, target)| {
            let state = match active.get(partition) {
                Some(assignment) => PartitionState {
                    current: assignment.replicas.clone(),
                    target: target.clone(),
                    done: false,
                },
                None => PartitionState {
                    current: described.get(partition).cloned().unwrap_or_default(),
                    target: target.clone(),
                    done: true,
                },
            };
            (partition.clone(), state)
        })
        .collect()
}

/// `ReassignPartitionsCommand.partitionReassignmentStatesToString`.
pub fn status_lines(states: &BTreeMap<TopicPartition, PartitionState>) -> Vec<String> {
    std::iter::once("Status of partition reassignment:".to_owned())
        .chain(states.iter().map(|(partition, state)| {
            if !state.done {
                format!("Reassignment of partition {partition} is still in progress.")
            } else if state.current == state.target {
                format!("Reassignment of partition {partition} is completed.")
            } else {
                format!(
                    "There is no active reassignment of partition {partition}, but replica set \
                     is {} rather than {}.",
                    ids(&state.current),
                    ids(&state.target)
                )
            }
        }))
        .collect()
}

/// The JSON rendering of the partition states.
pub fn states_json(states: &BTreeMap<TopicPartition, PartitionState>) -> Vec<Value> {
    states
        .iter()
        .map(|(partition, state)| {
            let progress = match (state.done, state.current == state.target) {
                (false, _) => "in_progress",
                (true, true) => "completed",
                (true, false) => "mismatch",
            };
            json!({
                "topic": partition.topic,
                "partition": partition.partition,
                "current_replicas": state.current,
                "target_replicas": state.target,
                "status": progress,
            })
        })
        .collect()
}

/// The sources and destinations of one partition's movement, as Kafka's
/// `PartitionMove`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionMove {
    pub sources: BTreeSet<i32>,
    pub destinations: BTreeSet<i32>,
}

/// Every moving partition, by topic and partition index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MoveMap(pub BTreeMap<String, BTreeMap<i32, PartitionMove>>);

impl MoveMap {
    /// `ReassignPartitionsCommand.calculateProposedMoveMap`: the moves of the
    /// active reassignments, then of the proposed ones. A proposal for a
    /// moving partition keeps that movement's sources.
    ///
    /// # Errors
    /// Returns Kafka's message for a proposal with no current replicas.
    pub fn proposed(
        active: &BTreeMap<TopicPartition, PartitionAssignment>,
        proposed: &BTreeMap<TopicPartition, Vec<i32>>,
        current: &BTreeMap<TopicPartition, Vec<i32>>,
    ) -> Result<Self, String> {
        let mut moves = BTreeMap::<String, BTreeMap<i32, PartitionMove>>::new();
        for (partition, assignment) in active {
            let destinations = assignment
                .adding_replicas
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let sources = assignment
                .replicas
                .iter()
                .copied()
                .filter(|id| !destinations.contains(id))
                .collect();
            moves.entry(partition.topic.clone()).or_default().insert(
                partition.partition,
                PartitionMove {
                    sources,
                    destinations,
                },
            );
        }
        for (partition, replicas) in proposed {
            let topic = moves.entry(partition.topic.clone()).or_default();
            let sources = match (topic.get(&partition.partition), current.get(partition)) {
                (Some(movement), _) => movement.sources.clone(),
                (None, Some(current)) => current.iter().copied().collect(),
                (None, None) => {
                    return Err(format!(
                        "Trying to reassign a topic partition {partition} with 0 replicas"
                    ));
                }
            };
            let destinations = replicas
                .iter()
                .copied()
                .filter(|id| !sources.contains(id))
                .collect();
            topic.insert(
                partition.partition,
                PartitionMove {
                    sources,
                    destinations,
                },
            );
        }
        Ok(Self(moves))
    }

    /// `calculateLeaderThrottles`: `partition:broker` for each source, by
    /// topic, sorted as strings.
    pub fn leader_throttles(&self) -> BTreeMap<String, String> {
        self.throttles(|movement| movement.sources.iter().copied().collect())
    }

    /// `calculateFollowerThrottles`: `partition:broker` for each destination
    /// that is not a source, by topic, sorted as strings.
    pub fn follower_throttles(&self) -> BTreeMap<String, String> {
        self.throttles(|movement| {
            movement
                .destinations
                .difference(&movement.sources)
                .copied()
                .collect()
        })
    }

    fn throttles(&self, brokers: impl Fn(&PartitionMove) -> Vec<i32>) -> BTreeMap<String, String> {
        self.0
            .iter()
            .map(|(topic, partitions)| {
                let components = partitions
                    .iter()
                    .flat_map(|(partition, movement)| {
                        brokers(movement)
                            .into_iter()
                            .map(move |broker| format!("{partition}:{broker}"))
                    })
                    .collect::<BTreeSet<_>>();
                (
                    topic.clone(),
                    components.into_iter().collect::<Vec<_>>().join(","),
                )
            })
            .collect()
    }

    /// `calculateReassigningBrokers`: every source and destination.
    pub fn brokers(&self) -> BTreeSet<i32> {
        self.0
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(|movement| movement.sources.iter().chain(&movement.destinations))
            .copied()
            .collect()
    }
}

/// The throttle configs that `--execute` writes: per-topic throttled
/// replicas, and per-broker rates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Throttles {
    /// Each topic's configs, by topic.
    pub topics: BTreeMap<String, BTreeMap<&'static str, String>>,
    /// Each broker's configs, by broker ID.
    pub brokers: BTreeMap<i32, BTreeMap<&'static str, String>>,
    throttle: i64,
    log_dir_throttle: i64,
}

impl Throttles {
    /// What `kafka-reassign-partitions --execute` writes for `throttle` and
    /// `log_dir_throttle`, each of which is off when negative.
    pub fn new(
        moves: &MoveMap,
        log_dir_moves: &BTreeMap<Replica, String>,
        throttle: i64,
        log_dir_throttle: i64,
    ) -> Self {
        let mut throttles = Self {
            throttle,
            log_dir_throttle,
            ..Self::default()
        };
        if throttle >= 0 {
            let leaders = moves.leader_throttles();
            let followers = moves.follower_throttles();
            for (topic, value) in leaders {
                throttles
                    .topics
                    .entry(topic)
                    .or_default()
                    .insert(LEADER_REPLICAS, value);
            }
            for (topic, value) in followers {
                throttles
                    .topics
                    .entry(topic)
                    .or_default()
                    .insert(FOLLOWER_REPLICAS, value);
            }
            for broker in moves.brokers() {
                let configs = throttles.brokers.entry(broker).or_default();
                configs.insert(LEADER_RATE, throttle.to_string());
                configs.insert(FOLLOWER_RATE, throttle.to_string());
            }
        }
        if log_dir_throttle >= 0 {
            for replica in log_dir_moves.keys() {
                throttles
                    .brokers
                    .entry(replica.broker)
                    .or_default()
                    .insert(LOG_DIR_RATE, log_dir_throttle.to_string());
            }
        }
        throttles
    }

    /// The warning that `--execute` prints before it writes a throttle.
    pub fn warning_line(&self) -> Option<String> {
        (self.throttle >= 0 || self.log_dir_throttle >= 0).then(|| {
            "Warning: You must run --verify periodically, until the reassignment completes, to \
             ensure the throttle is removed."
                .to_owned()
        })
    }

    /// The line that `--execute` prints once the inter-broker throttle is
    /// written.
    pub fn inter_broker_line(&self) -> Option<String> {
        (self.throttle >= 0).then(|| {
            format!(
                "The inter-broker throttle limit was set to {} B/s",
                self.throttle
            )
        })
    }

    /// The line that `--execute` prints once the log dir throttle is
    /// written.
    pub fn log_dir_line(&self) -> Option<String> {
        (self.log_dir_throttle >= 0).then(|| {
            format!(
                "The replica-alter-dir throttle limit was set to {} B/s",
                self.log_dir_throttle
            )
        })
    }

    /// The lines that `--execute` prints about the throttles.
    pub fn lines(&self) -> Vec<String> {
        [
            self.warning_line(),
            self.inter_broker_line(),
            self.log_dir_line(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// The configs of `keys` of each broker that has any, as Kafka writes
    /// each group in one `incrementalAlterConfigs` call.
    pub fn broker_configs(&self, keys: &[&str]) -> BTreeMap<i32, BTreeMap<&'static str, String>> {
        self.brokers
            .iter()
            .map(|(broker, configs)| {
                let configs = configs
                    .iter()
                    .filter(|(name, _)| keys.contains(name))
                    .map(|(name, value)| (*name, value.clone()))
                    .collect::<BTreeMap<_, _>>();
                (*broker, configs)
            })
            .filter(|(_, configs)| !configs.is_empty())
            .collect()
    }

    /// The JSON rendering of the configs.
    pub fn json(&self) -> Value {
        json!({
            "topics": self.topics,
            "brokers": self
                .brokers
                .iter()
                .map(|(broker, configs)| (broker.to_string(), configs))
                .collect::<BTreeMap<_, _>>(),
        })
    }
}

/// `ReassignPartitionsCommand.currentPartitionReplicaAssignmentToString`.
pub fn rollback_lines(current_json: &str) -> Vec<String> {
    vec![
        "Current partition replica assignment".into(),
        String::new(),
        current_json.to_owned(),
        String::new(),
        "Save this to use as the --reassignment-json-file option during rollback".into(),
    ]
}

/// The success line of `--execute`.
pub fn started_line(partitions: &[TopicPartition]) -> String {
    format!(
        "Successfully started partition reassignment{} for {}",
        if partitions.len() == 1 { "" } else { "s" },
        join(partitions, ",")
    )
}

/// The lines that `--execute` prints as each log dir move starts. Kafka
/// ends each with a space.
pub fn move_lines(moves: &BTreeMap<Replica, String>) -> Vec<String> {
    moves
        .iter()
        .map(|(replica, log_dir)| {
            format!(
                "Successfully started moving log directory to {log_dir} for replica {}-{} with \
                 broker {} ",
                replica.topic, replica.partition, replica.broker
            )
        })
        .collect()
}

/// The success line of `--cancel`.
pub fn cancelled_line(partitions: &BTreeSet<TopicPartition>) -> String {
    format!(
        "Successfully cancelled partition reassignment{} for: {}",
        if partitions.len() == 1 { "" } else { "s" },
        join(partitions, ",")
    )
}

/// `partition: message` for each partition that the controller refused,
/// sorted, as Kafka lists the errors of `--execute` and `--cancel`. The
/// message is the exception's: the broker's, or the error's default.
pub fn partition_errors(errors: &BTreeMap<TopicPartition, KafkaError>) -> Vec<String> {
    errors
        .iter()
        .map(|(partition, error)| format!("{partition}: {}", java_message(error)))
        .collect()
}

/// Where one replica's log dir move stands, as Kafka's `LogDirMoveState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveState {
    /// The replica is in no live log dir of its broker.
    MissingLogDir,
    /// The replica is being copied to `future`.
    Active {
        current: String,
        target: String,
        future: String,
    },
    /// No move is active and the replica is not in the target log dir.
    Cancelled { current: String, target: String },
    /// The replica is in the target log dir.
    Completed,
}

impl MoveState {
    /// `ReassignPartitionsCommand.findLogDirMoveStates` for one replica.
    pub fn new(info: &ReplicaLogDirInfo, target: &str) -> Self {
        match (&info.current, &info.future) {
            (None, _) => Self::MissingLogDir,
            (Some(current), None) if current.path == target => Self::Completed,
            (Some(current), None) => Self::Cancelled {
                current: current.path.clone(),
                target: target.to_owned(),
            },
            (Some(current), Some(future)) => Self::Active {
                current: current.path.clone(),
                target: target.to_owned(),
                future: future.path.clone(),
            },
        }
    }

    /// Whether the move is over, as `LogDirMoveState.done`.
    pub const fn done(&self) -> bool {
        matches!(self, Self::Cancelled { .. } | Self::Completed)
    }
}

/// `ReassignPartitionsCommand.replicaMoveStatesToString`, one entry per
/// line, by broker, topic and partition.
pub fn move_state_lines(states: &BTreeMap<Replica, MoveState>) -> Vec<String> {
    states
        .iter()
        .map(|(replica, state)| {
            let partition = format!("{}-{}", replica.topic, replica.partition);
            let broker = replica.broker;
            match state {
                MoveState::MissingLogDir => format!(
                    "Partition {partition} is not found in any live log dir on broker {broker}. \
                     There is likely an offline log directory on the broker."
                ),
                MoveState::Active { target, future, .. } if target == future => {
                    format!("Reassignment of replica {replica} is still in progress.")
                }
                MoveState::Active { target, future, .. } => format!(
                    "Partition {partition} on broker {broker} is being moved to log dir {future} \
                     instead of {target}."
                ),
                MoveState::Cancelled { current, target } => format!(
                    "Partition {partition} on broker {broker} is not being moved from log dir \
                     {current} to {target}."
                ),
                MoveState::Completed => {
                    format!("Reassignment of replica {replica} completed successfully.")
                }
            }
        })
        .collect()
}

/// The line that `clearAllThrottles` prints for `names`, each a broker or
/// a topic.
pub fn clearing_line(level: &str, noun: &str, names: &[String]) -> String {
    format!(
        "Clearing {level}-level throttles on {noun}{} {}",
        if names.len() == 1 { "" } else { "s" },
        names.join(",")
    )
}

/// `ReassignPartitionsCommand.getBrokerMetadata`: the nodes of `brokers`
/// that the cluster has, with their racks when rack-aware.
///
/// # Errors
/// Returns Kafka's message when only some of the brokers have a rack.
pub fn usable_brokers(
    nodes: &[ClusterNode],
    brokers: &[i32],
    rack_aware: bool,
) -> Result<Vec<UsableBroker>, String> {
    let usable = nodes
        .iter()
        .filter(|node| brokers.contains(&node.id))
        .map(|node| UsableBroker {
            id: node.id,
            rack: node.rack.clone().filter(|_| rack_aware),
            fenced: false,
        })
        .collect::<Vec<_>>();
    let rackless = usable.iter().filter(|broker| broker.rack.is_none()).count();
    if rack_aware && rackless != 0 && rackless != usable.len() {
        return Err(
            "Not all brokers have rack information. Add --disable-rack-aware in command \
                    line to make replica assignment without rack information."
                .into(),
        );
    }
    Ok(usable)
}

#[cfg(test)]
mod tests;
