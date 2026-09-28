//! `krabka log-dirs`, the counterpart of `kafka-log-dirs`.
//!
//! `--describe` asks every broker, or the brokers of `--broker-list`, for its
//! log directories and prints the JVM tool's three stdout lines, the last of
//! which is its single-line JSON document, byte for byte. `DescribeLogDirs`
//! answers for the broker it reaches, so the command connects to each broker
//! in turn, at most `BROKER_FAN_OUT` at once.
//!
//! `kafka-log-dirs` aborts with a stack trace when one broker fails. This
//! command instead prints the document for the brokers that answered, names
//! each failed broker on stderr, and exits 1. With every broker answering, the
//! output is the JVM tool's.
//!
//! `--alter` moves replicas between the log directories of one broker
//! (KIP-113). The JVM tools do that only through `kafka-reassign-partitions`,
//! so the flag is a krabka addition, and it goes through the confirmation and
//! `--dry-run` guardrails.

use std::collections::{BTreeMap, BTreeSet};

use clap::{ArgGroup, Args};
use krabka_client_admin::{AlterReplicaLogDirOutcome, LogDirInfo};
use krabka_client_core::ConnectionOptions;
use serde_json::{Value, json};

use crate::{
    cluster::{BROKER_FAN_OUT, broker_client, cluster_brokers},
    compat::{
        KafkaException, copy_capacity, default_capacity, hash_order, hash_set_copy_capacity,
        presized_capacity, string_hash, topic_partition_hash,
    },
    connection::ConnectionArgs,
    fan_out,
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
};

/// `CLUSTER_AUTHORIZATION_FAILED`: what Kafka's admin client reports for a
/// broker that answers with no log directory.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// `LOG_DIR_NOT_FOUND`: the broker has no log directory at the target path.
const LOG_DIR_NOT_FOUND: i16 = 57;

#[derive(Debug, Args)]
#[command(
    group(ArgGroup::new("action").required(true).multiple(false).args(["describe", "alter"])),
    // `requires` against a bare flag is always met, because clap counts the
    // flag's `false` default as present. A group of one is met only when the
    // flag is given.
    group(ArgGroup::new("describing").args(["describe"])),
    group(ArgGroup::new("altering").args(["alter"])),
)]
pub struct LogDirsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Describe the specified log directories on the specified brokers.
    #[arg(long)]
    describe: bool,
    /// Move replicas between the log directories of one broker (krabka).
    #[arg(long, requires_all = ["broker", "replica_move"])]
    alter: bool,
    /// The list of topics to be queried in the form "topic1,topic2,topic3".
    /// All topics will be queried if no topic list is specified.
    #[arg(long, default_value = "", requires = "describing")]
    topic_list: String,
    /// The list of brokers to be queried in the form "0,1,2". All brokers in
    /// the cluster will be queried if no broker list is specified.
    #[arg(long, default_value = "", requires = "describing")]
    broker_list: String,
    /// With `--alter`: the broker whose replicas move.
    #[arg(long, requires = "altering")]
    broker: Option<i32>,
    /// With `--alter`: one move, `TOPIC:PARTITION=LOG_DIR`. Repeatable.
    #[arg(long = "move", value_parser = parse_move, requires = "altering")]
    replica_move: Vec<ReplicaMove>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// One `--move`: a replica of `broker` and the directory it moves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaMove {
    topic: String,
    partition: i32,
    log_dir: String,
}

fn parse_move(value: &str) -> Result<ReplicaMove, String> {
    let (replica, log_dir) = value
        .split_once('=')
        .ok_or_else(|| format!("expected TOPIC:PARTITION=LOG_DIR, got {value}"))?;
    let (topic, partition) = replica
        .rsplit_once(':')
        .ok_or_else(|| format!("expected TOPIC:PARTITION=LOG_DIR, got {value}"))?;
    let partition = partition
        .parse::<i32>()
        .ok()
        .filter(|partition| *partition >= 0)
        .ok_or_else(|| format!("invalid partition '{partition}' in {value}"))?;
    if topic.is_empty() || log_dir.is_empty() {
        return Err(format!("expected TOPIC:PARTITION=LOG_DIR, got {value}"));
    }
    Ok(ReplicaMove {
        topic: topic.to_owned(),
        partition,
        log_dir: log_dir.to_owned(),
    })
}

/// Splits a `kafka-log-dirs` list at commas and drops empty entries.
fn split_list(value: &str) -> impl Iterator<Item = &str> {
    value.split(',').filter(|entry| !entry.is_empty())
}

/// `--broker-list` as Kafka reads it, in first-seen order without repeats.
fn broker_list(value: &str) -> Result<Vec<i32>, CommandError> {
    let mut brokers = Vec::new();
    for entry in split_list(value) {
        let broker = entry
            .parse::<i32>()
            .map_err(|_| format!("For input string: \"{entry}\""))?;
        if !brokers.contains(&broker) {
            brokers.push(broker);
        }
    }
    Ok(brokers)
}

/// `Integer.hashCode()`.
const fn integer_hash(value: i32) -> i32 {
    value
}

/// The brokers to query, in the order `kafka-log-dirs` holds them: every
/// cluster broker when `requested` is empty, else the requested ones. A
/// requested broker the cluster does not have is an error that names both
/// sets, as Kafka words it.
fn brokers_to_query(cluster: &[i32], requested: &[i32]) -> Result<Vec<i32>, CommandError> {
    let cluster_set = hash_order(
        cluster.to_vec(),
        default_capacity(cluster.len()),
        |broker| integer_hash(*broker),
    );
    let missing = requested
        .iter()
        .copied()
        .filter(|broker| !cluster.contains(broker))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        let missing = hash_order(missing, hash_set_copy_capacity(requested.len()), |broker| {
            integer_hash(*broker)
        });
        return Err(format!(
            "ERROR: The given brokers do not exist from --broker-list: {}. Current existent brokers: {}",
            join(&missing),
            join(&cluster_set)
        )
        .into());
    }
    let wanted = if requested.is_empty() {
        cluster_set
    } else {
        requested.to_vec()
    };
    let count = wanted.len();
    Ok(hash_order(
        wanted,
        hash_set_copy_capacity(count),
        |broker| integer_hash(*broker),
    ))
}

/// The order of the brokers in the JSON document: the maps that Kafka's admin
/// client copies the per-broker results through.
fn document_order(brokers: Vec<i32>) -> Vec<i32> {
    let count = brokers.len();
    let brokers = hash_order(brokers, presized_capacity(count, count), |broker| {
        integer_hash(*broker)
    });
    let brokers = hash_order(brokers, copy_capacity(count), |broker| {
        integer_hash(*broker)
    });
    hash_order(brokers, presized_capacity(count, count), |broker| {
        integer_hash(*broker)
    })
}

fn join(brokers: &[i32]) -> String {
    brokers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// One replica of a log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Replica {
    topic: String,
    partition: i32,
    size: i64,
    offset_lag: i64,
    is_future: bool,
}

/// One log directory, its replicas already in the order Kafka prints them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogDir {
    path: String,
    error: Option<&'static str>,
    replicas: Vec<Replica>,
}

/// The log directories of one broker as `kafka-log-dirs` prints them: in the
/// order of its `HashMap`s, with the replicas of `topics` only, or of every
/// topic when `topics` is empty.
fn printable_log_dirs(infos: &[LogDirInfo], topics: &BTreeSet<&str>) -> Vec<LogDir> {
    let dirs = infos
        .iter()
        .map(|info| {
            let replicas = info
                .topics
                .iter()
                .flat_map(|topic| {
                    topic.partitions.iter().map(|partition| Replica {
                        topic: topic.name.clone(),
                        partition: partition.partition_index,
                        size: partition.partition_size,
                        offset_lag: partition.offset_lag,
                        is_future: partition.is_future_key,
                    })
                })
                .collect::<Vec<_>>();
            let hash = |replica: &Replica| topic_partition_hash(&replica.topic, replica.partition);
            let all = replicas.len();
            let replicas = hash_order(replicas, default_capacity(all), hash)
                .into_iter()
                .filter(|replica| topics.is_empty() || topics.contains(replica.topic.as_str()))
                .collect::<Vec<_>>();
            let kept = replicas.len();
            LogDir {
                path: info.log_dir.clone(),
                error: info
                    .error
                    .as_ref()
                    .map(|error| KafkaException::for_code(error.code).class()),
                replicas: hash_order(replicas, default_capacity(kept), hash),
            }
        })
        .collect::<Vec<_>>();
    let count = dirs.len();
    hash_order(dirs, presized_capacity(count, count), |dir| {
        string_hash(&dir.path)
    })
}

/// A JSON string literal.
fn quoted(value: &str) -> String {
    Value::String(value.to_owned()).to_string()
}

/// The JSON document of `kafka-log-dirs`, with its keys in the order that
/// Jackson writes the tool's `HashMap`s.
fn document(brokers: &[(i32, Vec<LogDir>)]) -> String {
    let brokers = brokers
        .iter()
        .map(|(broker, dirs)| {
            let dirs = dirs
                .iter()
                .map(|dir| {
                    let partitions = dir
                        .replicas
                        .iter()
                        .map(|replica| {
                            format!(
                                "{{\"partition\":{},\"size\":{},\"offsetLag\":{},\"isFuture\":{}}}",
                                quoted(&format!("{}-{}", replica.topic, replica.partition)),
                                replica.size,
                                replica.offset_lag,
                                replica.is_future
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    format!(
                        "{{\"partitions\":[{partitions}],\"error\":{},\"logDir\":{}}}",
                        dir.error.map_or_else(|| "null".to_owned(), quoted),
                        quoted(&dir.path)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{\"broker\":{broker},\"logDirs\":[{dirs}]}}")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{\"brokers\":[{brokers}],\"version\":1}}")
}

/// What one broker answered, or why it did not.
type BrokerAnswer = Result<Vec<LogDirInfo>, CommandError>;

/// The `--describe` report from each queried broker's answer, in query order.
fn describe_result(queried: &[i32], answers: Vec<BrokerAnswer>, topics: &str) -> CommandResult {
    let topics = split_list(topics).collect::<BTreeSet<_>>();
    let mut answered = BTreeMap::new();
    let mut notices = Vec::new();
    let mut failed_brokers = Vec::new();
    for (broker, answer) in queried.iter().zip(answers) {
        let answer = answer.and_then(|infos| {
            if infos.is_empty() {
                Err(CommandError::Other(
                    KafkaException::for_code(CLUSTER_AUTHORIZATION_FAILED).to_java_string(),
                ))
            } else {
                Ok(infos)
            }
        });
        match answer {
            Ok(infos) => {
                answered.insert(*broker, printable_log_dirs(&infos, &topics));
            }
            Err(error) => {
                notices.push(format!(
                    "ERROR: failed to describe the log directories of broker {broker}: {error}"
                ));
                failed_brokers.push(json!({"broker": broker, "message": error.to_string()}));
            }
        }
    }
    let responded = queried
        .iter()
        .copied()
        .filter(|broker| answered.contains_key(broker))
        .collect::<Vec<_>>();
    let brokers = document_order(responded.clone())
        .into_iter()
        .filter_map(|broker| answered.remove(&broker).map(|dirs| (broker, dirs)))
        .collect::<Vec<_>>();
    let document = document(&brokers);
    let mut data: Value = serde_json::from_str(&document).expect("the document is JSON");
    data["failed_brokers"] = Value::Array(failed_brokers);
    let failed = !notices.is_empty();
    CommandResult::rows(
        vec![
            "Querying brokers for log directories information".to_owned(),
            format!(
                "Received log directory information from brokers {}",
                join(&responded)
            ),
            document,
        ],
        data,
        failed,
    )
    .with_notices(notices)
}

async fn describe_broker(endpoint: &str, options: &ConnectionOptions) -> BrokerAnswer {
    let mut client = broker_client(endpoint, options).await?;
    Ok(client.describe_log_dirs(None).await?)
}

impl LogDirsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        if !self.connection.bootstrap_controller.is_empty() {
            return Err(
                "--bootstrap-controller is not supported by log-dirs; use --bootstrap-server"
                    .into(),
            );
        }
        if self.connection.bootstrap_server.is_empty() {
            return Err("Missing required argument \"[bootstrap-server]\"".into());
        }
        if self.confirm.dry_run && self.describe {
            return Err("--dry-run is only valid with --alter".into());
        }
        let options = self.connection.options("log-dirs").await?;
        let brokers = cluster_brokers(&self.connection, &options).await?;
        if self.alter {
            return self.alter(&brokers, &options).await;
        }
        let requested = broker_list(&self.broker_list)?;
        let queried = brokers_to_query(&brokers.keys().copied().collect::<Vec<_>>(), &requested)?;
        let answers = fan_out::bounded(queried.clone(), BROKER_FAN_OUT, |broker| {
            let endpoint = brokers[&broker].clone();
            let options = &options;
            async move { describe_broker(&endpoint, options).await }
        })
        .await;
        Ok(describe_result(&queried, answers, &self.topic_list))
    }

    async fn alter(
        &self,
        brokers: &BTreeMap<i32, String>,
        options: &ConnectionOptions,
    ) -> Result<CommandResult, CommandError> {
        let broker = self.broker.ok_or("--broker is required with --alter")?;
        let endpoint = brokers.get(&broker).ok_or_else(|| {
            format!(
                "ERROR: The given broker does not exist: {broker}. Current existent brokers: {}",
                join(&brokers.keys().copied().collect::<Vec<_>>())
            )
        })?;
        let mut client = broker_client(endpoint, options).await?;
        let mut filter = BTreeMap::<String, Vec<i32>>::new();
        for replica in &self.replica_move {
            filter
                .entry(replica.topic.clone())
                .or_default()
                .push(replica.partition);
        }
        let current = client.describe_log_dirs(Some(&filter)).await?;
        if self.confirm.dry_run {
            let outcomes = self
                .replica_move
                .iter()
                .map(|replica| AlterReplicaLogDirOutcome {
                    topic: replica.topic.clone(),
                    partition: replica.partition,
                    error: (!current.iter().any(|dir| dir.log_dir == replica.log_dir)).then(|| {
                        krabka_client_admin::KafkaError {
                            code: LOG_DIR_NOT_FOUND,
                            name: "LOG_DIR_NOT_FOUND",
                            message: Some(
                                KafkaException::for_code(LOG_DIR_NOT_FOUND)
                                    .message()
                                    .to_owned(),
                            ),
                        }
                    }),
                })
                .collect::<Vec<_>>();
            return Ok(moved(broker, &self.replica_move, &current, &outcomes).into_dry_run());
        }
        confirm(
            self.confirm.yes,
            "krabka log-dirs",
            Impact {
                summary: format!(
                    "move {} replica(s) between the log directories of broker {broker}",
                    self.replica_move.len()
                ),
                resources: self
                    .replica_move
                    .iter()
                    .map(|replica| {
                        format!(
                            "{}-{} -> {}",
                            replica.topic, replica.partition, replica.log_dir
                        )
                    })
                    .collect(),
            },
        )
        .await?;
        let mut assignments = BTreeMap::<String, BTreeMap<String, Vec<i32>>>::new();
        for replica in &self.replica_move {
            assignments
                .entry(replica.log_dir.clone())
                .or_default()
                .entry(replica.topic.clone())
                .or_default()
                .push(replica.partition);
        }
        let assignments = assignments
            .into_iter()
            .map(|(dir, topics)| (dir, topics.into_iter().collect()))
            .collect();
        let outcomes = client.alter_replica_log_dirs(&assignments).await?;
        Ok(moved(broker, &self.replica_move, &current, &outcomes))
    }
}

/// The directory that holds the current replica of `topic`-`partition`.
fn current_dir<'a>(current: &'a [LogDirInfo], topic: &str, partition: i32) -> Option<&'a str> {
    current
        .iter()
        .find(|dir| {
            dir.topics.iter().any(|entry| {
                entry.name == topic
                    && entry.partitions.iter().any(|replica| {
                        replica.partition_index == partition && !replica.is_future_key
                    })
            })
        })
        .map(|dir| dir.log_dir.as_str())
}

/// The `--alter` report: one line per requested move.
fn moved(
    broker: i32,
    moves: &[ReplicaMove],
    current: &[LogDirInfo],
    outcomes: &[AlterReplicaLogDirOutcome],
) -> CommandResult {
    let rows = moves
        .iter()
        .map(|replica| {
            let error = outcomes
                .iter()
                .find(|outcome| {
                    outcome.topic == replica.topic && outcome.partition == replica.partition
                })
                .and_then(|outcome| outcome.error.clone());
            (
                replica,
                current_dir(current, &replica.topic, replica.partition),
                error,
            )
        })
        .collect::<Vec<_>>();
    let human = rows
        .iter()
        .map(|(replica, from, error)| match error {
            Some(error) => format!(
                "{}-{}\tERROR\t{} ({})",
                replica.topic, replica.partition, error.name, error.code
            ),
            None => format!(
                "Moving replica {}-{} on broker {broker} from {} to {}.",
                replica.topic,
                replica.partition,
                from.unwrap_or("-"),
                replica.log_dir
            ),
        })
        .collect();
    let data = rows
        .iter()
        .map(|(replica, from, error)| {
            json!({
                "broker": broker,
                "topic": replica.topic,
                "partition": replica.partition,
                "from": from,
                "to": replica.log_dir,
                "error": kafka_error(error.as_ref()),
            })
        })
        .collect::<Vec<_>>();
    CommandResult::rows(human, data, rows.iter().any(|(.., error)| error.is_some()))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser;
    use krabka_client_admin::{KafkaError, LogDirPartitionInfo, LogDirTopicInfo};

    use super::*;

    #[derive(Debug, Parser)]
    struct Command {
        #[command(flatten)]
        args: LogDirsArgs,
    }

    fn partition(index: i32, size: i64, offset_lag: i64, is_future: bool) -> LogDirPartitionInfo {
        LogDirPartitionInfo {
            partition_index: index,
            partition_size: size,
            offset_lag,
            is_future_key: is_future,
        }
    }

    fn dir(
        path: &str,
        error: Option<i16>,
        topics: Vec<(&str, Vec<LogDirPartitionInfo>)>,
    ) -> LogDirInfo {
        LogDirInfo {
            log_dir: path.into(),
            error: error.map(|code| KafkaError {
                code,
                name: "KAFKA_STORAGE_ERROR",
                message: None,
            }),
            topics: topics
                .into_iter()
                .map(|(name, partitions)| LogDirTopicInfo {
                    name: name.into(),
                    partitions,
                })
                .collect(),
        }
    }

    // Two brokers, two directories on broker 1, a directory-level error, a
    // future replica and an empty directory.
    fn two_brokers() -> Vec<BrokerAnswer> {
        vec![
            Ok(vec![
                dir(
                    "/data/a",
                    None,
                    vec![
                        (
                            "orders",
                            vec![partition(0, 152, 0, false), partition(1, 0, 0, false)],
                        ),
                        ("events", vec![partition(3, 7, 2, true)]),
                    ],
                ),
                dir("/data/b", Some(56), vec![]),
            ]),
            Ok(vec![dir("/var/lib/krabka", None, vec![])]),
        ]
    }

    #[test]
    fn the_document_matches_kafka_log_dirs_byte_for_byte() {
        let result = describe_result(&[1, 2], two_brokers(), "");
        check!(
            result.human
                == [
                    "Querying brokers for log directories information",
                    "Received log directory information from brokers 1,2",
                    concat!(
                        r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"events-3","size":7,"offsetLag":2,"isFuture":true},"#,
                        r#"{"partition":"orders-0","size":152,"offsetLag":0,"isFuture":false},{"partition":"orders-1","size":0,"offsetLag":0,"isFuture":false}],"#,
                        r#""error":null,"logDir":"/data/a"},{"partitions":[],"error":"org.apache.kafka.common.errors.KafkaStorageException","logDir":"/data/b"}]},"#,
                        r#"{"broker":2,"logDirs":[{"partitions":[],"error":null,"logDir":"/var/lib/krabka"}]}],"version":1}"#,
                    ),
                ]
        );
        check!(!result.failed);
        check!(result.notices.is_empty());
    }

    #[test]
    fn topic_list_narrows_the_replicas() {
        let result = describe_result(&[1, 2], two_brokers(), "orders,,");
        check!(
            result.human[2]
                == concat!(
                    r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"orders-0","size":152,"offsetLag":0,"isFuture":false},"#,
                    r#"{"partition":"orders-1","size":0,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/data/a"},"#,
                    r#"{"partitions":[],"error":"org.apache.kafka.common.errors.KafkaStorageException","logDir":"/data/b"}]},"#,
                    r#"{"broker":2,"logDirs":[{"partitions":[],"error":null,"logDir":"/var/lib/krabka"}]}],"version":1}"#,
                )
        );
    }

    #[test]
    fn a_failed_broker_leaves_a_partial_document_a_notice_and_a_failure() {
        struct Case {
            answers: Vec<BrokerAnswer>,
            received: &'static str,
            document: &'static str,
            notices: Vec<&'static str>,
        }
        let empty = r#"{"partitions":[],"error":null,"logDir":"/d"}"#;
        let ok = || Ok(vec![dir("/d", None, vec![])]);
        let refused = || Err(CommandError::Other("connection refused".into()));
        let cases = [
            Case {
                answers: vec![ok(), refused()],
                received: "1",
                document: r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[],"error":null,"logDir":"/d"}]}],"version":1}"#,
                notices: vec![
                    "ERROR: failed to describe the log directories of broker 2: connection refused",
                ],
            },
            Case {
                answers: vec![refused(), ok()],
                received: "2",
                document: r#"{"brokers":[{"broker":2,"logDirs":[{"partitions":[],"error":null,"logDir":"/d"}]}],"version":1}"#,
                notices: vec![
                    "ERROR: failed to describe the log directories of broker 1: connection refused",
                ],
            },
            Case {
                answers: vec![refused(), Ok(vec![])],
                received: "",
                document: r#"{"brokers":[],"version":1}"#,
                notices: vec![
                    "ERROR: failed to describe the log directories of broker 1: connection refused",
                    "ERROR: failed to describe the log directories of broker 2: org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.",
                ],
            },
            Case {
                answers: vec![ok(), ok()],
                received: "1,2",
                document: r#"{"brokers":[{"broker":1,"logDirs":[{"partitions":[],"error":null,"logDir":"/d"}]},{"broker":2,"logDirs":[{"partitions":[],"error":null,"logDir":"/d"}]}],"version":1}"#,
                notices: vec![],
            },
        ];
        for case in cases {
            let failed = !case.notices.is_empty();
            let result = describe_result(&[1, 2], case.answers, "");
            check!(
                result.human[1]
                    == format!(
                        "Received log directory information from brokers {}",
                        case.received
                    )
            );
            check!(result.human[2] == case.document);
            check!(result.notices == case.notices);
            check!(result.failed == failed);
            check!(
                result.data["failed_brokers"].as_array().map(Vec::len)
                    == Some(result.notices.len())
            );
        }
        check!(empty.len() > 0);
    }

    #[test]
    fn broker_selection_follows_kafka_log_dirs() {
        type Case = (&'static [i32], &'static str, Result<Vec<i32>, &'static str>);
        let cases: [Case; 5] = [
            (&[1, 2, 3], "", Ok(vec![1, 2, 3])),
            (&[3, 1, 2], "", Ok(vec![1, 2, 3])),
            (&[1, 2, 3], "3,1,,3", Ok(vec![1, 3])),
            (
                &[1],
                "1,5,3",
                Err(
                    "ERROR: The given brokers do not exist from --broker-list: 3,5. Current existent brokers: 1",
                ),
            ),
            (&[1], "x", Err("For input string: \"x\"")),
        ];
        for (cluster, list, expected) in cases {
            let actual = broker_list(list)
                .and_then(|requested| brokers_to_query(cluster, &requested))
                .map_err(|error| error.to_string());
            check!(actual == expected.map_err(ToOwned::to_owned));
        }
    }

    #[test]
    fn moves_parse_as_topic_partition_and_directory() {
        let cases = [
            (
                "orders:3=/data/b",
                Ok(ReplicaMove {
                    topic: "orders".into(),
                    partition: 3,
                    log_dir: "/data/b".into(),
                }),
            ),
            (
                "a:b:0=/x=y",
                Ok(ReplicaMove {
                    topic: "a:b".into(),
                    partition: 0,
                    log_dir: "/x=y".into(),
                }),
            ),
            (
                "orders:-1=/d",
                Err("invalid partition '-1' in orders:-1=/d".to_owned()),
            ),
            (
                "orders=/d",
                Err("expected TOPIC:PARTITION=LOG_DIR, got orders=/d".to_owned()),
            ),
            (
                "orders:0",
                Err("expected TOPIC:PARTITION=LOG_DIR, got orders:0".to_owned()),
            ),
        ];
        for (value, expected) in cases {
            check!(parse_move(value) == expected);
        }
    }

    #[test]
    fn describe_and_alter_flags_parse_as_the_jvm_tool_spells_them() {
        let parsed = Command::try_parse_from([
            "log-dirs",
            "--describe",
            "--broker-list",
            "0,1",
            "--topic-list",
            "orders",
            "--bootstrap-server",
            "h:9092",
        ])
        .unwrap()
        .args;
        check!(
            (
                parsed.describe,
                parsed.broker_list.as_str(),
                parsed.topic_list.as_str()
            ) == (true, "0,1", "orders")
        );
        let parsed = Command::try_parse_from([
            "log-dirs",
            "--alter",
            "--broker",
            "1",
            "--move",
            "orders:0=/b",
            "--dry-run",
        ])
        .unwrap()
        .args;
        check!(
            (
                parsed.alter,
                parsed.broker,
                parsed.replica_move.len(),
                parsed.confirm.dry_run
            ) == (true, Some(1), 1, true)
        );
        for argv in [
            &["log-dirs"][..],
            &["log-dirs", "--describe", "--alter"],
            &["log-dirs", "--alter", "--broker", "1"],
            &["log-dirs", "--describe", "--move", "t:0=/d"],
        ] {
            assert!(Command::try_parse_from(argv).is_err(), "{argv:?}");
        }
    }

    #[test]
    fn the_alter_report_names_each_move_and_its_error() {
        let moves = vec![
            parse_move("orders:0=/b").unwrap(),
            parse_move("orders:1=/nope").unwrap(),
        ];
        let current = vec![dir(
            "/a",
            None,
            vec![(
                "orders",
                vec![partition(0, 1, 0, false), partition(1, 1, 0, false)],
            )],
        )];
        let outcomes = vec![
            AlterReplicaLogDirOutcome {
                topic: "orders".into(),
                partition: 0,
                error: None,
            },
            AlterReplicaLogDirOutcome {
                topic: "orders".into(),
                partition: 1,
                error: Some(KafkaError {
                    code: 57,
                    name: "LOG_DIR_NOT_FOUND",
                    message: None,
                }),
            },
        ];
        let result = moved(1, &moves, &current, &outcomes);
        check!(
            result.human
                == [
                    "Moving replica orders-0 on broker 1 from /a to /b.",
                    "orders-1\tERROR\tLOG_DIR_NOT_FOUND (57)",
                ]
        );
        check!(result.failed);
    }
}
