//! `krabka reassign-partitions`, the counterpart of `kafka-reassign-partitions`.
//!
//! The actions are Kafka's: `--generate`, `--execute`, `--verify`,
//! `--cancel` and `--list`, with the option rules and messages of
//! `ReassignPartitionsCommand`. `--execute --topic <t> --replication-factor
//! <n>` and `--verify` with the same pair are a krabka extension that
//! converges one topic to a replication factor.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use clap::Args;
use krabka_client_admin::{
    AdminClient, AlterConfigOp, AlterPartitionReassignmentsOptions, ClusterNode, ConfigResource,
    IncrementalAlterConfigsOptions, KafkaError, PartitionAssignment, TopicPartitionReplica,
};
use krabka_units::{Time, convert::TimeExt as _};
use serde_json::{Value, json};

use crate::{
    cluster,
    common::java_exception,
    connection::ConnectionArgs,
    jvm::{hash_set_order, string_hash},
    output::{CommandError, CommandResult},
    replica_placer::{self, Lcg48},
    safety::{ConfirmArgs, Impact, confirm},
    topic_partition::{TopicPartition, join},
};

mod files;
mod plan;

use self::{
    files::{ReassignmentFile, Replica},
    plan::{
        FOLLOWER_RATE, FOLLOWER_REPLICAS, LEADER_RATE, LEADER_REPLICAS, LOG_DIR_RATE, MoveMap,
        MoveState,
    },
};

/// `ReassignPartitionsCommand.BROKER_LEVEL_THROTTLES`, in Kafka's order.
const BROKER_LEVEL_THROTTLES: [&str; 3] = [LEADER_RATE, FOLLOWER_RATE, LOG_DIR_RATE];
/// `ReassignPartitionsCommand.TOPIC_LEVEL_THROTTLES`, in Kafka's order.
const TOPIC_LEVEL_THROTTLES: [&str; 2] = [LEADER_REPLICAS, FOLLOWER_REPLICAS];
/// `REPLICA_NOT_AVAILABLE`: the replica has not reached the broker yet.
const REPLICA_NOT_AVAILABLE: i16 = 9;
/// How long `--execute` waits between attempts to start a log dir move.
const MOVE_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

const CANNOT_EXECUTE_BECAUSE_OF_EXISTING: &str = "Cannot execute because there is an existing \
     partition assignment.  Use --additional to override this and create a new partition \
     assignment in addition to the existing one. The --additional flag can also be used to \
     change the throttle by resubmitting the current reassignment.";

#[derive(Debug, Args)]
#[command(mut_arg("timeout", |arg| {
    arg.value_parser(timeout_ms)
        .default_value("10000")
        .help("The maximum time in ms to wait for log directory replica assignment to begin, \
               and the deadline of each request")
}))]
/// Each flag is an `Option<bool>` that clap sets to `Some(true)` when the
/// flag is present, as `krabka topics` does for its actions.
pub struct ReassignPartitionsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Generate a candidate partition reassignment configuration.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    generate: Option<bool>,
    /// Kick off the reassignment as specified by --reassignment-json-file.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    execute: Option<bool>,
    /// Verify if the reassignment completed as specified by
    /// --reassignment-json-file.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    verify: Option<bool>,
    /// Cancel an active reassignment.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    cancel: Option<bool>,
    /// List all active partition reassignments.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    list: Option<bool>,
    /// The JSON file with the partition reassignment configuration.
    #[arg(long)]
    reassignment_json_file: Option<PathBuf>,
    /// The JSON file with the topics to move, for --generate.
    #[arg(long)]
    topics_to_move_json_file: Option<PathBuf>,
    /// The brokers to reassign the partitions to, as "0,1,2", for
    /// --generate.
    #[arg(long)]
    broker_list: Option<String>,
    /// Disable rack aware replica assignment.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    disable_rack_aware: Option<bool>,
    /// The throttle of partition movement between brokers, in bytes/sec.
    #[arg(long, allow_negative_numbers = true)]
    throttle: Option<i64>,
    /// The throttle of replica movement between log directories, in
    /// bytes/sec.
    #[arg(long, allow_negative_numbers = true)]
    replica_alter_log_dirs_throttle: Option<i64>,
    /// Execute this reassignment in addition to any other ongoing ones.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    additional: Option<bool>,
    /// Do not modify broker or topic throttles.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    preserve_throttles: Option<bool>,
    /// Deny a change of a partition's replication factor.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    disallow_replication_factor_change: Option<bool>,
    /// The topic to converge to --replication-factor (krabka extension).
    #[arg(long)]
    topic: Option<String>,
    /// The replication factor to converge --topic to (krabka extension).
    #[arg(long, allow_negative_numbers = true)]
    replication_factor: Option<i32>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// `--timeout` in milliseconds, as `kafka-reassign-partitions` reads it, or
/// with a unit, as the other krabka commands read it.
fn timeout_ms(value: &str) -> Result<Time, String> {
    if let Ok(millis) = value.parse::<i64>() {
        return Ok(Time::from_millis(millis.max(0)));
    }
    let invalid = || format!("timeout must be milliseconds or have a unit, not `{value}`");
    if let Ok(time) = value.parse() {
        return Ok(time);
    }
    let unit = value.find(char::is_alphabetic).ok_or_else(invalid)?;
    format!("{} {}", &value[..unit], &value[unit..])
        .parse()
        .map_err(|_| invalid())
}

/// A validated command line.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Generate {
        topics_file: PathBuf,
        broker_list: String,
        rack_aware: bool,
    },
    Execute {
        file: PathBuf,
        additional: bool,
        throttle: i64,
        log_dir_throttle: i64,
        disallow_replication_factor_change: bool,
    },
    Verify {
        file: PathBuf,
        preserve_throttles: bool,
    },
    Cancel {
        file: PathBuf,
        preserve_throttles: bool,
    },
    List,
    ReplicationFactor {
        topic: String,
        replication_factor: i32,
        execute: bool,
    },
}

impl ReassignPartitionsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let action = self.validate()?;
        let timeout = self.connection.timeout;
        let mut client = self.connection.connect("reassign-partitions").await?;
        let client = &mut client;
        match action {
            Action::List => list(client, timeout).await,
            Action::Generate {
                topics_file,
                broker_list,
                rack_aware,
            } => {
                let text = read(&topics_file).await?;
                generate(client, &text, &broker_list, rack_aware).await
            }
            Action::Execute {
                file,
                additional,
                throttle,
                log_dir_throttle,
                disallow_replication_factor_change,
            } => {
                let file = files::parse_reassignment(&read(&file).await?)?;
                let options = ExecuteOptions {
                    additional,
                    throttle,
                    log_dir_throttle,
                    disallow_replication_factor_change,
                    timeout,
                    confirm: self.confirm,
                };
                execute(client, &file, options).await
            }
            Action::Verify {
                file,
                preserve_throttles,
            } => {
                let file = files::parse_reassignment(&read(&file).await?)?;
                verify(client, &file, preserve_throttles, timeout).await
            }
            Action::Cancel {
                file,
                preserve_throttles,
            } => {
                let file = files::parse_reassignment(&read(&file).await?)?;
                let options = CancelOptions {
                    preserve_throttles,
                    timeout,
                    confirm: self.confirm,
                };
                cancel(client, &file, options).await
            }
            Action::ReplicationFactor {
                topic,
                replication_factor,
                execute,
            } => {
                let target = (topic.as_str(), replication_factor);
                if execute {
                    converge(client, target, timeout, self.confirm).await
                } else {
                    convergence(client, target, timeout).await
                }
            }
        }
    }

    /// `ReassignPartitionsCommand.validateAndParseArgs`, with its messages,
    /// and the rules of the krabka extension.
    fn validate(&self) -> Result<Action, String> {
        let actions = [
            ("generate", self.generate.is_some()),
            ("execute", self.execute.is_some()),
            ("verify", self.verify.is_some()),
            ("cancel", self.cancel.is_some()),
            ("list", self.list.is_some()),
        ];
        let chosen = actions
            .iter()
            .filter(|(_, chosen)| *chosen)
            .map(|(name, _)| *name)
            .collect::<Vec<_>>();
        let [action] = chosen[..] else {
            return Err(
                "Command must include exactly one action: --generate, --execute, \
                        --verify, --cancel, --list"
                    .into(),
            );
        };
        let server = !self.connection.bootstrap_server.is_empty();
        let controller = !self.connection.bootstrap_controller.is_empty();
        if server && controller {
            return Err(
                "Please don't specify both --bootstrap-server and --bootstrap-controller".into(),
            );
        }
        if !server && !controller {
            return Err(
                "Please specify either --bootstrap-server or --bootstrap-controller".into(),
            );
        }
        if self.topic.is_some() || self.replication_factor.is_some() {
            return self.validate_replication_factor(action);
        }
        let required: &[(&str, bool)] = match action {
            "generate" => &[
                (
                    "topics-to-move-json-file",
                    self.topics_to_move_json_file.is_some(),
                ),
                ("broker-list", self.broker_list.is_some()),
            ],
            "list" => &[],
            _ => &[(
                "reassignment-json-file",
                self.reassignment_json_file.is_some(),
            )],
        };
        if let Some((name, _)) = required.iter().find(|(_, present)| !present) {
            return Err(format!("Missing required argument \"[{name}]\""));
        }
        let given = [
            ("bootstrap-server", server),
            ("bootstrap-controller", controller),
            (
                "reassignment-json-file",
                self.reassignment_json_file.is_some(),
            ),
            (
                "topics-to-move-json-file",
                self.topics_to_move_json_file.is_some(),
            ),
            ("broker-list", self.broker_list.is_some()),
            ("disable-rack-aware", self.disable_rack_aware.is_some()),
            ("throttle", self.throttle.is_some()),
            (
                "replica-alter-log-dirs-throttle",
                self.replica_alter_log_dirs_throttle.is_some(),
            ),
            ("additional", self.additional.is_some()),
            ("preserve-throttles", self.preserve_throttles.is_some()),
            (
                "disallow-replication-factor-change",
                self.disallow_replication_factor_change.is_some(),
            ),
        ];
        let permitted = permitted_options(action, server);
        if let Some((name, _)) = given
            .iter()
            .find(|(name, present)| *present && !permitted.contains(name))
        {
            return Err(format!(
                "Option \"[{name}]\" can't be used with action \"[{action}]\""
            ));
        }
        if (self.confirm.dry_run || self.confirm.yes) && !matches!(action, "execute" | "cancel") {
            return Err("--dry-run and --yes are only valid with --execute or --cancel".into());
        }
        let file = || self.reassignment_json_file.clone().unwrap_or_default();
        Ok(match action {
            "generate" => Action::Generate {
                topics_file: self.topics_to_move_json_file.clone().unwrap_or_default(),
                broker_list: self.broker_list.clone().unwrap_or_default(),
                rack_aware: self.disable_rack_aware.is_none(),
            },
            "execute" => Action::Execute {
                file: file(),
                additional: self.additional.is_some(),
                throttle: self.throttle.unwrap_or(-1),
                log_dir_throttle: self.replica_alter_log_dirs_throttle.unwrap_or(-1),
                disallow_replication_factor_change: self
                    .disallow_replication_factor_change
                    .is_some(),
            },
            "verify" => Action::Verify {
                file: file(),
                preserve_throttles: self.preserve_throttles.is_some(),
            },
            "cancel" => Action::Cancel {
                file: file(),
                preserve_throttles: self.preserve_throttles.is_some(),
            },
            _ => Action::List,
        })
    }

    fn validate_replication_factor(&self, action: &str) -> Result<Action, String> {
        let (Some(topic), Some(replication_factor)) = (&self.topic, self.replication_factor) else {
            return Err("--topic and --replication-factor must be given together".into());
        };
        let others = self.reassignment_json_file.is_some()
            || self.topics_to_move_json_file.is_some()
            || self.broker_list.is_some()
            || self.disable_rack_aware.is_some()
            || self.throttle.is_some()
            || self.replica_alter_log_dirs_throttle.is_some()
            || self.additional.is_some()
            || self.preserve_throttles.is_some()
            || self.disallow_replication_factor_change.is_some()
            || !self.connection.bootstrap_controller.is_empty();
        if !matches!(action, "execute" | "verify") || others {
            return Err(
                "--topic and --replication-factor take only --execute or --verify and \
                        the broker connection flags"
                    .into(),
            );
        }
        if action == "verify" && (self.confirm.dry_run || self.confirm.yes) {
            return Err("--dry-run and --yes are only valid with --execute or --cancel".into());
        }
        Ok(Action::ReplicationFactor {
            topic: topic.clone(),
            replication_factor,
            execute: action == "execute",
        })
    }
}

/// The options that `kafka-reassign-partitions` permits with `action`.
fn permitted_options(action: &str, bootstrap_server: bool) -> Vec<&'static str> {
    let bootstrap = if bootstrap_server {
        "bootstrap-server"
    } else {
        "bootstrap-controller"
    };
    match action {
        "verify" => vec![
            "bootstrap-server",
            "reassignment-json-file",
            "preserve-throttles",
        ],
        "generate" => vec![
            "bootstrap-server",
            "topics-to-move-json-file",
            "broker-list",
            "disable-rack-aware",
        ],
        "execute" => vec![
            "reassignment-json-file",
            "additional",
            "bootstrap-server",
            "throttle",
            "replica-alter-log-dirs-throttle",
            "disallow-replication-factor-change",
        ],
        "cancel" => vec![bootstrap, "reassignment-json-file", "preserve-throttles"],
        _ => vec![bootstrap],
    }
}

async fn read(path: &Path) -> Result<String, CommandError> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|error| CommandError::Other(format!("{}: {error}", path.display())))
}

/// Every active reassignment in `filter` (all of them when it is empty), by
/// partition.
async fn active_reassignments(
    client: &mut AdminClient,
    filter: &BTreeMap<String, Vec<i32>>,
    timeout: Time,
) -> Result<BTreeMap<TopicPartition, PartitionAssignment>, CommandError> {
    Ok(client
        .list_partition_reassignments(filter, timeout)
        .await?
        .into_iter()
        .map(|assignment| {
            (
                TopicPartition::new(assignment.topic.clone(), assignment.partition),
                assignment,
            )
        })
        .collect())
}

/// The current replicas of every partition of `topics`. A topic that does
/// not exist has no partitions.
async fn topic_replicas(
    client: &mut AdminClient,
    topics: &BTreeSet<String>,
) -> Result<BTreeMap<TopicPartition, Vec<i32>>, CommandError> {
    if topics.is_empty() {
        return Ok(BTreeMap::new());
    }
    let names = topics.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(client
        .describe_partition_assignments(&names)
        .await?
        .into_iter()
        .filter(|assignment| topics.contains(&assignment.topic))
        .map(|assignment| {
            (
                TopicPartition::new(assignment.topic, assignment.partition),
                assignment.replicas,
            )
        })
        .collect())
}

/// Kafka's refusal of a topic that `describeTopics` does not find.
fn missing_topic(
    topics: &BTreeSet<String>,
    found: &BTreeMap<TopicPartition, Vec<i32>>,
) -> Result<(), String> {
    match topics
        .iter()
        .find(|topic| !found.keys().any(|partition| &partition.topic == *topic))
    {
        Some(topic) => Err(format!(
            "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Topic {topic} not \
             found."
        )),
        None => Ok(()),
    }
}

/// `ReassignPartitionsCommand.getReplicasForPartitions`: the current replicas
/// of `partitions`, with Kafka's refusal of a missing topic or partition.
async fn partition_replicas(
    client: &mut AdminClient,
    partitions: &[TopicPartition],
) -> Result<BTreeMap<TopicPartition, Vec<i32>>, CommandError> {
    let topics = partitions
        .iter()
        .map(|partition| partition.topic.clone())
        .collect::<BTreeSet<_>>();
    let all = topic_replicas(client, &topics).await?;
    missing_topic(&topics, &all)?;
    missing_partitions(partitions, &all)?;
    Ok(all
        .into_iter()
        .filter(|(partition, _)| partitions.contains(partition))
        .collect())
}

fn missing_partitions(
    partitions: &[TopicPartition],
    found: &BTreeMap<TopicPartition, Vec<i32>>,
) -> Result<(), String> {
    let missing = partitions
        .iter()
        .filter(|partition| !found.contains_key(*partition))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let missing = hash_set_order(missing, TopicPartition::java_hash);
    Err(format!(
        "org.apache.kafka.common.errors.UnknownTopicOrPartitionException: Unable to find \
         partition: {}",
        join(&missing, ", ")
    ))
}

async fn list(client: &mut AdminClient, timeout: Time) -> Result<CommandResult, CommandError> {
    let active = active_reassignments(client, &BTreeMap::new(), timeout).await?;
    let data = active
        .values()
        .map(|assignment| {
            json!({
                "topic": assignment.topic,
                "partition": assignment.partition,
                "replicas": assignment.replicas,
                "adding_replicas": assignment.adding_replicas,
                "removing_replicas": assignment.removing_replicas,
            })
        })
        .collect::<Vec<_>>();
    Ok(CommandResult::success(plan::list_lines(&active), data))
}

async fn generate(
    client: &mut AdminClient,
    topics_json: &str,
    broker_list: &str,
    rack_aware: bool,
) -> Result<CommandResult, CommandError> {
    let (brokers, topics) = files::parse_generate(topics_json, broker_list)?;
    let topics = topics.into_iter().collect::<BTreeSet<_>>();
    let current = topic_replicas(client, &topics).await?;
    missing_topic(&topics, &current)?;
    let nodes = cluster::cluster_nodes(client, false).await?;
    let current_log_dirs = replica_log_dirs(client, &current, &nodes).await?;
    let usable = plan::usable_brokers(&nodes, &brokers, rack_aware)?;
    let by_key = current
        .iter()
        .map(|(partition, replicas)| {
            (
                (partition.topic.clone(), partition.partition),
                replicas.clone(),
            )
        })
        .collect();
    let proposed = replica_placer::propose(&mut Lcg48::from_time(), &by_key, &usable)?
        .into_iter()
        .map(|((topic, partition), replicas)| (TopicPartition::new(topic, partition), replicas))
        .collect::<BTreeMap<_, _>>();
    let current_json = files::format_reassignment(&current, &current_log_dirs);
    let proposed_json = files::format_reassignment(&proposed, &BTreeMap::new());
    let human = vec![
        "Current partition replica assignment".to_owned(),
        current_json.clone(),
        String::new(),
        "Proposed partition reassignment configuration".to_owned(),
        proposed_json.clone(),
    ];
    let parse = |text: &str| serde_json::from_str::<Value>(text).unwrap_or(Value::Null);
    Ok(CommandResult::success(
        human,
        json!({"current": parse(&current_json), "proposed": parse(&proposed_json)}),
    ))
}

fn client_replica(replica: &Replica) -> TopicPartitionReplica {
    TopicPartitionReplica {
        topic: replica.topic.clone(),
        partition: replica.partition,
        broker_id: replica.broker,
    }
}

fn replica_of(replica: TopicPartitionReplica) -> Replica {
    Replica {
        broker: replica.broker_id,
        topic: replica.topic,
        partition: replica.partition,
    }
}

/// The failure of a Kafka admin future, as `kafka-reassign-partitions`
/// reports it.
fn future_error(error: &KafkaError) -> CommandError {
    CommandError::Other(java_exception(error))
}

/// `ReassignPartitionsCommand.getReplicaToLogDir`: the current log dir of
/// each replica of `current` on a live node, from `DescribeLogDirs`.
async fn replica_log_dirs(
    client: &mut AdminClient,
    current: &BTreeMap<TopicPartition, Vec<i32>>,
    nodes: &[ClusterNode],
) -> Result<BTreeMap<Replica, String>, CommandError> {
    let replicas = current
        .iter()
        .flat_map(|(partition, brokers)| {
            brokers
                .iter()
                .filter(|broker| nodes.iter().any(|node| node.id == **broker))
                .map(|broker| TopicPartitionReplica {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                    broker_id: *broker,
                })
        })
        .collect::<Vec<_>>();
    if replicas.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut log_dirs = BTreeMap::new();
    for (replica, result) in client.describe_replica_log_dirs(&replicas).await {
        if let Some(current) = result.map_err(|error| future_error(&error))?.current {
            log_dirs.insert(replica_of(replica), current.path);
        }
    }
    Ok(log_dirs)
}

/// `ReassignPartitionsCommand.findLogDirMoveStates`.
async fn move_states(
    client: &mut AdminClient,
    targets: &BTreeMap<Replica, String>,
) -> Result<BTreeMap<Replica, MoveState>, CommandError> {
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let replicas = targets.keys().map(client_replica).collect::<Vec<_>>();
    let infos = client.describe_replica_log_dirs(&replicas).await;
    let mut states = BTreeMap::new();
    for (replica, info) in infos {
        let info = info.map_err(|error| future_error(&error))?;
        let replica = replica_of(replica);
        let state = MoveState::new(&info, &targets[&replica]);
        states.insert(replica, state);
    }
    Ok(states)
}

/// Writes `configs` with one `IncrementalAlterConfigs` call, and fails with
/// the first resource's error, as Kafka's `all().get()` does.
async fn alter_configs(
    client: &mut AdminClient,
    configs: &BTreeMap<ConfigResource, Vec<AlterConfigOp>>,
) -> Result<(), CommandError> {
    if configs.is_empty() {
        return Ok(());
    }
    let results = client
        .incremental_alter_configs(configs, IncrementalAlterConfigsOptions::default())
        .await?;
    match results.into_values().find_map(Result::err) {
        Some(error) => Err(future_error(&error)),
        None => Ok(()),
    }
}

/// The `SET` operations of `configs`, in the order of `order`.
fn set_ops(configs: &BTreeMap<&'static str, String>, order: &[&str]) -> Vec<AlterConfigOp> {
    order
        .iter()
        .filter_map(|name| {
            configs
                .get(name)
                .map(|value| AlterConfigOp::set(*name, value.clone()))
        })
        .collect()
}

/// `modifyTopicThrottles`, `modifyInterBrokerThrottle` and
/// `modifyLogDirThrottle`: each group of throttle configs in its own call,
/// each followed by its line.
async fn write_throttles(
    client: &mut AdminClient,
    throttles: &plan::Throttles,
    human: &mut Vec<String>,
) -> Result<(), CommandError> {
    if let Some(line) = throttles.inter_broker_line() {
        let topics = throttles
            .topics
            .iter()
            .map(|(topic, configs)| {
                (
                    ConfigResource::topic(topic.clone()),
                    set_ops(configs, &TOPIC_LEVEL_THROTTLES),
                )
            })
            .filter(|(_, ops)| !ops.is_empty())
            .collect();
        alter_configs(client, &topics).await?;
        let brokers = throttles
            .broker_configs(&[LEADER_RATE, FOLLOWER_RATE])
            .iter()
            .map(|(broker, configs)| {
                (
                    ConfigResource::broker(*broker),
                    set_ops(configs, &[LEADER_RATE, FOLLOWER_RATE]),
                )
            })
            .collect();
        alter_configs(client, &brokers).await?;
        human.push(line);
    }
    if let Some(line) = throttles.log_dir_line() {
        let brokers = throttles
            .broker_configs(&[LOG_DIR_RATE])
            .iter()
            .map(|(broker, configs)| {
                (
                    ConfigResource::broker(*broker),
                    set_ops(configs, &[LOG_DIR_RATE]),
                )
            })
            .collect();
        alter_configs(client, &brokers).await?;
        human.push(line);
    }
    Ok(())
}

/// `ReassignPartitionsCommand.clearAllThrottles`: the throttle configs of
/// every broker of the cluster and of the targets, then of every target
/// topic, are deleted. Returns the lines that Kafka prints.
async fn clear_all_throttles(
    client: &mut AdminClient,
    targets: &[(TopicPartition, Vec<i32>)],
    dry_run: bool,
) -> Result<Vec<String>, CommandError> {
    let brokers = cluster::cluster_nodes(client, false)
        .await?
        .iter()
        .map(|node| node.id)
        .chain(
            targets
                .iter()
                .flat_map(|(_, replicas)| replicas.iter().copied()),
        )
        .collect::<Vec<_>>();
    let brokers = hash_set_order(brokers, |id| *id);
    let topics = hash_set_order(
        targets
            .iter()
            .map(|(partition, _)| partition.topic.clone())
            .collect(),
        |topic| string_hash(topic),
    );
    let broker_names = brokers.iter().map(ToString::to_string).collect::<Vec<_>>();
    let mut lines = vec![plan::clearing_line("broker", "broker", &broker_names)];
    if !dry_run {
        let configs = brokers
            .iter()
            .map(|broker| {
                (
                    ConfigResource::broker(*broker),
                    BROKER_LEVEL_THROTTLES
                        .iter()
                        .map(|name| AlterConfigOp::delete(*name))
                        .collect(),
                )
            })
            .collect();
        alter_configs(client, &configs).await?;
    }
    lines.push(plan::clearing_line("topic", "topic", &topics));
    if !dry_run {
        let configs = topics
            .iter()
            .map(|topic| {
                (
                    ConfigResource::topic(topic.clone()),
                    TOPIC_LEVEL_THROTTLES
                        .iter()
                        .map(|name| AlterConfigOp::delete(*name))
                        .collect(),
                )
            })
            .collect();
        alter_configs(client, &configs).await?;
    }
    Ok(lines)
}

/// `ReassignPartitionsCommand.executeMoves`: starts each log dir move with
/// `AlterReplicaLogDirs`, and tries again every 100 ms, until `timeout`,
/// the replicas that have not reached their broker yet. Returns the lines
/// that Kafka prints, and whether it failed.
async fn execute_moves(
    client: &mut AdminClient,
    moves: &BTreeMap<Replica, String>,
    timeout: Time,
) -> (Vec<String>, bool) {
    let start = tokio::time::Instant::now();
    let mut pending = moves.clone();
    let mut lines = Vec::new();
    loop {
        let request = pending
            .iter()
            .map(|(replica, path)| (client_replica(replica), path.clone()))
            .collect();
        let mut started = BTreeMap::new();
        for (replica, result) in client.alter_replica_log_dirs(&request).await {
            let replica = replica_of(replica);
            match result {
                Ok(()) => {
                    let path = pending[&replica].clone();
                    started.insert(replica, path);
                }
                Err(error) if error.code == REPLICA_NOT_AVAILABLE => {}
                Err(_) => {
                    lines.push(format!("Error: Failed to alter dir for {replica}"));
                    return (lines, true);
                }
            }
        }
        lines.extend(plan::move_lines(&started));
        for replica in started.keys() {
            pending.remove(replica);
        }
        if pending.is_empty() {
            return (lines, false);
        }
        if start.elapsed() >= timeout.to_std() {
            lines.push(format!(
                "Timed out before log directory move{} could be started for: {}",
                if pending.len() == 1 { "" } else { "s" },
                pending
                    .keys()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            return (lines, true);
        }
        tokio::time::sleep(MOVE_RETRY_BACKOFF).await;
    }
}

/// Kafka's `verifyBrokerIds`: the first broker of `brokers`, in `HashSet`
/// order, that the cluster does not have.
fn unknown_broker(
    brokers: &BTreeMap<TopicPartition, Vec<i32>>,
    nodes: &[ClusterNode],
) -> Result<(), String> {
    let ids = hash_set_order(brokers.values().flatten().copied().collect(), |id| *id);
    match ids
        .into_iter()
        .find(|id| !nodes.iter().any(|node| node.id == *id))
    {
        Some(id) => Err(format!("Unknown broker id {id}")),
        None => Ok(()),
    }
}

#[derive(Debug, Clone, Copy)]
struct ExecuteOptions {
    additional: bool,
    throttle: i64,
    log_dir_throttle: i64,
    disallow_replication_factor_change: bool,
    timeout: Time,
    confirm: ConfirmArgs,
}

async fn execute(
    client: &mut AdminClient,
    file: &ReassignmentFile,
    options: ExecuteOptions,
) -> Result<CommandResult, CommandError> {
    files::check_execute(file)?;
    let proposed = file.targets().into_iter().collect::<BTreeMap<_, _>>();
    let moves = file.log_dir_moves();
    let dry_run = options.confirm.dry_run;
    let active = active_reassignments(client, &BTreeMap::new(), options.timeout).await?;
    if !options.additional && !active.is_empty() {
        return Err(CANNOT_EXECUTE_BECAUSE_OF_EXISTING.into());
    }
    let nodes = cluster::cluster_nodes(client, false).await?;
    unknown_broker(&proposed, &nodes)?;
    let partitions = proposed.keys().cloned().collect::<Vec<_>>();
    let current = partition_replicas(client, &partitions).await?;
    let current_log_dirs = replica_log_dirs(client, &current, &nodes).await?;
    let rollback = files::format_reassignment(&current, &current_log_dirs);
    let mut human = plan::rollback_lines(&rollback);
    let move_map = MoveMap::proposed(&active, &proposed, &current)?;
    let throttles = plan::Throttles::new(
        &move_map,
        &moves,
        options.throttle,
        options.log_dir_throttle,
    );
    let started = plan::started_line(&partitions);
    let data = json!({
        "rollback": serde_json::from_str::<Value>(&rollback).unwrap_or(Value::Null),
        "partitions": proposed
            .iter()
            .map(|(p, replicas)| json!({"topic": p.topic, "partition": p.partition, "replicas": replicas}))
            .collect::<Vec<_>>(),
        "throttles": throttles.json(),
    });
    if dry_run {
        human.extend(throttles.lines());
        human.push(started);
        human.extend(plan::move_lines(&moves));
        return Ok(CommandResult::success(human, data).into_dry_run());
    }
    confirm(
        options.confirm.yes,
        "krabka reassign-partitions",
        Impact {
            summary: format!("reassign {} partition(s)", partitions.len()),
            resources: proposed
                .iter()
                .map(|(partition, replicas)| {
                    format!("{partition}: replicas {}", plan::ids(replicas))
                })
                .collect(),
        },
    )
    .await?;
    human.extend(throttles.warning_line());
    write_throttles(client, &throttles, &mut human).await?;
    let request = proposed
        .iter()
        .map(|(partition, replicas)| {
            (
                (partition.topic.clone(), partition.partition),
                Some(replicas.clone()),
            )
        })
        .collect();
    let errors = alter_reassignments(
        client,
        &request,
        options.timeout,
        AlterPartitionReassignmentsOptions {
            allow_replication_factor_change: !options.disallow_replication_factor_change,
        },
    )
    .await?;
    if !errors.is_empty() {
        human.push("Error reassigning partition(s):".into());
        human.extend(plan::partition_errors(&errors));
        return Ok(CommandResult::rows(human, data, true));
    }
    human.push(started);
    let mut failed = false;
    if !moves.is_empty() {
        let (lines, moves_failed) = execute_moves(client, &moves, options.timeout).await;
        human.extend(lines);
        failed = moves_failed;
    }
    Ok(CommandResult::rows(human, data, failed))
}

/// Sends `AlterPartitionReassignments` and returns the error of each
/// partition that failed. A controller without the version that the options
/// need fails every partition, as Kafka's request builder does.
async fn alter_reassignments(
    client: &mut AdminClient,
    request: &BTreeMap<(String, i32), Option<Vec<i32>>>,
    timeout: Time,
    options: AlterPartitionReassignmentsOptions,
) -> Result<BTreeMap<TopicPartition, KafkaError>, CommandError> {
    let outcomes = match client
        .alter_partition_assignments_with(request, timeout, options)
        .await
    {
        Ok(outcomes) => outcomes,
        Err(krabka_client_admin::AdminError::Transport(
            krabka_client_core::ClientError::IncompatibleVersion { .. },
        )) if !options.allow_replication_factor_change => {
            let error = KafkaError {
                code: UNSUPPORTED_VERSION,
                name: "UNSUPPORTED_VERSION",
                message: Some(ALLOW_REPLICATION_FACTOR_CHANGE_UNSUPPORTED.to_owned()),
            };
            return Ok(request
                .keys()
                .map(|(topic, partition)| {
                    (
                        TopicPartition::new(topic.clone(), *partition),
                        error.clone(),
                    )
                })
                .collect());
        }
        Err(error) => return Err(error.into()),
    };
    Ok(outcomes
        .into_iter()
        .filter_map(|outcome| {
            outcome
                .error
                .map(|error| (TopicPartition::new(outcome.topic, outcome.partition), error))
        })
        .collect())
}

/// `UNSUPPORTED_VERSION`.
const UNSUPPORTED_VERSION: i16 = 35;
/// The message of the exception that Kafka's
/// `AlterPartitionReassignmentsRequest.Builder` throws for a controller
/// without v1.
const ALLOW_REPLICATION_FACTOR_CHANGE_UNSUPPORTED: &str = "The broker does not support the \
     AllowReplicationFactorChange option for the AlterPartitionReassignments API. Consider \
     re-sending the request without the option or updating the server version";

async fn verify(
    client: &mut AdminClient,
    file: &ReassignmentFile,
    preserve_throttles: bool,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    let active = active_reassignments(client, &BTreeMap::new(), timeout).await?;
    let targets = file.targets();
    let topics = targets
        .iter()
        .filter(|(partition, _)| !active.contains_key(partition))
        .map(|(partition, _)| partition.topic.clone())
        .collect::<BTreeSet<_>>();
    let described = topic_replicas(client, &topics).await?;
    let states = plan::partition_states(&targets, &active, &described);
    let mut human = plan::status_lines(&states);
    let move_states = move_states(client, &file.log_dir_moves()).await?;
    let move_lines = plan::move_state_lines(&move_states);
    // Kafka prints the replica-move report as one string, so no moves print
    // one empty line.
    if move_lines.is_empty() {
        human.push(String::new());
    } else {
        human.extend(move_lines);
    }
    let parts_ongoing = !active.is_empty();
    let moves_ongoing = move_states.values().any(|state| !state.done());
    let clear = !parts_ongoing && !moves_ongoing && !preserve_throttles;
    if clear {
        human.extend(clear_all_throttles(client, &targets, false).await?);
    }
    let incomplete = moves_ongoing
        || states
            .values()
            .any(|state| !state.done || state.current != state.target);
    let data = json!({
        "partitions": plan::states_json(&states),
        "moves": move_states
            .iter()
            .map(|(replica, state)| json!({
                "topic": replica.topic,
                "partition": replica.partition,
                "broker": replica.broker,
                "done": state.done(),
            }))
            .collect::<Vec<_>>(),
        "throttles_cleared": clear,
    });
    Ok(CommandResult::rows(human, data, incomplete))
}

#[derive(Debug, Clone, Copy)]
struct CancelOptions {
    preserve_throttles: bool,
    timeout: Time,
    confirm: ConfirmArgs,
}

async fn cancel(
    client: &mut AdminClient,
    file: &ReassignmentFile,
    options: CancelOptions,
) -> Result<CommandResult, CommandError> {
    let targets = file.targets();
    let mut filter = BTreeMap::<String, Vec<i32>>::new();
    for (partition, _) in &targets {
        let indexes = filter.entry(partition.topic.clone()).or_default();
        if !indexes.contains(&partition.partition) {
            indexes.push(partition.partition);
        }
    }
    let reassigning = active_reassignments(client, &filter, options.timeout)
        .await?
        .into_values()
        .filter(|assignment| {
            !assignment.adding_replicas.is_empty() || !assignment.removing_replicas.is_empty()
        })
        .map(|assignment| TopicPartition::new(assignment.topic, assignment.partition))
        .collect::<BTreeSet<_>>();
    let moving = move_states(client, &file.log_dir_moves())
        .await?
        .into_iter()
        .filter_map(|(replica, state)| match state {
            MoveState::Active { current, .. } => Some((replica, current)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let dry_run = options.confirm.dry_run;
    let changes = !reassigning.is_empty() || !moving.is_empty() || !options.preserve_throttles;
    if changes && !dry_run {
        confirm(
            options.confirm.yes,
            "krabka reassign-partitions",
            Impact {
                summary: format!(
                    "cancel {} partition reassignment(s) and {} log dir move(s)",
                    reassigning.len(),
                    moving.len()
                ),
                resources: reassigning
                    .iter()
                    .map(ToString::to_string)
                    .chain(moving.keys().map(ToString::to_string))
                    .collect(),
            },
        )
        .await?;
    }
    let mut human = Vec::new();
    let data = |cleared: bool| {
        json!({
            "cancelled": reassigning
                .iter()
                .map(|p| json!({"topic": p.topic, "partition": p.partition}))
                .collect::<Vec<_>>(),
            "moves": moving
                .keys()
                .map(|r| json!({"topic": r.topic, "partition": r.partition, "broker": r.broker}))
                .collect::<Vec<_>>(),
            "throttles_cleared": cleared,
        })
    };
    if reassigning.is_empty() {
        human.push("None of the specified partition reassignments are active.".to_owned());
    } else if dry_run {
        human.push(plan::cancelled_line(&reassigning));
    } else {
        // `None` sends a null `replicas`, which cancels the reassignment.
        let request = reassigning
            .iter()
            .map(|partition| ((partition.topic.clone(), partition.partition), None))
            .collect();
        let errors = alter_reassignments(
            client,
            &request,
            options.timeout,
            AlterPartitionReassignmentsOptions::default(),
        )
        .await?;
        if !errors.is_empty() {
            human.push(format!(
                "Error cancelling partition reassignment{} for:",
                if errors.len() == 1 { "" } else { "s" }
            ));
            human.extend(plan::partition_errors(&errors));
            return Ok(CommandResult::rows(human, data(false), true));
        }
        human.push(plan::cancelled_line(&reassigning));
    }
    // Kafka prints "None of the specified partition moves are active."
    // without a line end, so what follows shares the line.
    let mut no_moves = None;
    if moving.is_empty() {
        no_moves = Some("None of the specified partition moves are active.".to_owned());
    } else if dry_run {
        human.extend(plan::move_lines(&moving));
    } else {
        let (lines, failed) = execute_moves(client, &moving, options.timeout).await;
        human.extend(lines);
        if failed {
            return Ok(CommandResult::rows(human, data(false), true));
        }
    }
    let mut clearing = if options.preserve_throttles {
        Vec::new()
    } else {
        clear_all_throttles(client, &targets, dry_run).await?
    };
    if let Some(mut line) = no_moves {
        if !clearing.is_empty() {
            line.push_str(&clearing.remove(0));
        }
        human.push(line);
    }
    human.extend(clearing);
    let result = CommandResult::success(human, data(!options.preserve_throttles && !dry_run));
    Ok(if dry_run {
        result.into_dry_run()
    } else {
        result
    })
}

/// The krabka extension's `--execute`: converge the topic to the
/// replication factor.
async fn converge(
    client: &mut AdminClient,
    (topic, replication_factor): (&str, i32),
    timeout: Time,
    confirm_args: ConfirmArgs,
) -> Result<CommandResult, CommandError> {
    if confirm_args.dry_run {
        let report = convergence(client, (topic, replication_factor), timeout).await?;
        return Ok(report.into_dry_run());
    }
    confirm(
        confirm_args.yes,
        "krabka reassign-partitions",
        Impact {
            summary: format!("converge topic {topic} to replication factor {replication_factor}"),
            resources: vec![topic.to_owned()],
        },
    )
    .await?;
    let status = client
        .reconcile_topic_replication_factor(topic, replication_factor, timeout)
        .await?;
    Ok(convergence_report(topic, &format!("{status:?}"), false))
}

/// The krabka extension's `--verify`: whether the topic has the replication
/// factor. A reassignment in progress or a mismatch exits 1.
async fn convergence(
    client: &mut AdminClient,
    (topic, replication_factor): (&str, i32),
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    let assignments = client.describe_partition_assignments(&[topic]).await?;
    let partitions = assignments
        .iter()
        .map(|assignment| assignment.partition)
        .collect::<Vec<_>>();
    let active = client
        .list_partition_reassignments(&BTreeMap::from([(topic.to_owned(), partitions)]), timeout)
        .await?;
    let status = if !active.is_empty() {
        "ReassignmentInProgress"
    } else if !assignments.is_empty()
        && assignments
            .iter()
            .all(|assignment| i32::try_from(assignment.replicas.len()) == Ok(replication_factor))
    {
        "InSync"
    } else {
        "ReplicationFactorMismatch"
    };
    Ok(convergence_report(topic, status, status != "InSync"))
}

fn convergence_report(topic: &str, status: &str, incomplete: bool) -> CommandResult {
    CommandResult::rows(
        vec![format!("{topic}: {status}")],
        json!({"topic": topic, "status": status}),
        incomplete,
    )
}

#[cfg(test)]
mod tests;
