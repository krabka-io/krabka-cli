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
use krabka_client_admin::{AdminClient, PartitionAssignment};
use krabka_units::{Time, convert::TimeExt as _};
use serde_json::{Value, json};

use crate::{
    cluster,
    common::unsupported,
    connection::ConnectionArgs,
    jvm::hash_set_order,
    output::{CommandError, CommandResult},
    replica_placer::{self, Lcg48},
    safety::{ConfirmArgs, Impact, confirm},
    topic_partition::{TopicPartition, join},
};

mod files;
mod plan;

use self::{
    files::{ReassignmentFile, Replica},
    plan::MoveMap,
};

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
        refuse_unsupported(&action, self.confirm.dry_run)?;
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
                ..
            } => {
                let file = files::parse_reassignment(&read(&file).await?)?;
                let options = ExecuteOptions {
                    additional,
                    throttle,
                    log_dir_throttle,
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
                cancel(client, &file, preserve_throttles, timeout, self.confirm).await
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

/// Refuses, before any request, a sub-feature that needs an `AdminClient`
/// call that the pinned `krabka-client-admin` does not provide, so a
/// command never stops half-way through its changes.
fn refuse_unsupported(action: &Action, dry_run: bool) -> Result<(), CommandError> {
    match action {
        Action::Execute {
            disallow_replication_factor_change: true,
            ..
        } => Err(unsupported(
            "--disallow-replication-factor-change",
            "AdminClient::alter_partition_assignments with allow_replication_factor_change",
        )),
        Action::Execute { throttle, .. } if *throttle >= 0 && !dry_run => Err(unsupported(
            "--throttle",
            "AdminClient::incremental_alter_configs for BROKER resources",
        )),
        Action::Execute {
            log_dir_throttle, ..
        } if *log_dir_throttle >= 0 && !dry_run => Err(unsupported(
            "--replica-alter-log-dirs-throttle",
            "AdminClient::incremental_alter_configs for BROKER resources",
        )),
        _ => Ok(()),
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
    let current_log_dirs = replica_log_dirs(client, &current, "reassign-partitions --generate")?;
    let nodes = cluster::cluster_nodes(client, false, "reassign-partitions --generate")?;
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

/// The current log dir of each replica, as Kafka's `getReplicaToLogDir`.
///
/// # Errors
/// Always, in this build: the pinned `krabka-client-admin` has no
/// `describe_replica_log_dirs`.
fn replica_log_dirs(
    _client: &mut AdminClient,
    _current: &BTreeMap<TopicPartition, Vec<i32>>,
    feature: &str,
) -> Result<BTreeMap<Replica, String>, CommandError> {
    Err(unsupported(
        feature,
        "AdminClient::describe_replica_log_dirs",
    ))
}

#[derive(Debug, Clone, Copy)]
struct ExecuteOptions {
    additional: bool,
    throttle: i64,
    log_dir_throttle: i64,
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
    if !moves.is_empty() && !dry_run {
        return Err(unsupported(
            "moving replicas between log directories",
            "AdminClient::alter_replica_log_dirs by TopicPartitionReplica",
        ));
    }
    let active = active_reassignments(client, &BTreeMap::new(), options.timeout).await?;
    if !options.additional && !active.is_empty() {
        return Err(CANNOT_EXECUTE_BECAUSE_OF_EXISTING.into());
    }
    // Kafka also checks each broker ID against `DescribeCluster` here; the
    // pinned client cannot, so the controller refuses an unknown broker per
    // partition instead.
    let partitions = proposed.keys().cloned().collect::<Vec<_>>();
    let current = partition_replicas(client, &partitions).await?;
    // Kafka prints each replica's current log dir; the pinned client cannot
    // read them, and `any` keeps the rollback file valid.
    let rollback = files::format_reassignment(&current, &BTreeMap::new());
    let mut human = plan::rollback_lines(&rollback);
    let move_map = MoveMap::proposed(&active, &proposed, &current)?;
    let throttles = plan::Throttles::new(
        &move_map,
        &moves,
        options.throttle,
        options.log_dir_throttle,
    );
    human.extend(throttles.lines());
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
    let request = proposed
        .iter()
        .map(|(partition, replicas)| {
            (
                (partition.topic.clone(), partition.partition),
                Some(replicas.clone()),
            )
        })
        .collect();
    let outcomes = client
        .alter_partition_assignments(&request, options.timeout)
        .await?;
    let errors = plan::partition_errors(&outcomes);
    let failed = !errors.is_empty();
    if failed {
        human.push("Error reassigning partition(s):".into());
        human.extend(errors);
    } else {
        human.push(started);
    }
    Ok(CommandResult::rows(human, data, failed))
}

async fn verify(
    client: &mut AdminClient,
    file: &ReassignmentFile,
    preserve_throttles: bool,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    if !file.log_dir_moves().is_empty() {
        return Err(unsupported(
            "verifying replica moves between log directories",
            "AdminClient::describe_replica_log_dirs",
        ));
    }
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
    // `kafka-reassign-partitions` prints the replica-move report next; with
    // no moves it is one empty line.
    human.push(String::new());
    let incomplete = states
        .values()
        .any(|state| !state.done || state.current != state.target);
    // Kafka clears every throttle once no reassignment is active.
    let throttle_error = (active.is_empty() && !preserve_throttles).then(throttles_unsupported);
    if let Some(error) = &throttle_error {
        human.push(format!("Error: {error}"));
    }
    let data = json!({
        "partitions": plan::states_json(&states),
        "throttles_cleared": false,
        "error": throttle_error,
    });
    Ok(CommandResult::rows(
        human,
        data,
        incomplete || throttle_error.is_some(),
    ))
}

fn throttles_unsupported() -> String {
    format!(
        "{}. Pass --preserve-throttles to keep them.",
        unsupported(
            "clearing the reassignment throttles",
            "AdminClient::describe_cluster and AdminClient::incremental_alter_configs for BROKER \
             resources",
        )
    )
}

async fn cancel(
    client: &mut AdminClient,
    file: &ReassignmentFile,
    preserve_throttles: bool,
    timeout: Time,
    confirm_args: ConfirmArgs,
) -> Result<CommandResult, CommandError> {
    if !file.log_dir_moves().is_empty() {
        return Err(unsupported(
            "cancelling replica moves between log directories",
            "AdminClient::describe_replica_log_dirs",
        ));
    }
    let mut filter = BTreeMap::<String, Vec<i32>>::new();
    for (partition, _) in file.targets() {
        let indexes = filter.entry(partition.topic).or_default();
        if !indexes.contains(&partition.partition) {
            indexes.push(partition.partition);
        }
    }
    let reassigning = active_reassignments(client, &filter, timeout)
        .await?
        .into_values()
        .filter(|assignment| {
            !assignment.adding_replicas.is_empty() || !assignment.removing_replicas.is_empty()
        })
        .map(|assignment| TopicPartition::new(assignment.topic, assignment.partition))
        .collect::<BTreeSet<_>>();
    let mut human = Vec::new();
    let mut failed = false;
    if reassigning.is_empty() {
        human.push("None of the specified partition reassignments are active.".to_owned());
    } else if confirm_args.dry_run {
        human.push(plan::cancelled_line(&reassigning));
    } else {
        confirm(
            confirm_args.yes,
            "krabka reassign-partitions",
            Impact {
                summary: format!("cancel {} partition reassignment(s)", reassigning.len()),
                resources: reassigning.iter().map(ToString::to_string).collect(),
            },
        )
        .await?;
        // `None` sends a null `replicas`, which cancels the reassignment.
        let request = reassigning
            .iter()
            .map(|partition| ((partition.topic.clone(), partition.partition), None))
            .collect();
        let outcomes = client
            .alter_partition_assignments(&request, timeout)
            .await?;
        let errors = plan::partition_errors(&outcomes);
        if errors.is_empty() {
            human.push(plan::cancelled_line(&reassigning));
        } else {
            failed = true;
            human.push(format!(
                "Error cancelling partition reassignment{} for:",
                if errors.len() == 1 { "" } else { "s" }
            ));
            human.extend(errors);
        }
    }
    let throttle_error = (!failed && !preserve_throttles).then(throttles_unsupported);
    if !failed {
        // Kafka prints this line without a line end, so what follows shares
        // the line.
        let mut no_moves = "None of the specified partition moves are active.".to_owned();
        if let Some(error) = &throttle_error {
            no_moves.push_str("Error: ");
            no_moves.push_str(error);
        }
        human.push(no_moves);
    }
    let data = json!({
        "cancelled": reassigning
            .iter()
            .map(|p| json!({"topic": p.topic, "partition": p.partition}))
            .collect::<Vec<_>>(),
        "throttles_cleared": false,
        "error": throttle_error,
    });
    let result = CommandResult::rows(human, data, failed || throttle_error.is_some());
    Ok(if confirm_args.dry_run {
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
