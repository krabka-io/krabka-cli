//! `krabka leader-election`, the counterpart of `kafka-leader-election`.
//!
//! The option checks run in the command, not in clap, so each refusal has the
//! text and the exit code (1) of `kafka-leader-election`.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use clap::Args;
use krabka_client_admin::{AdminClient, AdminError, KafkaError};
use serde_json::json;

use crate::{
    common::java_exception,
    connection::ConnectionArgs,
    jvm::hash_set_order,
    kafka_json,
    output::{CommandError, CommandResult, kafka_error},
    topic_partition::{TopicPartition, join},
};

/// `ELECTION_NOT_NEEDED`: the partition already has the leader that the
/// election would choose.
const ELECTION_NOT_NEEDED: i16 = 84;
/// `REQUEST_TIMED_OUT`, which Kafka's admin client raises as a
/// `TimeoutException`.
const REQUEST_TIMED_OUT: i16 = 7;
/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

#[derive(Debug, Args)]
pub struct LeaderElectionArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// (DEPRECATED) Configuration properties files to pass to the admin
    /// client. Use --command-config instead.
    #[arg(long = "admin.config")]
    admin_config: Option<PathBuf>,
    /// The JSON file with the list of partitions for which leader elections
    /// should be performed, as `{"partitions": [{"topic": "foo",
    /// "partition": 1}]}`.
    #[arg(long)]
    path_to_json_file: Option<PathBuf>,
    /// Name of topic for which to perform an election.
    #[arg(long)]
    topic: Option<String>,
    /// Partition id for which to perform an election. Required with --topic.
    #[arg(long, allow_negative_numbers = true)]
    partition: Option<i32>,
    /// Perform election on all of the eligible topic partitions.
    #[arg(long)]
    all_topic_partitions: bool,
    /// Type of election to attempt: `preferred` or `unclean`.
    #[arg(long)]
    election_type: Option<String>,
}

/// Kafka's `ElectionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// The preferred replica, when it is in sync (KIP-183).
    Preferred,
    /// Any live replica when no in-sync replica is live (KIP-460).
    Unclean,
}

impl ElectionType {
    const fn name(self) -> &'static str {
        match self {
            Self::Preferred => "PREFERRED",
            Self::Unclean => "UNCLEAN",
        }
    }

    /// The client's election type.
    const fn client(self) -> krabka_client_admin::ElectionType {
        match self {
            Self::Preferred => krabka_client_admin::ElectionType::Preferred,
            Self::Unclean => krabka_client_admin::ElectionType::Unclean,
        }
    }

    /// Parses as joptsimple's `EnumConverter` does, in any case.
    fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_uppercase().as_str() {
            "PREFERRED" => Ok(Self::Preferred),
            "UNCLEAN" => Ok(Self::Unclean),
            _ => Err(format!(
                "Cannot parse argument '{value}' of option election-type"
            )),
        }
    }
}

/// Which partitions the election covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// Every eligible partition: `ElectLeadersRequest.topic_partitions` is
    /// null.
    All,
    /// These partitions, which may be none.
    Partitions(BTreeSet<TopicPartition>),
}

impl Selection {
    /// The `partitions` argument of Kafka's `Admin.electLeaders`: `None`
    /// for every partition, which the request sends as a null
    /// `topic_partitions`, and otherwise the partitions, which may be none.
    #[must_use]
    pub fn request_partitions(&self) -> Option<Vec<(String, i32)>> {
        match self {
            Self::All => None,
            Self::Partitions(partitions) => Some(
                partitions
                    .iter()
                    .map(|partition| (partition.topic.clone(), partition.partition))
                    .collect(),
            ),
        }
    }
}

/// Where the partitions of a validated command line come from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    /// `--all-topic-partitions`.
    All,
    /// `--topic` with `--partition`.
    Partitions(BTreeSet<TopicPartition>),
    /// `--path-to-json-file`, read after the validation.
    File(PathBuf),
}

/// A validated `kafka-leader-election` command line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Election {
    kind: ElectionType,
    target: Target,
    /// Lines that `kafka-leader-election` prints before it connects.
    notices: Vec<String>,
}

impl LeaderElectionArgs {
    pub async fn run(mut self) -> Result<CommandResult, CommandError> {
        let Election {
            kind: election_type,
            target,
            notices,
        } = self.validate()?;
        let selection = match target {
            Target::All => Selection::All,
            Target::Partitions(partitions) => Selection::Partitions(partitions),
            Target::File(path) => {
                let text = tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|error| format!("{}: {error}", path.display()))?;
                Selection::Partitions(parse_election_data(&text)?)
            }
        };
        if let Some(config) = self.admin_config.take() {
            self.connection.command_config = Some(config);
        }
        let mut client = self.connection.connect("leader-election").await?;
        let report = match elect_leaders(&mut client, election_type, &selection).await {
            Ok(results) => election_report(election_type, &results),
            Err(error) => call_failure(error)?,
        };
        Ok(CommandResult {
            human: notices.into_iter().chain(report.human).collect(),
            ..report
        })
    }

    /// `LeaderElectionCommandOptions.validate`, then the deprecated-option
    /// handling of `LeaderElectionCommand.run`. The JSON file is read later.
    fn validate(&self) -> Result<Election, String> {
        let mut missing = Vec::new();
        if self.connection.bootstrap_server.is_empty() {
            missing.push("bootstrap-server");
        }
        if self.election_type.is_none() {
            missing.push("election-type");
        }
        if !missing.is_empty() {
            return Err(format!(
                "Missing required option(s): {}",
                missing.join(", ")
            ));
        }
        if !self.connection.bootstrap_controller.is_empty() {
            return Err("bootstrap-controller is not a recognized option".into());
        }
        let election_type = ElectionType::parse(self.election_type.as_deref().unwrap_or_default())?;
        if self.admin_config.is_some() && self.connection.command_config.is_some() {
            return Err(
                "Option \"[admin.config]\" can't be used with option \"[command-config]\"".into(),
            );
        }
        let chosen = [
            self.topic.is_some(),
            self.all_topic_partitions,
            self.path_to_json_file.is_some(),
        ]
        .into_iter()
        .filter(|chosen| *chosen)
        .count();
        if chosen != 1 {
            return Err(
                "One and only one of the following options is required: topic, \
                        all-topic-partitions, path-to-json-file"
                    .into(),
            );
        }
        let target = match (&self.topic, self.partition, &self.path_to_json_file) {
            (Some(_), None, _) => return Err("Missing required option(s): partition".into()),
            (None, Some(_), _) => {
                return Err("Option partition is only allowed if topic is used".into());
            }
            (Some(topic), Some(partition), _) => {
                Target::Partitions(BTreeSet::from([TopicPartition::new(topic, partition)]))
            }
            (None, None, Some(path)) => Target::File(path.clone()),
            (None, None, None) => Target::All,
        };
        let notices = if self.admin_config.is_some() {
            vec![
                "Option --admin.config has been deprecated and will be removed in a future \
                 version. Use --command-config instead."
                    .into(),
            ]
        } else {
            Vec::new()
        };
        Ok(Election {
            kind: election_type,
            target,
            notices,
        })
    }
}

/// `LeaderElectionCommand.parseReplicaElectionData` of the file's text.
///
/// # Errors
/// Returns the message that `kafka-leader-election` prints for the same
/// file.
fn parse_election_data(text: &str) -> Result<BTreeSet<TopicPartition>, String> {
    let document =
        kafka_json::parse_full(text).ok_or_else(|| "Replica election data is empty".to_owned())?;
    let object = kafka_json::document_object(document.as_ref())?;
    let partitions = object
        .get("partitions")
        .ok_or_else(|| "Replica election data is missing \"partitions\" field".to_owned())?;
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for entry in kafka_json::array(partitions)? {
        let entry = kafka_json::object(entry)?;
        let topic = kafka_json::string(kafka_json::field(entry, "topic")?)?;
        let partition = kafka_json::int(kafka_json::field(entry, "partition")?)?;
        let partition = TopicPartition::new(topic, partition);
        if !seen.insert(partition.clone()) {
            duplicates.insert(partition);
        }
    }
    if !duplicates.is_empty() {
        return Err(format!(
            "Replica election data contains duplicate partitions: [{}]",
            join(&duplicates, ", ")
        ));
    }
    Ok(seen)
}

/// The result of each partition's election: `None` for an elected leader,
/// or the partition's error.
type ElectionResults = BTreeMap<TopicPartition, Option<KafkaError>>;

/// Sends `ElectLeaders` for `selection`.
///
/// # Errors
/// Returns the error of the `ElectLeaders` call.
async fn elect_leaders(
    client: &mut AdminClient,
    election_type: ElectionType,
    selection: &Selection,
) -> Result<ElectionResults, AdminError> {
    let partitions = selection.request_partitions();
    Ok(client
        .elect_leaders(election_type.client(), partitions.as_deref())
        .await?
        .into_iter()
        .map(|((topic, partition), result)| (TopicPartition::new(topic, partition), result.err()))
        .collect())
}

/// What `kafka-leader-election` does when the whole call fails: it prints a
/// line for a timeout or a refused authorization and fails with the same
/// message, and fails with the error otherwise.
fn call_failure(error: AdminError) -> Result<CommandResult, CommandError> {
    let message = match &error {
        AdminError::Broker {
            code: REQUEST_TIMED_OUT,
            ..
        } => "Timeout waiting for election results",
        AdminError::Broker {
            code: CLUSTER_AUTHORIZATION_FAILED,
            ..
        } => "Not authorized to perform leader election",
        _ => return Err(error.into()),
    };
    Ok(
        CommandResult::rows(vec![message.to_owned()], json!({"error": message}), true)
            .with_notices(vec![message.to_owned()]),
    )
}

/// What `kafka-leader-election` prints for the results: the elected
/// partitions, the partitions that needed no election, then one line per
/// failed partition, each group in the order of Kafka's `HashSet` or
/// `HashMap`. `ELECTION_NOT_NEEDED` is not a failure. A failure ends with
/// `<n> replica(s) could not be elected` on stderr.
fn election_report(election_type: ElectionType, results: &ElectionResults) -> CommandResult {
    let name = election_type.name();
    let succeeded = results
        .iter()
        .filter(|(_, error)| error.is_none())
        .map(|(partition, _)| partition.clone())
        .collect::<Vec<_>>();
    let succeeded = hash_set_order(succeeded, TopicPartition::java_hash);
    let noop = results
        .iter()
        .filter(|(_, error)| {
            error
                .as_ref()
                .is_some_and(|e| e.code == ELECTION_NOT_NEEDED)
        })
        .map(|(partition, _)| partition.clone())
        .collect::<Vec<_>>();
    let noop = hash_set_order(noop, TopicPartition::java_hash);
    let failed = results
        .iter()
        .filter_map(|(partition, error)| {
            error
                .as_ref()
                .filter(|e| e.code != ELECTION_NOT_NEEDED)
                .map(|error| (partition.clone(), error))
        })
        .collect::<Vec<_>>();
    let failed = crate::jvm::hash_order(failed, crate::jvm::Table::Default, |(partition, _)| {
        partition.java_hash()
    });
    let mut human = Vec::new();
    if !succeeded.is_empty() {
        human.push(format!(
            "Successfully completed leader election ({name}) for partitions {}",
            join(&succeeded, ", ")
        ));
    }
    if !noop.is_empty() {
        human.push(format!(
            "Valid replica already elected for partitions {}",
            join(&noop, ", ")
        ));
    }
    for (partition, error) in &failed {
        human.push(format!(
            "Error completing leader election ({name}) for partition: {partition}: {}",
            java_exception(error)
        ));
    }
    let data = results
        .iter()
        .map(|(partition, error)| {
            json!({
                "topic": partition.topic,
                "partition": partition.partition,
                "election_type": name,
                "error": kafka_error(error.as_ref()),
            })
        })
        .collect::<Vec<_>>();
    let notices = if failed.is_empty() {
        Vec::new()
    } else {
        vec![format!("{} replica(s) could not be elected", failed.len())]
    };
    CommandResult::rows(human, data, !failed.is_empty()).with_notices(notices)
}

#[cfg(test)]
mod tests;
