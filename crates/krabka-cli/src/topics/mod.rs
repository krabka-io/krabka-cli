//! `krabka topics`, the counterpart of `kafka-topics`.
//!
//! The flags, the checks on them, their messages, and the stdout of each
//! action follow Kafka 4.3.1's `TopicCommand`, so an operator's existing
//! `kafka-topics` invocation runs unchanged. `tests/conformance.rs` lists
//! each difference that remains, with its reason.
//!
//! Failures print in two places. A per-topic failure that the broker reports
//! prints on stdout as Kafka prints it, `Error while executing topic command
//! : <message>`, and the command exits 1. A failure of the command as a whole
//! goes through the output layer, on stderr, with Kafka's message.
//!
//! Some sub-features need admin calls that the pinned `krabka-client-admin`
//! does not have. Each fails with a message that names the missing call.

mod config;
mod describe;
mod java;

use std::collections::{BTreeMap, BTreeSet};

use clap::{ArgAction, Args};
use krabka_client_admin::{
    AdminClient, AdminError, ConfigResource, CreatePartitionsOp, CreateTopicOutcome,
    CreateTopicSpec, DeleteTopicOutcome, DescribeConfigsOptions, KafkaError, TopicMetadataEntry,
    TopicMutationOptions,
};
use krabka_client_core::ClientError;
use krabka_units::Time;
use serde_json::{Value, json};

use self::{
    config::{parse_replica_assignment, parse_topic_configs},
    describe::{Partition, Reassignment, Selector, Selectors, Topic, TopicReport, describe_topic},
    java::{
        IncludeList, describe_order, has_collision_chars, is_internal, uuid_from_string,
        uuid_to_string,
    },
};
use crate::{
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
};

const ACTIONS: &str =
    "Command must include exactly one action: --list, --describe, --create, --alter or --delete";
const ALTER_CONFIGS_HINT: &str =
    " (To alter topic configurations, the kafka-configs tool can be used.)";
const DELETE_CONFIG_NOTICE: &str = "delete-config option is no longer supported and deprecated \
                                    since version 4.0. The config will be fully removed in future \
                                    releases.";
const TOPIC_ID_NOTICE: &str = "Only topic id will be used when both --topic and --topic-id are \
                               specified and topicId is not Uuid.ZERO_UUID";
const COLLISION_WARNING: &str = "WARNING: Due to limitations in metric names, topics with a \
                                 period ('.') or underscore ('_') could collide. To avoid issues \
                                 it is best to use either, but not both.";
const ERROR_PREFIX: &str = "Error while executing topic command : ";

/// `kafka-topics`: create, delete, describe, or change a topic.
///
/// The action and selector flags count their occurrences, because
/// `kafka-topics` accepts a flag more than once. Any count above zero sets
/// the flag.
#[derive(Debug, Args, PartialEq)]
#[command(args_override_self = true)]
pub struct TopicsArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    /// List all available topics.
    #[arg(long, action = ArgAction::Count)]
    pub list: u8,
    /// Create a new topic.
    #[arg(long, action = ArgAction::Count)]
    pub create: u8,
    /// Delete a topic.
    #[arg(long, action = ArgAction::Count)]
    pub delete: u8,
    /// Alter the number of partitions and replica assignment. (To alter topic
    /// configurations, the kafka-configs tool can be used.)
    #[arg(long, action = ArgAction::Count)]
    pub alter: u8,
    /// List details for the given topics.
    #[arg(long, action = ArgAction::Count)]
    pub describe: u8,
    /// The topic to create, alter, describe or delete. It also accepts a
    /// regular expression, except for --create option. krabka also accepts
    /// the flag more than once.
    #[arg(long, value_name = "topic", action = ArgAction::Append)]
    pub topic: Vec<String>,
    /// The topic-id to describe.
    #[arg(long, value_name = "topic-id", action = ArgAction::Append, allow_hyphen_values = true)]
    pub topic_id: Vec<String>,
    /// A topic configuration override for the topic being created. It is
    /// supported only in combination with --create. (To alter topic
    /// configurations, the kafka-configs tool can be used.)
    #[arg(long, value_name = "name=value", action = ArgAction::Append)]
    pub config: Vec<String>,
    /// This option is no longer supported and has been deprecated since 4.0.
    #[arg(long, value_name = "name", action = ArgAction::Append)]
    pub delete_config: Vec<String>,
    /// The number of partitions for the topic being created or altered. If
    /// not supplied with --create, the topic uses the cluster default.
    /// (WARNING: If partitions are increased for a topic that has a key, the
    /// partition logic or ordering of the messages will be affected).
    #[arg(
        long,
        value_name = "# of partitions",
        action = ArgAction::Append,
        allow_hyphen_values = true
    )]
    pub partitions: Vec<String>,
    /// The replication factor for each partition in the topic being created.
    /// If not supplied, the topic uses the cluster default.
    #[arg(
        long,
        value_name = "replication factor",
        action = ArgAction::Append,
        allow_hyphen_values = true
    )]
    pub replication_factor: Vec<String>,
    /// A list of manual partition-to-broker assignments for the topic being
    /// created or altered.
    #[arg(
        long,
        value_name = "broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , \
                      broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...",
        action = ArgAction::Append
    )]
    pub replica_assignment: Vec<String>,
    /// If set when describing topics, only show under-replicated partitions.
    #[arg(long, action = ArgAction::Count)]
    pub under_replicated_partitions: u8,
    /// If set when describing topics, only show partitions whose leader is
    /// not available.
    #[arg(long, action = ArgAction::Count)]
    pub unavailable_partitions: u8,
    /// If set when describing topics, only show partitions whose isr count is
    /// less than the configured minimum.
    #[arg(long, action = ArgAction::Count)]
    pub under_min_isr_partitions: u8,
    /// If set when describing topics, only show partitions whose isr count is
    /// equal to the configured minimum.
    #[arg(long, action = ArgAction::Count)]
    pub at_min_isr_partitions: u8,
    /// If set when describing topics, only show topics that have overridden
    /// configs.
    #[arg(long, action = ArgAction::Count)]
    pub topics_with_overrides: u8,
    /// If set when altering or deleting or describing topics, the action will
    /// only execute if the topic exists.
    #[arg(long)]
    pub if_exists: bool,
    /// If set when creating topics, the create request will not fail if the
    /// topic already exists, but the request will still be sent.
    #[arg(long)]
    pub if_not_exists: bool,
    /// Exclude internal topics when listing or describing topics. By default,
    /// the internal topics are included.
    #[arg(long)]
    pub exclude_internal: bool,
    /// The maximum partition size to be included in one
    /// `DescribeTopicPartitions` response.
    #[arg(
        long,
        value_name = "maximum number of partitions per response",
        action = ArgAction::Append,
        allow_hyphen_values = true
    )]
    pub partition_size_limit_per_response: Vec<String>,
    #[command(flatten)]
    pub confirm: ConfirmArgs,
}

/// Which topics an action applies to: the `--topic` patterns, as Kafka's
/// `TopicCommand.getTopics` and `ensureTopicExists` read them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    /// The `--topic` values. Each is a whole-name regular expression. No
    /// value selects every topic.
    pub patterns: Vec<String>,
    pub exclude_internal: bool,
    /// Whether a non-empty pattern that matches no topic is an error.
    pub require_exists: bool,
}

/// What `--create` sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    pub names: Vec<String>,
    /// `None` leaves the count to the cluster default.
    pub partitions: Option<i32>,
    /// `None` leaves the factor to the cluster default.
    pub replication_factor: Option<i32>,
    pub configs: BTreeMap<String, String>,
    pub if_not_exists: bool,
}

/// Which topics `--describe` describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Names(Selection),
    /// `--topic-id`, when it is not the zero ID.
    Id {
        id: [u8; 16],
        exclude_internal: bool,
        require_exists: bool,
    },
}

/// The checked form of one `krabka topics` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    List(Selection),
    Create(Create),
    Alter {
        selection: Selection,
        partitions: i32,
    },
    Delete(Selection),
    Describe {
        target: Target,
        selectors: Selectors,
    },
}

/// An invocation that passed every check that `kafka-topics` makes before it
/// sends a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub action: Action,
    /// Lines that `kafka-topics` prints on stdout before the result.
    pub notices: Vec<String>,
    /// Whether `kafka-topics` prints the `--delete-config` notice on stderr.
    pub delete_config_notice: bool,
}

/// The error for a sub-feature that needs an admin call that the pinned
/// `krabka-client-admin` does not have.
fn not_supported(feature: &str, needs: &str) -> String {
    format!(
        "{feature} is not supported by this build: it needs {needs}, which the pinned \
         krabka-client-admin does not have"
    )
}

/// Whether a counted flag was given.
const fn set(count: u8) -> bool {
    count > 0
}

/// `OptionSet.valueOf` on a single-valued option: at most one value.
fn single<'a>(values: &'a [String], option: &str) -> Result<Option<&'a str>, String> {
    match values {
        [] => Ok(None),
        [value] => Ok(Some(value)),
        _ => Err(format!(
            "Found multiple arguments for option {option}, but you asked for only one"
        )),
    }
}

/// A single-valued integer option, as joptsimple converts it.
fn int_option(values: &[String], option: &str) -> Result<Option<i32>, String> {
    single(values, option)?
        .map(|value| {
            value
                .parse()
                .map_err(|_| format!("Cannot parse argument '{value}' of option {option}"))
        })
        .transpose()
}

impl TopicsArgs {
    fn action_name(&self) -> &'static str {
        [
            (set(self.create), "create"),
            (set(self.list), "list"),
            (set(self.alter), "alter"),
            (set(self.describe), "describe"),
            (set(self.delete), "delete"),
        ]
        .into_iter()
        .find_map(|(set, name)| set.then_some(name))
        .unwrap_or_default()
    }

    /// Kafka's `CommandLineUtils.checkInvalidArgs`: `used` is refused with an
    /// action outside `actions`, and with any of `others`.
    fn check_invalid(
        &self,
        used: (bool, &str),
        actions: &[&str],
        others: &[(bool, &str)],
    ) -> Result<(), String> {
        let (present, name) = used;
        if !present {
            return Ok(());
        }
        let action = self.action_name();
        let conflict = (!actions.contains(&action))
            .then_some(action)
            .or_else(|| others.iter().find_map(|(set, other)| set.then_some(*other)));
        match conflict {
            Some(other) => Err(format!(
                "Option \"[{name}]\" can't be used with option \"[{other}]\""
            )),
            None => Ok(()),
        }
    }

    /// Every check that `kafka-topics` makes before it sends a request, in
    /// its order, after the check on krabka's own `--dry-run`.
    ///
    /// # Errors
    /// Returns the message of the first check that fails.
    pub fn plan(&self) -> Result<Plan, String> {
        if self.confirm.dry_run && !(set(self.create) || set(self.delete)) {
            return Err("--dry-run is only valid with --create or --delete".into());
        }
        self.check_args()?;
        let mut notices = Vec::new();
        if set(self.describe)
            && self.if_exists
            && !self.topic.is_empty()
            && !self.topic_id.is_empty()
        {
            notices.push(TOPIC_ID_NOTICE.to_owned());
        }
        let selection = |require_exists: bool| Selection {
            patterns: self.topic.clone(),
            exclude_internal: self.exclude_internal,
            require_exists,
        };
        let action = if set(self.list) {
            Action::List(selection(false))
        } else if set(self.create) {
            Action::Create(self.plan_create()?)
        } else if set(self.alter) {
            let partitions = int_option(&self.partitions, "partitions")?;
            if self.replica_assignment()?.is_some() {
                return Err(not_supported(
                    "--replica-assignment with --alter",
                    "a replica assignment in CreatePartitionsOp",
                ));
            }
            Action::Alter {
                selection: selection(!self.if_exists),
                partitions: partitions.unwrap_or_default(),
            }
        } else if set(self.delete) {
            Action::Delete(selection(!self.if_exists))
        } else {
            self.plan_describe(selection(!self.if_exists))?
        };
        if !set(self.create) {
            for pattern in &self.topic {
                IncludeList::new(pattern)?;
            }
        }
        Ok(Plan {
            action,
            notices,
            delete_config_notice: !self.delete_config.is_empty(),
        })
    }

    /// Kafka's `TopicCommandOptions.checkArgs`.
    fn check_args(&self) -> Result<(), String> {
        let actions = [
            set(self.create),
            set(self.list),
            set(self.alter),
            set(self.describe),
            set(self.delete),
        ];
        if actions.iter().filter(|set| **set).count() != 1 {
            return Err(ACTIONS.into());
        }
        if self.connection.bootstrap_server.is_empty()
            && self.connection.bootstrap_controller.is_empty()
        {
            return Err("--bootstrap-server must be specified".into());
        }
        let (has_topic, has_id) = (!self.topic.is_empty(), !self.topic_id.is_empty());
        if set(self.describe) && self.if_exists && !has_topic && !has_id {
            return Err("--topic or --topic-id is required to describe a topic".into());
        }
        if !set(self.list) && !set(self.describe) && !has_topic {
            return Err("Missing required argument \"[topic]\"".into());
        }
        if set(self.alter) {
            if !self.config.is_empty() {
                return Err(format!(
                    "Option combination \"[[bootstrap-server], [config]]\" can't be used with \
                     option \"[alter]\"{ALTER_CONFIGS_HINT}"
                ));
            }
            if self.partitions.is_empty() {
                return Err("Missing required argument \"[partitions]\"".into());
            }
        }
        let present = |values: &[String], name: &'static str| (!values.is_empty(), name);
        self.check_invalid(present(&self.config, "config"), &["alter", "create"], &[])?;
        self.check_invalid(
            present(&self.partitions, "partitions"),
            &["alter", "create"],
            &[],
        )?;
        self.check_invalid(
            present(&self.replication_factor, "replication-factor"),
            &["create"],
            &[],
        )?;
        let assignment = present(&self.replica_assignment, "replica-assignment");
        self.check_invalid(assignment, &["alter", "create"], &[])?;
        if set(self.create) {
            self.check_invalid(
                assignment,
                &["create"],
                &[
                    present(&self.partitions, "partitions"),
                    present(&self.replication_factor, "replication-factor"),
                ],
            )?;
        }
        let overrides = (set(self.topics_with_overrides), "topics-with-overrides");
        let reports = [
            (
                set(self.under_replicated_partitions),
                "under-replicated-partitions",
            ),
            (
                set(self.under_min_isr_partitions),
                "under-min-isr-partitions",
            ),
            (set(self.at_min_isr_partitions), "at-min-isr-partitions"),
            (set(self.unavailable_partitions), "unavailable-partitions"),
        ];
        for report in reports {
            self.check_invalid(report, &["describe"], &[overrides])?;
        }
        self.check_invalid(overrides, &["describe"], &reports)?;
        self.check_invalid(
            (self.if_exists, "if-exists"),
            &["alter", "delete", "describe"],
            &[],
        )?;
        self.check_invalid((self.if_not_exists, "if-not-exists"), &["create"], &[])?;
        self.check_invalid(
            (self.exclude_internal, "exclude-internal"),
            &["list", "describe"],
            &[],
        )?;
        Ok(())
    }

    /// `--replica-assignment`, parsed when it is set and not empty, as
    /// Kafka's `TopicCommandOptions.replicaAssignment` does.
    fn replica_assignment(&self) -> Result<Option<Vec<Vec<i32>>>, String> {
        single(&self.replica_assignment, "replica-assignment")?
            .filter(|value| !value.is_empty())
            .map(parse_replica_assignment)
            .transpose()
    }

    /// Kafka's `CommandTopicPartition` and the checks of `createTopic`.
    fn plan_create(&self) -> Result<Create, String> {
        let partitions = int_option(&self.partitions, "partitions")?;
        let replication_factor = int_option(&self.replication_factor, "replication-factor")?;
        let assignment = self.replica_assignment()?;
        let configs = parse_topic_configs(&self.config)?;
        if replication_factor.is_some_and(|factor| !(1..=i32::from(i16::MAX)).contains(&factor)) {
            return Err(format!(
                "The replication factor must be between 1 and {} inclusive",
                i16::MAX
            ));
        }
        if partitions.is_some_and(|count| count < 1) {
            return Err("The partitions must be greater than 0".into());
        }
        if assignment.is_some() {
            return Err(not_supported(
                "--replica-assignment with --create",
                "a replica assignment in CreateTopicSpec",
            ));
        }
        Ok(Create {
            names: self.topic.clone(),
            partitions,
            replication_factor,
            configs,
            if_not_exists: self.if_not_exists,
        })
    }

    fn plan_describe(&self, selection: Selection) -> Result<Action, String> {
        int_option(
            &self.partition_size_limit_per_response,
            "partition-size-limit-per-response",
        )?;
        let id = single(&self.topic_id, "topic-id")?
            .map(uuid_from_string)
            .transpose()?
            .filter(|id| *id != [0; 16]);
        let target = match id {
            Some(id) => Target::Id {
                id,
                exclude_internal: self.exclude_internal,
                require_exists: !self.if_exists,
            },
            None => Target::Names(selection),
        };
        let selectors = [
            (
                set(self.under_replicated_partitions),
                Selector::UnderReplicated,
            ),
            (set(self.unavailable_partitions), Selector::Unavailable),
            (set(self.under_min_isr_partitions), Selector::UnderMinIsr),
            (set(self.at_min_isr_partitions), Selector::AtMinIsr),
            (
                set(self.topics_with_overrides),
                Selector::TopicsWithOverrides,
            ),
        ]
        .into_iter()
        .filter_map(|(set, selector)| set.then_some(selector))
        .collect();
        Ok(Action::Describe {
            target,
            selectors: Selectors(selectors),
        })
    }

    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let plan = self.plan()?;
        if plan.delete_config_notice {
            eprintln!("{DELETE_CONFIG_NOTICE}");
        }
        let mut client = self.connection.connect("topics").await?;
        let timeout = self.connection.timeout;
        let mut result = match plan.action {
            Action::List(selection) => list(&mut client, &selection).await?,
            Action::Create(create) => {
                create_topics(&mut client, &create, self.confirm.dry_run, timeout).await?
            }
            Action::Alter {
                selection,
                partitions,
            } => alter(&mut client, &selection, partitions, timeout).await?,
            Action::Delete(selection) => {
                delete(&mut client, &selection, self.confirm, timeout).await?
            }
            Action::Describe { target, selectors } => {
                describe(&mut client, &target, &selectors, timeout).await?
            }
        };
        result.human.splice(0..0, plan.notices);
        Ok(result)
    }
}

/// Every topic that the cluster lists, sorted by name, as Kafka's
/// `listTopics` with internal topics included.
async fn listing(client: &mut AdminClient) -> Result<Vec<TopicMetadataEntry>, AdminError> {
    let mut topics = client.metadata(&[]).await?.topics;
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}

/// Kafka's `TopicCommand.getTopics` followed by `ensureTopicExists`, over
/// the sorted names of every topic.
///
/// # Errors
/// Returns Kafka's message for an invalid pattern, and for a pattern that
/// matches no topic when the selection requires one.
pub fn resolve(all: &[String], selection: &Selection) -> Result<Vec<String>, String> {
    let allowed = |topic: &&String| !(selection.exclude_internal && is_internal(topic));
    if selection.patterns.is_empty() {
        return Ok(all.iter().filter(allowed).cloned().collect());
    }
    let mut selected = BTreeSet::new();
    for pattern in &selection.patterns {
        let filter = IncludeList::new(pattern)?;
        let found = all
            .iter()
            .filter(allowed)
            .filter(|topic| filter.matches(topic))
            .cloned()
            .collect::<Vec<_>>();
        if selection.require_exists && !pattern.is_empty() && found.is_empty() {
            return Err(format!("Topic '{pattern}' does not exist as expected"));
        }
        selected.extend(found);
    }
    Ok(selected.into_iter().collect())
}

fn names(entries: &[TopicMetadataEntry]) -> Vec<String> {
    entries.iter().map(|entry| entry.name.clone()).collect()
}

/// The message of the exception that Kafka's admin client raises for a
/// per-topic error: the broker's message, or the error's default message.
fn error_message(error: &KafkaError) -> String {
    if let Some(message) = error.message.as_ref().filter(|message| !message.is_empty()) {
        return message.clone();
    }
    let default = match error.code {
        3 => "This server does not host this topic-partition.",
        7 => "The request timed out.",
        17 => "The request attempted to perform an operation on an invalid topic.",
        29 => "Topic authorization failed.",
        31 => "Cluster authorization failed.",
        35 => "The version of API is not supported.",
        36 => "Topic with this name already exists.",
        37 => "Number of partitions is below 1.",
        38 => "Replication factor is below 1 or larger than the number of available brokers.",
        39 => "Replica assignment is invalid.",
        40 => "Configuration is invalid.",
        41 => "This is not the correct controller for this cluster.",
        42 => {
            "This most likely occurs because of a request being malformed by the client library \
             or the message was sent to an incompatible broker. See the broker logs for more \
             details."
        }
        44 => "Request parameters do not satisfy the configured policy.",
        73 => "Topic deletion is disabled.",
        89 => "The throttling quota has been exceeded.",
        100 => "This server does not host this topic ID.",
        _ => error.name,
    };
    default.to_owned()
}

fn failure_line(error: &KafkaError) -> String {
    format!("{ERROR_PREFIX}{}", error_message(error))
}

async fn list(
    client: &mut AdminClient,
    selection: &Selection,
) -> Result<CommandResult, CommandError> {
    let entries = listing(client).await?;
    let selected = resolve(&names(&entries), selection)?;
    let values = entries
        .iter()
        .filter(|entry| selected.contains(&entry.name))
        .map(|entry| {
            json!({
                "topic": entry.name,
                "topic_id": entry.topic_id.map(|id| uuid_to_string(id.as_bytes())),
                "partitions": entry.partition_count,
                "replication_factor": entry.replication_factor,
                "error": kafka_error(entry.error.as_ref()),
            })
        })
        .collect::<Vec<_>>();
    // `String.join("\n", topics)` then `println`: no topic prints one empty
    // line.
    let human = if selected.is_empty() {
        vec![String::new()]
    } else {
        selected
    };
    Ok(CommandResult::success(human, values))
}

/// The report of `--create`, from the outcome of each topic.
fn created(outcomes: &mut [CreateTopicOutcome], create: &Create) -> CommandResult {
    outcomes.sort_by(|a, b| a.name.cmp(&b.name));
    let mut human = Vec::new();
    if create.names.iter().any(|name| has_collision_chars(name)) {
        human.push(COLLISION_WARNING.to_owned());
    }
    let mut failed = false;
    for outcome in outcomes.iter() {
        match &outcome.error {
            Some(error) if error.code == 36 && create.if_not_exists => {}
            Some(error) => {
                failed = true;
                human.push(failure_line(error));
            }
            None => human.push(format!("Created topic {}.", outcome.name)),
        }
    }
    let values = outcomes
        .iter()
        .map(|outcome| {
            json!({
                "topic": outcome.name,
                "topic_id": outcome.topic_id.map(|id| uuid_to_string(id.as_bytes())),
                "error": kafka_error(outcome.error.as_ref()),
            })
        })
        .collect::<Vec<_>>();
    CommandResult::rows(human, values, failed)
}

async fn create_topics(
    client: &mut AdminClient,
    create: &Create,
    dry_run: bool,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    if dry_run {
        // What CreateTopics would answer for each topic, from the metadata
        // that the broker holds now.
        let names = create.names.iter().map(String::as_str).collect::<Vec<_>>();
        let existing = client.metadata(&names).await?;
        let mut outcomes = create
            .names
            .iter()
            .map(|name| {
                let exists = existing
                    .topics
                    .iter()
                    .any(|topic| &topic.name == name && topic.error.is_none());
                CreateTopicOutcome {
                    name: name.clone(),
                    topic_id: None,
                    error: exists.then(|| KafkaError {
                        code: 36,
                        name: "TOPIC_ALREADY_EXISTS",
                        message: Some(format!("Topic '{name}' already exists.")),
                    }),
                    throttle_time: None,
                }
            })
            .collect::<Vec<_>>();
        return Ok(created(&mut outcomes, create).into_dry_run());
    }
    // -1 asks the broker for its default, as Kafka's `NewTopic` does for an
    // absent count or factor.
    let specs = create
        .names
        .iter()
        .map(|name| CreateTopicSpec {
            name: name.clone(),
            partitions: create.partitions.unwrap_or(-1),
            replicas: create.replication_factor.unwrap_or(-1),
            configs: create.configs.clone(),
            replica_assignments: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let mut outcomes = client
        .create_topics(&specs, TopicMutationOptions::with_timeout(timeout))
        .await?;
    Ok(created(&mut outcomes, create))
}

/// A report whose human form prints only the failed rows, because
/// `kafka-topics` prints nothing for a successful `--alter` or `--delete`.
fn failures_only(rows: Vec<(String, Option<KafkaError>, Value)>) -> CommandResult {
    let mut rows = rows;
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let failed = rows.iter().any(|(_, error, _)| error.is_some());
    let human = rows
        .iter()
        .filter_map(|(_, error, _)| error.as_ref().map(failure_line))
        .collect();
    let values = rows
        .into_iter()
        .map(|(_, _, value)| value)
        .collect::<Vec<_>>();
    CommandResult::rows(human, values, failed)
}

async fn alter(
    client: &mut AdminClient,
    selection: &Selection,
    partitions: i32,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    let selected = resolve(&names(&listing(client).await?), selection)?;
    if selected.is_empty() {
        return Ok(CommandResult::success(Vec::new(), Vec::<Value>::new()));
    }
    let ops = selected
        .iter()
        .map(|name| CreatePartitionsOp {
            name: name.clone(),
            new_total_count: partitions,
            assignments: None,
        })
        .collect::<Vec<_>>();
    let outcomes = client
        .create_partitions(&ops, TopicMutationOptions::with_timeout(timeout))
        .await?;
    Ok(failures_only(
        outcomes
            .into_iter()
            .map(|outcome| {
                let value = json!({
                    "topic": outcome.name,
                    "partitions": partitions,
                    "error": kafka_error(outcome.error.as_ref()),
                });
                (outcome.name, outcome.error, value)
            })
            .collect(),
    ))
}

fn deleted(outcomes: Vec<DeleteTopicOutcome>) -> CommandResult {
    failures_only(
        outcomes
            .into_iter()
            .map(|outcome| {
                let value = json!({
                    "topic": outcome.name,
                    "error": kafka_error(outcome.error.as_ref()),
                });
                (outcome.name, outcome.error, value)
            })
            .collect(),
    )
}

async fn delete(
    client: &mut AdminClient,
    selection: &Selection,
    safety: ConfirmArgs,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    let entries = listing(client).await?;
    let selected = resolve(&names(&entries), selection)?;
    if safety.dry_run {
        // What DeleteTopics would answer for each topic: a topic that the
        // metadata lists with an error fails as it would fail there. The
        // human report also names each topic, because a successful delete
        // prints nothing.
        let outcomes = entries
            .into_iter()
            .filter(|entry| selected.contains(&entry.name))
            .map(|entry| DeleteTopicOutcome {
                name: entry.name,
                error: entry.error,
                throttle_time: None,
            })
            .collect::<Vec<_>>();
        let mut report = deleted(outcomes);
        report.human.splice(0..0, selected);
        return Ok(report.into_dry_run());
    }
    if selected.is_empty() {
        return Ok(CommandResult::success(Vec::new(), Vec::<Value>::new()));
    }
    confirm(
        safety.yes,
        "krabka topics",
        Impact {
            summary: format!("delete {} topic(s)", selected.len()),
            resources: selected.clone(),
        },
    )
    .await?;
    let names = selected.iter().map(String::as_str).collect::<Vec<_>>();
    Ok(deleted(
        client
            .delete_topics(&names, TopicMutationOptions::with_timeout(timeout))
            .await?,
    ))
}

/// The ongoing reassignments of the described partitions. As in Kafka, a
/// broker that does not support the call, or does not authorize it, has none
/// to report.
async fn reassignments(
    client: &mut AdminClient,
    topics: &[Topic],
    timeout: Time,
) -> Result<BTreeMap<(String, i32), Reassignment>, AdminError> {
    let filter = topics
        .iter()
        .map(|topic| {
            let partitions = topic.partitions.iter().map(|p| p.index).collect();
            (topic.name.clone(), partitions)
        })
        .collect::<BTreeMap<_, Vec<_>>>();
    match client.list_partition_reassignments(&filter, timeout).await {
        Ok(ongoing) => Ok(ongoing
            .into_iter()
            .map(|assignment| {
                (
                    (assignment.topic, assignment.partition),
                    Reassignment {
                        replicas: assignment.replicas,
                        adding: assignment.adding_replicas,
                        removing: assignment.removing_replicas,
                    },
                )
            })
            .collect()),
        Err(
            AdminError::Broker { code: 31 | 35, .. }
            | AdminError::Transport(ClientError::IncompatibleVersion { .. }),
        ) => Ok(BTreeMap::new()),
        Err(error) => Err(error),
    }
}

/// The topics that `--describe` resolves, in the order that it prints them.
fn described(entries: &[TopicMetadataEntry], target: &Target) -> Result<Vec<String>, String> {
    match target {
        Target::Names(selection) => Ok(describe_order(resolve(&names(entries), selection)?)),
        Target::Id {
            id,
            exclude_internal,
            require_exists,
        } => {
            let found = entries
                .iter()
                .filter(|entry| !(*exclude_internal && is_internal(&entry.name)))
                .filter(|entry| entry.topic_id.is_some_and(|topic| topic.as_bytes() == id))
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>();
            if *require_exists && found.is_empty() {
                return Err(format!(
                    "TopicId '{}' does not exist as expected",
                    uuid_to_string(id)
                ));
            }
            Ok(found)
        }
    }
}

async fn describe(
    client: &mut AdminClient,
    target: &Target,
    selectors: &Selectors,
    timeout: Time,
) -> Result<CommandResult, CommandError> {
    let entries = listing(client).await?;
    let ordered = described(&entries, target)?;
    if ordered.is_empty() {
        return Ok(CommandResult::success(Vec::new(), Vec::<Value>::new()));
    }
    if !selectors.has(Selector::TopicsWithOverrides) {
        return Err(not_supported(
            "--describe with per-partition Leader, Isr and Elr lines",
            "AdminClient::describe_topics, and AdminClient::describe_cluster for \
             --unavailable-partitions",
        )
        .into());
    }
    // `--topics-with-overrides` prints the summary line alone, which needs
    // each partition's replicas and no leader, ISR or ELR.
    let refs = ordered.iter().map(String::as_str).collect::<Vec<_>>();
    let mut partitions = BTreeMap::<String, Vec<Partition>>::new();
    for assignment in client.describe_partition_assignments(&refs).await? {
        partitions
            .entry(assignment.topic)
            .or_default()
            .push(Partition {
                index: assignment.partition,
                leader: None,
                replicas: assignment.replicas,
                isr: Vec::new(),
                elr: None,
                last_known_elr: None,
            });
    }
    let topics = ordered
        .iter()
        .map(|name| {
            let mut partitions = partitions.remove(name).unwrap_or_default();
            partitions.sort_by_key(|partition| partition.index);
            Topic {
                name: name.clone(),
                id: entries
                    .iter()
                    .find(|entry| &entry.name == name)
                    .and_then(|entry| entry.topic_id.map(|id| *id.as_bytes())),
                partitions,
            }
        })
        .collect::<Vec<_>>();
    let resources = ordered
        .iter()
        .map(ConfigResource::topic)
        .collect::<Vec<_>>();
    let configs = client
        .describe_configs(&resources, DescribeConfigsOptions::default())
        .await?
        .into_iter()
        .map(|(resource, config)| {
            let overrides = config
                .map_err(|error| CommandError::from(failure_line(&error)))?
                .dynamic_overrides(&resource);
            Ok((resource.name, overrides))
        })
        .collect::<Result<BTreeMap<_, _>, CommandError>>()?;
    let reassignments = reassignments(client, &topics, timeout).await?;
    let empty = BTreeMap::new();
    let mut human = Vec::new();
    let mut values = Vec::new();
    for topic in &topics {
        let topic_reassignments = reassignments
            .iter()
            .filter(|((name, _), _)| name == &topic.name)
            .map(|((_, partition), reassignment)| (*partition, reassignment.clone()))
            .collect();
        let report = TopicReport {
            topic,
            configs: configs.get(&topic.name).unwrap_or(&empty),
            min_isr: Err(not_supported(
                "min.insync.replicas",
                "AdminClient::describe_topics",
            )),
            reassignments: &topic_reassignments,
        };
        if let Some((lines, value)) = describe_topic(&report, selectors, &BTreeSet::new())? {
            human.extend(lines);
            values.push(value);
        }
    }
    Ok(CommandResult::success(human, values))
}

#[cfg(test)]
mod tests;
