//! `krabka consumer-groups`, the counterpart of `kafka-consumer-groups`
//! (`ConsumerGroupCommand` at Kafka 4.3.1).
//!
//! The flags, their rules and their messages are the JVM tool's, and each
//! action prints the JVM tool's tables column for column (see [`render`]).
//! Three krabka rules differ from Kafka 4.3.1, each on the safe side:
//!
//! - `--reset-offsets` needs `--dry-run` or `--execute`, as Kafka 5.0 will.
//! - `--delete` and `--delete-offsets` ask for confirmation, or `--yes`, and
//!   honour `--dry-run`, which the JVM tool ignores for them.
//! - A failure exits 1, where the JVM tool prints `Error: ...` and exits 0.
//!
//! The pinned `krabka-client-rs` revision lacks several `AdminClient` calls
//! that the JVM tool relies on: `describe_consumer_groups`, `list_offsets`,
//! `delete_consumer_groups`, `delete_consumer_group_offsets`, and a
//! `list_groups` that reports group state and type. Each is behind a seam
//! here, [`Groups`] and [`OffsetLookup`], whose implementation for this build
//! answers with a "not supported by this build" error. `--describe` still
//! prints the committed offsets it can read and says on stderr which columns
//! it could not fill.

mod render;
mod reset;

use std::{collections::BTreeMap, path::PathBuf};

use clap::{ArgGroup, Args};
use regex::Regex;
use serde_json::{Value, json};

use self::{
    render::{
        DeleteOffsetRow, ListedGroup, MISSING, MemberRow, OffsetRow, StateRow,
        delete_offsets_table, export_csv, lag, list_table, members_table, offsets_table,
        reset_table, state_table,
    },
    reset::{Partition, Plan, Scenario},
};
use crate::{
    cluster::{BROKER_FAN_OUT, broker_client, cluster_brokers},
    compat::{KafkaException, default_capacity, hash_order, not_supported, string_hash},
    connection::ConnectionArgs,
    fan_out,
    get_offsets::{OffsetLookup, Unavailable},
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
};

/// The most groups that `--describe` asks about at once, each on its own
/// connection to the group's coordinator.
pub const GROUP_FAN_OUT: usize = 8;

/// `GROUP_ID_NOT_FOUND`.
const GROUP_ID_NOT_FOUND: i16 = 69;

/// The states that `--list --state` accepts for consumer groups, in the order
/// the JVM tool names them.
const CONSUMER_GROUP_STATES: [&str; 7] = [
    "Dead",
    "CompletingRebalance",
    "Empty",
    "Stable",
    "Assigning",
    "Reconciling",
    "PreparingRebalance",
];

/// The types that `--list --type` accepts, in the order the JVM tool names
/// them.
const CONSUMER_GROUP_TYPES: [&str; 2] = ["Consumer", "Classic"];

#[derive(Debug, Args, PartialEq)]
#[command(
    group(ArgGroup::new("action").required(true).multiple(false).args(["list", "describe", "delete", "reset_offsets", "delete_offsets", "validate_regex"])),
    group(ArgGroup::new("scenario").multiple(false).args(["to_offset", "to_earliest", "to_latest", "to_current", "shift_by", "to_datetime", "by_duration", "from_file"])),
    group(ArgGroup::new("execution").multiple(false).args(["dry_run", "execute"])),
    group(ArgGroup::new("lists_or_describes").multiple(true).args(["list", "describe"])),
    // `requires` against a bare flag is always met, because clap counts the
    // flag's `false` default as present. A group of one is met only when the
    // flag is given.
    group(ArgGroup::new("listing").args(["list"])),
    group(ArgGroup::new("describing").args(["describe"])),
    group(ArgGroup::new("resetting").args(["reset_offsets"])),
)]
pub struct ConsumerGroupsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// List all consumer groups.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    list: Option<bool>,
    /// Describe consumer group and list offset lag related to given group.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    describe: Option<bool>,
    /// Delete topic partition offsets and ownership information over the
    /// entire consumer group.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    delete: Option<bool>,
    /// Reset offsets of consumer group.
    #[arg(long, num_args = 0, default_missing_value = "true", requires_all = ["scenario", "execution"])]
    reset_offsets: Option<bool>,
    /// Delete offsets of consumer group.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    delete_offsets: Option<bool>,
    /// Validate that the syntax of the provided regular expression is valid.
    #[arg(long, value_name = "REGEX")]
    validate_regex: Option<String>,
    /// The consumer group we wish to act on. Repeatable.
    #[arg(long)]
    group: Vec<String>,
    /// Apply to all consumer groups.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    all_groups: Option<bool>,
    /// A topic, or `topic:0,1,2` for `--reset-offsets`. Repeatable.
    #[arg(long)]
    topic: Vec<String>,
    /// Consider all topics assigned to a group in the `reset-offsets` process.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    all_topics: Option<bool>,
    /// Execute operation. Supported operations: reset-offsets.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    execute: Option<bool>,
    /// Export operation execution to a CSV format.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    export: Option<bool>,
    /// Reset offsets to a specific offset.
    #[arg(long, allow_hyphen_values = true, requires = "resetting")]
    to_offset: Option<i64>,
    /// Reset offsets to earliest offset.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    to_earliest: Option<bool>,
    /// Reset offsets to latest offset.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    to_latest: Option<bool>,
    /// Reset offsets to current offset.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "resetting"
    )]
    to_current: Option<bool>,
    /// Reset offsets shifting current offset by 'n'.
    #[arg(long, allow_hyphen_values = true, requires = "resetting")]
    shift_by: Option<i64>,
    /// Reset offsets to offset from datetime, `YYYY-MM-DDThh:mm:ss.sss`.
    #[arg(long, requires = "resetting")]
    to_datetime: Option<String>,
    /// Reset offsets to offset by duration from current timestamp,
    /// `PnDTnHnMnS`.
    #[arg(long, allow_hyphen_values = true, requires = "resetting")]
    by_duration: Option<String>,
    /// Reset offsets to values defined in CSV file.
    #[arg(long, requires = "resetting")]
    from_file: Option<PathBuf>,
    /// Describe members of the group.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "describing"
    )]
    members: Option<bool>,
    /// Describe the group and list all topic partitions in the group along
    /// with their offset lag. The default sub-action of `--describe`.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        requires = "describing"
    )]
    offsets: Option<bool>,
    /// With `--describe`, include the state of the group. With `--list`,
    /// show the state of each group, or list the groups in these states.
    #[arg(long, num_args = 0..=1, default_missing_value = "", requires = "lists_or_describes")]
    state: Option<String>,
    /// With `--list`, show the type of each group, or list the groups of these
    /// types.
    #[arg(long = "type", num_args = 0..=1, default_missing_value = "", requires = "listing")]
    group_type: Option<String>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// One member of a described group, as Kafka's `MemberDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub consumer_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<Partition>,
    pub target_assignment: Option<Vec<Partition>>,
    pub epoch: Option<i32>,
    pub upgraded: Option<bool>,
}

/// A described group, as Kafka's `ConsumerGroupDescription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    pub state: String,
    /// The coordinator's host, port and id.
    pub coordinator: (String, i32, i32),
    pub partition_assignor: String,
    pub members: Vec<Member>,
    pub group_epoch: Option<i32>,
    pub target_assignment_epoch: Option<i32>,
}

/// The group calls that the pinned `AdminClient` lacks.
///
/// This is the seam for `list_groups` with states and types,
/// `describe_consumer_groups`, `delete_consumer_groups` and
/// `delete_consumer_group_offsets`. [`Unavailable`] implements it for this
/// build. Each method is called only after its guardrails pass, and a deleter
/// reports whether it can run before the command asks for confirmation.
pub trait Groups {
    /// `listGroups` for consumer groups with the given state and type
    /// filters, each empty for no filter.
    async fn list(&self, states: &[&str], types: &[&str])
    -> Result<Vec<ListedGroup>, CommandError>;

    /// `describeConsumerGroups` for one group.
    async fn describe(&self, group: &str) -> Result<GroupDescription, CommandError>;

    /// Whether [`Groups::delete`] and [`Groups::delete_offsets`] can run, or
    /// why not.
    fn can_delete(&self, what: &str) -> Result<(), CommandError>;

    /// `deleteConsumerGroups` for one group: its error code, or `None`.
    async fn delete(&self, group: &str) -> Result<Option<i16>, CommandError>;

    /// `deleteConsumerGroupOffsets`: the top-level error code, and the error
    /// code of each partition.
    async fn delete_offsets(
        &self,
        group: &str,
        partitions: &[Partition],
    ) -> Result<(Option<i16>, BTreeMap<Partition, Option<i16>>), CommandError>;
}

impl Groups for Unavailable {
    async fn list(
        &self,
        _states: &[&str],
        _types: &[&str],
    ) -> Result<Vec<ListedGroup>, CommandError> {
        Err(not_supported(
            "--list with --state or --type",
            "list_groups with ListGroupsOptions (group state and type)",
        ))
    }

    async fn describe(&self, _group: &str) -> Result<GroupDescription, CommandError> {
        Err(not_supported(
            "describing a consumer group",
            "describe_consumer_groups",
        ))
    }

    fn can_delete(&self, what: &str) -> Result<(), CommandError> {
        let method = if what == "--delete" {
            "delete_consumer_groups"
        } else {
            "delete_consumer_group_offsets"
        };
        Err(not_supported(what, method))
    }

    async fn delete(&self, _group: &str) -> Result<Option<i16>, CommandError> {
        self.can_delete("--delete").map(|()| None)
    }

    async fn delete_offsets(
        &self,
        _group: &str,
        _partitions: &[Partition],
    ) -> Result<(Option<i16>, BTreeMap<Partition, Option<i16>>), CommandError> {
        self.can_delete("--delete-offsets")
            .map(|()| (None, BTreeMap::new()))
    }
}

/// The lines of a rendered table, for [`CommandResult::human`], which writes
/// each with a newline.
fn lines(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    text.strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .map(ToOwned::to_owned)
        .collect()
}

/// A Kafka usage error: the message the JVM tool prints before its usage.
fn usage(message: impl Into<String>) -> CommandError {
    CommandError::Other(message.into())
}

impl ConsumerGroupsArgs {
    /// Runs the command. `verbose` is `--verbose`, which the CLI's global
    /// flag of the same name carries to this command.
    pub async fn run(self, verbose: bool) -> Result<CommandResult, CommandError> {
        Box::pin(self.run_with(verbose, &Unavailable, &Unavailable)).await
    }

    /// Runs the command against the given seams.
    pub async fn run_with(
        self,
        verbose: bool,
        groups: &impl Groups,
        offsets: &impl OffsetLookup,
    ) -> Result<CommandResult, CommandError> {
        self.check_args(verbose)?;
        if let Some(regex) = &self.validate_regex {
            return Ok(validate_regex(regex));
        }
        if self.list.is_some() {
            self.list_groups(groups).await
        } else if self.describe.is_some() {
            self.describe_groups(verbose, groups).await
        } else if self.delete.is_some() {
            self.delete_groups(groups).await
        } else if self.reset_offsets.is_some() {
            self.reset(offsets).await
        } else {
            self.delete_group_offsets(groups).await
        }
    }

    /// `ConsumerGroupCommandOptions.checkArgs`, for the rules that clap does
    /// not already enforce, with the JVM tool's messages.
    fn check_args(&self, verbose: bool) -> Result<(), CommandError> {
        let groups = !self.group.is_empty();
        let takes_group = |action: &str| {
            usage(format!(
                "Option [{action}] takes one of these options: [all-groups], [group]"
            ))
        };
        if verbose && self.describe.is_none() {
            return Err(usage(
                "Option(s) [verbose] are unavailable given other options on the command line",
            ));
        }
        if self.describe.is_some() {
            if !groups && self.all_groups.is_none() {
                return Err(takes_group("describe"));
            }
            let sub_actions = [
                self.members.is_some(),
                self.offsets.is_some(),
                self.state.is_some(),
            ];
            if sub_actions.iter().filter(|given| **given).count() > 1 {
                return Err(usage(
                    "Option [describe] takes at most one of these options: [members], [offsets], [state]",
                ));
            }
            if self.state.as_deref().is_some_and(|state| !state.is_empty()) {
                return Err(usage("Option [describe] does not take a value for [state]"));
            }
        }
        if self.delete.is_some() {
            if !groups && self.all_groups.is_none() {
                return Err(takes_group("delete"));
            }
            if !self.topic.is_empty() {
                return Err(usage(
                    "The consumer does not support topic-specific offset deletion from a consumer group.",
                ));
            }
        }
        if self.delete_offsets.is_some() && (!groups || self.topic.is_empty()) {
            return Err(usage(
                "Option [delete-offsets] takes the following options: [topic], [group]",
            ));
        }
        if self.reset_offsets.is_some() && !groups && self.all_groups.is_none() {
            return Err(takes_group("reset-offsets"));
        }
        if groups && self.all_groups.is_some() {
            return Err(usage(
                "Option \"[group]\" can't be used with option \"[all-groups]\"",
            ));
        }
        if groups && self.list.is_some() {
            return Err(usage(
                "Option \"[group]\" can't be used with option \"[list]\"",
            ));
        }
        if !self.topic.is_empty() && (self.list.is_some() || self.describe.is_some()) {
            let action = if self.list.is_some() {
                "list"
            } else {
                "describe"
            };
            return Err(usage(format!(
                "Option \"[topic]\" can't be used with option \"[{action}]\""
            )));
        }
        if self.confirm.dry_run && (self.list.is_some() || self.describe.is_some()) {
            return Err(usage(
                "--dry-run is only valid with --reset-offsets, --delete or --delete-offsets",
            ));
        }
        Ok(())
    }

    /// Every group that some broker coordinates, asking each broker in the
    /// cluster metadata, as Kafka's `listGroups` does.
    async fn all_groups(&self) -> Result<Vec<String>, CommandError> {
        let options = self.connection.options("consumer-groups").await?;
        let brokers = cluster_brokers(&self.connection, &options).await?;
        let answers = fan_out::bounded(
            brokers.into_values().collect(),
            BROKER_FAN_OUT,
            |endpoint| {
                let options = &options;
                async move {
                    let mut client = broker_client(&endpoint, options).await?;
                    Ok::<_, CommandError>(client.list_groups().await?)
                }
            },
        )
        .await;
        let mut groups = Vec::new();
        for answer in answers {
            for group in answer? {
                if !groups.contains(&group) {
                    groups.push(group);
                }
            }
        }
        Ok(groups)
    }

    /// The groups that the command acts on: `--group`, or every group under
    /// `--all-groups`.
    async fn target_groups(&self) -> Result<Vec<String>, CommandError> {
        if self.all_groups.is_some() {
            self.all_groups().await
        } else {
            Ok(self.group.clone())
        }
    }

    async fn list_groups(&self, source: &impl Groups) -> Result<CommandResult, CommandError> {
        let include_state = self.state.is_some();
        let include_type = self.group_type.is_some();
        if include_state || include_type {
            let states = match self.state.as_deref().filter(|states| !states.is_empty()) {
                Some(states) => group_states(states)?,
                None => Vec::new(),
            };
            let types = match self.group_type.as_deref().filter(|types| !types.is_empty()) {
                Some(types) => group_types(types)?,
                None => Vec::new(),
            };
            let listed = source.list(&states, &types).await?;
            let data = listed
                .iter()
                .map(|group| json!({"group": group.group_id, "type": group.group_type, "state": group.state}))
                .collect::<Vec<_>>();
            let table = list_table(&listed, include_type, include_state);
            return Ok(CommandResult::success(lines(&table), data));
        }
        let groups = self.all_groups().await?;
        let data = groups
            .iter()
            .map(|group| json!({"group": group, "type": null, "state": null}))
            .collect::<Vec<_>>();
        Ok(CommandResult::success(groups, data))
    }

    async fn describe_groups(
        &self,
        verbose: bool,
        described: &impl Groups,
    ) -> Result<CommandResult, CommandError> {
        let groups = self.target_groups().await?;
        if self.members.is_some() || self.state.is_some() {
            return describe_members_or_state(&groups, self.members.is_some(), verbose, described)
                .await;
        }
        let answers = fan_out::bounded(groups.clone(), GROUP_FAN_OUT, |group| async move {
            let mut client = self.connection.connect("consumer-groups").await?;
            Ok::<_, CommandError>(client.list_consumer_group_offsets(&group).await?)
        })
        .await;
        Ok(offsets_report(
            groups.into_iter().zip(answers).collect(),
            verbose,
        ))
    }

    async fn delete_groups(&self, deleter: &impl Groups) -> Result<CommandResult, CommandError> {
        let groups = self.target_groups().await?;
        if self.confirm.dry_run {
            let existing = self.all_groups().await?;
            let results = groups
                .iter()
                .map(|group| {
                    let error = (!existing.contains(group)).then_some(GROUP_ID_NOT_FOUND);
                    (group.clone(), error)
                })
                .collect::<Vec<_>>();
            return Ok(delete_report(&results)
                .with_notices(vec![DELETE_DRY_RUN_CAVEAT.to_owned()])
                .into_kafka_dry_run());
        }
        deleter.can_delete("--delete")?;
        confirm(
            self.confirm.yes,
            "krabka consumer-groups",
            Impact {
                summary: format!("delete {} consumer group(s)", groups.len()),
                resources: groups.clone(),
            },
        )
        .await?;
        let mut results = Vec::new();
        for group in &groups {
            results.push((group.clone(), deleter.delete(group).await?));
        }
        Ok(delete_report(&results))
    }

    /// The partitions of `--delete-offsets`: the named ones, and every
    /// partition of each topic named without partitions, or its describe
    /// error.
    async fn delete_offsets_scope(&self) -> Result<Vec<DeleteOffsetRow>, CommandError> {
        let mut rows = Vec::<DeleteOffsetRow>::new();
        let mut whole = Vec::new();
        for topic in &self.topic {
            match reset::topic_arg(topic)? {
                (name, Some(partitions)) => rows.extend(
                    partitions
                        .into_iter()
                        .map(|partition| ((name.clone(), Some(partition)), None)),
                ),
                (name, None) => whole.push(name),
            }
        }
        if !whole.is_empty() {
            let names = whole.iter().map(String::as_str).collect::<Vec<_>>();
            let mut client = self.connection.connect("consumer-groups").await?;
            let metadata = client.metadata(&names).await?;
            let assignments = client.describe_partition_assignments(&names).await?;
            for topic in metadata.topics {
                match topic.error {
                    Some(error) => rows.push((
                        (topic.name, None),
                        Some(KafkaException::for_code(error.code).to_java_string()),
                    )),
                    None => rows.extend(
                        assignments
                            .iter()
                            .filter(|assignment| assignment.topic == topic.name)
                            .map(|assignment| {
                                ((assignment.topic.clone(), Some(assignment.partition)), None)
                            }),
                    ),
                }
            }
        }
        rows.sort();
        rows.dedup();
        Ok(rows)
    }

    async fn delete_group_offsets(
        &self,
        deleter: &impl Groups,
    ) -> Result<CommandResult, CommandError> {
        let group = &self.group[0];
        let mut rows = self.delete_offsets_scope().await?;
        if self.confirm.dry_run {
            return Ok(delete_offsets_report(group, None, &rows)
                .with_notices(vec![DELETE_OFFSETS_DRY_RUN_CAVEAT.to_owned()])
                .into_kafka_dry_run());
        }
        deleter.can_delete("--delete-offsets")?;
        let partitions = rows
            .iter()
            .filter_map(|((topic, partition), _)| {
                partition.map(|partition| (topic.clone(), partition))
            })
            .collect::<Vec<_>>();
        confirm(
            self.confirm.yes,
            "krabka consumer-groups",
            Impact {
                summary: format!(
                    "delete the committed offsets of {} partition(s) from group {group}",
                    partitions.len()
                ),
                resources: partitions
                    .iter()
                    .map(|(topic, partition)| format!("{topic}-{partition}"))
                    .collect(),
            },
        )
        .await?;
        let (top_level, errors) = deleter.delete_offsets(group, &partitions).await?;
        for ((topic, partition), error) in &mut rows {
            if let Some(partition) = partition
                && let Some(Some(code)) = errors.get(&(topic.clone(), *partition))
            {
                *error = Some(KafkaException::for_code(*code).to_java_string());
            }
        }
        Ok(delete_offsets_report(group, top_level, &rows))
    }

    /// The scenario of one group.
    fn scenario(
        &self,
        group: &str,
        file: Option<&BTreeMap<String, BTreeMap<Partition, i64>>>,
    ) -> Result<Scenario, CommandError> {
        Ok(if let Some(offset) = self.to_offset {
            Scenario::ToOffset(offset)
        } else if self.to_earliest.is_some() {
            Scenario::ToEarliest
        } else if self.to_latest.is_some() {
            Scenario::ToLatest
        } else if let Some(shift) = self.shift_by {
            Scenario::ShiftBy(shift)
        } else if let Some(datetime) = &self.to_datetime {
            Scenario::ToDatetime(reset::parse_datetime(datetime)?)
        } else if let Some(duration) = &self.by_duration {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| {
                    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
                });
            Scenario::ByDuration(now.saturating_sub(reset::parse_duration(duration)?))
        } else if let Some(file) = file {
            Scenario::FromFile(file.get(group).cloned())
        } else {
            Scenario::ToCurrent
        })
    }

    async fn reset(&self, lookup: &impl OffsetLookup) -> Result<CommandResult, CommandError> {
        let groups = self.target_groups().await?;
        let file = match &self.from_file {
            Some(path) => {
                let csv = tokio::fs::read_to_string(path).await.map_err(|_| {
                    format!(
                        "java.io.IOException: Unable to read file {}",
                        path.display()
                    )
                })?;
                Some(reset::parse_reset_file(&csv, &self.group)?)
            }
            None => None,
        };
        if self.all_topics.is_none() && self.topic.is_empty() && file.is_none() {
            return Err(usage(
                "One of the reset scopes should be defined: --all-topics, --topic.",
            ));
        }
        let mut client = self.connection.connect("consumer-groups").await?;
        let mut stdout = Vec::new();
        let mut notices = Vec::new();
        let mut plans = Vec::<(String, Plan)>::new();
        let mut failures = Vec::new();
        for group in &groups {
            let scenario = self.scenario(group, file.as_ref())?;
            let committed = if self.all_topics.is_some()
                || matches!(scenario, Scenario::ShiftBy(_) | Scenario::ToCurrent)
            {
                client.list_consumer_group_offsets(group).await?
            } else {
                BTreeMap::new()
            };
            let partitions = if self.all_topics.is_some() {
                committed.keys().cloned().collect()
            } else if self.topic.is_empty() {
                Vec::new()
            } else {
                topic_partitions(&self.topic, &mut client).await?
            };
            let planned = reset::plan(&scenario, group, &partitions, &committed, lookup).await?;
            stdout.extend(planned.stdout);
            notices.extend(planned.notices);
            if self.execute.is_some() && !planned.plan.is_empty() {
                let offsets = planned.plan.iter().cloned().collect::<BTreeMap<_, _>>();
                let outcomes = client.alter_consumer_group_offsets(group, &offsets).await?;
                failures.extend(outcomes.into_iter().filter_map(|outcome| {
                    outcome
                        .error
                        .map(|error| (group.clone(), (outcome.topic, outcome.partition), error))
                }));
            }
            plans.push((group.clone(), planned.plan));
        }
        notices.push(RESET_ACTIVE_GROUP_CAVEAT.to_owned());
        let result = reset_report(
            plans,
            &failures,
            stdout,
            self.export.is_some(),
            self.group.len() == 1,
        )
        .with_notices(notices);
        Ok(if self.execute.is_some() {
            result
        } else {
            result.into_kafka_dry_run()
        })
    }
}

/// What a `--delete --dry-run` cannot know in this build.
const DELETE_DRY_RUN_CAVEAT: &str = "WARN: this build cannot tell whether a group has members, for which DeleteGroups answers NON_EMPTY_GROUP: that needs AdminClient::describe_consumer_groups";

/// What a `--delete-offsets --dry-run` cannot know in this build.
const DELETE_OFFSETS_DRY_RUN_CAVEAT: &str = "WARN: this build cannot tell whether the group is subscribed to these topics, for which OffsetDelete answers GROUP_SUBSCRIBED_TO_TOPIC: that needs AdminClient::describe_consumer_groups";

/// What a `--reset-offsets` cannot check in this build.
const RESET_ACTIVE_GROUP_CAVEAT: &str = "WARN: this build does not check that the groups are inactive, as Kafka does before a reset: that needs AdminClient::describe_consumer_groups";

/// `--describe --members` or `--describe --state`: each group's description
/// is the whole report, so the first failure ends the command, as it does in
/// the JVM tool.
async fn describe_members_or_state(
    groups: &[String],
    members: bool,
    verbose: bool,
    described: &impl Groups,
) -> Result<CommandResult, CommandError> {
    let what = if members {
        "--describe --members"
    } else {
        "--describe --state"
    };
    let mut sorted = groups.to_vec();
    sorted.sort();
    sorted.dedup();
    let mut text = String::new();
    let mut data = Vec::new();
    let mut notices = Vec::new();
    for group in &sorted {
        let description = described
            .describe(group)
            .await
            .map_err(|error| match error {
                CommandError::Unsupported(_) => not_supported(what, "describe_consumer_groups"),
                other => other,
            })?;
        if let Some(notice) = state_notice(group, &description.state) {
            notices.push(notice);
        }
        if description.state == "Dead" {
            continue;
        }
        if members {
            let rows = member_rows(group, &description);
            text.push_str(&members_table(&rows, verbose));
            data.push(json!({"group": group, "state": description.state, "members": rows.iter().map(member_row_json).collect::<Vec<_>>()}));
        } else {
            let row = state_row(group, &description);
            text.push_str(&state_table(&row, verbose));
            data.push(json!({
                "group": group,
                "coordinator": row.coordinator,
                "assignment_strategy": row.assignment_strategy,
                "state": row.state,
                "members": row.members,
            }));
        }
    }
    Ok(CommandResult::success(lines(&text), data).with_notices(notices))
}

/// What `shouldPrintMemberState` writes for a group in `state`.
fn state_notice(group: &str, state: &str) -> Option<String> {
    match state {
        "Dead" => Some(format!("Error: Consumer group '{group}' does not exist.")),
        "Empty" => Some(format!("Consumer group '{group}' has no active members.")),
        "PreparingRebalance" | "CompletingRebalance" | "Assigning" | "Reconciling" => {
            Some(format!("Warning: Consumer group '{group}' is rebalancing."))
        }
        _ => None,
    }
}

/// The `--describe --offsets` report from each group's committed offsets, in
/// group order. A group whose offsets could not be read is a notice and a
/// failure, and the other groups still print.
/// One group and its committed offsets, or why they could not be read.
type GroupOffsets = (String, Result<BTreeMap<Partition, i64>, CommandError>);

fn offsets_report(mut answers: Vec<GroupOffsets>, verbose: bool) -> CommandResult {
    answers.sort_by(|(left, _), (right, _)| left.cmp(right));
    answers.dedup_by(|(left, _), (right, _)| left == right);
    let mut text = String::new();
    let mut data = Vec::new();
    let mut notices = Vec::new();
    let mut failed = false;
    for (group, answer) in answers {
        match answer {
            Ok(committed) => {
                let rows = offset_rows(&group, &[], &committed, &BTreeMap::new());
                if rows.is_empty() {
                    notices.push(format!(
                        "Consumer group '{group}' has no committed offsets."
                    ));
                } else {
                    text.push_str(&offsets_table(&rows, verbose));
                }
                data.extend(rows.iter().map(offset_row_json));
            }
            Err(error) => {
                failed = true;
                notices.push(format!(
                    "Error: Executing consumer group command failed for group '{group}' due to {error}"
                ));
                data.push(json!({"group": group, "error": error.to_string()}));
            }
        }
    }
    notices.push(
        not_supported(
            "LOG-END-OFFSET and LAG (they need AdminClient::list_offsets) and CONSUMER-ID, HOST and CLIENT-ID",
            "describe_consumer_groups",
        )
        .to_string(),
    );
    CommandResult::rows(lines(&text), data, failed).with_notices(notices)
}

/// The partitions of `--topic` for `--reset-offsets`, as
/// `parseTopicPartitionsToReset` resolves them, checked to exist as
/// `checkAllTopicPartitionsValid` checks them.
async fn topic_partitions(
    topics: &[String],
    client: &mut krabka_client_admin::AdminClient,
) -> Result<Vec<Partition>, CommandError> {
    let mut named = Vec::new();
    let mut whole = Vec::new();
    for topic in topics {
        match reset::topic_arg(topic)? {
            (name, Some(partitions)) => {
                named.extend(
                    partitions
                        .into_iter()
                        .map(|partition| (name.clone(), partition)),
                );
            }
            (name, None) => whole.push(name),
        }
    }
    let mut topic_names = named
        .iter()
        .map(|(topic, _)| topic.as_str())
        .chain(whole.iter().map(String::as_str))
        .collect::<Vec<_>>();
    topic_names.sort_unstable();
    topic_names.dedup();
    let metadata = client.metadata(&topic_names).await?;
    if let Some(error) = metadata
        .topics
        .iter()
        .filter(|topic| whole.contains(&topic.name))
        .find_map(|topic| topic.error.as_ref())
    {
        return Err(KafkaException::for_code(error.code).message().into());
    }
    let existing = client
        .describe_partition_assignments(&topic_names)
        .await?
        .into_iter()
        .map(|assignment| (assignment.topic, assignment.partition))
        .collect::<Vec<_>>();
    let missing = named
        .iter()
        .filter(|partition| !existing.contains(partition))
        .map(|(topic, partition)| format!("{topic}-{partition}"))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!("The partitions \"{}\" do not exist", missing.join(",")).into());
    }
    let mut partitions = named;
    partitions.extend(
        existing
            .into_iter()
            .filter(|(topic, _)| whole.contains(topic)),
    );
    Ok(partitions)
}

/// The `--reset-offsets` report: the table, or the CSV of `--export`, of
/// every group's plan, in the order of the JVM tool's `HashMap`.
fn reset_report(
    plans: Vec<(String, Plan)>,
    failures: &[(String, Partition, krabka_client_admin::KafkaError)],
    stdout: Vec<String>,
    export: bool,
    single_group: bool,
) -> CommandResult {
    let capacity = default_capacity(plans.len());
    let plans = hash_order(plans, capacity, |(group, _)| string_hash(group));
    let data = plans
        .iter()
        .flat_map(|(group, plan)| {
            plan.iter().map(move |(partition, offset)| {
                let error = failures
                    .iter()
                    .find(|(failed_group, failed, _)| failed_group == group && failed == partition)
                    .map(|(.., error)| error);
                json!({
                    "group": group,
                    "topic": partition.0,
                    "partition": partition.1,
                    "new_offset": offset,
                    "error": kafka_error(error),
                })
            })
        })
        .collect::<Vec<_>>();
    let text = if let Some((.., error)) = failures.first() {
        format!(
            "\nError: Executing consumer group command failed due to {}\n",
            KafkaException::for_code(error.code).to_java_string()
        )
    } else if export {
        export_csv(&plans, single_group)
    } else {
        reset_table(&plans)
    };
    let mut human = stdout;
    human.extend(lines(&text));
    CommandResult::rows(human, data, !failures.is_empty())
}

/// `groupStatesFromString`: the states of `--list --state`, which must be
/// consumer-group states.
fn group_states(input: &str) -> Result<Vec<&'static str>, CommandError> {
    input
        .split(',')
        .map(|state| {
            CONSUMER_GROUP_STATES
                .iter()
                .copied()
                .find(|known| known.eq_ignore_ascii_case(state.trim()))
                .ok_or_else(|| {
                    usage(format!(
                        "Invalid state list '{input}'. Valid states are: {}",
                        CONSUMER_GROUP_STATES.join(", ")
                    ))
                })
        })
        .collect()
}

/// `consumerGroupTypesFromString`: the types of `--list --type`, `classic`
/// or `consumer`.
fn group_types(input: &str) -> Result<Vec<&'static str>, CommandError> {
    input
        .split(',')
        .map(|kind| {
            CONSUMER_GROUP_TYPES
                .iter()
                .copied()
                .find(|known| known.eq_ignore_ascii_case(kind.trim()))
                .ok_or_else(|| {
                    usage(format!(
                        "Invalid types list '{input}'. Valid types are: {}",
                        CONSUMER_GROUP_TYPES.join(", ")
                    ))
                })
        })
        .collect()
}

/// `--validate-regex`.
fn validate_regex(regex: &str) -> CommandResult {
    match Regex::new(regex) {
        Ok(_) => CommandResult::success(
            vec![format!("The regular expression `{regex}` is valid.")],
            json!({"regex": regex, "valid": true, "error": null}),
        ),
        Err(error) => {
            let text = error.to_string();
            let description = text
                .lines()
                .find_map(|line| line.strip_prefix("error: "))
                .unwrap_or(&text)
                .to_owned();
            CommandResult::success(
                vec![format!(
                    "The regular expression `{regex}` is invalid: {description}."
                )],
                json!({"regex": regex, "valid": false, "error": description}),
            )
        }
    }
}

/// The `--describe --offsets` rows of one group, as `collectGroupsOffsets`
/// builds them: the partitions of each member with an assignment, the members
/// with the most partitions first, then the committed partitions that no
/// member holds. Rows within a member are sorted by topic and partition.
#[must_use]
pub fn offset_rows(
    group: &str,
    members: &[Member],
    committed: &BTreeMap<Partition, i64>,
    log_end: &BTreeMap<Partition, i64>,
) -> Vec<OffsetRow> {
    let text = |member: Option<&Member>, field: fn(&Member) -> &String| {
        Some(member.map_or_else(|| MISSING.to_owned(), |member| field(member).clone()))
    };
    let row = |partition: &Partition, member: Option<&Member>| {
        let offset = committed.get(partition).copied();
        let end = log_end.get(partition).copied();
        OffsetRow {
            group: group.to_owned(),
            topic: Some(partition.0.clone()),
            partition: Some(partition.1),
            leader_epoch: None,
            offset,
            log_end_offset: end,
            lag: lag(offset, end),
            consumer_id: text(member, |member| &member.consumer_id),
            host: text(member, |member| &member.host),
            client_id: text(member, |member| &member.client_id),
        }
    };
    let mut assigned = members
        .iter()
        .filter(|member| !member.assignment.is_empty())
        .collect::<Vec<_>>();
    assigned.sort_by_key(|member| std::cmp::Reverse(member.assignment.len()));
    let mut rows = Vec::new();
    for member in &assigned {
        let mut partitions = member.assignment.clone();
        partitions.sort();
        rows.extend(
            partitions
                .iter()
                .map(|partition| row(partition, Some(member))),
        );
    }
    rows.extend(
        committed
            .keys()
            .filter(|partition| {
                !assigned
                    .iter()
                    .any(|member| member.assignment.contains(partition))
            })
            .map(|partition| row(partition, None)),
    );
    rows
}

fn offset_row_json(row: &OffsetRow) -> Value {
    json!({
        "group": row.group,
        "topic": row.topic,
        "partition": row.partition,
        "leader_epoch": row.leader_epoch,
        "current_offset": row.offset,
        "log_end_offset": row.log_end_offset,
        "lag": row.lag,
        "consumer_id": row.consumer_id,
        "host": row.host,
        "client_id": row.client_id,
    })
}

fn member_row_json(row: &MemberRow) -> Value {
    json!({
        "consumer_id": row.consumer_id,
        "group_instance_id": row.group_instance_id,
        "host": row.host,
        "client_id": row.client_id,
        "partitions": row.assignment.len(),
    })
}

/// The `--describe --members` rows of one group.
#[must_use]
pub fn member_rows(group: &str, description: &GroupDescription) -> Vec<MemberRow> {
    description
        .members
        .iter()
        .map(|member| MemberRow {
            group: group.to_owned(),
            consumer_id: member.consumer_id.clone(),
            group_instance_id: member.group_instance_id.clone().unwrap_or_default(),
            host: member.host.clone(),
            client_id: member.client_id.clone(),
            assignment: member.assignment.clone(),
            target_assignment: member.target_assignment.clone().unwrap_or_default(),
            current_epoch: member.epoch,
            target_epoch: description.target_assignment_epoch,
            upgraded: member.upgraded,
        })
        .collect()
}

/// The `--describe --state` row of one group.
#[must_use]
pub fn state_row(group: &str, description: &GroupDescription) -> StateRow {
    let (host, port, id) = &description.coordinator;
    StateRow {
        group: group.to_owned(),
        coordinator: format!("{host}:{port}  ({id})"),
        assignment_strategy: description.partition_assignor.clone(),
        state: description.state.clone(),
        members: description.members.len(),
        group_epoch: description.group_epoch,
        target_assignment_epoch: description.target_assignment_epoch,
    }
}

/// The `--delete` report of `deleteGroups`: each group and its error code, or
/// `None` for a deleted group.
fn delete_report(results: &[(String, Option<i16>)]) -> CommandResult {
    let quoted = |groups: &[&(String, Option<i16>)]| {
        groups
            .iter()
            .map(|(group, _)| format!("'{group}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let pick = |failed: bool| {
        let groups = results
            .iter()
            .filter(|(_, error)| error.is_some() == failed)
            .collect::<Vec<_>>();
        let capacity = default_capacity(groups.len());
        hash_order(groups, capacity, |(group, _)| string_hash(group))
    };
    let (failed, succeeded) = (pick(true), pick(false));
    let mut human = Vec::new();
    if failed.is_empty() {
        human.push(format!(
            "Deletion of requested consumer groups ({}) was successful.",
            quoted(&succeeded)
        ));
    } else {
        human.push(String::new());
        human.push("Error: Deletion of some consumer groups failed:".to_owned());
        for (group, error) in &failed {
            let exception = KafkaException::for_code(error.unwrap_or_default());
            human.push(format!(
                "* Group '{group}' could not be deleted due to: {}",
                exception.to_java_string()
            ));
        }
        if !succeeded.is_empty() {
            human.push(String::new());
            human.push(format!(
                "These consumer groups were deleted successfully: {}",
                quoted(&succeeded)
            ));
        }
    }
    let data = results
        .iter()
        .map(|(group, error)| {
            json!({
                "group": group,
                "error": error.map(|code| {
                    let exception = KafkaException::for_code(code);
                    json!({"code": code, "name": exception.name(), "message": exception.message()})
                }),
            })
        })
        .collect::<Vec<_>>();
    CommandResult::rows(human, data, !failed.is_empty())
}

/// The `--delete-offsets` report: the verdict line that the top-level error
/// selects, then the partition table.
fn delete_offsets_report(
    group: &str,
    top_level: Option<i16>,
    rows: &[DeleteOffsetRow],
) -> CommandResult {
    let partition_failed = rows.iter().any(|(_, error)| error.is_some());
    let verdict = match top_level.map(KafkaException::for_code) {
        None if !partition_failed => {
            format!("Request succeeded for deleting offsets from group {group}.")
        }
        None => {
            "\nError: Encountered some partition-level error, see the follow-up details.".to_owned()
        }
        Some(exception) => match exception.name() {
            "INVALID_GROUP_ID"
            | "GROUP_ID_NOT_FOUND"
            | "GROUP_AUTHORIZATION_FAILED"
            | "NON_EMPTY_GROUP" => format!("\nError: {}", exception.message()),
            "GROUP_SUBSCRIBED_TO_TOPIC"
            | "TOPIC_AUTHORIZATION_FAILED"
            | "UNKNOWN_TOPIC_OR_PARTITION" => {
                "\nError: Encountered some partition-level error, see the follow-up details."
                    .to_owned()
            }
            name => format!("\nError: Encountered some unknown error: {name}"),
        },
    };
    let mut text = format!("{verdict}\n");
    text.push_str(&delete_offsets_table(rows));
    let data = json!({
        "group": group,
        "error": top_level.map(|code| KafkaException::for_code(code).name()),
        "partitions": rows
            .iter()
            .map(|((topic, partition), error)| json!({"topic": topic, "partition": partition, "error": error}))
            .collect::<Vec<_>>(),
    });
    CommandResult::rows(lines(&text), data, top_level.is_some() || partition_failed)
}

#[cfg(test)]
mod tests;
