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
//! - `--describe` of several groups prints every group it could describe
//!   and names each failed group on stderr, where the JVM tool stops at the
//!   first failure.

mod render;
mod reset;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use clap::{ArgGroup, Args};
use krabka_client_admin::{
    AdminClient, ConsumerGroupDescription, DescribeGroupsOptions, DescribeTopicsOptions,
    KafkaError, OffsetSpec,
    groups::{GroupListing, GroupState, GroupType, ListGroupsError, ListGroupsOptions},
};
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
    compat::KafkaException,
    connection::ConnectionArgs,
    fan_out,
    get_offsets::{ListOffsets, OffsetLookup},
    jvm::{Table, hash_order, hash_set_order, string_hash, topic_partition_hash},
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
};

/// The most groups that `--describe` asks about at once, each on its own
/// connection to the group's coordinator.
pub const GROUP_FAN_OUT: usize = 8;

/// `GROUP_ID_NOT_FOUND`.
const GROUP_ID_NOT_FOUND: i16 = 69;

/// `NON_EMPTY_GROUP`.
const NON_EMPTY_GROUP: i16 = 68;

/// `UNKNOWN_SERVER_ERROR`.
const UNKNOWN_SERVER_ERROR: i16 = -1;

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

/// A described group, from the admin client's `ConsumerGroupDescription`.
fn group_description(description: ConsumerGroupDescription) -> GroupDescription {
    GroupDescription {
        state: description.group_state.as_str().to_owned(),
        coordinator: (
            description.coordinator.host,
            description.coordinator.port,
            description.coordinator.id,
        ),
        partition_assignor: description.partition_assignor,
        members: description
            .members
            .into_iter()
            .map(|member| Member {
                consumer_id: member.member_id,
                group_instance_id: member.group_instance_id,
                client_id: member.client_id,
                host: member.host,
                assignment: member.assignment.into_iter().collect(),
                target_assignment: member
                    .target_assignment
                    .map(|target| target.into_iter().collect()),
                epoch: member.member_epoch,
                upgraded: member.upgraded,
            })
            .collect(),
        group_epoch: description.group_epoch,
        target_assignment_epoch: description.target_assignment_epoch,
    }
}

/// `Throwable.toString()` of the exception of a per-group or per-partition
/// error: its class, and its message or the default message of its code.
fn exception_text(error: &KafkaError) -> String {
    let exception = KafkaException::for_code(error.code);
    match error.message.as_deref() {
        Some(message) if !message.is_empty() => format!("{}: {message}", exception.class()),
        _ => exception.to_java_string(),
    }
}

/// The listings in the order of the `HashMap<String, GroupListing>` that
/// Kafka's `ListGroupsResults` collects them in.
fn listing_order(listed: Vec<GroupListing>) -> Vec<GroupListing> {
    hash_order(listed, Table::Default, |group| string_hash(&group.group_id))
}

/// The exception of a broker that failed `ListGroups`, as Kafka's
/// `ListGroupsResults.addError` builds it.
fn list_failure(failure: &ListGroupsError) -> CommandError {
    let exception = KafkaException::for_code(failure.error.code);
    let node = format!(
        "{}:{} (id: {} rack: null isFenced: false)",
        failure.host, failure.port, failure.node_id
    );
    CommandError::Other(match failure.error.message.as_deref() {
        Some(message) if !message.is_empty() => format!(
            "{}: Error listing groups on {node}: {message}",
            exception.class()
        ),
        _ => format!("{}: Error listing groups on {node}", exception.class()),
    })
}

/// One group and its description, or why it could not be described.
type Described = (String, Result<GroupDescription, CommandError>);

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
        self.check_args(verbose)?;
        if let Some(regex) = &self.validate_regex {
            return Ok(validate_regex(regex));
        }
        let mut client = self.connection.connect("consumer-groups").await?;
        if self.list.is_some() {
            self.list_groups(&client).await
        } else if self.describe.is_some() {
            Box::pin(self.describe_groups(verbose, &client)).await
        } else if self.delete.is_some() {
            self.delete_groups(&client).await
        } else if self.reset_offsets.is_some() {
            Box::pin(self.reset(&mut client)).await
        } else {
            self.delete_group_offsets(&mut client).await
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

    /// Every consumer group that some broker coordinates, as Kafka's
    /// `listConsumerGroups` lists them with `ListGroupsOptions.forConsumerGroups`:
    /// the admin client asks every broker, and the groups come in the order
    /// of the admin client's `HashMap` of listings. A broker that fails fails
    /// the listing, as `ListGroupsResult.all()` does.
    async fn all_groups(client: &AdminClient) -> Result<Vec<String>, CommandError> {
        let listed = client
            .list_groups(&ListGroupsOptions::for_consumer_groups())
            .await?;
        Ok(
            listing_order(listed.all().map_err(|failure| list_failure(&failure))?)
                .into_iter()
                .map(|group| group.group_id)
                .collect(),
        )
    }

    /// The groups that the command acts on: `--group`, or every group under
    /// `--all-groups`, each once.
    async fn target_groups(&self, client: &AdminClient) -> Result<Vec<String>, CommandError> {
        let mut groups = if self.all_groups.is_some() {
            Self::all_groups(client).await?
        } else {
            self.group.clone()
        };
        let mut seen = BTreeSet::new();
        groups.retain(|group| seen.insert(group.clone()));
        Ok(groups)
    }

    async fn list_groups(&self, client: &AdminClient) -> Result<CommandResult, CommandError> {
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
            // `forConsumerGroups().inGroupStates(states).withTypes(types)`:
            // an empty type set replaces the consumer types, and the
            // protocol types stay.
            let options = ListGroupsOptions {
                group_states: states
                    .iter()
                    .map(|state| GroupState::parse(state))
                    .collect(),
                types: types.iter().map(|kind| GroupType::parse(kind)).collect(),
                ..ListGroupsOptions::for_consumer_groups()
            };
            let listed = client.list_groups(&options).await?;
            let listed = listing_order(listed.all().map_err(|failure| list_failure(&failure))?)
                .into_iter()
                .map(|listing| ListedGroup {
                    group_id: listing.group_id,
                    group_type: listing
                        .group_type
                        .unwrap_or(GroupType::Unknown)
                        .as_str()
                        .to_owned(),
                    state: listing
                        .group_state
                        .unwrap_or(GroupState::Unknown)
                        .as_str()
                        .to_owned(),
                })
                .collect::<Vec<_>>();
            let data = listed
                .iter()
                .map(|group| json!({"group": group.group_id, "type": group.group_type, "state": group.state}))
                .collect::<Vec<_>>();
            let table = list_table(&listed, include_type, include_state);
            return Ok(CommandResult::success(lines(&table), data));
        }
        let groups = Self::all_groups(client).await?;
        let data = groups
            .iter()
            .map(|group| json!({"group": group, "type": null, "state": null}))
            .collect::<Vec<_>>();
        Ok(CommandResult::success(groups, data))
    }

    async fn describe_groups(
        &self,
        verbose: bool,
        client: &AdminClient,
    ) -> Result<CommandResult, CommandError> {
        let mut groups = self.target_groups(client).await?;
        groups.sort();
        let described = describe(client, &groups).await;
        if self.members.is_some() || self.state.is_some() {
            return Ok(members_or_state_report(
                described,
                self.members.is_some(),
                verbose,
            ));
        }
        let answers = fan_out::bounded(
            described,
            GROUP_FAN_OUT,
            |(group, description)| async move {
                let answer = match description {
                    Ok(description) => self
                        .group_offsets(client, &group, &description)
                        .await
                        .map(|rows| (description.state, rows)),
                    Err(error) => Err(error),
                };
                (group, answer)
            },
        )
        .await;
        Ok(offsets_report(answers, verbose))
    }

    /// The `--describe --offsets` rows of one described group: its committed
    /// offsets, read on a connection of its own, and the log-end offset of
    /// each partition that a member holds or that has a committed offset.
    async fn group_offsets(
        &self,
        client: &AdminClient,
        group: &str,
        description: &GroupDescription,
    ) -> Result<Vec<OffsetRow>, CommandError> {
        let mut connection = self.connection.connect("consumer-groups").await?;
        let committed = connection.list_consumer_group_offsets(group).await?;
        let mut partitions = committed.keys().cloned().collect::<BTreeSet<_>>();
        for member in &description.members {
            partitions.extend(member.assignment.iter().cloned());
        }
        let partitions = partitions.into_iter().collect::<Vec<_>>();
        let log_end = log_end_offsets(client, &partitions).await?;
        Ok(offset_rows(
            group,
            &description.members,
            &committed,
            &log_end,
        ))
    }

    async fn delete_groups(&self, client: &AdminClient) -> Result<CommandResult, CommandError> {
        let groups = self.target_groups(client).await?;
        let names = groups.iter().map(String::as_str).collect::<Vec<_>>();
        if self.confirm.dry_run {
            let mut described = describe_raw(client, &names).await;
            let results = groups
                .iter()
                .map(|group| {
                    let error = match described.remove(group) {
                        Some(Ok(description)) => match description.group_state {
                            GroupState::Dead => Some(error_of(GROUP_ID_NOT_FOUND)),
                            GroupState::Empty => None,
                            _ => Some(error_of(NON_EMPTY_GROUP)),
                        },
                        Some(Err(error)) => Some(error),
                        None => None,
                    };
                    (group.clone(), error)
                })
                .collect::<Vec<_>>();
            return Ok(delete_report(&results).into_kafka_dry_run());
        }
        confirm(
            self.confirm.yes,
            "krabka consumer-groups",
            Impact {
                summary: format!("delete {} consumer group(s)", groups.len()),
                resources: groups.clone(),
            },
        )
        .await?;
        let mut deleted = if names.is_empty() {
            BTreeMap::new()
        } else {
            client.delete_consumer_groups(&names).await
        };
        let results = groups
            .iter()
            .map(|group| {
                let error = match deleted.remove(group) {
                    Some(Ok(())) => None,
                    Some(Err(error)) => Some(error),
                    None => Some(KafkaError {
                        code: UNKNOWN_SERVER_ERROR,
                        name: KafkaException::for_code(UNKNOWN_SERVER_ERROR).name(),
                        message: Some(format!("Group {group} was not included in the response")),
                    }),
                };
                (group.clone(), error)
            })
            .collect::<Vec<_>>();
        Ok(delete_report(&results))
    }

    /// The partitions of `--delete-offsets`: the named ones, and every
    /// partition of each topic named without partitions, or its describe
    /// error.
    async fn delete_offsets_scope(
        &self,
        client: &AdminClient,
    ) -> Result<Vec<DeleteOffsetRow>, CommandError> {
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
        let names = whole.iter().map(String::as_str).collect::<Vec<_>>();
        for (topic, layout) in topic_layout(client, &names).await {
            match layout {
                Err(error) => rows.push(((topic, None), Some(exception_text(&error)))),
                Ok(partitions) => rows.extend(
                    partitions
                        .into_iter()
                        .map(|(partition, _)| ((topic.clone(), Some(partition)), None)),
                ),
            }
        }
        rows.sort();
        rows.dedup();
        Ok(rows)
    }

    async fn delete_group_offsets(
        &self,
        client: &mut AdminClient,
    ) -> Result<CommandResult, CommandError> {
        let group = self.group[0].clone();
        let mut rows = self.delete_offsets_scope(client).await?;
        if self.confirm.dry_run {
            let (top_level, notices) = match describe_raw(client, &[&group]).await.remove(&group) {
                Some(Ok(description)) => match description.group_state {
                    GroupState::Dead => (Some(GROUP_ID_NOT_FOUND), Vec::new()),
                    GroupState::Empty => (None, Vec::new()),
                    _ => (None, vec![DELETE_OFFSETS_DRY_RUN_CAVEAT.to_owned()]),
                },
                Some(Err(error)) => (Some(error.code), Vec::new()),
                None => (None, Vec::new()),
            };
            return Ok(delete_offsets_report(&group, top_level, &rows)
                .with_notices(notices)
                .into_kafka_dry_run());
        }
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
        let top_level = match client
            .delete_consumer_group_offsets(&group, &partitions)
            .await
        {
            Ok(outcomes) => apply_offset_deletions(&mut rows, &partitions, &outcomes),
            Err(krabka_client_admin::AdminError::Broker { code, .. }) => {
                let text = KafkaException::for_code(code).to_java_string();
                for ((_, partition), error) in &mut rows {
                    if partition.is_some() {
                        *error = Some(text.clone());
                    }
                }
                Some(code)
            }
            Err(other) => return Err(other.into()),
        };
        Ok(delete_offsets_report(&group, top_level, &rows))
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

    async fn reset(&self, client: &mut AdminClient) -> Result<CommandResult, CommandError> {
        let groups = self.target_groups(client).await?;
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
        let names = groups.iter().map(String::as_str).collect::<Vec<_>>();
        let mut described = describe_raw(client, &names).await;
        let mut stdout = Vec::new();
        let mut notices = Vec::new();
        let mut plans = Vec::<(String, Plan)>::new();
        let mut failure = None;
        for group in hash_order(groups.clone(), Table::Default, |group| string_hash(group)) {
            match described.remove(&group) {
                Some(Ok(description))
                    if !matches!(
                        description.group_state,
                        GroupState::Empty | GroupState::Dead
                    ) =>
                {
                    stdout.push(String::new());
                    stdout.push(format!(
                        "Error: Assignments can only be reset if the group '{group}' is inactive, but the current state is {}.",
                        description.group_state
                    ));
                    plans.push((group, Vec::new()));
                    continue;
                }
                Some(Err(error)) if error.code != GROUP_ID_NOT_FOUND => {
                    return Err(exception_text(&error).into());
                }
                _ => {}
            }
            let scenario = self.scenario(&group, file.as_ref())?;
            let committed = if self.all_topics.is_some()
                || matches!(scenario, Scenario::ShiftBy(_) | Scenario::ToCurrent)
            {
                client.list_consumer_group_offsets(&group).await?
            } else {
                BTreeMap::new()
            };
            let partitions = if self.all_topics.is_some() {
                committed.keys().cloned().collect()
            } else if self.topic.is_empty() {
                Vec::new()
            } else {
                topic_partitions(&self.topic, client).await?
            };
            check_partitions_valid(client, &partitions).await?;
            let planned = reset::plan(
                &scenario,
                &group,
                &partitions,
                &committed,
                &ListOffsets::new(client),
            )
            .await?;
            stdout.extend(planned.stdout);
            notices.extend(planned.notices);
            if self.execute.is_some() && !planned.plan.is_empty() {
                let offsets = planned.plan.iter().cloned().collect::<BTreeMap<_, _>>();
                let outcomes = client
                    .alter_consumer_group_offsets(&group, &offsets)
                    .await?;
                failure = outcomes.into_iter().find_map(|outcome| outcome.error);
                if failure.is_some() {
                    break;
                }
            }
            plans.push((group, planned.plan));
        }
        let result = reset_report(
            plans,
            failure.as_ref(),
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

/// What a `--delete-offsets --dry-run` cannot know.
const DELETE_OFFSETS_DRY_RUN_CAVEAT: &str = "WARN: the group has members, and a dry run does not check whether it is subscribed to these topics, for which OffsetDelete answers GROUP_SUBSCRIBED_TO_TOPIC";

/// An error of `code` with its default message.
fn error_of(code: i16) -> KafkaError {
    KafkaError {
        code,
        name: KafkaException::for_code(code).name(),
        message: None,
    }
}

/// `describeConsumerGroups` for `groups`, or nothing for no group.
async fn describe_raw(
    client: &AdminClient,
    groups: &[&str],
) -> BTreeMap<String, Result<ConsumerGroupDescription, KafkaError>> {
    if groups.is_empty() {
        return BTreeMap::new();
    }
    client
        .describe_consumer_groups(groups, DescribeGroupsOptions::default())
        .await
}

/// Each of `groups` and its description, or why it could not be described,
/// in the order of `groups`.
async fn describe(client: &AdminClient, groups: &[String]) -> Vec<Described> {
    let names = groups.iter().map(String::as_str).collect::<Vec<_>>();
    let mut described = describe_raw(client, &names).await;
    groups
        .iter()
        .map(|group| {
            let answer = match described.remove(group) {
                Some(Ok(description)) => Ok(group_description(description)),
                Some(Err(error)) => Err(CommandError::Other(exception_text(&error))),
                None => Err(CommandError::Other(format!(
                    "the coordinator did not describe group {group}"
                ))),
            };
            (group.clone(), answer)
        })
        .collect()
}

/// The partitions of each of `topics` and whether each has a leader, or the
/// error of the topic, as `describeTopics` reports them.
type TopicLayout = BTreeMap<String, Result<Vec<(i32, bool)>, KafkaError>>;

async fn topic_layout(client: &AdminClient, topics: &[&str]) -> TopicLayout {
    if topics.is_empty() {
        return BTreeMap::new();
    }
    client
        .describe_topics(topics, DescribeTopicsOptions::default())
        .await
        .into_iter()
        .map(|(topic, description)| {
            let layout = description.map(|description| {
                description
                    .partitions
                    .iter()
                    .map(|partition| (partition.partition, partition.leader.is_some()))
                    .collect()
            });
            (topic, layout)
        })
        .collect()
}

/// The distinct topics of `partitions`.
fn topics_of(partitions: &[Partition]) -> Vec<&str> {
    let mut topics = partitions
        .iter()
        .map(|(topic, _)| topic.as_str())
        .collect::<Vec<_>>();
    topics.sort_unstable();
    topics.dedup();
    topics
}

/// The log-end offset of each of `partitions`, as `describePartitions` reads
/// them: a partition without a leader has none, and a topic that cannot be
/// described or a failed lookup fails the group, as
/// `filterNoneLeaderPartitions` and `getLogEndOffsets` do.
async fn log_end_offsets(
    client: &AdminClient,
    partitions: &[Partition],
) -> Result<BTreeMap<Partition, i64>, CommandError> {
    if partitions.is_empty() {
        return Ok(BTreeMap::new());
    }
    let layout = topic_layout(client, &topics_of(partitions)).await;
    if let Some(error) = layout.values().find_map(|layout| layout.as_ref().err()) {
        return Err(exception_text(error).into());
    }
    let led = partitions
        .iter()
        .filter(|(topic, partition)| {
            layout.get(topic).is_some_and(|layout| {
                layout
                    .as_ref()
                    .is_ok_and(|partitions| partitions.contains(&(*partition, true)))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if led.is_empty() {
        return Ok(BTreeMap::new());
    }
    ListOffsets::new(client)
        .offsets(&led, OffsetSpec::Latest)
        .await?
        .into_iter()
        .map(|(partition, answer)| {
            answer
                .map(|offset| (partition, offset))
                .map_err(|code| KafkaException::for_code(code).to_java_string().into())
        })
        .collect()
}

/// `checkAllTopicPartitionsValid`: every partition exists and has a leader.
async fn check_partitions_valid(
    client: &AdminClient,
    partitions: &[Partition],
) -> Result<(), CommandError> {
    if partitions.is_empty() {
        return Ok(());
    }
    let layout = topic_layout(client, &topics_of(partitions)).await;
    let find = |(topic, partition): &Partition| {
        layout
            .get(topic)
            .and_then(|layout| layout.as_ref().ok())
            .and_then(|partitions| partitions.iter().find(|(id, _)| id == partition))
            .map(|(_, leader)| *leader)
    };
    let names = |wanted: &dyn Fn(Option<bool>) -> bool| {
        partitions
            .iter()
            .filter(|partition| wanted(find(partition)))
            .map(|(topic, partition)| format!("{topic}-{partition}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let missing = names(&|found| found.is_none());
    if !missing.is_empty() {
        return Err(format!("The partitions \"{missing}\" do not exist").into());
    }
    let leaderless = names(&|found| found == Some(false));
    if !leaderless.is_empty() {
        return Err(format!("The partitions \"{leaderless}\" have no leader").into());
    }
    Ok(())
}

/// Sets the status of each partition row from an `OffsetDelete` answer, and
/// returns the top-level error that `DeleteConsumerGroupOffsetsResult.all()`
/// fails with: the error of the first failed partition in the order of the
/// request's `HashSet`, or `UNKNOWN_SERVER_ERROR` for a partition the answer
/// leaves out.
fn apply_offset_deletions(
    rows: &mut [DeleteOffsetRow],
    partitions: &[Partition],
    outcomes: &[krabka_client_admin::ConsumerGroupOffsetOutcome],
) -> Option<i16> {
    let answered = outcomes
        .iter()
        .map(|outcome| {
            (
                (outcome.topic.clone(), outcome.partition),
                outcome.error.as_ref().map(|error| error.code),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let failure = |partition: &Partition| match answered.get(partition) {
        None => Some((
            UNKNOWN_SERVER_ERROR,
            format!(
                "java.lang.IllegalArgumentException: Offset deletion result for partition \"{}-{}\" was not included in the response",
                partition.0, partition.1
            ),
        )),
        Some(None) => None,
        Some(Some(code)) => Some((*code, KafkaException::for_code(*code).to_java_string())),
    };
    for ((topic, partition), error) in rows.iter_mut() {
        if let Some(partition) = partition
            && let Some((_, text)) = failure(&(topic.clone(), *partition))
        {
            *error = Some(text);
        }
    }
    hash_set_order(partitions.to_vec(), |(topic, partition)| {
        topic_partition_hash(topic, *partition)
    })
    .iter()
    .find_map(|partition| failure(partition).map(|(code, _)| code))
}

/// What `shouldPrintMemberState` prints for a group in `state` with `rows`
/// data rows: text for stdout, lines for stderr, and whether the group's
/// table follows. A state that a consumer group cannot be in fails the group,
/// as the JVM tool's `KafkaException` does.
fn member_state(
    group: &str,
    state: &str,
    rows: usize,
) -> Result<(String, Vec<String>, bool), CommandError> {
    Ok(match state {
        "Dead" => (
            format!("\nError: Consumer group '{group}' does not exist.\n"),
            Vec::new(),
            false,
        ),
        "Empty" => (
            String::new(),
            vec![
                String::new(),
                format!("Consumer group '{group}' has no active members."),
            ],
            rows > 0,
        ),
        "PreparingRebalance" | "CompletingRebalance" | "Assigning" | "Reconciling" => (
            String::new(),
            vec![
                String::new(),
                format!("Warning: Consumer group '{group}' is rebalancing."),
            ],
            rows > 0,
        ),
        "Stable" => (String::new(), Vec::new(), rows > 0),
        other => {
            return Err(CommandError::Other(format!(
                "org.apache.kafka.common.KafkaException: Expected a valid consumer group state, but found '{other}'."
            )));
        }
    })
}

/// The notice of a group that could not be described.
fn group_failure(group: &str, error: &CommandError) -> String {
    format!("Error: Executing consumer group command failed for group '{group}' due to {error}")
}

/// `--describe --members` or `--describe --state` of each described group,
/// in group order. A group that could not be described is a notice and a
/// failure, and the other groups still print.
fn members_or_state_report(
    described: Vec<Described>,
    members: bool,
    verbose: bool,
) -> CommandResult {
    let mut text = String::new();
    let mut data = Vec::new();
    let mut notices = Vec::new();
    let mut failed = false;
    for (group, answer) in described {
        let rows = |description: &GroupDescription| {
            if members {
                description.members.len()
            } else {
                1
            }
        };
        let answer = answer.and_then(|description| {
            member_state(&group, &description.state, rows(&description))
                .map(|state| (description, state))
        });
        let (description, (stdout, stderr, print)) = match answer {
            Ok(answer) => answer,
            Err(error) => {
                failed = true;
                notices.push(group_failure(&group, &error));
                data.push(json!({"group": group, "error": error.to_string()}));
                continue;
            }
        };
        text.push_str(&stdout);
        notices.extend(stderr);
        if members {
            let rows = member_rows(&group, &description);
            text.push_str(&members_table(&rows, verbose));
            data.push(json!({"group": group, "state": description.state, "members": rows.iter().map(member_row_json).collect::<Vec<_>>()}));
        } else {
            let row = state_row(&group, &description);
            if print {
                text.push_str(&state_table(&row, verbose));
            }
            data.push(json!({
                "group": group,
                "coordinator": row.coordinator,
                "assignment_strategy": row.assignment_strategy,
                "state": row.state,
                "members": row.members,
            }));
        }
    }
    CommandResult::rows(lines(&text), data, failed).with_notices(notices)
}

/// One group and its state and `--describe --offsets` rows, or why they
/// could not be read.
type GroupOffsets = (String, Result<(String, Vec<OffsetRow>), CommandError>);

/// The `--describe --offsets` report of each group, in group order. A group
/// whose offsets could not be read is a notice and a failure, and the other
/// groups still print.
fn offsets_report(answers: Vec<GroupOffsets>, verbose: bool) -> CommandResult {
    let mut text = String::new();
    let mut data = Vec::new();
    let mut notices = Vec::new();
    let mut failed = false;
    for (group, answer) in answers {
        let answer = answer.and_then(|(state, rows)| {
            member_state(&group, &state, rows.len()).map(|printed| (rows, printed))
        });
        match answer {
            Ok((rows, (stdout, stderr, print))) => {
                text.push_str(&stdout);
                notices.extend(stderr);
                if print {
                    text.push_str(&offsets_table(&rows, verbose));
                }
                data.extend(rows.iter().map(offset_row_json));
            }
            Err(error) => {
                failed = true;
                notices.push(group_failure(&group, &error));
                data.push(json!({"group": group, "error": error.to_string()}));
            }
        }
    }
    CommandResult::rows(lines(&text), data, failed).with_notices(notices)
}

/// The partitions of `--topic` for `--reset-offsets`, as
/// `parseTopicPartitionsToReset` resolves them: the named partitions, then
/// every partition of each topic named without partitions.
async fn topic_partitions(
    topics: &[String],
    client: &AdminClient,
) -> Result<Vec<Partition>, CommandError> {
    let mut partitions = Vec::new();
    let mut whole = Vec::new();
    for topic in topics {
        match reset::topic_arg(topic)? {
            (name, Some(named)) => {
                partitions.extend(named.into_iter().map(|partition| (name.clone(), partition)));
            }
            (name, None) => whole.push(name),
        }
    }
    let names = whole.iter().map(String::as_str).collect::<Vec<_>>();
    for (topic, layout) in topic_layout(client, &names).await {
        match layout {
            Ok(layout) => partitions.extend(
                layout
                    .into_iter()
                    .map(|(partition, _)| (topic.clone(), partition)),
            ),
            Err(error) => {
                return Err(error
                    .message
                    .unwrap_or_else(|| KafkaException::for_code(error.code).message().to_owned())
                    .into());
            }
        }
    }
    Ok(partitions)
}

/// The `--reset-offsets` report: the table, or the CSV of `--export`, of
/// every group's plan, in the order of the JVM tool's `HashMap`.
fn reset_report(
    plans: Vec<(String, Plan)>,
    failure: Option<&KafkaError>,
    stdout: Vec<String>,
    export: bool,
    single_group: bool,
) -> CommandResult {
    let plans = hash_order(plans, Table::Default, |(group, _)| string_hash(group));
    let data = plans
        .iter()
        .flat_map(|(group, plan)| {
            plan.iter().map(move |(partition, offset)| {
                json!({
                    "group": group,
                    "topic": partition.0,
                    "partition": partition.1,
                    "new_offset": offset,
                })
            })
        })
        .collect::<Vec<_>>();
    let text = if let Some(error) = failure {
        format!(
            "\nError: Executing consumer group command failed due to {}\n",
            error
                .message
                .as_deref()
                .filter(|message| !message.is_empty())
                .unwrap_or(KafkaException::for_code(error.code).message())
        )
    } else if export {
        export_csv(&plans, single_group)
    } else {
        reset_table(&plans)
    };
    let mut human = stdout;
    human.extend(lines(&text));
    CommandResult::rows(
        human,
        json!({"plans": data, "error": kafka_error(failure)}),
        failure.is_some(),
    )
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

/// The `--delete` report of `deleteGroups`: each group and its error, or
/// `None` for a deleted group.
fn delete_report(results: &[(String, Option<KafkaError>)]) -> CommandResult {
    let quoted = |groups: &[&(String, Option<KafkaError>)]| {
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
        hash_order(groups, Table::Default, |(group, _)| string_hash(group))
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
            if let Some(error) = error {
                human.push(format!(
                    "* Group '{group}' could not be deleted due to: {}",
                    exception_text(error)
                ));
            }
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
                "error": error.as_ref().map(|error| {
                    let exception = KafkaException::for_code(error.code);
                    json!({"code": error.code, "name": exception.name(), "message": error.message.as_deref().unwrap_or(exception.message())})
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
