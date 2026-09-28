//! `krabka transactions`, the counterpart of `kafka-transactions`.
//!
//! The subcommands, their flags, their checks and their tables are those of
//! `TransactionsCommand` in Kafka 4.3.1. `--bootstrap-server` and
//! `--command-config` come before the subcommand, as they do there:
//!
//! ```text
//! krabka transactions --bootstrap-server host:9092 describe --transactional-id t
//! ```
//!
//! `describe` and `forceTerminateTransaction` run on the pinned
//! krabka-client-rs. `list`, `describe-producers`, `abort` and `find-hanging`
//! need admin calls that the pinned client does not have. They check their
//! flags as Kafka does and then fail with a "not supported by this build"
//! error that names the missing call.

use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use krabka_client_admin::TransactionDescription;
use krabka_units::convert::TimeExt as _;
use serde_json::{Value, json};

use crate::{
    connection::ConnectionArgs,
    kafka_errors::admin_error,
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, confirm},
};

/// The value that Kafka's tables print for a value that is not known.
const NONE: &str = "None";

/// The columns of `describe`, in Kafka's order.
const DESCRIBE_HEADERS: [&str; 9] = [
    "CoordinatorId",
    "TransactionalId",
    "ProducerId",
    "ProducerEpoch",
    "TransactionState",
    "TransactionTimeoutMs",
    "CurrentTransactionStartTimeMs",
    "TransactionDurationMs",
    "TopicPartitions",
];

/// The state names of Kafka's `TransactionState`. Any other name prints as
/// `Unknown`, as `TransactionState.parse` maps it.
const TRANSACTION_STATES: [&str; 7] = [
    "Ongoing",
    "PrepareAbort",
    "PrepareCommit",
    "CompleteAbort",
    "CompleteCommit",
    "Empty",
    "PrepareEpochFence",
];

#[derive(Debug, Args)]
pub struct TransactionsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[command(subcommand)]
    command: TransactionsCommand,
}

#[derive(Debug, Subcommand)]
enum TransactionsCommand {
    /// List transactions.
    List(ListArgs),
    /// Describe the state of an active transactional-id.
    Describe(DescribeArgs),
    /// Describe the states of active producers for a topic partition.
    DescribeProducers(DescribeProducersArgs),
    /// Abort a hanging transaction (requires administrative privileges).
    Abort(AbortArgs),
    /// Find hanging transactions.
    FindHanging(FindHangingArgs),
    /// Force abort an ongoing transaction on transactionalId (requires
    /// administrative privileges).
    #[command(name = "forceTerminateTransaction", alias = "force-terminate")]
    ForceTerminateTransaction(ForceTerminateArgs),
}

#[derive(Debug, Args)]
struct ListArgs {
    /// Duration (in millis) to filter by: if < 0, all transactions will be
    /// returned; otherwise, only transactions running longer than this
    /// duration will be returned.
    #[arg(long, allow_negative_numbers = true)]
    duration_filter: Option<i64>,
    /// Transactional id regular expression pattern to filter by.
    #[arg(long)]
    transactional_id_pattern: Option<String>,
}

#[derive(Debug, Args)]
struct DescribeArgs {
    /// Transactional id. Repeat the flag to describe more than one.
    #[arg(long, required = true)]
    transactional_id: Vec<String>,
}

#[derive(Debug, Args)]
struct DescribeProducersArgs {
    /// Optional broker id to describe the producer state on a specific
    /// replica.
    #[arg(long, allow_negative_numbers = true)]
    broker_id: Option<i32>,
    /// Topic name.
    #[arg(long)]
    topic: String,
    /// Partition number.
    #[arg(long, allow_negative_numbers = true)]
    partition: i32,
}

#[derive(Debug, Args)]
struct AbortArgs {
    /// Topic name.
    #[arg(long)]
    topic: String,
    /// Partition number.
    #[arg(long, allow_negative_numbers = true)]
    partition: i32,
    /// Start offset of the transaction to abort (brokers on 3.0 and above).
    #[arg(long, allow_negative_numbers = true)]
    start_offset: Option<i64>,
    /// Producer id (brokers older than 3.0).
    #[arg(long, allow_negative_numbers = true)]
    producer_id: Option<i64>,
    /// Producer epoch (brokers older than 3.0).
    #[arg(long, allow_negative_numbers = true)]
    producer_epoch: Option<i16>,
    /// Coordinator epoch (brokers older than 3.0).
    #[arg(long, allow_negative_numbers = true)]
    coordinator_epoch: Option<i32>,
}

#[derive(Debug, Args)]
struct FindHangingArgs {
    /// Broker id to search for hanging transactions.
    #[arg(long, allow_negative_numbers = true)]
    broker_id: Option<i32>,
    /// Maximum transaction timeout in minutes to limit the scope of the
    /// search.
    #[arg(long, default_value_t = 15, allow_negative_numbers = true)]
    max_transaction_timeout: i32,
    /// Topic name to limit search to (required if --partition is specified).
    #[arg(long)]
    topic: Option<String>,
    /// Partition number.
    #[arg(long, allow_negative_numbers = true)]
    partition: Option<i32>,
}

#[derive(Debug, Args)]
struct ForceTerminateArgs {
    /// Transactional id.
    #[arg(long = "transactionalId", visible_alias = "transactional-id")]
    transactional_id: String,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// The transaction that `abort` identifies, as Kafka's
/// `AbortTransactionCommand.execute` reads it from the flags.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AbortTarget {
    /// The open transaction that starts at this offset, which Kafka resolves
    /// through `DescribeProducers`.
    StartOffset(i64),
    /// An explicit producer generation, for brokers older than 3.0.
    Producer {
        producer_id: i64,
        producer_epoch: i16,
        coordinator_epoch: i32,
    },
}

impl TransactionsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        match self.command {
            TransactionsCommand::List(args) => Err(args.unsupported()),
            TransactionsCommand::DescribeProducers(args) => Err(args.unsupported()),
            TransactionsCommand::Abort(args) => Err(args.unsupported()),
            TransactionsCommand::FindHanging(args) => Err(args.unsupported()),
            TransactionsCommand::Describe(args) => {
                let client = self.connection.connect("transactions").await?;
                let mut descriptions = Vec::with_capacity(args.transactional_id.len());
                for id in &args.transactional_id {
                    let description = client.describe_transaction(id).await.map_err(|error| {
                        failure(
                            &format!(
                                "Failed to describe transaction state of transactional-id `{id}`"
                            ),
                            &admin_error(error),
                        )
                    })?;
                    descriptions.push(description);
                }
                Ok(described(&descriptions, now_ms()))
            }
            TransactionsCommand::ForceTerminateTransaction(args) => {
                let id = args.transactional_id;
                // Kafka prints nothing when the call succeeds.
                let report = CommandResult::success(Vec::new(), json!({"transactional_id": id}));
                if args.confirm.dry_run {
                    self.connection.connect("transactions").await?;
                    return Ok(report.into_dry_run());
                }
                confirm(
                    args.confirm.yes,
                    "krabka transactions",
                    Impact {
                        summary: "force-terminate the transaction of 1 transactional id".into(),
                        resources: vec![id.clone()],
                    },
                )
                .await?;
                let client = self.connection.connect("transactions").await?;
                client
                    .force_terminate_transaction(&id)
                    .await
                    .map_err(|error| {
                        failure(
                            &format!("Failed to force terminate transactionalId `{id}`"),
                            &admin_error(error),
                        )
                    })?;
                Ok(report)
            }
        }
    }
}

impl ListArgs {
    fn unsupported(&self) -> CommandError {
        let mut filters = Vec::new();
        if let Some(duration) = self.duration_filter {
            filters.push(format!("--duration-filter {duration}"));
        }
        if let Some(pattern) = &self.transactional_id_pattern {
            filters.push(format!("--transactional-id-pattern {pattern}"));
        }
        let context = if filters.is_empty() {
            "Failed to list transactions".to_owned()
        } else {
            format!("Failed to list transactions with {}", filters.join(" "))
        };
        unsupported(&context, &["list_transactions"])
    }
}

impl DescribeProducersArgs {
    fn unsupported(&self) -> CommandError {
        let on = self
            .broker_id
            .map_or_else(|| "leader".to_owned(), |broker| format!("broker {broker}"));
        unsupported(
            &format!(
                "Failed to describe producers for partition {}-{} on {on}",
                self.topic, self.partition
            ),
            &["describe_producers"],
        )
    }
}

impl AbortArgs {
    /// The transaction that the flags identify, with Kafka's messages for a
    /// flag set that identifies none.
    fn target(&self) -> Result<AbortTarget, String> {
        if let Some(start_offset) = self.start_offset {
            return Ok(AbortTarget::StartOffset(start_offset));
        }
        let producer_id = self.producer_id.ok_or(
            "The transaction to abort must be identified either with --start-offset (for brokers \
             on 3.0 or above) or with --producer-id, --producer-epoch, and --coordinator-epoch \
             (for older brokers)",
        )?;
        let producer_epoch = self
            .producer_epoch
            .ok_or("Missing required argument --producer-epoch")?;
        let coordinator_epoch = self
            .coordinator_epoch
            .ok_or("Missing required argument --coordinator-epoch")?;
        // A transaction that a new producer id started, and that hung before
        // its first commit or abort, has coordinator epoch -1 in
        // DescribeProducers. Kafka then uses 0, which no leader epoch is below.
        Ok(AbortTarget::Producer {
            producer_id,
            producer_epoch,
            coordinator_epoch: coordinator_epoch.max(0),
        })
    }

    fn unsupported(&self) -> CommandError {
        let partition = format!("{}-{}", self.topic, self.partition);
        match self.target() {
            Err(message) => message.into(),
            Ok(AbortTarget::StartOffset(_)) => unsupported(
                &format!("Failed to validate producer state for partition {partition}"),
                &["describe_producers", "abort_transaction"],
            ),
            Ok(AbortTarget::Producer {
                producer_id,
                producer_epoch,
                coordinator_epoch,
            }) => unsupported(
                &format!(
                    "Failed to abort transaction AbortTransactionSpec(topicPartition={partition}, \
                     producerId={producer_id}, producerEpoch={producer_epoch}, \
                     coordinatorEpoch={coordinator_epoch})"
                ),
                &["abort_transaction"],
            ),
        }
    }
}

impl FindHangingArgs {
    /// Kafka's checks of the search scope.
    fn check(&self) -> Result<(), String> {
        if self.topic.is_none() && self.broker_id.is_none() {
            return Err(
                "The `find-hanging` command requires either --topic or --broker-id to \
                        limit the scope of the search"
                    .into(),
            );
        }
        if self.partition.is_some() && self.topic.is_none() {
            return Err("The --partition argument requires --topic to be provided".into());
        }
        Ok(())
    }

    fn unsupported(&self) -> CommandError {
        match self.check() {
            Err(message) => message.into(),
            Ok(()) => unsupported(
                &format!(
                    "Failed to find hanging transactions older than {} minutes",
                    self.max_transaction_timeout
                ),
                &["describe_topics", "describe_producers", "list_transactions"],
            ),
        }
    }
}

/// A failure in Kafka's `printErrorAndExit(message, cause)` shape, which ends
/// the cause with a period.
fn failure(context: &str, cause: &CommandError) -> CommandError {
    let cause = cause.to_string();
    let cause = cause.strip_suffix('.').unwrap_or(&cause);
    CommandError::Other(format!(
        "{context}: {cause}. Enable debug logging for additional detail."
    ))
}

/// The failure of a subcommand that needs admin calls the pinned
/// krabka-client-rs does not have.
fn unsupported(context: &str, methods: &[&str]) -> CommandError {
    CommandError::Other(format!(
        "{context}: not supported by this build; it needs AdminClient::{} from a newer \
         krabka-client-rs",
        methods.join(" and AdminClient::")
    ))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// The state name that Kafka prints for `state`.
fn transaction_state(state: &str) -> &str {
    if TRANSACTION_STATES.contains(&state) {
        state
    } else {
        "Unknown"
    }
}

/// The report of `describe`, with the transaction durations measured at
/// `now_ms`.
///
/// The pinned `TransactionDescription` carries neither the coordinator id nor
/// the partitions of the transaction, so `CoordinatorId` and
/// `TopicPartitions` print `None`, the value that Kafka's table prints for an
/// unknown value, and are `null` in JSON.
fn described(descriptions: &[TransactionDescription], now_ms: i64) -> CommandResult {
    let start_of = |description: &TransactionDescription| {
        (description.start_time_ms >= 0).then_some(description.start_time_ms)
    };
    let rows = descriptions
        .iter()
        .map(|description| {
            let start = start_of(description);
            vec![
                NONE.to_owned(),
                description.transactional_id.clone(),
                description.producer_id.to_string(),
                description.producer_epoch.to_string(),
                transaction_state(&description.state).to_owned(),
                description.timeout.millis_i64().to_string(),
                start.map_or_else(|| NONE.to_owned(), |start| start.to_string()),
                start.map_or_else(|| NONE.to_owned(), |start| (now_ms - start).to_string()),
                NONE.to_owned(),
            ]
        })
        .collect::<Vec<_>>();
    let data = descriptions
        .iter()
        .map(|description| {
            let start = start_of(description);
            json!({
                "coordinator_id": Value::Null,
                "transactional_id": description.transactional_id,
                "producer_id": description.producer_id,
                "producer_epoch": description.producer_epoch,
                "transaction_state": transaction_state(&description.state),
                "transaction_timeout_ms": description.timeout.millis_i64(),
                "current_transaction_start_time_ms": start,
                "transaction_duration_ms": start.map(|start| now_ms - start),
                "topic_partitions": Value::Null,
            })
        })
        .collect::<Vec<_>>();
    CommandResult::success(pretty_table(&DESCRIBE_HEADERS, &rows), data)
}

/// The lines of Kafka's `ToolsUtils.prettyPrintTable`: each cell is padded to
/// the width of its column and followed by a tab, the last cell included.
/// A width counts UTF-16 code units, as a Java `String.length` does.
fn pretty_table(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
    let width = |cell: &str| cell.encode_utf16().count();
    let mut widths = headers
        .iter()
        .map(|header| width(header))
        .collect::<Vec<_>>();
    for row in rows {
        for (column, cell) in widths.iter_mut().zip(row) {
            *column = (*column).max(width(cell));
        }
    }
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        cells
            .zip(&widths)
            .fold(String::new(), |mut line, (cell, column)| {
                line.push_str(cell);
                line.push_str(&" ".repeat(column - width(cell)));
                line.push('\t');
                line
            })
    };
    std::iter::once(line(&mut headers.iter().copied()))
        .chain(
            rows.iter()
                .map(|row| line(&mut row.iter().map(String::as_str))),
        )
        .collect()
}

#[cfg(test)]
mod tests;
