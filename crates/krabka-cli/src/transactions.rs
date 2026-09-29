//! `krabka transactions`, the counterpart of `kafka-transactions`.
//!
//! The subcommands, their flags, their checks, their tables and their error
//! messages are those of `TransactionsCommand` in Kafka 4.3.1.
//! `--bootstrap-server` and `--command-config` come before the subcommand, as
//! they do there:
//!
//! ```text
//! krabka transactions --bootstrap-server host:9092 describe --transactional-id t
//! ```
//!
//! Most subcommands run on krabka-client-rs's admin calls. Two requests go
//! to one named broker, which the admin client cannot target: `list`, whose
//! table names the coordinator that listed each transaction and whose
//! `--transactional-id-pattern` the client's filter does not carry, and
//! `describe-producers` or `find-hanging` with `--broker-id`. Those send
//! `ListTransactions` and `DescribeProducers` on a connection of their own to
//! each broker that `DescribeCluster` names, as Kafka's
//! `AllBrokersStrategy` and `StaticBrokerStrategy` do.
//!
//! `abort` and `forceTerminateTransaction` change the cluster, so they take
//! `--dry-run` and `--yes` and otherwise ask for confirmation.

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Args, Subcommand};
use krabka_client_admin::{
    AbortTransactionSpec, AdminClient, AdminError, ClusterNode, DescribeClusterOptions,
    DescribeTopicsOptions, KafkaError, ListTopicsOptions, ListTransactionsFilter,
    ProducerStateInfo, TransactionDescription, TransactionListing,
};
use krabka_client_core::{Connection, ConnectionOptions, connection_target_host, transport};
use krabka_protocol::owned::{
    describe_producers_request::{
        DescribeProducersRequest, TopicRequest as DescribeProducersTopicRequest,
    },
    list_transactions_request::{self, ListTransactionsRequest},
};
use krabka_units::convert::TimeExt as _;
use serde_json::{Value, json};

use crate::{
    connection::ConnectionArgs,
    fan_out, jvm,
    kafka_errors::admin_error,
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, confirm},
};

/// `COORDINATOR_LOAD_IN_PROGRESS`.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// How long `list` asks a loading coordinator again, as Kafka's default
/// `default.api.timeout.ms`.
const LIST_RETRY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
/// The wait between two `ListTransactions` requests to a loading
/// coordinator, as Kafka's default `retry.backoff.ms`.
const LIST_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
/// `NOT_LEADER_OR_FOLLOWER`.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
/// `TRANSACTIONAL_ID_NOT_FOUND`.
const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
/// The most topics or partitions of one `find-hanging` request, as Kafka's
/// `FindHangingTransactionsCommand.MAX_BATCH_SIZE`.
const MAX_BATCH_SIZE: usize = 500;
/// The most brokers that `list` asks at the same time.
const BROKER_FAN_OUT: usize = 16;

/// The columns of `list`, in Kafka's order.
const LIST_HEADERS: [&str; 4] = [
    "TransactionalId",
    "Coordinator",
    "ProducerId",
    "TransactionState",
];

/// The columns of `describe-producers`, in Kafka's order.
const PRODUCERS_HEADERS: [&str; 6] = [
    "ProducerId",
    "ProducerEpoch",
    "LatestCoordinatorEpoch",
    "LastSequence",
    "LastTimestamp",
    "CurrentTransactionStartOffset",
];

/// The columns of `find-hanging`, in Kafka's order.
const HANGING_HEADERS: [&str; 8] = [
    "Topic",
    "Partition",
    "ProducerId",
    "ProducerEpoch",
    "CoordinatorEpoch",
    "StartOffset",
    "LastTimestamp",
    "Duration(min)",
];

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
    #[command(flatten)]
    confirm: ConfirmArgs,
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
        let connection = self.connection;
        match self.command {
            TransactionsCommand::List(args) => args.run(&connection).await,
            TransactionsCommand::Describe(args) => args.run(&connection).await,
            TransactionsCommand::DescribeProducers(args) => args.run(&connection).await,
            TransactionsCommand::Abort(args) => args.run(&connection).await,
            TransactionsCommand::FindHanging(args) => args.run(&connection).await,
            TransactionsCommand::ForceTerminateTransaction(args) => args.run(&connection).await,
        }
    }
}

impl ListArgs {
    /// The `ListTransactions` request of the flags, as Kafka's
    /// `ListTransactionsHandler.buildBatchedRequest` builds it.
    fn request(&self) -> ListTransactionsRequest {
        ListTransactionsRequest {
            duration_filter: self.duration_filter.unwrap_or(-1),
            transactional_id_pattern: self
                .transactional_id_pattern
                .clone()
                .filter(|pattern| !pattern.is_empty()),
            ..Default::default()
        }
    }

    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        let request = self.request();
        let fail = |cause: CommandError| failure("Failed to list transactions", &cause);
        let client = connection.connect("transactions").await?;
        let options = connection.options("transactions").await?;
        let nodes = client
            .describe_cluster(DescribeClusterOptions::default())
            .await
            .map_err(|error| fail(admin_error(error)))?
            .nodes;
        let answers = fan_out::bounded(nodes, BROKER_FAN_OUT, |node| {
            let (options, request) = (&options, request.clone());
            async move {
                let listings = list_on_broker(options, &node, request).await;
                (node.id, listings)
            }
        })
        .await;
        let mut by_broker = Vec::with_capacity(answers.len());
        for (broker, listings) in answers {
            by_broker.push((broker, listings.map_err(fail)?));
        }
        Ok(listed(by_broker))
    }
}

/// The transactions that the coordinator on `node` holds, as one
/// `ListTransactions` request to it.
async fn list_on_broker(
    options: &ConnectionOptions,
    node: &ClusterNode,
    request: ListTransactionsRequest,
) -> Result<Vec<TransactionListing>, CommandError> {
    let connection = broker_connection(options, node).await?;
    let max_version = connection
        .advertised_api_range(list_transactions_request::API_KEY)
        .map_or(0, |(_, max)| max);
    if request.duration_filter >= 0 && max_version < 1 {
        return Err(CommandError::Other(
            "Duration filter can be set only when using API version 1 or higher. If client is \
             connected to an older broker, do not specify duration filter or set duration \
             filter to -1."
                .into(),
        ));
    }
    if request.transactional_id_pattern.is_some() && max_version < 2 {
        return Err(CommandError::Other(
            "Transactional ID pattern filter can be set only when using API version 2 or \
             higher. If client is connected to an older broker, do not specify the pattern \
             filter."
                .into(),
        ));
    }
    let deadline = tokio::time::Instant::now() + LIST_RETRY_DEADLINE;
    let response = loop {
        let response = connection
            .send(request.clone())
            .await
            .map_err(|error| admin_error(error.into()))?;
        // Kafka's `ListTransactionsHandler` asks again while the coordinator
        // loads its state.
        if response.error_code == COORDINATOR_LOAD_IN_PROGRESS
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(LIST_RETRY_BACKOFF).await;
            continue;
        }
        break response;
    };
    if response.error_code != 0 {
        let message = if response.error_code == COORDINATOR_NOT_AVAILABLE {
            format!(
                "ListTransactions request sent to broker {} failed because the coordinator is \
                 shutting down",
                node.id
            )
        } else {
            format!(
                "ListTransactions request sent to broker {} failed with an unexpected exception",
                node.id
            )
        };
        return Err(admin_error(AdminError::Broker {
            api: "ListTransactions",
            code: response.error_code,
            name: "UNKNOWN",
            message: Some(message),
        }));
    }
    Ok(response
        .transaction_states
        .into_iter()
        .map(|state| TransactionListing {
            transactional_id: state.transactional_id,
            producer_id: state.producer_id,
            state: state.transaction_state,
        })
        .collect())
}

/// The table of `list`: the brokers in the order of Kafka's
/// `HashMap<Integer, ...>`, and each broker's listings in answer order.
fn listed(by_broker: Vec<(i32, Vec<TransactionListing>)>) -> CommandResult {
    let by_broker = jvm::hash_order(by_broker, jvm::Table::Default, |(broker, _)| {
        jvm::integer_hash(*broker)
    });
    let mut rows = Vec::new();
    let mut data = Vec::new();
    for (broker, listings) in &by_broker {
        for listing in listings {
            let state = transaction_state(&listing.state);
            rows.push(vec![
                listing.transactional_id.clone(),
                broker.to_string(),
                listing.producer_id.to_string(),
                state.to_owned(),
            ]);
            data.push(json!({
                "transactional_id": listing.transactional_id,
                "coordinator": broker,
                "producer_id": listing.producer_id,
                "transaction_state": state,
            }));
        }
    }
    CommandResult::success(pretty_table(&LIST_HEADERS, &rows), data)
}

impl DescribeArgs {
    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        let client = connection.connect("transactions").await?;
        let mut descriptions = Vec::with_capacity(self.transactional_id.len());
        for id in &self.transactional_id {
            let description = client.describe_transaction(id).await.map_err(|error| {
                failure(
                    &format!("Failed to describe transaction state of transactional-id `{id}`"),
                    &admin_error(error),
                )
            })?;
            descriptions.push(description);
        }
        Ok(described(&descriptions, now_ms()))
    }
}

impl DescribeProducersArgs {
    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        let on = self
            .broker_id
            .map_or_else(|| "leader".to_owned(), |broker| format!("broker {broker}"));
        let partition = (self.topic.clone(), self.partition);
        let client = connection.connect("transactions").await?;
        let options = connection.options("transactions").await?;
        let mut states = producer_states(&client, &options, self.broker_id, &[partition])
            .await
            .map_err(|(_, cause)| cause);
        let states = states
            .as_mut()
            .map(|states| states.pop().map(|(_, states)| states).unwrap_or_default())
            .map_err(|cause| {
                failure(
                    &format!(
                        "Failed to describe producers for partition {}-{} on {on}",
                        self.topic, self.partition
                    ),
                    cause,
                )
            })?;
        Ok(producers_table(&states))
    }
}

/// The table of `describe-producers`.
fn producers_table(states: &[ProducerStateInfo]) -> CommandResult {
    let rows = states
        .iter()
        .map(|state| {
            vec![
                state.producer_id.to_string(),
                state.producer_epoch.to_string(),
                state.coordinator_epoch.to_string(),
                state.last_sequence.to_string(),
                state.last_timestamp_ms.to_string(),
                state
                    .current_txn_start_offset
                    .map_or_else(|| NONE.to_owned(), |offset| offset.to_string()),
            ]
        })
        .collect::<Vec<_>>();
    let data = states.iter().map(producer_json).collect::<Vec<_>>();
    CommandResult::success(pretty_table(&PRODUCERS_HEADERS, &rows), data)
}

fn producer_json(state: &ProducerStateInfo) -> Value {
    json!({
        "producer_id": state.producer_id,
        "producer_epoch": state.producer_epoch,
        "latest_coordinator_epoch": state.coordinator_epoch,
        "last_sequence": state.last_sequence,
        "last_timestamp": state.last_timestamp_ms,
        "current_transaction_start_offset": state.current_txn_start_offset,
    })
}

/// A producer state as Kafka's `DescribeProducersHandler` reads it: a
/// negative coordinator epoch is absent, which the tool prints as `-1`, and a
/// negative start offset is no open transaction.
fn normalized(state: &ProducerStateInfo) -> ProducerStateInfo {
    ProducerStateInfo {
        coordinator_epoch: state.coordinator_epoch.max(-1),
        current_txn_start_offset: state.current_txn_start_offset.filter(|offset| *offset >= 0),
        ..state.clone()
    }
}

/// The failure of one partition of a producer-state lookup, and its cause.
type PartitionFailure = ((String, i32), CommandError);

/// The producer states of `partitions`, from their leaders, or from the
/// broker `broker_id` when it is given, as Kafka's `describeProducers` with
/// `DescribeProducersOptions.brokerId` does. The first failed partition in
/// request order fails the whole lookup, as Kafka's `all()` does.
async fn producer_states(
    client: &AdminClient,
    options: &ConnectionOptions,
    broker_id: Option<i32>,
    partitions: &[(String, i32)],
) -> Result<Vec<((String, i32), Vec<ProducerStateInfo>)>, PartitionFailure> {
    let mut answers = match broker_id {
        None => client
            .describe_producers(partitions)
            .await
            .into_iter()
            .map(|(key, result)| {
                (
                    key,
                    result.map_err(|error| kafka_failure("DescribeProducers", error)),
                )
            })
            .collect::<BTreeMap<_, _>>(),
        Some(broker) => {
            let answer = producers_on_broker(client, options, broker, partitions).await;
            match answer {
                Ok(answers) => answers,
                Err(cause) => {
                    let key = partitions.first().cloned().unwrap_or_default();
                    return Err((key, cause));
                }
            }
        }
    };
    let mut states = Vec::with_capacity(partitions.len());
    for key in partitions {
        match answers.remove(key) {
            Some(Ok(found)) => {
                states.push((key.clone(), found.iter().map(normalized).collect()));
            }
            Some(Err(cause)) => return Err((key.clone(), cause)),
            None => {
                return Err((
                    key.clone(),
                    CommandError::Other(format!(
                        "the DescribeProducers response did not contain a result for partition \
                         {}-{}",
                        key.0, key.1
                    )),
                ));
            }
        }
    }
    Ok(states)
}

/// One `DescribeProducers` request for `partitions` to the broker
/// `broker_id`, as Kafka's `StaticBrokerStrategy` sends it. A
/// `NOT_LEADER_OR_FOLLOWER` answer is final there, since the caller named the
/// broker.
async fn producers_on_broker(
    client: &AdminClient,
    options: &ConnectionOptions,
    broker_id: i32,
    partitions: &[(String, i32)],
) -> Result<BTreeMap<(String, i32), Result<Vec<ProducerStateInfo>, CommandError>>, CommandError> {
    let nodes = client
        .describe_cluster(DescribeClusterOptions::default())
        .await
        .map_err(admin_error)?
        .nodes;
    let node = nodes
        .iter()
        .find(|node| node.id == broker_id)
        .ok_or_else(|| {
            CommandError::Other(
                "Timed out waiting for a node assignment. Call: describeProducers".into(),
            )
        })?;
    let connection = broker_connection(options, node).await?;
    let mut by_topic = BTreeMap::<&str, Vec<i32>>::new();
    for (topic, partition) in partitions {
        by_topic.entry(topic).or_default().push(*partition);
    }
    let request = DescribeProducersRequest {
        topics: by_topic
            .into_iter()
            .map(|(name, partition_indexes)| DescribeProducersTopicRequest {
                name: name.to_owned(),
                partition_indexes,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let response = connection
        .send(request)
        .await
        .map_err(|error| admin_error(error.into()))?;
    let mut out = BTreeMap::new();
    for topic in response.topics {
        for partition in topic.partitions {
            let key = (topic.name.clone(), partition.partition_index);
            let result = if partition.error_code == 0 {
                Ok(partition
                    .active_producers
                    .iter()
                    .map(|state| ProducerStateInfo {
                        producer_id: state.producer_id,
                        producer_epoch: state.producer_epoch,
                        last_sequence: state.last_sequence,
                        last_timestamp_ms: state.last_timestamp,
                        coordinator_epoch: state.coordinator_epoch,
                        current_txn_start_offset: Some(state.current_txn_start_offset),
                    })
                    .collect())
            } else {
                let message = if partition.error_code == NOT_LEADER_OR_FOLLOWER {
                    format!(
                        "Failed to describe active producers for partition {}-{} on brokerId \
                         {broker_id}",
                        key.0, key.1
                    )
                } else {
                    partition.error_message.clone().unwrap_or_default()
                };
                Err(admin_error(AdminError::Broker {
                    api: "DescribeProducers",
                    code: partition.error_code,
                    name: "UNKNOWN",
                    message: Some(message).filter(|message| !message.is_empty()),
                }))
            };
            out.insert(key, result);
        }
    }
    Ok(out)
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

    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        let target = self.target()?;
        let client = connection.connect("transactions").await?;
        let spec = match target {
            AbortTarget::Producer {
                producer_id,
                producer_epoch,
                coordinator_epoch,
            } => AbortTransactionSpec {
                topic: self.topic.clone(),
                partition: self.partition,
                producer_id,
                producer_epoch,
                coordinator_epoch,
            },
            AbortTarget::StartOffset(start_offset) => self.spec_at(&client, start_offset).await?,
        };
        let described = spec_string(&spec);
        let report = CommandResult::success(
            Vec::new(),
            json!({
                "topic": spec.topic,
                "partition": spec.partition,
                "producer_id": spec.producer_id,
                "producer_epoch": spec.producer_epoch,
                "coordinator_epoch": spec.coordinator_epoch,
            }),
        );
        if self.confirm.dry_run {
            return Ok(report.into_dry_run());
        }
        confirm(
            self.confirm.yes,
            "krabka transactions",
            Impact {
                summary: "abort 1 hanging transaction".into(),
                resources: vec![described.clone()],
            },
        )
        .await?;
        client.abort_transaction(&spec).await.map_err(|error| {
            failure(
                &format!("Failed to abort transaction {described}"),
                &admin_error(error),
            )
        })?;
        Ok(report)
    }

    /// The spec of the open transaction that starts at `start_offset`, as
    /// Kafka's `AbortTransactionCommand.buildAbortSpec` finds it.
    async fn spec_at(
        &self,
        client: &AdminClient,
        start_offset: i64,
    ) -> Result<AbortTransactionSpec, CommandError> {
        let partition = format!("{}-{}", self.topic, self.partition);
        let key = (self.topic.clone(), self.partition);
        let states = client
            .describe_producers(std::slice::from_ref(&key))
            .await
            .remove(&key)
            .unwrap_or_else(|| Ok(Vec::new()))
            .map_err(|error| {
                failure(
                    &format!("Failed to validate producer state for partition {partition}"),
                    &kafka_failure("DescribeProducers", error),
                )
            })?;
        let state = states
            .iter()
            .map(normalized)
            .find(|state| state.current_txn_start_offset == Some(start_offset))
            .ok_or_else(|| {
                CommandError::Other(format!(
                    "Could not find any open transactions starting at offset {start_offset} on \
                     partition {partition}"
                ))
            })?;
        Ok(AbortTransactionSpec {
            topic: self.topic.clone(),
            partition: self.partition,
            producer_id: state.producer_id,
            producer_epoch: java_short(state.producer_epoch),
            coordinator_epoch: state.coordinator_epoch.max(0),
        })
    }
}

/// Java's `(short)` cast of an `int`: the low 16 bits.
fn java_short(value: i32) -> i16 {
    let [low, high, ..] = value.to_le_bytes();
    i16::from_le_bytes([low, high])
}

/// Kafka's `AbortTransactionSpec.toString`.
fn spec_string(spec: &AbortTransactionSpec) -> String {
    format!(
        "AbortTransactionSpec(topicPartition={}-{}, producerId={}, producerEpoch={}, \
         coordinatorEpoch={})",
        spec.topic, spec.partition, spec.producer_id, spec.producer_epoch, spec.coordinator_epoch
    )
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

    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        self.check()?;
        let max_timeout_ms = i64::from(self.max_transaction_timeout) * 60_000;
        let client = connection.connect("transactions").await?;
        let options = connection.options("transactions").await?;
        let partitions = self.partitions_to_search(&client).await?;
        let mut candidates = Vec::new();
        for batch in partitions.chunks(MAX_BATCH_SIZE) {
            let states = producer_states(&client, &options, self.broker_id, batch)
                .await
                .map_err(|(_, cause)| {
                    failure(
                        &format!(
                            "Failed to describe producers for {} partitions on broker {}",
                            batch.len(),
                            java_optional(self.broker_id)
                        ),
                        &cause,
                    )
                })?;
            candidates.extend(open_candidates(
                states,
                batch.len(),
                now_ms(),
                max_timeout_ms,
            ));
        }
        if candidates.is_empty() {
            return Ok(hanging_table(&[], now_ms()));
        }
        let by_producer = group_by_producer(candidates);
        let producer_ids = by_producer.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let listings = client
            .list_transactions(&ListTransactionsFilter {
                producer_id_filters: producer_ids.clone(),
                ..Default::default()
            })
            .await
            .map_err(|error| {
                failure(
                    &format!(
                        "Failed to list transactions for {} producers",
                        producer_ids.len()
                    ),
                    &admin_error(error),
                )
            })?;
        let mut transactional_ids = BTreeMap::new();
        for listing in listings {
            if producer_ids.contains(&listing.producer_id) {
                transactional_ids.insert(listing.producer_id, listing.transactional_id);
            }
        }
        let mut descriptions = BTreeMap::new();
        for id in transactional_ids.values() {
            if descriptions.contains_key(id) {
                continue;
            }
            let description = match client.describe_transaction(id).await {
                Ok(description) => Some(description),
                Err(AdminError::Broker {
                    code: TRANSACTIONAL_ID_NOT_FOUND,
                    ..
                }) => None,
                Err(error) => {
                    return Err(failure(
                        &format!(
                            "Failed to describe {} transactions",
                            transactional_ids.len()
                        ),
                        &admin_error(error),
                    ));
                }
            };
            descriptions.insert(id.clone(), description);
        }
        let hanging = hanging_transactions(by_producer, &transactional_ids, &descriptions);
        Ok(hanging_table(&hanging, now_ms()))
    }

    /// The partitions to search, as Kafka's `collectTopicPartitionsToSearch`
    /// finds them: the one partition named, or every partition of the named
    /// topic or of every topic, kept only where `--broker-id` holds a
    /// replica.
    async fn partitions_to_search(
        &self,
        client: &AdminClient,
    ) -> Result<Vec<(String, i32)>, CommandError> {
        let topics = match (&self.topic, self.partition) {
            (Some(topic), Some(partition)) => return Ok(vec![(topic.clone(), partition)]),
            (Some(topic), None) => vec![topic.clone()],
            (None, _) => {
                let listed = client
                    .list_topics(ListTopicsOptions {
                        list_internal: true,
                    })
                    .await
                    .map_err(|error| failure("Failed to list topics", &admin_error(error)))?;
                jvm::hash_order(
                    listed.into_keys().collect(),
                    jvm::Table::Default,
                    |name: &String| jvm::string_hash(name),
                )
            }
        };
        let mut partitions = Vec::new();
        for batch in topics.chunks(MAX_BATCH_SIZE) {
            let names = batch.iter().map(String::as_str).collect::<Vec<_>>();
            let mut described = client
                .describe_topics(&names, DescribeTopicsOptions::default())
                .await;
            let fail = |cause: CommandError| {
                failure(
                    &format!("Failed to describe {} topics", batch.len()),
                    &cause,
                )
            };
            let mut descriptions = Vec::with_capacity(batch.len());
            for name in batch {
                match described.remove(name) {
                    Some(Ok(description)) => descriptions.push(description),
                    Some(Err(error)) => {
                        return Err(fail(kafka_failure("DescribeTopicPartitions", error)));
                    }
                    None => {
                        return Err(fail(CommandError::Other(format!(
                            "the answer did not describe topic {name}"
                        ))));
                    }
                }
            }
            let descriptions = jvm::hash_order(
                descriptions,
                jvm::Table::WithCapacity(batch.len()),
                |description| jvm::string_hash(&description.name),
            );
            for description in descriptions {
                for partition in &description.partitions {
                    let replica = self.broker_id.is_none_or(|broker| {
                        partition.replicas.iter().any(|node| node.id == broker)
                    });
                    if replica {
                        partitions.push((description.name.clone(), partition.partition));
                    }
                }
            }
        }
        Ok(partitions)
    }
}

/// An open transaction that `find-hanging` found on a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenTransaction {
    partition: (String, i32),
    state: ProducerStateInfo,
}

/// The open transactions of `states` whose last write is older than
/// `max_timeout_ms`, in the order of Kafka's `HashMap<TopicPartition, ...>`
/// of `batch_len` partitions.
fn open_candidates(
    states: Vec<((String, i32), Vec<ProducerStateInfo>)>,
    batch_len: usize,
    now_ms: i64,
    max_timeout_ms: i64,
) -> Vec<OpenTransaction> {
    let states = jvm::hash_order(states, jvm::Table::WithCapacity(batch_len), |(key, _)| {
        jvm::topic_partition_hash(&key.0, key.1)
    });
    states
        .into_iter()
        .flat_map(|(partition, states)| {
            states
                .into_iter()
                .filter(|state| {
                    state.current_txn_start_offset.is_some()
                        && now_ms - state.last_timestamp_ms > max_timeout_ms
                })
                .map(move |state| OpenTransaction {
                    partition: partition.clone(),
                    state,
                })
        })
        .collect()
}

/// `Long.hashCode`.
fn long_hash(value: i64) -> i32 {
    let bits = value.cast_unsigned();
    let [a, b, c, d, ..] = (bits ^ (bits >> 32)).to_le_bytes();
    i32::from_le_bytes([a, b, c, d])
}

/// The candidates grouped by producer id, in the order of Kafka's
/// `HashMap<Long, List<OpenTransaction>>`.
fn group_by_producer(candidates: Vec<OpenTransaction>) -> Vec<(i64, Vec<OpenTransaction>)> {
    let mut groups = Vec::<(i64, Vec<OpenTransaction>)>::new();
    for candidate in candidates {
        let id = candidate.state.producer_id;
        match groups.iter_mut().find(|(producer, _)| *producer == id) {
            Some((_, group)) => group.push(candidate),
            None => groups.push((id, vec![candidate])),
        }
    }
    jvm::hash_order(groups, jvm::Table::Default, |(id, _)| long_hash(*id))
}

/// Kafka's `filterHangingTransactions`: an open transaction hangs when no
/// transactional id holds its producer id, when the coordinator does not
/// know that id, or when the coordinator's transaction does not include the
/// partition.
fn hanging_transactions(
    by_producer: Vec<(i64, Vec<OpenTransaction>)>,
    transactional_ids: &BTreeMap<i64, String>,
    descriptions: &BTreeMap<String, Option<TransactionDescription>>,
) -> Vec<OpenTransaction> {
    let mut hanging = Vec::new();
    for (producer_id, open) in by_producer {
        let description = transactional_ids
            .get(&producer_id)
            .and_then(|id| descriptions.get(id))
            .and_then(Option::as_ref);
        match description {
            None => hanging.extend(open),
            Some(description) => hanging.extend(
                open.into_iter()
                    .filter(|open| !description.topic_partitions.contains(&open.partition)),
            ),
        }
    }
    hanging
}

/// The table of `find-hanging`, with the durations measured at `now_ms`.
fn hanging_table(hanging: &[OpenTransaction], now_ms: i64) -> CommandResult {
    let rows = hanging
        .iter()
        .map(|open| {
            vec![
                open.partition.0.clone(),
                open.partition.1.to_string(),
                open.state.producer_id.to_string(),
                open.state.producer_epoch.to_string(),
                open.state.coordinator_epoch.to_string(),
                open.state
                    .current_txn_start_offset
                    .unwrap_or(-1)
                    .to_string(),
                open.state.last_timestamp_ms.to_string(),
                ((now_ms - open.state.last_timestamp_ms) / 60_000).to_string(),
            ]
        })
        .collect::<Vec<_>>();
    let data = hanging
        .iter()
        .map(|open| {
            json!({
                "topic": open.partition.0,
                "partition": open.partition.1,
                "producer_id": open.state.producer_id,
                "producer_epoch": open.state.producer_epoch,
                "coordinator_epoch": open.state.coordinator_epoch,
                "start_offset": open.state.current_txn_start_offset.unwrap_or(-1),
                "last_timestamp": open.state.last_timestamp_ms,
                "duration_min": (now_ms - open.state.last_timestamp_ms) / 60_000,
            })
        })
        .collect::<Vec<_>>();
    CommandResult::success(pretty_table(&HANGING_HEADERS, &rows), data)
}

/// Java's `Optional.toString` of an optional broker id.
fn java_optional(value: Option<i32>) -> String {
    value.map_or_else(
        || "Optional.empty".to_owned(),
        |value| format!("Optional[{value}]"),
    )
}

impl ForceTerminateArgs {
    async fn run(self, connection: &ConnectionArgs) -> Result<CommandResult, CommandError> {
        let id = self.transactional_id;
        // Kafka prints nothing when the call succeeds.
        let report = CommandResult::success(Vec::new(), json!({"transactional_id": id}));
        if self.confirm.dry_run {
            connection.connect("transactions").await?;
            return Ok(report.into_dry_run());
        }
        confirm(
            self.confirm.yes,
            "krabka transactions",
            Impact {
                summary: "force-terminate the transaction of 1 transactional id".into(),
                resources: vec![id.clone()],
            },
        )
        .await?;
        let client = connection.connect("transactions").await?;
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

/// A per-partition Kafka error of `api` as a [`CommandError`].
fn kafka_failure(api: &'static str, error: KafkaError) -> CommandError {
    admin_error(AdminError::Broker {
        api,
        code: error.code,
        name: error.name,
        message: error.message,
    })
}

/// A connection of its own to `node`, with the admin client's options.
async fn broker_connection(
    options: &ConnectionOptions,
    node: &ClusterNode,
) -> Result<Connection, CommandError> {
    let address = if node.host.contains(':') && !node.host.starts_with('[') {
        format!("[{}]:{}", node.host, node.port)
    } else {
        format!("{}:{}", node.host, node.port)
    };
    let mut options = options.clone();
    if let Some(security) = options.security.as_mut() {
        **security = security.for_target_host(connection_target_host(&address));
    }
    let resolved = transport::resolve(&address)
        .await
        .map_err(|error| format!("resolve {address}: {error}"))?;
    let socket = resolved
        .first()
        .copied()
        .ok_or_else(|| format!("resolve {address}: no address"))?;
    Connection::connect_with_options(socket, options)
        .await
        .map_err(|error| admin_error(error.into()))
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

/// The partitions of a transaction as Kafka's table prints them:
/// `TopicPartition.toString`, joined by commas.
fn topic_partitions(description: &TransactionDescription) -> Vec<String> {
    description
        .topic_partitions
        .iter()
        .map(|(topic, partition)| format!("{topic}-{partition}"))
        .collect()
}

/// The report of `describe`, with the transaction durations measured at
/// `now_ms`.
fn described(descriptions: &[TransactionDescription], now_ms: i64) -> CommandResult {
    let start_of = |description: &TransactionDescription| description.start_time_ms;
    let rows = descriptions
        .iter()
        .map(|description| {
            let start = start_of(description);
            vec![
                description.coordinator_id.to_string(),
                description.transactional_id.clone(),
                description.producer_id.to_string(),
                description.producer_epoch.to_string(),
                transaction_state(&description.state).to_owned(),
                description.timeout.millis_i64().to_string(),
                start.map_or_else(|| NONE.to_owned(), |start| start.to_string()),
                start.map_or_else(|| NONE.to_owned(), |start| (now_ms - start).to_string()),
                topic_partitions(description).join(","),
            ]
        })
        .collect::<Vec<_>>();
    let data = descriptions
        .iter()
        .map(|description| {
            let start = start_of(description);
            json!({
                "coordinator_id": description.coordinator_id,
                "transactional_id": description.transactional_id,
                "producer_id": description.producer_id,
                "producer_epoch": description.producer_epoch,
                "transaction_state": transaction_state(&description.state),
                "transaction_timeout_ms": description.timeout.millis_i64(),
                "current_transaction_start_time_ms": start,
                "transaction_duration_ms": start.map(|start| now_ms - start),
                "topic_partitions": topic_partitions(description),
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
