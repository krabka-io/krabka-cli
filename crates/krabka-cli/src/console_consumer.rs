//! `krabka console-consumer`, the counterpart of `kafka-console-consumer`.
//!
//! The flags are `ConsoleConsumerOptions`' at Kafka 4.3.1, deprecated
//! spellings included, and they are checked in the order and with the
//! messages that the JVM tool uses. Records go to stdout in the shape the
//! `--formatter` class writes them. Everything else goes to stderr, so
//! `krabka console-consumer ... | wc -l` counts records.
//!
//! Two paths consume, chosen by the flags. `--topic` or `--include` alone
//! joins a consumer group, the one `--group` names or a generated
//! `console-consumer-<n>`, with `Consumer` from `krabka-client-consumer`.
//! `--partition` reads one partition without joining a group, with the
//! single-partition fetch of `krabka-client-core`.

mod formatter;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::{BuildHasher as _, Hasher as _},
    io,
    net::SocketAddr,
    path::PathBuf,
    time::Instant,
};

use clap::Args;
use krabka_client_consumer::{
    Assignor, AutoOffsetReset, Consumer, ConsumerError, IsolationLevel, OffsetAndMetadata,
};
use krabka_client_core::{
    Client, ClientError, Connection, ConnectionOptions, FetchMinBytes, IsolatedFetch,
    fetch_partition_with_isolation_progress,
};
use krabka_protocol::primitives::uuid::Uuid;
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use self::formatter::{DEFAULT_FORMATTER, Field, Formatter, Record};
use crate::{
    connection::{ConnectionArgs, Properties},
    console::{
        bool_property, cancel_on_ctrl_c, config_error, fail, key_value_args, load_properties,
        number_property, overlay, warn,
    },
    exit::Exit,
    output::{OutputFormat, emit_error},
};

const COMMAND: &str = "krabka console-consumer";
const NOT_SUPPORTED: &str = "not supported by this build";
/// How long one poll waits before the loop checks `--timeout-ms` and Ctrl-C
/// again.
const POLL_SLICE_MS: i64 = 1_000;
/// `fetch.max.wait.ms`, which bounds one fetch of the `--partition` path.
const DEFAULT_FETCH_MAX_WAIT_MS: i64 = 500;
/// How long past its wait the `--partition` path gives a fetch to answer.
const FETCH_GRACE_MS: u64 = 1_000;
/// How long the `--partition` path waits before it retries a retriable error.
const RETRY_BACKOFF_MS: u64 = 100;

/// The flags of `kafka-console-consumer`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Args)]
pub struct ConsoleConsumerArgs {
    /// The topic to consume from.
    #[arg(long)]
    topic: Option<String>,
    /// Regular expression specifying list of topics to include for
    /// consumption.
    #[arg(long)]
    include: Option<String>,
    /// The partition to consume from. Consumption starts from the end of the
    /// partition unless '--offset' is specified.
    #[arg(long, allow_hyphen_values = true)]
    partition: Option<i32>,
    /// The offset to consume from (a non-negative number), or 'earliest'
    /// which means from beginning, or 'latest' which means from end.
    #[arg(long, allow_hyphen_values = true)]
    offset: Option<String>,
    /// (DEPRECATED) Consumer config properties in the form key=value. Use
    /// --command-property instead.
    #[arg(long)]
    consumer_property: Vec<String>,
    /// Consumer config properties in the form key=value.
    #[arg(long)]
    command_property: Vec<String>,
    /// (DEPRECATED) Consumer config properties file. Use --command-config
    /// instead.
    #[arg(long = "consumer.config")]
    consumer_config: Option<PathBuf>,
    /// Consumer config properties file. Note that --command-property takes
    /// precedence over this config.
    #[arg(long)]
    command_config: Option<PathBuf>,
    /// The name of a class to use for formatting kafka messages for display.
    #[arg(long, default_value = DEFAULT_FORMATTER)]
    formatter: String,
    /// (DEPRECATED) The properties to initialize the message formatter. Use
    /// --formatter-property instead.
    #[arg(long)]
    property: Vec<String>,
    /// The properties to initialize the message formatter: print.timestamp,
    /// print.key, print.offset, print.epoch, print.partition, print.headers,
    /// print.value, key.separator, line.separator, headers.separator,
    /// null.literal, key.deserializer, value.deserializer and
    /// headers.deserializer.
    #[arg(long)]
    formatter_property: Vec<String>,
    /// Config properties file to initialize the message formatter. Note that
    /// --formatter-property takes precedence over this config.
    #[arg(long)]
    formatter_config: Option<PathBuf>,
    /// If the consumer does not already have an established offset to consume
    /// from, start with the earliest message present in the log rather than
    /// the latest message.
    #[arg(long)]
    from_beginning: bool,
    /// The maximum number of messages to consume before exiting. If not set,
    /// consumption is continual.
    #[arg(long, allow_hyphen_values = true)]
    max_messages: Option<i32>,
    /// If specified, exit if no message is available for consumption for the
    /// specified interval.
    #[arg(long, allow_hyphen_values = true)]
    timeout_ms: Option<i64>,
    /// If there is an error when processing a message, skip it instead of
    /// halt.
    #[arg(long)]
    skip_message_on_error: bool,
    /// REQUIRED: The server(s) to connect to.
    #[arg(long, env = "KRABKA_BOOTSTRAP_SERVER")]
    bootstrap_server: Option<String>,
    /// The name of the class to use for deserializing keys.
    #[arg(long)]
    key_deserializer: Option<String>,
    /// The name of the class to use for deserializing values.
    #[arg(long)]
    value_deserializer: Option<String>,
    /// Log lifecycle events of the consumer in addition to logging consumed
    /// messages. (This is specific for system tests.)
    #[arg(long)]
    enable_systest_events: bool,
    /// Set to `read_committed` in order to filter out transactional messages
    /// which are not committed. Set to `read_uncommitted` to read all
    /// messages.
    #[arg(long)]
    isolation_level: Option<String>,
    /// The consumer group id of the consumer.
    #[arg(long)]
    group: Option<String>,
}

/// Why the command stopped before it consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Refusal {
    /// The command line is not valid: `printUsageAndExit` in the JVM tool.
    Usage(String),
    /// The configuration is not valid, or a feature is missing.
    Failure(String),
}

impl Refusal {
    const fn exit(&self) -> Exit {
        match self {
            Self::Usage(_) => Exit::Usage,
            Self::Failure(_) => Exit::Failure,
        }
    }

    fn message(&self) -> &str {
        match self {
            Self::Usage(message) | Self::Failure(message) => message,
        }
    }
}

/// Where consumption starts on the `--partition` path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOffset {
    Earliest,
    Latest,
    At(i64),
}

/// What the command consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    /// Subscribe to one topic.
    Topic(String),
    /// Subscribe to every topic that the Java regular expression matches.
    Include(String),
    /// Read one partition without joining a group.
    Partition {
        topic: String,
        partition: i32,
        offset: StartOffset,
    },
}

/// `auto.offset.reset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OffsetReset {
    Earliest,
    Latest,
    None,
}

impl From<OffsetReset> for AutoOffsetReset {
    fn from(reset: OffsetReset) -> Self {
        match reset {
            OffsetReset::Earliest => Self::Earliest,
            OffsetReset::Latest => Self::Latest,
            OffsetReset::None => Self::None,
        }
    }
}

/// The consumer settings that krabka reads from the merged client
/// properties, validated as Kafka's `ConsumerConfig` validates them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientSettings {
    group_id: String,
    auto_offset_reset: OffsetReset,
    isolation_level: IsolationLevel,
    enable_auto_commit: bool,
    auto_commit_interval_ms: i64,
    session_timeout_ms: i64,
    heartbeat_interval_ms: i64,
    rebalance_timeout_ms: i64,
    fetch_min_bytes: i64,
    fetch_max_bytes: i64,
    max_partition_fetch_bytes: i64,
    fetch_max_wait_ms: i64,
    group_instance_id: Option<String>,
    client_rack: Option<String>,
    assignors: Vec<Assignor>,
}

/// The command line, checked and resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    bootstrap: Vec<String>,
    source: Source,
    /// The merged consumer properties, with `auto.offset.reset`,
    /// `client.id`, `isolation.level` and, for a named group, `group.id`
    /// set as `buildConsumerProps` sets them.
    properties: Properties,
    /// The group that `--group` or a property names. `None` generates
    /// `console-consumer-<n>` at run time.
    group: Option<String>,
    /// `-1` consumes without a bound.
    max_messages: i32,
    /// `None` waits forever.
    timeout_ms: Option<i64>,
    skip_message_on_error: bool,
    formatter: Formatter,
    formatter_warnings: Vec<String>,
    systest_events: bool,
    /// The deprecation warnings that the JVM tool prints.
    warnings: Vec<String>,
}

impl ConsoleConsumerArgs {
    /// Checks the command line as `ConsoleConsumerOptions` does, in the same
    /// order, and resolves it.
    fn plan(&self) -> Result<Plan, Refusal> {
        let usage = |message: &str| Refusal::Usage(message.to_owned());
        if usize::from(self.topic.is_some()) + usize::from(self.include.is_some()) != 1 {
            return Err(usage(
                "Exactly one of the following arguments is required: [topic], [include]",
            ));
        }
        if self.partition.is_some() {
            if self.topic.is_none() {
                return Err(usage("The topic is required when partition is specified."));
            }
            if self.from_beginning && self.offset.is_some() {
                return Err(usage(
                    "Options from-beginning and offset cannot be specified together.",
                ));
            }
        } else if self.offset.is_some() {
            return Err(usage("The partition is required when offset is specified."));
        }
        let Some(bootstrap) = self.bootstrap_server.as_deref() else {
            return Err(usage("Missing required argument \"[bootstrap-server]\""));
        };
        if !self.consumer_property.is_empty() && !self.command_property.is_empty() {
            return Err(usage(
                "Options --consumer-property and --command-property cannot be specified together.",
            ));
        }
        if self.consumer_config.is_some() && self.command_config.is_some() {
            return Err(usage(
                "Options --consumer.config and --command-config cannot be specified together.",
            ));
        }
        let mut warnings = Vec::new();
        if !self.consumer_property.is_empty() {
            warnings.push(
                "Option --consumer-property is deprecated and will be removed in a future version. Use --command-property instead."
                    .to_owned(),
            );
        }
        if self.consumer_config.is_some() {
            warnings.push(
                "Option --consumer.config is deprecated and will be removed in a future version. Use --command-config instead."
                    .to_owned(),
            );
        }
        let from_file = self
            .consumer_config
            .as_ref()
            .or(self.command_config.as_ref())
            .map(|path| load_properties(path))
            .transpose()
            .map_err(Refusal::Failure)?
            .unwrap_or_default();
        let extra = key_value_args(if self.consumer_property.is_empty() {
            &self.command_property
        } else {
            &self.consumer_property
        });
        let group = self.group(&from_file, &extra)?;
        let properties = self.consumer_properties(from_file, &extra, group.as_ref())?;
        let source = match (&self.topic, &self.include, self.partition) {
            (Some(topic), _, Some(partition)) => Source::Partition {
                topic: topic.clone(),
                partition,
                offset: self.start_offset()?,
            },
            (Some(topic), _, None) => Source::Topic(topic.clone()),
            (None, Some(pattern), _) => {
                java_pattern(pattern).map_err(Refusal::Failure)?;
                Source::Include(pattern.clone())
            }
            (None, None, _) => unreachable!("one of --topic and --include is checked above"),
        };
        let (formatter, formatter_warnings) = self.formatter(&mut warnings)?;
        Ok(Plan {
            bootstrap: bootstrap.split(',').map(str::to_owned).collect(),
            source,
            properties,
            group,
            max_messages: self.max_messages.unwrap_or(-1),
            timeout_ms: self.timeout_ms.filter(|&timeout| timeout >= 0),
            skip_message_on_error: self.skip_message_on_error,
            formatter,
            formatter_warnings,
            systest_events: self.enable_systest_events,
            warnings,
        })
    }

    /// `checkConsumerGroup`: every place that names a group names the same
    /// one, and a group does not go with `--partition`.
    fn group(&self, from_file: &Properties, extra: &Properties) -> Result<Option<String>, Refusal> {
        let mut groups = Vec::new();
        for group in [
            self.group.as_deref(),
            from_file.get("group.id"),
            extra.get("group.id"),
        ]
        .into_iter()
        .flatten()
        {
            if !groups.iter().any(|seen| seen == group) {
                groups.push(group.to_owned());
            }
        }
        if groups.len() > 1 {
            return Err(Refusal::Usage(format!(
                "The group ids provided in different places (directly using '--group', via '--consumer-property', or via '--consumer.config') do not match. Detected group ids: {}",
                groups
                    .iter()
                    .map(|group| format!("'{group}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if !groups.is_empty() && self.partition.is_some() {
            return Err(Refusal::Usage(
                "Options group and partition cannot be specified together.".to_owned(),
            ));
        }
        Ok(groups.pop())
    }

    /// `buildConsumerProps`.
    fn consumer_properties(
        &self,
        from_file: Properties,
        extra: &Properties,
        group: Option<&String>,
    ) -> Result<Properties, Refusal> {
        let mut properties = overlay(from_file, extra);
        match properties.get("auto.offset.reset") {
            Some(reset) if self.from_beginning && reset != "earliest" => {
                return Err(Refusal::Failure(format!(
                    "Can't simultaneously specify --from-beginning and 'auto.offset.reset={reset}', please remove one option"
                )));
            }
            Some(_) => {}
            None => properties.insert(
                "auto.offset.reset",
                if self.from_beginning {
                    "earliest"
                } else {
                    "latest"
                },
            ),
        }
        if properties.get("client.id").is_none() {
            properties.insert("client.id", "console-consumer");
        }
        if self.isolation_level.is_some() || properties.get("isolation.level").is_none() {
            properties.insert(
                "isolation.level",
                self.isolation_level
                    .as_deref()
                    .unwrap_or("read_uncommitted"),
            );
        }
        match group {
            Some(group) => properties.insert("group.id", group.as_str()),
            // The generated group and its offsets are not meant to be used
            // again, so it does not commit unless asked to.
            None if properties.get("enable.auto.commit").is_none() => {
                properties.insert("enable.auto.commit", "false");
            }
            None => {}
        }
        Ok(properties)
    }

    /// `parseOffset`.
    fn start_offset(&self) -> Result<StartOffset, Refusal> {
        let Some(offset) = &self.offset else {
            return Ok(if self.from_beginning {
                StartOffset::Earliest
            } else {
                StartOffset::Latest
            });
        };
        match offset.to_ascii_lowercase().as_str() {
            "earliest" => Ok(StartOffset::Earliest),
            "latest" => Ok(StartOffset::Latest),
            _ => match offset.parse::<i64>() {
                Ok(offset) if offset >= 0 => Ok(StartOffset::At(offset)),
                _ => Err(Refusal::Usage(format!(
                    "The provided offset value '{offset}' is incorrect. Valid values are 'earliest', 'latest', or a non-negative long."
                ))),
            },
        }
    }

    /// `buildFormatter` and `formatterArgs`.
    fn formatter(&self, warnings: &mut Vec<String>) -> Result<(Formatter, Vec<String>), Refusal> {
        if !self.formatter_property.is_empty() && !self.property.is_empty() {
            return Err(Refusal::Usage(
                "Options --property and --formatter-property cannot be specified together."
                    .to_owned(),
            ));
        }
        if !self.property.is_empty() {
            warnings.push(
                "Option --property is deprecated and will be removed in a future version. Use --formatter-property instead."
                    .to_owned(),
            );
        }
        let mut properties = self
            .formatter_config
            .as_ref()
            .map(|path| load_properties(path))
            .transpose()
            .map_err(Refusal::Usage)?
            .unwrap_or_default();
        for (key, class) in [
            ("key.deserializer", &self.key_deserializer),
            ("value.deserializer", &self.value_deserializer),
        ] {
            if let Some(class) = class.as_deref().filter(|class| !class.is_empty()) {
                properties.insert(key, class);
            }
        }
        let arguments = if self.property.is_empty() {
            &self.formatter_property
        } else {
            &self.property
        };
        let properties = overlay(properties, &key_value_args(arguments));
        Formatter::new(&self.formatter, &properties).map_err(Refusal::Usage)
    }
}

impl ClientSettings {
    /// Reads and validates the settings, as `new KafkaConsumer` does.
    fn from_properties(properties: &Properties, group_id: String) -> Result<Self, String> {
        let auto_offset_reset = match properties.get("auto.offset.reset").unwrap_or("latest") {
            "earliest" => OffsetReset::Earliest,
            "latest" => OffsetReset::Latest,
            "none" => OffsetReset::None,
            other if other.starts_with("by_duration:") => {
                return Err(format!(
                    "auto.offset.reset={other} is {NOT_SUPPORTED}: the by_duration strategy (KIP-1106) needs AutoOffsetReset::ByDuration from a newer krabka-client-consumer"
                ));
            }
            other => {
                return Err(config_error(
                    "auto.offset.reset",
                    other,
                    &format!(
                        "Invalid value `{other}` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'."
                    ),
                ));
            }
        };
        let isolation_level = match properties
            .get("isolation.level")
            .unwrap_or("read_uncommitted")
        {
            "read_uncommitted" => IsolationLevel::ReadUncommitted,
            "read_committed" => IsolationLevel::ReadCommitted,
            other => {
                return Err(config_error(
                    "isolation.level",
                    other,
                    "String must be one of: read_committed, read_uncommitted",
                ));
            }
        };
        if let Some(protocol) = properties.get("group.protocol")
            && !protocol.eq_ignore_ascii_case("classic")
        {
            return Err(if protocol.eq_ignore_ascii_case("consumer") {
                format!(
                    "group.protocol=consumer is {NOT_SUPPORTED}: the KIP-848 consumer protocol needs a newer krabka-client-consumer"
                )
            } else {
                config_error(
                    "group.protocol",
                    protocol,
                    "String must be one of (case insensitive): CLASSIC, CONSUMER",
                )
            });
        }
        let int = |name: &str, default: i64| number_property(properties, name, "INT", default);
        Ok(Self {
            group_id,
            auto_offset_reset,
            isolation_level,
            enable_auto_commit: bool_property(properties, "enable.auto.commit", true)?,
            auto_commit_interval_ms: int("auto.commit.interval.ms", 5_000)?,
            session_timeout_ms: int("session.timeout.ms", 45_000)?,
            heartbeat_interval_ms: int("heartbeat.interval.ms", 3_000)?,
            rebalance_timeout_ms: int("max.poll.interval.ms", 300_000)?,
            fetch_min_bytes: int("fetch.min.bytes", 1)?,
            fetch_max_bytes: int("fetch.max.bytes", 52_428_800)?,
            max_partition_fetch_bytes: int("max.partition.fetch.bytes", 1_048_576)?,
            fetch_max_wait_ms: int("fetch.max.wait.ms", DEFAULT_FETCH_MAX_WAIT_MS)?,
            group_instance_id: properties
                .get("group.instance.id")
                .filter(|id| !id.is_empty())
                .map(str::to_owned),
            client_rack: properties
                .get("client.rack")
                .filter(|rack| !rack.is_empty())
                .map(str::to_owned),
            assignors: assignors(properties.get("partition.assignment.strategy"))?,
        })
    }
}

/// The assignors of `partition.assignment.strategy`, in preference order.
/// Kafka's default list is `RangeAssignor, CooperativeStickyAssignor`. Each
/// entry is a class name, simple or qualified, or its protocol name.
fn assignors(strategy: Option<&str>) -> Result<Vec<Assignor>, String> {
    let Some(list) = strategy else {
        return Ok(Assignor::DEFAULT_LIST.to_vec());
    };
    list.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| match entry.rsplit('.').next().unwrap_or(entry) {
            "RangeAssignor" | "range" => Ok(Assignor::Range),
            "RoundRobinAssignor" | "roundrobin" => Ok(Assignor::RoundRobin),
            "StickyAssignor" | "sticky" => Ok(Assignor::Sticky),
            "CooperativeStickyAssignor" | "cooperative-sticky" => Ok(Assignor::CooperativeSticky),
            _ => Err(format!(
                "partition.assignment.strategy={entry} is not supported; krabka implements \
                 RangeAssignor, RoundRobinAssignor, StickyAssignor and CooperativeStickyAssignor"
            )),
        })
        .collect()
}

/// A Java regular expression as `Pattern.matcher(topic).matches()` applies
/// it: anchored at both ends.
fn java_pattern(pattern: &str) -> Result<regex::Regex, String> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|error| format!("--include {pattern}: {error}"))
}

/// A generated group id, `console-consumer-<n>` with `n` below 100000, as
/// the JVM tool generates it.
fn generated_group() -> String {
    let random = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    format!("console-consumer-{}", random % 100_000)
}

/// Runs `console-consumer`: stdout carries the records, stderr the rest.
pub(crate) async fn run(args: ConsoleConsumerArgs, format: OutputFormat) -> Exit {
    let plan = match args.plan() {
        Ok(plan) => plan,
        Err(refusal) => {
            let _ = emit_error(COMMAND, refusal.message(), refusal.exit(), format);
            return refusal.exit();
        }
    };
    for warning in &plan.warnings {
        warn(warning);
    }
    for warning in &plan.formatter_warnings {
        tracing::error!("{warning}");
    }
    let (cancel, watcher) = cancel_on_ctrl_c();
    let mut stdout = io::stdout().lock();
    let exit = consume(plan, format, &mut stdout, &cancel).await;
    watcher.abort();
    exit
}

/// How the consume loop ended.
#[derive(Debug, PartialEq, Eq)]
enum Ended {
    /// `--max-messages`, `--timeout-ms`, or a closed stdout.
    Done,
    /// Ctrl-C.
    Cancelled,
    /// A record failed to format and `--skip-message-on-error` is off, or the
    /// consumer failed.
    Failed(String),
}

/// Consumes as `ConsoleConsumer.run` does, writing records to `out`.
async fn consume(
    plan: Plan,
    format: OutputFormat,
    out: &mut dyn io::Write,
    cancel: &CancellationToken,
) -> Exit {
    let group = plan.group.clone().unwrap_or_else(generated_group);
    let settings = match ClientSettings::from_properties(&plan.properties, group) {
        Ok(settings) => settings,
        Err(message) => return fail(COMMAND, &unknown_error(&message), format),
    };
    let options =
        match ConnectionArgs::client_options(&plan.properties, &plan.bootstrap, "console-consumer")
        {
            Ok(options) => options,
            Err(error) => return fail(COMMAND, &unknown_error(&error.to_string()), format),
        };
    let started = tokio::select! {
        source = Stream::open(&plan, &settings, options) => source,
        () = cancel.cancelled() => {
            report_count(0, format);
            return Exit::Cancelled;
        }
    };
    let mut stream = match started {
        Ok(stream) => stream,
        Err(message) => return fail(COMMAND, &unknown_error(&message), format),
    };
    let (count, ended) = process(&plan, &mut stream, format, out, cancel).await;
    stream.close().await;
    report_count(count, format);
    match ended {
        Ended::Done => Exit::Success,
        Ended::Cancelled => {
            if plan.systest_events && format == OutputFormat::Human {
                let _ = writeln!(out, "shutdown_complete");
            }
            Exit::Cancelled
        }
        Ended::Failed(message) => fail(COMMAND, &unknown_error(&message), format),
    }
}

fn unknown_error(message: &str) -> String {
    format!("Unknown error when running consumer: {message}")
}

/// `reportRecordCount`. Under `--output json` stderr is kept for the error
/// envelope.
fn report_count(count: u64, format: OutputFormat) {
    if format == OutputFormat::Human {
        eprintln!("Processed a total of {count} messages");
    }
}

/// `ConsoleConsumer.process`: receive, count, format, write, until the bound.
async fn process(
    plan: &Plan,
    stream: &mut Stream,
    format: OutputFormat,
    out: &mut dyn io::Write,
    cancel: &CancellationToken,
) -> (u64, Ended) {
    let mut count: u64 = 0;
    let bound = u64::try_from(plan.max_messages).ok();
    loop {
        if plan.max_messages != -1 && bound.is_none_or(|bound| count >= bound) {
            return (count, Ended::Done);
        }
        let record = match stream.receive(plan.timeout_ms, cancel).await {
            Ok(record) => record,
            Err(Stop::Timeout) => {
                tracing::error!(
                    "Error processing message, terminating consumer process: TimeoutException"
                );
                return (count, Ended::Done);
            }
            Err(Stop::Cancelled) => return (count, Ended::Cancelled),
            Err(Stop::Failed(message)) => {
                tracing::error!(
                    "Error processing message, terminating consumer process: {message}"
                );
                return (count, Ended::Failed(message));
            }
        };
        count += 1;
        let written = match render(&plan.formatter, &record, format) {
            Ok(bytes) => out.write_all(&bytes).and_then(|()| out.flush()),
            Err(message) if plan.skip_message_on_error => {
                tracing::error!("Error processing message, skipping this message: {message}");
                Ok(())
            }
            Err(message) => return (count, Ended::Failed(message)),
        };
        if written.is_err() {
            eprintln!("Unable to write to standard out, closing consumer.");
            return (count, Ended::Done);
        }
        stream.processed(&record);
        stream.maybe_commit(false).await;
    }
}

/// The bytes written for `record`: the formatter's output, or under
/// `--output json` one `{"data": ...}` line.
fn render(formatter: &Formatter, record: &Record, format: OutputFormat) -> Result<Vec<u8>, String> {
    let (bytes, log) = formatter.format(record)?;
    if let Some(line) = log {
        tracing::info!("{line}");
    }
    match format {
        OutputFormat::Human => Ok(bytes),
        OutputFormat::Json if *formatter == Formatter::NoOp => Ok(Vec::new()),
        OutputFormat::Json => {
            let headers = record
                .headers
                .iter()
                .map(|(key, value)| {
                    Ok(json!({"key": key, "value": formatter.render_value(value.as_deref(), Field::Header)?}))
                })
                .collect::<Result<Vec<Value>, String>>()?;
            let data = json!({
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "timestamp": record.timestamp,
                "key": formatter.render_value(record.key.as_deref(), Field::Key)?,
                "value": formatter.render_value(record.value.as_deref(), Field::Value)?,
                "headers": headers,
            });
            let mut line =
                serde_json::to_vec(&json!({ "data": data })).map_err(|e| e.to_string())?;
            line.push(b'\n');
            Ok(line)
        }
    }
}

/// Why `receive` returned no record.
#[derive(Debug, PartialEq, Eq)]
enum Stop {
    Timeout,
    Cancelled,
    Failed(String),
}

/// A source of records and the records it has buffered.
struct Stream {
    reader: Reader,
    buffered: VecDeque<Record>,
}

enum Reader {
    Group(Box<GroupReader>),
    Partition(Box<PartitionReader>),
}

impl Stream {
    async fn open(
        plan: &Plan,
        settings: &ClientSettings,
        options: ConnectionOptions,
    ) -> Result<Self, String> {
        let reader = match &plan.source {
            Source::Topic(topic) => Reader::Group(Box::new(
                GroupReader::start(plan, settings, options, vec![topic.clone()]).await?,
            )),
            Source::Include(pattern) => {
                let topics = matching_topics(plan, &options, pattern).await?;
                Reader::Group(Box::new(
                    GroupReader::start(plan, settings, options, topics).await?,
                ))
            }
            Source::Partition {
                topic,
                partition,
                offset,
            } => Reader::Partition(Box::new(
                PartitionReader::start(plan, settings, options, topic, *partition, *offset).await?,
            )),
        };
        Ok(Self {
            reader,
            buffered: VecDeque::new(),
        })
    }

    /// `ConsumerWrapper.receive`: the next record, polling until one arrives
    /// or `timeout_ms` passes with none.
    async fn receive(
        &mut self,
        timeout_ms: Option<i64>,
        cancel: &CancellationToken,
    ) -> Result<Record, Stop> {
        let started = Instant::now();
        loop {
            if let Some(record) = self.buffered.pop_front() {
                return Ok(record);
            }
            let waited = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
            let slice = timeout_ms.map_or(POLL_SLICE_MS, |timeout| {
                (timeout - waited).clamp(0, POLL_SLICE_MS)
            });
            let records = tokio::select! {
                records = self.poll(Time::from_millis(slice)) => records.map_err(Stop::Failed)?,
                () = cancel.cancelled() => return Err(Stop::Cancelled),
            };
            self.buffered.extend(records);
            self.maybe_commit(false).await;
            let waited = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
            if self.buffered.is_empty() && timeout_ms.is_some_and(|timeout| waited > timeout) {
                return Err(Stop::Timeout);
            }
        }
    }

    async fn poll(&mut self, timeout: Time) -> Result<Vec<Record>, String> {
        match &mut self.reader {
            Reader::Group(reader) => reader.poll(timeout).await,
            Reader::Partition(reader) => reader.poll(timeout).await,
        }
    }

    fn processed(&mut self, record: &Record) {
        if let Reader::Group(reader) = &mut self.reader {
            reader
                .processed
                .insert((record.topic.clone(), record.partition), record.offset + 1);
        }
    }

    async fn maybe_commit(&mut self, closing: bool) {
        if let Reader::Group(reader) = &mut self.reader {
            reader.maybe_commit(closing).await;
        }
    }

    /// `ConsumerWrapper.cleanup`: commits what was processed, and never what
    /// was buffered but not processed, then leaves the group.
    async fn close(mut self) {
        self.maybe_commit(true).await;
        if let Reader::Group(reader) = self.reader
            && let Err(error) = reader.consumer.close().await
        {
            tracing::warn!("closing the consumer failed: {error}");
        }
    }
}

/// The subscribed path.
struct GroupReader {
    consumer: Consumer,
    auto_commit: Option<std::time::Duration>,
    last_commit: Instant,
    /// The next offset of each partition, one past the last record written.
    processed: BTreeMap<(String, i32), i64>,
    committed: BTreeMap<(String, i32), i64>,
}

impl GroupReader {
    async fn start(
        plan: &Plan,
        settings: &ClientSettings,
        options: ConnectionOptions,
        topics: Vec<String>,
    ) -> Result<Self, String> {
        let consumer = Consumer::builder()
            .bootstrap(plan.bootstrap.join(","))
            .client_id(options.client_id.clone())
            .group_id(settings.group_id.clone())
            .subscribe(topics)
            .auto_offset_reset(settings.auto_offset_reset.into())
            .isolation_level(settings.isolation_level)
            .assignors(settings.assignors.clone())
            .session_timeout(Time::from_millis(settings.session_timeout_ms))
            .heartbeat_interval(Time::from_millis(settings.heartbeat_interval_ms))
            .max_poll_interval(Time::from_millis(settings.rebalance_timeout_ms))
            .fetch_min(ByteSize::from_bytes_i64(settings.fetch_min_bytes))
            .fetch_max(ByteSize::from_bytes_i64(settings.fetch_max_bytes))
            .fetch_partition_max(ByteSize::from_bytes_i64(settings.max_partition_fetch_bytes))
            .request_timeout(options.request_timeout)
            .maybe_group_instance_id(settings.group_instance_id.clone())
            .maybe_client_rack(settings.client_rack.clone())
            .maybe_security(options.security.map(|security| *security))
            .build()
            .await
            .map_err(|error| consumer_error(&error))?;
        let interval = u64::try_from(settings.auto_commit_interval_ms).unwrap_or(0);
        Ok(Self {
            consumer,
            auto_commit: settings
                .enable_auto_commit
                .then(|| std::time::Duration::from_millis(interval)),
            last_commit: Instant::now(),
            processed: BTreeMap::new(),
            committed: BTreeMap::new(),
        })
    }

    async fn poll(&mut self, timeout: Time) -> Result<Vec<Record>, String> {
        let records = self
            .consumer
            .poll(timeout)
            .await
            .map_err(|error| consumer_error(&error))?;
        Ok(records
            .into_iter()
            .map(|record| Record {
                topic: record.topic,
                partition: record.partition,
                offset: record.offset,
                timestamp: record.timestamp,
                key: record.key.map(|key| key.to_vec()),
                value: record.value.map(|value| value.to_vec()),
                headers: record
                    .headers
                    .into_iter()
                    .map(|header| (header.key, header.value.map(|value| value.to_vec())))
                    .collect(),
            })
            .collect())
    }

    /// Commits the processed offsets of the partitions that this member
    /// still owns, every `auto.commit.interval.ms` and on close, when
    /// `enable.auto.commit` is on.
    async fn maybe_commit(&mut self, closing: bool) {
        let Some(interval) = self.auto_commit else {
            return;
        };
        if !closing && self.last_commit.elapsed() < interval {
            return;
        }
        self.last_commit = Instant::now();
        let assigned = self.consumer.assignment().await;
        let offsets = self
            .processed
            .iter()
            .filter(|(partition, offset)| {
                assigned.contains(partition) && self.committed.get(*partition) != Some(offset)
            })
            .map(|(partition, offset)| (partition.clone(), *offset))
            .collect::<HashMap<_, _>>();
        if offsets.is_empty() {
            return;
        }
        let commits = offsets
            .iter()
            .map(|(partition, offset)| (partition.clone(), OffsetAndMetadata::new(*offset)))
            .collect();
        match self.consumer.commit_offsets_sync(commits).await {
            Ok(()) => self.committed.extend(offsets),
            Err(error) => tracing::warn!("offset commit failed: {error}"),
        }
    }
}

/// The topics that `--include` matches now, internal topics excluded as
/// `exclude.internal.topics` excludes them by default.
///
/// The pinned `Consumer` subscribes to a fixed list, so the pattern is
/// resolved once, when the command starts.
async fn matching_topics(
    plan: &Plan,
    options: &ConnectionOptions,
    pattern: &str,
) -> Result<Vec<String>, String> {
    let pattern = java_pattern(pattern)?;
    let exclude_internal = bool_property(&plan.properties, "exclude.internal.topics", true)?;
    let client = metadata_client(plan, options).await?;
    let metadata = client
        .refresh_metadata()
        .await
        .map_err(|error| client_error(&error))?;
    client.close();
    let mut topics = metadata
        .topics
        .into_iter()
        .filter(|topic| !(exclude_internal && topic.is_internal))
        .filter_map(|topic| topic.name)
        .filter(|name| pattern.is_match(name))
        .collect::<Vec<_>>();
    topics.sort();
    if topics.is_empty() {
        return Err(format!(
            "no topic matches --include {pattern}; subscribing to topics created later is {NOT_SUPPORTED}, because pattern subscription needs Consumer::subscribe_regex from a newer krabka-client-consumer",
            pattern = pattern
                .as_str()
                .trim_start_matches("^(?:")
                .trim_end_matches(")$"),
        ));
    }
    Ok(topics)
}

async fn metadata_client(plan: &Plan, options: &ConnectionOptions) -> Result<Client, String> {
    Client::builder()
        .bootstrap(plan.bootstrap.join(","))
        .client_id(options.client_id.clone())
        .socket_connection_setup_timeout(options.socket_connection_setup_timeout)
        .request_timeout(options.request_timeout)
        .maybe_security(options.security.clone().map(|security| *security))
        .build()
        .await
        .map_err(|error| client_error(&error))
}

/// The `--partition` path: fetches from the partition leader and never
/// joins a group.
struct PartitionReader {
    client: Client,
    options: ConnectionOptions,
    bootstrap: Vec<String>,
    topic: String,
    partition: i32,
    next_offset: i64,
    started_at_earliest: bool,
    fetch: FetchBounds,
    connection: Option<Leader>,
}

struct Leader {
    connection: Connection,
    topic_id: Uuid,
}

/// The fetch bounds that the consumer properties set.
#[derive(Debug, Clone, Copy)]
struct FetchBounds {
    max_wait_ms: i64,
    max: ByteSize,
    partition_max: ByteSize,
    min: FetchMinBytes,
    isolation_level: i8,
}

impl PartitionReader {
    async fn start(
        plan: &Plan,
        settings: &ClientSettings,
        options: ConnectionOptions,
        topic: &str,
        partition: i32,
        offset: StartOffset,
    ) -> Result<Self, String> {
        let next_offset = match offset {
            // The log start is 0 until retention or DeleteRecords moves it;
            // a fetch at 0 past that point is refused below.
            StartOffset::Earliest => 0,
            StartOffset::At(offset) => offset,
            StartOffset::Latest => {
                return Err(format!(
                    "--offset latest, the default with --partition, is {NOT_SUPPORTED}: the log end offset needs AdminClient::list_offsets from a newer krabka-client-admin; pass --offset earliest or an offset"
                ));
            }
        };
        let fetch = FetchBounds {
            max_wait_ms: settings.fetch_max_wait_ms,
            max: ByteSize::from_bytes_i64(settings.fetch_max_bytes),
            partition_max: ByteSize::from_bytes_i64(settings.max_partition_fetch_bytes),
            min: FetchMinBytes::new(i32::try_from(settings.fetch_min_bytes).unwrap_or(i32::MAX))
                .map_err(|error| {
                    config_error(
                        "fetch.min.bytes",
                        &settings.fetch_min_bytes.to_string(),
                        &error,
                    )
                })?,
            isolation_level: match settings.isolation_level {
                IsolationLevel::ReadUncommitted => 0,
                IsolationLevel::ReadCommitted => 1,
            },
        };
        let client = metadata_client(plan, &options).await?;
        Ok(Self {
            client,
            options,
            bootstrap: plan.bootstrap.clone(),
            topic: topic.to_owned(),
            partition,
            next_offset,
            started_at_earliest: offset == StartOffset::Earliest,
            fetch,
            connection: None,
        })
    }

    /// Connects to the partition leader that the metadata names, or to the
    /// first bootstrap broker when the leader has no address that can be
    /// dialled.
    async fn connect(&mut self) -> Result<(), String> {
        let metadata = self
            .client
            .refresh_metadata()
            .await
            .map_err(|error| client_error(&error))?;
        let Some(topic) = metadata
            .topics
            .iter()
            .find(|topic| topic.name.as_deref() == Some(self.topic.as_str()))
        else {
            return Err(format!("Topic {} not present in metadata", self.topic));
        };
        if topic.error_code != 0 {
            return Err(format!(
                "Metadata for topic {} failed with error code {}",
                self.topic, topic.error_code
            ));
        }
        let Some(partition) = topic
            .partitions
            .iter()
            .find(|partition| partition.partition_index == self.partition)
        else {
            return Err(format!(
                "Partition {}-{} not present in metadata",
                self.topic, self.partition
            ));
        };
        let leader = metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == partition.leader_id && broker.port > 0)
            .map(|broker| format!("{}:{}", broker.host, broker.port));
        let address = match leader {
            Some(address) => address,
            None => self.bootstrap.first().cloned().unwrap_or_default(),
        };
        let address = resolve(&address).await?;
        let connection = Connection::connect_with_options(address, self.options.clone())
            .await
            .map_err(|error| client_error(&error))?;
        self.connection = Some(Leader {
            connection,
            topic_id: topic.topic_id,
        });
        Ok(())
    }

    async fn poll(&mut self, timeout: Time) -> Result<Vec<Record>, String> {
        if self.connection.is_none() {
            self.connect().await?;
        }
        let Some(leader) = &self.connection else {
            return Ok(Vec::new());
        };
        let max_wait = timeout.millis_i64().min(self.fetch.max_wait_ms).max(0);
        // A broker that does not answer must not hold the loop past
        // `--timeout-ms`: the fetch gets its wait and a grace period, and an
        // unanswered one reconnects.
        let deadline =
            std::time::Duration::from_millis(u64::try_from(max_wait).unwrap_or(0) + FETCH_GRACE_MS);
        let fetch = fetch_partition_with_isolation_progress(
            &leader.connection,
            IsolatedFetch {
                topic: &self.topic,
                topic_id: leader.topic_id,
                partition: self.partition,
                fetch_offset: self.next_offset,
                max_wait: Time::from_millis(max_wait),
                max: self.fetch.max,
                partition_max: self.fetch.partition_max,
                fetch_min: self.fetch.min,
                isolation_level: self.fetch.isolation_level,
            },
        );
        let result = match tokio::time::timeout(deadline, fetch).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => return self.recover(&error).await,
            Err(_) => {
                self.reconnect_later().await;
                return Ok(Vec::new());
            }
        };
        if let Some(next) = result.next_offset {
            self.next_offset = self.next_offset.max(next);
        }
        Ok(result
            .records
            .into_iter()
            .map(|record| Record {
                topic: self.topic.clone(),
                partition: self.partition,
                offset: record.offset,
                timestamp: record.timestamp,
                key: record.key.map(|key| key.to_vec()),
                value: record.value.map(|value| value.to_vec()),
                headers: record
                    .headers
                    .into_iter()
                    .map(|header| (header.key, header.value.map(|value| value.to_vec())))
                    .collect(),
            })
            .collect())
    }

    /// Reconnects after a leader change or a lost connection, and refuses an
    /// offset that is out of range.
    async fn recover(&mut self, error: &ClientError) -> Result<Vec<Record>, String> {
        const OFFSET_OUT_OF_RANGE: i16 = 1;
        const RETRIABLE: [i16; 5] = [3, 5, 6, 74, 75];
        match error {
            ClientError::Server {
                error_code: OFFSET_OUT_OF_RANGE,
            } => Err(if self.started_at_earliest && self.next_offset == 0 {
                format!(
                    "the log start of {}-{} is past offset 0, and finding it is {NOT_SUPPORTED}: it needs AdminClient::list_offsets from a newer krabka-client-admin; pass --offset with an offset in range",
                    self.topic, self.partition
                )
            } else {
                format!(
                    "offset {} is out of range for {}-{}, and resetting it is {NOT_SUPPORTED}: it needs AdminClient::list_offsets from a newer krabka-client-admin",
                    self.next_offset, self.topic, self.partition
                )
            }),
            ClientError::Server { error_code } if RETRIABLE.contains(error_code) => {
                self.reconnect_later().await;
                Ok(Vec::new())
            }
            ClientError::Connect { .. }
            | ClientError::Disconnected
            | ClientError::Timeout(_)
            | ClientError::Io(_) => {
                self.reconnect_later().await;
                Ok(Vec::new())
            }
            other => Err(client_error(other)),
        }
    }

    async fn reconnect_later(&mut self) {
        if let Some(leader) = self.connection.take() {
            leader.connection.close();
        }
        tokio::time::sleep(std::time::Duration::from_millis(RETRY_BACKOFF_MS)).await;
    }
}

async fn resolve(address: &str) -> Result<SocketAddr, String> {
    tokio::net::lookup_host(address)
        .await
        .map_err(|error| format!("resolve {address}: {error}"))?
        .next()
        .ok_or_else(|| format!("resolve {address}: no address"))
}

fn consumer_error(error: &ConsumerError) -> String {
    match error {
        ConsumerError::Client(error) => client_error(error),
        ConsumerError::StartupAfterJoin(error) => consumer_error(error),
        other => other.to_string(),
    }
}

fn client_error(error: &ClientError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests;
