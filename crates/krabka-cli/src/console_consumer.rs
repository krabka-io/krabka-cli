//! `krabka console-consumer`, the counterpart of `kafka-console-consumer`.
//!
//! The flags are `ConsoleConsumerOptions`' at Kafka 4.3.1, deprecated
//! spellings included, and they are checked in the order and with the
//! messages that the JVM tool uses. Records go to stdout in the shape the
//! `--formatter` class writes them. Everything else goes to stderr, so
//! `krabka console-consumer ... | wc -l` counts records.
//!
//! Both paths consume with `Consumer` from `krabka-client-consumer`, as the
//! JVM tool uses one `KafkaConsumer`. `--topic` subscribes and `--include`
//! subscribes to a pattern, so a topic created later joins the subscription;
//! both join the group that `--group` names or a generated
//! `console-consumer-<n>`. `--partition` assigns the partition and seeks, as
//! `ConsumerWrapper.seek` does, and joins no group.

mod formatter;

use std::{
    collections::{BTreeMap, VecDeque},
    hash::{BuildHasher as _, Hasher as _},
    io,
    path::PathBuf,
    time::Instant,
};

use clap::Args;
use krabka_client_consumer::{
    Assignor, AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord, GroupProtocol,
    IsolationLevel, TopicPattern,
};
use krabka_client_core::ConnectionOptions;
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
/// The line that `maybePrintConsumerProtocolMessage` prints.
const PROTOCOL_MESSAGE: &str = "The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol";
/// How long one poll waits before the loop checks `--timeout-ms` and Ctrl-C
/// again.
const POLL_SLICE_MS: i64 = 1_000;

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
    /// Assign one partition, without joining a group.
    Partition {
        topic: String,
        partition: i32,
        offset: StartOffset,
    },
}

/// How the consumer's metadata requests treat topics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TopicSettings {
    /// `exclude.internal.topics`: a pattern does not match an internal topic.
    exclude_internal: bool,
    /// `allow.auto.create.topics`.
    allow_auto_create: bool,
}

/// The consumer settings that krabka reads from the merged client
/// properties, validated as Kafka's `ConsumerConfig` validates them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientSettings {
    group_id: String,
    auto_offset_reset: AutoOffsetReset,
    isolation_level: IsolationLevel,
    group_protocol: GroupProtocol,
    group_remote_assignor: Option<String>,
    enable_auto_commit: bool,
    auto_commit_interval_ms: i64,
    session_timeout_ms: i64,
    heartbeat_interval_ms: i64,
    max_poll_interval_ms: i64,
    max_poll_records: i64,
    fetch_min_bytes: i64,
    fetch_max_bytes: i64,
    max_partition_fetch_bytes: i64,
    fetch_max_wait_ms: i64,
    metadata_max_age_ms: i64,
    default_api_timeout_ms: i64,
    topics: TopicSettings,
    enable_metrics_push: bool,
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

/// The configs that `ConsumerConfig.checkUnsupportedConfigsPostProcess`
/// refuses under each `group.protocol`, in Kafka's order.
const CLASSIC_UNSUPPORTED: [&str; 3] = [
    "group.remote.assignor",
    "share.acknowledgement.mode",
    "share.acquire.mode",
];
const CONSUMER_UNSUPPORTED: [&str; 5] = [
    "partition.assignment.strategy",
    "heartbeat.interval.ms",
    "session.timeout.ms",
    "share.acknowledgement.mode",
    "share.acquire.mode",
];

impl ClientSettings {
    /// Reads and validates the settings, as `new KafkaConsumer` does.
    fn from_properties(properties: &Properties, group_id: String) -> Result<Self, String> {
        let reset = properties.get("auto.offset.reset").unwrap_or("latest");
        let auto_offset_reset = reset.parse::<AutoOffsetReset>().map_err(|_| {
            config_error(
                "auto.offset.reset",
                reset,
                &format!(
                    "Invalid value `{reset}` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'."
                ),
            )
        })?;
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
        let group_protocol = group_protocol(properties)?;
        let int = |name: &str, default: i64| number_property(properties, name, "INT", default);
        let long = |name: &str, default: i64| number_property(properties, name, "LONG", default);
        let text = |name: &str| {
            properties
                .get(name)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        };
        Ok(Self {
            group_id,
            auto_offset_reset,
            isolation_level,
            group_protocol,
            group_remote_assignor: text("group.remote.assignor"),
            enable_auto_commit: bool_property(properties, "enable.auto.commit", true)?,
            auto_commit_interval_ms: int("auto.commit.interval.ms", 5_000)?,
            session_timeout_ms: int("session.timeout.ms", 45_000)?,
            heartbeat_interval_ms: int("heartbeat.interval.ms", 3_000)?,
            max_poll_interval_ms: int("max.poll.interval.ms", 300_000)?,
            max_poll_records: int("max.poll.records", 500)?,
            fetch_min_bytes: int("fetch.min.bytes", 1)?,
            fetch_max_bytes: int("fetch.max.bytes", 52_428_800)?,
            max_partition_fetch_bytes: int("max.partition.fetch.bytes", 1_048_576)?,
            fetch_max_wait_ms: int("fetch.max.wait.ms", 500)?,
            metadata_max_age_ms: long("metadata.max.age.ms", 300_000)?,
            default_api_timeout_ms: int("default.api.timeout.ms", 60_000)?,
            topics: TopicSettings {
                exclude_internal: bool_property(properties, "exclude.internal.topics", true)?,
                allow_auto_create: bool_property(properties, "allow.auto.create.topics", true)?,
            },
            enable_metrics_push: bool_property(properties, "enable.metrics.push", true)?,
            group_instance_id: text("group.instance.id"),
            client_rack: text("client.rack"),
            assignors: assignors(properties.get("partition.assignment.strategy"))?,
        })
    }
}

/// `group.protocol`, case insensitive, and the configs that the protocol
/// does not take.
fn group_protocol(properties: &Properties) -> Result<GroupProtocol, String> {
    let protocol = properties.get("group.protocol").unwrap_or("classic");
    let (protocol, unsupported, name) = if protocol.eq_ignore_ascii_case("classic") {
        (GroupProtocol::Classic, &CLASSIC_UNSUPPORTED[..], "CLASSIC")
    } else if protocol.eq_ignore_ascii_case("consumer") {
        (
            GroupProtocol::Consumer,
            &CONSUMER_UNSUPPORTED[..],
            "CONSUMER",
        )
    } else {
        return Err(config_error(
            "group.protocol",
            protocol,
            "String must be one of (case insensitive): CLASSIC, CONSUMER",
        ));
    };
    let set = unsupported
        .iter()
        .copied()
        .filter(|name| {
            properties
                .get(name)
                .is_some_and(|value| !value.trim().is_empty())
        })
        .collect::<Vec<_>>();
    if set.is_empty() {
        Ok(protocol)
    } else {
        Err(format!(
            "{} cannot be set when group.protocol={name}",
            set.join(", ")
        ))
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
    maybe_print_protocol_message(&plan.source, settings.group_protocol, format);
    let started = tokio::select! {
        source = Stream::open(&plan.source, &plan.bootstrap, &settings, options) => source,
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

/// `maybePrintConsumerProtocolMessage`: a subscribed run of the classic
/// protocol points at KIP-848 on stderr, as the JVM tool does under its
/// default log level. Under `--output json` stderr is kept for the error
/// envelope.
fn maybe_print_protocol_message(source: &Source, protocol: GroupProtocol, format: OutputFormat) {
    if format == OutputFormat::Human
        && protocol == GroupProtocol::Classic
        && !matches!(source, Source::Partition { .. })
    {
        eprintln!("{PROTOCOL_MESSAGE}");
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

/// `ConsumerWrapper`: the consumer and the records that the last poll
/// returned and the loop has not processed yet.
struct Stream {
    consumer: Consumer,
    buffered: VecDeque<Record>,
}

impl Stream {
    /// `new KafkaConsumer` and the `ConsumerWrapper` constructor: subscribe
    /// to the topic or the pattern, or assign the partition and seek.
    async fn open(
        source: &Source,
        bootstrap: &[String],
        settings: &ClientSettings,
        options: ConnectionOptions,
    ) -> Result<Self, String> {
        let (subscribe, pattern) = match source {
            Source::Topic(topic) => (vec![topic.clone()], None),
            Source::Include(pattern) => {
                let pattern = java_pattern(pattern)?;
                let matcher = TopicPattern::new(move |topic| pattern.is_match(topic));
                (Vec::new(), Some(matcher))
            }
            Source::Partition { .. } => (Vec::new(), None),
        };
        // Kafka's consumer finds the group coordinator of a manual assignment
        // only to commit, so the `--partition` path names its generated group
        // only when a property turns `enable.auto.commit` on.
        let group_id = match source {
            Source::Partition { .. } if !settings.enable_auto_commit => None,
            _ => Some(settings.group_id.clone()),
        };
        let millis = Time::from_millis;
        let bytes = ByteSize::from_bytes_i64;
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.join(","))
            .client_id(options.client_id.clone())
            .maybe_group_id(group_id)
            .subscribe(subscribe)
            .maybe_subscribe_pattern(pattern)
            .exclude_internal_topics(settings.topics.exclude_internal)
            .allow_auto_create_topics(settings.topics.allow_auto_create)
            .group_protocol(settings.group_protocol)
            .maybe_group_remote_assignor(settings.group_remote_assignor.clone())
            .auto_offset_reset(settings.auto_offset_reset)
            .isolation_level(settings.isolation_level)
            .assignors(settings.assignors.clone())
            .session_timeout(millis(settings.session_timeout_ms))
            .heartbeat_interval(millis(settings.heartbeat_interval_ms))
            .max_poll_interval(millis(settings.max_poll_interval_ms))
            .max_poll_records(usize::try_from(settings.max_poll_records).unwrap_or(usize::MAX))
            .fetch_min(bytes(settings.fetch_min_bytes))
            .fetch_max(bytes(settings.fetch_max_bytes))
            .fetch_partition_max(bytes(settings.max_partition_fetch_bytes))
            .fetch_max_wait(millis(settings.fetch_max_wait_ms))
            .metadata_max_age(millis(settings.metadata_max_age_ms))
            // Kafka's consumer sees a topic that starts to match `--include`,
            // or a partition added to a subscribed topic, at its next metadata
            // refresh, which `metadata.max.age.ms` schedules.
            .subscription_metadata_refresh_interval(millis(settings.metadata_max_age_ms.max(1)))
            .default_api_timeout(millis(settings.default_api_timeout_ms))
            .request_timeout(options.request_timeout)
            .socket_connection_setup_timeout(options.socket_connection_setup_timeout)
            .enable_auto_commit(settings.enable_auto_commit)
            .auto_commit_interval(millis(settings.auto_commit_interval_ms))
            .enable_metrics_push(settings.enable_metrics_push)
            .maybe_group_instance_id(settings.group_instance_id.clone())
            .maybe_client_rack(settings.client_rack.clone())
            .maybe_security(options.security.map(|security| *security))
            .build()
            .await
            .map_err(|error| consumer_error(&error))?;
        if let Source::Partition {
            topic,
            partition,
            offset,
        } = source
        {
            // `ConsumerWrapper.seek`.
            let assigned = [(topic.clone(), *partition)];
            let sought = async {
                consumer.assign(&assigned).await?;
                match offset {
                    StartOffset::Earliest => consumer.seek_to_beginning(&assigned).await,
                    StartOffset::Latest => consumer.seek_to_end(&assigned).await,
                    StartOffset::At(offset) => {
                        consumer.seek(topic.clone(), *partition, *offset).await
                    }
                }
            };
            if let Err(error) = sought.await {
                let message = consumer_error(&error);
                let _ = consumer.close().await;
                return Err(message);
            }
        }
        Ok(Self {
            consumer,
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
                records = self.consumer.poll(Time::from_millis(slice)) => {
                    records.map_err(|error| Stop::Failed(consumer_error(&error)))?
                }
                () = cancel.cancelled() => return Err(Stop::Cancelled),
            };
            self.buffered.extend(records.into_iter().map(record));
            let waited = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
            if self.buffered.is_empty() && timeout_ms.is_some_and(|timeout| waited > timeout) {
                return Err(Stop::Timeout);
            }
        }
    }

    /// `ConsumerWrapper.cleanup`: seeks each partition back to its first
    /// record that was polled but not processed, so the commit on close
    /// commits only what was processed, then closes the consumer.
    async fn close(self) {
        for (topic, partition, offset) in unconsumed(&self.buffered) {
            if let Err(error) = self.consumer.seek(topic, partition, offset).await {
                tracing::warn!("resetting an unconsumed offset failed: {error}");
            }
        }
        if let Err(error) = self.consumer.close().await {
            tracing::warn!("closing the consumer failed: {error}");
        }
    }
}

/// `resetUnconsumedOffsets`: the smallest offset of each partition among the
/// records that were polled but not processed.
fn unconsumed(buffered: &VecDeque<Record>) -> Vec<(String, i32, i64)> {
    let mut smallest = BTreeMap::new();
    for record in buffered {
        smallest
            .entry((record.topic.clone(), record.partition))
            .or_insert(record.offset);
    }
    smallest
        .into_iter()
        .map(|((topic, partition), offset)| (topic, partition, offset))
        .collect()
}

/// The formatter's view of a consumed record.
fn record(record: ConsumerRecord) -> Record {
    Record {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        timestamp: record.timestamp,
        timestamp_type: record.timestamp_type,
        key: record.key.map(|key| key.to_vec()),
        value: record.value.map(|value| value.to_vec()),
        headers: record
            .headers
            .into_iter()
            .map(|header| (header.key, header.value.map(|value| value.to_vec())))
            .collect(),
    }
}

fn consumer_error(error: &ConsumerError) -> String {
    match error {
        ConsumerError::Client(error) => error.to_string(),
        ConsumerError::StartupAfterJoin(error) => consumer_error(error),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests;
