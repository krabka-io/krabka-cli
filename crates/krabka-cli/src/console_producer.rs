//! `krabka console-producer`, the counterpart of `kafka-console-producer`.
//!
//! The flags are `ConsoleProducerOptions`' at Kafka 4.3.1, deprecated
//! spellings included, checked in the order and with the messages that the
//! JVM tool uses. Each line of stdin is one record, split into headers, key
//! and value as `LineMessageReader` splits it.
//!
//! One behaviour differs on purpose. Without `--sync` the JVM tool logs a
//! failed send and still exits 0; krabka logs it the same way and exits 1,
//! because a record that was not written is a failure of the command.

mod reader;

use std::{
    io::{self, IsTerminal as _, Write as _},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use clap::Args;
use krabka_client_producer::{
    Acks, Compression, Producer, ProducerError, ProducerRecord, RecordSizeLimit,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use serde_json::json;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, BufReader};
use tokio_util::sync::CancellationToken;

pub(crate) use self::reader::LineReader;
use self::reader::split_lines;
use crate::{
    connection::{ConnectionArgs, Properties},
    console::{
        bool_property, cancel_on_ctrl_c, config_error, fail, key_value_args, load_properties,
        number_property, overlay, warn,
    },
    exit::Exit,
    output::{CommandResult, OutputFormat, emit_error, emit_success},
};

const COMMAND: &str = "krabka console-producer";
const LINE_READER: &str = "org.apache.kafka.tools.LineMessageReader";

/// The flags of `kafka-console-producer`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Args)]
pub struct ConsoleProducerArgs {
    /// REQUIRED: The topic name to produce messages to.
    #[arg(long)]
    topic: Option<String>,
    /// REQUIRED: The server(s) to connect to. The broker list string in the
    /// form HOST1:PORT1,HOST2:PORT2.
    #[arg(long, env = "KRABKA_BOOTSTRAP_SERVER")]
    bootstrap_server: Option<String>,
    /// If set message send requests to the brokers are synchronously, one at
    /// a time as they arrive.
    #[arg(long)]
    sync: bool,
    /// The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or
    /// 'zstd'. If specified without value, then it defaults to 'gzip'.
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    compression_codec: Option<String>,
    /// The buffer size in bytes allocated for a partition: `batch.size`.
    #[arg(long, allow_hyphen_values = true)]
    batch_size: Option<i32>,
    /// The number of retries before the producer gives up and drops a
    /// message: `retries`.
    #[arg(long, allow_hyphen_values = true)]
    message_send_max_retries: Option<i32>,
    /// The time the producer waits before a retry: `retry.backoff.ms`.
    #[arg(long, allow_hyphen_values = true)]
    retry_backoff_ms: Option<i64>,
    /// The maximum time in ms a message waits for a batch to fill:
    /// `linger.ms`.
    #[arg(long, allow_hyphen_values = true)]
    timeout: Option<i64>,
    /// The required `acks` of the producer requests.
    #[arg(long, allow_hyphen_values = true)]
    request_required_acks: Option<String>,
    /// The ack timeout of the producer requests. Value must be non-negative
    /// and non-zero.
    #[arg(long, allow_hyphen_values = true)]
    request_timeout_ms: Option<i32>,
    /// The period after which metadata is refreshed: `metadata.max.age.ms`.
    #[arg(long, allow_hyphen_values = true)]
    metadata_expiry_ms: Option<i64>,
    /// The max time that the producer will block for during a send request.
    #[arg(long, allow_hyphen_values = true)]
    max_block_ms: Option<i64>,
    /// The total memory used by the producer to buffer records:
    /// `buffer.memory`.
    #[arg(long, allow_hyphen_values = true)]
    max_memory_bytes: Option<i64>,
    /// (Deprecated) The buffer size in bytes allocated for a partition. Use
    /// --batch-size instead.
    #[arg(long, allow_hyphen_values = true)]
    max_partition_memory_bytes: Option<i32>,
    /// The class name of the class to use for reading lines from standard in.
    #[arg(long, default_value = LINE_READER)]
    line_reader: String,
    /// The size of the tcp RECV size: `send.buffer.bytes`.
    #[arg(long, allow_hyphen_values = true)]
    socket_buffer_size: Option<i32>,
    /// (DEPRECATED) Properties for the message reader. Use --reader-property
    /// instead.
    #[arg(long)]
    property: Vec<String>,
    /// Properties for the message reader: parse.key, parse.headers,
    /// ignore.error, key.separator, headers.delimiter, headers.separator,
    /// headers.key.separator and null.marker.
    #[arg(long)]
    reader_property: Vec<String>,
    /// Config properties file for the message reader. Note that
    /// --reader-property takes precedence over this config.
    #[arg(long)]
    reader_config: Option<PathBuf>,
    /// (DEPRECATED) Producer config properties in the form key=value. Use
    /// --command-property instead.
    #[arg(long)]
    producer_property: Vec<String>,
    /// Producer config properties in the form key=value.
    #[arg(long)]
    command_property: Vec<String>,
    /// (DEPRECATED) Producer config properties file. Use --command-config
    /// instead.
    #[arg(long = "producer.config")]
    producer_config: Option<PathBuf>,
    /// Producer config properties file. Note that --command-property takes
    /// precedence over this config.
    #[arg(long)]
    command_config: Option<PathBuf>,
}

/// Why the command stopped before it produced.
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

/// The producer settings, validated as Kafka's `ProducerConfig` validates
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Settings {
    compression: Compression,
    acks: Acks,
    enable_idempotence: bool,
    linger_ms: u64,
    batch_size: usize,
    request_timeout_ms: u64,
    retries: i32,
    retry_backoff_ms: u64,
    retry_backoff_max_ms: u64,
    max_block_ms: u64,
    delivery_timeout_ms: u64,
    buffer_memory: u64,
    max_request_size: u64,
    max_in_flight: u64,
    metadata_max_age_ms: u64,
    metadata_max_idle_ms: u64,
    enable_metrics_push: bool,
    /// `send.buffer.bytes`; `None` for -1.
    send_buffer: Option<u64>,
    /// `receive.buffer.bytes`; `None` for -1.
    receive_buffer: Option<u64>,
}

/// The command line, checked and resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    bootstrap: Vec<String>,
    topic: String,
    sync: bool,
    /// The merged producer properties, as `producerProps` builds them.
    properties: Properties,
    reader: LineReader,
    /// The deprecation warnings that the JVM tool prints.
    warnings: Vec<String>,
}

impl ConsoleProducerArgs {
    /// Checks the command line as `ConsoleProducerOptions.checkArgs` does.
    fn plan(&self) -> Result<Plan, Refusal> {
        let usage = |message: &str| Refusal::Usage(message.to_owned());
        let Some(topic) = &self.topic else {
            return Err(usage("Missing required argument \"[topic]\""));
        };
        if self.command_config.is_some() && self.producer_config.is_some() {
            return Err(usage(
                "Options --command-config and --producer.config cannot be specified together.",
            ));
        }
        if !self.command_property.is_empty() && !self.producer_property.is_empty() {
            return Err(usage(
                "Options --command-property and --producer-property cannot be specified together.",
            ));
        }
        if !self.reader_property.is_empty() && !self.property.is_empty() {
            return Err(usage(
                "Options --reader-property and --property cannot be specified together.",
            ));
        }
        let mut warnings = Vec::new();
        for (given, message) in [
            (
                !self.producer_property.is_empty(),
                "Warning: --producer-property is deprecated and will be removed in a future version. Use --command-property instead.",
            ),
            (
                self.producer_config.is_some(),
                "Warning: --producer.config is deprecated and will be removed in a future version. Use --command-config instead.",
            ),
            (
                !self.property.is_empty(),
                "Warning: --property is deprecated and will be removed in a future version. Use --reader-property instead.",
            ),
            (
                self.max_partition_memory_bytes.is_some(),
                "Warning: --max-partition-memory-bytes is deprecated and will be removed in Apache Kafka 5.0. Use --batch-size instead.",
            ),
        ] {
            if given {
                warnings.push(message.to_owned());
            }
        }
        let bootstrap =
            validate_bootstrap(self.bootstrap_server.as_deref()).map_err(Refusal::Usage)?;
        if self.line_reader != LINE_READER && self.line_reader != "LineMessageReader" {
            return Err(Refusal::Failure(format!(
                "{}: no such reader; krabka builds in {LINE_READER}",
                self.line_reader
            )));
        }
        let reader =
            LineReader::configure(&self.reader_properties()?, topic).map_err(Refusal::Failure)?;
        Ok(Plan {
            bootstrap,
            topic: topic.clone(),
            sync: self.sync,
            properties: self.producer_properties()?,
            reader,
            warnings,
        })
    }

    /// `readerProps`: the reader config, then `topic`, then the reader
    /// properties.
    fn reader_properties(&self) -> Result<Properties, Refusal> {
        let mut properties = self
            .reader_config
            .as_ref()
            .map(|path| load_properties(path))
            .transpose()
            .map_err(Refusal::Failure)?
            .unwrap_or_default();
        if let Some(topic) = &self.topic {
            properties.insert("topic", topic.as_str());
        }
        let arguments = if self.property.is_empty() {
            &self.reader_property
        } else {
            &self.property
        };
        Ok(overlay(properties, &key_value_args(arguments)))
    }

    /// `producerProps`.
    fn producer_properties(&self) -> Result<Properties, Refusal> {
        let from_file = self
            .producer_config
            .as_ref()
            .or(self.command_config.as_ref())
            .map(|path| load_properties(path))
            .transpose()
            .map_err(Refusal::Failure)?
            .unwrap_or_default();
        let extra = key_value_args(if self.producer_property.is_empty() {
            &self.command_property
        } else {
            &self.producer_property
        });
        let mut properties = overlay(from_file, &extra);
        if let Some(bootstrap) = &self.bootstrap_server {
            properties.insert("bootstrap.servers", bootstrap.as_str());
        }
        let compression = match self.compression_codec.as_deref() {
            None => "none",
            Some("") => "gzip",
            Some(codec) => codec,
        };
        properties.insert("compression.type", compression);
        if properties.get("client.id").is_none() {
            properties.insert("client.id", "console-producer");
        }
        let merge =
            |properties: &mut Properties, key: &str, flag: Option<String>, default: &str| {
                if flag.is_some() || properties.get(key).is_none() {
                    properties.insert(key, flag.as_deref().unwrap_or(default));
                }
            };
        let text = |value: Option<i64>| value.map(|value| value.to_string());
        merge(&mut properties, "linger.ms", text(self.timeout), "1000");
        merge(
            &mut properties,
            "acks",
            self.request_required_acks.clone(),
            "-1",
        );
        merge(
            &mut properties,
            "request.timeout.ms",
            text(self.request_timeout_ms.map(i64::from)),
            "1500",
        );
        merge(
            &mut properties,
            "retries",
            text(self.message_send_max_retries.map(i64::from)),
            "3",
        );
        merge(
            &mut properties,
            "retry.backoff.ms",
            text(self.retry_backoff_ms),
            "100",
        );
        merge(
            &mut properties,
            "send.buffer.bytes",
            text(self.socket_buffer_size.map(i64::from)),
            "102400",
        );
        merge(
            &mut properties,
            "buffer.memory",
            text(self.max_memory_bytes),
            "33554432",
        );
        merge(
            &mut properties,
            "batch.size",
            text(self.batch_size.map(i64::from)),
            "16384",
        );
        merge(
            &mut properties,
            "batch.size",
            text(self.max_partition_memory_bytes.map(i64::from)),
            "16384",
        );
        merge(
            &mut properties,
            "metadata.max.age.ms",
            text(self.metadata_expiry_ms),
            "300000",
        );
        merge(
            &mut properties,
            "max.block.ms",
            text(self.max_block_ms),
            "60000",
        );
        Ok(properties)
    }
}

/// `ToolsUtils.validateBootstrapServer`: every comma-separated entry is
/// `host:port`, with an optional `scheme://` and an optional `[` `]` around
/// the host.
fn validate_bootstrap(bootstrap: Option<&str>) -> Result<Vec<String>, String> {
    let Some(bootstrap) = bootstrap.filter(|value| !value.trim().is_empty()) else {
        return Err("Error while validating the bootstrap address".to_owned());
    };
    let entries = bootstrap.split(',').map(str::to_owned).collect::<Vec<_>>();
    if entries.iter().all(|entry| has_port(entry)) {
        Ok(entries)
    } else {
        Err("Please provide valid host:port like host1:9091,host2:9092".to_owned())
    }
}

/// `Utils.getPort(address) != null`: the whole entry matches
/// `^(?:[0-9a-zA-Z\-%._]*://)?\[?([0-9a-zA-Z\-%._:]*)]?:([0-9]+)`.
fn has_port(address: &str) -> bool {
    let host_char = |c: char| c.is_ascii_alphanumeric() || "-%._".contains(c);
    let rest = match address.split_once("://") {
        Some((scheme, rest)) if scheme.chars().all(host_char) => rest,
        _ => address,
    };
    let Some((host, port)) = rest.rsplit_once(':') else {
        return false;
    };
    let host = host.strip_prefix('[').unwrap_or(host);
    let host = host.strip_suffix(']').unwrap_or(host);
    !port.is_empty()
        && port.chars().all(|c| c.is_ascii_digit())
        && host.chars().all(|c| host_char(c) || c == ':')
}

impl Settings {
    /// Reads and validates the settings, as `new KafkaProducer` does.
    fn from_properties(properties: &Properties) -> Result<Self, String> {
        if properties.get("transactional.id").is_some() {
            return Err(
                "Cannot perform a 'send' before completing a call to initTransactions when transactions are enabled."
                    .to_owned(),
            );
        }
        let compression = properties.get("compression.type").unwrap_or("none");
        let compression = compression.parse::<Compression>().map_err(|_| {
            config_error(
                "compression.type",
                compression,
                "String must be one of: none, gzip, snappy, lz4, zstd",
            )
        })?;
        let acks_text = properties.get("acks").unwrap_or("all");
        let acks = match acks_text {
            "all" | "-1" => Acks::All,
            "1" => Acks::One,
            "0" => Acks::Zero,
            other => {
                return Err(config_error(
                    "acks",
                    other,
                    "String must be one of: all, -1, 0, 1",
                ));
            }
        };
        let int = |name: &str, default: i64| number_property(properties, name, "INT", default);
        let long = |name: &str, default: i64| number_property(properties, name, "LONG", default);
        let at_least = |name: &str, value: i64, min: i64| {
            if value < min {
                Err(config_error(
                    name,
                    &value.to_string(),
                    &format!("Value must be at least {min}"),
                ))
            } else {
                Ok(u64::try_from(value).unwrap_or(0))
            }
        };
        // `send.buffer.bytes` and `receive.buffer.bytes` are at least -1, and
        // -1 keeps the operating system's socket buffer.
        let socket_buffer = |name: &str, default: i64| -> Result<Option<u64>, String> {
            let value = int(name, default)?;
            if value < -1 {
                return Err(config_error(
                    name,
                    &value.to_string(),
                    "Value must be at least -1",
                ));
            }
            Ok(u64::try_from(value).ok())
        };
        let send_buffer = socket_buffer("send.buffer.bytes", 131_072)?;
        let receive_buffer = socket_buffer("receive.buffer.bytes", 32_768)?;
        let retries = int("retries", i64::from(i32::MAX))?;
        let retries = i32::try_from(at_least("retries", retries, 0)?).unwrap_or(i32::MAX);
        let enable_idempotence = idempotence(properties, acks, retries)?;
        let linger_ms = at_least("linger.ms", long("linger.ms", 5)?, 0)?;
        let batch_size = at_least("batch.size", int("batch.size", 16_384)?, 0)?;
        let request_timeout_ms =
            at_least("request.timeout.ms", int("request.timeout.ms", 30_000)?, 0)?;
        let retry_backoff_ms = at_least("retry.backoff.ms", long("retry.backoff.ms", 100)?, 0)?;
        let retry_backoff_max_ms = at_least(
            "retry.backoff.max.ms",
            long("retry.backoff.max.ms", 1_000)?,
            0,
        )?;
        let max_block_ms = at_least("max.block.ms", long("max.block.ms", 60_000)?, 0)?;
        let delivery_timeout_ms = at_least(
            "delivery.timeout.ms",
            int("delivery.timeout.ms", 120_000)?,
            0,
        )?;
        let buffer_memory = at_least("buffer.memory", long("buffer.memory", 33_554_432)?, 0)?;
        let max_request_size =
            at_least("max.request.size", int("max.request.size", 1_048_576)?, 0)?;
        let max_in_flight = at_least(
            "max.in.flight.requests.per.connection",
            int("max.in.flight.requests.per.connection", 5)?,
            1,
        )?;
        if enable_idempotence && max_in_flight > 5 {
            return Err(format!(
                "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is {max_in_flight}."
            ));
        }
        let metadata_max_age_ms = at_least(
            "metadata.max.age.ms",
            long("metadata.max.age.ms", 300_000)?,
            0,
        )?;
        let metadata_max_idle_ms = at_least(
            "metadata.max.idle.ms",
            long("metadata.max.idle.ms", 300_000)?,
            5_000,
        )?;
        Ok(Self {
            compression,
            acks,
            enable_idempotence,
            linger_ms,
            // A zero batch size disables batching; krabka's smallest batch
            // holds one record.
            batch_size: usize::try_from(batch_size.max(1)).unwrap_or(usize::MAX),
            request_timeout_ms,
            retries,
            retry_backoff_ms,
            retry_backoff_max_ms,
            max_block_ms,
            delivery_timeout_ms,
            buffer_memory,
            max_request_size,
            max_in_flight,
            metadata_max_age_ms,
            metadata_max_idle_ms,
            enable_metrics_push: bool_property(properties, "enable.metrics.push", true)?,
            send_buffer,
            receive_buffer,
        })
    }
}

/// `postProcessAndValidateIdempotenceConfigs`: idempotence is on unless it is
/// turned off, and it turns itself off for `acks` other than `all` or for
/// `retries=0` unless it was asked for.
fn idempotence(properties: &Properties, acks: Acks, retries: i32) -> Result<bool, String> {
    let configured = properties.get("enable.idempotence").is_some();
    let enabled = bool_property(properties, "enable.idempotence", true)?;
    if !enabled {
        return Ok(false);
    }
    if retries == 0 {
        if configured {
            return Err(
                "Must set retries to non-zero when using the idempotent producer.".to_owned(),
            );
        }
        tracing::info!("Idempotence will be disabled because retries is set to 0.");
        return Ok(false);
    }
    if acks != Acks::All {
        if configured {
            return Err("Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.".to_owned());
        }
        tracing::info!(
            "Idempotence will be disabled because acks is set to {}, not set to 'all'.",
            acks.wire()
        );
        return Ok(false);
    }
    Ok(true)
}

/// Runs `console-producer`: reads stdin, sends each line, flushes on EOF.
pub(crate) async fn run(args: ConsoleProducerArgs, format: OutputFormat) -> Exit {
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
    let (cancel, watcher) = cancel_on_ctrl_c();
    let prompt =
        format == OutputFormat::Human && io::stdin().is_terminal() && io::stdout().is_terminal();
    let input = BufReader::new(tokio::io::stdin());
    let exit = produce(plan, input, prompt, format, &cancel).await;
    watcher.abort();
    exit
}

/// How many records were sent and failed.
#[derive(Debug, Default)]
struct Counts {
    sent: AtomicU64,
    failed: AtomicU64,
}

/// Starts the producer, sends every line of `input`, and closes it.
async fn produce(
    plan: Plan,
    mut input: impl AsyncBufRead + Unpin,
    prompt: bool,
    format: OutputFormat,
    cancel: &CancellationToken,
) -> Exit {
    let settings = match Settings::from_properties(&plan.properties) {
        Ok(settings) => settings,
        Err(message) => return fail(COMMAND, &message, format),
    };
    let options =
        match ConnectionArgs::client_options(&plan.properties, &plan.bootstrap, "console-producer")
        {
            Ok(options) => options,
            Err(error) => return fail(COMMAND, &error.to_string(), format),
        };
    let millis = Duration::from_millis;
    let size = |bytes: u64| usize::try_from(bytes).unwrap_or(usize::MAX);
    let started = Producer::builder()
        .bootstrap(plan.bootstrap.join(","))
        .client_id(options.client_id.clone())
        .compression(settings.compression)
        .enable_idempotence(settings.enable_idempotence)
        .acks(settings.acks)
        .linger(millis(settings.linger_ms))
        .batch_size(settings.batch_size)
        .request_timeout(millis(settings.request_timeout_ms))
        .retries(settings.retries)
        .retry_backoff(millis(settings.retry_backoff_ms))
        .retry_backoff_max(millis(settings.retry_backoff_max_ms))
        .delivery_timeout(millis(settings.delivery_timeout_ms))
        .flush_timeout(millis(settings.delivery_timeout_ms.max(1)))
        .max_block(millis(settings.max_block_ms))
        .buffer_memory(size(settings.buffer_memory))
        .max_request_size(size(settings.max_request_size))
        .max_in_flight_per_connection(size(settings.max_in_flight))
        .metadata_max_age(millis(settings.metadata_max_age_ms))
        .metadata_max_idle(millis(settings.metadata_max_idle_ms))
        .enable_metrics_push(settings.enable_metrics_push)
        .send_buffer(settings.send_buffer.map(ByteSize::from_bytes))
        .receive_buffer(settings.receive_buffer.map(ByteSize::from_bytes))
        .maybe_security(options.security.clone().map(|security| *security))
        .build();
    let producer = tokio::select! {
        producer = started => producer,
        () = cancel.cancelled() => return Exit::Cancelled,
    };
    let producer = match producer {
        Ok(producer) => producer,
        Err(error) => return fail(COMMAND, &producer_error(&error), format),
    };
    let counts = Arc::new(Counts::default());
    let mut reader = plan.reader.clone();
    let mut pending = tokio::task::JoinSet::new();
    let mut failure = None;
    let mut cancelled = false;
    'read: loop {
        if prompt {
            print!(">");
            let _ = io::stdout().flush();
        }
        let mut chunk = Vec::new();
        let read = tokio::select! {
            read = input.read_until(b'\n', &mut chunk) => read,
            () = cancel.cancelled() => { cancelled = true; break; }
        };
        match read {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) => {
                failure = Some(format!("read standard input: {error}"));
                break;
            }
        }
        for line in split_lines(&chunk) {
            let record = match reader.record(&String::from_utf8_lossy(line)) {
                Ok(record) => record,
                Err(message) => {
                    failure = Some(message);
                    break 'read;
                }
            };
            let described = describe(&record);
            // `enqueue` waits up to `max.block.ms` for metadata that holds the
            // topic, as `KafkaProducer.waitOnMetadata` does, and fails the
            // record when it times out.
            let queued = tokio::select! {
                queued = producer.enqueue(record) => queued,
                () = cancel.cancelled() => { cancelled = true; break 'read; }
            };
            let delivery = async move {
                match queued {
                    Ok(delivery) => delivery.await,
                    Err(error) => Err(error),
                }
            };
            if plan.sync {
                if let Err(message) = delivered(delivery.await, &counts) {
                    failure = Some(message);
                    break 'read;
                }
            } else {
                let counts = Arc::clone(&counts);
                pending.spawn(async move {
                    if let Err(message) = delivered(delivery.await, &counts) {
                        tracing::error!(
                            "Error when sending message to {described} with error: {message}"
                        );
                    }
                });
            }
        }
    }
    let closed = producer.close().await;
    while pending.join_next().await.is_some() {}
    let sent = counts.sent.load(Ordering::Acquire);
    let failed = counts.failed.load(Ordering::Acquire);
    if let (None, Err(error)) = (&failure, closed) {
        failure = Some(producer_error(&error));
    }
    if let Some(message) = failure {
        return fail(COMMAND, &message, format);
    }
    if cancelled {
        return Exit::Cancelled;
    }
    if format == OutputFormat::Json {
        let result = CommandResult::rows(
            Vec::new(),
            json!({"topic": plan.topic, "sent": sent, "failed": failed}),
            failed > 0,
        );
        let _ = emit_success(&result, format);
    }
    if failed > 0 {
        return fail(
            COMMAND,
            &format!("{failed} of {} records failed to send", sent + failed),
            format,
        );
    }
    Exit::Success
}

/// The record as `ErrorLoggingCallback` describes it: sizes, not contents.
fn describe(record: &ProducerRecord) -> String {
    let size = |bytes: Option<&[u8]>| {
        bytes.map_or_else(
            || "null".to_owned(),
            |bytes| format!("{} bytes", bytes.len()),
        )
    };
    format!(
        "topic {} with key: {}, value: {}",
        record.topic,
        size(record.key.as_deref()),
        size(record.value.as_deref())
    )
}

/// Counts the outcome of one send.
fn delivered(
    outcome: Result<krabka_client_producer::RecordMetadata, ProducerError>,
    counts: &Counts,
) -> Result<(), String> {
    let result = match outcome {
        Ok(_) => Ok(()),
        Err(error) => Err(producer_error(&error)),
    };
    match result {
        Ok(()) => counts.sent.fetch_add(1, Ordering::AcqRel),
        Err(_) => counts.failed.fetch_add(1, Ordering::AcqRel),
    };
    result
}

fn producer_error(error: &ProducerError) -> String {
    match error {
        ProducerError::Server(code) => match error_name(*code) {
            Some(name) => format!("{name} ({code})"),
            None => format!("broker error code {code}"),
        },
        // `KafkaProducer.ensureValidRecordSize` names the configs as Kafka
        // spells them.
        ProducerError::RecordTooLarge {
            record_size,
            limit: RecordSizeLimit::MaxRequestSize(max),
        } => format!(
            "The message is {record_size} bytes when serialized which is larger than {max}, which is the value of the max.request.size configuration."
        ),
        ProducerError::RecordTooLarge {
            record_size,
            limit: RecordSizeLimit::BufferMemory,
        } => format!(
            "The message is {record_size} bytes when serialized which is larger than the total memory buffer you have configured with the buffer.memory configuration."
        ),
        other => other.to_string(),
    }
}

/// The Kafka name of an error code that a produce can return.
fn error_name(code: i16) -> Option<&'static str> {
    Some(match code {
        2 => "CORRUPT_MESSAGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        10 => "MESSAGE_TOO_LARGE",
        17 => "INVALID_TOPIC_EXCEPTION",
        18 => "RECORD_LIST_TOO_LARGE",
        19 => "NOT_ENOUGH_REPLICAS",
        20 => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        21 => "INVALID_REQUIRED_ACKS",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        31 => "CLUSTER_AUTHORIZATION_FAILED",
        32 => "INVALID_TIMESTAMP",
        43 => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        45 => "OUT_OF_ORDER_SEQUENCE_NUMBER",
        47 => "INVALID_PRODUCER_EPOCH",
        53 => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        59 => "UNKNOWN_PRODUCER_ID",
        76 => "UNSUPPORTED_COMPRESSION_TYPE",
        87 => "INVALID_RECORD",
        _ => return None,
    })
}

#[cfg(test)]
mod tests;
