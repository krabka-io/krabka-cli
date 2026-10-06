//! `krabka storage`, the counterpart of `kafka-storage`.
//!
//! # Command shape
//!
//! `kafka-storage` at Kafka 4.3.1 is one tool with five subcommands: `info`,
//! `format`, `version-mapping`, `feature-dependencies` and `random-uuid`.
//! `krabka storage` has the same five, with the same names and the same short
//! flags (`-c`, `-r`, `-f`). An operator then replaces `kafka-storage.sh` with
//! `krabka storage` and keeps the rest of the command line. Four siblings of
//! `format` at the top level would put `version-mapping` beside
//! `krabka features version-mapping`, with two command lines for one lookup.
//! `krabka storage format` takes `krabka format`'s flags, because the flags of
//! the formatter are `krabka-format`'s.
//!
//! # Divergences
//!
//! - `info` reads Kafka's `meta.properties`, which `krabka format` writes,
//!   and prints Kafka's report layout. It adds one `Found features:` line with
//!   the feature levels that the bootstrap records of the metadata log
//!   directory finalize. A file that is
//!   not version 1, or whose ids are not Kafka `Uuid`s, is a problem, because
//!   the broker and `krabka format` refuse it. The broker takes its configuration from flags and the environment, so
//!   `info` takes `--log-dir` as well as Kafka's `--config`, and it reads
//!   `log.dirs`, `log.dir` and `metadata.log.dir` from a `--config` file as
//!   `kafka-storage` does.
//! - `version-mapping --all` prints every level, and `feature-dependencies`
//!   without `--feature` prints the whole dependency graph. Both are krabka
//!   additions. The Kafka forms print what `kafka-storage` prints.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use clap::{ArgGroup, Args, Subcommand};
use krabka_format::{
    ClusterId, DirectoryId, META_PROPERTIES, META_PROPERTIES_VERSION, MetaProperties,
};
use krabka_ids::KafkaUuid;
use krabka_metadata::{
    MetadataRecord, feature_registry, from_kafka_record, metadata_version::KRAFT_VERSION_FEATURE,
};
use krabka_protocol::records::Record;
use serde_json::{Value, json};

use crate::{
    connection::Properties,
    feature_catalog::{Dialect, FeatureDependenciesArgs, VersionMappingArgs},
    output::{CommandError, CommandResult},
};

/// The bootstrap records, as `u32` little-endian length-prefixed payloads.
const BOOTSTRAP_RECORDS: &str = "bootstrap.records.bin";
/// Kafka's `log.dir` default, which applies when a `--config` file sets
/// neither `log.dirs` nor `log.dir`.
const DEFAULT_LOG_DIR: &str = "/tmp/kafka-logs";

#[derive(Debug, Args)]
pub struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    /// Get information about the log directories on this node.
    Info(InfoArgs),
    /// Format the log directories on this node.
    Format(Box<krabka_format::FormatArgs>),
    /// Look up the corresponding features for a given metadata version. With
    /// no --release-version, print the mapping of the latest metadata version.
    VersionMapping(VersionMappingArgs),
    /// Look up dependencies for a given feature version. An unknown feature or
    /// an undefined version is an error. --feature can repeat.
    FeatureDependencies(FeatureDependenciesArgs),
    /// Print a random UUID.
    RandomUuid,
}

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("directories").required(true).args(["config", "log_dir"])))]
struct InfoArgs {
    /// A Kafka configuration file. `info` reads `log.dirs`, `log.dir` and
    /// `metadata.log.dir` from it.
    #[arg(short = 'c', long, conflicts_with = "log_dir")]
    config: Option<PathBuf>,
    /// A log directory to report on. Comma-separated, and the flag can repeat.
    #[arg(long, value_delimiter = ',')]
    log_dir: Vec<PathBuf>,
}

impl StorageArgs {
    /// The formatter's arguments when the subcommand is `format`, which runs
    /// `krabka-format` and exits with its code, or `self` otherwise.
    ///
    /// # Errors
    /// Returns `self` when the subcommand is not `format`.
    pub fn into_format(self) -> Result<krabka_format::FormatArgs, Box<Self>> {
        match self.command {
            StorageCommand::Format(args) => Ok(*args),
            command => Err(Box::new(Self { command })),
        }
    }

    pub async fn run(self) -> Result<CommandResult, CommandError> {
        match self.command {
            StorageCommand::Info(args) => {
                let directories = args.directories().await?;
                Ok(info(&directories))
            }
            StorageCommand::VersionMapping(args) => args.run(),
            StorageCommand::FeatureDependencies(args) => args.run(Dialect::Storage),
            StorageCommand::RandomUuid => Ok(random_uuid(KafkaUuid::random())),
            StorageCommand::Format(_) => {
                Err("krabka storage format runs through krabka-format".into())
            }
        }
    }
}

impl InfoArgs {
    /// The directories to report on, sorted and without duplicates, as
    /// `StorageTool.configToLogDirectories` collects them.
    async fn directories(&self) -> Result<Vec<PathBuf>, CommandError> {
        let Some(path) = &self.config else {
            return Ok(self
                .log_dir
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect());
        };
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        let properties = Properties::parse(&bytes).map_err(|error| error.to_string())?;
        Ok(config_directories(&properties))
    }
}

/// `log.dirs`, else `log.dir`, else Kafka's default, then `metadata.log.dir`.
fn config_directories(properties: &Properties) -> Vec<PathBuf> {
    let log_dirs = properties
        .get("log.dirs")
        .or_else(|| properties.get("log.dir"))
        .unwrap_or(DEFAULT_LOG_DIR);
    log_dirs
        .split(',')
        .map(str::trim)
        .filter(|dir| !dir.is_empty())
        .chain(properties.get("metadata.log.dir"))
        .map(PathBuf::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// What one directory holds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LogDir {
    Missing,
    NotADirectory,
    Unformatted,
    Formatted(Formatted),
    Unreadable(String),
}

/// `random-uuid`: the id in Kafka's `Uuid.toString` form, 22 unpadded
/// base64url characters.
fn random_uuid(id: KafkaUuid) -> CommandResult {
    let id = id.to_string();
    CommandResult::success(vec![id.clone()], json!({"uuid": id}))
}

/// The identity and the bootstrap feature levels of a formatted directory.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Formatted {
    cluster_id: ClusterId,
    node_id: i32,
    /// Absent from a file that Kafka wrote before KIP-858, which Kafka still
    /// reads.
    directory_id: Option<DirectoryId>,
    /// Every registered feature except `kraft.version`, at the level that the
    /// bootstrap records finalize. A feature that has no record is at level
    /// 0, as Kafka treats an absent feature. `None` for a directory without
    /// bootstrap records: `krabka format` writes them only into the metadata
    /// log directory.
    features: Option<BTreeMap<String, i16>>,
}

fn inspect(path: &Path) -> LogDir {
    if !path.is_dir() {
        return if path.exists() {
            LogDir::NotADirectory
        } else {
            LogDir::Missing
        };
    }
    let meta = path.join(META_PROPERTIES);
    if !meta.exists() {
        return LogDir::Unformatted;
    }
    read_formatted(path).unwrap_or_else(LogDir::Unreadable)
}

fn read_formatted(path: &Path) -> Result<LogDir, String> {
    let loading = |file: &Path, error: &dyn std::fmt::Display| {
        format!("Error loading {}: {error}", file.display())
    };
    let meta = path.join(META_PROPERTIES);
    let properties = MetaProperties::read(path)
        .map_err(|error| loading(&meta, &error))?
        .ok_or_else(|| loading(&meta, &"file not found"))?;
    let records_path = path.join(BOOTSTRAP_RECORDS);
    let features = match std::fs::read(&records_path) {
        Ok(bytes) => Some(feature_levels(
            &decode_records(&bytes).map_err(|error| loading(&records_path, &error))?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(loading(&records_path, &error)),
    };
    Ok(LogDir::Formatted(Formatted {
        cluster_id: properties.cluster_id,
        node_id: properties.node_id,
        directory_id: properties.directory_id,
        features,
    }))
}

/// Splits `bootstrap.records.bin` into its records.
fn decode_records(mut bytes: &[u8]) -> Result<Vec<MetadataRecord>, String> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        let (prefix, rest) = bytes
            .split_first_chunk::<4>()
            .ok_or("truncated length prefix")?;
        let length = usize::try_from(u32::from_le_bytes(*prefix))
            .map_err(|_| "record length does not fit in memory")?;
        if rest.len() < length {
            return Err("truncated record body".into());
        }
        let (payload, rest) = rest.split_at(length);
        let record = Record {
            value: Some(payload.to_vec().into()),
            ..Record::default()
        };
        records.push(from_kafka_record(&record).map_err(|error| error.to_string())?);
        bytes = rest;
    }
    Ok(records)
}

fn feature_levels(records: &[MetadataRecord]) -> BTreeMap<String, i16> {
    let finalized = records
        .iter()
        .filter_map(|record| match record {
            MetadataRecord::V1FeatureLevel(feature) => Some((feature.name.as_str(), feature.level)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    feature_registry()
        .iter()
        .map(|feature| feature.name())
        .filter(|name| *name != KRAFT_VERSION_FEATURE)
        .map(|name| (name.to_owned(), finalized.get(name).copied().unwrap_or(0)))
        .collect()
}

fn braces(entries: impl Iterator<Item = String>) -> String {
    format!("{{{}}}", entries.collect::<Vec<_>>().join(", "))
}

fn heading(one: &str, many: &str, count: usize) -> String {
    if count == 1 { one } else { many }.to_owned()
}

/// `StorageTool.infoCommand` over `directories`.
fn info(directories: &[PathBuf]) -> CommandResult {
    if directories.is_empty() {
        return CommandResult::success(
            vec!["No directories specified.".to_owned()],
            json!({"directories": [], "problems": []}),
        );
    }
    let inspected = directories
        .iter()
        .map(|path| (path, inspect(path)))
        .collect::<Vec<_>>();
    let mut problems = Vec::new();
    let mut first: Option<&Formatted> = None;
    for (path, directory) in &inspected {
        let display = path.display();
        match directory {
            LogDir::Missing => problems.push(format!("{display} does not exist")),
            LogDir::NotADirectory => problems.push(format!("{display} is not a directory")),
            LogDir::Unformatted => problems.push(format!("{display} is not formatted.")),
            LogDir::Unreadable(message) => problems.push(message.clone()),
            LogDir::Formatted(formatted) => match first {
                None => first = Some(formatted),
                Some(first) if first.cluster_id != formatted.cluster_id => {
                    problems.push("Mismatched cluster IDs between storage directories.".to_owned());
                }
                Some(_) => {}
            },
        }
    }
    let found = inspected
        .iter()
        .filter(|(_, directory)| !matches!(directory, LogDir::Missing | LogDir::NotADirectory))
        .map(|(path, _)| path.display().to_string())
        .collect::<Vec<_>>();

    let mut human = Vec::new();
    if !found.is_empty() {
        human.push(heading(
            "Found log directory:",
            "Found log directories:",
            found.len(),
        ));
        human.extend(found.iter().map(|path| format!("  {path}")));
        human.push(String::new());
    }
    if let Some(first) = first {
        // Kafka prints `MetaProperties.toProperties` through a `TreeMap`,
        // so the keys are in their natural order.
        human.push(format!(
            "Found metadata: {}",
            braces(
                [
                    Some(format!("cluster.id={}", first.cluster_id)),
                    first.directory_id.map(|id| format!("directory.id={id}")),
                    Some(format!("node.id={}", first.node_id)),
                    Some(format!("version={META_PROPERTIES_VERSION}")),
                ]
                .into_iter()
                .flatten()
            )
        ));
        let features = inspected.iter().find_map(|(_, directory)| match directory {
            LogDir::Formatted(formatted) => formatted.features.as_ref(),
            _ => None,
        });
        if let Some(features) = features {
            human.push(format!(
                "Found features: {}",
                braces(
                    features
                        .iter()
                        .map(|(name, level)| format!("{name}={level}"))
                )
            ));
        }
        human.push(String::new());
    }
    if !problems.is_empty() {
        human.push(heading("Found problem:", "Found problems:", problems.len()));
        human.extend(problems.iter().map(|problem| format!("  {problem}")));
        human.push(String::new());
    }
    let data = json!({
        "directories": inspected
            .iter()
            .map(|(path, directory)| directory_json(path, directory))
            .collect::<Vec<_>>(),
        "problems": problems,
    });
    CommandResult::rows(human, data, !problems.is_empty())
}

fn directory_json(path: &Path, directory: &LogDir) -> Value {
    let path = path.display().to_string();
    match directory {
        LogDir::Missing => json!({"path": path, "status": "missing"}),
        LogDir::NotADirectory => json!({"path": path, "status": "not_a_directory"}),
        LogDir::Unformatted => json!({"path": path, "status": "unformatted"}),
        LogDir::Unreadable(message) => {
            json!({"path": path, "status": "unreadable", "error": message})
        }
        LogDir::Formatted(formatted) => json!({
            "path": path,
            "status": "formatted",
            "cluster_id": formatted.cluster_id.to_string(),
            "node_id": formatted.node_id,
            "directory_id": formatted.directory_id.map(|id| id.to_string()),
            "version": META_PROPERTIES_VERSION,
            "features": formatted.features,
        }),
    }
}

#[cfg(test)]
mod tests;
