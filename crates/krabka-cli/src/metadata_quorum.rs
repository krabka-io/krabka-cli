//! `krabka metadata-quorum`, the counterpart of `kafka-metadata-quorum`.
//!
//! The connection flags come before the action, as they do for
//! `kafka-metadata-quorum`: `krabka metadata-quorum --bootstrap-server
//! host:9092 describe --replication`.

use std::{
    collections::BTreeMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Args, Subcommand};
use krabka_client_admin::{MetadataQuorum, QuorumReplica};
use krabka_ids::KafkaUuid;
use serde_json::{Value, json};

use crate::{
    cluster,
    common::unsupported,
    connection::{ConnectionArgs, Properties},
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, confirm},
};

#[derive(Debug, Args)]
pub struct MetadataQuorumArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[command(subcommand)]
    command: QuorumCommand,
}

#[derive(Debug, Subcommand)]
enum QuorumCommand {
    /// Describe the metadata quorum info.
    Describe(DescribeArgs),
    /// Add a controller to the `KRaft` controller cluster.
    AddController(AddControllerArgs),
    /// Remove a controller from the `KRaft` controller cluster.
    RemoveController(RemoveControllerArgs),
}

#[derive(Debug, Args)]
struct DescribeArgs {
    /// A short summary of the quorum status.
    #[arg(long)]
    status: bool,
    /// Detailed information about the status of replication.
    #[arg(long)]
    replication: bool,
    /// Human-readable output.
    #[arg(long)]
    human_readable: bool,
}

#[derive(Debug, Args)]
struct AddControllerArgs {
    #[command(flatten)]
    confirm: ConfirmArgs,
}

#[derive(Debug, Args)]
struct RemoveControllerArgs {
    /// The id of the controller to remove.
    #[arg(short = 'i', long, allow_negative_numbers = true)]
    controller_id: i32,
    /// The directory ID of the controller to remove.
    #[arg(short = 'd', long, allow_hyphen_values = true)]
    controller_directory_id: String,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

impl MetadataQuorumArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        match self.command {
            QuorumCommand::Describe(args) => describe(&self.connection, &args).await,
            QuorumCommand::AddController(args) => add_controller(&self.connection, args).await,
            QuorumCommand::RemoveController(args) => {
                remove_controller(&self.connection, args).await
            }
        }
    }
}

/// Which describe report the flags select, with `kafka-metadata-quorum`'s
/// refusals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Report {
    Status,
    Replication { human_readable: bool },
}

fn report(args: &DescribeArgs) -> Result<Report, String> {
    match (args.status, args.replication) {
        (true, true) => Err(
            "Only one of --status or --replication should be specified with describe sub-command"
                .into(),
        ),
        (false, true) => Ok(Report::Replication {
            human_readable: args.human_readable,
        }),
        (true, false) if args.human_readable => {
            Err("The option --human-readable is only supported along with --replication".into())
        }
        (true, false) => Ok(Report::Status),
        (false, false) => Err(
            "One of --status or --replication must be specified with describe sub-command".into(),
        ),
    }
}

async fn describe(
    connection: &ConnectionArgs,
    args: &DescribeArgs,
) -> Result<CommandResult, CommandError> {
    match report(args)? {
        Report::Status => {
            let mut client = connection.connect("metadata-quorum").await?;
            // `kafka-metadata-quorum describe --status` reads the cluster ID
            // from `DescribeCluster` before it reads the quorum.
            let cluster_id = cluster::cluster_id(&mut client, "metadata-quorum describe --status")?;
            let quorum = client.describe_metadata_quorum().await?;
            let cluster_id = cluster_id.unwrap_or_else(|| "null".into());
            let human = status_lines(&cluster_id, &quorum, &NodeEndpoints::new())?;
            Ok(CommandResult::success(
                human,
                status_json(&cluster_id, &quorum),
            ))
        }
        Report::Replication { human_readable } => {
            let mut client = connection.connect("metadata-quorum").await?;
            let quorum = client.describe_metadata_quorum().await?;
            let now = human_readable.then(now_ms);
            let rows = replication_rows(&quorum, now)?;
            let human = pretty_table(&REPLICATION_HEADERS, &rows);
            let data = rows
                .iter()
                .map(|row| {
                    json!({
                        "node_id": row[0],
                        "directory_id": row[1],
                        "log_end_offset": row[2],
                        "lag": row[3],
                        "last_fetch_timestamp": row[4],
                        "last_caught_up_timestamp": row[5],
                        "status": row[6],
                    })
                })
                .collect::<Vec<_>>();
            Ok(CommandResult::success(human, data))
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

const REPLICATION_HEADERS: [&str; 7] = [
    "NodeId",
    "DirectoryId",
    "LogEndOffset",
    "Lag",
    "LastFetchTimestamp",
    "LastCaughtUpTimestamp",
    "Status",
];

/// The leader's replica state, which Kafka finds among the voters.
fn leader(quorum: &MetadataQuorum) -> Result<&QuorumReplica, String> {
    quorum
        .voters
        .iter()
        .find(|voter| voter.node_id == quorum.leader_id)
        .ok_or_else(|| "No value present".to_owned())
}

/// The rows of `describe --replication`: the leader, the other voters, then
/// the observers. With `now`, the timestamps are relative, as
/// `--human-readable` prints them.
fn replication_rows(quorum: &MetadataQuorum, now: Option<i64>) -> Result<Vec<[String; 7]>, String> {
    let leader = leader(quorum)?;
    let followers = quorum
        .voters
        .iter()
        .filter(|voter| voter.node_id != quorum.leader_id);
    std::iter::once((leader, "Leader"))
        .chain(followers.map(|voter| (voter, "Follower")))
        .chain(
            quorum
                .observers
                .iter()
                .map(|observer| (observer, "Observer")),
        )
        .map(|(replica, status)| {
            Ok([
                replica.node_id.to_string(),
                KafkaUuid(replica.directory_id).to_string(),
                replica.log_end_offset.to_string(),
                (leader.log_end_offset - replica.log_end_offset).to_string(),
                timestamp(replica.last_fetch_timestamp, now, "last fetch")?,
                timestamp(replica.last_caught_up_timestamp, now, "last caught up")?,
                status.to_owned(),
            ])
        })
        .collect()
}

/// One timestamp column: `-1` when unknown, the epoch milliseconds, or
/// `<n> ms ago` under `--human-readable`.
fn timestamp(value: i64, now: Option<i64>, description: &str) -> Result<String, String> {
    match now {
        _ if value == -1 => Ok("-1".into()),
        None => Ok(value.to_string()),
        Some(now) if value > 0 && value <= now => Ok(format!("{} ms ago", now - value)),
        Some(now) => Err(format!(
            "Error while computing relative time, possible drift in system clock.\nCurrent \
             timestamp is {now}, {description} timestamp is {value}"
        )),
    }
}

/// Kafka's `ToolsUtils.prettyPrintTable`: each cell left-justified to the
/// widest cell of its column and followed by a tab.
fn pretty_table<const N: usize>(headers: &[&str; N], rows: &[[String; N]]) -> Vec<String> {
    let mut widths = headers.map(|header| header.chars().count());
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |cells: &mut dyn Iterator<Item = &str>| {
        let mut line = String::new();
        for (cell, width) in cells.zip(widths) {
            line.push_str(cell);
            line.extend(std::iter::repeat_n(
                ' ',
                width.saturating_sub(cell.chars().count()),
            ));
            line.push('\t');
        }
        line
    };
    std::iter::once(line(&mut headers.iter().copied()))
        .chain(
            rows.iter()
                .map(|row| line(&mut row.iter().map(String::as_str))),
        )
        .collect()
}

/// The advertised listeners of a quorum node, as `QuorumInfo.Node`.
type NodeEndpoints = BTreeMap<i32, Vec<VoterEndpoint>>;

/// The lines of `describe --status`.
///
/// `nodes` holds the endpoints of each node, which the pinned
/// `MetadataQuorum` does not carry yet; a node without endpoints prints none,
/// as Kafka prints a node that `DescribeQuorum` did not describe.
fn status_lines(
    cluster_id: &str,
    quorum: &MetadataQuorum,
    nodes: &NodeEndpoints,
) -> Result<Vec<String>, String> {
    let leader = leader(quorum)?;
    let max_lag_follower = quorum
        .voters
        .iter()
        .enumerate()
        .reduce(|min, voter| {
            if voter.1.log_end_offset < min.1.log_end_offset {
                voter
            } else {
                min
            }
        })
        .ok_or_else(|| "No value present".to_owned())?;
    let max_follower_lag = leader.log_end_offset - max_lag_follower.1.log_end_offset;
    let leader_index = quorum
        .voters
        .iter()
        .position(|voter| voter.node_id == quorum.leader_id);
    let max_follower_lag_time_ms = if leader_index == Some(max_lag_follower.0) {
        0
    } else if leader.last_caught_up_timestamp != -1
        && max_lag_follower.1.last_caught_up_timestamp != -1
    {
        leader.last_caught_up_timestamp - max_lag_follower.1.last_caught_up_timestamp
    } else {
        -1
    };
    Ok(vec![
        format!("ClusterId:              {cluster_id}"),
        format!("LeaderId:               {}", quorum.leader_id),
        format!("LeaderEpoch:            {}", quorum.leader_epoch),
        format!("HighWatermark:          {}", quorum.high_watermark),
        format!("MaxFollowerLag:         {max_follower_lag}"),
        format!("MaxFollowerLagTimeMs:   {max_follower_lag_time_ms}"),
        format!(
            "CurrentVoters:          {}",
            replica_list(&quorum.voters, nodes)
        ),
        format!(
            "CurrentObservers:       {}",
            replica_list(&quorum.observers, nodes)
        ),
    ])
}

/// The JSON rendering of `describe --status`.
fn status_json(cluster_id: &str, quorum: &MetadataQuorum) -> Value {
    let replicas = |replicas: &[QuorumReplica]| {
        replicas
            .iter()
            .map(|replica| {
                json!({
                    "id": replica.node_id,
                    "directory_id": KafkaUuid(replica.directory_id).to_string(),
                    "log_end_offset": replica.log_end_offset,
                })
            })
            .collect::<Vec<_>>()
    };
    json!({
        "cluster_id": cluster_id,
        "leader_id": quorum.leader_id,
        "leader_epoch": quorum.leader_epoch,
        "high_watermark": quorum.high_watermark,
        "current_voters": replicas(&quorum.voters),
        "current_observers": replicas(&quorum.observers),
    })
}

/// `[{"id": 1, "directoryId": "...", "endpoints": ["CONTROLLER://h:9093"]}, ...]`,
/// as `MetadataQuorumCommand.Node.toString` renders each replica.
fn replica_list(replicas: &[QuorumReplica], nodes: &NodeEndpoints) -> String {
    let entries = replicas
        .iter()
        .map(|replica| {
            let mut entry = format!("{{\"id\": {}", replica.node_id);
            let directory_id = KafkaUuid(replica.directory_id);
            if directory_id != KafkaUuid::ZERO {
                entry.push_str(", \"directoryId\": \"");
                entry.push_str(&directory_id.to_string());
                entry.push('"');
            }
            if let Some(endpoints) = nodes.get(&replica.node_id).filter(|e| !e.is_empty()) {
                let endpoints = endpoints
                    .iter()
                    .map(|endpoint| format!("\"{endpoint}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                entry.push_str(", \"endpoints\": [");
                entry.push_str(&endpoints);
                entry.push(']');
            }
            entry.push('}');
            entry
        })
        .collect::<Vec<_>>();
    format!("[{}]", entries.join(", "))
}

/// One listener endpoint of a voter, as Kafka's `RaftVoterEndpoint`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VoterEndpoint {
    listener: String,
    host: String,
    port: i32,
}

impl std::fmt::Display for VoterEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(f, "{}://[{}]:{}", self.listener, self.host, self.port)
        } else {
            write!(f, "{}://{}:{}", self.listener, self.host, self.port)
        }
    }
}

/// The identity that `add-controller` reads from the controller's
/// configuration file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NewController {
    id: i32,
    directory_id: KafkaUuid,
    endpoints: Vec<VoterEndpoint>,
}

async fn add_controller(
    connection: &ConnectionArgs,
    args: AddControllerArgs,
) -> Result<CommandResult, CommandError> {
    let Some(path) = &connection.command_config else {
        return Err(
            "You must supply the configuration file of the controller you are adding \
                    when using add-controller."
                .into(),
        );
    };
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|_| format!("Properties file {} does not exists!", path.display()))?;
    let properties = Properties::parse(&bytes).map_err(|error| error.to_string())?;
    let controller = new_controller(&properties, &read_meta_properties)?;
    let data = json!({
        "controller_id": controller.id,
        "directory_id": controller.directory_id.to_string(),
        "endpoints": controller.endpoints.iter().map(ToString::to_string).collect::<Vec<_>>(),
    });
    // Like `kafka-metadata-quorum`, a dry run reads only local files.
    if args.confirm.dry_run {
        return Ok(
            CommandResult::success(vec![added_line(&controller, true)], data).into_dry_run(),
        );
    }
    Err(unsupported(
        "metadata-quorum add-controller",
        "AdminClient::add_raft_voter",
    ))
}

fn added_line(controller: &NewController, dry_run: bool) -> String {
    format!(
        "{} controller {} with directory id {} and endpoints: {}",
        if dry_run {
            "DRY RUN of adding"
        } else {
            "Added"
        },
        controller.id,
        controller.directory_id,
        controller
            .endpoints
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The contents of the metadata directory's identity file, by file name.
type MetaReader<'a> = dyn Fn(&Path) -> Option<(String, String)> + 'a;

/// Reads `meta.properties`, which Kafka writes, or else
/// `meta.properties.json`, which `krabka format` writes. Returns the file
/// name and its contents.
fn read_meta_properties(directory: &Path) -> Option<(String, String)> {
    ["meta.properties", "meta.properties.json"]
        .into_iter()
        .find_map(|name| {
            std::fs::read_to_string(directory.join(name))
                .ok()
                .map(|text| (name.to_owned(), text))
        })
}

/// `MetadataQuorumCommand.handleAddController` up to the request: the node ID,
/// the metadata directory's ID and the controller endpoints, with Kafka's
/// refusals.
fn new_controller(
    properties: &Properties,
    read_meta: &MetaReader<'_>,
) -> Result<NewController, String> {
    let id = controller_id(properties)?;
    let directory = metadata_directory(properties)?;
    let directory_id = metadata_directory_id(&directory, read_meta)?;
    let endpoints = controller_endpoints(properties)?;
    Ok(NewController {
        id,
        directory_id,
        endpoints,
    })
}

fn controller_id(properties: &Properties) -> Result<i32, String> {
    let value = properties.get("node.id").ok_or_else(|| {
        "node.id not found in configuration file. Is this a valid controller configuration file?"
            .to_owned()
    })?;
    let id = value
        .parse::<i32>()
        .map_err(|_| format!("For input string: \"{value}\""))?;
    if id < 0 {
        return Err(
            "node.id was negative in configuration file. Is this a valid controller \
             configuration file?"
                .into(),
        );
    }
    if !properties
        .get("process.roles")
        .unwrap_or_default()
        .contains("controller")
    {
        return Err(
            "process.roles did not contain 'controller' in configuration file. Is this a valid \
             controller configuration file?"
                .into(),
        );
    }
    Ok(id)
}

fn metadata_directory(properties: &Properties) -> Result<String, String> {
    if let Some(directory) = properties.get("metadata.log.dir") {
        return Ok(directory.to_owned());
    }
    if let Some(directories) = properties.get("log.dirs") {
        return Ok(directories.split(',').next().unwrap_or_default().to_owned());
    }
    Err(
        "Neither metadata.log.dir nor log.dirs were found. Is this a valid controller \
         configuration file?"
            .into(),
    )
}

fn metadata_directory_id(directory: &str, read_meta: &MetaReader<'_>) -> Result<KafkaUuid, String> {
    let Some((name, text)) = read_meta(Path::new(directory)) else {
        return Err(format!("Unable to read meta.properties from {directory}"));
    };
    let missing = || format!("No directory id found in {directory}");
    if Path::new(&name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
    {
        let value: Value = serde_json::from_str(&text)
            .map_err(|_| format!("Unable to read meta.properties from {directory}"))?;
        let id = value
            .get("directory_id")
            .and_then(Value::as_str)
            .ok_or_else(missing)?;
        return KafkaUuid::parse_kafka_or_hyphenated(id)
            .map_err(|error| format!("Unable to read directory_id as a Uuid: {error}"));
    }
    let properties = Properties::parse(text.as_bytes()).map_err(|error| error.to_string())?;
    let id = properties.get("directory.id").ok_or_else(missing)?;
    id.parse::<KafkaUuid>()
        .map_err(|error| format!("Unable to read directory.id as a Uuid: {error}"))
}

/// `SocketServerConfigs.listenerListToEndPoints` of one CSV property: each
/// `NAME://host:port` entry, by upper-cased listener name.
fn listener_entries(value: &str) -> Result<Vec<VoterEndpoint>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(str::trim)
        .map(|entry| {
            let unparsable = || format!("Unable to parse {entry} to a broker endpoint");
            let (name, rest) = entry.rsplit_once("://").ok_or_else(unparsable)?;
            let (host, port) = rest.rsplit_once(':').ok_or_else(unparsable)?;
            let host = host.strip_prefix('[').unwrap_or(host);
            let host = host.strip_suffix(']').unwrap_or(host);
            let valid_host = host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '%' | '.' | '_' | ':'));
            let valid_port = port.strip_prefix('-').unwrap_or(port);
            if !valid_host
                || valid_port.is_empty()
                || !valid_port.chars().all(|c| c.is_ascii_digit())
            {
                return Err(unparsable());
            }
            Ok(VoterEndpoint {
                listener: name.to_uppercase(),
                host: if host.is_empty() {
                    "localhost".into()
                } else {
                    host.to_owned()
                },
                port: port.parse().map_err(|_| unparsable())?,
            })
        })
        .collect()
}

/// `MetadataQuorumCommand.getControllerAdvertisedListeners`: the endpoint of
/// each `controller.listener.names` entry, from `advertised.listeners` or
/// else `listeners`, without duplicates.
fn controller_endpoints(properties: &Properties) -> Result<Vec<VoterEndpoint>, String> {
    let mut by_name = BTreeMap::new();
    for property in ["listeners", "advertised.listeners"] {
        for endpoint in listener_entries(properties.get(property).unwrap_or_default())? {
            by_name.insert(endpoint.listener.clone(), endpoint);
        }
    }
    let names = properties.get("controller.listener.names").ok_or_else(|| {
        "controller.listener.names was not found. Is this a valid controller configuration file?"
            .to_owned()
    })?;
    let mut endpoints: Vec<VoterEndpoint> = Vec::new();
    for name in names.split(',') {
        if name.trim().is_empty() {
            return Err("The provided listener name is null or empty string".into());
        }
        let name = name.to_uppercase();
        let endpoint = by_name.get(&name).ok_or_else(|| {
            format!("Cannot find information about controller listener name: {name}")
        })?;
        if !endpoints.contains(endpoint) {
            endpoints.push(endpoint.clone());
        }
    }
    Ok(endpoints)
}

async fn remove_controller(
    _connection: &ConnectionArgs,
    args: RemoveControllerArgs,
) -> Result<CommandResult, CommandError> {
    let (id, directory_id) = removal(args.controller_id, &args.controller_directory_id)?;
    let data = json!({"controller_id": id, "directory_id": directory_id.to_string()});
    // Like `kafka-metadata-quorum`, a dry run contacts no broker.
    if args.confirm.dry_run {
        return Ok(
            CommandResult::success(vec![removed_line(id, directory_id, true)], data).into_dry_run(),
        );
    }
    confirm(
        args.confirm.yes,
        "krabka metadata-quorum remove-controller",
        Impact {
            summary: format!("remove KRaft controller {id} from the metadata quorum"),
            resources: vec![format!("controller {id} with directory id {directory_id}")],
        },
    )
    .await?;
    // `kafka-metadata-quorum` sends no cluster ID; the pinned
    // `remove_raft_voter` needs one, and only `describe_cluster` can supply it.
    Err(unsupported(
        "metadata-quorum remove-controller",
        "AdminClient::remove_raft_voter with an optional cluster ID (or \
         AdminClient::describe_cluster)",
    ))
}

/// `MetadataQuorumCommand.handleRemoveController`'s checks.
fn removal(controller_id: i32, directory_id: &str) -> Result<(i32, KafkaUuid), String> {
    if controller_id < 0 {
        return Err(format!("Invalid negative --controller-id: {controller_id}"));
    }
    let directory_id = directory_id
        .parse::<KafkaUuid>()
        .map_err(|error| format!("Failed to parse --controller-directory-id: {error}"))?;
    Ok((controller_id, directory_id))
}

fn removed_line(id: i32, directory_id: KafkaUuid, dry_run: bool) -> String {
    format!(
        "{} KRaft controller {id} with directory id {directory_id}",
        if dry_run {
            "DRY RUN of removing "
        } else {
            "Removed "
        }
    )
}

#[cfg(test)]
mod tests;
