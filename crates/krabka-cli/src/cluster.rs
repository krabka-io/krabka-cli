//! `krabka cluster`, the counterpart of `kafka-cluster`.
//!
//! Each action takes its own connection flags, as `kafka-cluster` does:
//! `-b`/`--bootstrap-server`, `-C`/`--bootstrap-controller`,
//! `-c`/`--command-config`, and the deprecated `--config`.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use krabka_client_admin::{AdminClient, AdminError, ClusterNode, DescribeClusterOptions};
use krabka_client_core::ClientError;
use serde_json::json;

use crate::{
    connection::ConnectionArgs,
    jvm::{Table, hash_order},
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, confirm},
};

/// `UNSUPPORTED_VERSION`, which Kafka raises as `UnsupportedVersionException`.
const UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug, Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    command: ClusterCommand,
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    /// Get information about the ID of a cluster.
    ClusterId(ClusterIdArgs),
    /// Unregister a broker.
    Unregister(UnregisterArgs),
    /// List endpoints.
    ListEndpoints(ListEndpointsArgs),
}

/// The connection flags of one `kafka-cluster` action.
#[derive(Debug, Args)]
#[command(
    mut_arg("bootstrap_server", |arg| arg.short('b')),
    mut_arg("bootstrap_controller", |arg| arg.short('C')),
    mut_arg("command_config", |arg| arg.short('c'))
)]
struct ClusterConnection {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// (DEPRECATED) A property file containing configurations for the Admin
    /// client. Use --command-config instead.
    #[arg(long)]
    config: Option<PathBuf>,
}

impl ClusterConnection {
    /// The connection to use, and the deprecation notice that `kafka-cluster`
    /// prints first on stdout when `--config` is given.
    fn resolve(self) -> Result<(ConnectionArgs, Vec<String>), CommandError> {
        let Self {
            mut connection,
            config,
        } = self;
        match (config, &connection.command_config) {
            (Some(_), Some(_)) => {
                Err("--config and --command-config cannot be specified together.".into())
            }
            (Some(config), None) => {
                connection.command_config = Some(config);
                Ok((
                    connection,
                    vec![
                        "Option --config has been deprecated and will be removed in a future \
                         version. Use --command-config instead."
                            .into(),
                    ],
                ))
            }
            (None, _) => Ok((connection, Vec::new())),
        }
    }
}

#[derive(Debug, Args)]
struct ClusterIdArgs {
    #[command(flatten)]
    connection: ClusterConnection,
}

#[derive(Debug, Args)]
struct UnregisterArgs {
    #[command(flatten)]
    connection: ClusterConnection,
    /// The ID of the broker to unregister.
    #[arg(short = 'i', long, allow_negative_numbers = true)]
    id: i32,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

#[derive(Debug, Args)]
struct ListEndpointsArgs {
    #[command(flatten)]
    connection: ClusterConnection,
    /// Whether to include fenced brokers when listing broker endpoints.
    #[arg(long)]
    include_fenced_brokers: bool,
}

impl ClusterArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        match self.command {
            ClusterCommand::ClusterId(args) => cluster_id_command(args).await,
            ClusterCommand::Unregister(args) => unregister_command(args).await,
            ClusterCommand::ListEndpoints(args) => list_endpoints_command(args).await,
        }
    }
}

async fn cluster_id_command(args: ClusterIdArgs) -> Result<CommandResult, CommandError> {
    let (connection, mut human) = args.connection.resolve()?;
    let client = connection.connect("cluster").await?;
    let cluster_id = cluster_id(&client).await?;
    human.push(cluster_id_line(Some(&cluster_id)));
    Ok(CommandResult::success(
        human,
        json!({"cluster_id": cluster_id}),
    ))
}

/// The line that `kafka-cluster cluster-id` prints.
fn cluster_id_line(cluster_id: Option<&str>) -> String {
    cluster_id.map_or_else(
        || "No cluster ID found. The Kafka version is probably too old.".into(),
        |id| format!("Cluster ID: {id}"),
    )
}

async fn unregister_command(args: UnregisterArgs) -> Result<CommandResult, CommandError> {
    let (connection, mut human) = args.connection.resolve()?;
    let id = args.id;
    let mut client = connection.connect("cluster").await?;
    if args.confirm.dry_run {
        human.push(unregistered_line(id));
        return Ok(
            CommandResult::success(human, json!({"broker_id": id, "unregistered": true}))
                .into_dry_run(),
        );
    }
    confirm(
        args.confirm.yes,
        "krabka cluster unregister",
        Impact {
            summary: format!("unregister broker {id}"),
            resources: vec![format!("broker {id}")],
        },
    )
    .await?;
    let outcome = unregister_outcome(client.unregister_broker(id).await)?;
    let unregistered = matches!(outcome, Unregistered::Done);
    human.push(match outcome {
        Unregistered::Done => unregistered_line(id),
        Unregistered::NotSupported => {
            "The target cluster does not support the broker unregistration API.".into()
        }
    });
    Ok(CommandResult::success(
        human,
        json!({"broker_id": id, "unregistered": unregistered}),
    ))
}

fn unregistered_line(id: i32) -> String {
    format!("Broker {id} is no longer registered.")
}

/// What `kafka-cluster unregister` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unregistered {
    /// The controller removed the registration.
    Done,
    /// The cluster does not support `UnregisterBroker`. `kafka-cluster` says
    /// so and exits 0.
    NotSupported,
}

fn unregister_outcome(result: Result<(), AdminError>) -> Result<Unregistered, CommandError> {
    match result {
        Ok(()) => Ok(Unregistered::Done),
        Err(
            AdminError::Transport(ClientError::IncompatibleVersion { .. })
            | AdminError::Broker {
                code: UNSUPPORTED_VERSION,
                ..
            },
        ) => Ok(Unregistered::NotSupported),
        Err(error) => Err(error.into()),
    }
}

async fn list_endpoints_command(args: ListEndpointsArgs) -> Result<CommandResult, CommandError> {
    let (connection, mut human) = args.connection.resolve()?;
    let controllers = !connection.bootstrap_controller.is_empty();
    if args.include_fenced_brokers && controllers {
        return Err(
            "The option --include-fenced-brokers is only supported with --bootstrap-server option"
                .into(),
        );
    }
    let client = connection.connect("cluster").await?;
    let nodes = match cluster_nodes(&client, args.include_fenced_brokers).await {
        Ok(nodes) => nodes,
        // `kafka-cluster list-endpoints` prints the message of an
        // `UnsupportedVersionException` and exits 0.
        Err(AdminError::Transport(ClientError::IncompatibleVersion { broker_max, .. }))
            if args.include_fenced_brokers =>
        {
            human.push(format!(
                "Attempted to write a non-default includeFencedBrokers at version {broker_max}"
            ));
            return Ok(CommandResult::success(human, Vec::<()>::new()));
        }
        Err(error) => return Err(error.into()),
    };
    human.extend(endpoint_lines(&nodes, controllers));
    let endpoint_type = if controllers { "controller" } else { "broker" };
    let data = nodes
        .iter()
        .map(|node| {
            json!({
                "id": node.id,
                "host": node.host,
                "port": node.port,
                "rack": node.rack,
                "fenced": node.is_fenced,
                "endpoint_type": endpoint_type,
            })
        })
        .collect::<Vec<_>>();
    Ok(CommandResult::success(human, data))
}

/// The cluster ID that `DescribeCluster` reports.
///
/// # Errors
/// Returns the error of the `DescribeCluster` call.
pub(crate) async fn cluster_id(client: &AdminClient) -> Result<String, AdminError> {
    Ok(client
        .describe_cluster(DescribeClusterOptions::default())
        .await?
        .cluster_id)
}

/// The nodes that `DescribeCluster` reports, in the order of Kafka's
/// `DescribeClusterResponse.nodes()`, a `HashMap` keyed by node ID.
///
/// # Errors
/// Returns the error of the `DescribeCluster` call.
pub(crate) async fn cluster_nodes(
    client: &AdminClient,
    include_fenced_brokers: bool,
) -> Result<Vec<ClusterNode>, AdminError> {
    let nodes = client
        .describe_cluster(DescribeClusterOptions {
            include_fenced_brokers,
            ..DescribeClusterOptions::default()
        })
        .await?
        .nodes;
    Ok(node_order(nodes))
}

/// `nodes` in the iteration order of Kafka's `Collectors.toMap` by node ID.
fn node_order(nodes: Vec<ClusterNode>) -> Vec<ClusterNode> {
    let mut unique = Vec::<ClusterNode>::new();
    for node in nodes {
        match unique.iter_mut().find(|known| known.id == node.id) {
            Some(known) => *known = node,
            None => unique.push(node),
        }
    }
    hash_order(unique, Table::Default, |node| node.id)
}

/// Left-justifies `value` in `width` columns, as Java's `%-<width>s` does.
fn pad(value: &str, width: usize) -> String {
    format!("{value:<width$}")
}

/// The table that `kafka-cluster list-endpoints` prints.
///
/// The host and rack columns are as wide as their longest value, or 100 and
/// 10 when no node has one. A node without a rack prints `null`, as Java
/// formats a null string.
fn endpoint_lines(nodes: &[ClusterNode], controllers: bool) -> Vec<String> {
    let host_width = nodes
        .iter()
        .map(|node| node.host.chars().count())
        .max()
        .unwrap_or(100);
    let rack_width = nodes
        .iter()
        .filter_map(|node| node.rack.as_ref().map(|rack| rack.chars().count()))
        .max()
        .unwrap_or(10);
    let row = |id: &str, host: &str, port: &str, rack: &str, state: Option<&str>, kind: &str| {
        let mut line = format!(
            "{} {} {} {} ",
            pad(id, 10),
            pad(host, host_width),
            pad(port, 10),
            pad(rack, rack_width)
        );
        if let Some(state) = state {
            line.push_str(&pad(state, 10));
            line.push(' ');
        }
        line.push_str(&pad(kind, 15));
        line
    };
    let header_state = (!controllers).then_some("STATE");
    let mut lines = vec![row(
        "ID",
        "HOST",
        "PORT",
        "RACK",
        header_state,
        "ENDPOINT_TYPE",
    )];
    for node in nodes {
        let state = (!controllers).then_some(if node.is_fenced { "fenced" } else { "unfenced" });
        lines.push(row(
            &node.id.to_string(),
            &node.host,
            &node.port.to_string(),
            node.rack.as_deref().unwrap_or("null"),
            state,
            if controllers { "controller" } else { "broker" },
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct Command {
        #[command(subcommand)]
        command: ClusterCommand,
    }

    fn node(id: i32, host: &str, rack: Option<&str>, is_fenced: bool) -> ClusterNode {
        ClusterNode {
            id,
            host: host.into(),
            port: 9092,
            rack: rack.map(Into::into),
            is_fenced,
        }
    }

    #[test]
    fn nodes_list_in_kafkas_hash_map_order() {
        let ids = |nodes: Vec<ClusterNode>| nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        let cases = [
            (vec![3, 1, 2], vec![1, 2, 3]),
            (vec![17, 1, 16], vec![16, 17, 1]),
            (vec![2, 2, 1], vec![1, 2]),
        ];
        for (given, expected) in cases {
            let nodes = given
                .into_iter()
                .map(|id| node(id, "h", None, false))
                .collect();
            check!(ids(node_order(nodes)) == expected);
        }
    }

    #[test]
    fn endpoints_render_as_kafka_cluster_prints_them() {
        let cases = [
            (
                vec![node(1, "localhost", None, false)],
                false,
                vec![
                    "ID         HOST      PORT       RACK       STATE      ENDPOINT_TYPE  ",
                    "1          localhost 9092       null       unfenced   broker         ",
                ],
            ),
            (
                vec![node(1, "localhost", None, false)],
                true,
                vec![
                    "ID         HOST      PORT       RACK       ENDPOINT_TYPE  ",
                    "1          localhost 9092       null       controller     ",
                ],
            ),
            (
                vec![
                    node(1, "a.example", Some("r1"), false),
                    node(12, "b", None, true),
                ],
                false,
                vec![
                    "ID         HOST      PORT       RACK STATE      ENDPOINT_TYPE  ",
                    "1          a.example 9092       r1 unfenced   broker         ",
                    "12         b         9092       null fenced     broker         ",
                ],
            ),
        ];
        for (nodes, controllers, expected) in cases {
            check!(endpoint_lines(&nodes, controllers) == expected);
        }
        check!(
            endpoint_lines(&[], false)
                == vec![format!(
                    "ID         {}PORT       RACK       STATE      ENDPOINT_TYPE  ",
                    pad("HOST", 101)
                )]
        );
    }

    #[test]
    fn the_cluster_id_line_matches_kafka_cluster() {
        check!(
            cluster_id_line(Some("5L6g3nShT-eMCtK--X86sw")) == "Cluster ID: 5L6g3nShT-eMCtK--X86sw"
        );
        check!(
            cluster_id_line(None) == "No cluster ID found. The Kafka version is probably too old."
        );
    }

    #[test]
    fn an_unsupported_unregistration_api_is_reported_not_failed() {
        let cases = [
            (Ok(()), Some(Unregistered::Done)),
            (
                Err(AdminError::Transport(ClientError::IncompatibleVersion {
                    api_key: 64,
                    broker_min: 0,
                    broker_max: 0,
                    client_min: 1,
                    client_max: 1,
                })),
                Some(Unregistered::NotSupported),
            ),
            (
                Err(AdminError::Broker {
                    api: "UnregisterBroker",
                    code: 35,
                    name: "UNSUPPORTED_VERSION",
                    message: None,
                }),
                Some(Unregistered::NotSupported),
            ),
            (
                Err(AdminError::Broker {
                    api: "UnregisterBroker",
                    code: 102,
                    name: "BROKER_ID_NOT_REGISTERED",
                    message: None,
                }),
                None,
            ),
        ];
        for (result, expected) in cases {
            check!(unregister_outcome(result).ok() == expected);
        }
    }

    #[test]
    fn kafka_cluster_flags_parse() {
        let parse =
            |argv: &[&str]| Command::try_parse_from(argv).map(|cli| format!("{:?}", cli.command));
        check!(parse(&["cluster", "cluster-id", "-b", "h:1"]).is_ok());
        check!(
            parse(&[
                "cluster",
                "cluster-id",
                "-C",
                "h:1",
                "-c",
                "admin.properties"
            ])
            .is_ok()
        );
        check!(
            parse(&[
                "cluster",
                "unregister",
                "--bootstrap-server",
                "h:1",
                "-i",
                "5"
            ])
            .is_ok()
        );
        check!(parse(&["cluster", "unregister", "-b", "h:1", "--id", "-1"]).is_ok());
        check!(parse(&["cluster", "unregister", "-b", "h:1"]).is_err());
        check!(
            parse(&[
                "cluster",
                "list-endpoints",
                "-b",
                "h:1",
                "--include-fenced-brokers"
            ])
            .is_ok()
        );
        check!(parse(&["cluster", "cluster-id", "-b", "h:1", "-C", "h:2"]).is_err());
    }

    #[test]
    fn the_deprecated_config_flag_is_used_with_its_notice() {
        let resolve = |argv: &[&str]| {
            let Command {
                command: ClusterCommand::ClusterId(args),
            } = Command::try_parse_from(argv).unwrap()
            else {
                panic!("expected cluster-id")
            };
            args.connection
                .resolve()
                .map(|(connection, notice)| (connection.command_config, notice))
                .map_err(|error| error.to_string())
        };
        check!(
            resolve(&[
                "cluster",
                "cluster-id",
                "-b",
                "h:1",
                "--config",
                "a.properties"
            ]) == Ok((
                Some(PathBuf::from("a.properties")),
                vec![
                    "Option --config has been deprecated and will be removed in a future \
                         version. Use --command-config instead."
                        .to_owned()
                ]
            ))
        );
        check!(
            resolve(&["cluster", "cluster-id", "-b", "h:1", "-c", "b.properties"])
                == Ok((Some(PathBuf::from("b.properties")), Vec::new()))
        );
        check!(
            resolve(&[
                "cluster",
                "cluster-id",
                "-b",
                "h:1",
                "--config",
                "a.properties",
                "--command-config",
                "b.properties"
            ]) == Err("--config and --command-config cannot be specified together.".to_owned())
        );
    }
}
