//! The `krabka` operator CLI.
//!
//! Built-in subcommands are compiled in; everything else is discovered on
//! `PATH` as `krabka-<name>`, the way git and cargo find their own. That is
//! what lets large, independently deployed tools remain separate while the
//! operator commands needed by the demo, including `krabka gres`, ship in one
//! reliable CLI image.

use std::{ffi::OsString, future::Future};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use tokio_util::sync::CancellationToken;

mod acls;
mod cluster;
mod common;
mod compat;
mod configs;
pub mod connection;
mod console;
mod console_consumer;
mod console_producer;
mod consumer_groups;
mod delegation_tokens;
mod delete_records;
pub mod exit;
pub mod external;
mod fan_out;
mod feature_catalog;
mod features;
mod get_offsets;
mod gres;
mod jvm;
mod kafka_errors;
mod kafka_json;
mod leader_election;
mod log_dirs;
mod metadata_quorum;
pub mod output;
mod reassign_partitions;
mod replica_placer;
pub mod safety;
mod storage;
mod topic_partition;
mod topics;
mod transactions;

use self::{
    exit::Exit,
    output::{CommandError, CommandResult, OutputArgs, OutputFormat, emit_error, emit_success},
};

#[derive(Parser)]
#[command(
    name = "krabka",
    version,
    about = "Krabka operator CLI",
    // An unrecognised subcommand is not an error here: it may be an external
    // one. clap hands it over rather than rejecting it.
    allow_external_subcommands = true,
    after_help = external::short_help(),
    after_long_help = external::long_help()
)]
pub struct Cli {
    #[command(flatten)]
    output: OutputArgs,

    #[arg(short = 'v', long, action = ArgAction::Count, conflicts_with = "quiet", global = true)]
    verbose: u8,

    #[arg(short = 'q', long, action = ArgAction::Count, conflicts_with = "verbose", global = true)]
    quiet: u8,

    #[arg(long, value_enum, default_value_t, global = true)]
    log_format: LogFormat,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Format a fresh log directory, with optional seed SCRAM credentials.
    Format(krabka_format::FormatArgs),

    /// Create, delete, list and describe topics.
    Topics(topics::TopicsArgs),

    /// Describe and alter entity configs, quotas and SCRAM credentials.
    Configs(configs::ConfigsArgs),

    /// List, add and remove access-control entries.
    Acls(acls::AclsArgs),

    /// List, describe, delete and reset consumer groups and their offsets.
    ConsumerGroups(consumer_groups::ConsumerGroupsArgs),

    /// Describe the log directories of brokers, or move replicas between them.
    LogDirs(log_dirs::LogDirsArgs),

    /// Print the offsets of topic partitions.
    GetOffsets(get_offsets::GetOffsetsArgs),

    /// Describe, upgrade, downgrade and disable feature flags (KIP-584,
    /// KIP-1022), as `kafka-features` does.
    Features(features::FeaturesArgs),

    /// Report on and format log directories, as `kafka-storage` does.
    Storage(storage::StorageArgs),

    /// Describe the `KRaft` metadata quorum, and add or remove controllers.
    MetadataQuorum(metadata_quorum::MetadataQuorumArgs),

    /// Get the cluster ID, unregister a broker, or list endpoints.
    Cluster(cluster::ClusterArgs),

    /// Elect the preferred or an unclean leader for partitions.
    LeaderElection(leader_election::LeaderElectionArgs),

    /// Generate, execute, verify, cancel or list partition reassignments.
    ReassignPartitions(reassign_partitions::ReassignPartitionsArgs),

    /// Operate the Gres tenant registry and range layout.
    Gres(gres::GresArgs),

    /// List, describe, abort and force-terminate transactions.
    Transactions(transactions::TransactionsArgs),

    /// Create, renew, expire and describe delegation tokens.
    DelegationTokens(delegation_tokens::DelegationTokensArgs),

    /// Delete the records of partitions below an offset.
    DeleteRecords(delete_records::DeleteRecordsArgs),

    /// Read records from topics and write them to stdout.
    ConsoleConsumer(console_consumer::ConsoleConsumerArgs),

    /// Read lines from stdin and produce each as a record.
    ConsoleProducer(console_producer::ConsoleProducerArgs),

    /// Anything not built in, delegated to `krabka-<name>` on `PATH`.
    #[command(external_subcommand)]
    External(Vec<OsString>),
}

/// The log filter when `RUST_LOG` is not set: `info`, raised by each `-v` and
/// lowered by each `-q`.
const fn default_filter(verbose: u8, quiet: u8) -> &'static str {
    match (verbose, quiet) {
        (0, 0) => "info",
        (1, _) => "debug",
        (2.., _) => "trace",
        (_, 1) => "warn",
        (_, 2) => "error",
        (_, 3..) => "off",
    }
}

/// Parses the command line, installs logging, and runs the command.
pub async fn run() -> Exit {
    let cli = Cli::parse();
    // `kafka-consumer-groups --verbose` selects more columns, not more logs.
    let log_verbosity = if matches!(cli.command, Command::ConsumerGroups(_)) {
        0
    } else {
        cli.verbose
    };
    let default_filter = default_filter(log_verbosity, cli.quiet);
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    match cli.log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_writer(std::io::stderr)
            .with_env_filter(filter)
            .init(),
    }
    let output = cli.output.output;
    match cli.command {
        Command::Format(args) => Exit::Passthrough(krabka_format::run(args).await),
        Command::Topics(args) => run_admin("topics", Box::pin(args.run()), output).await,
        Command::Configs(args) => run_admin("configs", Box::pin(args.run()), output).await,
        Command::Acls(args) => run_admin("acls", Box::pin(args.run()), output).await,
        Command::ConsumerGroups(args) => {
            run_admin(
                "consumer-groups",
                Box::pin(args.run(cli.verbose > 0)),
                output,
            )
            .await
        }
        Command::MetadataQuorum(args) => {
            run_admin("metadata-quorum", Box::pin(args.run()), output).await
        }
        Command::Cluster(args) => run_admin("cluster", Box::pin(args.run()), output).await,
        Command::LeaderElection(args) => {
            run_admin("leader-election", Box::pin(args.run()), output).await
        }
        Command::LogDirs(args) => run_admin("log-dirs", Box::pin(args.run()), output).await,
        Command::GetOffsets(args) => run_admin("get-offsets", Box::pin(args.run()), output).await,
        Command::Features(args) => run_admin("features", Box::pin(args.run()), output).await,
        Command::Storage(args) => match args.into_format() {
            Ok(format) => Exit::Passthrough(krabka_format::run(format).await),
            Err(args) => run_admin("storage", Box::pin(args.run()), output).await,
        },
        Command::ReassignPartitions(args) => {
            run_admin("reassign-partitions", Box::pin(args.run()), output).await
        }
        Command::Gres(args) => run_admin("gres", Box::pin(gres_run(args)), output).await,
        Command::Transactions(args) => {
            run_admin("transactions", Box::pin(args.run()), output).await
        }
        Command::DelegationTokens(args) => {
            run_admin("delegation-tokens", Box::pin(args.run()), output).await
        }
        Command::DeleteRecords(args) => {
            run_admin("delete-records", Box::pin(args.run()), output).await
        }
        Command::ConsoleConsumer(args) => Box::pin(console_consumer::run(args, output)).await,
        Command::ConsoleProducer(args) => Box::pin(console_producer::run(args, output)).await,
        Command::External(argv) => external::run(&argv).await,
    }
}

async fn gres_run(args: gres::GresArgs) -> Result<CommandResult, CommandError> {
    Ok(gres::run(args).await?)
}

async fn run_admin<E: Into<CommandError>>(
    command: &str,
    future: impl Future<Output = Result<CommandResult, E>>,
    format: OutputFormat,
) -> Exit {
    let cancel = CancellationToken::new();
    let on_signal = cancel.clone();
    let watcher = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            on_signal.cancel();
        }
    });
    let command = format!("krabka {command}");
    let outcome = run_until_cancelled(&command, future, &cancel, format).await;
    watcher.abort();
    outcome
}

/// Runs a command until it finishes or `cancel` fires, and renders the result.
///
/// A cancelled command exits [`Exit::Cancelled`]. The command reports no
/// partial result, because the request in flight may or may not have reached
/// the broker.
async fn run_until_cancelled<E: Into<CommandError>>(
    command: &str,
    future: impl Future<Output = Result<CommandResult, E>>,
    cancel: &CancellationToken,
    format: OutputFormat,
) -> Exit {
    tokio::select! {
        result = future => match result {
            Ok(result) => {
                let failed = result.failed;
                match emit_success(&result, format) {
                    // A reader that closes early, as `| head` does, is not a
                    // failure of the command. The Rust runtime ignores
                    // SIGPIPE, so the closed pipe arrives here as an error.
                    Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => {
                        let _ = emit_error(command, &error.to_string(), Exit::Failure, format);
                        Exit::Failure
                    }
                    _ => Exit::from_failed(failed),
                }
            }
            Err(error) => {
                let error = error.into();
                let _ = emit_error(command, &error.to_string(), error.exit(), format);
                error.exit()
            }
        },
        () = cancel.cancelled() => {
            let _ = emit_error(
                command,
                "cancelled; a request in flight may already have reached the broker",
                Exit::Cancelled,
                format,
            );
            Exit::Cancelled
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use assert2::check;
    use clap::Parser;

    use super::{Cli, Command};

    /// An unknown subcommand is delegated rather than rejected, and the name
    /// and its arguments arrive intact: `allow_external_subcommands` off, or
    /// the argv split wrong, and every external subcommand breaks.
    #[test]
    fn an_unknown_subcommand_is_delegated_with_its_arguments() {
        let cli = Cli::try_parse_from(["krabka", "example", "value", "--flag"])
            .expect("an unknown subcommand is delegated, not refused");
        let Command::External(argv) = cli.command else {
            panic!("expected the external arm");
        };
        check!(
            argv == vec![
                OsString::from("example"),
                OsString::from("value"),
                OsString::from("--flag"),
            ]
        );
    }

    /// Naming `restore` in the help text must not turn it into a built-in: it
    /// still takes the external arm, with its arguments intact.
    #[test]
    fn the_documented_restore_subcommand_is_still_delegated() {
        let cli = Cli::try_parse_from(["krabka", "restore", "--to-offset", "42"])
            .expect("restore is external, not built in");
        let Command::External(argv) = cli.command else {
            panic!("expected the external arm");
        };
        check!(
            argv == vec![
                OsString::from("restore"),
                OsString::from("--to-offset"),
                OsString::from("42"),
            ]
        );
    }

    /// The admin UI is built in this repository but is still delegated, not
    /// compiled in: `krabka admin-ui` runs the `krabka-admin-ui` binary from
    /// PATH, so the CLI does not take the UI's dependency graph.
    #[test]
    fn the_admin_ui_subcommand_is_delegated() {
        let cli = Cli::try_parse_from(["krabka", "admin-ui", "--session-ttl", "8h"])
            .expect("admin-ui is external, not built in");
        let Command::External(argv) = cli.command else {
            panic!("expected the external arm");
        };
        check!(
            argv == vec![
                OsString::from("admin-ui"),
                OsString::from("--session-ttl"),
                OsString::from("8h"),
            ]
        );
    }

    /// A built-in still wins over delegation, so shipping a `krabka-format` on
    /// PATH cannot shadow the compiled-in one.
    #[test]
    fn a_built_in_subcommand_is_not_delegated() {
        let cli =
            Cli::try_parse_from(["krabka", "format", "--log-dir", "/tmp/x", "--node-id", "1"])
                .expect("format is built in");
        check!(matches!(cli.command, Command::Format(_)));
    }

    #[test]
    fn gres_is_built_in() {
        let cli = Cli::try_parse_from([
            "krabka",
            "gres",
            "describe",
            "--bootstrap",
            "broker:9092",
            "--name",
            "demo",
        ])
        .expect("built-in gres command parses");
        check!(matches!(cli.command, Command::Gres(_)));
    }

    #[test]
    fn admin_commands_accept_kafka_style_flags_and_global_output() {
        let commands = [
            vec![
                "krabka",
                "topics",
                "--list",
                "--bootstrap-server",
                "host:9092",
                "--output",
                "json",
            ],
            vec![
                "krabka",
                "configs",
                "--describe",
                "--entity-name",
                "orders",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "acls",
                "--list",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "consumer-groups",
                "--describe",
                "--group",
                "orders",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "consumer-groups",
                "--reset-offsets",
                "--group",
                "workers",
                "--topic",
                "orders:0",
                "--to-offset",
                "42",
                "--execute",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "log-dirs",
                "--describe",
                "--broker-list",
                "0,1",
                "--topic-list",
                "orders",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "get-offsets",
                "--topic-partitions",
                "orders:0-2",
                "--time",
                "-2",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "features",
                "--bootstrap-server",
                "host:9092",
                "describe",
            ],
            vec![
                "krabka",
                "features",
                "--bootstrap-controller",
                "controller:9093",
                "upgrade",
                "--feature",
                "metadata.version=20",
                "--feature",
                "kraft.version=1",
            ],
            vec!["krabka", "storage", "version-mapping", "-r", "4.0"],
            vec!["krabka", "storage", "info", "-c", "server.properties"],
            vec![
                "krabka",
                "reassign-partitions",
                "--verify",
                "--topic",
                "orders",
                "--replication-factor",
                "1",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "reassign-partitions",
                "--bootstrap-server",
                "host:9092",
                "--execute",
                "--reassignment-json-file",
                "plan.json",
                "--throttle",
                "50000000",
                "--additional",
            ],
            vec![
                "krabka",
                "metadata-quorum",
                "--bootstrap-controller",
                "controller:9093",
                "describe",
                "--replication",
                "--human-readable",
            ],
            vec![
                "krabka",
                "cluster",
                "list-endpoints",
                "-b",
                "host:9092",
                "--include-fenced-brokers",
            ],
            vec![
                "krabka",
                "leader-election",
                "--bootstrap-server",
                "host:9092",
                "--election-type",
                "preferred",
                "--all-topic-partitions",
            ],
        ];

        for argv in commands {
            Cli::try_parse_from(argv).expect("admin command parses");
        }
    }

    #[test]
    fn verbosity_raises_and_lowers_the_default_log_filter() {
        let cases = [
            (0, 0, "info"),
            (1, 0, "debug"),
            (2, 0, "trace"),
            (5, 0, "trace"),
            (0, 1, "warn"),
            (0, 2, "error"),
            (0, 3, "off"),
        ];
        for (verbose, quiet, expected) in cases {
            check!(super::default_filter(verbose, quiet) == expected);
        }
    }

    #[test]
    fn conflicting_feature_actions_are_rejected() {
        let refused: &[&[&str]] = &[
            &["krabka", "features", "--bootstrap-server", "host:9092"],
            &[
                "krabka",
                "features",
                "--bootstrap-server",
                "host:9092",
                "describe",
                "--feature",
                "metadata.version=20",
            ],
            &["krabka", "storage", "info"],
        ];
        for argv in refused {
            check!(Cli::try_parse_from(*argv).is_err(), "{argv:?}");
        }
    }

    #[test]
    fn storage_format_takes_the_formatter_flags() {
        let cli = Cli::try_parse_from([
            "krabka",
            "storage",
            "format",
            "--log-dir",
            "/tmp/x",
            "--node-id",
            "1",
        ])
        .expect("storage format parses");
        let Command::Storage(args) = cli.command else {
            panic!("expected the storage arm");
        };
        check!(args.into_format().is_ok());
    }

    #[test]
    fn negative_offset_values_reach_command_validation() {
        Cli::try_parse_from([
            "krabka",
            "consumer-groups",
            "--reset-offsets",
            "--group",
            "workers",
            "--topic",
            "orders:-1",
            "--to-offset",
            "-1",
            "--execute",
            "--bootstrap-server",
            "host:9092",
        ])
        .expect("negative numbers parse for deterministic command validation");
    }

    #[tokio::test]
    async fn destructive_and_read_only_admin_misuse_fails_before_connecting() {
        let cli = Cli::try_parse_from([
            "krabka",
            "acls",
            "--remove",
            "--yes",
            "--bootstrap-server",
            "host:9092",
        ])
        .expect("valid syntax");
        let Command::Acls(args) = cli.command else {
            panic!("expected ACL command")
        };
        check!(
            args.run()
                .await
                .unwrap_err()
                .to_string()
                .contains("You must provide at least one resource")
        );

        let cli =
            Cli::try_parse_from(["krabka", "topics", "--list", "--dry-run"]).expect("valid syntax");
        let Command::Topics(args) = cli.command else {
            panic!("expected topics command")
        };
        check!(
            args.run()
                .await
                .unwrap_err()
                .to_string()
                .contains("only valid")
        );
    }
}
