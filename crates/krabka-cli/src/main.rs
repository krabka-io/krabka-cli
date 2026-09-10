//! The `krabka` operator CLI.
//!
//! Built-in subcommands are compiled in; everything else is discovered on
//! `PATH` as `krabka-<name>`, the way git and cargo find their own. That is
//! what lets a subcommand live in the repository that owns the thing it
//! operates on -- `krabka gres` ships from the gres repository as
//! `krabka-gres` -- without this crate depending on any of them. Compiling
//! them in would mean this binary's dependency graph growing to the union of
//! every product in the organisation.

use std::{ffi::OsString, future::Future, process::Command as Process};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};

mod admin;
mod connection;
mod format;
mod ids;
mod output;

use output::{OutputArgs, OutputFormat, emit_error, emit_success};

/// Prefix an external subcommand's binary carries: `krabka-gres` provides
/// `krabka gres`.
const EXTERNAL_PREFIX: &str = "krabka-";

/// Tail of `krabka -h`.
///
/// Short help stays one screen, so this gives only the rule and the two names
/// an operator can then look for.
const SHORT_EXTERNAL_HELP: &str = "\
Any subcommand that is not built in runs as `krabka-<name>` from PATH, such as
`restore` and `gres`. Run `krabka --help` for where each one ships from.";

/// Tail of `krabka --help`.
///
/// Without this list, an external subcommand is invisible: an operator who
/// installed one cannot see it here, and an operator who did not install one
/// learns the name only after a guess fails.
const LONG_EXTERNAL_HELP: &str = "\
External subcommands:
  A subcommand that is not built in runs as `krabka-<name>` from PATH, the way
  git runs `git-foo`. Krabka looks the binary up at run time, so a subcommand
  that you did not install does not run.

  restore  Point-in-time restore of a cluster data directory. The
           krabka-broker repository ships it as `krabka-restore`.
  gres     The gres repository ships it as `krabka-gres`.";

#[derive(Parser)]
#[command(
    name = "krabka",
    version,
    about = "Krabka operator CLI",
    // An unrecognised subcommand is not an error here: it may be an external
    // one. clap hands it over rather than rejecting it.
    allow_external_subcommands = true,
    after_help = SHORT_EXTERNAL_HELP,
    after_long_help = LONG_EXTERNAL_HELP
)]
struct Cli {
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
    Format(format::FormatArgs),

    /// Create, delete, list and describe topics.
    Topics(admin::TopicsArgs),

    /// Describe and alter topic configuration.
    Configs(admin::ConfigsArgs),

    /// List, add and remove access-control entries.
    Acls(admin::AclsArgs),

    /// Inspect consumer-group offsets.
    ConsumerGroups(admin::ConsumerGroupsArgs),

    /// Inspect supported features or update metadata.version.
    Features(admin::FeaturesArgs),

    /// Execute or verify replication-factor reassignment.
    ReassignPartitions(admin::ReassignPartitionsArgs),

    /// Anything not built in, delegated to `krabka-<name>` on `PATH`.
    #[command(external_subcommand)]
    External(Vec<OsString>),
}

/// Runs `krabka-<name>` with the remaining arguments, passing its exit status
/// straight through.
///
/// Signals are reported by `ExitStatus::code()` as `None`; a shell reports the
/// same death as 128 + signal, so that is what this returns rather than
/// collapsing it to a generic failure.
fn run_external(argv: &[OsString]) -> i32 {
    let (name, rest) = argv.split_first().expect("clap yields a non-empty argv");
    let mut binary = OsString::from(EXTERNAL_PREFIX);
    binary.push(name);

    match Process::new(&binary).args(rest).status() {
        Ok(status) => status.code().unwrap_or_else(|| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt as _;
                status.signal().map_or(1, |signal| 128 + signal)
            }
            #[cfg(not(unix))]
            {
                1
            }
        }),
        Err(error) => spawn_failure(&error, &binary, name),
    }
}

/// Exit code for a subcommand that could not be spawned.
///
/// The two cases are worth telling apart, and the shell already has codes for
/// them: 127 is "no such command", which for `krabka foo` means neither a
/// built-in nor a `krabka-foo` exists and the user probably mistyped or has not
/// installed it. 126 is "found but could not be run" -- present on PATH but not
/// executable, or a bad interpreter -- which is an installation problem rather
/// than a wrong name, and deserves the underlying error rather than a
/// suggestion to read `--help`.
fn spawn_failure(error: &std::io::Error, binary: &OsString, name: &OsString) -> i32 {
    if error.kind() == std::io::ErrorKind::NotFound {
        eprintln!(
            "krabka: `{}` is not a krabka command, and no `{}` was found on PATH",
            name.to_string_lossy(),
            binary.to_string_lossy(),
        );
        eprintln!("krabka: see `krabka --help` for the built-in commands");
        127
    } else {
        eprintln!(
            "krabka: failed to run {}: {error}",
            binary.to_string_lossy()
        );
        126
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let default_filter = match (cli.verbose, cli.quiet) {
        (_, 1..) => "warn",
        (1, 0) => "debug",
        (2.., 0) => "trace",
        _ => "info",
    };
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
    let rc = match cli.command {
        Command::Format(args) => format::run(args, output).await,
        Command::Topics(args) => run_admin(args.run(), output).await,
        Command::Configs(args) => run_admin(args.run(), output).await,
        Command::Acls(args) => run_admin(args.run(), output).await,
        Command::ConsumerGroups(args) => run_admin(args.run(), output).await,
        Command::Features(args) => run_admin(args.run(), output).await,
        Command::ReassignPartitions(args) => run_admin(args.run(), output).await,
        Command::External(argv) => run_external(&argv),
    };
    std::process::exit(rc);
}

async fn run_admin(
    future: impl Future<Output = Result<output::CommandResult, String>>,
    format: OutputFormat,
) -> i32 {
    tokio::select! {
        result = future => match result {
            Ok(result) => {
                let failed = result.failed;
                if let Err(error) = emit_success(&result, format) {
                    let _ = emit_error(&error.to_string(), 1, format);
                    1
                } else {
                    i32::from(failed)
                }
            }
            Err(error) => {
                let _ = emit_error(&error, 1, format);
                1
            }
        },
        signal = tokio::signal::ctrl_c() => {
            let message = signal.map_or_else(|error| format!("Ctrl-C handler failed: {error}"), |()| "interrupted".into());
            let _ = emit_error(&message, 130, format);
            130
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use assert2::check;
    use clap::{CommandFactory as _, Parser};

    use super::{Cli, Command, spawn_failure};

    /// An unknown subcommand is delegated rather than rejected, and the name
    /// and its arguments arrive intact: `allow_external_subcommands` off, or
    /// the argv split wrong, and every external subcommand breaks.
    #[test]
    fn an_unknown_subcommand_is_delegated_with_its_arguments() {
        let cli = Cli::try_parse_from(["krabka", "gres", "list-tenants", "--bootstrap", "h:9092"])
            .expect("an unknown subcommand is delegated, not refused");
        let Command::External(argv) = cli.command else {
            panic!("expected the external arm");
        };
        check!(
            argv == vec![
                OsString::from("gres"),
                OsString::from("list-tenants"),
                OsString::from("--bootstrap"),
                OsString::from("h:9092"),
            ]
        );
    }

    /// Long help states the `krabka-<name>` rule and names each known external
    /// subcommand with the binary that provides it, because nothing else in the
    /// CLI tells an operator that an external subcommand exists.
    #[test]
    fn long_help_documents_the_external_subcommand_convention() {
        let help = Cli::command().render_long_help().to_string();

        check!(help.contains("krabka-<name>"));
        check!(help.contains("PATH"));
        check!(help.contains("run time"));
        check!(help.contains("restore"));
        check!(help.contains("krabka-restore"));
        check!(help.contains("krabka-broker"));
        check!(help.contains("gres"));
        check!(help.contains("krabka-gres"));
    }

    /// Short help carries the rule and the names too: an operator who types
    /// `-h` gets the same discovery path, only shorter.
    #[test]
    fn short_help_points_at_the_external_subcommand_convention() {
        let help = Cli::command().render_help().to_string();

        check!(help.contains("krabka-<name>"));
        check!(help.contains("PATH"));
        check!(help.contains("restore"));
        check!(help.contains("gres"));
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
                "orders",
                "--partition",
                "0",
                "--to-offset",
                "42",
                "--yes",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "features",
                "--describe",
                "--bootstrap-server",
                "host:9092",
            ],
            vec![
                "krabka",
                "features",
                "--upgrade",
                "--feature",
                "metadata.version=20",
                "--feature",
                "kraft.version=1",
                "--bootstrap-controller",
                "controller:9093",
            ],
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
        ];

        for argv in commands {
            Cli::try_parse_from(argv).expect("admin command parses");
        }
    }

    #[test]
    fn conflicting_acl_principals_are_rejected() {
        assert!(
            Cli::try_parse_from([
                "krabka",
                "acls",
                "--list",
                "--allow-principal",
                "User:alice",
                "--deny-principal",
                "User:bob",
                "--bootstrap-server",
                "host:9092",
            ])
            .is_err()
        );
    }

    #[test]
    fn conflicting_feature_actions_are_rejected() {
        assert!(
            Cli::try_parse_from([
                "krabka",
                "features",
                "--describe",
                "--upgrade",
                "--feature",
                "metadata.version=20",
                "--bootstrap-server",
                "host:9092",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "krabka",
                "features",
                "--describe",
                "--feature",
                "metadata.version=20",
                "--bootstrap-server",
                "host:9092",
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn destructive_and_read_only_admin_misuse_fails_before_connecting() {
        let cli =
            Cli::try_parse_from(["krabka", "acls", "--remove", "--yes"]).expect("valid syntax");
        let Command::Acls(args) = cli.command else {
            panic!("expected ACL command")
        };
        check!(args.run().await.unwrap_err().contains("scope filter"));

        let cli =
            Cli::try_parse_from(["krabka", "topics", "--list", "--dry-run"]).expect("valid syntax");
        let Command::Topics(args) = cli.command else {
            panic!("expected topics command")
        };
        check!(args.run().await.unwrap_err().contains("only valid"));
    }

    /// A missing external binary is 127 and an unrunnable one is 126, matching
    /// what a shell reports for each.
    ///
    /// The mapping is asserted rather than an actual spawn: what a failed
    /// lookup returns is the environment's to decide, and a sandbox that
    /// answers `PermissionDenied` where a normal PATH answers `NotFound` would
    /// make a spawn-based test disagree with itself depending on where it ran.
    #[test]
    fn a_failed_spawn_distinguishes_missing_from_unrunnable() {
        use std::io::{Error, ErrorKind};

        let binary = OsString::from("krabka-gres");
        let name = OsString::from("gres");

        check!(spawn_failure(&Error::from(ErrorKind::NotFound), &binary, &name) == 127);
        check!(spawn_failure(&Error::from(ErrorKind::PermissionDenied), &binary, &name) == 126);
        check!(spawn_failure(&Error::from(ErrorKind::Other), &binary, &name) == 126);
    }
}
