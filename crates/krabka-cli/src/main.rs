//! The `krabka` operator CLI.
//!
//! Built-in subcommands are compiled in; everything else is discovered on
//! `PATH` as `krabka-<name>`, the way git and cargo find their own. That is
//! what lets a subcommand live in the repository that owns the thing it
//! operates on -- `krabka gres` ships from the gres repository as
//! `krabka-gres` -- without this crate depending on any of them. Compiling
//! them in would mean this binary's dependency graph growing to the union of
//! every product in the organisation.

use std::{ffi::OsString, process::Command as Process};

use clap::{Parser, Subcommand};

mod format;
mod ids;

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
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Format a fresh log directory, with optional seed SCRAM credentials.
    Format(format::FormatArgs),

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let rc = match cli.command {
        Command::Format(args) => format::run(args).await,
        Command::External(argv) => run_external(&argv),
    };
    std::process::exit(rc);
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
