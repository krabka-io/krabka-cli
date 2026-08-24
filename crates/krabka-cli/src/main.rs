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

#[derive(Parser)]
#[command(
    name = "krabka",
    version,
    about = "Krabka operator CLI",
    // An unrecognised subcommand is not an error here: it may be an external
    // one. clap hands it over rather than rejecting it.
    allow_external_subcommands = true
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
    use clap::Parser;

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
