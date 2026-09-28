//! Delegation of a subcommand that is not built in to `krabka-<name>` on
//! `PATH`, the way git runs `git-foo` and cargo runs `cargo-foo`.

use std::{
    ffi::{OsStr, OsString},
    fmt::Write as _,
    path::{Path, PathBuf},
    process::ExitStatus,
};

use crate::exit::Exit;

/// The prefix that an external subcommand's binary carries: `krabka-restore`
/// provides `krabka restore`.
pub const EXTERNAL_PREFIX: &str = "krabka-";

/// One external subcommand that `krabka --help` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownExternal {
    /// The subcommand, as the operator types it after `krabka`.
    pub name: &'static str,
    /// The binary on `PATH` that provides it.
    pub binary: &'static str,
    /// What the subcommand does and which repository ships the binary.
    pub summary: &'static str,
}

/// The external subcommands that ship from a krabka-io repository.
///
/// The help text is rendered from this table, so an operator can find every
/// plugin that exists even when it is not installed.
pub const KNOWN_EXTERNALS: &[KnownExternal] = &[
    KnownExternal {
        name: "admin-ui",
        binary: "krabka-admin-ui",
        summary: "The operator web UI. This repository builds it as a separate binary, so `krabka` keeps its own dependency graph.",
    },
    KnownExternal {
        name: "backup",
        binary: "krabka-backup",
        summary: "Captures the inputs of a point-in-time restore. The krabka-broker repository ships it.",
    },
    KnownExternal {
        name: "barrier",
        binary: "krabka-barrier",
        summary: "Defines barrier groups, triggers and verifies cuts. The krabka-broker repository ships it.",
    },
    KnownExternal {
        name: "guard",
        binary: "krabka-guard",
        summary: "Freezes and thaws topic writes, with a two-person break-glass rule. The krabka-broker repository ships it.",
    },
    KnownExternal {
        name: "restore",
        binary: "krabka-restore",
        summary: "Point-in-time restore of a cluster data directory. The krabka-broker repository ships it.",
    },
];

/// The tail of `krabka -h`: the rule and the names, on one screen.
#[must_use]
pub fn short_help() -> String {
    let names = KNOWN_EXTERNALS
        .iter()
        .map(|external| format!("`{}`", external.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Any subcommand that is not built in runs as `krabka-<name>` from PATH, such as {names}. Run `krabka --help` for where each one ships from."
    )
}

/// The tail of `krabka --help`: the rule, then each known external
/// subcommand with the binary that provides it.
#[must_use]
pub fn long_help() -> String {
    let mut help = String::from(
        "External subcommands:\n  A subcommand that is not built in runs as `krabka-<name>` from PATH, the way git runs `git-foo`. Krabka looks the binary up at run time, so a subcommand that you did not install does not run.\n",
    );
    let width = KNOWN_EXTERNALS
        .iter()
        .map(|external| external.name.len())
        .max()
        .unwrap_or(0);
    for external in KNOWN_EXTERNALS {
        let _ = write!(
            help,
            "\n  {:width$}  `{}`: {}",
            external.name, external.binary, external.summary
        );
    }
    help
}

/// Where the `krabka-<name>` binary for one subcommand is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// An executable file.
    Executable(PathBuf),
    /// A file with the name, but no candidate on `PATH` can be executed.
    NotExecutable(PathBuf),
    /// No file with the name is on `PATH`.
    Missing,
}

/// Finds `krabka-<name>` on `path`, a `PATH`-style list.
///
/// The first executable candidate wins. When a file with the name exists but
/// none is executable, the first such file is returned so the operator sees
/// which install is broken, as a shell reports 126 for the same case. An empty
/// `PATH` entry is skipped rather than read as the working directory, so a
/// stray file in the directory where `krabka` runs cannot become a plugin.
#[must_use]
pub fn resolve(name: &OsStr, path: Option<&OsStr>) -> Resolution {
    if Path::new(name).components().count() != 1 || name.is_empty() {
        return Resolution::Missing;
    }
    let mut binary = OsString::from(EXTERNAL_PREFIX);
    binary.push(name);
    let mut not_executable = None;
    for dir in path.map(std::env::split_paths).into_iter().flatten() {
        if dir.as_os_str().is_empty() {
            continue;
        }
        let candidate = dir.join(&binary);
        if !candidate.is_file() {
            continue;
        }
        if is_executable(&candidate) {
            return Resolution::Executable(candidate);
        }
        not_executable.get_or_insert(candidate);
    }
    not_executable.map_or(Resolution::Missing, Resolution::NotExecutable)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    path.metadata()
        .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

/// Runs `krabka-<name>` with the remaining arguments and returns how the
/// process ends.
///
/// The plugin inherits stdin, stdout and stderr. Its exit code passes through
/// unchanged, and a death by signal n is reported as 128 + n, as a shell
/// reports it.
///
/// On unix, the parent ignores SIGINT and SIGQUIT while the plugin runs. The
/// plugin is in the same process group, so Ctrl-C at a terminal reaches it,
/// and the plugin rather than `krabka` decides how the command ends. SIGTERM
/// sent to `krabka` is forwarded to the plugin.
pub async fn run(argv: &[OsString]) -> Exit {
    let Some((name, rest)) = argv.split_first() else {
        return Exit::Usage;
    };
    let path = std::env::var_os("PATH");
    let binary = match resolve(name, path.as_deref()) {
        Resolution::Executable(binary) => binary,
        Resolution::NotExecutable(binary) => {
            eprintln!(
                "krabka: {} was found on PATH but is not executable",
                binary.display()
            );
            return Exit::CannotExecute;
        }
        Resolution::Missing => {
            eprintln!(
                "krabka: `{}` is not a krabka command, and no `{EXTERNAL_PREFIX}{}` was found on PATH",
                name.to_string_lossy(),
                name.to_string_lossy(),
            );
            eprintln!("krabka: see `krabka --help` for the built-in commands");
            return Exit::NotFound;
        }
    };
    match supervise(&binary, rest).await {
        Ok(status) => exit_for(status),
        Err(error) => {
            eprintln!("krabka: failed to run {}: {error}", binary.display());
            Exit::CannotExecute
        }
    }
}

#[cfg(unix)]
async fn supervise(binary: &Path, args: &[OsString]) -> std::io::Result<ExitStatus> {
    use tokio::signal::unix::{SignalKind, signal};

    // Registered before the spawn, so no signal can arrive between the spawn
    // and the registration and take the default action on the parent.
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut quit = signal(SignalKind::quit())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut child = tokio::process::Command::new(binary).args(args).spawn()?;
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw);
    loop {
        tokio::select! {
            status = child.wait() => return status,
            _ = interrupt.recv() => {}
            _ = quit.recv() => {}
            _ = terminate.recv() => {
                if let Some(pid) = pid {
                    let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
                }
            }
        }
    }
}

#[cfg(not(unix))]
async fn supervise(binary: &Path, args: &[OsString]) -> std::io::Result<ExitStatus> {
    tokio::process::Command::new(binary)
        .args(args)
        .status()
        .await
}

fn exit_for(status: ExitStatus) -> Exit {
    if let Some(code) = status.code() {
        return Exit::Passthrough(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        status.signal().map_or(Exit::Failure, Exit::Signal)
    }
    #[cfg(not(unix))]
    {
        Exit::Failure
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn a_name_with_a_path_separator_is_never_resolved() {
        let dir = tempfile::tempdir().unwrap();
        let path = std::env::join_paths([dir.path()]).unwrap();
        for name in ["../krabka-x", "a/b", ""] {
            assert!(resolve(OsStr::new(name), Some(&path)) == Resolution::Missing);
        }
    }

    #[test]
    fn an_unset_path_resolves_nothing() {
        assert!(resolve(OsStr::new("restore"), None) == Resolution::Missing);
    }
}
