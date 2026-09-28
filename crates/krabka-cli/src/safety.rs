//! Guardrails for commands that change a cluster: `--dry-run`, `--yes` and a
//! confirmation prompt.
//!
//! The JVM tools have neither flag, so both are krabka additions. A script
//! written for `kafka-topics` never passes them and is not affected.
//!
//! `--dry-run` is a contract: the command performs every validation and every
//! read it needs to know what it would touch, issues no mutating request,
//! and reports through the `--output` layer in the shape of the real report,
//! marked as a dry run. An operator reads the dry run, then runs the same
//! command without the flag.

use std::io::{self, BufRead, IsTerminal as _, Write};

use clap::Args;

use crate::exit::Exit;

/// `--dry-run` and `--yes`, for a command whose only guard is the
/// confirmation prompt.
#[derive(Debug, Args, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConfirmArgs {
    /// Validate and resolve everything, change nothing, and report what would
    /// happen.
    #[arg(long)]
    pub dry_run: bool,
    /// Do not ask for confirmation.
    #[arg(short = 'y', long, env = "KRABKA_ASSUME_YES")]
    pub yes: bool,
}

/// [`ConfirmArgs`] and `--force`, for a command that also refuses an
/// operation on a safety check.
///
/// `--yes` answers the prompt. `--force` overrides a safety check, such as a
/// refusal to delete an internal topic. A correctness check is never
/// overridden.
#[derive(Debug, Args, Clone, Copy, Default, PartialEq, Eq)]
pub struct SafetyArgs {
    #[command(flatten)]
    pub confirm: ConfirmArgs,
    /// Proceed past a refusal that is a safety check rather than a
    /// correctness check.
    #[arg(long)]
    pub force: bool,
}

/// What a confirmed operation will change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impact {
    /// One line that states the change, for example `delete 2 topic(s)`.
    pub summary: String,
    /// The resources that the change touches, one per line of the prompt.
    pub resources: Vec<String>,
}

/// Why an operation did not proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Stdin is not a terminal and `--yes` was not given. Exits
    /// [`Exit::Usage`], because the operator corrects it on the command line.
    NonInteractive,
    /// The operator did not answer yes. Exits [`Exit::Cancelled`].
    Declined,
}

impl Refusal {
    /// The exit code of the refusal.
    #[must_use]
    pub const fn exit(&self) -> Exit {
        match self {
            Self::NonInteractive => Exit::Usage,
            Self::Declined => Exit::Cancelled,
        }
    }

    /// The message that the output layer prints.
    #[must_use]
    pub const fn message(&self) -> &'static str {
        match self {
            Self::NonInteractive => {
                "refusing to prompt for confirmation on a non-interactive stdin; pass --yes to proceed"
            }
            Self::Declined => "not confirmed; nothing was changed",
        }
    }
}

/// Asks for confirmation of `impact` on the process's stdin and stderr.
///
/// The prompt goes to stderr, so stdout carries only the command's report. It
/// runs on a blocking thread, so Ctrl-C still cancels the command while the
/// prompt waits.
///
/// # Errors
/// Returns the [`Refusal`] when the operation must not proceed.
pub async fn confirm(yes: bool, command: &str, impact: Impact) -> Result<(), Refusal> {
    if yes {
        return Ok(());
    }
    let command = command.to_owned();
    tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let interactive = stdin.is_terminal();
        confirm_with(
            yes,
            interactive,
            &command,
            &impact,
            &mut stdin.lock(),
            &mut io::stderr().lock(),
        )
    })
    .await
    .unwrap_or(Err(Refusal::Declined))
}

/// The decision of [`confirm`], with the terminal check and both streams
/// passed in.
///
/// Under `yes` it returns at once and neither reads nor writes. On a stdin
/// that is not a terminal it refuses without reading, so a pipeline fails at
/// once instead of waiting on a read. Otherwise it prints the impact and
/// proceeds only on `y` or `yes`, in any case. End of input declines.
///
/// # Errors
/// Returns the [`Refusal`] when the operation must not proceed.
pub fn confirm_with(
    yes: bool,
    interactive: bool,
    command: &str,
    impact: &Impact,
    input: &mut dyn BufRead,
    prompt: &mut dyn Write,
) -> Result<(), Refusal> {
    if yes {
        return Ok(());
    }
    if !interactive {
        return Err(Refusal::NonInteractive);
    }
    let asked = (|| -> io::Result<String> {
        writeln!(prompt, "{command}: this will {}:", impact.summary)?;
        for resource in &impact.resources {
            writeln!(prompt, "  {resource}")?;
        }
        write!(prompt, "Proceed? [y/N] ")?;
        prompt.flush()?;
        let mut answer = String::new();
        input.read_line(&mut answer)?;
        Ok(answer)
    })();
    match asked {
        Ok(answer) if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") => Ok(()),
        _ => Err(Refusal::Declined),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser;

    use super::*;

    fn impact() -> Impact {
        Impact {
            summary: "delete 2 topic(s)".into(),
            resources: vec!["orders".into(), "payments".into()],
        }
    }

    const PROMPT: &str =
        "krabka topics: this will delete 2 topic(s):\n  orders\n  payments\nProceed? [y/N] ";

    #[test]
    fn confirm_proceeds_refuses_or_prompts() {
        struct Case {
            interactive: bool,
            yes: bool,
            stdin: &'static str,
            outcome: Result<(), Refusal>,
            prompt: &'static str,
            unread: &'static str,
        }
        let cases = [
            Case {
                interactive: false,
                yes: true,
                stdin: "n\n",
                outcome: Ok(()),
                prompt: "",
                unread: "n\n",
            },
            Case {
                interactive: true,
                yes: true,
                stdin: "n\n",
                outcome: Ok(()),
                prompt: "",
                unread: "n\n",
            },
            Case {
                interactive: false,
                yes: false,
                stdin: "y\n",
                outcome: Err(Refusal::NonInteractive),
                prompt: "",
                unread: "y\n",
            },
            Case {
                interactive: true,
                yes: false,
                stdin: "y\nrest",
                outcome: Ok(()),
                prompt: PROMPT,
                unread: "rest",
            },
            Case {
                interactive: true,
                yes: false,
                stdin: "  YES \n",
                outcome: Ok(()),
                prompt: PROMPT,
                unread: "",
            },
            Case {
                interactive: true,
                yes: false,
                stdin: "n\n",
                outcome: Err(Refusal::Declined),
                prompt: PROMPT,
                unread: "",
            },
            Case {
                interactive: true,
                yes: false,
                stdin: "\n",
                outcome: Err(Refusal::Declined),
                prompt: PROMPT,
                unread: "",
            },
            Case {
                interactive: true,
                yes: false,
                stdin: "",
                outcome: Err(Refusal::Declined),
                prompt: PROMPT,
                unread: "",
            },
        ];
        for case in cases {
            let mut input = case.stdin.as_bytes();
            let mut prompt = Vec::new();
            let outcome = confirm_with(
                case.yes,
                case.interactive,
                "krabka topics",
                &impact(),
                &mut input,
                &mut prompt,
            );
            check!(
                (outcome, String::from_utf8(prompt).unwrap(), input)
                    == (case.outcome, case.prompt.to_owned(), case.unread.as_bytes())
            );
        }
    }

    #[test]
    fn a_non_interactive_refusal_is_a_usage_error_that_names_yes() {
        let refusal = Refusal::NonInteractive;
        check!(refusal.exit() == Exit::Usage);
        check!(refusal.message().contains("--yes"));
        check!(Refusal::Declined.exit() == Exit::Cancelled);
    }

    #[derive(Debug, Parser)]
    struct Command {
        #[command(flatten)]
        safety: SafetyArgs,
    }

    #[test]
    fn safety_flags_parse_from_the_command_line() {
        let parsed = |argv: &[&str]| Command::try_parse_from(argv).unwrap().safety;
        let all = SafetyArgs {
            confirm: ConfirmArgs {
                dry_run: true,
                yes: true,
            },
            force: true,
        };
        check!(parsed(&["krabka", "--dry-run", "--yes", "--force"]) == all);
        check!(parsed(&["krabka", "--dry-run", "-y", "--force"]) == all);
        check!(parsed(&["krabka"]) == SafetyArgs::default());
        assert!(Command::try_parse_from(["krabka", "--yes=false"]).is_err());
    }

    #[tokio::test]
    async fn confirm_under_yes_returns_without_touching_stdin() {
        assert!(confirm(true, "krabka topics", impact()).await == Ok(()));
    }
}
