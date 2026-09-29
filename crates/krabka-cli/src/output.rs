//! The `--output` rendering layer.
//!
//! A command returns an [`Emit`] payload or a [`CommandError`], and this
//! module decides where each goes and in what shape. Under `--output human`
//! the payload is the stdout shape of the equivalent `kafka-*` tool, and a
//! failure is one line on stderr. Under `--output json` stdout carries exactly
//! `{"data": ...}`, with `"dry_run": true` beside it for a dry run, and stderr
//! carries exactly `{"error": {"code": ..., "message": ...}}`. The error code
//! is the process exit code, so a script that reads the envelope and a script
//! that reads `$?` agree. Logs go to stderr and never to stdout.

use std::{fmt, io};

use clap::{Args, ValueEnum};
use krabka_client_admin::{AdminError, KafkaError};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{exit::Exit, safety::Refusal};

/// The shape of a command's output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// The stdout shape of the equivalent `kafka-*` tool.
    #[default]
    Human,
    /// One JSON document: `{"data": ...}` on stdout, or `{"error": ...}` on
    /// stderr.
    Json,
}

/// The `--output` flag.
#[derive(Debug, Args, Clone, Copy)]
pub struct OutputArgs {
    /// The shape of the output.
    #[arg(
        long,
        env = "KRABKA_OUTPUT",
        value_enum,
        default_value_t,
        global = true
    )]
    pub output: OutputFormat,
}

/// A command payload with a human and a JSON rendering.
pub trait Emit {
    /// Writes the human rendering.
    ///
    /// # Errors
    /// Returns the error of the writer.
    fn human(&self, writer: &mut dyn io::Write) -> io::Result<()>;

    /// The JSON rendering, which goes inside `{"data": ...}`.
    fn json(&self) -> Value;

    /// Whether the payload reports what a `--dry-run` would do rather than
    /// what was done.
    fn dry_run(&self) -> bool {
        false
    }

    /// Lines that the human rendering writes to stderr rather than stdout,
    /// where the equivalent `kafka-*` tool prints them to stderr: warnings
    /// and per-resource failures that do not end the command. The JSON
    /// rendering carries the same facts inside `data`, so it ignores these.
    fn notices(&self) -> Vec<String> {
        Vec::new()
    }
}

/// The line that marks the human report of a `--dry-run`.
const DRY_RUN_MARKER: &str = "DRY RUN: no change was made.";

/// The payload that the admin commands return: prepared human lines, a JSON
/// value, and whether any row failed.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandResult {
    /// The human rendering, one entry per line.
    pub human: Vec<String>,
    /// The JSON rendering.
    pub data: Value,
    /// Whether at least one row failed. The command then exits
    /// [`Exit::Failure`] after it prints every row.
    pub failed: bool,
    /// Whether this reports a `--dry-run`.
    pub dry_run: bool,
    /// Lines for stderr under `--output human`. See [`Emit::notices`].
    pub notices: Vec<String>,
    /// Whether the human dry-run marker goes to stderr rather than stdout,
    /// for a command whose stdout must stay byte-identical to the Kafka
    /// tool's own dry-run report.
    pub marker_on_stderr: bool,
    /// How the human rendering ends its last line.
    pub last_line: LastLine,
}

/// How the human rendering of a [`CommandResult`] ends its last line.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LastLine {
    /// With a newline, as every other line.
    #[default]
    Newline,
    /// With no newline, where the Kafka tool writes that line with `printf`
    /// and no `%n`.
    Bare,
}

impl CommandResult {
    /// A payload with no failed row.
    ///
    /// # Panics
    /// Panics if `data` does not serialize to JSON, which for the plain data
    /// types that commands pass is a bug in the command.
    pub fn success<T: Serialize>(human: Vec<String>, data: T) -> Self {
        Self::rows(human, data, false)
    }

    /// A payload of rows, some of which may have failed.
    ///
    /// # Panics
    /// Panics if `data` does not serialize to JSON, which for the plain data
    /// types that commands pass is a bug in the command.
    pub fn rows<T: Serialize>(human: Vec<String>, data: T, failed: bool) -> Self {
        Self {
            human,
            data: serde_json::to_value(data).expect("command data serializes to JSON"),
            failed,
            dry_run: false,
            notices: Vec::new(),
            marker_on_stderr: false,
            last_line: LastLine::Newline,
        }
    }

    /// Ends the human rendering without a newline after its last line.
    #[must_use]
    pub fn without_final_newline(self) -> Self {
        Self {
            last_line: LastLine::Bare,
            ..self
        }
    }

    /// Adds lines for stderr under `--output human`.
    #[must_use]
    pub fn with_notices(mut self, notices: Vec<String>) -> Self {
        self.notices.extend(notices);
        self
    }

    /// Marks the payload as the report of a `--dry-run`.
    #[must_use]
    pub fn into_dry_run(self) -> Self {
        Self {
            dry_run: true,
            ..self
        }
    }

    /// Marks the payload as the report of a `--dry-run` whose human stdout is
    /// already the Kafka tool's dry-run report. The human marker then goes to
    /// stderr, and the JSON marker is unchanged.
    #[must_use]
    pub fn into_kafka_dry_run(self) -> Self {
        Self {
            dry_run: true,
            marker_on_stderr: true,
            ..self
        }
    }
}

impl Emit for CommandResult {
    fn human(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        if self.dry_run && !self.marker_on_stderr {
            writeln!(writer, "{DRY_RUN_MARKER}")?;
        }
        for (index, line) in self.human.iter().enumerate() {
            if self.last_line == LastLine::Bare && index + 1 == self.human.len() {
                write!(writer, "{line}")?;
            } else {
                writeln!(writer, "{line}")?;
            }
        }
        Ok(())
    }

    fn json(&self) -> Value {
        self.data.clone()
    }

    fn dry_run(&self) -> bool {
        self.dry_run
    }

    fn notices(&self) -> Vec<String> {
        let marker = (self.dry_run && self.marker_on_stderr).then(|| DRY_RUN_MARKER.to_owned());
        marker
            .into_iter()
            .chain(self.notices.iter().cloned())
            .collect()
    }
}

/// Why a command failed as a whole.
#[derive(Debug)]
pub enum CommandError {
    /// The broker refused the call, as `AdminError::Broker` reports it.
    Broker {
        api: &'static str,
        code: i16,
        name: &'static str,
        message: Option<String>,
    },
    /// The command did not proceed, such as a declined confirmation.
    Refused(Refusal),
    /// The command line is not valid in a way that clap cannot check, such as
    /// a combination of flags that the JVM tool refuses. Exits
    /// [`Exit::Usage`].
    Usage(String),
    /// The command needs an `AdminClient` call that the pinned
    /// `krabka-client-rs` revision does not have. See
    /// [`crate::compat::not_supported`].
    Unsupported(String),
    /// Any other failure, as a message.
    Other(String),
}

impl CommandError {
    /// The exit code of the failure.
    #[must_use]
    pub const fn exit(&self) -> Exit {
        match self {
            Self::Refused(refusal) => refusal.exit(),
            Self::Usage(_) => Exit::Usage,
            Self::Broker { .. } | Self::Unsupported(_) | Self::Other(_) => Exit::Failure,
        }
    }
}

impl From<Refusal> for CommandError {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<AdminError> for CommandError {
    fn from(error: AdminError) -> Self {
        match error {
            AdminError::Broker {
                api,
                code,
                name,
                message,
            } => Self::Broker {
                api,
                code,
                // The client names only the codes it acts on. Kafka's
                // `Errors` names every code, and runbooks search for those.
                name: if name == "UNKNOWN" {
                    crate::compat::KafkaException::for_code(code).name()
                } else {
                    name
                },
                message,
            },
            other => Self::Other(other.to_string()),
        }
    }
}

impl From<String> for CommandError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

impl From<&str> for CommandError {
    fn from(message: &str) -> Self {
        Self::Other(message.to_owned())
    }
}

/// The message names the Kafka error name and code, because operator runbooks
/// search for those names: `CreateTopics failed: TOPIC_ALREADY_EXISTS (36):
/// topic 'orders' already exists`.
impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker {
                api,
                code,
                name,
                message,
            } => {
                write!(f, "{api} failed: {name} ({code})")?;
                match message {
                    Some(message) if !message.is_empty() => write!(f, ": {message}"),
                    _ => Ok(()),
                }
            }
            Self::Refused(refusal) => f.write_str(refusal.message()),
            Self::Usage(message) | Self::Unsupported(message) | Self::Other(message) => {
                f.write_str(message)
            }
        }
    }
}

/// Renders a per-row Kafka error as JSON, or `null` for a row that succeeded.
#[must_use]
pub fn kafka_error(error: Option<&KafkaError>) -> Value {
    error.map_or(
        Value::Null,
        |error| json!({"code": error.code, "name": error.name, "message": error.message}),
    )
}

/// Writes a payload in `format`.
///
/// # Errors
/// Returns the error of the writer.
pub fn render_success(
    value: &impl Emit,
    format: OutputFormat,
    writer: &mut dyn io::Write,
) -> io::Result<()> {
    match format {
        OutputFormat::Human => value.human(writer),
        OutputFormat::Json => {
            let mut envelope = Map::new();
            envelope.insert("data".into(), value.json());
            if value.dry_run() {
                envelope.insert("dry_run".into(), Value::Bool(true));
            }
            serde_json::to_writer(&mut *writer, &Value::Object(envelope))?;
            writeln!(writer)
        }
    }
}

/// Writes a failure of `command` in `format`.
///
/// # Errors
/// Returns the error of the writer.
pub fn render_error(
    command: &str,
    message: &str,
    code: Exit,
    format: OutputFormat,
    writer: &mut dyn io::Write,
) -> io::Result<()> {
    match format {
        OutputFormat::Human => writeln!(writer, "{command}: {message}"),
        OutputFormat::Json => {
            serde_json::to_writer(
                &mut *writer,
                &json!({"error": {"code": code.code(), "message": message}}),
            )?;
            writeln!(writer)
        }
    }
}

/// Writes the [`Emit::notices`] of a payload in `format`: one line each under
/// `--output human`, nothing under `--output json`.
///
/// # Errors
/// Returns the error of the writer.
pub fn render_notices(
    value: &impl Emit,
    format: OutputFormat,
    writer: &mut dyn io::Write,
) -> io::Result<()> {
    if format == OutputFormat::Human {
        for notice in value.notices() {
            writeln!(writer, "{notice}")?;
        }
    }
    Ok(())
}

/// Writes a payload to stdout and its notices to stderr.
///
/// # Errors
/// Returns the error of stdout or stderr.
pub fn emit_success(value: &impl Emit, format: OutputFormat) -> io::Result<()> {
    render_notices(value, format, &mut io::stderr().lock())?;
    render_success(value, format, &mut io::stdout().lock())
}

/// Writes a failure of `command` to stderr.
///
/// # Errors
/// Returns the error of stderr.
pub fn emit_error(
    command: &str,
    message: &str,
    code: Exit,
    format: OutputFormat,
) -> io::Result<()> {
    render_error(command, message, code, format, &mut io::stderr().lock())
}

#[cfg(test)]
mod tests;
