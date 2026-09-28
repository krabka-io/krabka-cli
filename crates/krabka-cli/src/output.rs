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
}

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
        }
    }

    /// Marks the payload as the report of a `--dry-run`.
    #[must_use]
    pub fn into_dry_run(self) -> Self {
        Self {
            dry_run: true,
            ..self
        }
    }
}

impl Emit for CommandResult {
    fn human(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        if self.dry_run {
            writeln!(writer, "DRY RUN: no change was made.")?;
        }
        for line in &self.human {
            writeln!(writer, "{line}")?;
        }
        Ok(())
    }

    fn json(&self) -> Value {
        self.data.clone()
    }

    fn dry_run(&self) -> bool {
        self.dry_run
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
    /// Any other failure, as a message.
    Other(String),
}

impl CommandError {
    /// The exit code of the failure.
    #[must_use]
    pub const fn exit(&self) -> Exit {
        match self {
            Self::Refused(refusal) => refusal.exit(),
            Self::Broker { .. } | Self::Other(_) => Exit::Failure,
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
                name,
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
            Self::Other(message) => f.write_str(message),
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

/// Writes a payload to stdout.
///
/// # Errors
/// Returns the error of stdout.
pub fn emit_success(value: &impl Emit, format: OutputFormat) -> io::Result<()> {
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
