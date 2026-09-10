use std::io::{self, Write as _};

use clap::{Args, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Args, Clone, Copy)]
pub struct OutputArgs {
    #[arg(
        long,
        env = "KRABKA_OUTPUT",
        value_enum,
        default_value_t,
        global = true
    )]
    pub output: OutputFormat,
}

pub trait Emit {
    fn human(&self, writer: &mut dyn io::Write) -> io::Result<()>;
    fn json(&self) -> Value;
}

#[derive(Debug)]
pub struct CommandResult {
    pub human: Vec<String>,
    pub data: Value,
    pub failed: bool,
}

impl CommandResult {
    pub fn success<T: Serialize>(human: Vec<String>, data: T) -> Self {
        Self {
            human,
            data: serde_json::to_value(data).expect("serializable command result"),
            failed: false,
        }
    }

    pub fn rows<T: Serialize>(human: Vec<String>, data: T, failed: bool) -> Self {
        Self {
            human,
            data: serde_json::to_value(data).expect("serializable command result"),
            failed,
        }
    }
}

impl Emit for CommandResult {
    fn human(&self, writer: &mut dyn io::Write) -> io::Result<()> {
        for line in &self.human {
            writeln!(writer, "{line}")?;
        }
        Ok(())
    }

    fn json(&self) -> Value {
        self.data.clone()
    }
}

pub fn emit_success(value: &impl Emit, format: OutputFormat) -> io::Result<()> {
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    match format {
        OutputFormat::Human => value.human(&mut writer),
        OutputFormat::Json => {
            serde_json::to_writer(&mut writer, &json!({"data": value.json()}))?;
            writeln!(writer)
        }
    }
}

pub fn emit_error(message: &str, code: i32, format: OutputFormat) -> io::Result<()> {
    let stderr = io::stderr();
    let mut writer = stderr.lock();
    match format {
        OutputFormat::Human => writeln!(writer, "krabka: {message}"),
        OutputFormat::Json => {
            serde_json::to_writer(
                &mut writer,
                &json!({"error": {"code": code, "message": message}}),
            )?;
            writeln!(writer)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn command_result_emits_human_and_json() {
        let result = CommandResult::success(vec!["ok".into()], json!({"topic": "orders"}));
        let mut human = Vec::new();
        result.human(&mut human).unwrap();
        assert!(human == b"ok\n");
        assert!(result.json() == json!({"topic": "orders"}));
    }
}
