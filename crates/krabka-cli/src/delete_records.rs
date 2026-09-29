//! `krabka delete-records`, the counterpart of `kafka-delete-records`.
//!
//! The flags, the offset JSON file, the checks and the report are those of
//! `DeleteRecordsCommand` in Kafka 4.3.1. The file is
//!
//! ```json
//! {"partitions": [{"topic": "foo", "partition": 1, "offset": 1}], "version": 1}
//! ```
//!
//! An offset of `-1` deletes up to the high watermark. Any other offset
//! reaches the broker as written, so a negative offset fails there with
//! `OFFSET_OUT_OF_RANGE`, as it does for the JVM tool.
//!
//! The command deletes data, so it asks for confirmation. `--yes` answers the
//! prompt and `--dry-run` reports what would be deleted.

use std::{collections::BTreeMap, path::PathBuf};

use clap::Args;
use krabka_client_admin::{DeleteRecordsOp, DeleteRecordsOutcome};
use serde_json::{Map, Value, json};

use crate::{
    connection::ConnectionArgs,
    kafka_errors::{admin_error, exception_text, row_error},
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
};

/// The offset that deletes every record below the high watermark.
const HIGH_WATERMARK: i64 = -1;

#[derive(Debug, Args)]
pub struct DeleteRecordsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// The JSON file with offset per partition. The format to use is
    /// {"partitions": [{"topic": "foo", "partition": 1, "offset": 1}],
    /// "version": 1}
    #[arg(long, required = true)]
    offset_json_file: PathBuf,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

impl DeleteRecordsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let text = tokio::fs::read_to_string(&self.offset_json_file)
            .await
            .map_err(|error| {
                format!(
                    "read offset json file {}: {error}",
                    self.offset_json_file.display()
                )
            })?;
        let ops = parse_offset_json(&text)?;
        let mut client = self.connection.connect("delete-records").await?;
        if self.confirm.dry_run {
            let mut topics = ops.iter().map(|op| op.topic.as_str()).collect::<Vec<_>>();
            topics.sort_unstable();
            topics.dedup();
            let metadata = client.metadata(&topics).await.map_err(admin_error)?;
            let partition_counts = metadata
                .topics
                .into_iter()
                .map(|topic| {
                    let count = topic
                        .error
                        .map_or(Ok(topic.partition_count), |error| Err(error.code));
                    (topic.name, count)
                })
                .collect::<BTreeMap<_, _>>();
            return Ok(planned(&ops, &partition_counts).into_dry_run());
        }
        confirm(
            self.confirm.yes,
            "krabka delete-records",
            Impact {
                summary: format!("delete records from {} partition(s)", ops.len()),
                resources: ops.iter().map(describe_op).collect(),
            },
        )
        .await?;
        let outcomes = client
            .delete_records(&ops, self.connection.timeout)
            .await
            .map_err(admin_error)?;
        Ok(deleted(&ops, &outcomes))
    }
}

/// Parses Kafka's offset JSON file into one operation per partition, in file
/// order, with the messages of Kafka's parser for a file that it refuses.
fn parse_offset_json(text: &str) -> Result<Vec<DeleteRecordsOp>, String> {
    let json = serde_json::from_str::<Value>(text)
        .map_err(|_| "The input string is not a valid JSON".to_owned())?;
    let root = object(&json)?;
    let version = root.get("version").map_or(Ok(1), integer)?;
    if version != 1 {
        return Err(format!("Not supported version field value {version}"));
    }
    let partitions = root
        .get("partitions")
        .ok_or_else(|| "Missing partitions field".to_owned())?;
    let partitions = partitions
        .as_array()
        .ok_or_else(|| format!("Expected JSON array, received {partitions}"))?;
    let mut ops = Vec::<DeleteRecordsOp>::with_capacity(partitions.len());
    let mut duplicates = Vec::<String>::new();
    for entry in partitions {
        let entry = object(entry)?;
        let op = DeleteRecordsOp {
            topic: string(field(entry, "topic")?)?,
            partition: integer(field(entry, "partition")?)?,
            offset: long(field(entry, "offset")?)?,
        };
        let name = format!("{}-{}", op.topic, op.partition);
        let seen = ops
            .iter()
            .any(|seen| (&seen.topic, seen.partition) == (&op.topic, op.partition));
        if seen && !duplicates.contains(&name) {
            duplicates.push(name);
        }
        ops.push(op);
    }
    if !duplicates.is_empty() {
        return Err(format!(
            "Offset json file contains duplicate topic partitions: {}",
            duplicates.join(",")
        ));
    }
    Ok(ops)
}

fn object(value: &Value) -> Result<&Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("Expected JSON object, received {value}"))
}

fn field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a Value, String> {
    object
        .get(name)
        .ok_or_else(|| format!("No such field exists: `{name}`"))
}

fn string(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("Expected `String` value, received {value}"))
}

fn integer(value: &Value) -> Result<i32, String> {
    value
        .as_i64()
        .and_then(|number| i32::try_from(number).ok())
        .ok_or_else(|| format!("Expected `Integer` value, received {value}"))
}

fn long(value: &Value) -> Result<i64, String> {
    value
        .as_i64()
        .ok_or_else(|| format!("Expected `Long` value, received {value}"))
}

fn describe_op(op: &DeleteRecordsOp) -> String {
    if op.offset == HIGH_WATERMARK {
        format!("{}-{} before the high watermark", op.topic, op.partition)
    } else {
        format!("{}-{} before offset {}", op.topic, op.partition, op.offset)
    }
}

/// The two lines that Kafka prints around the call, before the rows.
fn report(rows: Vec<String>) -> Vec<String> {
    [
        "Executing records delete operation",
        "Records delete operation completed:",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain(rows)
    .collect()
}

/// The report of the call, one row per operation in file order.
fn deleted(ops: &[DeleteRecordsOp], outcomes: &[DeleteRecordsOutcome]) -> CommandResult {
    let rows = ops
        .iter()
        .map(|op| {
            let outcome = outcomes
                .iter()
                .find(|outcome| (&outcome.topic, outcome.partition) == (&op.topic, op.partition));
            // A partition that the answer leaves out fails as Kafka's
            // DeleteRecordsHandler sanity check fails it.
            let result = outcome.map_or(Err(None), |outcome| match outcome.error_code {
                0 => Ok(outcome.low_watermark),
                code => Err(Some(code)),
            });
            (op, result)
        })
        .collect::<Vec<_>>();
    let failed = rows.iter().any(|(_, result)| result.is_err());
    let human = rows
        .iter()
        .map(|(op, result)| {
            let partition = format!("{}-{}", op.topic, op.partition);
            match result {
                Ok(low_watermark) => {
                    format!("partition: {partition}\tlow_watermark: {low_watermark}")
                }
                Err(Some(code)) => {
                    format!("partition: {partition}\terror: {}", exception_text(*code))
                }
                Err(None) => format!(
                    "partition: {partition}\terror: org.apache.kafka.common.errors.ApiException: \
                     The response did not contain a result for topic partition {partition}"
                ),
            }
        })
        .collect();
    let data = rows
        .iter()
        .map(|(op, result)| {
            json!({
                "topic": op.topic,
                "partition": op.partition,
                "offset": op.offset,
                "low_watermark": result.ok(),
                "error": match result {
                    Ok(_) => Value::Null,
                    Err(code) => kafka_error(Some(&row_error(code.unwrap_or(-1)))),
                },
            })
        })
        .collect::<Vec<_>>();
    CommandResult::rows(report(human), data, failed)
}

/// The report of a dry run: for each operation, the offset that it would
/// delete below, or the error that the broker's metadata already names.
///
/// `partition_counts` maps each topic to its partition count, or to the error
/// code of a topic that the metadata does not serve.
fn planned(
    ops: &[DeleteRecordsOp],
    partition_counts: &BTreeMap<String, Result<i32, i16>>,
) -> CommandResult {
    const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    let rows = ops
        .iter()
        .map(|op| {
            let error = match partition_counts.get(&op.topic) {
                Some(Ok(count)) if (0..*count).contains(&op.partition) => None,
                Some(Err(code)) => Some(*code),
                _ => Some(UNKNOWN_TOPIC_OR_PARTITION),
            };
            (op, error)
        })
        .collect::<Vec<_>>();
    let failed = rows.iter().any(|(_, error)| error.is_some());
    let human = rows
        .iter()
        .map(|(op, error)| {
            let partition = format!("{}-{}", op.topic, op.partition);
            match error {
                None if op.offset == HIGH_WATERMARK => {
                    format!("partition: {partition}\tdelete_before: high_watermark")
                }
                None => format!("partition: {partition}\tdelete_before: {}", op.offset),
                Some(code) => format!("partition: {partition}\terror: {}", exception_text(*code)),
            }
        })
        .collect();
    let data = rows
        .iter()
        .map(|(op, error)| {
            json!({
                "topic": op.topic,
                "partition": op.partition,
                "offset": op.offset,
                "low_watermark": Value::Null,
                "error": error.map_or(Value::Null, |code| kafka_error(Some(&row_error(code)))),
            })
        })
        .collect::<Vec<_>>();
    CommandResult::rows(report(human), data, failed)
}

#[cfg(test)]
mod tests;
