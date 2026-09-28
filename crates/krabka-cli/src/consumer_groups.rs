//! `krabka consumer-groups`, the counterpart of `kafka-consumer-groups`.

use std::collections::BTreeMap;

use clap::{ArgGroup, Args};
use serde_json::json;

use crate::{
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
};

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["describe", "reset_offsets"])))]
pub struct ConsumerGroupsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    describe: bool,
    #[arg(long)]
    reset_offsets: bool,
    #[arg(long)]
    group: String,
    #[arg(long, requires = "reset_offsets")]
    topic: Option<String>,
    #[arg(long, requires = "reset_offsets", allow_hyphen_values = true)]
    partition: Option<i32>,
    #[arg(long, requires = "reset_offsets", allow_hyphen_values = true)]
    to_offset: Option<i64>,
    #[arg(long, requires = "reset_offsets")]
    yes: bool,
}

impl ConsumerGroupsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        if self.reset_offsets && !self.yes {
            return Err("offset reset requires --yes".into());
        }
        if self.partition.is_some_and(|value| value < 0)
            || self.to_offset.is_some_and(|value| value < 0)
        {
            return Err("--partition and --to-offset must be non-negative".into());
        }
        let mut client = self.connection.connect("consumer-groups").await?;
        if self.reset_offsets {
            let topic = self
                .topic
                .ok_or("--topic is required with --reset-offsets")?;
            let partition = self
                .partition
                .ok_or("--partition is required with --reset-offsets")?;
            let offset = self
                .to_offset
                .ok_or("--to-offset is required with --reset-offsets")?;
            let outcomes = client
                .alter_consumer_group_offsets(
                    &self.group,
                    &BTreeMap::from([((topic, partition), offset)]),
                )
                .await?;
            let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
            let human = outcomes
                .iter()
                .map(|outcome| match &outcome.error {
                    Some(error) => format!(
                        "{}\t{}\t{}\tERROR\t{} ({})",
                        self.group, outcome.topic, outcome.partition, error.name, error.code
                    ),
                    None => format!(
                        "Reset group {} topic {} partition {}.",
                        self.group, outcome.topic, outcome.partition
                    ),
                })
                .collect();
            let values = outcomes
                .iter()
                .map(|outcome| json!({"group": self.group, "topic": outcome.topic, "partition": outcome.partition, "error": kafka_error(outcome.error.as_ref())}))
                .collect::<Vec<_>>();
            return Ok(CommandResult::rows(human, values, failed));
        }
        let offsets = client.list_consumer_group_offsets(&self.group).await?;
        Ok(group_offsets_result(&self.group, &offsets))
    }
}

fn group_offsets_result(group: &str, offsets: &BTreeMap<(String, i32), i64>) -> CommandResult {
    let values = offsets
            .iter()
            .map(|((topic, partition), offset)| {
                json!({"group": group, "topic": topic, "partition": partition, "offset": offset})
            })
            .collect::<Vec<_>>();
    let human = offsets
        .iter()
        .map(|((topic, partition), offset)| format!("{group}\t{topic}\t{partition}\t{offset}"))
        .collect();
    CommandResult::success(human, values)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn group_offset_human_output_has_unquoted_topic() {
        let result = group_offsets_result("workers", &BTreeMap::from([(("orders".into(), 0), 42)]));
        assert!(result.human == ["workers\torders\t0\t42"]);
    }
}
