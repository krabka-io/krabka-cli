//! `krabka configs`, the counterpart of `kafka-configs`.

use clap::{ArgGroup, Args};
use krabka_client_admin::IncrementalAlterOp;
use serde_json::json;

use crate::{
    common::key_value,
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
};

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["describe", "alter"])))]
pub struct ConfigsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    describe: bool,
    #[arg(long)]
    alter: bool,
    #[arg(long, default_value = "topics", value_parser = ["topics"])]
    entity_type: String,
    #[arg(long)]
    entity_name: String,
    #[arg(long, value_parser = key_value)]
    add_config: Vec<(String, String)>,
    #[arg(long)]
    delete_config: Vec<String>,
}

impl ConfigsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let mut client = self.connection.connect("configs").await?;
        if self.describe {
            let result = client.describe_configs(&[&self.entity_name]).await?;
            let values = result
                .iter()
                .map(|item| json!({"entity_type": self.entity_type, "entity_name": item.topic, "configs": item.overrides}))
                .collect::<Vec<_>>();
            let human = result
                .iter()
                .flat_map(|item| {
                    item.overrides
                        .iter()
                        .map(|(key, value)| format!("{}\t{}={}", item.topic, key, value))
                })
                .collect();
            return Ok(CommandResult::success(human, values));
        }
        let mut ops = self
            .add_config
            .into_iter()
            .map(|(key, value)| IncrementalAlterOp::Set {
                topic: self.entity_name.clone(),
                key,
                value,
            })
            .collect::<Vec<_>>();
        ops.extend(
            self.delete_config
                .into_iter()
                .map(|key| IncrementalAlterOp::Delete {
                    topic: self.entity_name.clone(),
                    key,
                }),
        );
        if ops.is_empty() {
            return Err("--alter requires --add-config or --delete-config".into());
        }
        let outcomes = client.incremental_alter_configs(&ops).await?;
        let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
        let human = outcomes
            .iter()
            .map(|outcome| match &outcome.error {
                Some(err) => format!("{}\tERROR\t{} ({})", outcome.topic, err.name, err.code),
                None => format!("Completed updating config for topic {}.", outcome.topic),
            })
            .collect();
        let values = outcomes
            .iter()
            .map(|outcome| json!({"topic": outcome.topic, "error": kafka_error(outcome.error.as_ref())}))
            .collect::<Vec<_>>();
        Ok(CommandResult::rows(human, values, failed))
    }
}
