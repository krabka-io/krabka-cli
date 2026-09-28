//! `krabka configs`, the counterpart of `kafka-configs`.

use std::collections::BTreeMap;

use clap::{ArgGroup, Args};
use krabka_client_admin::{
    AlterConfigOp, ConfigResource, DescribeConfigsOptions, IncrementalAlterConfigsOptions,
};
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
            let resource = ConfigResource::topic(&self.entity_name);
            let result = client
                .describe_configs(
                    std::slice::from_ref(&resource),
                    DescribeConfigsOptions::default(),
                )
                .await?;
            let mut values = Vec::new();
            let mut human = Vec::new();
            for (resource, config) in &result {
                let overrides = config
                    .as_ref()
                    .map_err(|error| {
                        CommandError::from(format!("{} ({})", error.name, error.code))
                    })?
                    .dynamic_overrides(resource);
                human.extend(
                    overrides
                        .iter()
                        .map(|(key, value)| format!("{}\t{}={}", resource.name, key, value)),
                );
                values.push(json!({"entity_type": self.entity_type, "entity_name": resource.name, "configs": overrides}));
            }
            return Ok(CommandResult::success(human, values));
        }
        let mut ops = self
            .add_config
            .into_iter()
            .map(|(key, value)| AlterConfigOp::set(key, value))
            .collect::<Vec<_>>();
        ops.extend(self.delete_config.into_iter().map(AlterConfigOp::delete));
        if ops.is_empty() {
            return Err("--alter requires --add-config or --delete-config".into());
        }
        let changes = BTreeMap::from([(ConfigResource::topic(&self.entity_name), ops)]);
        let outcomes = client
            .incremental_alter_configs(&changes, IncrementalAlterConfigsOptions::default())
            .await?;
        let failed = outcomes.values().any(Result::is_err);
        let human = outcomes
            .iter()
            .map(|(resource, outcome)| match outcome {
                Err(err) => format!("{}\tERROR\t{} ({})", resource.name, err.name, err.code),
                Ok(()) => format!("Completed updating config for topic {}.", resource.name),
            })
            .collect();
        let values = outcomes
            .iter()
            .map(|(resource, outcome)| json!({"topic": resource.name, "error": kafka_error(outcome.as_ref().err())}))
            .collect::<Vec<_>>();
        Ok(CommandResult::rows(human, values, failed))
    }
}
