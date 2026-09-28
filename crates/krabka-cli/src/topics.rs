//! `krabka topics`, the counterpart of `kafka-topics`.

use std::collections::BTreeMap;

use clap::{ArgGroup, Args};
use krabka_client_admin::CreateTopicSpec;
use serde_json::json;

use crate::{
    common::key_value,
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
};

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["create", "delete", "list", "describe"])))]
pub struct TopicsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    create: Option<bool>,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    delete: Option<bool>,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    list: Option<bool>,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    describe: Option<bool>,
    #[arg(long)]
    topic: Vec<String>,
    #[arg(long, default_value_t = 1)]
    partitions: i32,
    #[arg(long, default_value_t = 1)]
    replication_factor: i32,
    #[arg(long, value_parser = key_value)]
    config: Vec<(String, String)>,
    #[arg(long)]
    yes: bool,
    #[arg(long)]
    dry_run: bool,
}

impl TopicsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        if (self.create.is_some() || self.delete.is_some() || self.describe.is_some())
            && self.topic.is_empty()
        {
            return Err("--topic is required for this operation".into());
        }
        if self.delete.is_some() && !self.yes && !self.dry_run {
            return Err("topic deletion requires --yes (or --dry-run)".into());
        }
        if self.dry_run && (self.list.is_some() || self.describe.is_some()) {
            return Err("--dry-run is only valid with --create or --delete".into());
        }
        if self.dry_run {
            return Ok(CommandResult::success(
                self.topic
                    .iter()
                    .map(|topic| format!("Would change topic {topic}"))
                    .collect(),
                json!({"topics": self.topic, "dry_run": true}),
            ));
        }
        let mut client = self.connection.connect("topics").await?;
        if self.create.is_some() {
            let configs = self.config.into_iter().collect::<BTreeMap<_, _>>();
            let specs = self
                .topic
                .iter()
                .map(|name| CreateTopicSpec {
                    name: name.clone(),
                    partitions: self.partitions,
                    replicas: self.replication_factor,
                    configs: configs.clone(),
                })
                .collect::<Vec<_>>();
            let outcomes = client
                .create_topics(&specs, self.connection.timeout)
                .await?;
            let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
            let values = outcomes
                .iter()
                .map(|outcome| {
                    json!({"topic": outcome.name, "topic_id": outcome.topic_id, "error": kafka_error(outcome.error.as_ref())})
                })
                .collect::<Vec<_>>();
            let human = outcomes
                .iter()
                .map(|outcome| match &outcome.error {
                    Some(err) => format!("{}\tERROR\t{} ({})", outcome.name, err.name, err.code),
                    None => format!("Created topic {}.", outcome.name),
                })
                .collect();
            return Ok(CommandResult::rows(human, values, failed));
        }
        if self.delete.is_some() {
            let names = self.topic.iter().map(String::as_str).collect::<Vec<_>>();
            let outcomes = client
                .delete_topics(&names, self.connection.timeout)
                .await?;
            let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
            let values = outcomes
                .iter()
                .map(|outcome| {
                    json!({"topic": outcome.name, "error": kafka_error(outcome.error.as_ref())})
                })
                .collect::<Vec<_>>();
            let human = outcomes
                .iter()
                .map(|outcome| match &outcome.error {
                    Some(err) => format!("{}\tERROR\t{} ({})", outcome.name, err.name, err.code),
                    None => format!("Deleted topic {}.", outcome.name),
                })
                .collect();
            return Ok(CommandResult::rows(human, values, failed));
        }
        let names = self.topic.iter().map(String::as_str).collect::<Vec<_>>();
        let metadata = client.metadata(&names).await?;
        let values = metadata
            .topics
            .iter()
            .map(|topic| {
                json!({"topic": topic.name, "topic_id": topic.topic_id, "partitions": topic.partition_count, "replication_factor": topic.replication_factor, "error": kafka_error(topic.error.as_ref())})
            })
            .collect::<Vec<_>>();
        let failed = metadata.topics.iter().any(|topic| topic.error.is_some());
        let human = metadata
            .topics
            .iter()
            .map(|topic| {
                if let Some(err) = &topic.error {
                    format!("{}\tERROR\t{} ({})", topic.name, err.name, err.code)
                } else if self.describe.is_some() {
                    format!(
                        "Topic: {}\tPartitionCount: {}\tReplicationFactor: {}",
                        topic.name, topic.partition_count, topic.replication_factor
                    )
                } else {
                    topic.name.clone()
                }
            })
            .collect();
        Ok(CommandResult::rows(human, values, failed))
    }
}
