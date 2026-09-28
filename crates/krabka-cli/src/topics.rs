//! `krabka topics`, the counterpart of `kafka-topics`.

use std::collections::BTreeMap;

use clap::{ArgGroup, Args};
use krabka_client_admin::{CreateTopicOutcome, CreateTopicSpec, DeleteTopicOutcome, KafkaError};
use serde_json::json;

use crate::{
    common::key_value,
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
    safety::{ConfirmArgs, Impact, confirm},
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
    #[command(flatten)]
    confirm: ConfirmArgs,
}

impl TopicsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        if (self.create.is_some() || self.delete.is_some() || self.describe.is_some())
            && self.topic.is_empty()
        {
            return Err("--topic is required for this operation".into());
        }
        let dry_run = self.confirm.dry_run;
        if dry_run && (self.list.is_some() || self.describe.is_some()) {
            return Err("--dry-run is only valid with --create or --delete".into());
        }
        let mut client = self.connection.connect("topics").await?;
        if self.create.is_some() {
            let result = if dry_run {
                // What CreateTopics would answer for each topic, from the
                // metadata the broker holds now.
                let names = self.topic.iter().map(String::as_str).collect::<Vec<_>>();
                let existing = client.metadata(&names).await?;
                let outcomes = self
                    .topic
                    .iter()
                    .map(|name| {
                        let exists = existing
                            .topics
                            .iter()
                            .any(|topic| &topic.name == name && topic.error.is_none());
                        CreateTopicOutcome {
                            name: name.clone(),
                            topic_id: None,
                            error: exists.then(|| KafkaError {
                                code: 36,
                                name: "TOPIC_ALREADY_EXISTS",
                                message: Some(format!("Topic '{name}' already exists.")),
                            }),
                        }
                    })
                    .collect::<Vec<_>>();
                created(&outcomes).into_dry_run()
            } else {
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
                created(
                    &client
                        .create_topics(&specs, self.connection.timeout)
                        .await?,
                )
            };
            return Ok(result);
        }
        if self.delete.is_some() {
            let names = self.topic.iter().map(String::as_str).collect::<Vec<_>>();
            if dry_run {
                // What DeleteTopics would answer for each topic: a topic that
                // the metadata does not know fails as it would fail there.
                let metadata = client.metadata(&names).await?;
                let outcomes = metadata
                    .topics
                    .into_iter()
                    .map(|topic| DeleteTopicOutcome {
                        name: topic.name,
                        error: topic.error,
                    })
                    .collect::<Vec<_>>();
                return Ok(deleted(&outcomes).into_dry_run());
            }
            confirm(
                self.confirm.yes,
                "krabka topics",
                Impact {
                    summary: format!("delete {} topic(s)", names.len()),
                    resources: self.topic.clone(),
                },
            )
            .await?;
            let outcomes = client
                .delete_topics(&names, self.connection.timeout)
                .await?;
            return Ok(deleted(&outcomes));
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

fn created(outcomes: &[CreateTopicOutcome]) -> CommandResult {
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
    CommandResult::rows(human, values, failed)
}

fn deleted(outcomes: &[DeleteTopicOutcome]) -> CommandResult {
    let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
    let values = outcomes
        .iter()
        .map(|outcome| json!({"topic": outcome.name, "error": kafka_error(outcome.error.as_ref())}))
        .collect::<Vec<_>>();
    let human = outcomes
        .iter()
        .map(|outcome| match &outcome.error {
            Some(err) => format!("{}\tERROR\t{} ({})", outcome.name, err.name, err.code),
            None => format!("Deleted topic {}.", outcome.name),
        })
        .collect();
    CommandResult::rows(human, values, failed)
}
