use std::collections::BTreeMap;

use clap::{ArgGroup, Args, ValueEnum};
use krabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, CreateTopicSpec, IncrementalAlterOp, PatternType,
    PermissionType, ResourceType,
};
use serde_json::{Value, json};

use crate::{connection::ConnectionArgs, output::CommandResult};

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
    pub async fn run(self) -> Result<CommandResult, String> {
        if (self.create.is_some() || self.delete.is_some() || self.describe.is_some())
            && self.topic.is_empty()
        {
            return Err("--topic is required for this operation".into());
        }
        if self.delete.is_some() && !self.yes && !self.dry_run {
            return Err("topic deletion requires --yes (or --dry-run)".into());
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
        let mut client = self.connection.connect("topics").await.map_err(error)?;
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
                .await
                .map_err(error)?;
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
                .await
                .map_err(error)?;
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
        let metadata = client.metadata(&names).await.map_err(error)?;
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
                if self.describe.is_some() {
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
    pub async fn run(self) -> Result<CommandResult, String> {
        let mut client = self.connection.connect("configs").await.map_err(error)?;
        if self.describe {
            let result = client
                .describe_configs(&[&self.entity_name])
                .await
                .map_err(error)?;
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
        let outcomes = client
            .incremental_alter_configs(&ops)
            .await
            .map_err(error)?;
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum AclOperationArg {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

impl From<AclOperationArg> for AclOperation {
    fn from(value: AclOperationArg) -> Self {
        match value {
            AclOperationArg::All => Self::All,
            AclOperationArg::Read => Self::Read,
            AclOperationArg::Write => Self::Write,
            AclOperationArg::Create => Self::Create,
            AclOperationArg::Delete => Self::Delete,
            AclOperationArg::Alter => Self::Alter,
            AclOperationArg::Describe => Self::Describe,
            AclOperationArg::ClusterAction => Self::ClusterAction,
            AclOperationArg::DescribeConfigs => Self::DescribeConfigs,
            AclOperationArg::AlterConfigs => Self::AlterConfigs,
            AclOperationArg::IdempotentWrite => Self::IdempotentWrite,
        }
    }
}

#[derive(Debug, Args)]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["list", "add", "remove"])))]
pub struct AclsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    list: Option<bool>,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    add: Option<bool>,
    #[arg(long, num_args = 0, default_missing_value = "true")]
    remove: Option<bool>,
    #[arg(long)]
    topic: Option<String>,
    #[arg(long)]
    allow_principal: Option<String>,
    #[arg(long)]
    deny_principal: Option<String>,
    #[arg(long, value_enum)]
    operation: Option<AclOperationArg>,
    #[arg(long, default_value = "*")]
    host: String,
    #[arg(long)]
    yes: bool,
}

impl AclsArgs {
    pub async fn run(self) -> Result<CommandResult, String> {
        if self.remove.is_some() && !self.yes {
            return Err("ACL removal requires --yes".into());
        }
        let permission = if self.deny_principal.is_some() {
            PermissionType::Deny
        } else {
            PermissionType::Allow
        };
        let principal = self
            .allow_principal
            .clone()
            .or_else(|| self.deny_principal.clone());
        let filter = AclEntryFilter {
            resource_type: self.topic.as_ref().map(|_| ResourceType::Topic),
            resource_name: self.topic.clone(),
            pattern_type: self.topic.as_ref().map(|_| PatternType::Literal),
            principal: principal.clone(),
            host: (self.host != "*").then(|| self.host.clone()),
            operation: self.operation.map(Into::into),
            permission_type: principal.as_ref().map(|_| permission),
        };
        let mut client = self.connection.connect("acls").await.map_err(error)?;
        if self.list.is_some() {
            return Ok(acl_entries(
                &client.describe_acls(&filter).await.map_err(error)?,
            ));
        }
        if self.add.is_some() {
            let entry = AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: self.topic.ok_or("--topic is required with --add")?,
                pattern_type: PatternType::Literal,
                principal: principal.ok_or("--allow-principal or --deny-principal is required")?,
                host: self.host,
                operation: self.operation.ok_or("--operation is required")?.into(),
                permission_type: permission,
            };
            let outcomes = client.create_acls(&[entry]).await.map_err(error)?;
            let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
            return Ok(CommandResult::rows(
                vec![
                    if failed {
                        "ACL creation failed"
                    } else {
                        "ACL created"
                    }
                    .into(),
                ],
                outcomes
                    .iter()
                    .map(|outcome| json!({"error": kafka_error(outcome.error.as_ref())}))
                    .collect::<Vec<_>>(),
                failed,
            ));
        }
        let outcomes = client.delete_acls(&[filter]).await.map_err(error)?;
        let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
        let entries = outcomes
            .into_iter()
            .flat_map(|outcome| outcome.matched)
            .collect::<Vec<_>>();
        let mut result = acl_entries(&entries);
        result.failed = failed;
        Ok(result)
    }
}

fn acl_entries(entries: &[AclEntry]) -> CommandResult {
    let values = entries
        .iter()
        .map(|entry| {
            json!({"resource_type": format!("{:?}", entry.resource_type), "resource_name": entry.resource_name, "pattern_type": format!("{:?}", entry.pattern_type), "principal": entry.principal, "host": entry.host, "operation": format!("{:?}", entry.operation), "permission": format!("{:?}", entry.permission_type)})
        })
        .collect::<Vec<_>>();
    let human = entries
        .iter()
        .map(|entry| {
            format!(
                "{:?}\t{}\t{}\t{:?}\t{:?}",
                entry.resource_type,
                entry.resource_name,
                entry.principal,
                entry.operation,
                entry.permission_type
            )
        })
        .collect();
    CommandResult::success(human, values)
}

#[derive(Debug, Args)]
pub struct ConsumerGroupsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    describe: bool,
    #[arg(long)]
    reset_offsets: bool,
    #[arg(long)]
    group: String,
}

impl ConsumerGroupsArgs {
    pub async fn run(self) -> Result<CommandResult, String> {
        if self.reset_offsets {
            return Err(
                "offset reset needs the pending krabka-client-rs AlterConsumerGroupOffsets wrapper"
                    .into(),
            );
        }
        if !self.describe {
            return Err("--describe or --reset-offsets is required".into());
        }
        let mut client = self
            .connection
            .connect("consumer-groups")
            .await
            .map_err(error)?;
        let offsets = client
            .list_consumer_group_offsets(&self.group)
            .await
            .map_err(error)?;
        let values = offsets
            .iter()
            .map(|((topic, partition), offset)| {
                json!({"group": self.group, "topic": topic, "partition": partition, "offset": offset})
            })
            .collect::<Vec<_>>();
        let human = values
            .iter()
            .map(|value| {
                format!(
                    "{}\t{}\t{}\t{}",
                    self.group, value["topic"], value["partition"], value["offset"]
                )
            })
            .collect();
        Ok(CommandResult::success(human, values))
    }
}

#[derive(Debug, Args)]
pub struct FeaturesArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    describe: bool,
    #[arg(long)]
    upgrade: bool,
    #[arg(long)]
    downgrade: bool,
    #[arg(long, value_parser = feature_level)]
    feature: Vec<(String, i16)>,
}

impl FeaturesArgs {
    pub async fn run(self) -> Result<CommandResult, String> {
        if self.describe {
            return Err(
                "feature inspection needs the pending krabka-client-rs DescribeFeatures wrapper"
                    .into(),
            );
        }
        if self.upgrade == self.downgrade || self.feature.len() != 1 {
            return Err("choose --upgrade or --downgrade with one --feature name=level".into());
        }
        let (name, level) = &self.feature[0];
        if name != "metadata.version" {
            return Err("the pinned admin client currently updates only metadata.version".into());
        }
        let mut client = self.connection.connect("features").await.map_err(error)?;
        let update = client
            .update_metadata_version(*level, self.downgrade, self.connection.timeout)
            .await
            .map_err(error)?;
        Ok(CommandResult::success(
            vec![format!("metadata.version was updated to {}.", update.level)],
            json!({"feature": "metadata.version", "level": update.level}),
        ))
    }
}

#[derive(Debug, Args)]
pub struct ReassignPartitionsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    execute: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    replication_factor: i32,
    #[arg(long)]
    yes: bool,
}

impl ReassignPartitionsArgs {
    pub async fn run(self) -> Result<CommandResult, String> {
        if self.execute == self.verify {
            return Err("choose --execute or --verify".into());
        }
        if self.execute && !self.yes {
            return Err("reassignment execution requires --yes".into());
        }
        let mut client = self
            .connection
            .connect("reassign-partitions")
            .await
            .map_err(error)?;
        let status = client
            .reconcile_topic_replication_factor(
                &self.topic,
                self.replication_factor,
                self.connection.timeout,
            )
            .await
            .map_err(error)?;
        let status = format!("{status:?}");
        let incomplete = self.verify && status != "InSync";
        Ok(CommandResult::rows(
            vec![format!("{}: {status}", self.topic)],
            json!({"topic": self.topic, "status": status}),
            incomplete,
        ))
    }
}

fn key_value(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .ok_or_else(|| "expected key=value".into())
}

fn feature_level(value: &str) -> Result<(String, i16), String> {
    let (name, value) = key_value(value)?;
    let level = value
        .parse()
        .map_err(|_| "feature level must be an integer".to_string())?;
    Ok((name, level))
}

fn kafka_error(error: Option<&krabka_client_admin::KafkaError>) -> Value {
    error.map_or(
        Value::Null,
        |error| json!({"code": error.code, "name": error.name, "message": error.message}),
    )
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn key_value_parser_preserves_equals_in_value() {
        assert!(key_value("a=b=c").unwrap() == ("a".into(), "b=c".into()));
        assert!(key_value("missing").is_err());
    }
}
