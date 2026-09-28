//! `krabka acls`, the counterpart of `kafka-acls`.

use clap::{ArgGroup, Args, ValueEnum};
use krabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, DeleteAclFilterOutcome, PatternType, PermissionType,
    ResourceType,
};
use serde_json::json;

use crate::{
    common::{error, kafka_error},
    connection::ConnectionArgs,
    output::CommandResult,
};

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
#[command(
    group(ArgGroup::new("action").required(true).multiple(false).args(["list", "add", "remove"])),
    group(ArgGroup::new("principal").multiple(false).args(["allow_principal", "deny_principal"]))
)]
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
    #[arg(long)]
    host: Option<String>,
    #[arg(long)]
    yes: bool,
}

impl AclsArgs {
    pub async fn run(self) -> Result<CommandResult, String> {
        if self.remove.is_some() && !self.yes {
            return Err("ACL removal requires --yes".into());
        }
        if self.remove.is_some()
            && self.topic.is_none()
            && self.allow_principal.is_none()
            && self.deny_principal.is_none()
            && self.operation.is_none()
            && self.host.is_none()
        {
            return Err("ACL removal requires at least one scope filter".into());
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
        let filter = acl_filter(
            self.topic.as_deref(),
            principal.as_deref(),
            self.host.as_deref(),
            self.operation,
            permission,
        );
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
                host: self.host.unwrap_or_else(|| "*".into()),
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
        Ok(acl_delete_outcomes(&outcomes))
    }
}

fn acl_filter(
    topic: Option<&str>,
    principal: Option<&str>,
    host: Option<&str>,
    operation: Option<AclOperationArg>,
    permission: PermissionType,
) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: topic.map(|_| ResourceType::Topic),
        resource_name: topic.map(str::to_owned),
        pattern_type: topic.map(|_| PatternType::Literal),
        principal: principal.map(str::to_owned),
        host: host.map(str::to_owned),
        operation: operation.map(Into::into),
        permission_type: principal.map(|_| permission),
    }
}

fn acl_delete_outcomes(outcomes: &[DeleteAclFilterOutcome]) -> CommandResult {
    let entries = outcomes
        .iter()
        .flat_map(|outcome| &outcome.matched)
        .cloned()
        .collect::<Vec<_>>();
    let mut result = acl_entries(&entries);
    for error in outcomes.iter().filter_map(|outcome| outcome.error.as_ref()) {
        result
            .human
            .push(format!("ERROR\t{} ({})", error.name, error.code));
        result
            .data
            .as_array_mut()
            .expect("ACL entries serialize as an array")
            .push(json!({"error": kafka_error(Some(error))}));
    }
    result.failed = outcomes.iter().any(|outcome| outcome.error.is_some());
    result
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
                "{:?}\t{}\t{:?}\t{}\t{}\t{:?}\t{:?}",
                entry.resource_type,
                entry.resource_name,
                entry.pattern_type,
                entry.principal,
                entry.host,
                entry.operation,
                entry.permission_type
            )
        })
        .collect();
    CommandResult::success(human, values)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_client_admin::KafkaError;
    use serde_json::json;

    use super::*;

    #[test]
    fn acl_human_output_distinguishes_host_and_pattern() {
        let result = acl_entries(&[AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Prefixed,
            principal: "User:alice".into(),
            host: "10.0.0.1".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }]);
        assert!(result.human[0].contains("Prefixed"));
        assert!(result.human[0].contains("10.0.0.1"));
    }

    #[test]
    fn explicit_wildcard_acl_host_remains_exact() {
        let explicit = acl_filter(Some("orders"), None, Some("*"), None, PermissionType::Allow);
        let omitted = acl_filter(Some("orders"), None, None, None, PermissionType::Allow);
        assert!(explicit.host == Some("*".into()));
        assert!(omitted.host.is_none());
    }

    #[test]
    fn acl_delete_error_is_rendered() {
        let result = acl_delete_outcomes(&[DeleteAclFilterOutcome {
            error: Some(KafkaError {
                code: 31,
                name: "CLUSTER_AUTHORIZATION_FAILED",
                message: Some("denied".into()),
            }),
            matched: Vec::new(),
        }]);
        assert!(result.failed);
        assert!(result.human == ["ERROR\tCLUSTER_AUTHORIZATION_FAILED (31)"]);
        assert!(
            result.data
                == json!([{"error": {"code": 31, "name": "CLUSTER_AUTHORIZATION_FAILED", "message": "denied"}}])
        );
    }
}
