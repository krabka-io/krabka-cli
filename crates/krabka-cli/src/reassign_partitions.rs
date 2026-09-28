//! `krabka reassign-partitions`, the counterpart of `kafka-reassign-partitions`.

use std::collections::BTreeMap;

use clap::Args;
use serde_json::json;

use crate::{common::error, connection::ConnectionArgs, output::CommandResult};

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
        let (status, incomplete) = if self.execute {
            let status = client
                .reconcile_topic_replication_factor(
                    &self.topic,
                    self.replication_factor,
                    self.connection.timeout,
                )
                .await
                .map_err(error)?;
            (format!("{status:?}"), false)
        } else {
            let assignments = client
                .describe_partition_assignments(&[&self.topic])
                .await
                .map_err(error)?;
            let partitions = assignments
                .iter()
                .map(|assignment| assignment.partition)
                .collect::<Vec<_>>();
            let active = client
                .list_partition_reassignments(
                    &BTreeMap::from([(self.topic.clone(), partitions)]),
                    self.connection.timeout,
                )
                .await
                .map_err(error)?;
            if !active.is_empty() {
                ("ReassignmentInProgress".into(), true)
            } else if !assignments.is_empty()
                && assignments.iter().all(|assignment| {
                    i32::try_from(assignment.replicas.len()) == Ok(self.replication_factor)
                })
            {
                ("InSync".into(), false)
            } else {
                ("ReplicationFactorMismatch".into(), true)
            }
        };
        Ok(CommandResult::rows(
            vec![format!("{}: {status}", self.topic)],
            json!({"topic": self.topic, "status": status}),
            incomplete,
        ))
    }
}
