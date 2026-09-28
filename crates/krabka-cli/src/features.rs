//! `krabka features`, the counterpart of `kafka-features`.

use clap::{ArgGroup, Args};
use krabka_client_admin::FeatureUpdate;
use serde_json::json;

use crate::{
    common::key_value,
    connection::ConnectionArgs,
    output::{CommandError, CommandResult, kafka_error},
};

#[derive(Debug, Args)]
#[command(
    group(ArgGroup::new("action").required(true).multiple(false).args(["describe", "upgrade", "downgrade"])),
    group(ArgGroup::new("update").multiple(false).args(["upgrade", "downgrade"]))
)]
pub struct FeaturesArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[arg(long)]
    describe: bool,
    #[arg(long)]
    upgrade: bool,
    #[arg(long)]
    downgrade: bool,
    #[arg(long, value_parser = feature_level, requires = "update")]
    feature: Vec<(String, i16)>,
}

impl FeaturesArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        if self.describe {
            let mut client = self.connection.connect("features").await?;
            let metadata = client.describe_features().await?;
            let human = metadata
                .supported
                .iter()
                .map(|range| {
                    let finalized = metadata
                        .finalized
                        .iter()
                        .find(|value| value.name == range.name)
                        .map_or("-".into(), |value| {
                            format!("{}-{}", value.min_version, value.max_version)
                        });
                    format!(
                        "{}\t{}-{}\t{}",
                        range.name, range.min_version, range.max_version, finalized
                    )
                })
                .collect();
            return Ok(CommandResult::success(
                human,
                json!({"supported": metadata.supported.iter().map(|range| json!({"feature": range.name, "min": range.min_version, "max": range.max_version})).collect::<Vec<_>>(), "finalized": metadata.finalized.iter().map(|range| json!({"feature": range.name, "min": range.min_version, "max": range.max_version})).collect::<Vec<_>>(), "finalized_features_epoch": metadata.finalized_features_epoch}),
            ));
        }
        if self.feature.is_empty() {
            return Err(
                "choose --upgrade or --downgrade with one or more --feature name=level".into(),
            );
        }
        let updates = self
            .feature
            .iter()
            .map(|(name, level)| FeatureUpdate {
                name: name.clone(),
                max_version_level: *level,
                safe_downgrade: self.downgrade,
            })
            .collect::<Vec<_>>();
        let mut client = self.connection.connect("features").await?;
        let outcomes = client
            .update_features(&updates, self.connection.timeout)
            .await?;
        let failed = outcomes.iter().any(|outcome| outcome.error.is_some());
        let human = outcomes
            .iter()
            .map(|outcome| match &outcome.error {
                Some(error) => format!("{}\tERROR\t{} ({})", outcome.name, error.name, error.code),
                None => format!("Updated feature {}.", outcome.name),
            })
            .collect();
        let values = outcomes
            .iter()
            .map(|outcome| json!({"feature": outcome.name, "error": kafka_error(outcome.error.as_ref())}))
            .collect::<Vec<_>>();
        Ok(CommandResult::rows(human, values, failed))
    }
}

fn feature_level(value: &str) -> Result<(String, i16), String> {
    let (name, value) = key_value(value)?;
    let level = value
        .parse()
        .map_err(|_| "feature level must be an integer".to_string())?;
    Ok((name, level))
}
