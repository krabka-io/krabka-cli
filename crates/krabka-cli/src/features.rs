//! `krabka features`, the counterpart of `kafka-features`.
//!
//! The command line is `FeatureCommand.java`'s at Kafka 4.3.1: the connection
//! flags come before the subcommand, and the subcommands are `describe`,
//! `upgrade`, `downgrade`, `disable`, `version-mapping` and
//! `feature-dependencies`, with the same flags, the same stdout lines and the
//! same error messages.
//!
//! `--dry-run` sends the same `UpdateFeatures` request with `validateOnly`
//! set, and the controller's answer for each feature decides its "can be" or
//! "Can not" line, as in `kafka-features`. `--unsafe` sends
//! `UNSAFE_DOWNGRADE`, and `describe --node-id` asks that one node.
//!
//! krabka adds one thing: it rejects an update that the local feature
//! registry already refuses before it sends the request. That covers an
//! unknown feature name and a level outside the feature's supported range
//! and, except in a dry run, a KIP-1022 dependency that the proposed levels
//! do not meet. A dry run leaves the dependency check to the controller, whose
//! validate-only answer is the report.

use std::collections::BTreeMap;

use clap::{Args, Subcommand};
use krabka_client_admin::{
    AdminError, DescribeFeaturesOptions, FeatureMetadata, FeatureUpdate, KafkaError,
    UpdateFeaturesOptions, UpdateFeaturesResults, UpgradeType,
};
use krabka_metadata::{
    feature, feature_registry,
    metadata_version::{KRAFT_VERSION_FEATURE, METADATA_VERSION_FEATURE},
};
use serde_json::{Value, json};

use crate::{
    compat::KafkaException,
    connection::ConnectionArgs,
    feature_catalog::{
        Dialect, FeatureDependenciesArgs, VersionMappingArgs, default_level, level_to_string,
        parse_name_and_level, production_features, resolve_release,
    },
    output::{CommandError, CommandResult},
};

/// The printed notice of `kafka-features upgrade --metadata`, leading space
/// included.
const METADATA_FLAG_NOTICE: &str =
    " `metadata` flag is deprecated and may be removed in a future release.";

#[derive(Debug, Args)]
pub struct FeaturesArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[command(subcommand)]
    command: FeaturesCommand,
}

#[derive(Debug, Subcommand)]
enum FeaturesCommand {
    /// Describes the current active feature flags.
    Describe(DescribeArgs),
    /// Upgrade one or more feature flags.
    Upgrade(UpgradeArgs),
    /// Downgrade one or more feature flags.
    Downgrade(DowngradeArgs),
    /// Disable one or more feature flags. This is the same as downgrading the
    /// version to zero.
    Disable(DisableArgs),
    /// Look up the corresponding features for a given metadata version. With
    /// no --release-version, print the mapping of the latest metadata version.
    VersionMapping(VersionMappingArgs),
    /// Look up dependencies for a given feature version. An unknown feature or
    /// an undefined version is an error. --feature can repeat.
    FeatureDependencies(FeatureDependenciesArgs),
}

#[derive(Debug, Args)]
struct DescribeArgs {
    /// The node id to which the requests should be sent. If not specified,
    /// the requests will be sent to an arbitrary controller/broker.
    #[arg(long, allow_hyphen_values = true)]
    node_id: Option<i32>,
}

#[derive(Debug, Args)]
struct UpgradeArgs {
    /// DEPRECATED -- The level to which we should upgrade the metadata. For
    /// example, 3.3-IV3.
    #[arg(long)]
    metadata: Option<String>,
    /// The release version to update all features to. For example, 3.9-IV0
    /// will set metadata.version=21 and kraft.version=1. Use the
    /// version-mapping command to learn which features will be set for any
    /// given version.
    #[arg(long)]
    release_version: Option<String>,
    /// A feature upgrade we should perform, in feature=level format. For
    /// example: `metadata.version=5`.
    #[arg(long, allow_hyphen_values = true)]
    feature: Vec<String>,
    /// Validate this upgrade, but do not perform it.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct DowngradeArgs {
    /// DEPRECATED -- The level to which we should downgrade the metadata. For
    /// example, 3.3-IV0.
    #[arg(long)]
    metadata: Option<String>,
    /// The release version to downgrade all features to. For example, 3.9-IV0
    /// will set metadata.version=21 and kraft.version=1. Use the
    /// version-mapping command to learn which features will be set for any
    /// given version.
    #[arg(long)]
    release_version: Option<String>,
    /// A feature downgrade we should perform, in feature=level format. For
    /// example: `metadata.version=5`.
    #[arg(long, allow_hyphen_values = true)]
    feature: Vec<String>,
    /// Perform this downgrade even if it may irreversibly destroy metadata.
    #[arg(long = "unsafe")]
    unsafe_downgrade: bool,
    /// Validate this downgrade, but do not perform it.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct DisableArgs {
    /// A feature flag to disable.
    #[arg(long)]
    feature: Vec<String>,
    /// Disable this feature flag even if it may irreversibly destroy
    /// metadata.
    #[arg(long = "unsafe")]
    unsafe_downgrade: bool,
    /// Perform a dry-run of this disable operation.
    #[arg(long)]
    dry_run: bool,
}

/// `FeatureCommand.downgradeType`: `UNSAFE_DOWNGRADE` with `--unsafe`, else
/// `SAFE_DOWNGRADE`.
const fn downgrade_type(unsafe_downgrade: bool) -> UpgradeType {
    if unsafe_downgrade {
        UpgradeType::UnsafeDowngrade
    } else {
        UpgradeType::SafeDowngrade
    }
}

/// The name of the `FeatureUpdate.UpgradeType` constant.
const fn upgrade_type_name(upgrade_type: UpgradeType) -> &'static str {
    match upgrade_type {
        UpgradeType::Upgrade => "UPGRADE",
        UpgradeType::SafeDowngrade => "SAFE_DOWNGRADE",
        UpgradeType::UnsafeDowngrade => "UNSAFE_DOWNGRADE",
    }
}

/// The subcommand that asked for the updates, which decides the wording of
/// the report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Upgrade,
    Downgrade,
    Disable,
}

impl Op {
    const fn verb(self) -> &'static str {
        match self {
            Self::Upgrade => "upgrade",
            Self::Downgrade => "downgrade",
            Self::Disable => "disable",
        }
    }
}

/// What an `upgrade`, `downgrade` or `disable` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    op: Op,
    /// Lines printed before the report, as the deprecated `--metadata` flag
    /// prints its notice.
    notices: Vec<String>,
    /// The updates, by feature name.
    updates: BTreeMap<String, FeatureUpdate>,
    dry_run: bool,
}

/// Why one feature update failed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowError {
    code: i16,
    name: &'static str,
    message: String,
}

impl FeaturesArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let plan = match self.command {
            FeaturesCommand::VersionMapping(args) => return args.run(),
            FeaturesCommand::FeatureDependencies(args) => return args.run(Dialect::Features),
            FeaturesCommand::Describe(args) => return describe(&self.connection, &args).await,
            FeaturesCommand::Upgrade(args) => Plan::upgrade_or_downgrade(
                Op::Upgrade,
                args.metadata.as_deref(),
                args.release_version.as_deref(),
                &args.feature,
                UpgradeType::Upgrade,
                args.dry_run,
            )?,
            FeaturesCommand::Downgrade(args) => Plan::upgrade_or_downgrade(
                Op::Downgrade,
                args.metadata.as_deref(),
                args.release_version.as_deref(),
                &args.feature,
                downgrade_type(args.unsafe_downgrade),
                args.dry_run,
            )?,
            FeaturesCommand::Disable(args) => Plan::disable(
                &args.feature,
                downgrade_type(args.unsafe_downgrade),
                args.dry_run,
            )?,
        };
        plan.validate()?;
        let mut client = self.connection.connect("features").await?;
        if !plan.dry_run && plan.needs_current_levels() {
            let metadata = client
                .describe_features(DescribeFeaturesOptions {
                    timeout: Some(self.connection.timeout),
                    node_id: None,
                })
                .await?;
            let proposed = proposed_levels(&plan.updates, &metadata);
            if let Some((name, level, message)) =
                unmet_dependency(&plan.updates, &proposed, registry_dependencies)
            {
                return Err(format!(
                    "Invalid update version {level} for feature {name}. {message}"
                )
                .into());
            }
        }
        let outcome = client
            .update_features(
                &plan.updates,
                UpdateFeaturesOptions {
                    timeout: Some(self.connection.timeout),
                    validate_only: plan.dry_run,
                },
            )
            .await;
        let errors = outcome_errors(&plan.updates, outcome)?;
        let report = plan.report(|name| errors.get(name).cloned().flatten());
        Ok(if plan.dry_run {
            report.into_kafka_dry_run()
        } else {
            report
        })
    }
}

impl Plan {
    /// `handleUpgradeOrDowngrade`.
    fn upgrade_or_downgrade(
        op: Op,
        metadata: Option<&str>,
        release_version: Option<&str>,
        features: &[String],
        upgrade_type: UpgradeType,
        dry_run: bool,
    ) -> Result<Self, String> {
        if release_version.is_some() && (metadata.is_some() || !features.is_empty()) {
            return Err("Can not specify `release-version` with other feature flags.".into());
        }
        let mut notices = Vec::new();
        let mut updates = BTreeMap::new();
        if let Some(release) = release_version {
            let version = resolve_release(release)?;
            let level = version.feature_level();
            updates.insert(
                METADATA_VERSION_FEATURE.to_owned(),
                new_update(level, upgrade_type)?,
            );
            for feature in production_features() {
                let default = default_level(feature, level);
                // Kafka does not send an upgrade of a feature to level 0.
                if upgrade_type != UpgradeType::Upgrade || default > 0 {
                    updates.insert(
                        feature.name().to_owned(),
                        new_update(default, upgrade_type)?,
                    );
                }
            }
        } else {
            if let Some(metadata) = metadata {
                notices.push(METADATA_FLAG_NOTICE.to_owned());
                let version = resolve_release(metadata)?;
                updates.insert(
                    METADATA_VERSION_FEATURE.to_owned(),
                    new_update(version.feature_level(), upgrade_type)?,
                );
            }
            for spec in features {
                let (name, level) = parse_name_and_level(spec)?;
                let update = new_update(level, upgrade_type)?;
                if updates.insert(name.clone(), update).is_some() {
                    return Err(format!("Feature {name} was specified more than once."));
                }
            }
        }
        Ok(Self {
            op,
            notices,
            updates,
            dry_run,
        })
    }

    /// `handleDisable`.
    fn disable(
        features: &[String],
        upgrade_type: UpgradeType,
        dry_run: bool,
    ) -> Result<Self, String> {
        let mut updates = BTreeMap::new();
        for name in features {
            if updates
                .insert(name.clone(), new_update(0, upgrade_type)?)
                .is_some()
            {
                return Err(format!("Feature {name} was specified more than once."));
            }
        }
        Ok(Self {
            op: Op::Disable,
            notices: Vec::new(),
            updates,
            dry_run,
        })
    }

    /// The checks of `FeatureCommand.update` and of `Admin.updateFeatures`,
    /// then krabka's registry checks, all before any request.
    fn validate(&self) -> Result<(), String> {
        if self.updates.is_empty() {
            return Err(format!(
                "You must specify at least one feature to {}",
                self.op.verb()
            ));
        }
        if self.updates.keys().any(|name| name.trim().is_empty()) {
            return Err("Provided feature can not be empty.".into());
        }
        for (name, update) in &self.updates {
            let Some(registered) = feature(name) else {
                let mut known = feature_registry()
                    .iter()
                    .map(|feature| feature.name())
                    .collect::<Vec<_>>();
                known.sort_unstable();
                return Err(format!(
                    "Unsupported feature: {name}. Supported features are: {}",
                    known.join(", ")
                ));
            };
            let (min, max) = registered.supported_range();
            if !(min..=max).contains(&update.max_version_level()) {
                return Err(format!(
                    "feature {name}={} is outside the supported range {min}..={max}",
                    update.max_version_level()
                ));
            }
        }
        Ok(())
    }

    /// Whether an update declares a KIP-1022 dependency, which needs the
    /// cluster's current levels to check.
    fn needs_current_levels(&self) -> bool {
        self.updates.iter().any(|(name, update)| {
            !registry_dependencies(name, update.max_version_level()).is_empty()
        })
    }

    /// The report of `FeatureCommand.update`, one line per feature in name
    /// order. `error` gives the failure of each feature.
    fn report(&self, error: impl Fn(&str) -> Option<RowError>) -> CommandResult {
        let rows = self
            .updates
            .iter()
            .map(|(name, update)| (name, update, error(name)))
            .collect::<Vec<_>>();
        let failures = rows.iter().filter(|(_, _, error)| error.is_some()).count();
        let human = self
            .notices
            .iter()
            .cloned()
            .chain(rows.iter().map(|(name, update, error)| {
                self.line(name, update.max_version_level(), error.as_ref())
            }))
            .collect();
        let data = json!({
            "operation": self.op.verb(),
            "updates": rows
                .iter()
                .map(|(name, update, error)| json!({
                    "feature": name,
                    "level": update.max_version_level(),
                    "upgrade_type": upgrade_type_name(update.upgrade_type()),
                    "error": error.as_ref().map_or(Value::Null, |error| json!({
                        "code": error.code,
                        "name": error.name,
                        "message": error.message,
                    })),
                }))
                .collect::<Vec<_>>(),
            "failures": failures,
        });
        let notices = if failures > 0 {
            vec![format!(
                "{failures} out of {} operation(s) failed.",
                self.updates.len()
            )]
        } else {
            Vec::new()
        };
        CommandResult::rows(human, data, failures > 0).with_notices(notices)
    }

    fn line(&self, name: &str, level: i16, error: Option<&RowError>) -> String {
        if let Some(error) = error {
            let helper = if self.dry_run {
                "Can not "
            } else {
                "Could not "
            };
            let suffix = match self.op {
                Op::Disable => format!("disable {name}"),
                op => format!("{} {name} to {level}", op.verb()),
            };
            format!("{helper}{suffix}. {}", error.message)
        } else {
            let verb = if self.dry_run { " can be " } else { " was " };
            let object = match self.op {
                Op::Disable => "disabled.".to_owned(),
                op => format!("{}d to {level}.", op.verb()),
            };
            format!("{name}{verb}{object}")
        }
    }
}

/// `new FeatureUpdate(level, upgradeType)`, with the message of the
/// `IllegalArgumentException` it throws for level 0 with `UPGRADE` or a
/// negative level.
fn new_update(level: i16, upgrade_type: UpgradeType) -> Result<FeatureUpdate, String> {
    FeatureUpdate::new(level, upgrade_type).map_err(|error| match error {
        AdminError::InvalidArgument(message) => message,
        other => other.to_string(),
    })
}

fn registry_dependencies(name: &str, level: i16) -> &'static [(&'static str, i16)] {
    if name == METADATA_VERSION_FEATURE || name == KRAFT_VERSION_FEATURE {
        return &[];
    }
    feature(name).map_or(&[][..], |feature| feature.dependencies(level))
}

/// The levels the controller validates against: the finalized levels, with
/// the requested updates applied.
fn proposed_levels(
    updates: &BTreeMap<String, FeatureUpdate>,
    metadata: &FeatureMetadata,
) -> BTreeMap<String, i16> {
    metadata
        .finalized
        .iter()
        .map(|range| (range.name.clone(), range.max_version))
        .chain(
            updates
                .iter()
                .map(|(name, update)| (name.clone(), update.max_version_level())),
        )
        .collect()
}

/// `Feature.validateVersion` for every update other than `metadata.version`
/// and `kraft.version`: the first update whose dependency `proposed` does not
/// meet, with the controller's message.
fn unmet_dependency(
    updates: &BTreeMap<String, FeatureUpdate>,
    proposed: &BTreeMap<String, i16>,
    dependencies: impl Fn(&str, i16) -> &'static [(&'static str, i16)],
) -> Option<(String, i16, String)> {
    updates.iter().find_map(|(name, update)| {
        dependencies(name, update.max_version_level())
            .iter()
            .find(|(dependency, min)| proposed.get(*dependency).is_none_or(|level| level < min))
            .map(|(dependency, min)| {
                (
                    name.clone(),
                    update.max_version_level(),
                    format!(
                        "{name} could not be set to {} because it depends on {dependency} level {min}",
                        update.max_version_level()
                    ),
                )
            })
    })
}

/// The failure of each requested feature, as `FeatureCommand.update` reads
/// each future of `UpdateFeaturesResult.values`.
fn outcome_errors(
    updates: &BTreeMap<String, FeatureUpdate>,
    outcome: Result<UpdateFeaturesResults, AdminError>,
) -> Result<BTreeMap<String, Option<RowError>>, CommandError> {
    match outcome {
        Ok(results) => Ok(updates
            .keys()
            .map(|name| {
                let error = match results.get(name) {
                    Some(Ok(())) => None,
                    Some(Err(error)) => Some(RowError::from_kafka(error)),
                    None => Some(RowError::from_kafka(&KafkaError {
                        code: -1,
                        name: "UNKNOWN_SERVER_ERROR",
                        message: Some(format!(
                            "The controller response did not contain a result for feature {name}"
                        )),
                    })),
                };
                (name.clone(), error)
            })
            .collect()),
        // A call that fails as a whole, as at its deadline, fails every
        // feature's future with the same error.
        Err(AdminError::Broker {
            api: "UpdateFeatures",
            code,
            name,
            message,
        }) => {
            let error = RowError::from_kafka(&KafkaError {
                code,
                name,
                message,
            });
            Ok(updates
                .keys()
                .map(|feature| (feature.clone(), Some(error.clone())))
                .collect())
        }
        Err(other) => Err(other.into()),
    }
}

impl RowError {
    /// The error of one feature. Its message is the one the controller sent,
    /// or else the default message of Kafka's exception for the code, as
    /// `Errors.exception(null)` gives it.
    fn from_kafka(error: &KafkaError) -> Self {
        let exception = KafkaException::for_code(error.code);
        let known = KafkaException::is_known(error.code);
        Self {
            code: error.code,
            name: if known { exception.name() } else { error.name },
            message: error
                .message
                .clone()
                .unwrap_or_else(|| exception.message().to_owned()),
        }
    }
}

/// `handleDescribe`.
async fn describe(
    connection: &ConnectionArgs,
    args: &DescribeArgs,
) -> Result<CommandResult, CommandError> {
    if let Some(node_id) = args.node_id.filter(|node_id| *node_id < 0) {
        return Err(format!("Invalid node id {node_id}: must be non-negative.").into());
    }
    let client = connection.connect("features").await?;
    let metadata = client
        .describe_features(DescribeFeaturesOptions {
            timeout: Some(connection.timeout),
            node_id: args.node_id,
        })
        .await?;
    Ok(render_describe(&metadata))
}

/// One line per supported feature, in name order, in the `printf` layout of
/// `handleDescribe`.
fn render_describe(metadata: &FeatureMetadata) -> CommandResult {
    let mut supported = metadata.supported.iter().collect::<Vec<_>>();
    supported.sort_by(|a, b| a.name.cmp(&b.name));
    let epoch = metadata.finalized_features_epoch;
    let rows = supported
        .iter()
        .map(|range| {
            let finalized = metadata
                .finalized
                .iter()
                .find(|finalized| finalized.name == range.name)
                .map_or(0, |finalized| finalized.max_version);
            (range, finalized)
        })
        .collect::<Vec<_>>();
    let human = rows
        .iter()
        .map(|(range, finalized)| {
            format!(
                "Feature: {:<40}  SupportedMinVersion: {:<15}  SupportedMaxVersion: {:<15}  FinalizedVersionLevel: {:<15}  Epoch: {}",
                range.name,
                level_to_string(&range.name, range.min_version),
                level_to_string(&range.name, range.max_version),
                level_to_string(&range.name, *finalized),
                epoch.map_or_else(|| "-".to_owned(), |epoch| epoch.to_string()),
            )
        })
        .collect();
    let data = json!({
        "features": rows
            .iter()
            .map(|(range, finalized)| json!({
                "feature": range.name,
                "supported_min_version": range.min_version,
                "supported_max_version": range.max_version,
                "finalized_version_level": finalized,
            }))
            .collect::<Vec<_>>(),
        "finalized_features_epoch": epoch,
    });
    CommandResult::success(human, data)
}

#[cfg(test)]
mod tests;
