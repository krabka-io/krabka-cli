//! `krabka features`, the counterpart of `kafka-features`.
//!
//! The command line is `FeatureCommand.java`'s at Kafka 4.3.1: the connection
//! flags come before the subcommand, and the subcommands are `describe`,
//! `upgrade`, `downgrade`, `disable`, `version-mapping` and
//! `feature-dependencies`, with the same flags, the same stdout lines and the
//! same error messages.
//!
//! krabka adds two things. First, it rejects an update that the local feature
//! registry already refuses before it sends the request: an unknown feature
//! name, a level outside the feature's supported range, and a KIP-1022
//! dependency that the proposed levels do not meet. Second, `--dry-run` sends
//! no `UpdateFeatures` request at all. `kafka-features` sends one with
//! `validateOnly` set, and `AdminClient::update_features` in the pinned
//! `krabka-client-admin` takes no options, so it cannot set that flag. The dry
//! run instead reads the cluster's supported and finalized levels and applies
//! the controller's checks to them. The report has the shape of Kafka's
//! dry-run report.
//!
//! Two `kafka-features` invocations fail with a "not supported by this build"
//! error that names the missing client call. `downgrade` or `disable` with
//! `--unsafe` fails, because the client's `FeatureUpdate` has only a
//! `safe_downgrade` flag and so cannot carry `UNSAFE_DOWNGRADE`. `describe
//! --node-id` fails, because `AdminClient::describe_features` takes no
//! `DescribeFeaturesOptions.nodeId`.

use std::collections::BTreeMap;

use clap::{Args, Subcommand};
use krabka_client_admin::{AdminError, FeatureMetadata, FeatureUpdate, FeatureUpdateOutcome};
use krabka_metadata::{
    feature, feature_registry,
    metadata_version::{KRAFT_VERSION_FEATURE, METADATA_VERSION_FEATURE},
};
use serde_json::{Value, json};

use crate::{
    connection::ConnectionArgs,
    feature_catalog::{
        Dialect, FeatureDependenciesArgs, VersionMappingArgs, level_to_string,
        parse_name_and_level, production_features, resolve_release,
    },
    output::{CommandError, CommandResult},
};

/// The refusal of `--unsafe`, which the pinned client cannot send.
const UNSAFE_UNSUPPORTED: &str = "--unsafe is not supported by this build: \
    krabka-client-admin's FeatureUpdate has no UNSAFE_DOWNGRADE upgrade type, only \
    safe_downgrade, so AdminClient::update_features cannot send an unsafe downgrade";

/// The refusal of `describe --node-id`, which the pinned client cannot send.
const NODE_ID_UNSUPPORTED: &str = "describe --node-id is not supported by this build: \
    AdminClient::describe_features in krabka-client-admin takes no node id \
    (Kafka's DescribeFeaturesOptions.nodeId), so it cannot send DescribeFeatures to one node";

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

/// `FeatureUpdate.UpgradeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpgradeType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

impl UpgradeType {
    const fn downgrade(unsafe_downgrade: bool) -> Self {
        if unsafe_downgrade {
            Self::UnsafeDowngrade
        } else {
            Self::SafeDowngrade
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Upgrade => "UPGRADE",
            Self::SafeDowngrade => "SAFE_DOWNGRADE",
            Self::UnsafeDowngrade => "UNSAFE_DOWNGRADE",
        }
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

/// One requested update: `FeatureUpdate(maxVersionLevel, upgradeType)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Update {
    level: i16,
    upgrade_type: UpgradeType,
}

/// What an `upgrade`, `downgrade` or `disable` asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    op: Op,
    /// Lines printed before the report, as the deprecated `--metadata` flag
    /// prints its notice.
    notices: Vec<String>,
    /// The updates, by feature name.
    updates: BTreeMap<String, Update>,
    dry_run: bool,
}

/// Why one feature update failed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowError {
    code: Option<i16>,
    name: Option<&'static str>,
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
                UpgradeType::downgrade(args.unsafe_downgrade),
                args.dry_run,
            )?,
            FeaturesCommand::Disable(args) => Plan::disable(
                &args.feature,
                UpgradeType::downgrade(args.unsafe_downgrade),
                args.dry_run,
            )?,
        };
        plan.validate()?;
        if !plan.dry_run
            && plan
                .updates
                .values()
                .any(|update| update.upgrade_type == UpgradeType::UnsafeDowngrade)
        {
            return Err(UNSAFE_UNSUPPORTED.into());
        }
        let mut client = self.connection.connect("features").await?;
        if plan.dry_run {
            let metadata = client.describe_features().await?;
            let error = predict_failure(&plan.updates, &metadata, registry_dependencies);
            return Ok(plan
                .report(|_| {
                    error.clone().map(|message| RowError {
                        code: None,
                        name: None,
                        message,
                    })
                })
                .into_dry_run());
        }
        if plan.needs_current_levels() {
            let metadata = client.describe_features().await?;
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
        let requests = plan
            .updates
            .iter()
            .map(|(name, update)| FeatureUpdate {
                name: name.clone(),
                max_version_level: update.level,
                safe_downgrade: update.upgrade_type != UpgradeType::Upgrade,
            })
            .collect::<Vec<_>>();
        let outcome = client
            .update_features(&requests, self.connection.timeout)
            .await;
        let errors = outcome_errors(&plan.updates, outcome)?;
        Ok(plan.report(|name| errors.get(name).cloned().flatten()))
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
                let default = feature.default_level(level);
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
                .insert(
                    name.clone(),
                    Update {
                        level: 0,
                        upgrade_type,
                    },
                )
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
            if !(min..=max).contains(&update.level) {
                return Err(format!(
                    "feature {name}={} is outside the supported range {min}..={max}",
                    update.level
                ));
            }
        }
        Ok(())
    }

    /// Whether an update declares a KIP-1022 dependency, which needs the
    /// cluster's current levels to check.
    fn needs_current_levels(&self) -> bool {
        self.updates
            .iter()
            .any(|(name, update)| !registry_dependencies(name, update.level).is_empty())
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
            .chain(
                rows.iter()
                    .map(|(name, update, error)| self.line(name, update.level, error.as_ref())),
            )
            .collect();
        let data = json!({
            "operation": self.op.verb(),
            "updates": rows
                .iter()
                .map(|(name, update, error)| json!({
                    "feature": name,
                    "level": update.level,
                    "upgrade_type": update.upgrade_type.name(),
                    "error": error.as_ref().map_or(Value::Null, |error| json!({
                        "code": error.code,
                        "name": error.name,
                        "message": error.message,
                    })),
                }))
                .collect::<Vec<_>>(),
            "failures": failures,
        });
        CommandResult::rows(human, data, failures > 0)
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

/// `new FeatureUpdate(level, upgradeType)`, which refuses these two values.
fn new_update(level: i16, upgrade_type: UpgradeType) -> Result<Update, String> {
    if level == 0 && upgrade_type == UpgradeType::Upgrade {
        return Err(format!(
            "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:{level} is < 1."
        ));
    }
    if level < 0 {
        return Err("Cannot specify a negative version level.".into());
    }
    Ok(Update {
        level,
        upgrade_type,
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
    updates: &BTreeMap<String, Update>,
    metadata: &FeatureMetadata,
) -> BTreeMap<String, i16> {
    metadata
        .finalized
        .iter()
        .map(|range| (range.name.clone(), range.max_version))
        .chain(
            updates
                .iter()
                .map(|(name, update)| (name.clone(), update.level)),
        )
        .collect()
}

/// `Feature.validateVersion` for every update other than `metadata.version`
/// and `kraft.version`: the first update whose dependency `proposed` does not
/// meet, with the controller's message.
fn unmet_dependency(
    updates: &BTreeMap<String, Update>,
    proposed: &BTreeMap<String, i16>,
    dependencies: impl Fn(&str, i16) -> &'static [(&'static str, i16)],
) -> Option<(String, i16, String)> {
    updates.iter().find_map(|(name, update)| {
        dependencies(name, update.level)
            .iter()
            .find(|(dependency, min)| proposed.get(*dependency).is_none_or(|level| level < min))
            .map(|(dependency, min)| {
                (
                    name.clone(),
                    update.level,
                    format!(
                        "{name} could not be set to {} because it depends on {dependency} level {min}",
                        update.level
                    ),
                )
            })
    })
}

/// The controller's verdict on `updates` (`FeatureControlManager.updateFeature`),
/// predicted from the cluster's supported and finalized levels. The controller
/// fails the whole request on the first bad update, so the prediction is one
/// message for every feature, or none.
///
/// A `metadata.version` downgrade that would lose metadata is not predicted:
/// the controller decides that from a per-level table that krabka does not
/// have.
fn predict_failure(
    updates: &BTreeMap<String, Update>,
    metadata: &FeatureMetadata,
    dependencies: impl Fn(&str, i16) -> &'static [(&'static str, i16)],
) -> Option<String> {
    let proposed = proposed_levels(updates, metadata);
    let reason = |name: &str, update: &Update| -> Option<String> {
        let current = metadata
            .finalized
            .iter()
            .find(|range| range.name == name)
            .map_or(0, |range| range.max_version);
        let (min, max) = metadata
            .supported
            .iter()
            .find(|range| range.name == name)
            .map_or((0, 0), |range| (range.min_version, range.max_version));
        if !(min..=max).contains(&update.level) {
            return Some(if max == 0 {
                "Broker does not support this feature.".to_owned()
            } else if min == max {
                format!("Broker only supports versions {min}")
            } else {
                format!("Broker only supports versions {min}-{max}")
            });
        }
        if update.level < current && update.upgrade_type == UpgradeType::Upgrade {
            return Some(
                "Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade."
                    .to_owned(),
            );
        }
        if update.level > current && update.upgrade_type != UpgradeType::Upgrade {
            return Some("Can't downgrade to a newer version.".to_owned());
        }
        if name == KRAFT_VERSION_FEATURE
            && update.upgrade_type != UpgradeType::Upgrade
            && update.level != current
        {
            return Some("Can't downgrade the version of this feature.".to_owned());
        }
        None
    };
    updates
        .iter()
        .find_map(|(name, update)| {
            reason(name, update).map(|message| (name.clone(), update.level, message))
        })
        .or_else(|| unmet_dependency(updates, &proposed, dependencies))
        .map(|(name, level, message)| {
            format!(
                "The update failed for all features since the following feature had an error: Invalid update version {level} for feature {name}. {message}"
            )
        })
}

/// The failure of each requested feature, as `KafkaAdminClient.updateFeatures`
/// completes each feature's future from the response.
fn outcome_errors(
    updates: &BTreeMap<String, Update>,
    outcome: Result<Vec<FeatureUpdateOutcome>, AdminError>,
) -> Result<BTreeMap<String, Option<RowError>>, CommandError> {
    match outcome {
        // UpdateFeatures v2 answers a success with no per-feature results.
        Ok(outcomes) if outcomes.is_empty() => {
            Ok(updates.keys().map(|name| (name.clone(), None)).collect())
        }
        Ok(outcomes) => Ok(updates
            .keys()
            .map(|name| {
                let error = match outcomes.iter().find(|outcome| &outcome.name == name) {
                    Some(outcome) => outcome.error.as_ref().map(|error| RowError {
                        code: Some(error.code),
                        name: Some(error.name),
                        message: error
                            .message
                            .clone()
                            .unwrap_or_else(|| error.name.to_owned()),
                    }),
                    None => Some(RowError {
                        code: None,
                        name: None,
                        message: format!(
                            "The controller response did not contain a result for feature {name}"
                        ),
                    }),
                };
                (name.clone(), error)
            })
            .collect()),
        // A top-level error fails every feature with the same message.
        Err(AdminError::Broker {
            api: "UpdateFeatures",
            code,
            name,
            message,
        }) => {
            let error = RowError {
                code: Some(code),
                name: Some(name),
                message: message.unwrap_or_else(|| name.to_owned()),
            };
            Ok(updates
                .keys()
                .map(|feature| (feature.clone(), Some(error.clone())))
                .collect())
        }
        Err(other) => Err(other.into()),
    }
}

/// `handleDescribe`.
async fn describe(
    connection: &ConnectionArgs,
    args: &DescribeArgs,
) -> Result<CommandResult, CommandError> {
    if let Some(node_id) = args.node_id {
        if node_id < 0 {
            return Err(format!("Invalid node id {node_id}: must be non-negative.").into());
        }
        return Err(NODE_ID_UNSUPPORTED.into());
    }
    let mut client = connection.connect("features").await?;
    Ok(render_describe(&client.describe_features().await?))
}

/// One line per supported feature, in name order, in the `printf` layout of
/// `handleDescribe`.
fn render_describe(metadata: &FeatureMetadata) -> CommandResult {
    let mut supported = metadata.supported.iter().collect::<Vec<_>>();
    supported.sort_by(|a, b| a.name.cmp(&b.name));
    let epoch =
        (metadata.finalized_features_epoch >= 0).then_some(metadata.finalized_features_epoch);
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
