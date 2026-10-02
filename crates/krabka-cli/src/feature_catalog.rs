//! The KIP-584 and KIP-1022 facts that `kafka-features` and `kafka-storage`
//! print without a cluster: the feature levels that a release implies
//! (`version-mapping`) and the dependencies of a feature level
//! (`feature-dependencies`).
//!
//! Both tools print the same lines from the same feature registry, so
//! `krabka features` and `krabka storage` share this module. The two tools word
//! their errors differently, and [`Dialect`] selects the wording.
//!
//! Every value comes from `krabka_metadata::feature_registry()` and the
//! `metadata.version` table, bounded to what Kafka 4.3.1 knows. A feature or
//! a level that krabka does not register is not printed, even where Kafka
//! 4.3.1 knows it. The table's trunk levels past Kafka 4.3.1's `4.4-IV0` are
//! not printed or accepted either.

use clap::Args;
use krabka_metadata::{
    Feature, feature, feature_registry,
    metadata_version::{
        self, KRAFT_VERSION_FEATURE, METADATA_VERSION_FEATURE, METADATA_VERSION_MIN,
        MetadataVersion,
    },
};
use serde_json::{Value, json};

use crate::output::{CommandError, CommandResult};

/// The order of Kafka 4.3.1's `Feature.PRODUCTION_FEATURES`, which is the order
/// in which both tools print features. A registered feature that is not in
/// this list follows these, in registry order.
const KAFKA_FEATURE_ORDER: &[&str] = &[
    "kraft.version",
    "transaction.version",
    "group.version",
    "eligible.leader.replicas.version",
    "share.version",
    "streams.version",
];

/// Kafka 4.3.1's `MetadataVersion.latestTesting()`, `4.4-IV0`: the highest
/// level that both tools accept by its `X.Y-IVn` name and list as supported.
/// They resolve a release with unstable versions enabled.
const KAFKA_LATEST_TESTING_METADATA_VERSION: i16 = 31;

/// The `metadata.version` level at which Kafka's `KRaftVersion.KRAFT_VERSION_1`
/// becomes the default, `3.9-IV0`.
const KRAFT_VERSION_1_METADATA_VERSION: i16 = 21;

/// Which tool's error wording to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// `kafka-features` (`FeatureCommand.java`).
    Features,
    /// `kafka-storage` (`StorageTool.scala`).
    Storage,
}

/// `version-mapping`: the feature levels that a release implies.
#[derive(Debug, Args)]
pub struct VersionMappingArgs {
    /// The release version to use for the corresponding feature mapping, for
    /// example `4.0` or `4.0-IV3`. The default is the latest release.
    #[arg(short = 'r', long)]
    release_version: Option<String>,
    /// Print the mapping of every `metadata.version` level, one row per level.
    /// A krabka addition.
    #[arg(long, conflicts_with = "release_version")]
    all: bool,
}

impl VersionMappingArgs {
    pub fn run(&self) -> Result<CommandResult, CommandError> {
        if self.all {
            return Ok(version_mapping_table());
        }
        let mapping = version_mapping(self.release_version.as_deref())?;
        Ok(CommandResult::success(mapping.human(), mapping.json()))
    }
}

/// `feature-dependencies`: the KIP-1022 dependencies of feature levels.
#[derive(Debug, Args)]
pub struct FeatureDependenciesArgs {
    /// The feature and level to look up dependencies for, in `feature=level`
    /// format, for example `metadata.version=5`. The flag can repeat. Without
    /// it, krabka prints every level of every registered feature.
    #[arg(short = 'f', long)]
    feature: Vec<String>,
}

impl FeatureDependenciesArgs {
    pub fn run(&self, dialect: Dialect) -> Result<CommandResult, CommandError> {
        let rows = if self.feature.is_empty() {
            all_feature_dependencies()
        } else {
            self.feature
                .iter()
                .map(|spec| {
                    let (name, level) = parse_dependency_query(spec, dialect)?;
                    feature_dependencies(&name, level, dialect)
                })
                .collect::<Result<Vec<_>, String>>()?
        };
        Ok(CommandResult::success(
            rows.iter().flat_map(FeatureDependencies::human).collect(),
            rows.iter()
                .map(FeatureDependencies::json)
                .collect::<Vec<_>>(),
        ))
    }
}

/// Every registered feature except `metadata.version`, in Kafka's order.
#[must_use]
pub fn production_features() -> Vec<&'static dyn Feature> {
    let mut features = feature_registry()
        .iter()
        .copied()
        .filter(|feature| feature.name() != METADATA_VERSION_FEATURE)
        .collect::<Vec<_>>();
    features.sort_by_key(|feature| kafka_rank(feature.name()));
    features
}

fn kafka_rank(name: &str) -> usize {
    KAFKA_FEATURE_ORDER
        .iter()
        .position(|known| *known == name)
        .unwrap_or(usize::MAX)
}

/// `Feature.defaultLevel`: the level of `feature` that a cluster formatted at
/// `metadata_level` finalizes. `kraft.version` follows Kafka's
/// `KRaftVersion`, 1 from `3.9-IV0`, where the registry's default is the
/// level that `krabka format` writes as a feature record, always 0.
#[must_use]
pub fn default_level(feature: &dyn Feature, metadata_level: i16) -> i16 {
    if feature.name() == KRAFT_VERSION_FEATURE {
        return i16::from(metadata_level >= KRAFT_VERSION_1_METADATA_VERSION);
    }
    if feature.name() == metadata_version::SHARE_VERSION_FEATURE {
        // Kafka 4.3.1 defines only SV_0 and SV_1, even at its testing 4.4-IV0.
        return feature.default_level(metadata_level).min(1);
    }
    feature.default_level(metadata_level)
}

/// Every `metadata.version` that Kafka 4.3.1 knows, lowest level first:
/// `MetadataVersion.VERSIONS`, up to `latestTesting()`.
fn metadata_versions() -> impl Iterator<Item = MetadataVersion> {
    (METADATA_VERSION_MIN..=KAFKA_LATEST_TESTING_METADATA_VERSION)
        .filter_map(metadata_version::from_feature_level)
}

/// `MetadataVersion.fromFeatureLevel` at Kafka 4.3.1: the version of
/// `level`, or `None` past `latestTesting()`.
fn known_metadata_version(level: i16) -> Option<MetadataVersion> {
    metadata_versions().find(|version| version.feature_level() == level)
}

/// `MetadataVersion.LATEST_PRODUCTION`, which both tools use when no release
/// is given. The table also holds newer, not yet production, levels.
#[must_use]
pub fn latest_production_metadata_version() -> MetadataVersion {
    metadata_version::from_feature_level(krabka_format::LATEST_PRODUCTION_METADATA_VERSION)
        .expect("the table holds the latest production level")
}

/// The name of `level` as `FeatureCommand.levelToString` prints it: the
/// `X.Y-IVn` name for `metadata.version`, `UNKNOWN <level>` for a
/// `metadata.version` level that Kafka 4.3.1 does not know, and the number for
/// any other feature.
#[must_use]
pub fn level_to_string(feature: &str, level: i16) -> String {
    if feature == METADATA_VERSION_FEATURE {
        return known_metadata_version(level).map_or_else(
            || format!("UNKNOWN {level}"),
            |version| version.ivn().to_owned(),
        );
    }
    level.to_string()
}

/// Resolves a release string as `MetadataVersion.fromVersionString(release,
/// true)` does at Kafka 4.3.1. Only the first two dot-separated segments
/// count, so `3.8.1` is `3.8`. A release without an `-IVn` suffix is the
/// highest production level of that release, so `4.4` is unknown. An `X.Y-IVn`
/// name resolves up to `latestTesting()`, `4.4-IV0`.
///
/// # Errors
/// Returns Kafka's message, which lists every known version, when Kafka 4.3.1
/// does not know the release.
pub fn resolve_release(release: &str) -> Result<MetadataVersion, String> {
    let segments = release.split('.').collect::<Vec<_>>();
    let key = if segments.len() <= 2 {
        release.to_owned()
    } else {
        segments[..2].join(".")
    };
    let production = krabka_format::LATEST_PRODUCTION_METADATA_VERSION;
    let resolved = metadata_versions().filter(|version| {
        if key.contains('-') {
            version.ivn() == key
        } else {
            version.short() == key && version.feature_level() <= production
        }
    });
    resolved
        .max_by_key(|version| version.feature_level())
        .ok_or_else(|| {
            format!(
                "Unknown metadata.version '{release}'. Supported metadata.version are: {}",
                metadata_versions()
                    .map(MetadataVersion::ivn)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

/// The feature levels that one release implies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionMapping {
    /// The release as the operator wrote it. Kafka prints it back unchanged.
    pub release: String,
    /// The `metadata.version` that the release resolves to.
    pub metadata_version: MetadataVersion,
    /// Each production feature with its default level at that release.
    pub features: Vec<(&'static str, i16)>,
}

impl VersionMapping {
    fn at(metadata_version: MetadataVersion, release: String) -> Self {
        let level = metadata_version.feature_level();
        Self {
            release,
            metadata_version,
            features: production_features()
                .into_iter()
                .map(|feature| (feature.name(), default_level(feature, level)))
                .collect(),
        }
    }

    /// The lines that both tools print.
    #[must_use]
    pub fn human(&self) -> Vec<String> {
        std::iter::once(format!(
            "{METADATA_VERSION_FEATURE}={} ({})",
            self.metadata_version.feature_level(),
            self.release
        ))
        .chain(
            self.features
                .iter()
                .map(|(name, level)| format!("{name}={level}")),
        )
        .collect()
    }

    fn row(&self) -> String {
        self.human().join(" ")
    }

    #[must_use]
    pub fn json(&self) -> Value {
        json!({
            "release_version": self.release,
            "metadata_version": {
                "level": self.metadata_version.feature_level(),
                "name": self.metadata_version.ivn(),
            },
            "features": self
                .features
                .iter()
                .map(|(name, level)| json!({"feature": name, "level": level}))
                .collect::<Vec<_>>(),
        })
    }
}

/// The mapping of `release`, or of the latest production release when it is
/// `None`.
///
/// # Errors
/// Returns the message of [`resolve_release`].
pub fn version_mapping(release: Option<&str>) -> Result<VersionMapping, String> {
    if let Some(release) = release {
        return Ok(VersionMapping::at(
            resolve_release(release)?,
            release.to_owned(),
        ));
    }
    let latest = latest_production_metadata_version();
    Ok(VersionMapping::at(latest, latest.ivn().to_owned()))
}

/// The mapping of every `metadata.version` level, one row per level.
fn version_mapping_table() -> CommandResult {
    let rows = metadata_versions()
        .map(|version| VersionMapping::at(version, version.ivn().to_owned()))
        .collect::<Vec<_>>();
    CommandResult::success(
        rows.iter().map(VersionMapping::row).collect(),
        rows.iter().map(VersionMapping::json).collect::<Vec<_>>(),
    )
}

/// One feature level that another feature level requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    pub feature: String,
    pub level: i16,
}

/// The dependencies of one feature level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureDependencies {
    pub feature: String,
    pub level: i16,
    pub dependencies: Vec<Dependency>,
}

impl FeatureDependencies {
    /// The lines that both tools print.
    #[must_use]
    pub fn human(&self) -> Vec<String> {
        let subject = format!("{}={}", self.feature, self.level);
        if self.feature == METADATA_VERSION_FEATURE {
            return vec![format!(
                "{subject} ({}) has no dependencies.",
                level_to_string(&self.feature, self.level)
            )];
        }
        if self.dependencies.is_empty() {
            return vec![format!("{subject} has no dependencies.")];
        }
        std::iter::once(format!("{subject} requires:"))
            .chain(self.dependencies.iter().map(|dependency| {
                if dependency.feature == METADATA_VERSION_FEATURE {
                    format!(
                        "    {}={} ({})",
                        dependency.feature,
                        dependency.level,
                        level_to_string(&dependency.feature, dependency.level)
                    )
                } else {
                    format!("    {}={}", dependency.feature, dependency.level)
                }
            }))
            .collect()
    }

    #[must_use]
    pub fn json(&self) -> Value {
        json!({
            "feature": self.feature,
            "level": self.level,
            "dependencies": self
                .dependencies
                .iter()
                .map(|dependency| json!({"feature": dependency.feature, "level": dependency.level}))
                .collect::<Vec<_>>(),
        })
    }
}

/// Parses one `feature=level` query in the wording of `dialect`.
///
/// # Errors
/// Returns the tool's message for a missing `=` or for a level that is not a
/// 16-bit integer.
pub fn parse_dependency_query(spec: &str, dialect: Dialect) -> Result<(String, i16), String> {
    match dialect {
        Dialect::Features => parse_name_and_level(spec),
        Dialect::Storage => {
            let (name, level) = spec.split_once('=').ok_or_else(|| {
                format!(
                    "Invalid feature format: {spec}. Expected format: 'feature=version' (e.g. 'group.version=1')"
                )
            })?;
            let (name, level) = (name.trim(), level.trim());
            let level = level
                .parse()
                .map_err(|_| format!("Invalid version format: {level} for feature {name}"))?;
            Ok((name.to_owned(), level))
        }
    }
}

/// Parses `feature=level` as `FeatureCommand.parseNameAndLevel` does: both
/// sides are trimmed, and the level is a 16-bit integer.
///
/// # Errors
/// Returns Kafka's message for a missing `=` or for a level that does not
/// parse.
pub fn parse_name_and_level(spec: &str) -> Result<(String, i16), String> {
    let (name, level) = spec.split_once('=').ok_or_else(|| {
        format!("Can't parse feature=level string {spec}: equals sign not found.")
    })?;
    let level = level.trim();
    let level = level.parse().map_err(|_| {
        format!("Can't parse feature=level string {spec}: unable to parse {level} as a short.")
    })?;
    Ok((name.trim().to_owned(), level))
}

/// The dependencies of `name` at `level`.
///
/// # Errors
/// Returns the tool's message for an unknown feature, an unknown
/// `metadata.version` level, or a level that the feature does not define.
pub fn feature_dependencies(
    name: &str,
    level: i16,
    dialect: Dialect,
) -> Result<FeatureDependencies, String> {
    if name == METADATA_VERSION_FEATURE {
        if known_metadata_version(level).is_none() {
            return Err(format!("Unknown metadata.version {level}"));
        }
        return Ok(FeatureDependencies {
            feature: name.to_owned(),
            level,
            dependencies: Vec::new(),
        });
    }
    let registered = feature(name).ok_or_else(|| format!("Unknown feature: {name}"))?;
    let (min, max) = registered.supported_range();
    if !(min..=max).contains(&level) {
        return Err(match dialect {
            Dialect::Features => format!("No feature:{name} with feature level {level}"),
            Dialect::Storage => {
                format!("Feature level {level} is not supported for feature {name}")
            }
        });
    }
    Ok(dependencies_of(registered, level))
}

fn dependencies_of(feature: &dyn Feature, level: i16) -> FeatureDependencies {
    FeatureDependencies {
        feature: feature.name().to_owned(),
        level,
        dependencies: feature
            .dependencies(level)
            .iter()
            .map(|(name, level)| Dependency {
                feature: (*name).to_owned(),
                level: *level,
            })
            .collect(),
    }
}

/// The dependencies of every level of every registered feature:
/// `metadata.version` first, then the production features in Kafka's order.
#[must_use]
pub fn all_feature_dependencies() -> Vec<FeatureDependencies> {
    let metadata_versions = metadata_versions().map(|version| FeatureDependencies {
        feature: METADATA_VERSION_FEATURE.to_owned(),
        level: version.feature_level(),
        dependencies: Vec::new(),
    });
    let features = production_features().into_iter().flat_map(|feature| {
        let (min, max) = feature.supported_range();
        (min..=max).map(move |level| dependencies_of(feature, level))
    });
    metadata_versions.chain(features).collect()
}

#[cfg(test)]
mod tests;
