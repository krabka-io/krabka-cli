//! `krabka configs`, the counterpart of `kafka-configs`.
//!
//! The flags, the checks and their order, the messages and the stdout lines
//! are those of `ConfigCommand` in Kafka 4.3.1. A check that `kafka-configs`
//! makes on the command line is made here before any connection, so a
//! command that Kafka refuses is refused here with the same message.
//!
//! Config resources (topics, brokers, broker loggers, client-metrics
//! subscriptions and groups) go through `DescribeConfigs` and
//! `IncrementalAlterConfigs`, client quotas of users, clients and ips through
//! `DescribeClientQuotas` and `AlterClientQuotas`, and SCRAM credentials
//! through `DescribeUserScramCredentials` and `AlterUserScramCredentials`,
//! as `ConfigCommand` sends them.

mod java;
mod parse;
mod render;

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, ToSocketAddrs as _},
    path::PathBuf,
};

use clap::{Arg, ArgAction, ArgMatches, Args, FromArgMatches};
use krabka_client_admin::{
    AdminClient, AdminError, AlterConfigOp, AlterConfigOpType, ClientQuotaAlteration,
    ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotas, Config,
    ConfigResource, ConfigResourceType, ConfigSource, DescribeClusterOptions,
    DescribeConfigsOptions, ENTITY_CLIENT_ID, ENTITY_IP, ENTITY_USER,
    IncrementalAlterConfigsOptions, KafkaError, ListTopicsOptions, QuotaOp, ScramDeletion,
    ScramUpsertion, groups::ListGroupsOptions,
};
use krabka_client_core::ClientError;
use serde_json::json;

use self::{
    parse::{Mechanism, ScramCredential},
    render::DescribedConfig,
};
use crate::{
    connection::{ConnectionArgs, Properties},
    jvm::{Table, hash_order, integer_hash, string_hash},
    output::{CommandError, CommandResult},
};

const TOPICS: &str = "topics";
const CLIENTS: &str = "clients";
const USERS: &str = "users";
const BROKERS: &str = "brokers";
const IPS: &str = "ips";
const CLIENT_METRICS: &str = "client-metrics";
const GROUPS: &str = "groups";
const BROKER_LOGGERS: &str = "broker-loggers";

/// The entity types, in the order of Kafka's message that lists them.
const ENTITY_TYPES: [&str; 8] = [
    TOPICS,
    CLIENTS,
    USERS,
    BROKERS,
    IPS,
    CLIENT_METRICS,
    GROUPS,
    BROKER_LOGGERS,
];

/// `QuotaConfig.isClientOrUserQuotaConfig`.
const USER_AND_CLIENT_QUOTAS: [&str; 4] = [
    "producer_byte_rate",
    "consumer_byte_rate",
    "request_percentage",
    "controller_mutation_rate",
];

/// `QuotaConfig.ipConfigs`.
const IP_QUOTAS: [&str; 1] = ["connection_creation_rate"];

/// The message of the `DUPLICATE_RESOURCE` answer that a Kafka controller
/// gives to an `AlterUserScramCredentials` request that changes one user
/// twice, as `kafka-configs` sends it for two SCRAM changes at once.
const ALTERED_TWICE: &str = "AlterUserScramCredentials failed: DUPLICATE_RESOURCE (92): A user credential cannot be altered twice in the same request";

#[derive(Debug, Args)]
pub struct ConfigsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Alter the configuration for the entity.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "alter"
    )]
    alter: Option<bool>,
    /// List configs for the given entity.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "describe"
    )]
    describe: Option<bool>,
    /// List all configs for the given entity, including static configs if
    /// available.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "all"
    )]
    all: Option<bool>,
    /// Type of entity
    /// (topics/clients/users/brokers/broker-loggers/ips/client-metrics/groups).
    #[arg(long, value_name = "ENTITY_TYPE")]
    entity_type: Vec<String>,
    #[command(flatten)]
    entity: EntitySelectors,
    /// Key Value pairs of configs to add. Square brackets can be used to
    /// group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'.
    #[arg(long, value_name = "CONFIGS", allow_hyphen_values = true)]
    add_config: Vec<String>,
    /// Path to a properties file with configs to add.
    #[arg(long, value_name = "PATH")]
    add_config_file: Vec<PathBuf>,
    /// Config keys to remove 'k1,k2'.
    #[arg(
        long,
        value_name = "KEYS",
        value_delimiter = ',',
        allow_hyphen_values = true
    )]
    delete_config: Vec<String>,
    /// The topic's name.
    #[arg(long)]
    topic: Vec<String>,
    /// The client's ID.
    #[arg(long)]
    client: Vec<String>,
    /// The config defaults for all clients.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "client_defaults"
    )]
    client_defaults: Option<bool>,
    /// The user's principal name.
    #[arg(long)]
    user: Vec<String>,
    /// The config defaults for all users.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "user_defaults"
    )]
    user_defaults: Option<bool>,
    /// The broker's ID.
    #[arg(long)]
    broker: Vec<String>,
    /// The config defaults for all brokers.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "broker_defaults"
    )]
    broker_defaults: Option<bool>,
    /// The broker's ID for its logger config.
    #[arg(long)]
    broker_logger: Vec<String>,
    /// The config defaults for all IPs.
    #[arg(
        long,
        num_args = 0,
        default_missing_value = "true",
        overrides_with = "ip_defaults"
    )]
    ip_defaults: Option<bool>,
    /// The IP address.
    #[arg(long)]
    ip: Vec<String>,
    /// The group's ID.
    #[arg(long)]
    group: Vec<String>,
    /// The client metrics config resource name.
    #[arg(long)]
    client_metrics: Vec<String>,
}

/// `--entity-name` and `--entity-default`, which name the entities of the
/// `--entity-type` flags in the order they appear on the command line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct EntitySelectors {
    /// One entry for each `--entity-name` and each `--entity-default`, in
    /// command-line order. `--entity-default` is the empty name.
    names: Vec<String>,
    /// The `--entity-name` values alone.
    entity_names: Vec<String>,
    entity_default: bool,
}

const ENTITY_NAME: &str = "entity_name";
const ENTITY_DEFAULT: &str = "entity_default";

impl FromArgMatches for EntitySelectors {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let entity_names = matches
            .get_many::<String>(ENTITY_NAME)
            .map(|values| values.cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let positions = |id| {
            matches
                .indices_of(id)
                .map(Iterator::collect::<Vec<_>>)
                .unwrap_or_default()
        };
        let mut ordered = positions(ENTITY_NAME)
            .into_iter()
            .zip(entity_names.iter().cloned())
            .chain(
                positions(ENTITY_DEFAULT)
                    .into_iter()
                    .map(|index| (index, String::new())),
            )
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(index, _)| *index);
        Ok(Self {
            names: ordered.into_iter().map(|(_, name)| name).collect(),
            entity_default: matches.get_many::<String>(ENTITY_DEFAULT).is_some(),
            entity_names,
        })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for EntitySelectors {
    fn augment_args(command: clap::Command) -> clap::Command {
        command
            .arg(
                Arg::new(ENTITY_NAME)
                    .long("entity-name")
                    .value_name("ENTITY_NAME")
                    .action(ArgAction::Append)
                    .help(
                        "Name of entity (topic name/client id/user principal name/broker \
                         id/ip/client metrics/group id)",
                    ),
            )
            .arg(
                Arg::new(ENTITY_DEFAULT)
                    .long("entity-default")
                    .num_args(0)
                    .default_missing_value("")
                    .action(ArgAction::Append)
                    .help(
                        "Default entity name for clients/users/brokers/ips (applies to \
                         corresponding entity type)",
                    ),
            )
    }

    fn augment_args_for_update(command: clap::Command) -> clap::Command {
        Self::augment_args(command)
    }
}

/// What a checked command line asks for.
#[derive(Debug, Clone, PartialEq)]
enum Plan {
    /// Describe the configs of one config resource, or of every resource of
    /// the type, as `describeResourceConfig` does. `entity_type` is
    /// `topics`, `brokers`, `broker-loggers`, `client-metrics` or `groups`,
    /// and an empty name is the default broker.
    DescribeResources {
        entity_type: &'static str,
        name: Option<String>,
        all: bool,
    },
    /// Delete, then set, configs of one topic, client-metrics subscription,
    /// broker (or the default broker, the empty name) or group, as
    /// `alterResourceConfig` does.
    AlterResource {
        entity_type: &'static str,
        name: String,
        deletes: Vec<String>,
        sets: Vec<(String, String)>,
    },
    /// Delete, then set, levels of loggers of one broker, after checking
    /// that the broker has each logger.
    AlterBrokerLoggers {
        broker: String,
        deletes: Vec<String>,
        sets: Vec<(String, String)>,
    },
    /// Describe the quotas of the entities that `components` match, then
    /// the SCRAM credentials of `scram_users`, as
    /// `describeClientQuotaAndUserScramCredentialConfigs` and
    /// `describeQuotaConfigs` do.
    DescribeQuotas {
        components: Vec<ClientQuotaFilterComponent>,
        scram_users: Option<Vec<String>>,
    },
    /// Set and remove quotas of one entity, as `alterQuotaConfigs` does. The
    /// values are parsed after the current quotas are read, as Kafka does.
    /// `entity_type` and `name` are the head of the command line, which the
    /// completion line names.
    AlterQuotas {
        entity_type: &'static str,
        name: String,
        entity: ClientQuotaEntity,
        /// The filter that reads the current quotas of the entity, in
        /// command-line order.
        components: Vec<ClientQuotaFilterComponent>,
        sets: Vec<(String, String)>,
        deletes: Vec<String>,
    },
    /// Set or delete one SCRAM credential of one user, or change nothing.
    /// `entity_type` is `clients` only for a `clients` alter that has no
    /// config to change, which Kafka reports as a client.
    AlterUserScram {
        entity_type: &'static str,
        name: String,
        change: Option<ScramChange>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScramChange {
    Upsert(ScramCredential),
    Delete(Mechanism),
}

/// The entity type of the command line as a static string. Every type is
/// checked against [`ENTITY_TYPES`] before this is called.
fn static_type(entity_type: &str) -> &'static str {
    ENTITY_TYPES
        .iter()
        .copied()
        .find(|known| *known == entity_type)
        .expect("the entity type is checked")
}

/// The `ClientQuotaEntity` type of a quota entity type of the command line.
fn quota_entity_type(entity_type: &str) -> &'static str {
    match entity_type {
        USERS => ENTITY_USER,
        CLIENTS => ENTITY_CLIENT_ID,
        _ => ENTITY_IP,
    }
}

/// The `ClientQuotaEntity` of `alterQuotaConfigs`: each entity type to its
/// name, `None` for the default entity.
fn quota_entity(types: &[String], names: &[String]) -> ClientQuotaEntity {
    types
        .iter()
        .zip(names)
        .map(|(entity_type, name)| {
            (
                quota_entity_type(entity_type).to_owned(),
                (!name.is_empty()).then(|| name.clone()),
            )
        })
        .collect()
}

/// The filter components of `getAllClientQuotasConfigs`: one for each
/// entity type, the entity that its name names, the default entity for the
/// empty name, and any entity of the type when it has no name.
fn quota_components(types: &[String], names: &[String]) -> Vec<ClientQuotaFilterComponent> {
    types
        .iter()
        .enumerate()
        .map(|(index, entity_type)| {
            let entity_type = quota_entity_type(entity_type);
            match names.get(index).map(String::as_str) {
                Some("") => ClientQuotaFilterComponent::of_default_entity(entity_type),
                Some(name) => ClientQuotaFilterComponent::of_entity(entity_type, name),
                None => ClientQuotaFilterComponent::of_entity_type(entity_type),
            }
        })
        .collect()
}

/// `options.valueOf` on an option that the command line repeats.
fn single<'a, T>(values: &'a [T], option: &str) -> Result<Option<&'a T>, String> {
    match values {
        [] => Ok(None),
        [value] => Ok(Some(value)),
        _ => Err(format!(
            "Found multiple arguments for option {option}, but you asked for only one"
        )),
    }
}

/// `InetAddress.getByName` succeeds for the name.
fn resolves(host: &str) -> bool {
    host.is_empty()
        || host.parse::<IpAddr>().is_ok()
        || (host, 0)
            .to_socket_addrs()
            .is_ok_and(|mut addresses| addresses.next().is_some())
}

impl ConfigsArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let plan = self.plan(&resolves)?;
        // Boxed, so the future of `run` stays small.
        let mut client = Box::pin(self.connection.connect("configs")).await?;
        Box::pin(plan.execute(&mut client)).await
    }

    /// The entity flags, in `ConfigCommandOptions.entityFlags` order.
    fn entity_flags(&self) -> [(&[String], &'static str, &'static str); 8] {
        [
            (&self.topic, "topic", TOPICS),
            (&self.client, "client", CLIENTS),
            (&self.user, "user", USERS),
            (&self.broker, "broker", BROKERS),
            (&self.broker_logger, "broker-logger", BROKER_LOGGERS),
            (&self.ip, "ip", IPS),
            (&self.client_metrics, "client-metrics", CLIENT_METRICS),
            (&self.group, "group", GROUPS),
        ]
    }

    /// The entity-defaults flags, in `entityDefaultsFlags` order.
    const fn default_flags(&self) -> [(bool, &'static str); 4] {
        [
            (self.client_defaults.is_some(), CLIENTS),
            (self.user_defaults.is_some(), USERS),
            (self.broker_defaults.is_some(), BROKERS),
            (self.ip_defaults.is_some(), IPS),
        ]
    }

    fn has_entity_flag(&self) -> bool {
        self.entity_flags()
            .iter()
            .any(|(values, _, _)| !values.is_empty())
            || self.default_flags().iter().any(|(set, _)| *set)
    }

    /// `ConfigCommandOptions.entityTypes`.
    fn entity_types(&self) -> Vec<String> {
        let flags = self
            .entity_flags()
            .into_iter()
            .filter(|(values, _, _)| !values.is_empty())
            .map(|(_, _, entity_type)| entity_type);
        let defaults = self
            .default_flags()
            .into_iter()
            .filter(|(set, _)| *set)
            .map(|(_, entity_type)| entity_type);
        self.entity_type
            .iter()
            .cloned()
            .chain(flags.chain(defaults).map(str::to_owned))
            .collect()
    }

    /// `ConfigCommandOptions.entityNames`. A default entity is the empty
    /// name.
    fn entity_names(&self) -> Result<Vec<String>, String> {
        let mut names = self.entity.names.clone();
        for (values, option, _) in self.entity_flags() {
            if let Some(value) = single(values, option)? {
                names.push(value.clone());
            }
        }
        for (set, _) in self.default_flags() {
            if set {
                names.push(String::new());
            }
        }
        Ok(names)
    }

    /// `ConfigCommandOptions.checkArgs`, then the checks that
    /// `processCommand`, `alterConfig` and `describeConfig` make before their
    /// first request. `resolve` says whether a name is an IP address or a
    /// host that resolves.
    fn plan(&self, resolve: &dyn Fn(&str) -> bool) -> Result<Plan, String> {
        if usize::from(self.alter.is_some()) + usize::from(self.describe.is_some()) != 1 {
            return Err("Command must include exactly one action: --describe, --alter".into());
        }
        if self.describe.is_some() {
            for (present, option) in [
                (!self.add_config.is_empty(), "add-config"),
                (!self.delete_config.is_empty(), "delete-config"),
            ] {
                if present {
                    return Err(format!(
                        "Option \"[describe]\" can't be used with option \"[{option}]\""
                    ));
                }
            }
        }
        let types = self.entity_types();
        let mut extra = types.clone();
        for entity_type in dedup(&types) {
            let first = extra
                .iter()
                .position(|candidate| candidate == &entity_type)
                .expect("every distinct type is in the list");
            extra.remove(first);
        }
        if !extra.is_empty() {
            return Err(format!(
                "Duplicate entity type(s) specified: {}",
                extra.join(",")
            ));
        }
        if self.connection.bootstrap_server.is_empty()
            && self.connection.bootstrap_controller.is_empty()
        {
            return Err(
                "Either --bootstrap-server or --bootstrap-controller must be specified.".into(),
            );
        }
        if let Some(invalid) = types
            .iter()
            .find(|entity_type| !ENTITY_TYPES.contains(&entity_type.as_str()))
        {
            return Err(format!(
                "Invalid entity type {invalid}, the entity type must be one of {} with a --bootstrap-server or --bootstrap-controller argument",
                ENTITY_TYPES.join(", ")
            ));
        }
        let head = match types.as_slice() {
            [] => return Err("At least one entity type must be specified".into()),
            [head] => head.as_str(),
            // Duplicates are refused above, so two types are `users` and
            // `clients` when neither is anything else.
            [head, other] if [head, other].iter().all(|t| *t == USERS || *t == CLIENTS) => {
                head.as_str()
            }
            _ => {
                return Err(
                    "Only 'users' and 'clients' entity types may be specified together".into(),
                );
            }
        };
        if (!self.entity.names.is_empty() || !self.entity_type.is_empty()) && self.has_entity_flag()
        {
            return Err(
                "--entity-{type,name,default} should not be used in conjunction with specific entity flags"
                    .into(),
            );
        }
        let names = self.entity_names()?;
        let has_name = names.iter().any(|name| !name.is_empty());
        let has_default = names.iter().any(String::is_empty);
        let contains = |entity_type: &str| types.iter().any(|candidate| candidate == entity_type);
        if has_name && (contains(BROKERS) || contains(BROKER_LOGGERS)) {
            for (values, option) in [
                (self.entity.entity_names.as_slice(), "entity-name"),
                (self.broker.as_slice(), "broker"),
                (self.broker_logger.as_slice(), "broker-logger"),
            ] {
                if let Some(id) = single(values, option)? {
                    java::parse_int(id).map_err(|_| {
                        format!(
                            "The entity name for {head} must be a valid integer broker id, but it is: {id}"
                        )
                    })?;
                }
            }
        }
        if has_name && contains(IPS) {
            for (values, option) in [
                (self.entity.entity_names.as_slice(), "entity-name"),
                (self.ip.as_slice(), "ip"),
            ] {
                if let Some(ip) = single(values, option)?
                    && !resolve(ip)
                {
                    return Err(format!(
                        "The entity name for {head} must be a valid IP or resolvable host, but it is: {ip}"
                    ));
                }
            }
        }
        let quota_types = [USERS, CLIENTS, BROKERS, IPS];
        if self.describe.is_some() {
            if !quota_types.iter().any(|entity_type| contains(entity_type))
                && self.entity.entity_default
            {
                return Err(format!(
                    "--entity-default must not be specified with --describe of {}",
                    types.join(",")
                ));
            }
            if contains(BROKER_LOGGERS) && !has_name {
                return Err(format!(
                    "An entity name must be specified with --describe of {}",
                    types.join(",")
                ));
            }
            return self.describe_plan(head, &types, &names);
        }
        if quota_types.iter().any(|entity_type| contains(entity_type)) {
            if !has_name && !has_default {
                return Err(
                    "An entity-name or default entity must be specified with --alter of users, clients, brokers or ips"
                        .into(),
                );
            }
        } else if !has_name {
            return Err(format!(
                "An entity name must be specified with --alter of {}",
                types.join(",")
            ));
        }
        if !self.add_config.is_empty() && !self.add_config_file.is_empty() {
            return Err("Only one of --add-config or --add-config-file must be specified".into());
        }
        if self.add_config.is_empty()
            && self.add_config_file.is_empty()
            && self.delete_config.is_empty()
        {
            return Err(
                "At least one of --add-config, --add-config-file, or --delete-config must be specified with --alter"
                    .into(),
            );
        }
        if types.len() != names.len() {
            return Err("An entity name must be specified for every entity type".into());
        }
        self.alter_plan(head, &types, &names)
    }

    /// `ConfigCommand.describeConfig` up to its first request.
    fn describe_plan(
        &self,
        head: &str,
        types: &[String],
        names: &[String],
    ) -> Result<Plan, String> {
        match head {
            TOPICS | BROKERS | BROKER_LOGGERS | CLIENT_METRICS | GROUPS => {
                let name = names.first().cloned();
                if head == TOPICS
                    && let Some(name) = &name
                {
                    parse::topic_name(name)?;
                }
                Ok(Plan::DescribeResources {
                    entity_type: static_type(head),
                    name,
                    all: self.all.is_some(),
                })
            }
            _ => {
                if names.len() > types.len() {
                    return Err("More entity names specified than entity types".into());
                }
                // Kafka describes SCRAM credentials only for users, and not
                // for the default user.
                let scram_users = (head != IPS
                    && !types.iter().any(|entity_type| entity_type == CLIENTS)
                    && !names.iter().any(String::is_empty))
                .then(|| names.to_vec());
                Ok(Plan::DescribeQuotas {
                    components: quota_components(types, names),
                    scram_users,
                })
            }
        }
    }

    /// The configs of `--add-config-file` and `--add-config`, keyed and in
    /// the iteration order of the `java.util.Properties` that
    /// `ConfigCommand.parseConfigsToBeAdded` fills, and checked by
    /// `validatePropsKey`.
    ///
    /// The keys are checked in the order of the `Properties`, and returned in
    /// the order of the Scala `Map` that `alterConfig` copies them into:
    /// the same order up to four keys, and a Scala `HashMap`'s above that.
    /// The order decides the order of the operations in the request and of
    /// the keys that an error message lists.
    fn configs_to_add(&self) -> Result<Vec<(String, String)>, String> {
        let mut configs = BTreeMap::new();
        if let Some(path) = single(&self.add_config_file, "add-config-file")? {
            let bytes = std::fs::read(path)
                .map_err(|error| format!("read --add-config-file {}: {error}", path.display()))?;
            let properties = Properties::parse(&bytes).map_err(|error| error.to_string())?;
            for (key, value) in properties.entries() {
                configs.insert(key.to_owned(), value.to_owned());
            }
        }
        if let Some(value) = single(&self.add_config, "add-config")? {
            configs.extend(parse::add_config(value)?);
        }
        let order = java::hash_map_order(configs.keys().map(String::as_str), None)
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for key in &order {
            parse::config_key(key)?;
        }
        Ok(java::scala_set_order(order)
            .into_iter()
            .map(|key| {
                let value = configs[&key].clone();
                (key, value)
            })
            .collect())
    }

    /// `ConfigCommand.alterConfig` up to its first request.
    fn alter_plan(&self, head: &str, types: &[String], names: &[String]) -> Result<Plan, String> {
        let adds = self.configs_to_add()?;
        let deletes = self
            .delete_config
            .iter()
            .map(|key| java::trim(key).to_owned())
            .collect::<Vec<_>>();
        let name = names[0].clone();
        match head {
            TOPICS | CLIENT_METRICS | BROKERS | GROUPS => {
                if head == BROKERS && !name.is_empty() {
                    java::parse_int(&name).map_err(|_| {
                        format!(
                            "The entity name for {head} must be a valid integer broker id, found: {name}"
                        )
                    })?;
                }
                Ok(Plan::AlterResource {
                    entity_type: static_type(head),
                    name,
                    deletes,
                    sets: adds,
                })
            }
            BROKER_LOGGERS => Ok(Plan::AlterBrokerLoggers {
                broker: name,
                deletes,
                sets: adds,
            }),
            IPS => {
                let unknown = adds
                    .iter()
                    .map(|(key, _)| key.as_str())
                    .chain(deletes.iter().map(String::as_str))
                    .filter(|key| !IP_QUOTAS.contains(key))
                    .collect::<Vec<_>>()
                    .join(",");
                if !unknown.is_empty() {
                    return Err(format!(
                        "Only connection quota configs can be added for '{IPS}' using --bootstrap-server. Unexpected config names: {unknown}"
                    ));
                }
                Ok(Plan::AlterQuotas {
                    entity_type: IPS,
                    name,
                    entity: quota_entity(types, names),
                    components: quota_components(types, names),
                    sets: adds,
                    deletes,
                })
            }
            _ => Self::alter_user_or_client_plan(head, types, names, adds, deletes),
        }
    }

    /// The `UserType | ClientType` arm of `ConfigCommand.alterConfig`.
    fn alter_user_or_client_plan(
        head: &str,
        types: &[String],
        names: &[String],
        adds: Vec<(String, String)>,
        deletes: Vec<String>,
    ) -> Result<Plan, String> {
        let name = names[0].clone();
        let is_quota = |key: &str| USER_AND_CLIENT_QUOTAS.contains(&key);
        let is_scram = |key: &str| Mechanism::from_config_key(key).is_some();
        let add_keys = adds.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>();
        let unknown_adds = add_keys
            .iter()
            .copied()
            .filter(|key| !is_quota(key) && !is_scram(key))
            .collect::<Vec<_>>();
        let scram_adds = add_keys
            .iter()
            .copied()
            .filter(|key| is_scram(key))
            .collect::<Vec<_>>();
        let unknown_deletes = deletes
            .iter()
            .map(String::as_str)
            .filter(|key| !is_quota(key) && !is_scram(key))
            .collect::<Vec<_>>();
        let scram_deletes = deletes
            .iter()
            .map(String::as_str)
            .filter(|key| is_scram(key))
            .collect::<Vec<_>>();
        let has_quota =
            add_keys.iter().any(|key| is_quota(key)) || deletes.iter().any(|key| is_quota(key));
        if head == CLIENTS || types.len() == 2 {
            if !unknown_adds.is_empty() || !scram_adds.is_empty() {
                let names = [unknown_adds.as_slice(), scram_adds.as_slice()].concat();
                return Err(format!(
                    "Only quota configs can be added for '{CLIENTS}' using --bootstrap-server. Unexpected config names: {}",
                    java::scala_set(&dedup(&names))
                ));
            }
            if !unknown_deletes.is_empty() || !scram_deletes.is_empty() {
                let names = [unknown_deletes.as_slice(), scram_deletes.as_slice()].concat();
                return Err(format!(
                    "Only quota configs can be deleted for '{CLIENTS}' using --bootstrap-server. Unexpected config names: {}",
                    java::scala_buffer(&names)
                ));
            }
        } else {
            if !unknown_adds.is_empty() {
                return Err(format!(
                    "Only quota and SCRAM credential configs can be added for '{USERS}' using --bootstrap-server. Unexpected config names: {}",
                    java::scala_set(&unknown_adds)
                ));
            }
            if !unknown_deletes.is_empty() {
                return Err(format!(
                    "Only quota and SCRAM credential configs can be deleted for '{USERS}' using --bootstrap-server. Unexpected config names: {}",
                    java::scala_buffer(&unknown_deletes)
                ));
            }
            if !scram_adds.is_empty() || !scram_deletes.is_empty() {
                if name.is_empty() {
                    return Err("The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.".into());
                }
                if has_quota {
                    return Err(format!(
                        "Cannot alter both quota and SCRAM credential configs simultaneously for '{USERS}' using --bootstrap-server."
                    ));
                }
            }
        }
        if has_quota {
            return Ok(Plan::AlterQuotas {
                entity_type: static_type(head),
                name,
                entity: quota_entity(types, names),
                components: quota_components(types, names),
                sets: adds,
                deletes,
            });
        }
        let mut changes = scram_deletes
            .iter()
            .filter_map(|key| Mechanism::from_config_key(key))
            .map(ScramChange::Delete)
            .collect::<Vec<_>>();
        for (key, value) in &adds {
            if let Some(mechanism) = Mechanism::from_config_key(key) {
                changes.push(ScramChange::Upsert(parse::scram_credential(
                    mechanism, value,
                )?));
            }
        }
        let empty_password = changes.iter().any(|change| {
            matches!(change, ScramChange::Upsert(credential) if credential.password.clone().expose().is_empty())
        });
        if empty_password {
            return Err("Password must not be empty".into());
        }
        if changes.len() > 1 {
            return Err(ALTERED_TWICE.into());
        }
        if types.len() == 2 {
            // Kafka reaches its SCRAM path with two entities only when there
            // is nothing to change, and fails an internal check there.
            return Err(format!(
                "Altering user SCRAM credentials should never occur for more zero or multiple users: List({})",
                names.join(", ")
            ));
        }
        Ok(Plan::AlterUserScram {
            entity_type: if head == CLIENTS { CLIENTS } else { USERS },
            name,
            change: changes.pop(),
        })
    }
}

/// The distinct items, in first-seen order.
fn dedup<S: AsRef<str> + Clone>(items: &[S]) -> Vec<S> {
    let mut seen = Vec::<S>::new();
    for item in items {
        if !seen.iter().any(|kept| kept.as_ref() == item.as_ref()) {
            seen.push(item.clone());
        }
    }
    seen
}

fn broker_error(api: &'static str, error: KafkaError) -> CommandError {
    CommandError::Broker {
        api,
        code: error.code,
        name: error.name,
        message: error.message,
    }
}

/// The line that `kafka-configs --alter` prints on success. An empty name
/// is the default entity.
fn completed(entity_type: &str, name: &str) -> CommandResult {
    let line = if name.is_empty() {
        format!("Completed updating default config for {entity_type} in the cluster.")
    } else {
        format!(
            "Completed updating config for {} {name}.",
            render::singular(entity_type)
        )
    };
    CommandResult::success(
        vec![line],
        json!({"entity_type": entity_type, "entity_name": name}),
    )
}

impl Plan {
    async fn execute(self, client: &mut AdminClient) -> Result<CommandResult, CommandError> {
        match self {
            Self::DescribeResources {
                entity_type,
                name,
                all,
            } => describe_resources(client, entity_type, name, all).await,
            Self::AlterResource {
                entity_type,
                name,
                deletes,
                sets,
            } => {
                let resource = config_resource(entity_type, &name);
                alter_resource(client, resource, deletes, sets)
                    .await
                    .map_err(|error| {
                        if error.code == UNSUPPORTED_VERSION {
                            CommandError::Other(INCREMENTAL_ALTER_CONFIGS_UNSUPPORTED.into())
                        } else {
                            broker_error("IncrementalAlterConfigs", error)
                        }
                    })?;
                Ok(completed(entity_type, &name))
            }
            Self::AlterBrokerLoggers {
                broker,
                deletes,
                sets,
            } => {
                let resource = config_resource(BROKER_LOGGERS, &broker);
                let loggers = resource_config(client, &resource, false).await?;
                let invalid = deletes
                    .iter()
                    .map(String::as_str)
                    .chain(sets.iter().map(|(key, _)| key.as_str()))
                    .filter(|logger| !loggers.entries.contains_key(*logger))
                    .collect::<Vec<_>>();
                if !invalid.is_empty() {
                    return Err(format!("Invalid broker logger(s): {}", invalid.join(",")).into());
                }
                alter_resource(client, resource, deletes, sets)
                    .await
                    .map_err(|error| broker_error("IncrementalAlterConfigs", error))?;
                Ok(completed(BROKER_LOGGERS, &broker))
            }
            Self::DescribeQuotas {
                components,
                scram_users,
            } => describe_quotas(client, components, scram_users).await,
            Self::AlterQuotas {
                entity_type,
                name,
                entity,
                components,
                sets,
                deletes,
            } => {
                let filter = ClientQuotaFilter::contains_only(components);
                let described = client.describe_client_quotas(&filter).await?;
                let current = quota_order(&described)
                    .into_iter()
                    .next()
                    .map(|(_, values)| values.clone())
                    .unwrap_or_default();
                let invalid = deletes
                    .iter()
                    .filter(|key| !current.contains_key(key.as_str()))
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                if !invalid.is_empty() {
                    return Err(format!("Invalid config(s): {}", invalid.join(",")).into());
                }
                let alteration = ClientQuotaAlteration {
                    entity,
                    ops: quota_ops(&sets, &deletes)?,
                };
                let results = client
                    .alter_client_quotas(std::slice::from_ref(&alteration), false)
                    .await?;
                if let Some(error) = results.into_values().find_map(Result::err) {
                    return Err(broker_error("AlterClientQuotas", error));
                }
                Ok(completed(entity_type, &name))
            }
            Self::AlterUserScram {
                entity_type,
                name,
                change,
            } => {
                let outcomes = match change {
                    None => Vec::new(),
                    Some(ScramChange::Upsert(credential)) => {
                        let upsertion = ScramUpsertion {
                            username: name.clone(),
                            password: credential.password.expose(),
                            iterations: credential.iterations,
                        };
                        match credential.mechanism {
                            Mechanism::Sha256 => {
                                client
                                    .alter_user_scram_credentials_sha256(&[upsertion], &[])
                                    .await?
                            }
                            Mechanism::Sha512 => {
                                client
                                    .alter_user_scram_credentials_sha512(&[upsertion], &[])
                                    .await?
                            }
                        }
                    }
                    Some(ScramChange::Delete(mechanism)) => {
                        let deletion = ScramDeletion {
                            username: name.clone(),
                        };
                        match mechanism {
                            Mechanism::Sha256 => {
                                client
                                    .alter_user_scram_credentials_sha256(&[], &[deletion])
                                    .await?
                            }
                            Mechanism::Sha512 => {
                                client
                                    .alter_user_scram_credentials_sha512(&[], &[deletion])
                                    .await?
                            }
                        }
                    }
                };
                if let Some(error) = outcomes.into_iter().find_map(|outcome| outcome.error) {
                    return Err(broker_error("AlterUserScramCredentials", error));
                }
                Ok(completed(entity_type, &name))
            }
        }
    }
}

/// `UNSUPPORTED_VERSION`.
const UNSUPPORTED_VERSION: i16 = 35;

/// `CLUSTER_AUTHORIZATION_FAILED`.
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// What `alterConfig` prints when the cluster does not support
/// `IncrementalAlterConfigs`.
const INCREMENTAL_ALTER_CONFIGS_UNSUPPORTED: &str = "The INCREMENTAL_ALTER_CONFIGS API is not supported by the cluster. The API is supported starting from version 2.3.0. You may want to use an older version of this tool to interact with your cluster, or upgrade your brokers to version 2.3.0 or newer to avoid this error.";

/// The config resource of an entity of the command line.
fn config_resource(entity_type: &str, name: &str) -> ConfigResource {
    let resource_type = match entity_type {
        TOPICS => ConfigResourceType::Topic,
        BROKERS => ConfigResourceType::Broker,
        BROKER_LOGGERS => ConfigResourceType::BrokerLogger,
        CLIENT_METRICS => ConfigResourceType::ClientMetrics,
        _ => ConfigResourceType::Group,
    };
    ConfigResource::new(resource_type, name)
}

/// The source of the configs that `--describe` without `--all` lists for a
/// resource, as `getResourceConfig` picks it. Broker loggers list every
/// entry.
fn dynamic_source(resource: &ConfigResource) -> Option<ConfigSource> {
    match resource.resource_type {
        ConfigResourceType::BrokerLogger => None,
        _ => resource.dynamic_config_source(),
    }
}

/// `alterResourceConfig`: one `IncrementalAlterConfigs` request that deletes
/// `deletes`, then sets `sets`. A delete carries the empty value, as the
/// `ConfigEntry(k, "")` that Kafka builds for it does.
async fn alter_resource(
    client: &mut AdminClient,
    resource: ConfigResource,
    deletes: Vec<String>,
    sets: Vec<(String, String)>,
) -> Result<(), KafkaError> {
    let ops = deletes
        .into_iter()
        .map(|name| AlterConfigOp {
            name,
            value: Some(String::new()),
            op_type: AlterConfigOpType::Delete,
        })
        .chain(
            sets.into_iter()
                .map(|(key, value)| AlterConfigOp::set(key, value)),
        )
        .collect::<Vec<_>>();
    let changes = BTreeMap::from([(resource, ops)]);
    let outcomes = client
        .incremental_alter_configs(&changes, IncrementalAlterConfigsOptions::default())
        .await
        .map_err(|error| admin_kafka_error(&error))?;
    outcomes
        .into_values()
        .find_map(Result::err)
        .map_or(Ok(()), Err)
}

/// The Kafka error of a call that failed as a whole.
fn admin_kafka_error(error: &AdminError) -> KafkaError {
    match error {
        AdminError::Broker {
            code,
            name,
            message,
            ..
        } => KafkaError {
            code: *code,
            name,
            message: message.clone(),
        },
        AdminError::Transport(ClientError::IncompatibleVersion { .. }) => KafkaError {
            code: UNSUPPORTED_VERSION,
            name: "UNSUPPORTED_VERSION",
            message: Some(error.to_string()),
        },
        other => KafkaError {
            code: -1,
            name: "UNKNOWN_SERVER_ERROR",
            message: Some(other.to_string()),
        },
    }
}

/// `getResourceConfig` up to its filter: the configs of one resource.
async fn resource_config(
    client: &AdminClient,
    resource: &ConfigResource,
    include_synonyms: bool,
) -> Result<Config, CommandError> {
    let options = DescribeConfigsOptions {
        include_synonyms,
        ..Default::default()
    };
    match client
        .describe_configs(std::slice::from_ref(resource), options)
        .await?
        .remove(resource)
    {
        Some(Ok(config)) => Ok(config),
        Some(Err(error)) => Err(broker_error("DescribeConfigs", error)),
        None => Err(format!("DescribeConfigs gave no result for {resource:?}").into()),
    }
}

/// The ops of `alterQuotaConfigs`: a set for each added config, in the
/// order of the configs, then a removal for each deleted one.
fn quota_ops(sets: &[(String, String)], deletes: &[String]) -> Result<Vec<QuotaOp>, String> {
    let mut ops = Vec::new();
    for (key, value) in sets {
        let value = java::parse_double(value)
            .ok_or_else(|| format!("Cannot parse quota configuration value for {key}: {value}"))?;
        ops.push(QuotaOp::Set {
            key: key.clone(),
            value,
        });
    }
    ops.extend(
        deletes
            .iter()
            .map(|key| QuotaOp::Remove { key: key.clone() }),
    );
    Ok(ops)
}

/// `ClientQuotaEntity.hashCode`: `Objects.hash(entries)`, where the hash of
/// a map is the sum of `key.hashCode() ^ value.hashCode()`, a null name
/// hashing to 0.
fn quota_entity_hash(entity: &ClientQuotaEntity) -> i32 {
    let entries = entity.iter().fold(0_i32, |sum, (entity_type, name)| {
        sum.wrapping_add(string_hash(entity_type) ^ name.as_deref().map_or(0, string_hash))
    });
    31_i32.wrapping_add(entries)
}

/// The entities of a describe in the order of the `HashMap` that
/// `DescribeClientQuotasResponse.complete` fills.
fn quota_order(quotas: &ClientQuotas) -> Vec<(&ClientQuotaEntity, &BTreeMap<String, f64>)> {
    hash_order(
        quotas.iter().collect(),
        Table::WithCapacity(quotas.len()),
        |(entity, _)| quota_entity_hash(entity),
    )
}

/// Every node id of the cluster, in the order of the `HashMap` that
/// `DescribeClusterResponse.nodes` collects.
async fn broker_ids(client: &AdminClient) -> Result<Vec<String>, CommandError> {
    let cluster = client
        .describe_cluster(DescribeClusterOptions::default())
        .await?;
    let ids = cluster.nodes.iter().map(|node| node.id).collect::<Vec<_>>();
    Ok(hash_order(ids, Table::Default, |id| integer_hash(*id))
        .into_iter()
        .map(|id| id.to_string())
        .collect())
}

/// Every topic name, internal ones included, in the order of the key set of
/// the `HashMap` that `listTopics` fills.
async fn topic_names(client: &AdminClient) -> Result<Vec<String>, CommandError> {
    let topics = client
        .list_topics(ListTopicsOptions {
            list_internal: true,
        })
        .await?;
    Ok(
        java::hash_map_order(topics.keys().map(String::as_str), None)
            .into_iter()
            .map(str::to_owned)
            .collect(),
    )
}

/// The names of the client-metrics subscriptions, in the order of the
/// answer.
async fn client_metrics_names(client: &AdminClient) -> Result<Vec<String>, CommandError> {
    let types = BTreeSet::from([ConfigResourceType::ClientMetrics]);
    Ok(client
        .list_config_resources(&types)
        .await?
        .into_iter()
        .map(|resource| resource.name)
        .collect())
}

/// Every group id that `listGroups().all` gives.
async fn group_ids(client: &AdminClient) -> Result<Vec<String>, CommandError> {
    let listed = client.list_groups(&ListGroupsOptions::default()).await?;
    Ok(listed
        .all()
        .map_err(|failure| broker_error("ListGroups", failure.error))?
        .into_iter()
        .map(|group| group.group_id)
        .collect())
}

/// `listGroupConfigResources`: the names of the group config resources, or
/// `None` for a broker that does not support listing them (KIP-1142) or
/// does not authorize it.
async fn group_config_names(client: &AdminClient) -> Result<Option<Vec<String>>, CommandError> {
    let types = BTreeSet::from([ConfigResourceType::Group]);
    match client.list_config_resources(&types).await {
        Ok(resources) => Ok(Some(
            resources
                .into_iter()
                .map(|resource| resource.name)
                .collect(),
        )),
        Err(error)
            if matches!(
                admin_kafka_error(&error).code,
                UNSUPPORTED_VERSION | CLUSTER_AUTHORIZATION_FAILED
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// Whether the entity that `--describe` names exists, as the checks of
/// `describeResourceConfig` decide before any config is read.
async fn entity_exists(
    client: &AdminClient,
    entity_type: &str,
    name: &str,
) -> Result<bool, CommandError> {
    Ok(match entity_type {
        TOPICS => topic_names(client).await?.iter().any(|topic| topic == name),
        BROKERS | BROKER_LOGGERS => {
            broker_ids(client).await?.iter().any(|id| id == name) || name.is_empty()
        }
        CLIENT_METRICS => client_metrics_names(client)
            .await?
            .iter()
            .any(|resource| resource == name),
        _ => {
            group_ids(client).await?.iter().any(|group| group == name)
                || !group_config_names(client)
                    .await?
                    .is_some_and(|names| names.iter().all(|resource| resource != name))
        }
    })
}

/// `describeResourceConfig`.
async fn describe_resources(
    client: &AdminClient,
    entity_type: &'static str,
    name: Option<String>,
    all: bool,
) -> Result<CommandResult, CommandError> {
    if !all
        && let Some(name) = &name
        && !entity_exists(client, entity_type, name).await?
    {
        return Ok(CommandResult::success(
            vec![render::missing(entity_type, name)],
            json!([{"entity_type": entity_type, "entity_name": name, "exists": false, "configs": []}]),
        ));
    }
    let entities = match name {
        Some(name) => vec![name],
        None => match entity_type {
            TOPICS => topic_names(client).await?,
            BROKERS | BROKER_LOGGERS => {
                let mut ids = broker_ids(client).await?;
                ids.push(String::new());
                ids
            }
            CLIENT_METRICS => client_metrics_names(client).await?,
            _ => {
                let mut ids = group_ids(client).await?;
                ids.extend(group_config_names(client).await?.unwrap_or_default());
                java::scala_set_order(ids)
            }
        },
    };
    let mut human = Vec::new();
    let mut data = Vec::new();
    for entity in entities {
        human.push(render::header(entity_type, &entity, all));
        let resource = config_resource(entity_type, &entity);
        let config = resource_config(client, &resource, true).await?;
        let source = if all { None } else { dynamic_source(&resource) };
        let configs = config
            .entries
            .values()
            .filter(|entry| source.is_none_or(|source| entry.source == source))
            .map(DescribedConfig::from)
            .collect::<Vec<_>>();
        human.extend(configs.iter().map(DescribedConfig::line));
        data.push(json!({
            "entity_type": entity_type,
            "entity_name": entity,
            "exists": true,
            "configs": configs.iter().map(DescribedConfig::json).collect::<Vec<_>>(),
        }));
    }
    Ok(CommandResult::success(human, data))
}

/// `describeClientQuotaAndUserScramCredentialConfigs` and
/// `describeQuotaConfigs`.
async fn describe_quotas(
    client: &mut AdminClient,
    components: Vec<ClientQuotaFilterComponent>,
    scram_users: Option<Vec<String>>,
) -> Result<CommandResult, CommandError> {
    let quotas = client
        .describe_client_quotas(&ClientQuotaFilter::contains_only(components))
        .await?;
    let ordered = quota_order(&quotas);
    let mut human = ordered
        .iter()
        .map(|(entity, values)| render::quota_line(entity, values))
        .collect::<Vec<_>>();
    let quota_data = ordered
        .iter()
        .map(|(entity, values)| render::quota_json(entity, values))
        .collect::<Vec<_>>();
    let mut scram_data = Vec::new();
    if let Some(users) = scram_users {
        let described = client.describe_user_scram_credentials(Some(&users)).await?;
        for user in render::scram_users(&described) {
            human.push(render::scram_line(user));
            scram_data.push(render::scram_json(user));
        }
    }
    Ok(CommandResult::success(
        human,
        json!({"quotas": quota_data, "scram_credentials": scram_data}),
    ))
}

#[cfg(test)]
mod tests;
