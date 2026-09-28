//! `krabka configs`, the counterpart of `kafka-configs`.
//!
//! The flags, the checks and their order, the messages and the stdout lines
//! are those of `ConfigCommand` in Kafka 4.3.1. A check that `kafka-configs`
//! makes on the command line is made here before any connection, so a
//! command that Kafka refuses is refused here with the same message.
//!
//! The pinned `krabka-client-admin` reads and changes the configs of topics
//! only, and the quotas of one named user only. Every other entity type,
//! `--all`, and the default-entity and every-entity quota forms pass the
//! command-line checks and then fail with a message that names the missing
//! admin call. See [`unsupported`].

mod java;
mod parse;
mod render;

use std::{
    collections::BTreeMap,
    net::{IpAddr, ToSocketAddrs as _},
    path::PathBuf,
};

use clap::{Arg, ArgAction, ArgMatches, Args, FromArgMatches};
use krabka_client_admin::{
    AdminClient, IncrementalAlterOp, KafkaError, QuotaOp, ScramDeletion, ScramUpsertion,
    diff_user_quotas,
};
use serde_json::{Value, json};

use self::{
    parse::{Mechanism, ScramCredential},
    render::DescribedConfig,
};
use crate::{
    connection::{ConnectionArgs, Properties},
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

/// What a checked command line asks for, with everything that the pinned
/// admin client can do.
#[derive(Debug, Clone, PartialEq)]
enum Plan {
    /// Describe the dynamic configs of one topic, or of every topic.
    DescribeTopics { name: Option<String> },
    /// Delete, then set, configs of one topic.
    AlterTopic {
        name: String,
        deletes: Vec<String>,
        sets: Vec<(String, String)>,
    },
    /// Describe the quotas and SCRAM credentials of one user.
    DescribeUser { name: String },
    /// Set and remove quotas of one user. The values are parsed after the
    /// current quotas are read, as Kafka does.
    AlterUserQuotas {
        name: String,
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

/// The message for a feature that needs an admin call that the pinned
/// `krabka-client-admin` does not have.
fn unsupported(subject: &str, needs: &str) -> String {
    format!(
        "{subject} is not supported by this build: it needs {needs}, which the pinned krabka-client-admin does not have"
    )
}

/// The admin calls that a config resource other than a topic needs.
const RESOURCE_CONFIG_CALLS: &str = "describe_configs and incremental_alter_configs for a config resource of any type (the pinned calls address topics only), and describe_cluster or list_config_resources to list the entities";

const CLIENT_QUOTA_CALLS: &str = "describe_client_quotas and alter_client_quotas for any client-quota entity (the pinned calls address one named user only)";

fn quota_unsupported(entity: &str) -> String {
    unsupported(&format!("the quotas of {entity}"), CLIENT_QUOTA_CALLS)
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
        let all = self.all.is_some();
        match head {
            TOPICS => {
                let name = names.first().cloned();
                if !all && let Some(name) = &name {
                    parse::topic_name(name)?;
                }
                if all {
                    return Err(unsupported(
                        "--describe --all",
                        "describe_configs that returns every config source, not only dynamic topic overrides",
                    ));
                }
                Ok(Plan::DescribeTopics { name })
            }
            BROKERS | BROKER_LOGGERS | CLIENT_METRICS | GROUPS => Err(unsupported(
                &format!("--entity-type {head}"),
                RESOURCE_CONFIG_CALLS,
            )),
            _ => {
                if names.len() > types.len() {
                    return Err("More entity names specified than entity types".into());
                }
                match (head, types.len(), names) {
                    (USERS, 1, [name]) if !name.is_empty() => {
                        Ok(Plan::DescribeUser { name: name.clone() })
                    }
                    (USERS, 1, []) => Err(quota_unsupported("every user")),
                    (USERS, 1, _) => Err(quota_unsupported("the default user")),
                    (IPS, _, _) => Err(quota_unsupported("an ip")),
                    (_, 2, _) => Err(quota_unsupported("a user's clients")),
                    _ => Err(quota_unsupported("a client")),
                }
            }
        }
    }

    /// The configs of `--add-config-file` and `--add-config`, keyed and in
    /// the iteration order of the `java.util.Properties` that
    /// `ConfigCommand.parseConfigsToBeAdded` fills, and checked by
    /// `validatePropsKey`.
    ///
    /// The order decides the order of the operations in the request and of
    /// the keys that an error message lists. It is exact for up to four keys.
    /// Above that Kafka copies the keys into a Scala hash map, whose order
    /// this does not model.
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
        Ok(order
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
            TOPICS => Ok(Plan::AlterTopic {
                name,
                deletes,
                sets: adds,
            }),
            BROKERS => {
                if !name.is_empty() {
                    java::parse_int(&name).map_err(|_| {
                        format!(
                            "The entity name for {head} must be a valid integer broker id, found: {name}"
                        )
                    })?;
                }
                Err(unsupported("--entity-type brokers", RESOURCE_CONFIG_CALLS))
            }
            BROKER_LOGGERS | CLIENT_METRICS | GROUPS => Err(unsupported(
                &format!("--entity-type {head}"),
                RESOURCE_CONFIG_CALLS,
            )),
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
                Err(quota_unsupported("an ip"))
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
            return if types.len() == 2 {
                Err(quota_unsupported("a user's clients"))
            } else if head == CLIENTS {
                Err(quota_unsupported("a client"))
            } else if name.is_empty() {
                Err(quota_unsupported("the default user"))
            } else {
                Ok(Plan::AlterUserQuotas {
                    name,
                    sets: adds,
                    deletes,
                })
            };
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
            Self::DescribeTopics { name } => describe_topics(client, name).await,
            Self::AlterTopic {
                name,
                deletes,
                sets,
            } => {
                let ops = deletes
                    .into_iter()
                    .map(|key| IncrementalAlterOp::Delete {
                        topic: name.clone(),
                        key,
                    })
                    .chain(
                        sets.into_iter()
                            .map(|(key, value)| IncrementalAlterOp::Set {
                                topic: name.clone(),
                                key,
                                value,
                            }),
                    )
                    .collect::<Vec<_>>();
                let outcomes = client.incremental_alter_configs(&ops).await?;
                if let Some(error) = outcomes.into_iter().find_map(|outcome| outcome.error) {
                    return Err(broker_error("IncrementalAlterConfigs", error));
                }
                Ok(completed(TOPICS, &name))
            }
            Self::DescribeUser { name } => describe_user(client, name).await,
            Self::AlterUserQuotas {
                name,
                sets,
                deletes,
            } => {
                let current = client.describe_user_quotas(&name).await?;
                let invalid = deletes
                    .iter()
                    .filter(|key| !current.contains_key(key.as_str()))
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                if !invalid.is_empty() {
                    return Err(format!("Invalid config(s): {}", invalid.join(",")).into());
                }
                let ops = quota_ops(&current, &sets, &deletes)?;
                if let Some(error) = client.alter_user_quotas(&name, &ops, false).await? {
                    return Err(broker_error("AlterClientQuotas", error));
                }
                Ok(completed(USERS, &name))
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

/// The quota changes for a user whose quotas are `current`: the `QuotaOp`
/// list that `diff_user_quotas` computes from `current` to the quotas after
/// `sets` and `deletes`.
fn quota_ops(
    current: &krabka_client_admin::UserQuotaConfig,
    sets: &[(String, String)],
    deletes: &[String],
) -> Result<Vec<QuotaOp>, String> {
    let mut desired = current.clone();
    for (key, value) in sets {
        let parsed = java::parse_double(value)
            .ok_or_else(|| format!("Cannot parse quota configuration value for {key}: {value}"))?;
        desired.insert(key.clone(), parsed);
    }
    for key in deletes {
        desired.remove(key);
    }
    Ok(diff_user_quotas(current, &desired))
}

async fn describe_topics(
    client: &mut AdminClient,
    name: Option<String>,
) -> Result<CommandResult, CommandError> {
    let metadata = client.metadata(&[]).await?;
    let topics = metadata
        .topics
        .iter()
        .filter(|topic| topic.error.is_none())
        .map(|topic| topic.name.as_str())
        .collect::<Vec<_>>();
    let entities = match &name {
        Some(name) if !topics.contains(&name.as_str()) => {
            return Ok(CommandResult::success(
                vec![render::missing(TOPICS, name)],
                json!([{"entity_type": TOPICS, "entity_name": name, "exists": false, "configs": []}]),
            ));
        }
        Some(name) => vec![name.clone()],
        None => java::hash_map_order(topics.iter().copied(), None)
            .into_iter()
            .map(str::to_owned)
            .collect(),
    };
    let mut human = Vec::new();
    let mut data = Vec::new();
    for entity in entities {
        let configs = client
            .describe_configs(&[&entity])
            .await?
            .first()
            .map(render::from_overrides)
            .unwrap_or_default();
        human.push(render::header(TOPICS, &entity, false));
        human.extend(configs.iter().map(DescribedConfig::line));
        data.push(json!({
            "entity_type": TOPICS,
            "entity_name": entity,
            "exists": true,
            "configs": configs.iter().map(DescribedConfig::json).collect::<Vec<_>>(),
        }));
    }
    Ok(CommandResult::success(human, data))
}

async fn describe_user(
    client: &mut AdminClient,
    name: String,
) -> Result<CommandResult, CommandError> {
    let quotas = client.describe_user_quotas(&name).await?;
    let users = client
        .describe_user_scram_credentials(Some(std::slice::from_ref(&name)))
        .await?;
    let mut human = render::quota_line(&name, &quotas)
        .into_iter()
        .collect::<Vec<_>>();
    human.extend(users.iter().filter_map(render::scram_line));
    let scram = users
        .iter()
        .find(|user| user.username == name && render::scram_line(user).is_some())
        .map_or(Value::Null, render::scram_json);
    Ok(CommandResult::success(
        human,
        json!([{
            "entity_type": USERS,
            "entity_name": name,
            "quotas": render::quota_json(&quotas),
            "scram_credentials": scram,
        }]),
    ))
}

#[cfg(test)]
mod tests;
