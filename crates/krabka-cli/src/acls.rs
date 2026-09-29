//! `krabka acls`, the counterpart of `kafka-acls`.
//!
//! The flags, the checks on them, the messages of those checks and the human
//! output are those of `AclCommand` in Apache Kafka 4.3.1, so an existing
//! `kafka-acls` invocation runs unchanged. Three things differ on purpose:
//!
//! - A refused command line exits [`Exit::Usage`](crate::exit::Exit::Usage),
//!   where the JVM tool exits 1.
//! - `--remove` asks once, on stderr, for every resource filter, through
//!   [`confirm`]. `--force`, as in Kafka, and `--yes` answer it. A stdin that
//!   is not a terminal refuses instead of asking.
//! - The JVM tool prints resources and entries in hash order. This command
//!   prints them sorted.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
};

use clap::Args;
use krabka_client_admin::{self as admin, AclEntry, AclEntryFilter, AdminClient, KafkaError};
use serde_json::{Value, json};

use crate::{
    connection::ConnectionArgs,
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, Refusal, confirm},
};

/// The name of the one cluster resource, `Resource.CLUSTER_NAME` in Kafka.
const CLUSTER_NAME: &str = "kafka-cluster";

/// The host of an entry that applies from every host.
const WILDCARD_HOST: &str = "*";

/// The action flags. Exactly one is required.
#[derive(Debug, Args, Clone, Copy)]
struct ActionArgs {
    /// Indicates you are trying to add ACLs.
    #[arg(long)]
    add: bool,
    /// Indicates you are trying to remove ACLs.
    #[arg(long)]
    remove: bool,
    /// List ACLs for the specified resource, use --topic <topic> or --group
    /// <group> or --cluster to specify a resource.
    #[arg(long)]
    list: bool,
}

/// The convenience flags that expand to the operations of a client role.
#[derive(Debug, Args, Clone, Copy)]
struct RoleArgs {
    /// Convenience option to add/remove ACLs for producer role. This will
    /// generate ACLs that allows WRITE,DESCRIBE and CREATE on topic.
    #[arg(long)]
    producer: bool,
    /// Convenience option to add/remove ACLs for consumer role. This will
    /// generate ACLs that allows READ,DESCRIBE on topic and READ on group.
    #[arg(long)]
    consumer: bool,
    /// Enable idempotence for the producer. This should be used in combination
    /// with the --producer option. Note that idempotence is enabled
    /// automatically if the producer is authorized to a particular
    /// transactional-id.
    #[arg(long)]
    idempotent: bool,
}

/// The flags of `kafka-acls`.
#[derive(Debug, Args)]
#[command(args_override_self = true)]
pub struct AclsArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    #[command(flatten)]
    action: ActionArgs,
    /// topic to which ACLs should be added or removed. A value of '*'
    /// indicates ACL should apply to all topics.
    #[arg(long, value_name = "topic")]
    topic: Vec<String>,
    /// Add/Remove cluster ACLs.
    #[arg(long)]
    cluster: bool,
    /// Consumer Group to which the ACLs should be added or removed. A value of
    /// '*' indicates the ACLs should apply to all groups.
    #[arg(long, value_name = "group")]
    group: Vec<String>,
    /// The transactionalId to which ACLs should be added or removed. A value
    /// of '*' indicates the ACLs should apply to all transactionalIds.
    #[arg(long, value_name = "transactional-id")]
    transactional_id: Vec<String>,
    /// Delegation token to which ACLs should be added or removed. A value of
    /// '*' indicates ACL should apply to all tokens.
    #[arg(long, value_name = "delegation-token")]
    delegation_token: Vec<String>,
    #[arg(
        long,
        value_name = "user-principal",
        help = "Specifies a user principal as a resource in relation with the operation. For \
                instance one could grant CreateTokens or DescribeTokens permission on a given \
                user principal."
    )]
    user_principal: Vec<String>,
    /// The type of the resource pattern or pattern filter. When adding acls,
    /// this should be a specific pattern type, e.g. 'literal' or 'prefixed'.
    /// When listing or removing acls, a specific pattern type can be used to
    /// list or remove acls from specific resource patterns, or use the filter
    /// values of 'any' or 'match', where 'any' will match any pattern type,
    /// but will match the resource name exactly, where as 'match' will perform
    /// pattern matching to list or remove all acls that affect the supplied
    /// resource(s). WARNING: 'match', when used in combination with the
    /// '--remove' switch, should be used with care.
    #[arg(
        long,
        value_name = "ANY|MATCH|LITERAL|PREFIXED",
        default_value = "LITERAL"
    )]
    resource_pattern_type: String,
    #[arg(
        long,
        help = "Operation that is being allowed or denied. Valid operation names are: All, Read, \
                Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, \
                AlterConfigs, IdempotentWrite, CreateTokens, DescribeTokens, TwoPhaseCommit \
                [default: All]"
    )]
    operation: Vec<String>,
    /// principal is in principalType:name format. Note that principalType must
    /// be supported by the Authorizer being used. For example, User:'*' is the
    /// wild card indicating all users.
    #[arg(long, value_name = "allow-principal")]
    allow_principal: Vec<String>,
    /// principal is in principalType:name format. By default anyone not added
    /// through --allow-principal is denied access. You only need to use this
    /// option as negation to already allowed set. Note that principalType must
    /// be supported by the Authorizer being used. AND PLEASE REMEMBER DENY
    /// RULES TAKES PRECEDENCE OVER ALLOW RULES.
    #[arg(long, value_name = "deny-principal")]
    deny_principal: Vec<String>,
    /// List ACLs for the specified principal. principal is in
    /// principalType:name format. Note that principalType must be supported by
    /// the Authorizer being used. Multiple --principal option can be passed.
    #[arg(long, value_name = "principal", num_args = 0..=1)]
    principal: Option<Vec<String>>,
    /// Host from which principals listed in --allow-principal will have
    /// access. If you have specified --allow-principal then the default for
    /// this option will be set to '*' which allows access from all hosts.
    #[arg(long, value_name = "allow-host")]
    allow_host: Vec<String>,
    /// Host from which principals listed in --deny-principal will be denied
    /// access. If you have specified --deny-principal then the default for
    /// this option will be set to '*' which denies access from all hosts.
    #[arg(long, value_name = "deny-host")]
    deny_host: Vec<String>,
    #[command(flatten)]
    roles: RoleArgs,
    /// Assume Yes to all queries and do not prompt.
    #[arg(long)]
    force: bool,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// A resource type, `ResourceType` in Kafka less its filter values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
    User,
}

impl ResourceType {
    #[cfg(test)]
    const ALL: [Self; 6] = [
        Self::Topic,
        Self::Group,
        Self::Cluster,
        Self::TransactionalId,
        Self::DelegationToken,
        Self::User,
    ];

    /// The Java enum name, which Kafka prints.
    const fn name(self) -> &'static str {
        match self {
            Self::Topic => "TOPIC",
            Self::Group => "GROUP",
            Self::Cluster => "CLUSTER",
            Self::TransactionalId => "TRANSACTIONAL_ID",
            Self::DelegationToken => "DELEGATION_TOKEN",
            Self::User => "USER",
        }
    }

    /// The operations an entry on this resource type can name besides
    /// [`Operation::All`], as `AclEntry.supportedOperations` lists them.
    const fn operations(self) -> &'static [Operation] {
        use Operation::{
            Alter, AlterConfigs, ClusterAction, Create, CreateTokens, Delete, Describe,
            DescribeConfigs, DescribeTokens, IdempotentWrite, Read, TwoPhaseCommit, Write,
        };
        match self {
            Self::Topic => &[
                Read,
                Write,
                Create,
                Describe,
                Delete,
                Alter,
                DescribeConfigs,
                AlterConfigs,
            ],
            Self::Group => &[Read, Describe, Delete, DescribeConfigs, AlterConfigs],
            Self::Cluster => &[
                Create,
                ClusterAction,
                DescribeConfigs,
                AlterConfigs,
                IdempotentWrite,
                Alter,
                Describe,
            ],
            Self::TransactionalId => &[Describe, Write, TwoPhaseCommit],
            Self::DelegationToken => &[Describe],
            Self::User => &[CreateTokens, DescribeTokens],
        }
    }

    const fn to_wire(self) -> admin::ResourceType {
        match self {
            Self::Topic => admin::ResourceType::Topic,
            Self::Group => admin::ResourceType::Group,
            Self::Cluster => admin::ResourceType::Cluster,
            Self::TransactionalId => admin::ResourceType::TransactionalId,
            Self::DelegationToken => admin::ResourceType::DelegationToken,
            Self::User => admin::ResourceType::User,
        }
    }

    const fn from_wire(value: admin::ResourceType) -> Self {
        match value {
            admin::ResourceType::Topic => Self::Topic,
            admin::ResourceType::Group => Self::Group,
            admin::ResourceType::Cluster => Self::Cluster,
            admin::ResourceType::TransactionalId => Self::TransactionalId,
            admin::ResourceType::DelegationToken => Self::DelegationToken,
            admin::ResourceType::User => Self::User,
        }
    }
}

/// A resource pattern type, `PatternType` in Kafka less `UNKNOWN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PatternType {
    Any,
    Match,
    Literal,
    Prefixed,
}

impl PatternType {
    const ALL: [Self; 4] = [Self::Any, Self::Match, Self::Literal, Self::Prefixed];

    /// The Java enum name, which Kafka prints.
    const fn name(self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::Match => "MATCH",
            Self::Literal => "LITERAL",
            Self::Prefixed => "PREFIXED",
        }
    }

    /// Parses `--resource-pattern-type` as jopt-simple's `EnumConverter`
    /// does: the enum name, in any case.
    fn parse(value: &str) -> Result<Self, CommandError> {
        Self::ALL
            .into_iter()
            .find(|pattern| pattern.name().eq_ignore_ascii_case(value))
            .ok_or_else(|| {
                usage(format!(
                    "Cannot parse argument '{value}' of option resource-pattern-type"
                ))
            })
    }

    /// Whether the type names a concrete pattern rather than a filter.
    const fn is_specific(self) -> bool {
        matches!(self, Self::Literal | Self::Prefixed)
    }

    /// The filter value, where `None` is the wire `ANY`.
    const fn to_wire_filter(self) -> Option<admin::PatternType> {
        match self {
            Self::Any => None,
            Self::Match => Some(admin::PatternType::Match),
            Self::Literal => Some(admin::PatternType::Literal),
            Self::Prefixed => Some(admin::PatternType::Prefixed),
        }
    }

    /// The pattern type of a stored ACL, which only a specific type names.
    fn to_wire(self) -> Result<admin::PatternType, CommandError> {
        match self {
            Self::Literal => Ok(admin::PatternType::Literal),
            Self::Prefixed => Ok(admin::PatternType::Prefixed),
            Self::Any | Self::Match => Err(CommandError::Other(format!(
                "a {} pattern type does not name a concrete resource pattern",
                self.name()
            ))),
        }
    }

    const fn from_wire(value: admin::PatternType) -> Self {
        match value {
            admin::PatternType::Literal => Self::Literal,
            admin::PatternType::Prefixed => Self::Prefixed,
            admin::PatternType::Match => Self::Match,
        }
    }
}

/// An operation, `AclOperation` in Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Operation {
    Unknown,
    Any,
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
    CreateTokens,
    DescribeTokens,
    TwoPhaseCommit,
}

impl Operation {
    const ALL: [Self; 16] = [
        Self::Unknown,
        Self::Any,
        Self::All,
        Self::Read,
        Self::Write,
        Self::Create,
        Self::Delete,
        Self::Alter,
        Self::Describe,
        Self::ClusterAction,
        Self::DescribeConfigs,
        Self::AlterConfigs,
        Self::IdempotentWrite,
        Self::CreateTokens,
        Self::DescribeTokens,
        Self::TwoPhaseCommit,
    ];

    /// The Java enum name, which Kafka prints.
    const fn name(self) -> &'static str {
        match self {
            Self::Unknown => "UNKNOWN",
            Self::Any => "ANY",
            Self::All => "ALL",
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Create => "CREATE",
            Self::Delete => "DELETE",
            Self::Alter => "ALTER",
            Self::Describe => "DESCRIBE",
            Self::ClusterAction => "CLUSTER_ACTION",
            Self::DescribeConfigs => "DESCRIBE_CONFIGS",
            Self::AlterConfigs => "ALTER_CONFIGS",
            Self::IdempotentWrite => "IDEMPOTENT_WRITE",
            Self::CreateTokens => "CREATE_TOKENS",
            Self::DescribeTokens => "DESCRIBE_TOKENS",
            Self::TwoPhaseCommit => "TWO_PHASE_COMMIT",
        }
    }

    /// Parses an `--operation` value as `SecurityUtils.operation` does: the
    /// Pascal-case name, such as `DescribeConfigs`, or that name in any case.
    /// A value that names no operation is [`Operation::Unknown`], which the
    /// resource-type check then refuses.
    fn parse(value: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|operation| pascal_case(operation.name()).eq_ignore_ascii_case(value))
            .unwrap_or(Self::Unknown)
    }

    fn to_wire(self) -> Result<admin::AclOperation, CommandError> {
        match self {
            Self::All => Ok(admin::AclOperation::All),
            Self::Read => Ok(admin::AclOperation::Read),
            Self::Write => Ok(admin::AclOperation::Write),
            Self::Create => Ok(admin::AclOperation::Create),
            Self::Delete => Ok(admin::AclOperation::Delete),
            Self::Alter => Ok(admin::AclOperation::Alter),
            Self::Describe => Ok(admin::AclOperation::Describe),
            Self::ClusterAction => Ok(admin::AclOperation::ClusterAction),
            Self::DescribeConfigs => Ok(admin::AclOperation::DescribeConfigs),
            Self::AlterConfigs => Ok(admin::AclOperation::AlterConfigs),
            Self::IdempotentWrite => Ok(admin::AclOperation::IdempotentWrite),
            Self::CreateTokens => Ok(admin::AclOperation::CreateTokens),
            Self::DescribeTokens => Ok(admin::AclOperation::DescribeTokens),
            Self::TwoPhaseCommit => Ok(admin::AclOperation::TwoPhaseCommit),
            Self::Unknown | Self::Any => Err(CommandError::Other(format!(
                "operation {} does not name a concrete operation",
                self.name()
            ))),
        }
    }

    const fn from_wire(value: admin::AclOperation) -> Self {
        match value {
            admin::AclOperation::All => Self::All,
            admin::AclOperation::Read => Self::Read,
            admin::AclOperation::Write => Self::Write,
            admin::AclOperation::Create => Self::Create,
            admin::AclOperation::Delete => Self::Delete,
            admin::AclOperation::Alter => Self::Alter,
            admin::AclOperation::Describe => Self::Describe,
            admin::AclOperation::ClusterAction => Self::ClusterAction,
            admin::AclOperation::DescribeConfigs => Self::DescribeConfigs,
            admin::AclOperation::AlterConfigs => Self::AlterConfigs,
            admin::AclOperation::IdempotentWrite => Self::IdempotentWrite,
            admin::AclOperation::TwoPhaseCommit => Self::TwoPhaseCommit,
            admin::AclOperation::CreateTokens => Self::CreateTokens,
            admin::AclOperation::DescribeTokens => Self::DescribeTokens,
        }
    }
}

/// A permission type, `AclPermissionType` in Kafka less its filter values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Permission {
    Allow,
    Deny,
}

impl Permission {
    #[cfg(test)]
    const ALL: [Self; 2] = [Self::Allow, Self::Deny];

    /// The Java enum name, which Kafka prints.
    const fn name(self) -> &'static str {
        match self {
            Self::Allow => "ALLOW",
            Self::Deny => "DENY",
        }
    }

    const fn to_wire(self) -> admin::PermissionType {
        match self {
            Self::Allow => admin::PermissionType::Allow,
            Self::Deny => admin::PermissionType::Deny,
        }
    }

    const fn from_wire(value: admin::PermissionType) -> Self {
        match value {
            admin::PermissionType::Allow => Self::Allow,
            admin::PermissionType::Deny => Self::Deny,
        }
    }
}

/// `SecurityUtils.toPascalCase`: `DESCRIBE_CONFIGS` becomes `DescribeConfigs`.
fn pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalize = true;
    for c in name.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            out.extend(c.to_uppercase());
            capitalize = false;
        } else {
            out.extend(c.to_lowercase());
        }
    }
    out
}

/// A resource pattern, or with a filter pattern type a resource pattern
/// filter: `ResourcePattern` and `ResourcePatternFilter` in Kafka, which print
/// the same way.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Resource {
    kind: ResourceType,
    name: String,
    pattern_type: PatternType,
}

impl Resource {
    fn cluster() -> Self {
        Self {
            kind: ResourceType::Cluster,
            name: CLUSTER_NAME.into(),
            pattern_type: PatternType::Literal,
        }
    }

    /// The filter that matches the resource and any entry on it.
    fn wire_filter(&self) -> AclEntryFilter {
        AclEntryFilter {
            resource_type: Some(self.kind.to_wire()),
            resource_name: Some(self.name.clone()),
            pattern_type: self.pattern_type.to_wire_filter(),
            ..AclEntryFilter::default()
        }
    }

    fn json(&self) -> Value {
        json!({
            "resource_type": self.kind.name(),
            "name": self.name,
            "pattern_type": self.pattern_type.name(),
        })
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResourcePattern(resourceType={}, name={}, patternType={})",
            self.kind.name(),
            self.name,
            self.pattern_type.name()
        )
    }
}

/// An access-control entry, `AccessControlEntry` in Kafka.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    principal: String,
    host: String,
    operation: Operation,
    permission: Permission,
}

impl Entry {
    /// The binding of the entry to `resource`, which must be a concrete
    /// pattern.
    fn wire(&self, resource: &Resource) -> Result<AclEntry, CommandError> {
        Ok(AclEntry {
            resource_type: resource.kind.to_wire(),
            resource_name: resource.name.clone(),
            pattern_type: resource.pattern_type.to_wire()?,
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation.to_wire()?,
            permission_type: self.permission.to_wire(),
        })
    }

    /// `resource` narrowed to this exact entry.
    fn wire_filter(&self, resource: &AclEntryFilter) -> Result<AclEntryFilter, CommandError> {
        Ok(AclEntryFilter {
            principal: Some(self.principal.clone()),
            host: Some(self.host.clone()),
            operation: Some(self.operation.to_wire()?),
            permission_type: Some(self.permission.to_wire()),
            ..resource.clone()
        })
    }

    fn json(&self) -> Value {
        json!({
            "principal": self.principal,
            "host": self.host,
            "operation": self.operation.name(),
            "permission_type": self.permission.name(),
        })
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(principal={}, host={}, operation={}, permissionType={})",
            self.principal,
            self.host,
            self.operation.name(),
            self.permission.name()
        )
    }
}

/// Splits a binding that the broker returned into its resource and entry.
fn binding(entry: AclEntry) -> (Resource, Entry) {
    (
        Resource {
            kind: ResourceType::from_wire(entry.resource_type),
            name: entry.resource_name,
            pattern_type: PatternType::from_wire(entry.pattern_type),
        },
        Entry {
            principal: entry.principal,
            host: entry.host,
            operation: Operation::from_wire(entry.operation),
            permission: Permission::from_wire(entry.permission_type),
        },
    )
}

/// Entries by resource. An empty set on a filter means every entry.
type Acls = BTreeMap<Resource, BTreeSet<Entry>>;

/// What the command line asks for, after every check that Kafka makes on it.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    List {
        filters: BTreeSet<Resource>,
        principals: Option<Vec<String>>,
    },
    Add(Acls),
    Remove(Acls),
}

fn usage(message: impl Into<String>) -> CommandError {
    CommandError::Usage(message.into())
}

/// `CommandLineUtils.checkInvalidArgs`: `used`, when given, refuses the first
/// of `invalid` that is also given.
fn check_invalid(used: (&str, bool), invalid: &[(&str, bool)]) -> Result<(), CommandError> {
    match invalid.iter().find(|(_, given)| *given) {
        Some((name, _)) if used.1 => Err(usage(format!(
            "Option \"[{}]\" can't be used with option \"[{name}]\"",
            used.0
        ))),
        _ => Ok(()),
    }
}

/// `SecurityUtils.parseKafkaPrincipal`, which keeps the principal as given.
fn principals(values: &[String]) -> Result<Vec<String>, CommandError> {
    let mut out: Vec<String> = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if !value.contains(':') {
            return Err(usage(format!(
                "expected a string in format principalType:principalName but got {value}"
            )));
        }
        if !out.iter().any(|seen| seen == value) {
            out.push(value.to_owned());
        }
    }
    Ok(out)
}

/// The hosts of `--allow-host` or `--deny-host`: the values given, else `*`
/// when the matching principal flag is given, else none.
fn hosts(values: &[String], principal_given: bool) -> Vec<String> {
    if !values.is_empty() {
        values.iter().map(|host| host.trim().to_owned()).collect()
    } else if principal_given {
        vec![WILDCARD_HOST.into()]
    } else {
        Vec::new()
    }
}

impl AclsArgs {
    /// Runs the command, asking on the terminal before `--remove`.
    ///
    /// # Errors
    /// Returns the refused command line, a declined confirmation, or the
    /// failure of the first request.
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let yes = self.confirm.yes || self.force;
        Box::pin(self.execute(move |impact| confirm(yes, "krabka acls", impact))).await
    }

    /// [`Self::run`], with the confirmation passed in.
    async fn execute<F, Fut>(self, confirm: F) -> Result<CommandResult, CommandError>
    where
        F: FnOnce(Impact) -> Fut,
        Fut: Future<Output = Result<(), Refusal>>,
    {
        let dry_run = self.confirm.dry_run;
        match self.plan()? {
            Plan::List {
                filters,
                principals,
            } => {
                let requests = list_requests(&filters);
                let mut client = self.connection.connect("acls").await?;
                let mut acls = Acls::new();
                for request in &requests {
                    for entry in client.describe_acls(request).await? {
                        let (resource, entry) = binding(entry);
                        acls.entry(resource).or_default().insert(entry);
                    }
                }
                Ok(listed(&acls, principals.as_deref()))
            }
            Plan::Add(acls) => {
                let steps = add_steps(acls)?;
                let mut client = self.connection.connect("acls").await?;
                add(&mut client, steps, dry_run).await
            }
            Plan::Remove(acls) => {
                let steps = remove_steps(acls)?;
                if !dry_run {
                    confirm(removal_impact(&steps)).await?;
                }
                let mut client = self.connection.connect("acls").await?;
                remove(&mut client, steps, dry_run).await
            }
        }
    }

    /// Checks the command line as `AclCommandOptions.checkArgs` and the
    /// action methods of `AclCommand` do, and resolves what it asks for.
    fn plan(&self) -> Result<Plan, CommandError> {
        if self.connection.bootstrap_server.is_empty()
            && self.connection.bootstrap_controller.is_empty()
        {
            return Err(usage(
                "One of --bootstrap-server or --bootstrap-controller must be specified",
            ));
        }
        let ActionArgs { add, remove, list } = self.action;
        if [add, remove, list]
            .into_iter()
            .filter(|given| *given)
            .count()
            != 1
        {
            return Err(usage(
                "Command must include exactly one action: --list, --add, --remove. ",
            ));
        }
        let RoleArgs {
            producer,
            consumer,
            idempotent,
        } = self.roles;
        let allow_host = ("allow-host", !self.allow_host.is_empty());
        let allow_principal = ("allow-principal", !self.allow_principal.is_empty());
        let deny_host = ("deny-host", !self.deny_host.is_empty());
        let deny_principal = ("deny-principal", !self.deny_principal.is_empty());
        let operation = ("operation", !self.operation.is_empty());
        check_invalid(
            ("list", list),
            &[
                ("producer", producer),
                ("consumer", consumer),
                allow_host,
                allow_principal,
                deny_host,
                deny_principal,
            ],
        )?;
        for role in [("producer", producer), ("consumer", consumer)] {
            check_invalid(role, &[operation, deny_principal, deny_host])?;
        }
        if self.principal.is_some() && !list {
            return Err(usage(
                "The --principal option is only available if --list is set",
            ));
        }
        if producer && self.topic.is_empty() {
            return Err(usage("With --producer you must specify a --topic"));
        }
        if idempotent && !producer {
            return Err(usage(
                "The --idempotent option is only available if --producer is set",
            ));
        }
        if consumer
            && (self.topic.is_empty()
                || self.group.is_empty()
                || (!producer && (self.cluster || !self.transactional_id.is_empty())))
        {
            return Err(usage(
                "With --consumer you must specify a --topic and a --group and no --cluster or \
                 --transactional-id option should be specified.",
            ));
        }
        if list && self.confirm.dry_run {
            return Err(usage("--dry-run is only valid with --add or --remove"));
        }
        let pattern = PatternType::parse(&self.resource_pattern_type)?;
        if list {
            return Ok(Plan::List {
                filters: self.resource_filters(pattern, false)?,
                principals: self.principal.as_deref().map(principals).transpose()?,
            });
        }
        if remove {
            return Ok(Plan::Remove(self.resource_acls(pattern)?));
        }
        if !pattern.is_specific() {
            return Err(usage(format!(
                "A '--resource-pattern-type' value of '{}' is not valid when adding acls.",
                pattern.name()
            )));
        }
        let acls = self.resource_acls(pattern)?;
        if acls.values().any(BTreeSet::is_empty) {
            return Err(usage(
                "You must specify one of: --allow-principal, --deny-principal when trying to add \
                 ACLs.",
            ));
        }
        Ok(Plan::Add(acls))
    }

    /// `getResourceFilterToAcls`: the entries of each resource, from the
    /// explicit flags or from the `--producer` and `--consumer` expansions.
    fn resource_acls(&self, pattern: PatternType) -> Result<Acls, CommandError> {
        use Operation::{Create, Describe, IdempotentWrite, Read, Write};
        let RoleArgs {
            producer,
            consumer,
            idempotent,
        } = self.roles;
        let mut acls = Acls::new();
        if !producer && !consumer {
            let operations = if self.operation.is_empty() {
                vec![Operation::All]
            } else {
                self.operation
                    .iter()
                    .map(|operation| Operation::parse(operation.trim()))
                    .collect()
            };
            let entries = self.entries(&operations)?;
            for resource in self.resource_filters(pattern, true)? {
                acls.insert(resource, entries.clone());
            }
        }
        if producer {
            let topic = self.entries(&[Write, Describe, Create])?;
            let transactional_id = self.entries(&[Write, Describe])?;
            for resource in self.resource_filters(pattern, true)? {
                let entries = match resource.kind {
                    ResourceType::Topic => &topic,
                    ResourceType::TransactionalId => &transactional_id,
                    _ => continue,
                };
                acls.insert(resource, entries.clone());
            }
            if idempotent {
                acls.insert(Resource::cluster(), self.entries(&[IdempotentWrite])?);
            }
        }
        if consumer {
            let topic = self.entries(&[Read, Describe])?;
            let group = self.entries(&[Read])?;
            for resource in self.resource_filters(pattern, true)? {
                let entries = match resource.kind {
                    ResourceType::Topic => &topic,
                    ResourceType::Group => &group,
                    _ => continue,
                };
                acls.entry(resource)
                    .or_default()
                    .extend(entries.iter().cloned());
            }
        }
        for (resource, entries) in &acls {
            let valid = resource.kind.operations();
            if entries
                .iter()
                .any(|entry| entry.operation != Operation::All && !valid.contains(&entry.operation))
            {
                let names = valid
                    .iter()
                    .chain([&Operation::All])
                    .map(|operation| operation.name())
                    .collect::<Vec<_>>();
                return Err(usage(format!(
                    "ResourceType {} only supports operations [{}]",
                    resource.kind.name(),
                    names.join(", ")
                )));
            }
        }
        Ok(acls)
    }

    /// `getAcl`: every allowed principal from every allowed host, and every
    /// denied principal from every denied host, for each of `operations`.
    fn entries(&self, operations: &[Operation]) -> Result<BTreeSet<Entry>, CommandError> {
        let allowed = principals(&self.allow_principal)?;
        let denied = principals(&self.deny_principal)?;
        let allowed_hosts = hosts(&self.allow_host, !self.allow_principal.is_empty());
        let denied_hosts = hosts(&self.deny_host, !self.deny_principal.is_empty());
        let mut entries = BTreeSet::new();
        for (principals, hosts, permission) in [
            (allowed, allowed_hosts, Permission::Allow),
            (denied, denied_hosts, Permission::Deny),
        ] {
            for principal in &principals {
                for &operation in operations {
                    if operation == Operation::Any {
                        return Err(usage("operation must not be ANY"));
                    }
                    for host in &hosts {
                        entries.insert(Entry {
                            principal: principal.clone(),
                            host: host.clone(),
                            operation,
                            permission,
                        });
                    }
                }
            }
        }
        Ok(entries)
    }

    /// `getResourceFilter`: the resources that the selector flags name.
    /// `--cluster` counts only with a literal pattern type, as in Kafka.
    fn resource_filters(
        &self,
        pattern: PatternType,
        required: bool,
    ) -> Result<BTreeSet<Resource>, CommandError> {
        let named = |kind, names: &[String], trim: bool| {
            names
                .iter()
                .map(|name| Resource {
                    kind,
                    name: if trim { name.trim() } else { name }.to_owned(),
                    pattern_type: pattern,
                })
                .collect::<Vec<_>>()
        };
        let mut filters = BTreeSet::new();
        filters.extend(named(ResourceType::Topic, &self.topic, true));
        if pattern == PatternType::Literal && (self.cluster || self.roles.idempotent) {
            filters.insert(Resource::cluster());
        }
        filters.extend(named(ResourceType::Group, &self.group, true));
        filters.extend(named(
            ResourceType::TransactionalId,
            &self.transactional_id,
            false,
        ));
        filters.extend(named(
            ResourceType::DelegationToken,
            &self.delegation_token,
            true,
        ));
        filters.extend(named(ResourceType::User, &self.user_principal, true));
        if filters.is_empty() && required {
            return Err(usage(
                "You must provide at least one resource: --topic <topic> or --cluster or --group \
                 <group> or --delegation-token <Delegation Token ID>",
            ));
        }
        Ok(filters)
    }
}

/// The `DescribeAcls` filters of `--list`: one per resource, or one that
/// matches everything when no resource is named.
fn list_requests(filters: &BTreeSet<Resource>) -> Vec<AclEntryFilter> {
    if filters.is_empty() {
        return vec![AclEntryFilter::default()];
    }
    filters.iter().map(Resource::wire_filter).collect()
}

/// One resource of `--add`: the filter that finds its current entries, and a
/// binding for each requested entry.
#[derive(Debug, PartialEq, Eq)]
struct AddStep {
    resource: Resource,
    existing: AclEntryFilter,
    creations: Vec<(Entry, AclEntry)>,
}

fn add_steps(acls: Acls) -> Result<Vec<AddStep>, CommandError> {
    acls.into_iter()
        .map(|(resource, entries)| {
            Ok(AddStep {
                existing: resource.wire_filter(),
                creations: entries
                    .into_iter()
                    .map(|entry| {
                        let wire = entry.wire(&resource)?;
                        Ok((entry, wire))
                    })
                    .collect::<Result<_, CommandError>>()?,
                resource,
            })
        })
        .collect()
}

/// One resource filter of `--remove`: the requested entries, empty for every
/// entry, and the `DeleteAcls` filters that remove them.
#[derive(Debug, PartialEq, Eq)]
struct RemoveStep {
    resource: Resource,
    entries: BTreeSet<Entry>,
    filters: Vec<AclEntryFilter>,
}

fn remove_steps(acls: Acls) -> Result<Vec<RemoveStep>, CommandError> {
    acls.into_iter()
        .map(|(resource, entries)| {
            let base = resource.wire_filter();
            let filters = if entries.is_empty() {
                vec![base]
            } else {
                entries
                    .iter()
                    .map(|entry| entry.wire_filter(&base))
                    .collect::<Result<_, _>>()?
            };
            Ok(RemoveStep {
                resource,
                entries,
                filters,
            })
        })
        .collect()
}

/// What `--remove` asks the operator to confirm.
fn removal_impact(steps: &[RemoveStep]) -> Impact {
    Impact {
        summary: format!("remove ACLs from {} resource filter(s)", steps.len()),
        resources: steps
            .iter()
            .flat_map(|step| {
                if step.entries.is_empty() {
                    vec![format!("all ACLs for resource filter `{}`", step.resource)]
                } else {
                    step.entries
                        .iter()
                        .map(|entry| format!("{entry} from resource filter `{}`", step.resource))
                        .collect()
                }
            })
            .collect(),
    }
}

/// The rows of `--add` or `--remove` so far.
#[derive(Default)]
struct Report {
    human: Vec<String>,
    rows: Vec<Value>,
    failed: bool,
}

impl Report {
    /// Ends the report on `error`, as the JVM tool ends on the first failed
    /// request. A failure before any row fails the whole command.
    fn stop(&mut self, error: CommandError, row: Option<Value>) -> Result<(), CommandError> {
        if self.rows.is_empty() && row.is_none() {
            return Err(error);
        }
        self.rows
            .push(row.unwrap_or_else(|| json!({"error": error_json(&error)})));
        self.human
            .push(format!("Error while executing ACL command: {error}"));
        self.failed = true;
        Ok(())
    }

    fn finish(self, dry_run: bool) -> CommandResult {
        let result = CommandResult::rows(self.human, self.rows, self.failed);
        if dry_run {
            result.into_dry_run()
        } else {
            result
        }
    }
}

fn error_json(error: &CommandError) -> Value {
    match error {
        CommandError::Broker {
            code,
            name,
            message,
            ..
        } => json!({"code": code, "name": name, "message": message}),
        other => json!({"message": other.to_string()}),
    }
}

fn broker_error(api: &'static str, error: KafkaError) -> CommandError {
    CommandError::Broker {
        api,
        code: error.code,
        name: error.name,
        message: error.message,
    }
}

/// `addAcls`: skips each entry that already exists and creates the rest, one
/// `CreateAcls` request per resource. A dry run creates nothing.
async fn add(
    client: &mut AdminClient,
    steps: Vec<AddStep>,
    dry_run: bool,
) -> Result<CommandResult, CommandError> {
    let mut report = Report::default();
    for step in steps {
        let existing = match client.describe_acls(&step.existing).await {
            Ok(existing) => existing.into_iter().map(binding).collect::<BTreeSet<_>>(),
            Err(error) => {
                report.stop(error.into(), None)?;
                break;
            }
        };
        let resource = step.resource;
        let (present, absent): (Vec<_>, Vec<_>) = step
            .creations
            .into_iter()
            .partition(|(entry, _)| existing.contains(&(resource.clone(), entry.clone())));
        for (entry, _) in &present {
            report.human.push(format!(
                "Acl (pattern={resource}, entry={entry}) already exists."
            ));
        }
        let mut error = None;
        if !absent.is_empty() {
            report
                .human
                .push(format!("Adding ACLs for resource `{resource}`: "));
            for (index, (entry, _)) in absent.iter().enumerate() {
                let indent = if index == 0 { " \t" } else { "\t" };
                report.human.push(format!("{indent}{entry}"));
            }
            report.human.push(String::new());
            if !dry_run {
                let creations = absent
                    .iter()
                    .map(|(_, wire)| wire.clone())
                    .collect::<Vec<_>>();
                error = match client.create_acls(&creations).await {
                    Ok(outcomes) => outcomes
                        .into_iter()
                        .find_map(|outcome| outcome.error)
                        .map(|error| broker_error("CreateAcls", error)),
                    Err(error) => Some(error.into()),
                };
            }
        }
        let row = json!({
            "resource": resource.json(),
            "existing": present.iter().map(|(entry, _)| entry.json()).collect::<Vec<_>>(),
            "added": absent.iter().map(|(entry, _)| entry.json()).collect::<Vec<_>>(),
            "error": error.as_ref().map(error_json),
        });
        if let Some(error) = error {
            report.stop(error, Some(row))?;
            break;
        }
        report.rows.push(row);
    }
    Ok(report.finish(dry_run))
}

/// `removeAcls`: one `DeleteAcls` request per resource filter. The JVM tool
/// prints nothing on success, so the human report is empty. A dry run finds
/// the entries that the request would remove with `DescribeAcls`, which
/// matches the same way.
async fn remove(
    client: &mut AdminClient,
    steps: Vec<RemoveStep>,
    dry_run: bool,
) -> Result<CommandResult, CommandError> {
    let mut report = Report::default();
    for step in steps {
        let mut removed = BTreeSet::new();
        let mut error = None;
        if dry_run {
            for filter in &step.filters {
                match client.describe_acls(filter).await {
                    Ok(entries) => removed.extend(entries.into_iter().map(binding)),
                    Err(failure) => {
                        error = Some(CommandError::from(failure));
                        break;
                    }
                }
            }
        } else {
            match client.delete_acls(&step.filters).await {
                Ok(outcomes) => {
                    for outcome in outcomes {
                        removed.extend(outcome.matched.into_iter().map(binding));
                        if error.is_none() {
                            error = outcome
                                .error
                                .map(|failure| broker_error("DeleteAcls", failure));
                        }
                    }
                }
                Err(failure) => error = Some(failure.into()),
            }
        }
        let row = json!({
            "filter": step.resource.json(),
            "acls": step.entries.iter().map(Entry::json).collect::<Vec<_>>(),
            "removed": removed
                .iter()
                .map(|(resource, entry)| json!({"resource": resource.json(), "acl": entry.json()}))
                .collect::<Vec<_>>(),
            "error": error.as_ref().map(error_json),
        });
        if let Some(error) = error {
            let partial = !removed.is_empty();
            report.stop(error, partial.then_some(row))?;
            break;
        }
        report.rows.push(row);
    }
    Ok(report.finish(dry_run))
}

/// `printResourceAcls`: each resource, then its entries, then a blank line.
fn current_acls(human: &mut Vec<String>, acls: &Acls) -> Value {
    for (resource, entries) in acls {
        human.push(format!("Current ACLs for resource `{resource}`:"));
        human.extend(entries.iter().map(|entry| format!("\t{entry}")));
        human.push(String::new());
    }
    acls.iter()
        .map(|(resource, entries)| {
            json!({
                "resource": resource.json(),
                "acls": entries.iter().map(Entry::json).collect::<Vec<_>>(),
            })
        })
        .collect()
}

/// `listAcls`: every resource, or with `--principal` the resources of each
/// principal, keeping only that principal's entries.
fn listed(acls: &Acls, principals: Option<&[String]>) -> CommandResult {
    let mut human = Vec::new();
    let data = match principals {
        Some(principals) if !principals.is_empty() => {
            let mut rows = Vec::with_capacity(principals.len());
            for principal in principals {
                human.push(format!("ACLs for principal `{principal}`"));
                let own = acls
                    .iter()
                    .filter_map(|(resource, entries)| {
                        let entries = entries
                            .iter()
                            .filter(|entry| &entry.principal == principal)
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        (!entries.is_empty()).then(|| (resource.clone(), entries))
                    })
                    .collect::<Acls>();
                let acls = current_acls(&mut human, &own);
                rows.push(json!({"principal": principal, "acls": acls}));
            }
            Value::Array(rows)
        }
        _ => current_acls(&mut human, acls),
    };
    CommandResult::success(human, data)
}

#[cfg(test)]
mod tests;
