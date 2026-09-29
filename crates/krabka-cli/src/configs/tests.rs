use std::collections::BTreeMap;

use assert2::check;
use clap::Parser;

use super::*;
use crate::connection::Secret;

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    configs: ConfigsArgs,
}

fn args(argv: &[&str]) -> ConfigsArgs {
    Cli::try_parse_from(
        ["configs", "--bootstrap-server", "host:9092"]
            .iter()
            .chain(argv),
    )
    .unwrap_or_else(|error| panic!("{argv:?} parses: {error}"))
    .configs
}

// Only `1.2.3.4` and `broker.example` resolve.
fn resolve(host: &str) -> bool {
    matches!(host, "1.2.3.4" | "broker.example")
}

fn plan(argv: &[&str]) -> Result<Plan, String> {
    args(argv).plan(&resolve)
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}

fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn command_lines_become_the_request_plan() {
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &[
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                "t1",
            ],
            Plan::DescribeResources {
                entity_type: TOPICS,
                name: Some("t1".into()),
                all: false,
            },
        ),
        (
            &["--describe", "--topic", "t1"],
            Plan::DescribeResources {
                entity_type: TOPICS,
                name: Some("t1".into()),
                all: false,
            },
        ),
        (
            &["--describe", "--entity-type", "topics"],
            Plan::DescribeResources {
                entity_type: TOPICS,
                name: None,
                all: false,
            },
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "topics",
                "--entity-name",
                "t1",
                "--add-config",
                "retention.ms=1000,cleanup.policy=[compact,delete]",
                "--delete-config",
                " segment.bytes , flush.ms",
            ],
            Plan::AlterResource {
                entity_type: TOPICS,
                name: "t1".into(),
                deletes: strings(&["segment.bytes", "flush.ms"]),
                sets: pairs(&[
                    ("cleanup.policy", "compact,delete"),
                    ("retention.ms", "1000"),
                ]),
            },
        ),
        (
            &[
                "--alter",
                "--topic",
                "t1",
                "--delete-config",
                "a",
                "--delete-config",
                "b",
            ],
            Plan::AlterResource {
                entity_type: TOPICS,
                name: "t1".into(),
                deletes: strings(&["a", "b"]),
                sets: Vec::new(),
            },
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn user_command_lines_become_the_request_plan() {
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &[
                "--describe",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
            ],
            Plan::DescribeQuotas {
                components: vec![ClientQuotaFilterComponent::of_entity("user", "alice")],
                scram_users: Some(strings(&["alice"])),
            },
        ),
        (
            &["--describe", "--user", "alice"],
            Plan::DescribeQuotas {
                components: vec![ClientQuotaFilterComponent::of_entity("user", "alice")],
                scram_users: Some(strings(&["alice"])),
            },
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-512=[password=s,iterations=8192]",
            ],
            Plan::AlterUserScram {
                entity_type: USERS,
                name: "alice".into(),
                change: Some(ScramChange::Upsert(ScramCredential {
                    mechanism: Mechanism::Sha512,
                    iterations: 4096,
                    password: Secret::new("s,iterations=8192".into()),
                })),
            },
        ),
        (
            &[
                "--alter",
                "--user",
                "alice",
                "--add-config",
                "SCRAM-SHA-256=[iterations=8192,password=s]",
            ],
            Plan::AlterUserScram {
                entity_type: USERS,
                name: "alice".into(),
                change: Some(ScramChange::Upsert(ScramCredential {
                    mechanism: Mechanism::Sha256,
                    iterations: 8192,
                    password: Secret::new("s".into()),
                })),
            },
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--delete-config",
                "SCRAM-SHA-512",
            ],
            Plan::AlterUserScram {
                entity_type: USERS,
                name: "alice".into(),
                change: Some(ScramChange::Delete(Mechanism::Sha512)),
            },
        ),
        // `,` alone parses to no config, which Kafka accepts and sends nothing
        // for.
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                ",",
            ],
            Plan::AlterUserScram {
                entity_type: USERS,
                name: "alice".into(),
                change: None,
            },
        ),
        (
            &["--alter", "--client", "c1", "--add-config", ","],
            Plan::AlterUserScram {
                entity_type: CLIENTS,
                name: "c1".into(),
                change: None,
            },
        ),
        (
            &["--alter", "--user-defaults", "--add-config", ","],
            Plan::AlterUserScram {
                entity_type: USERS,
                name: String::new(),
                change: None,
            },
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "consumer_byte_rate=1024",
                "--delete-config",
                "producer_byte_rate",
            ],
            Plan::AlterQuotas {
                entity_type: USERS,
                name: "alice".into(),
                entity: entity(&[("user", Some("alice"))]),
                components: vec![ClientQuotaFilterComponent::of_entity("user", "alice")],
                sets: pairs(&[("consumer_byte_rate", "1024")]),
                deletes: strings(&["producer_byte_rate"]),
            },
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn command_lines_that_kafka_refuses_are_refused_with_its_message() {
    let cases: Vec<(&[&str], &str)> = vec![
        (
            &["--entity-type", "topics"],
            "Command must include exactly one action: --describe, --alter",
        ),
        (
            &["--describe", "--alter", "--topic", "t1"],
            "Command must include exactly one action: --describe, --alter",
        ),
        (
            &["--describe", "--topic", "t1", "--add-config", "a=b"],
            "Option \"[describe]\" can't be used with option \"[add-config]\"",
        ),
        (
            &["--describe", "--topic", "t1", "--delete-config", "a"],
            "Option \"[describe]\" can't be used with option \"[delete-config]\"",
        ),
        (
            &["--describe", "--entity-type", "users", "--user", "x"],
            "Duplicate entity type(s) specified: users",
        ),
        (
            &["--describe", "--entity-type", "widgets"],
            "Invalid entity type widgets, the entity type must be one of topics, clients, users, brokers, ips, client-metrics, groups, broker-loggers with a --bootstrap-server or --bootstrap-controller argument",
        ),
        (
            &["--describe"],
            "At least one entity type must be specified",
        ),
        (
            &[
                "--describe",
                "--entity-type",
                "topics",
                "--entity-type",
                "brokers",
            ],
            "Only 'users' and 'clients' entity types may be specified together",
        ),
        (
            &["--describe", "--entity-name", "t1", "--topic", "t1"],
            "--entity-{type,name,default} should not be used in conjunction with specific entity flags",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "brokers",
                "--entity-name",
                "abc",
                "--add-config",
                "a=b",
            ],
            "The entity name for brokers must be a valid integer broker id, but it is: abc",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "ips",
                "--entity-name",
                "no.such.host.invalid",
                "--add-config",
                "connection_creation_rate=1",
            ],
            "The entity name for ips must be a valid IP or resolvable host, but it is: no.such.host.invalid",
        ),
        (
            &["--describe", "--entity-type", "topics", "--entity-default"],
            "--entity-default must not be specified with --describe of topics",
        ),
        (
            &["--describe", "--entity-type", "broker-loggers"],
            "An entity name must be specified with --describe of broker-loggers",
        ),
        (
            &["--alter", "--entity-type", "users", "--add-config", "a=b"],
            "An entity-name or default entity must be specified with --alter of users, clients, brokers or ips",
        ),
        (
            &["--alter", "--entity-type", "topics", "--add-config", "a=b"],
            "An entity name must be specified with --alter of topics",
        ),
        (
            &[
                "--alter",
                "--topic",
                "t1",
                "--add-config",
                "a=b",
                "--add-config-file",
                "/x",
            ],
            "Only one of --add-config or --add-config-file must be specified",
        ),
        (
            &["--alter", "--topic", "t1"],
            "At least one of --add-config, --add-config-file, or --delete-config must be specified with --alter",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-type",
                "clients",
                "--entity-name",
                "a",
                "--add-config",
                "consumer_byte_rate=1",
            ],
            "An entity name must be specified for every entity type",
        ),
        (
            &[
                "--alter",
                "--topic",
                "t1",
                "--add-config",
                "a=b",
                "--add-config",
                "c=d",
            ],
            "Found multiple arguments for option add-config, but you asked for only one",
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Err(expected.to_owned()), "{argv:?}");
    }
}

#[test]
fn names_and_configs_that_kafka_refuses_are_refused_with_its_message() {
    let cases: Vec<(&[&str], &str)> = vec![
        (
            &[
                "--describe",
                "--entity-type",
                "topics",
                "--entity-name",
                "bad name",
            ],
            "Topic name is invalid: 'bad name' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
        ),
        (
            &["--alter", "--topic", "t1", "--add-config", "bad key=1"],
            "Invalid character found for config key: bad key",
        ),
        (
            &["--alter", "--topic", "t1", "--add-config", "novalue"],
            parse::INVALID_ENTITY_CONFIG,
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "foo=1,bar=2",
            ],
            "Only quota and SCRAM credential configs can be added for 'users' using --bootstrap-server. Unexpected config names: Set(bar, foo)",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--delete-config",
                "foo,bar",
            ],
            "Only quota and SCRAM credential configs can be deleted for 'users' using --bootstrap-server. Unexpected config names: ArrayBuffer(foo, bar)",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-256=[password=x],consumer_byte_rate=5",
            ],
            "Cannot alter both quota and SCRAM credential configs simultaneously for 'users' using --bootstrap-server.",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-default",
                "--add-config",
                "SCRAM-SHA-256=[password=x]",
            ],
            "The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "clients",
                "--entity-name",
                "c1",
                "--add-config",
                "SCRAM-SHA-256=[password=x],foo=1",
            ],
            "Only quota configs can be added for 'clients' using --bootstrap-server. Unexpected config names: Set(foo, SCRAM-SHA-256)",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "clients",
                "--entity-name",
                "c1",
                "--delete-config",
                "SCRAM-SHA-256,foo",
            ],
            "Only quota configs can be deleted for 'clients' using --bootstrap-server. Unexpected config names: ArrayBuffer(foo, SCRAM-SHA-256)",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "ips",
                "--entity-name",
                "1.2.3.4",
                "--add-config",
                "foo=1",
                "--delete-config",
                "bar",
            ],
            "Only connection quota configs can be added for 'ips' using --bootstrap-server. Unexpected config names: foo,bar",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-256=[iterations=100,password=x]",
            ],
            "Iterations 100 is less than the minimum 4096 required for SCRAM-SHA-256",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-256=bogus",
            ],
            "Invalid credential property SCRAM_SHA_256=[redacted]",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-256=[iterations=,password=x]",
            ],
            "For input string: \"\"",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                "SCRAM-SHA-512=[password=]",
            ],
            "Password must not be empty",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "bob",
                "--add-config",
                "SCRAM-SHA-256=[password=x],SCRAM-SHA-512=[password=y]",
            ],
            ALTERED_TWICE,
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "u",
                "--entity-type",
                "clients",
                "--entity-name",
                "c",
                "--add-config",
                ",",
            ],
            "Altering user SCRAM credentials should never occur for more zero or multiple users: List(u, c)",
        ),
        (
            &[
                "--describe",
                "--entity-type",
                "users",
                "--entity-name",
                "bob",
                "--entity-name",
                "alice",
            ],
            "More entity names specified than entity types",
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Err(expected.to_owned()), "{argv:?}");
    }
}

#[test]
fn a_command_without_a_bootstrap_flag_is_refused_with_the_kafka_message() {
    let parsed = Cli::try_parse_from(["configs", "--describe", "--topic", "t1"])
        .unwrap()
        .configs;
    check!(
        parsed.plan(&resolve)
            == Err("Either --bootstrap-server or --bootstrap-controller must be specified.".into())
    );
}

fn entity(entries: &[(&str, Option<&str>)]) -> ClientQuotaEntity {
    entries
        .iter()
        .map(|(entity_type, name)| ((*entity_type).to_owned(), name.map(str::to_owned)))
        .collect()
}

fn of_entity(entity_type: &str, name: &str) -> ClientQuotaFilterComponent {
    ClientQuotaFilterComponent::of_entity(entity_type, name)
}

fn of_default(entity_type: &str) -> ClientQuotaFilterComponent {
    ClientQuotaFilterComponent::of_default_entity(entity_type)
}

fn resources(entity_type: &'static str, name: Option<&str>, all: bool) -> Plan {
    Plan::DescribeResources {
        entity_type,
        name: name.map(str::to_owned),
        all,
    }
}

fn quotas(components: Vec<ClientQuotaFilterComponent>, scram_users: Option<&[&str]>) -> Plan {
    Plan::DescribeQuotas {
        components,
        scram_users: scram_users.map(strings),
    }
}

#[test]
fn every_entity_type_and_form_becomes_its_plan() {
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &["--describe", "--topic", "t1", "--all"],
            resources(TOPICS, Some("t1"), true),
        ),
        (
            &[
                "--describe",
                "--entity-type",
                "brokers",
                "--entity-name",
                "1",
            ],
            resources(BROKERS, Some("1"), false),
        ),
        (
            &["--describe", "--broker-defaults"],
            resources(BROKERS, Some(""), false),
        ),
        (
            &["--describe", "--entity-type", "brokers", "--all"],
            resources(BROKERS, None, true),
        ),
        (
            &["--describe", "--broker-logger", "2"],
            resources(BROKER_LOGGERS, Some("2"), false),
        ),
        (
            &["--describe", "--client-metrics", "cm"],
            resources(CLIENT_METRICS, Some("cm"), false),
        ),
        (
            &["--describe", "--entity-type", "groups"],
            resources(GROUPS, None, false),
        ),
        (
            &["--describe", "--entity-type", "users"],
            quotas(
                vec![ClientQuotaFilterComponent::of_entity_type("user")],
                Some(&[]),
            ),
        ),
        (
            &["--describe", "--user-defaults"],
            quotas(vec![of_default("user")], None),
        ),
        (
            &["--describe", "--client", "c1"],
            quotas(vec![of_entity("client-id", "c1")], None),
        ),
        (
            &["--describe", "--user", "alice", "--client", "c1"],
            quotas(
                vec![of_entity("client-id", "c1"), of_entity("user", "alice")],
                None,
            ),
        ),
        (
            &[
                "--describe",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--entity-type",
                "clients",
            ],
            quotas(
                vec![
                    of_entity("user", "alice"),
                    ClientQuotaFilterComponent::of_entity_type("client-id"),
                ],
                None,
            ),
        ),
        (
            &["--describe", "--ip", "1.2.3.4"],
            quotas(vec![of_entity("ip", "1.2.3.4")], None),
        ),
        (
            &["--describe", "--ip-defaults"],
            quotas(vec![of_default("ip")], None),
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn every_alter_form_becomes_its_plan() {
    let cases: Vec<(&[&str], Plan)> = vec![
        (
            &[
                "--alter",
                "--entity-type",
                "brokers",
                "--entity-default",
                "--add-config",
                "log.cleaner.threads=2",
            ],
            Plan::AlterResource {
                entity_type: BROKERS,
                name: String::new(),
                deletes: Vec::new(),
                sets: pairs(&[("log.cleaner.threads", "2")]),
            },
        ),
        (
            &[
                "--alter",
                "--group",
                "g",
                "--add-config",
                "consumer.session.timeout.ms=50000",
            ],
            Plan::AlterResource {
                entity_type: GROUPS,
                name: "g".into(),
                deletes: Vec::new(),
                sets: pairs(&[("consumer.session.timeout.ms", "50000")]),
            },
        ),
        (
            &[
                "--alter",
                "--client-metrics",
                "cm",
                "--delete-config",
                "metrics",
            ],
            Plan::AlterResource {
                entity_type: CLIENT_METRICS,
                name: "cm".into(),
                deletes: strings(&["metrics"]),
                sets: Vec::new(),
            },
        ),
        (
            &[
                "--alter",
                "--broker-logger",
                "1",
                "--add-config",
                "kafka.server=DEBUG",
            ],
            Plan::AlterBrokerLoggers {
                broker: "1".into(),
                deletes: Vec::new(),
                sets: pairs(&[("kafka.server", "DEBUG")]),
            },
        ),
        (
            &[
                "--alter",
                "--client",
                "c1",
                "--add-config",
                "consumer_byte_rate=1",
            ],
            Plan::AlterQuotas {
                entity_type: CLIENTS,
                name: "c1".into(),
                entity: entity(&[("client-id", Some("c1"))]),
                components: vec![of_entity("client-id", "c1")],
                sets: pairs(&[("consumer_byte_rate", "1")]),
                deletes: Vec::new(),
            },
        ),
        (
            &[
                "--alter",
                "--user-defaults",
                "--add-config",
                "consumer_byte_rate=1",
            ],
            Plan::AlterQuotas {
                entity_type: USERS,
                name: String::new(),
                entity: entity(&[("user", None)]),
                components: vec![of_default("user")],
                sets: pairs(&[("consumer_byte_rate", "1")]),
                deletes: Vec::new(),
            },
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "a",
                "--entity-type",
                "clients",
                "--entity-default",
                "--add-config",
                "consumer_byte_rate=1",
            ],
            Plan::AlterQuotas {
                entity_type: USERS,
                name: "a".into(),
                entity: entity(&[("client-id", None), ("user", Some("a"))]),
                components: vec![of_entity("user", "a"), of_default("client-id")],
                sets: pairs(&[("consumer_byte_rate", "1")]),
                deletes: Vec::new(),
            },
        ),
        (
            &[
                "--alter",
                "--ip",
                "broker.example",
                "--add-config",
                "connection_creation_rate=10",
            ],
            Plan::AlterQuotas {
                entity_type: IPS,
                name: "broker.example".into(),
                entity: entity(&[("ip", Some("broker.example"))]),
                components: vec![of_entity("ip", "broker.example")],
                sets: pairs(&[("connection_creation_rate", "10")]),
                deletes: Vec::new(),
            },
        ),
    ];
    for (argv, expected) in cases {
        check!(plan(argv) == Ok(expected), "{argv:?}");
    }
}

#[test]
fn add_config_file_reads_a_properties_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("configs.properties");
    std::fs::write(
        &path,
        "# comment\nretention.ms = 1000\ncleanup.policy=compact,delete\n",
    )
    .unwrap();
    let path = path.to_str().unwrap();
    check!(
        plan(&["--alter", "--topic", "t1", "--add-config-file", path])
            == Ok(Plan::AlterResource {
                entity_type: TOPICS,
                name: "t1".into(),
                deletes: Vec::new(),
                sets: pairs(&[
                    ("cleanup.policy", "compact,delete"),
                    ("retention.ms", "1000")
                ]),
            })
    );
    check!(
        plan(&[
            "--alter",
            "--topic",
            "t1",
            "--add-config-file",
            "/nonexistent/file"
        ])
        .unwrap_err()
        .starts_with("read --add-config-file /nonexistent/file:")
    );
}

#[test]
fn entity_names_follow_the_command_line_order() {
    let parsed = args(&[
        "--describe",
        "--entity-type",
        "users",
        "--entity-default",
        "--entity-type",
        "clients",
        "--entity-name",
        "c1",
    ]);
    check!(parsed.entity_types() == strings(&["users", "clients"]));
    check!(parsed.entity_names() == Ok(strings(&["", "c1"])));
    let parsed = args(&["--describe", "--user", "alice", "--client-defaults"]);
    check!(
        parsed.entity_types()
            == strings(&["clients", "users"])
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
    );
    check!(parsed.entity_names() == Ok(strings(&["alice", ""])));
}

#[test]
fn quota_ops_set_each_added_config_then_remove_each_deleted_one() {
    let cases = [
        (
            pairs(&[
                ("consumer_byte_rate", "1024"),
                ("request_percentage", "12.5"),
            ]),
            strings(&["producer_byte_rate"]),
            Ok(vec![
                QuotaOp::Set {
                    key: "consumer_byte_rate".into(),
                    value: 1024.0,
                },
                QuotaOp::Set {
                    key: "request_percentage".into(),
                    value: 12.5,
                },
                QuotaOp::Remove {
                    key: "producer_byte_rate".into(),
                },
            ]),
        ),
        (
            pairs(&[("consumer_byte_rate", "abc")]),
            Vec::new(),
            Err("Cannot parse quota configuration value for consumer_byte_rate: abc".to_owned()),
        ),
    ];
    for (sets, deletes, expected) in cases {
        check!(
            quota_ops(&sets, &deletes) == expected,
            "{sets:?} {deletes:?}"
        );
    }
}

#[test]
fn quota_entities_hash_and_iterate_as_java_does() {
    // `ClientQuotaEntity.hashCode`, from the JDK of apache/kafka:4.3.1.
    let cases = [
        (entity(&[("user", Some("alice"))]), 96_435_562),
        (entity(&[("user", None)]), 3_599_338),
        (
            entity(&[("user", Some("alice")), ("client-id", Some("c1"))]),
            -1_807_703_075,
        ),
    ];
    for (entity, expected) in cases {
        check!(quota_entity_hash(&entity) == expected, "{entity:?}");
    }
    // The JDK iterates `new HashMap<>(3)` of these as `{user=null}`,
    // `{user=alice}`, `{user=bob}`.
    let described = [
        entity(&[("user", Some("alice"))]),
        entity(&[("user", None)]),
        entity(&[("user", Some("bob"))]),
    ]
    .into_iter()
    .map(|entity| (entity, BTreeMap::new()))
    .collect::<ClientQuotas>();
    check!(
        quota_order(&described)
            .into_iter()
            .map(|(entity, _)| entity.clone())
            .collect::<Vec<_>>()
            == [
                entity(&[("user", None)]),
                entity(&[("user", Some("alice"))]),
                entity(&[("user", Some("bob"))]),
            ]
    );
}

#[test]
fn a_parsed_scram_credential_does_not_render_its_password() {
    let plan = plan(&[
        "--alter",
        "--user",
        "alice",
        "--add-config",
        "SCRAM-SHA-512=[password=hunter2-plain]",
    ])
    .unwrap();
    check!(!format!("{plan:?}").contains("hunter2-plain"));
    check!(!format!("{:?}", args(&["--describe", "--user", "alice"])).contains("hunter2-plain"));
}
