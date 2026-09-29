//! The `--describe` lines of `kafka-configs`, and their JSON rendering.

use std::collections::BTreeMap;

use krabka_client_admin::{
    ClientQuotaEntity, ConfigEntry, ENTITY_CLIENT_ID, ENTITY_IP, ENTITY_USER, UserScramCredentials,
};
use serde_json::{Map, Value, json};

use super::java;
use crate::{compat::KafkaException, output::kafka_error};

/// `RESOURCE_NOT_FOUND`: the broker's answer for a user with no SCRAM
/// credential, which `DescribeUserScramCredentialsResult.users` leaves out.
const RESOURCE_NOT_FOUND: i16 = 91;

/// One synonym of a described config entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Synonym {
    /// The wire id of the `ConfigSource`.
    pub source: i8,
    pub name: String,
    pub value: Option<String>,
}

/// One config entry of a `DescribeConfigs` answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedConfig {
    pub name: String,
    pub value: Option<String>,
    pub sensitive: bool,
    pub synonyms: Vec<Synonym>,
}

impl DescribedConfig {
    /// The value to print: `None`, printed as Java prints a null string, for
    /// a sensitive entry whatever the broker sent.
    fn shown_value(&self) -> Option<&str> {
        if self.sensitive {
            None
        } else {
            self.value.as_deref()
        }
    }

    fn shown_synonym_value<'a>(&self, synonym: &'a Synonym) -> Option<&'a str> {
        if self.sensitive {
            None
        } else {
            synonym.value.as_deref()
        }
    }

    /// The body line of `kafka-configs --describe`:
    /// `  name=value sensitive=<bool> synonyms={SOURCE:name=value, ...}`.
    pub fn line(&self) -> String {
        let synonyms = self
            .synonyms
            .iter()
            .map(|synonym| {
                format!(
                    "{}:{}={}",
                    config_source_name(synonym.source),
                    synonym.name,
                    self.shown_synonym_value(synonym).unwrap_or("null")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "  {}={} sensitive={} synonyms={{{synonyms}}}",
            self.name,
            self.shown_value().unwrap_or("null"),
            self.sensitive
        )
    }

    pub fn json(&self) -> Value {
        json!({
            "name": self.name,
            "value": self.shown_value(),
            "sensitive": self.sensitive,
            "synonyms": self
                .synonyms
                .iter()
                .map(|synonym| json!({
                    "source": config_source_name(synonym.source),
                    "name": synonym.name,
                    "value": self.shown_synonym_value(synonym),
                }))
                .collect::<Vec<_>>(),
        })
    }
}

/// The name of a `ConfigEntry.ConfigSource`, from its wire id.
pub const fn config_source_name(id: i8) -> &'static str {
    match id {
        1 => "DYNAMIC_TOPIC_CONFIG",
        2 => "DYNAMIC_BROKER_CONFIG",
        3 => "DYNAMIC_DEFAULT_BROKER_CONFIG",
        4 => "STATIC_BROKER_CONFIG",
        5 => "DEFAULT_CONFIG",
        6 => "DYNAMIC_BROKER_LOGGER_CONFIG",
        7 => "DYNAMIC_CLIENT_METRICS_CONFIG",
        8 => "DYNAMIC_GROUP_CONFIG",
        _ => "UNKNOWN",
    }
}

impl From<&ConfigEntry> for DescribedConfig {
    fn from(entry: &ConfigEntry) -> Self {
        Self {
            name: entry.name.clone(),
            value: entry.value.clone(),
            sensitive: entry.is_sensitive,
            synonyms: entry
                .synonyms
                .iter()
                .map(|synonym| Synonym {
                    source: synonym.source.id(),
                    name: synonym.name.clone(),
                    value: synonym.value.clone(),
                })
                .collect(),
        }
    }
}

/// The header of one described entity. `entity_type` is plural, as on the
/// command line, and an empty `entity` is the cluster default.
pub fn header(entity_type: &str, entity: &str, all: bool) -> String {
    if entity.is_empty() {
        return format!("Default configs for {entity_type} in the cluster are:");
    }
    let source = if all { "All" } else { "Dynamic" };
    format!(
        "{source} configs for {} {entity} are:",
        singular(entity_type)
    )
}

/// The line for an entity that `--describe` names and that does not exist.
pub fn missing(entity_type: &str, entity: &str) -> String {
    format!(
        "The {} '{entity}' doesn't exist and doesn't have dynamic config.",
        singular(entity_type)
    )
}

/// `entityType.dropRight(1)`.
pub fn singular(entity_type: &str) -> &str {
    let mut chars = entity_type.chars();
    chars.next_back();
    chars.as_str()
}

/// The quota line of one entity, as `ConfigCommand.describeQuotaConfigs`
/// prints it: the user, the client and the ip of the entity, then its
/// values in the order of the `HashMap` that
/// `DescribeClientQuotasResponse.complete` fills.
pub fn quota_line(entity: &ClientQuotaEntity, values: &BTreeMap<String, f64>) -> String {
    let names = [
        (ENTITY_USER, "user-principal"),
        (ENTITY_CLIENT_ID, "client-id"),
        (ENTITY_IP, "ip"),
    ]
    .into_iter()
    .filter_map(|(entity_type, label)| {
        entity.get(entity_type).map(|name| match name {
            Some(name) => format!("{label} '{name}'"),
            None => format!("the default {label}"),
        })
    })
    .collect::<Vec<_>>()
    .join(", ");
    let entries = java::hash_map_order(values.keys().map(String::as_str), Some(values.len()))
        .into_iter()
        .map(|key| format!("{key}={}", java::double_to_string(values[key])))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Quota configs for {names} are {entries}")
}

pub fn quota_json(entity: &ClientQuotaEntity, values: &BTreeMap<String, f64>) -> Value {
    json!({
        "entity": entity,
        "values": Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json!(value)))
                .collect::<Map<_, _>>(),
        ),
    })
}

/// The users that `DescribeUserScramCredentialsResult.users` lists, each
/// with the first result of its name, which `description` reads: every
/// result but those of `RESOURCE_NOT_FOUND`, in the order of the answer.
pub fn scram_users(described: &[UserScramCredentials]) -> Vec<&UserScramCredentials> {
    described
        .iter()
        .filter(|user| {
            user.error
                .as_ref()
                .is_none_or(|error| error.code != RESOURCE_NOT_FOUND)
        })
        .map(|user| {
            described
                .iter()
                .find(|first| first.username == user.username)
                .expect("the user is in the answer")
        })
        .collect()
}

/// The SCRAM line of a described user, as
/// `describeClientQuotaAndUserScramCredentialConfigs` prints it. A failed
/// user prints the `ExecutionException` that `get` throws.
pub fn scram_line(user: &UserScramCredentials) -> String {
    match &user.error {
        Some(error) => {
            let exception = KafkaException::for_code(error.code);
            format!(
                "Error retrieving SCRAM credential configs for user-principal '{}': ExecutionException: {}: {}",
                user.username,
                exception.class(),
                error.message.as_deref().unwrap_or(exception.message())
            )
        }
        None => format!(
            "SCRAM credential configs for user-principal '{}' are {}",
            user.username,
            user.credentials
                .iter()
                .map(|credential| format!(
                    "{}=iterations={}",
                    credential.mechanism, credential.iterations
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub fn scram_json(user: &UserScramCredentials) -> Value {
    json!({
        "user": user.username,
        "credentials": user
            .credentials
            .iter()
            .map(|credential| json!({
                "mechanism": credential.mechanism,
                "iterations": credential.iterations,
            }))
            .collect::<Vec<_>>(),
        "error": kafka_error(user.error.as_ref()),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;
    use krabka_client_admin::{KafkaError, UserScramCredential};

    use super::*;

    fn synonym(source: i8, name: &str, value: Option<&str>) -> Synonym {
        Synonym {
            source,
            name: name.into(),
            value: value.map(Into::into),
        }
    }

    #[test]
    fn config_lines_have_the_jvm_shape_and_withhold_sensitive_values() {
        let cases = [
            (
                DescribedConfig {
                    name: "cleanup.policy".into(),
                    value: Some("compact,delete".into()),
                    sensitive: false,
                    synonyms: vec![
                        synonym(1, "cleanup.policy", Some("compact,delete")),
                        synonym(5, "log.cleanup.policy", Some("delete")),
                    ],
                },
                "  cleanup.policy=compact,delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact,delete, DEFAULT_CONFIG:log.cleanup.policy=delete}",
            ),
            (
                DescribedConfig {
                    name: "listener.name.plaintext.ssl.key.password".into(),
                    value: None,
                    sensitive: true,
                    synonyms: vec![synonym(2, "listener.name.plaintext.ssl.key.password", None)],
                },
                "  listener.name.plaintext.ssl.key.password=null sensitive=true synonyms={DYNAMIC_BROKER_CONFIG:listener.name.plaintext.ssl.key.password=null}",
            ),
            // A broker that sends a sensitive value anyway: it is withheld.
            (
                DescribedConfig {
                    name: "sasl.jaas.config".into(),
                    value: Some("hunter2".into()),
                    sensitive: true,
                    synonyms: vec![synonym(3, "sasl.jaas.config", Some("hunter2"))],
                },
                "  sasl.jaas.config=null sensitive=true synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:sasl.jaas.config=null}",
            ),
            (
                DescribedConfig {
                    name: "x".into(),
                    value: Some(String::new()),
                    sensitive: false,
                    synonyms: Vec::new(),
                },
                "  x= sensitive=false synonyms={}",
            ),
        ];
        for (config, expected) in cases {
            check!(config.line() == expected);
            check!(!config.json().to_string().contains("hunter2"));
        }
    }

    #[test]
    fn a_sensitive_entry_renders_a_null_value_in_json() {
        let config = DescribedConfig {
            name: "sasl.jaas.config".into(),
            value: Some("hunter2".into()),
            sensitive: true,
            synonyms: vec![synonym(8, "sasl.jaas.config", Some("hunter2"))],
        };
        check!(
            config.json()
                == json!({
                    "name": "sasl.jaas.config",
                    "value": null,
                    "sensitive": true,
                    "synonyms": [{"source": "DYNAMIC_GROUP_CONFIG", "name": "sasl.jaas.config", "value": null}],
                })
        );
    }

    #[test]
    fn a_config_entry_keeps_its_value_sensitivity_and_synonyms() {
        use krabka_client_admin::{ConfigSource, ConfigSynonym, ConfigType};
        let entry = ConfigEntry {
            name: "retention.ms".into(),
            value: Some("1000".into()),
            source: ConfigSource::DynamicTopicConfig,
            is_sensitive: false,
            is_read_only: false,
            synonyms: vec![
                ConfigSynonym {
                    name: "retention.ms".into(),
                    value: Some("1000".into()),
                    source: ConfigSource::DynamicTopicConfig,
                },
                ConfigSynonym {
                    name: "log.retention.ms".into(),
                    value: None,
                    source: ConfigSource::DefaultConfig,
                },
            ],
            config_type: ConfigType::Unknown,
            documentation: None,
        };
        check!(
            DescribedConfig::from(&entry)
                == DescribedConfig {
                    name: "retention.ms".into(),
                    value: Some("1000".into()),
                    sensitive: false,
                    synonyms: vec![
                        synonym(1, "retention.ms", Some("1000")),
                        synonym(5, "log.retention.ms", None),
                    ],
                }
        );
    }

    #[test]
    fn headers_name_the_entity_as_kafka_does() {
        let cases = [
            ("topics", "t1", false, "Dynamic configs for topic t1 are:"),
            ("topics", "t1", true, "All configs for topic t1 are:"),
            ("brokers", "1", false, "Dynamic configs for broker 1 are:"),
            (
                "brokers",
                "",
                false,
                "Default configs for brokers in the cluster are:",
            ),
            (
                "client-metrics",
                "cm",
                false,
                "Dynamic configs for client-metric cm are:",
            ),
        ];
        for (entity_type, entity, all, expected) in cases {
            check!(header(entity_type, entity, all) == expected);
        }
        check!(
            missing("topics", "nope")
                == "The topic 'nope' doesn't exist and doesn't have dynamic config."
        );
    }

    fn entity(entries: &[(&str, Option<&str>)]) -> ClientQuotaEntity {
        entries
            .iter()
            .map(|(entity_type, name)| ((*entity_type).to_owned(), name.map(str::to_owned)))
            .collect()
    }

    #[test]
    fn quota_lines_print_as_kafka_prints_them() {
        let values = |entries: &[(&str, f64)]| {
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), *value))
                .collect::<BTreeMap<_, _>>()
        };
        let cases = [
            (
                entity(&[("user", Some("alice"))]),
                values(&[
                    ("consumer_byte_rate", 1024.0),
                    ("producer_byte_rate", 2.0e7),
                    ("request_percentage", 12.5),
                    ("controller_mutation_rate", 3.0),
                ]),
                "Quota configs for user-principal 'alice' are producer_byte_rate=2.0E7, consumer_byte_rate=1024.0, controller_mutation_rate=3.0, request_percentage=12.5",
            ),
            (
                entity(&[("client-id", Some("c1")), ("user", None)]),
                values(&[("consumer_byte_rate", 1024.0)]),
                "Quota configs for the default user-principal, client-id 'c1' are consumer_byte_rate=1024.0",
            ),
            (
                entity(&[("client-id", None)]),
                values(&[("request_percentage", 50.0)]),
                "Quota configs for the default client-id are request_percentage=50.0",
            ),
            (
                entity(&[("ip", Some("1.2.3.4"))]),
                values(&[("connection_creation_rate", 10.0)]),
                "Quota configs for ip '1.2.3.4' are connection_creation_rate=10.0",
            ),
            (
                entity(&[("user", Some("alice"))]),
                values(&[]),
                "Quota configs for user-principal 'alice' are ",
            ),
        ];
        for (entity, values, expected) in cases {
            check!(quota_line(&entity, &values) == expected);
        }
    }

    #[test]
    fn scram_lines_print_as_kafka_prints_them() {
        let user = |credentials: Vec<UserScramCredential>, error: Option<KafkaError>| {
            UserScramCredentials {
                username: "alice".into(),
                credentials,
                error,
            }
        };
        let cases = [
            (
                user(
                    vec![
                        UserScramCredential {
                            mechanism: "SCRAM-SHA-256".into(),
                            iterations: 8192,
                        },
                        UserScramCredential {
                            mechanism: "SCRAM-SHA-512".into(),
                            iterations: 4096,
                        },
                    ],
                    None,
                ),
                "SCRAM credential configs for user-principal 'alice' are SCRAM-SHA-256=iterations=8192, SCRAM-SHA-512=iterations=4096",
            ),
            (
                user(
                    Vec::new(),
                    Some(KafkaError {
                        code: 31,
                        name: "CLUSTER_AUTHORIZATION_FAILED",
                        message: None,
                    }),
                ),
                "Error retrieving SCRAM credential configs for user-principal 'alice': ExecutionException: org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.",
            ),
            (
                user(
                    Vec::new(),
                    Some(KafkaError {
                        code: 58,
                        name: "SASL_AUTHENTICATION_FAILED",
                        message: Some("denied".into()),
                    }),
                ),
                "Error retrieving SCRAM credential configs for user-principal 'alice': ExecutionException: org.apache.kafka.common.errors.SaslAuthenticationException: denied",
            ),
        ];
        for (user, expected) in cases {
            check!(scram_line(&user) == expected);
        }
    }

    #[test]
    fn scram_users_skip_unknown_users_and_read_the_first_result() {
        let result = |name: &str, code: Option<i16>, iterations: i32| UserScramCredentials {
            username: name.into(),
            credentials: vec![UserScramCredential {
                mechanism: "SCRAM-SHA-256".into(),
                iterations,
            }],
            error: code.map(|code| KafkaError {
                code,
                name: "",
                message: None,
            }),
        };
        let described = [
            result("bob", Some(91), 0),
            result("alice", None, 4096),
            result("alice", None, 8192),
        ];
        check!(scram_users(&described) == vec![&described[1], &described[1]]);
    }
}
