//! The `--describe` lines of `kafka-configs`, and their JSON rendering.

use krabka_client_admin::{TopicConfigOverrides, UserQuotaConfig, UserScramCredentials};
use serde_json::{Map, Value, json};

use super::java;
use crate::output::kafka_error;

/// `RESOURCE_NOT_FOUND`: the broker's answer for a user with no SCRAM
/// credential, which `DescribeUserScramCredentialsResult.users` leaves out.
const RESOURCE_NOT_FOUND: i16 = 91;

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG` on the wire.
pub const DYNAMIC_TOPIC_CONFIG: i8 = 1;

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

/// The entries of a topic, sorted by name, from the dynamic overrides that
/// the pinned `AdminClient::describe_configs` returns.
///
/// That call asks for no synonyms and keeps only the entries whose source is
/// `DYNAMIC_TOPIC_CONFIG` and that carry a value. Each entry is therefore
/// not sensitive, and its first synonym, the only one known here, is itself.
/// Kafka also lists the broker-level synonyms below it.
pub fn from_overrides(overrides: &TopicConfigOverrides) -> Vec<DescribedConfig> {
    overrides
        .overrides
        .iter()
        .map(|(name, value)| DescribedConfig {
            name: name.clone(),
            value: Some(value.clone()),
            sensitive: false,
            synonyms: vec![Synonym {
                source: DYNAMIC_TOPIC_CONFIG,
                name: name.clone(),
                value: Some(value.clone()),
            }],
        })
        .collect()
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

/// The quota line of a user, or `None` when it has no quota, as
/// `ConfigCommand.describeQuotaConfigs` prints it. The keys are in the order
/// of the `HashMap` that `DescribeClientQuotasResponse.complete` fills.
pub fn quota_line(user: &str, quotas: &UserQuotaConfig) -> Option<String> {
    if quotas.is_empty() {
        return None;
    }
    let entries = java::hash_map_order(quotas.keys().map(String::as_str), Some(quotas.len()))
        .into_iter()
        .map(|key| format!("{key}={}", java::double_to_string(quotas[key])))
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "Quota configs for user-principal '{user}' are {entries}"
    ))
}

pub fn quota_json(quotas: &UserQuotaConfig) -> Value {
    Value::Object(
        quotas
            .iter()
            .map(|(key, value)| (key.clone(), json!(value)))
            .collect::<Map<_, _>>(),
    )
}

/// The SCRAM line of a described user, or `None` for a user that the broker
/// does not know, as `describeClientQuotaAndUserScramCredentialConfigs`
/// prints it.
pub fn scram_line(user: &UserScramCredentials) -> Option<String> {
    match &user.error {
        Some(error) if error.code == RESOURCE_NOT_FOUND => None,
        Some(error) => {
            let (class, default_message) = exception(error.code, error.name);
            Some(format!(
                "Error retrieving SCRAM credential configs for user-principal '{}': {class}: {}",
                user.username,
                error.message.as_deref().unwrap_or(default_message)
            ))
        }
        None => Some(format!(
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
        )),
    }
}

pub fn scram_json(user: &UserScramCredentials) -> Value {
    json!({
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

/// The simple class name and the default message of the exception that
/// `Errors.forCode(code).exception(null)` builds, for the errors a SCRAM
/// describe can carry. Any other code is shown by its Kafka error name.
const fn exception(code: i16, name: &'static str) -> (&'static str, &'static str) {
    match code {
        -1 => (
            "UnknownServerException",
            "The server experienced an unexpected error when processing the request.",
        ),
        31 => (
            "ClusterAuthorizationException",
            "Cluster authorization failed.",
        ),
        35 => (
            "UnsupportedVersionException",
            "The version of API is not supported.",
        ),
        41 => (
            "NotControllerException",
            "This is not the correct controller for this cluster.",
        ),
        _ => (name, ""),
    }
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
    fn overrides_become_sorted_dynamic_entries() {
        let overrides = TopicConfigOverrides {
            topic: "t1".into(),
            overrides: BTreeMap::from([
                ("retention.ms".into(), "1000".into()),
                ("cleanup.policy".into(), "compact".into()),
            ]),
        };
        check!(
            from_overrides(&overrides)
                == vec![
                    DescribedConfig {
                        name: "cleanup.policy".into(),
                        value: Some("compact".into()),
                        sensitive: false,
                        synonyms: vec![synonym(1, "cleanup.policy", Some("compact"))],
                    },
                    DescribedConfig {
                        name: "retention.ms".into(),
                        value: Some("1000".into()),
                        sensitive: false,
                        synonyms: vec![synonym(1, "retention.ms", Some("1000"))],
                    },
                ]
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

    #[test]
    fn quota_lines_print_as_kafka_prints_them() {
        let quotas = |entries: &[(&str, f64)]| {
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), *value))
                .collect::<UserQuotaConfig>()
        };
        let cases = [
            (quotas(&[]), None),
            (
                quotas(&[
                    ("consumer_byte_rate", 1024.0),
                    ("producer_byte_rate", 2.0e7),
                    ("request_percentage", 12.5),
                    ("controller_mutation_rate", 3.0),
                ]),
                Some(
                    "Quota configs for user-principal 'alice' are producer_byte_rate=2.0E7, consumer_byte_rate=1024.0, controller_mutation_rate=3.0, request_percentage=12.5",
                ),
            ),
            (
                quotas(&[("consumer_byte_rate", 1024.0)]),
                Some("Quota configs for user-principal 'alice' are consumer_byte_rate=1024.0"),
            ),
        ];
        for (quotas, expected) in cases {
            check!(quota_line("alice", &quotas) == expected.map(str::to_owned));
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
                Some(
                    "SCRAM credential configs for user-principal 'alice' are SCRAM-SHA-256=iterations=8192, SCRAM-SHA-512=iterations=4096",
                ),
            ),
            (
                user(
                    Vec::new(),
                    Some(KafkaError {
                        code: 91,
                        name: "RESOURCE_NOT_FOUND",
                        message: Some("no such user".into()),
                    }),
                ),
                None,
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
                Some(
                    "Error retrieving SCRAM credential configs for user-principal 'alice': ClusterAuthorizationException: Cluster authorization failed.",
                ),
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
                Some(
                    "Error retrieving SCRAM credential configs for user-principal 'alice': SASL_AUTHENTICATION_FAILED: denied",
                ),
            ),
        ];
        for (user, expected) in cases {
            check!(scram_line(&user) == expected.map(str::to_owned));
        }
    }
}
