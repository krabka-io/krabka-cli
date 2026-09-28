//! `krabka configs` end to end: the binary against a scripted broker that
//! records the body of every request, so a test decodes the exact request
//! the command sent.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::{assert, check};
use krabka_protocol::{
    Decode, Encode, ProtocolRequest,
    owned::{
        alter_client_quotas_request::{
            AlterClientQuotasRequest, EntityData as AlterEntity, EntryData as AlterEntry, OpData,
        },
        alter_client_quotas_response::{
            AlterClientQuotasResponse, EntityData as ResultEntity, EntryData as AlterResult,
        },
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialDeletion,
        },
        alter_user_scram_credentials_response::{
            AlterUserScramCredentialsResponse, AlterUserScramCredentialsResult,
        },
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
        describe_client_quotas_response::{
            DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
        },
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
        },
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
        metadata_request::MetadataRequest,
        metadata_response::{MetadataResponse, MetadataResponseTopic},
    },
    primitives::varint::get_uvarint,
};
use serde_json::{Value, json};

// One recorded request: its API key, its version, and the bytes after its
// correlation id.
type Request = (i16, i16, Vec<u8>);

// A broker that answers each API with the highest version the client
// supports, from a table of canned responses, and records every request.
struct Broker {
    inner: krabka_client_core::MockBroker,
    requests: Arc<Mutex<Vec<Request>>>,
}

// One canned response: the API it answers and its encoder, which takes the
// request's version and raw bytes.
type Canned = (i16, i16, Box<dyn Fn(i16, &[u8]) -> Vec<u8> + Send>);

fn canned<R: ProtocolRequest>(response: R::Response) -> Canned
where
    R::Response: Encode + Send + 'static,
{
    (
        R::API_KEY,
        R::MAX_VERSION,
        Box::new(move |version, _| encode_response::<R>(&response, version)),
    )
}

fn encode_response<R: ProtocolRequest>(response: &R::Response, version: i16) -> Vec<u8>
where
    R::Response: Encode,
{
    let mut body = Vec::new();
    if version >= R::FLEXIBLE_MIN {
        body.push(0);
    }
    response.encode(&mut body, version).unwrap();
    body
}

// A response that `answer` computes from the decoded request.
fn replier<R: ProtocolRequest + for<'de> Decode<'de>>(
    answer: impl Fn(R) -> R::Response + Send + 'static,
) -> Canned
where
    R::Response: Encode,
{
    (
        R::API_KEY,
        R::MAX_VERSION,
        Box::new(move |version, request| {
            encode_response::<R>(&answer(decode_request::<R>(version, request)), version)
        }),
    )
}

impl Broker {
    async fn start(replies: Vec<Canned>) -> Self {
        let versions = ApiVersionsResponse {
            api_keys: std::iter::once((api_versions_request::API_KEY, 3))
                .chain(replies.iter().map(|(key, max, _)| (*key, *max)))
                .map(|(api_key, max_version)| ApiVersion {
                    api_key,
                    min_version: 0,
                    max_version,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut versions_body = Vec::new();
        versions.encode(&mut versions_body, 0).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        let replies = replies
            .into_iter()
            .map(|(key, _, encode)| (key, encode))
            .collect::<BTreeMap<_, _>>();
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, body| {
            log.lock().unwrap().push((api_key, version, body.to_vec()));
            if api_key == api_versions_request::API_KEY {
                return Some(versions_body.clone());
            }
            replies.get(&api_key).map(|encode| encode(version, body))
        })
        .await;
        Self { inner, requests }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    // The API keys of the requests after `ApiVersions`, in order.
    fn api_keys(&self) -> Vec<i16> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(key, _, _)| *key)
            .filter(|key| *key != api_versions_request::API_KEY)
            .collect()
    }

    // Every raw request, header bytes after the correlation id included.
    fn raw(&self) -> Vec<Vec<u8>> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, body)| body.clone())
            .collect()
    }

    // The decoded requests of type `R`.
    fn decoded<R: ProtocolRequest + for<'de> Decode<'de>>(&self) -> Vec<R> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _, _)| *key == R::API_KEY)
            .map(|(_, version, body)| decode_request::<R>(*version, body))
            .collect()
    }
}

// Skips the rest of the request header, the nullable client id and the
// tagged fields of a flexible version, and decodes the body.
fn decode_request<R: ProtocolRequest + for<'de> Decode<'de>>(version: i16, bytes: &[u8]) -> R {
    let client_id_len = i16::from_be_bytes([bytes[0], bytes[1]]);
    let mut body = &bytes[2 + usize::try_from(client_id_len.max(0)).unwrap()..];
    if version >= R::FLEXIBLE_MIN {
        let fields = get_uvarint(&mut body).unwrap();
        assert!(fields == 0);
    }
    let request = R::decode(&mut body, version).unwrap();
    assert!(body.is_empty());
    request
}

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(args: Vec<String>, rust_log: &'static str) -> Run {
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .env("RUST_LOG", rust_log)
            .env_remove("KRABKA_OUTPUT")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        Run {
            code: out.status.code(),
            stdout: String::from_utf8(out.stdout).unwrap(),
            stderr: String::from_utf8(out.stderr).unwrap(),
        }
    })
    .await
    .unwrap()
}

fn configs(broker: &Broker, argv: &[&str]) -> Vec<String> {
    ["configs", "--bootstrap-server", &broker.address()]
        .iter()
        .chain(argv)
        .map(|arg| (*arg).to_owned())
        .collect()
}

fn json_output(argv: Vec<String>) -> Vec<String> {
    ["--output".to_owned(), "json".to_owned()]
        .into_iter()
        .chain(argv)
        .collect()
}

fn metadata(topics: &[&str]) -> Canned {
    canned::<MetadataRequest>(MetadataResponse {
        topics: topics
            .iter()
            .map(|name| MetadataResponseTopic {
                name: Some((*name).into()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

fn config(
    name: &str,
    value: Option<&str>,
    source: i8,
    sensitive: bool,
) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult {
        name: name.into(),
        value: value.map(Into::into),
        config_source: source,
        is_sensitive: sensitive,
        ..Default::default()
    }
}

fn describe_configs(topic: &str, entries: Vec<DescribeConfigsResourceResult>) -> Canned {
    canned::<DescribeConfigsRequest>(DescribeConfigsResponse {
        results: vec![DescribeConfigsResult {
            resource_type: 2,
            resource_name: topic.into(),
            configs: entries,
            ..Default::default()
        }],
        ..Default::default()
    })
}

// `DescribeConfigs` that answers each asked topic with no configs.
fn describe_configs_of_each_asked_topic() -> Canned {
    replier::<DescribeConfigsRequest>(|request: DescribeConfigsRequest| DescribeConfigsResponse {
        results: request
            .resources
            .into_iter()
            .map(|resource| DescribeConfigsResult {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_topic_sends_one_describe_configs_and_prints_the_jvm_lines() {
    let cases = [
        (
            vec![
                config("retention.ms", Some("1000"), 1, false),
                config("cleanup.policy", Some("compact,delete"), 1, false),
                config("segment.bytes", Some("1073741824"), 5, false),
            ],
            "Dynamic configs for topic t1 are:\n  cleanup.policy=compact,delete sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact,delete}\n  retention.ms=1000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=1000}\n",
        ),
        (Vec::new(), "Dynamic configs for topic t1 are:\n"),
    ];
    for (entries, expected) in cases {
        let broker = Broker::start(vec![
            metadata(&["t1", "other"]),
            describe_configs("t1", entries),
        ])
        .await;
        let run = krabka(
            configs(
                &broker,
                &[
                    "--describe",
                    "--entity-type",
                    "topics",
                    "--entity-name",
                    "t1",
                ],
            ),
            "off",
        )
        .await;
        check!((run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), expected, ""));
        check!(broker.api_keys() == [MetadataRequest::API_KEY, DescribeConfigsRequest::API_KEY]);
        check!(
            broker.decoded::<DescribeConfigsRequest>()
                == [DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: 2,
                        resource_name: "t1".into(),
                        configuration_keys: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                }]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_a_missing_topic_prints_the_kafka_notice_and_succeeds() {
    let broker = Broker::start(vec![metadata(&["t1"]), describe_configs("t1", Vec::new())]).await;
    let run = krabka(configs(&broker, &["--describe", "--topic", "nope"]), "off").await;
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "The topic 'nope' doesn't exist and doesn't have dynamic config.\n"
            )
    );
    check!(broker.api_keys() == [MetadataRequest::API_KEY]);
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_of_every_topic_follows_the_order_of_the_jvm_topic_set() {
    let broker = Broker::start(vec![
        metadata(&["t1", "orders", "payments"]),
        describe_configs_of_each_asked_topic(),
    ])
    .await;
    let run = krabka(
        configs(&broker, &["--describe", "--entity-type", "topics"]),
        "off",
    )
    .await;
    check!(
        run.stdout
            == "Dynamic configs for topic payments are:\nDynamic configs for topic orders are:\nDynamic configs for topic t1 are:\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_in_json_withholds_a_sensitive_value() {
    let broker = Broker::start(vec![
        metadata(&["t1"]),
        describe_configs("t1", vec![config("retention.ms", Some("1000"), 1, false)]),
    ])
    .await;
    let run = krabka(
        json_output(configs(&broker, &["--describe", "--topic", "t1"])),
        "off",
    )
    .await;
    let data: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(
        data == json!({"data": [{
            "entity_type": "topics",
            "entity_name": "t1",
            "exists": true,
            "configs": [{
                "name": "retention.ms",
                "value": "1000",
                "sensitive": false,
                "synonyms": [{"source": "DYNAMIC_TOPIC_CONFIG", "name": "retention.ms", "value": "1000"}],
            }],
        }]})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_topic_sends_deletes_then_sets_and_prints_the_kafka_line() {
    let broker = Broker::start(vec![canned::<IncrementalAlterConfigsRequest>(
        IncrementalAlterConfigsResponse {
            responses: vec![AlterConfigsResourceResponse {
                resource_type: 2,
                resource_name: "t1".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--entity-type",
                "topics",
                "--entity-name",
                "t1",
                "--add-config",
                "retention.ms=1000,cleanup.policy=[compact,delete]",
                "--delete-config",
                "segment.bytes",
            ],
        ),
        "off",
    )
    .await;
    check!(
        (run.code, run.stdout.as_str()) == (Some(0), "Completed updating config for topic t1.\n")
    );
    let alterable = |name: &str, operation: i8, value: Option<&str>| AlterableConfig {
        name: name.into(),
        config_operation: operation,
        value: value.map(Into::into),
        ..Default::default()
    };
    check!(
        broker.decoded::<IncrementalAlterConfigsRequest>()
            == [IncrementalAlterConfigsRequest {
                resources: vec![AlterConfigsResource {
                    resource_type: 2,
                    resource_name: "t1".into(),
                    configs: vec![
                        alterable("segment.bytes", 1, None),
                        alterable("cleanup.policy", 0, Some("compact,delete")),
                        alterable("retention.ms", 0, Some("1000")),
                    ],
                    ..Default::default()
                }],
                validate_only: false,
                ..Default::default()
            }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_topic_alter_fails_with_the_kafka_error() {
    let broker = Broker::start(vec![canned::<IncrementalAlterConfigsRequest>(
        IncrementalAlterConfigsResponse {
            responses: vec![AlterConfigsResourceResponse {
                error_code: 40,
                error_message: Some("Unknown topic config name: bogus.config".into()),
                resource_type: 2,
                resource_name: "t1".into(),
                ..Default::default()
            }],
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        json_output(configs(
            &broker,
            &["--alter", "--topic", "t1", "--add-config", "bogus.config=1"],
        )),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stdout.is_empty());
    let error: Value = serde_json::from_str(&run.stderr).unwrap();
    check!(
        error
            == json!({"error": {
                "code": 1,
                "message": "IncrementalAlterConfigs failed: INVALID_CONFIG (40): Unknown topic config name: bogus.config",
            }})
    );
}

fn user_quotas(values: &[(&str, f64)]) -> Canned {
    canned::<DescribeClientQuotasRequest>(DescribeClientQuotasResponse {
        entries: Some(if values.is_empty() {
            Vec::new()
        } else {
            vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                    ..Default::default()
                }],
                values: values
                    .iter()
                    .map(|(key, value)| ValueData {
                        key: (*key).into(),
                        value: *value,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }]
        }),
        ..Default::default()
    })
}

fn quota_filter() -> DescribeClientQuotasRequest {
    DescribeClientQuotasRequest {
        components: vec![ComponentData {
            entity_type: "user".into(),
            match_type: 0,
            match_: Some("alice".into()),
            ..Default::default()
        }],
        strict: true,
        ..Default::default()
    }
}

// The current quotas, the flags, and the ops that the alter sends.
type QuotaCase<'a> = (&'a [(&'a str, f64)], &'a [&'a str], Vec<OpData>);

fn altered_quotas() -> Canned {
    canned::<AlterClientQuotasRequest>(AlterClientQuotasResponse {
        entries: vec![AlterResult {
            entity: vec![ResultEntity {
                entity_type: "user".into(),
                entity_name: Some("alice".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_user_quotas_sends_the_diff_from_the_current_quotas() {
    let op = |key: &str, value: f64, remove: bool| OpData {
        key: key.into(),
        value,
        remove,
        ..Default::default()
    };
    let cases: [QuotaCase; 3] = [
        (
            &[],
            &["--add-config", "consumer_byte_rate=1024"],
            vec![op("consumer_byte_rate", 1024.0, false)],
        ),
        (
            &[("consumer_byte_rate", 1024.0), ("producer_byte_rate", 5.0)],
            &["--delete-config", "consumer_byte_rate"],
            vec![op("consumer_byte_rate", 0.0, true)],
        ),
        (
            &[("consumer_byte_rate", 1024.0)],
            &[
                "--add-config",
                "consumer_byte_rate=1024,producer_byte_rate=2e7",
            ],
            vec![op("producer_byte_rate", 2.0e7, false)],
        ),
    ];
    for (current, flags, ops) in cases {
        let broker = Broker::start(vec![user_quotas(current), altered_quotas()]).await;
        let argv = [
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
            ],
            flags,
        ]
        .concat();
        let run = krabka(configs(&broker, &argv), "off").await;
        check!(
            (run.code, run.stdout.as_str())
                == (Some(0), "Completed updating config for user alice.\n"),
            "{flags:?}"
        );
        check!(broker.decoded::<DescribeClientQuotasRequest>() == [quota_filter()]);
        check!(
            broker.decoded::<AlterClientQuotasRequest>()
                == [AlterClientQuotasRequest {
                    entries: vec![AlterEntry {
                        entity: vec![AlterEntity {
                            entity_type: "user".into(),
                            entity_name: Some("alice".into()),
                            ..Default::default()
                        }],
                        ops,
                        ..Default::default()
                    }],
                    validate_only: false,
                    ..Default::default()
                }],
            "{flags:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_a_quota_the_user_does_not_have_is_refused_before_any_change() {
    let broker = Broker::start(vec![
        user_quotas(&[("consumer_byte_rate", 1.0)]),
        altered_quotas(),
    ])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--user",
                "alice",
                "--delete-config",
                "producer_byte_rate",
            ],
        ),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(run.stderr == "krabka configs: Invalid config(s): producer_byte_rate\n");
    check!(broker.api_keys() == [DescribeClientQuotasRequest::API_KEY]);
}

fn altered_scram() -> Canned {
    canned::<AlterUserScramCredentialsRequest>(AlterUserScramCredentialsResponse {
        results: vec![AlterUserScramCredentialsResult {
            user: "alice".into(),
            ..Default::default()
        }],
        ..Default::default()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_upsert_sends_a_salted_password_and_the_plaintext_goes_nowhere() {
    const PLAINTEXT: &str = "plaintext-hunter2";
    let broker = Broker::start(vec![altered_scram()]).await;
    let spec = format!("SCRAM-SHA-512=[iterations=8192,password={PLAINTEXT}]");
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                "alice",
                "--add-config",
                &spec,
            ],
        ),
        "trace",
    )
    .await;
    check!(run.code == Some(0));
    check!(run.stdout == "Completed updating config for user alice.\n");
    check!(!run.stdout.contains(PLAINTEXT));
    check!(!run.stderr.contains(PLAINTEXT));
    check!(!broker.raw().iter().any(|bytes| {
        bytes
            .windows(PLAINTEXT.len())
            .any(|window| window == PLAINTEXT.as_bytes())
    }));
    let requests = broker.decoded::<AlterUserScramCredentialsRequest>();
    assert!(requests.len() == 1);
    let request = &requests[0];
    check!(request.deletions.is_empty());
    assert!(request.upsertions.len() == 1);
    let upsertion = &request.upsertions[0];
    check!(
        (
            upsertion.name.as_str(),
            upsertion.mechanism,
            upsertion.iterations
        ) == ("alice", 2, 8192)
    );
    check!(upsertion.salt.len() == 16);
    let expected = krabka_security::pbkdf2_salted(
        PLAINTEXT.as_bytes(),
        krabka_security::SaslMechanism::ScramSha512,
        8192,
        &upsertion.salt,
    );
    check!(upsertion.salted_password.as_ref() == expected.as_slice());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_delete_sends_the_mechanism_of_the_key() {
    let cases = [("SCRAM-SHA-256", 1), ("SCRAM-SHA-512", 2)];
    for (key, mechanism) in cases {
        let broker = Broker::start(vec![altered_scram()]).await;
        let run = krabka(
            configs(
                &broker,
                &["--alter", "--user", "alice", "--delete-config", key],
            ),
            "off",
        )
        .await;
        check!(run.code == Some(0), "{key}");
        check!(
            broker.decoded::<AlterUserScramCredentialsRequest>()
                == [AlterUserScramCredentialsRequest {
                    deletions: vec![ScramCredentialDeletion {
                        name: "alice".into(),
                        mechanism,
                        ..Default::default()
                    }],
                    upsertions: Vec::new(),
                    ..Default::default()
                }],
            "{key}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scram_delete_the_broker_refuses_fails_with_its_error() {
    let broker = Broker::start(vec![canned::<AlterUserScramCredentialsRequest>(
        AlterUserScramCredentialsResponse {
            results: vec![AlterUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 91,
                error_message: Some(
                    "Attempt to delete a user credential that does not exist".into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        },
    )])
    .await;
    let run = krabka(
        configs(
            &broker,
            &[
                "--alter",
                "--user",
                "alice",
                "--delete-config",
                "SCRAM-SHA-512",
            ],
        ),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    check!(
        run.stderr
            == "krabka configs: AlterUserScramCredentials failed: RESOURCE_NOT_FOUND (91): Attempt to delete a user credential that does not exist\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_user_prints_quotas_then_scram_credentials() {
    let scram = |results: Vec<DescribeUserScramCredentialsResult>| {
        canned::<DescribeUserScramCredentialsRequest>(DescribeUserScramCredentialsResponse {
            results,
            ..Default::default()
        })
    };
    let cases = [
        (
            &[
                ("consumer_byte_rate", 1024.0),
                ("producer_byte_rate", 2.0e7),
            ][..],
            vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                credential_infos: vec![
                    CredentialInfo {
                        mechanism: 1,
                        iterations: 8192,
                        ..Default::default()
                    },
                    CredentialInfo {
                        mechanism: 2,
                        iterations: 4096,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            "Quota configs for user-principal 'alice' are consumer_byte_rate=1024.0, producer_byte_rate=2.0E7\nSCRAM credential configs for user-principal 'alice' are SCRAM-SHA-256=iterations=8192, SCRAM-SHA-512=iterations=4096\n",
        ),
        (
            &[][..],
            vec![DescribeUserScramCredentialsResult {
                user: "alice".into(),
                error_code: 91,
                ..Default::default()
            }],
            "",
        ),
    ];
    for (quotas, results, expected) in cases {
        let broker = Broker::start(vec![user_quotas(quotas), scram(results)]).await;
        let run = krabka(
            configs(
                &broker,
                &[
                    "--describe",
                    "--entity-type",
                    "users",
                    "--entity-name",
                    "alice",
                ],
            ),
            "off",
        )
        .await;
        check!((run.code, run.stdout.as_str()) == (Some(0), expected));
        check!(broker.decoded::<DescribeClientQuotasRequest>() == [quota_filter()]);
        check!(
            broker.decoded::<DescribeUserScramCredentialsRequest>()
                == [DescribeUserScramCredentialsRequest {
                    users: Some(vec![UserName {
                        name: "alice".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unsupported_entity_type_fails_before_connecting() {
    let broker = Broker::start(Vec::new()).await;
    let run = krabka(
        json_output(configs(
            &broker,
            &[
                "--describe",
                "--entity-type",
                "brokers",
                "--entity-name",
                "1",
            ],
        )),
        "off",
    )
    .await;
    check!(run.code == Some(1));
    let error: Value = serde_json::from_str(&run.stderr).unwrap();
    check!(error["error"]["code"] == json!(1));
    check!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("--entity-type brokers is not supported by this build: it needs describe_configs and incremental_alter_configs for a config resource of any type")
    );
    check!(broker.raw().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn help_lists_the_kafka_configs_flags() {
    let run = krabka(vec!["configs".into(), "--help".into()], "off").await;
    check!(run.code == Some(0));
    for flag in [
        "--describe",
        "--alter",
        "--entity-type",
        "--entity-name",
        "--entity-default",
        "--add-config",
        "--add-config-file",
        "--delete-config",
        "--all",
        "--bootstrap-server",
        "--bootstrap-controller",
        "--command-config",
        "--topic",
        "--client",
        "--client-defaults",
        "--user",
        "--user-defaults",
        "--broker",
        "--broker-defaults",
        "--broker-logger",
        "--ip",
        "--ip-defaults",
        "--group",
        "--client-metrics",
    ] {
        check!(
            run.stdout.split_whitespace().any(|word| word == flag),
            "{flag}"
        );
    }
    for entity_type in [
        "topics",
        "clients",
        "users",
        "brokers",
        "broker-loggers",
        "ips",
        "client-metrics",
        "groups",
    ] {
        check!(run.stdout.contains(entity_type), "{entity_type}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alter_with_nothing_to_change_reports_the_entity_as_kafka_does() {
    let cases: [(&[&str], &str); 3] = [
        (
            &["--alter", "--client", "c1", "--add-config", ","],
            "Completed updating config for client c1.\n",
        ),
        (
            &["--alter", "--user", "alice", "--add-config", ","],
            "Completed updating config for user alice.\n",
        ),
        (
            &[
                "--alter",
                "--entity-type",
                "users",
                "--entity-default",
                "--add-config",
                ",",
            ],
            "Completed updating default config for users in the cluster.\n",
        ),
    ];
    for (argv, expected) in cases {
        let broker = Broker::start(Vec::new()).await;
        let run = krabka(configs(&broker, argv), "off").await;
        check!(
            (run.code, run.stdout.as_str()) == (Some(0), expected),
            "{argv:?}"
        );
        check!(broker.api_keys().is_empty(), "{argv:?}");
    }
}
