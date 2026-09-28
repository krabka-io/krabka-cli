//! `krabka acls` run as a subprocess against a scripted broker, asserting the
//! requests it sends, its exit code and its output.

use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
};

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_acls_request::{self, AclCreation, CreateAclsRequest},
        create_acls_response::{AclCreationResult, CreateAclsResponse},
        delete_acls_request::{self, DeleteAclsFilter, DeleteAclsRequest},
        delete_acls_response::{DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse},
        describe_acls_request::{self, DescribeAclsRequest},
        describe_acls_response::{AclDescription, DescribeAclsResource, DescribeAclsResponse},
    },
};
use serde_json::{Value, json};

const VERSION: i16 = 3;

// Kafka's wire codes.
const ANY: i8 = 1;
const TOPIC: i8 = 2;
const LITERAL: i8 = 3;
const PREFIXED: i8 = 4;
const READ: i8 = 3;
const WRITE: i8 = 4;
const CREATE: i8 = 5;
const DESCRIBE: i8 = 8;
const ALLOW: i8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Request {
    Describe(DescribeAclsRequest),
    Create(CreateAclsRequest),
    Delete(DeleteAclsRequest),
}

// A broker that answers each ACL API with one canned body and records every
// ACL request it decodes.
struct Broker {
    inner: krabka_client_core::MockBroker,
    received: Arc<Mutex<Vec<Request>>>,
}

fn encode<T: Encode>(message: &T, version: i16, header_tag: bool) -> Vec<u8> {
    let mut body = Vec::new();
    if header_tag {
        body.push(0);
    }
    message.encode(&mut body, version).unwrap();
    body
}

// The request body after the header's client id and, on a flexible version,
// its empty tagged-fields byte.
fn body(frame: &[u8]) -> &[u8] {
    let length = i16::from_be_bytes([frame[0], frame[1]]);
    let client_id = usize::try_from(length).unwrap_or(0);
    &frame[2 + client_id + 1..]
}

fn decode<T: for<'de> Decode<'de>>(frame: &[u8]) -> T {
    let mut bytes = body(frame);
    T::decode(&mut bytes, VERSION).unwrap()
}

impl Broker {
    async fn start(replies: BTreeMap<i16, Vec<u8>>) -> Self {
        let api_versions = encode(
            &ApiVersionsResponse {
                api_keys: [
                    (api_versions_request::API_KEY, 0, 3),
                    (describe_acls_request::API_KEY, 1, VERSION),
                    (create_acls_request::API_KEY, 1, VERSION),
                    (delete_acls_request::API_KEY, 1, VERSION),
                ]
                .into_iter()
                .map(|(api_key, min_version, max_version)| ApiVersion {
                    api_key,
                    min_version,
                    max_version,
                    ..Default::default()
                })
                .collect(),
                ..Default::default()
            },
            0,
            false,
        );
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&received);
        let inner = krabka_client_core::MockBroker::start(move |api_key, _, _, frame| {
            let request = match api_key {
                api_versions_request::API_KEY => return Some(api_versions.clone()),
                describe_acls_request::API_KEY => Request::Describe(decode(frame)),
                create_acls_request::API_KEY => Request::Create(decode(frame)),
                delete_acls_request::API_KEY => Request::Delete(decode(frame)),
                _ => return None,
            };
            log.lock().unwrap().push(request);
            replies.get(&api_key).cloned()
        })
        .await;
        Self { inner, received }
    }

    fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    fn received(&self) -> Vec<Request> {
        self.received.lock().unwrap().clone()
    }
}

fn described(resources: Vec<DescribeAclsResource>, error_code: i16) -> (i16, Vec<u8>) {
    (
        describe_acls_request::API_KEY,
        encode(
            &DescribeAclsResponse {
                error_code,
                error_message: (error_code != 0).then(|| "denied".into()),
                resources,
                ..Default::default()
            },
            VERSION,
            true,
        ),
    )
}

fn created(results: usize) -> (i16, Vec<u8>) {
    (
        create_acls_request::API_KEY,
        encode(
            &CreateAclsResponse {
                results: vec![AclCreationResult::default(); results],
                ..Default::default()
            },
            VERSION,
            true,
        ),
    )
}

fn deleted(matching_acls: Vec<DeleteAclsMatchingAcl>) -> (i16, Vec<u8>) {
    (
        delete_acls_request::API_KEY,
        encode(
            &DeleteAclsResponse {
                filter_results: vec![DeleteAclsFilterResult {
                    matching_acls,
                    ..Default::default()
                }],
                ..Default::default()
            },
            VERSION,
            true,
        ),
    )
}

fn topic(name: &str, pattern_type: i8, acls: &[(&str, i8)]) -> DescribeAclsResource {
    DescribeAclsResource {
        resource_type: TOPIC,
        resource_name: name.into(),
        pattern_type,
        acls: acls
            .iter()
            .map(|(principal, operation)| AclDescription {
                principal: (*principal).into(),
                host: "*".into(),
                operation: *operation,
                permission_type: ALLOW,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn describe(name: &str, pattern_type: i8) -> Request {
    Request::Describe(DescribeAclsRequest {
        resource_type_filter: TOPIC,
        resource_name_filter: Some(name.into()),
        pattern_type_filter: pattern_type,
        principal_filter: None,
        host_filter: None,
        operation: ANY,
        permission_type: ANY,
        ..Default::default()
    })
}

fn creation(principal: &str, name: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: TOPIC,
        resource_name: name.into(),
        resource_pattern_type: LITERAL,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: ALLOW,
        ..Default::default()
    }
}

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn krabka(address: &str, args: &[&str]) -> Run {
    let args = ["acls"]
        .iter()
        .chain(args)
        .chain(&[
            "--bootstrap-server",
            address,
            "--request-timeout-ms",
            "2000",
        ])
        .map(|arg| (*arg).to_owned())
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let out = Command::new(env!("CARGO_BIN_EXE_krabka"))
            .args(&args)
            .env("RUST_LOG", "off")
            .env_remove("KRABKA_ASSUME_YES")
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

#[tokio::test(flavor = "multi_thread")]
async fn list_sends_one_describe_built_from_the_flags_and_prints_kafka_s_shape() {
    let cases: [(&[&str], i8, &str); 2] = [
        (
            &["--list", "--topic", "t"],
            LITERAL,
            "Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=t, \
             patternType=LITERAL)`:\n\
             \t(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)\n\
             \t(principal=User:bob, host=*, operation=WRITE, permissionType=ALLOW)\n\
             \n",
        ),
        (
            &["--list", "--topic", "t", "--resource-pattern-type", "any"],
            ANY,
            "Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=t, \
             patternType=LITERAL)`:\n\
             \t(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)\n\
             \t(principal=User:bob, host=*, operation=WRITE, permissionType=ALLOW)\n\
             \n",
        ),
    ];
    for (args, pattern, stdout) in cases {
        let broker = Broker::start(BTreeMap::from([described(
            vec![topic(
                "t",
                LITERAL,
                &[("User:bob", WRITE), ("User:alice", READ)],
            )],
            0,
        )]))
        .await;
        let run = krabka(&broker.address(), args).await;
        check!(broker.received() == [describe("t", pattern)], "{args:?}");
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(0), stdout, ""),
            "{args:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_renders_the_json_envelope() {
    let broker = Broker::start(BTreeMap::from([described(
        vec![topic("orders", PREFIXED, &[("User:c", READ)])],
        0,
    )]))
    .await;
    let run = krabka(&broker.address(), &["--output", "json", "--list"]).await;
    check!(run.code == Some(0));
    let data: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(
        data == json!({"data": [{
            "resource": {"resource_type": "TOPIC", "name": "orders", "pattern_type": "PREFIXED"},
            "acls": [{"principal": "User:c", "host": "*", "operation": "READ", "permission_type": "ALLOW"}],
        }]})
    );
    check!(
        broker.received()
            == [Request::Describe(DescribeAclsRequest {
                resource_type_filter: ANY,
                resource_name_filter: None,
                pattern_type_filter: ANY,
                principal_filter: None,
                host_filter: None,
                operation: ANY,
                permission_type: ANY,
                ..Default::default()
            })]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn add_producer_creates_the_whole_operation_set_less_what_exists() {
    struct Case {
        existing: &'static [(&'static str, i8)],
        creations: Vec<AclCreation>,
        stdout: String,
    }
    let args = [
        "--add",
        "--allow-principal",
        "User:alice",
        "--producer",
        "--topic",
        "t",
    ];
    let header = "Adding ACLs for resource `ResourcePattern(resourceType=TOPIC, name=t, \
                  patternType=LITERAL)`: ";
    let cases = [
        Case {
            existing: &[],
            creations: vec![
                creation("User:alice", "t", WRITE),
                creation("User:alice", "t", CREATE),
                creation("User:alice", "t", DESCRIBE),
            ],
            stdout: format!(
                "{header}\n \
                 \t(principal=User:alice, host=*, operation=WRITE, permissionType=ALLOW)\n\
                 \t(principal=User:alice, host=*, operation=CREATE, permissionType=ALLOW)\n\
                 \t(principal=User:alice, host=*, operation=DESCRIBE, permissionType=ALLOW)\n\n"
            ),
        },
        Case {
            existing: &[("User:alice", WRITE), ("User:bob", CREATE)],
            creations: vec![
                creation("User:alice", "t", CREATE),
                creation("User:alice", "t", DESCRIBE),
            ],
            stdout: format!(
                "Acl (pattern=ResourcePattern(resourceType=TOPIC, name=t, patternType=LITERAL), \
                 entry=(principal=User:alice, host=*, operation=WRITE, permissionType=ALLOW)) \
                 already exists.\n\
                 {header}\n \
                 \t(principal=User:alice, host=*, operation=CREATE, permissionType=ALLOW)\n\
                 \t(principal=User:alice, host=*, operation=DESCRIBE, permissionType=ALLOW)\n\n"
            ),
        },
    ];
    for Case {
        existing,
        creations,
        stdout,
    } in cases
    {
        let broker = Broker::start(BTreeMap::from([
            described(vec![topic("t", LITERAL, existing)], 0),
            created(creations.len()),
        ]))
        .await;
        let run = krabka(&broker.address(), &args).await;
        check!(
            broker.received()
                == [
                    describe("t", LITERAL),
                    Request::Create(CreateAclsRequest {
                        creations,
                        ..Default::default()
                    }),
                ]
        );
        check!((run.code, run.stdout, run.stderr) == (Some(0), stdout, String::new()));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn add_of_entries_that_all_exist_creates_nothing() {
    let broker = Broker::start(BTreeMap::from([described(
        vec![topic("t", LITERAL, &[("User:a", READ)])],
        0,
    )]))
    .await;
    let run = krabka(
        &broker.address(),
        &[
            "--add",
            "--allow-principal",
            "User:a",
            "--operation",
            "Read",
            "--topic",
            "t",
        ],
    )
    .await;
    check!(broker.received() == [describe("t", LITERAL)]);
    check!(
        (run.code, run.stdout.as_str())
            == (
                Some(0),
                "Acl (pattern=ResourcePattern(resourceType=TOPIC, name=t, patternType=LITERAL), \
                 entry=(principal=User:a, host=*, operation=READ, permissionType=ALLOW)) already \
                 exists.\n"
            )
    );
}

fn delete_all(name: &str) -> Request {
    Request::Delete(DeleteAclsRequest {
        filters: vec![DeleteAclsFilter {
            resource_type_filter: TOPIC,
            resource_name_filter: Some(name.into()),
            pattern_type_filter: LITERAL,
            principal_filter: None,
            host_filter: None,
            operation: ANY,
            permission_type: ANY,
            ..Default::default()
        }],
        ..Default::default()
    })
}

fn matching(name: &str, principal: &str, operation: i8) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        resource_type: TOPIC,
        resource_name: name.into(),
        pattern_type: LITERAL,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: ALLOW,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_asks_first_and_force_or_yes_skip_the_question() {
    let refusal = "krabka acls: refusing to prompt for confirmation on a non-interactive stdin; \
                   pass --yes to proceed\n";
    let cases: [(&[&str], i32, &str); 3] = [
        (&[], 2, refusal),
        (&["--force"], 0, ""),
        (&["--yes"], 0, ""),
    ];
    for (extra, code, stderr) in cases {
        let broker = Broker::start(BTreeMap::from([deleted(vec![matching(
            "t", "User:a", READ,
        )])]))
        .await;
        let args = ["--remove", "--topic", "t"]
            .iter()
            .chain(extra)
            .copied()
            .collect::<Vec<_>>();
        let run = krabka(&broker.address(), &args).await;
        let requests = if code == 0 {
            vec![delete_all("t")]
        } else {
            Vec::new()
        };
        check!(broker.received() == requests, "{extra:?}");
        // Kafka prints nothing when a removal succeeds.
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(code), "", stderr),
            "{extra:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn remove_reports_what_it_removed_in_json() {
    let broker = Broker::start(BTreeMap::from([deleted(vec![matching(
        "t", "User:a", READ,
    )])]))
    .await;
    let run = krabka(
        &broker.address(),
        &["--output", "json", "--remove", "--force", "--topic", "t"],
    )
    .await;
    check!(run.code == Some(0));
    let data: Value = serde_json::from_str(&run.stdout).unwrap();
    check!(
        data == json!({"data": [{
            "filter": {"resource_type": "TOPIC", "name": "t", "pattern_type": "LITERAL"},
            "acls": [],
            "removed": [{
                "resource": {"resource_type": "TOPIC", "name": "t", "pattern_type": "LITERAL"},
                "acl": {"principal": "User:a", "host": "*", "operation": "READ", "permission_type": "ALLOW"},
            }],
            "error": null,
        }]})
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_reads_but_never_mutates() {
    let broker = Broker::start(BTreeMap::from([
        described(vec![topic("t", LITERAL, &[("User:a", READ)])], 0),
        deleted(Vec::new()),
        created(1),
    ]))
    .await;
    let remove = krabka(
        &broker.address(),
        &["--output", "json", "--remove", "--topic", "t", "--dry-run"],
    )
    .await;
    let add = krabka(
        &broker.address(),
        &[
            "--add",
            "--allow-principal",
            "User:b",
            "--operation",
            "Read",
            "--topic",
            "t",
            "--dry-run",
        ],
    )
    .await;
    check!(broker.received() == [describe("t", LITERAL), describe("t", LITERAL)]);
    let report: Value = serde_json::from_str(&remove.stdout).unwrap();
    check!(remove.code == Some(0));
    check!(report["dry_run"] == json!(true));
    check!(
        report["data"][0]["removed"]
            == json!([{
                "resource": {"resource_type": "TOPIC", "name": "t", "pattern_type": "LITERAL"},
                "acl": {"principal": "User:a", "host": "*", "operation": "READ", "permission_type": "ALLOW"},
            }])
    );
    check!(
        (add.code, add.stdout.as_str())
            == (
                Some(0),
                "DRY RUN: no change was made.\n\
                 Adding ACLs for resource `ResourcePattern(resourceType=TOPIC, name=t, \
                 patternType=LITERAL)`: \n \
                 \t(principal=User:b, host=*, operation=READ, permissionType=ALLOW)\n\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_broker_error_fails_the_command_with_the_kafka_error_name() {
    let broker = Broker::start(BTreeMap::from([described(Vec::new(), 31)])).await;
    let run = krabka(&broker.address(), &["--list"]).await;
    check!(
        (run.code, run.stdout.as_str(), run.stderr.as_str())
            == (
                Some(1),
                "",
                "krabka acls: DescribeAcls failed: CLUSTER_AUTHORIZATION_FAILED (31): denied\n"
            )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_command_line_exits_two_without_a_request() {
    let broker = Broker::start(BTreeMap::new()).await;
    let cases: [(&[&str], i32, &str); 3] = [
        (
            &["--list", "--producer"],
            2,
            "krabka acls: Option \"[list]\" can't be used with option \"[producer]\"\n",
        ),
        (
            &["--add", "--topic", "t"],
            2,
            "krabka acls: You must specify one of: --allow-principal, --deny-principal when \
             trying to add ACLs.\n",
        ),
        (
            &["--list", "--resource-pattern-type", "match", "--topic", "t"],
            1,
            "krabka acls: --resource-pattern-type match is not supported by this build: the \
             pinned krabka-client-admin has no MATCH ACL value\n",
        ),
    ];
    for (args, code, stderr) in cases {
        let run = krabka(&broker.address(), args).await;
        check!(
            (run.code, run.stdout.as_str(), run.stderr.as_str()) == (Some(code), "", stderr),
            "{args:?}"
        );
    }
    check!(broker.received() == []);
}
