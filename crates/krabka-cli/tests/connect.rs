//! `ConnectionArgs::connect` against a scripted broker.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_cli::connection::{ConnectionArgs, ConnectionError};
use krabka_client_admin::AdminError;
use krabka_client_core::ClientError;
use krabka_protocol::owned::{
    describe_cluster_request,
    describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    metadata_request,
    metadata_response::{MetadataResponse, MetadataResponseTopic},
};
use krabka_units::{Time, convert::TimeExt as _};

use self::support::{MockBroker, Received, Reply, respond};

const METADATA_VERSION: i16 = 12;

fn connection(bootstrap_server: Vec<String>, bootstrap_controller: Vec<String>) -> ConnectionArgs {
    ConnectionArgs {
        bootstrap_server,
        bootstrap_controller,
        command_config: None,
        client_id: None,
        request_timeout_ms: Some(300),
        timeout: Time::from_millis(2_000),
    }
}

fn metadata(topic_error: i16) -> Reply {
    respond(
        &MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("orders".into()),
                error_code: topic_error,
                ..Default::default()
            }],
            ..Default::default()
        },
        METADATA_VERSION,
        metadata_request::FLEXIBLE_MIN,
    )
}

async fn broker(reply: Reply) -> MockBroker {
    MockBroker::start(
        &[(metadata_request::API_KEY, 0, METADATA_VERSION)],
        BTreeMap::from([((metadata_request::API_KEY, METADATA_VERSION), reply)]),
    )
    .await
}

#[tokio::test]
async fn connect_reaches_the_broker_and_carries_per_topic_errors() {
    let broker = broker(metadata(3)).await;
    let mut client = connection(vec![broker.address()], Vec::new())
        .connect("test")
        .await
        .expect("connects");
    let metadata = client.metadata(&["orders"]).await.expect("metadata");
    assert!(metadata.topics.len() == 1);
    let error = metadata.topics[0].error.clone().expect("per-topic error");
    check!((error.code, error.name) == (3, "UNKNOWN_TOPIC_OR_PARTITION"));
    check!(
        broker.received()
            == [
                Received {
                    api_key: 18,
                    version: 0
                },
                Received {
                    api_key: metadata_request::API_KEY,
                    version: METADATA_VERSION
                },
            ]
    );
    broker.stop();
}

#[tokio::test]
async fn a_request_the_broker_drops_times_out() {
    let broker = broker(Reply::Silent).await;
    let mut client = connection(vec![broker.address()], Vec::new())
        .connect("test")
        .await
        .expect("connects");
    let error = client.metadata(&["orders"]).await.expect_err("times out");
    assert!(matches!(
        error,
        AdminError::Transport(ClientError::Timeout(timeout)) if timeout == Time::from_millis(300)
    ));
    broker.stop();
}

#[tokio::test]
async fn a_broker_error_during_controller_bootstrap_is_returned() {
    let broker = MockBroker::start(
        &[(describe_cluster_request::API_KEY, 0, 1)],
        BTreeMap::from([(
            (describe_cluster_request::API_KEY, 1),
            respond(
                &DescribeClusterResponse {
                    error_code: 31,
                    error_message: Some("denied".into()),
                    ..Default::default()
                },
                1,
                describe_cluster_request::FLEXIBLE_MIN,
            ),
        )]),
    )
    .await;
    let error = connection(Vec::new(), vec![broker.address()])
        .connect("test")
        .await
        .err()
        .expect("the broker error is returned");
    assert!(matches!(
        error,
        ConnectionError::Admin(AdminError::Broker {
            api: "DescribeCluster",
            code: 31,
            name: "CLUSTER_AUTHORIZATION_FAILED",
            ..
        })
    ));
    broker.stop();
}

#[tokio::test]
async fn bootstrap_controller_connects_through_describe_cluster() {
    let controller_reply = |port: i32| {
        respond(
            &DescribeClusterResponse {
                endpoint_type: 2,
                controller_id: 1,
                brokers: vec![DescribeClusterBroker {
                    broker_id: 1,
                    host: "127.0.0.1".into(),
                    port,
                    ..Default::default()
                }],
                ..Default::default()
            },
            1,
            describe_cluster_request::FLEXIBLE_MIN,
        )
    };
    // The bootstrap controller answers DescribeCluster with the active
    // controller, a second broker, and the client connects to that one.
    let target = MockBroker::start(
        &[
            (describe_cluster_request::API_KEY, 0, 1),
            (metadata_request::API_KEY, 0, METADATA_VERSION),
        ],
        BTreeMap::from([((metadata_request::API_KEY, METADATA_VERSION), metadata(0))]),
    )
    .await;
    let target_port = target
        .address()
        .rsplit_once(':')
        .unwrap()
        .1
        .parse()
        .unwrap();
    let bootstrap = MockBroker::start(
        &[(describe_cluster_request::API_KEY, 0, 1)],
        BTreeMap::from([(
            (describe_cluster_request::API_KEY, 1),
            controller_reply(target_port),
        )]),
    )
    .await;
    let mut client = connection(Vec::new(), vec![bootstrap.address()])
        .connect("test")
        .await
        .expect("connects through the controller");
    let metadata = client.metadata(&["orders"]).await.expect("metadata");
    check!(metadata.topics[0].error.is_none());
    check!(
        bootstrap
            .received()
            .iter()
            .any(|request| request.api_key == describe_cluster_request::API_KEY)
    );
    check!(
        target
            .received()
            .iter()
            .any(|request| request.api_key == metadata_request::API_KEY)
    );
    bootstrap.stop();
    target.stop();
}

#[tokio::test]
async fn connect_without_a_bootstrap_flag_is_refused() {
    let error = connection(Vec::new(), Vec::new())
        .connect("test")
        .await
        .err()
        .expect("refused");
    assert!(matches!(error, ConnectionError::MissingBootstrap));
}
