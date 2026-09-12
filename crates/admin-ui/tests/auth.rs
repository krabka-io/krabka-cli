use std::{path::PathBuf, pin::Pin, sync::Mutex, time::Duration};

use assert2::assert;
use krabka_admin_ui::{
    auth::{
        AuthService, LoginBroker, LoginRequest, LoginSuccess, any_address_accepts_tcp,
        build_scram_sha512_security,
    },
    config::{AdminUiConfig, BrokerSecurityConfig},
    error::UiError,
    permissions::{Capabilities, derive_capabilities},
    session::{SessionId, SessionStore},
};
use krabka_client_admin::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use krabka_client_core::security::SaslCredentials;
use krabka_security::{ListenerProtocol, SaslMechanism};
use krabka_units::secs;

const SCRAM_PLAINTEXT_PASSWORD: &str = "password-sentinel";
const SCRAM_SSL_PASSWORD: &str = "tls-password-sentinel";
const EXPECTED_PASSWORD: &str = "login-password-sentinel";
const SESSION_ID_SENTINEL: &str = "session-id-sentinel";

#[test]
fn build_security_uses_scram_sha512_only() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };

    let security = build_scram_sha512_security(&cfg, "alice", SCRAM_PLAINTEXT_PASSWORD);

    assert!(security.protocol == ListenerProtocol::SaslPlaintext);
    assert!(security.tls.is_none());
    assert!(security.sasl_host.is_none());
    assert_scram_sha512_credentials(security.sasl.as_ref(), "alice", SCRAM_PLAINTEXT_PASSWORD);
}

#[test]
fn build_security_preserves_sasl_ssl_tls_material() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9093".to_string()],
        security: BrokerSecurityConfig::SaslSsl {
            trust_roots_pem: Some(PathBuf::from("ca.pem")),
            server_name: "broker.example.test".to_string(),
            client_identity: Some((PathBuf::from("client.crt"), PathBuf::from("client.key"))),
        },
        ..AdminUiConfig::default()
    };

    let security = build_scram_sha512_security(&cfg, "carol", SCRAM_SSL_PASSWORD);
    let tls = security.tls.expect("SASL_SSL carries TLS config");

    assert!(security.protocol == ListenerProtocol::SaslSsl);
    assert!(tls.trust_roots_pem == Some(PathBuf::from("ca.pem")));
    assert!(tls.server_name == "broker.example.test");
    assert!(
        tls.client_identity == Some((PathBuf::from("client.crt"), PathBuf::from("client.key")))
    );
    assert!(security.sasl_host.is_none());
    assert_scram_sha512_credentials(security.sasl.as_ref(), "carol", SCRAM_SSL_PASSWORD);
}

#[test]
fn login_success_debug_redacts_session_id() {
    let success = LoginSuccess {
        username: "alice".to_string(),
        principal: "User:alice".to_string(),
        session_id: SESSION_ID_SENTINEL.to_string(),
    };

    let debug = format!("{success:?}");

    assert!(debug.contains("alice"));
    assert!(debug.contains("User:alice"));
    assert!(debug.contains("<redacted>"));
    assert_debug_does_not_contain_secret(&debug, SESSION_ID_SENTINEL, "session id");
}

#[tokio::test]
async fn login_uses_broker_probe_and_creates_session() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };
    let sessions = SessionStore::new(Duration::from_mins(1));
    let broker = RecordingLoginBroker::default();
    let service = AuthService::new_with_broker(&cfg, &sessions, &broker);

    let success = service
        .login(LoginRequest {
            username: "alice".to_string(),
            password: EXPECTED_PASSWORD.to_string(),
        })
        .await
        .expect("broker probe succeeds");

    assert!(success.username == "alice");
    assert!(success.principal == "User:alice");
    let calls = broker.calls.lock().expect("calls lock is not poisoned");
    assert!(calls.len() == 1);
    let call = &calls[0];
    assert!(call.bootstrap_addrs == ["127.0.0.1:9092"]);
    assert!(call.username == "alice");
    assert!(call.password_matched);
    assert!(call.password_len == EXPECTED_PASSWORD.len());
    drop(calls);

    let session_id = SessionId::try_from(success.session_id.as_str()).expect("session id is valid");
    let session = sessions.get(&session_id).expect("login creates session");
    assert!(session.user.username == "alice");
    assert!(session.user.principal == "User:alice");
}

#[test]
fn recording_login_broker_calls_do_not_debug_raw_passwords() {
    let broker = RecordingLoginBroker::default();
    broker
        .calls
        .lock()
        .expect("calls lock is not poisoned")
        .push(RecordedLoginCall::from_parts(
            &["127.0.0.1:9092".to_string()],
            "alice",
            EXPECTED_PASSWORD,
        ));

    let debug = format!(
        "{:?}",
        broker
            .calls
            .lock()
            .expect("calls lock is not poisoned")
            .as_slice()
    );

    assert_debug_does_not_contain_secret(&debug, EXPECTED_PASSWORD, "password");
}

fn assert_scram_sha512_credentials(
    credentials: Option<&SaslCredentials>,
    expected_username: &str,
    expected_password: &str,
) {
    let Some(SaslCredentials::Scram {
        mechanism,
        username,
        password,
    }) = credentials
    else {
        panic!("expected SCRAM credentials");
    };

    assert!(*mechanism == SaslMechanism::ScramSha512);
    assert!(username == expected_username);
    assert!(
        password == expected_password,
        "SCRAM password did not match expected test sentinel"
    );
}

fn assert_debug_does_not_contain_secret(debug: &str, secret: &str, label: &str) {
    assert!(!debug.contains(secret), "debug output leaked {label}");
}

#[derive(Default)]
struct RecordingLoginBroker {
    calls: Mutex<Vec<RecordedLoginCall>>,
}

#[derive(Debug)]
struct RecordedLoginCall {
    bootstrap_addrs: Vec<String>,
    username: String,
    password_matched: bool,
    password_len: usize,
}

impl RecordedLoginCall {
    fn from_parts(bootstrap_addrs: &[String], username: &str, password: &str) -> Self {
        Self {
            bootstrap_addrs: bootstrap_addrs.to_vec(),
            username: username.to_string(),
            password_matched: password == EXPECTED_PASSWORD,
            password_len: password.len(),
        }
    }
}

impl LoginBroker for RecordingLoginBroker {
    fn authenticate<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().expect("calls lock is not poisoned").push(
                RecordedLoginCall::from_parts(&cfg.bootstrap_addrs, username, password),
            );
            Ok(topic_reader_capabilities())
        })
    }
}

/// What an operator with one topic-describe ACL may do.
fn topic_reader_capabilities() -> Capabilities {
    derive_capabilities(
        "User:alice",
        &[AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "*".to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        }],
    )
}

#[tokio::test]
async fn login_stores_the_capabilities_the_broker_reported() {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        ..AdminUiConfig::default()
    };
    let sessions = SessionStore::new(Duration::from_mins(1));
    let broker = RecordingLoginBroker::default();
    let service = AuthService::new_with_broker(&cfg, &sessions, &broker);

    let success = service
        .login(LoginRequest {
            username: "alice".to_string(),
            password: EXPECTED_PASSWORD.to_string(),
        })
        .await
        .expect("broker probe succeeds");

    let session_id = SessionId::try_from(success.session_id.as_str()).expect("session id is valid");
    let session = sessions.get(&session_id).expect("login creates session");

    assert!(session.capabilities == topic_reader_capabilities());
    assert!(session.capabilities.can_view_topics());
    assert!(!session.capabilities.can_alter_acls());
}

#[tokio::test]
async fn tcp_probe_separates_a_listening_broker_from_a_dead_address() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe listener binds");
    let listening = listener
        .local_addr()
        .expect("probe listener has an address");
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second listener binds");
    let dead_addr = dead.local_addr().expect("second listener has an address");
    drop(dead);

    assert!(any_address_accepts_tcp(&[listening.to_string()], secs(2)).await);
    assert!(!any_address_accepts_tcp(&[dead_addr.to_string()], secs(2)).await);
}
