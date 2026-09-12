//! Broker-backed login for the admin UI.

use std::{fmt, future::Future, pin::Pin, time::Duration};

use krabka_client_admin::{AdminClient, AdminError};
use krabka_client_core::security::{ClientSecurity, SaslCredentials};
use krabka_security::SaslMechanism;
use krabka_units::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    admin::AdminFacade,
    config::AdminUiConfig,
    error::UiError,
    permissions::{Capabilities, derive_capabilities},
    session::{SessionCredentials, SessionStore, SessionUser},
};

/// How long the login path waits for one bootstrap address to accept a TCP
/// connection while it decides why an authenticated connect failed.
const BROKER_PROBE_TIMEOUT: Time = secs(2);

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct LoginSuccess {
    pub username: String,
    pub principal: String,
    pub session_id: String,
}

impl fmt::Debug for LoginSuccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginSuccess")
            .field("username", &self.username)
            .field("principal", &self.principal)
            .field("session_id", &"<redacted>")
            .finish()
    }
}

#[must_use]
pub fn build_scram_sha512_security(
    cfg: &AdminUiConfig,
    username: &str,
    password: &str,
) -> ClientSecurity {
    ClientSecurity {
        protocol: cfg.security.listener_protocol(),
        tls: cfg.security.tls(),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: username.to_string(),
            password: password.to_string(),
        }),
        sasl_host: None,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AdminClientLoginBroker;

pub trait LoginBroker {
    /// Signs the operator in and reports what the operator's ACLs permit.
    ///
    /// Returns [`UiError::NotAuthenticated`] when the broker rejects the
    /// credentials, and [`UiError::BrokerConnection`] when no broker answers.
    /// The caller reports those two as different HTTP statuses.
    fn authenticate<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>>;
}

impl LoginBroker for AdminClientLoginBroker {
    fn authenticate<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>> {
        Box::pin(async move {
            let security = build_scram_sha512_security(cfg, username, password);
            let client =
                match AdminClient::connect_secured(&cfg.bootstrap_addrs, Some(security)).await {
                    Ok(client) => client,
                    Err(error) => return Err(classify_connect_failure(cfg, error).await),
                };

            Ok(capabilities_from_broker(client, username).await)
        })
    }
}

impl<T: LoginBroker + ?Sized> LoginBroker for &T {
    fn authenticate<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Capabilities, UiError>> + Send + 'a>> {
        (*self).authenticate(cfg, username, password)
    }
}

/// Reads the operator's ACLs and derives what the UI shows.
///
/// A broker that refuses the ACL read, and an operator that matches no ACL at
/// all, both give every capability: a Kafka super user bypasses ACLs and so
/// matches no entry, and a cluster that runs without an authorizer refuses the
/// read. The UI cannot tell either case from a truly ungranted operator, so it
/// shows every page and lets the broker refuse what the operator may not do.
async fn capabilities_from_broker(client: AdminClient, username: &str) -> Capabilities {
    let mut facade = AdminFacade::new(client);
    let principal = format!("User:{username}");

    let Ok(entries) = facade.acl_entries().await else {
        return Capabilities::all();
    };
    let derived = derive_capabilities(&principal, &entries);

    if derived == Capabilities::none() {
        return Capabilities::all();
    }

    derived
}

/// Separates a rejected password from an unreachable broker.
///
/// `AdminClient::connect_secured` reports every broker-bootstrap failure as
/// `AdminError::Connect`, whatever the cause, so the error alone cannot say
/// which happened. A TCP probe of the same addresses decides it: a broker that
/// accepts a TCP connection runs, so the authenticated connect failed in the
/// handshake, and for this UI that means the credentials.
async fn classify_connect_failure(cfg: &AdminUiConfig, error: AdminError) -> UiError {
    if !matches!(error, AdminError::Connect { .. }) {
        return UiError::from(error);
    }

    if any_address_accepts_tcp(&cfg.bootstrap_addrs, BROKER_PROBE_TIMEOUT).await {
        return UiError::NotAuthenticated;
    }

    UiError::from(error)
}

/// Reports whether any of the addresses accepts a TCP connection inside the
/// timeout.
pub async fn any_address_accepts_tcp(addrs: &[String], timeout: Time) -> bool {
    let deadline = Duration::try_from_secs_f64(timeout.secs_f64()).unwrap_or(Duration::ZERO);

    for addr in addrs {
        if let Ok(Ok(stream)) =
            tokio::time::timeout(deadline, tokio::net::TcpStream::connect(addr.as_str())).await
        {
            drop(stream);
            return true;
        }
    }

    false
}

pub struct AuthService<'a, B = AdminClientLoginBroker> {
    cfg: &'a AdminUiConfig,
    sessions: &'a SessionStore,
    broker: B,
}

impl<'a> AuthService<'a, AdminClientLoginBroker> {
    #[must_use]
    pub const fn new(cfg: &'a AdminUiConfig, sessions: &'a SessionStore) -> Self {
        Self {
            cfg,
            sessions,
            broker: AdminClientLoginBroker,
        }
    }
}

impl<'a, B: LoginBroker> AuthService<'a, B> {
    #[must_use]
    pub const fn new_with_broker(
        cfg: &'a AdminUiConfig,
        sessions: &'a SessionStore,
        broker: B,
    ) -> Self {
        Self {
            cfg,
            sessions,
            broker,
        }
    }

    /// # Errors
    /// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
    pub async fn login(&self, request: LoginRequest) -> Result<LoginSuccess, UiError> {
        let capabilities = self
            .broker
            .authenticate(self.cfg, &request.username, &request.password)
            .await?;

        let principal = format!("User:{}", request.username);
        let session_id = self.sessions.create_authenticated(
            SessionUser {
                username: request.username.clone(),
                principal: principal.clone(),
            },
            SessionCredentials::scram_sha512(request.password),
            capabilities,
        );

        Ok(LoginSuccess {
            username: request.username,
            principal,
            session_id: session_id.expose_for_cookie().to_string(),
        })
    }
}
