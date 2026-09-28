//! A scripted Kafka broker for tests that reach the network layer.
//!
//! [`MockBroker`] wraps `krabka_client_core::MockBroker`. It answers each
//! request from a table of canned responses keyed by `(api_key, version)`,
//! records every request it receives, and injects failures: a broker error is
//! a canned body with a non-zero error code, and a timeout is a request that it
//! does not answer, so the client reaches its request timeout.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
    },
};

/// What the broker does with one `(api_key, version)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// Send this response, the header tagged-fields byte included where the
    /// version needs it.
    Respond(Vec<u8>),
    /// Send nothing. The client reaches its request timeout.
    Silent,
}

/// One request that the broker received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Received {
    pub api_key: i16,
    pub version: i16,
}

/// A running scripted broker.
pub struct MockBroker {
    inner: krabka_client_core::MockBroker,
    received: Arc<Mutex<Vec<Received>>>,
}

impl MockBroker {
    /// Starts a broker that answers `ApiVersions` with `advertised` and every
    /// other request from `replies`. A request with no row is dropped, which
    /// the client sees as a timeout.
    pub async fn start(
        advertised: &[(i16, i16, i16)],
        replies: BTreeMap<(i16, i16), Reply>,
    ) -> Self {
        let api_versions = api_versions(advertised);
        let received = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&received);
        let inner = krabka_client_core::MockBroker::start(move |api_key, version, _, _| {
            log.lock().unwrap().push(Received { api_key, version });
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions.clone());
            }
            match replies.get(&(api_key, version)) {
                Some(Reply::Respond(body)) => Some(body.clone()),
                Some(Reply::Silent) | None => None,
            }
        })
        .await;
        Self { inner, received }
    }

    /// The `host:port` that the broker listens on.
    pub fn address(&self) -> String {
        self.inner.addr.to_string()
    }

    /// Every request received so far, `ApiVersions` included, in order.
    pub fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }

    /// Stops the broker.
    pub fn stop(self) {
        self.inner.stop();
    }
}

/// Encodes a response body for `version`, with the response header's
/// tagged-fields byte when the version is flexible.
pub fn respond<T: Encode>(message: &T, version: i16, flexible_min: i16) -> Reply {
    let mut body = Vec::new();
    if version >= flexible_min {
        body.push(0);
    }
    message
        .encode(&mut body, version)
        .expect("encode canned response");
    Reply::Respond(body)
}

fn api_versions(advertised: &[(i16, i16, i16)]) -> Vec<u8> {
    let response = ApiVersionsResponse {
        api_keys: std::iter::once((api_versions_request::API_KEY, 0, 3))
            .chain(advertised.iter().copied())
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut body = Vec::new();
    response.encode(&mut body, 0).expect("encode ApiVersions");
    body
}
