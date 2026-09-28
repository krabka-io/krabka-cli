//! Per-broker connections for commands whose request answers only for the
//! broker it reaches, such as `DescribeLogDirs` and `ListGroups`.
//!
//! Kafka's admin client sends those requests to each broker in turn. The
//! pinned `AdminClient` talks to one broker, so these helpers read the broker
//! list from the cluster metadata and open an `AdminClient` on each broker.

use std::collections::BTreeMap;

use krabka_client_admin::AdminClient;
use krabka_client_core::{Client, ConnectionOptions};

use crate::{connection::ConnectionArgs, output::CommandError};

/// The most brokers that a command holds a connection to at once when it
/// asks every broker.
pub const BROKER_FAN_OUT: usize = 8;

/// `host:port`, with an IPv6 host in brackets.
fn endpoint(host: &str, port: i32) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The id and `host:port` of every broker in the cluster metadata, read
/// through `--bootstrap-server`.
///
/// # Errors
/// Returns the failure of the metadata request, or a refusal when
/// `--bootstrap-server` is not given.
pub async fn cluster_brokers(
    connection: &ConnectionArgs,
    options: &ConnectionOptions,
) -> Result<BTreeMap<i32, String>, CommandError> {
    if connection.bootstrap_server.is_empty() {
        return Err("Missing required argument \"[bootstrap-server]\"".into());
    }
    let client = Client::builder()
        .bootstrap(connection.bootstrap_server.join(","))
        .client_id(options.client_id.clone())
        .connect_timeout(options.connect_timeout)
        .request_timeout(options.request_timeout)
        .maybe_security(options.security.as_deref().cloned())
        .build()
        .await
        .map_err(|error| error.to_string())?;
    let metadata = client.refresh_metadata().await;
    client.close();
    let metadata = metadata.map_err(|error| error.to_string())?;
    Ok(metadata
        .brokers
        .into_iter()
        .map(|broker| (broker.node_id, endpoint(&broker.host, broker.port)))
        .collect())
}

/// An admin client connected to the broker at `endpoint`.
///
/// # Errors
/// Returns the failure to connect.
pub async fn broker_client(
    endpoint: &str,
    options: &ConnectionOptions,
) -> Result<AdminClient, CommandError> {
    Ok(AdminClient::connect_with_options(&[endpoint.to_owned()], options.clone()).await?)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn endpoints_bracket_ipv6_hosts() {
        let cases = [
            (endpoint("broker-1", 9092), "broker-1:9092"),
            (endpoint("::1", 9092), "[::1]:9092"),
            (endpoint("[::1]", 9092), "[::1]:9092"),
        ];
        for (actual, expected) in cases {
            check!(actual == expected);
        }
    }
}
