//! Kafka-compatible connection flags and `--command-config` parsing.
//!
//! Passwords are wrapped while they are parsed, but the upstream
//! `ConnectionOptions` and `SaslCredentials` debug implementations are not
//! redacted. Never log either value.

use std::{collections::BTreeMap, fmt, path::PathBuf};

use clap::Args;
use krabka_client_admin::{AdminClient, AdminError};
use krabka_client_core::{
    ConnectionOptions,
    security::{ClientSecurity, SaslCredentials, TlsConnectorConfig},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use krabka_units::{Time, convert::TimeExt as _};
use thiserror::Error;

#[derive(Debug, Args, Clone)]
pub struct ConnectionArgs {
    #[arg(
        long,
        env = "KRABKA_BOOTSTRAP_SERVER",
        value_delimiter = ',',
        conflicts_with = "bootstrap_controller"
    )]
    pub bootstrap_server: Vec<String>,
    #[arg(
        long,
        env = "KRABKA_BOOTSTRAP_CONTROLLER",
        value_delimiter = ',',
        conflicts_with = "bootstrap_server"
    )]
    pub bootstrap_controller: Vec<String>,
    #[arg(long)]
    pub command_config: Option<PathBuf>,
    #[arg(long)]
    pub client_id: Option<String>,
    #[arg(long)]
    pub request_timeout_ms: Option<i64>,
    #[arg(long, default_value = "30s", value_parser = parse_time)]
    pub timeout: Time,
}

fn parse_time(value: &str) -> Result<Time, String> {
    if let Ok(time) = value.parse() {
        return Ok(time);
    }
    let unit = value
        .find(char::is_alphabetic)
        .ok_or_else(|| "timeout must include a unit, for example 30s".to_string())?;
    format!("{} {}", &value[..unit], &value[unit..])
        .parse()
        .map_err(|error| format!("invalid timeout: {error}"))
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("one of --bootstrap-server or --bootstrap-controller is required")]
    MissingBootstrap,
    #[error("read command config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid command config: {0}")]
    Config(String),
    #[error(transparent)]
    Admin(#[from] AdminError),
}

pub struct Secret<T>(pub T);

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl ConnectionArgs {
    pub async fn connect(&self, command: &str) -> Result<AdminClient, ConnectionError> {
        let options = self.options(command).await?;
        if self.bootstrap_controller.is_empty() {
            if self.bootstrap_server.is_empty() {
                return Err(ConnectionError::MissingBootstrap);
            }
            Ok(AdminClient::connect_with_options(&self.bootstrap_server, options).await?)
        } else {
            Ok(
                AdminClient::connect_controller_with_options(&self.bootstrap_controller, options)
                    .await?,
            )
        }
    }

    pub async fn options(&self, command: &str) -> Result<ConnectionOptions, ConnectionError> {
        let properties = if let Some(path) = &self.command_config {
            let text =
                tokio::fs::read_to_string(path)
                    .await
                    .map_err(|source| ConnectionError::Read {
                        path: path.clone(),
                        source,
                    })?;
            parse_properties(&text)?
        } else {
            BTreeMap::new()
        };
        let mut options = ConnectionOptions {
            client_id: self
                .client_id
                .clone()
                .or_else(|| properties.get("client.id").cloned())
                .unwrap_or_else(|| format!("krabka-cli/{} {command}", env!("CARGO_PKG_VERSION"))),
            ..ConnectionOptions::default()
        };
        let request_timeout = if let Some(value) = self.request_timeout_ms {
            value
        } else {
            properties
                .get("request.timeout.ms")
                .map_or(Ok(30_000), |value| {
                    value.parse::<i64>().map_err(|_| {
                        ConnectionError::Config("request.timeout.ms must be an integer".into())
                    })
                })?
        };
        if request_timeout <= 0 {
            return Err(ConnectionError::Config(
                "request.timeout.ms must be positive".into(),
            ));
        }
        options.request_timeout = Time::from_millis(request_timeout);
        options.security = security(&properties, self.bootstrap_host())?.map(Box::new);
        Ok(options)
    }

    fn bootstrap_host(&self) -> &str {
        self.bootstrap_server
            .first()
            .or_else(|| self.bootstrap_controller.first())
            .map_or("localhost", |address| match address.strip_prefix('[') {
                Some(bracketed) => bracketed
                    .split_once(']')
                    .map_or(bracketed, |(host, _)| host),
                None => address.rsplit_once(':').map_or(address, |(host, _)| host),
            })
    }
}

fn parse_properties(text: &str) -> Result<BTreeMap<String, String>, ConnectionError> {
    let mut logical = Vec::new();
    let mut pending = String::new();
    for line in text.lines() {
        let trailing = line.chars().rev().take_while(|c| *c == '\\').count();
        pending.push_str(line.trim_start());
        if trailing % 2 == 1 {
            pending.pop();
        } else {
            logical.push(std::mem::take(&mut pending));
        }
    }
    if !pending.is_empty() {
        logical.push(pending);
    }
    let mut out = BTreeMap::new();
    for line in logical {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with(['#', '!']) {
            continue;
        }
        let split = property_split(trimmed);
        let (key, value) = trimmed.split_at(split);
        let value = value.trim_start_matches(|c: char| c.is_whitespace() || c == '=' || c == ':');
        out.insert(unescape(key.trim_end())?, unescape(value)?);
    }
    Ok(out)
}

fn property_split(line: &str) -> usize {
    let mut escaped = false;
    for (index, ch) in line.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '=' || ch == ':' || ch.is_whitespace() {
            return index;
        }
    }
    line.len()
}

fn unescape(value: &str) -> Result<String, ConnectionError> {
    let mut chars = value.chars();
    let mut out = String::new();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('u') => {
                let digits: String = chars.by_ref().take(4).collect();
                let code = u32::from_str_radix(&digits, 16)
                    .map_err(|_| ConnectionError::Config("invalid unicode escape".into()))?;
                out.push(
                    char::from_u32(code).ok_or_else(|| {
                        ConnectionError::Config("invalid unicode code point".into())
                    })?,
                );
            }
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    Ok(out)
}

fn security(
    properties: &BTreeMap<String, String>,
    bootstrap_host: &str,
) -> Result<Option<ClientSecurity>, ConnectionError> {
    let protocol = match properties
        .get("security.protocol")
        .map_or("PLAINTEXT", String::as_str)
    {
        "PLAINTEXT" => ListenerProtocol::Plaintext,
        "SSL" => ListenerProtocol::Ssl,
        "SASL_PLAINTEXT" => ListenerProtocol::SaslPlaintext,
        "SASL_SSL" => ListenerProtocol::SaslSsl,
        value => {
            return Err(ConnectionError::Config(format!(
                "unsupported security.protocol {value}"
            )));
        }
    };
    if protocol == ListenerProtocol::Plaintext && !properties.contains_key("sasl.mechanism") {
        return Ok(None);
    }
    let tls = protocol
        .requires_tls()
        .then(|| {
            for key in ["ssl.truststore.type", "ssl.keystore.type"] {
                if let Some(kind) = properties.get(key)
                    && kind != "PEM"
                {
                    return Err(ConnectionError::Config(format!(
                        "{key}={kind} is unsupported; expected PEM"
                    )));
                }
            }
            let identity = match (
                properties.get("ssl.keystore.location"),
                properties.get("ssl.key.location"),
            ) {
                (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
                (None, None) => None,
                _ => {
                    return Err(ConnectionError::Config(
                        "ssl.keystore.location and ssl.key.location must be provided together"
                            .into(),
                    ));
                }
            };
            Ok(TlsConnectorConfig {
                trust_roots_pem: properties.get("ssl.truststore.location").map(PathBuf::from),
                server_name: properties
                    .get("ssl.server.name")
                    .cloned()
                    .unwrap_or_else(|| bootstrap_host.to_string()),
                client_identity: identity,
            })
        })
        .transpose()?;
    let sasl = if protocol.requires_sasl() {
        Some(sasl_credentials(properties)?)
    } else {
        None
    };
    Ok(Some(ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host: properties.get("sasl.kerberos.service.host").cloned(),
    }))
}

fn sasl_credentials(
    properties: &BTreeMap<String, String>,
) -> Result<SaslCredentials, ConnectionError> {
    let mechanism = required(properties, "sasl.mechanism")?;
    let jaas = properties
        .get("sasl.jaas.config")
        .map(|value| jaas_options(value))
        .unwrap_or_default();
    let credential = match mechanism.as_str() {
        "PLAIN" => SaslCredentials::Plain {
            username: required_from(&jaas, "username")?,
            password: Secret(required_from(&jaas, "password")?).0,
        },
        "SCRAM-SHA-256" | "SCRAM-SHA-512" => SaslCredentials::Scram {
            mechanism: if mechanism == "SCRAM-SHA-256" {
                SaslMechanism::ScramSha256
            } else {
                SaslMechanism::ScramSha512
            },
            username: required_from(&jaas, "username")?,
            password: Secret(required_from(&jaas, "password")?).0,
        },
        "GSSAPI" => SaslCredentials::Gssapi {
            keytab_path: PathBuf::from(required_from(&jaas, "keyTab")?),
            client_principal: required_from(&jaas, "principal")?,
            service_name: properties
                .get("sasl.kerberos.service.name")
                .cloned()
                .unwrap_or_else(|| "kafka".into()),
            kdc_url: required(properties, "sasl.kerberos.kdc")?,
        },
        "OAUTHBEARER" => {
            if properties.contains_key("sasl.oauthbearer.token.endpoint.url") {
                return Err(ConnectionError::Config(
                    "sasl.oauthbearer.token.endpoint.url needs client-rs token-endpoint support"
                        .into(),
                ));
            }
            SaslCredentials::OAuthBearer {
                token_path: PathBuf::from(required(properties, "sasl.oauthbearer.token.file")?),
            }
        }
        value => {
            return Err(ConnectionError::Config(format!(
                "unsupported sasl.mechanism {value}"
            )));
        }
    };
    Ok(credential)
}

fn jaas_options(value: &str) -> BTreeMap<String, String> {
    let mut options = BTreeMap::new();
    let mut chars = value.trim_end_matches(';').chars().peekable();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        let mut key = String::from(ch);
        while let Some(&ch) = chars.peek() {
            if ch == '=' || ch.is_whitespace() {
                break;
            }
            key.push(ch);
            chars.next();
        }
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) {
            chars.next();
        }
        if chars.next() != Some('=') {
            while chars.peek().is_some_and(|ch| !ch.is_whitespace()) {
                chars.next();
            }
            continue;
        }
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) {
            chars.next();
        }
        let quote = chars.peek().copied().filter(|ch| *ch == '"' || *ch == '\'');
        if quote.is_some() {
            chars.next();
        }
        let mut parsed = String::new();
        let mut escaped = false;
        for ch in chars.by_ref() {
            if escaped {
                parsed.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if quote == Some(ch) || (quote.is_none() && ch.is_whitespace()) {
                break;
            } else {
                parsed.push(ch);
            }
        }
        options.insert(key, parsed);
    }
    options
}

fn required(properties: &BTreeMap<String, String>, key: &str) -> Result<String, ConnectionError> {
    required_from(properties, key)
}

fn required_from(values: &BTreeMap<String, String>, key: &str) -> Result<String, ConnectionError> {
    values
        .get(key)
        .cloned()
        .ok_or_else(|| ConnectionError::Config(format!("missing {key}")))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn properties_support_comments_escapes_and_continuations() {
        let parsed = parse_properties("! ignored\na\\:b = one\\\n  two\\u0033\n").unwrap();
        assert!(parsed == BTreeMap::from([("a:b".into(), "onetwo3".into())]));
    }

    #[test]
    fn secret_never_renders_its_value() {
        let secret = Secret("hunter2".to_string());
        assert!(format!("{secret}") == "[redacted]");
        assert!(format!("{secret:?}") == "[redacted]");
    }

    #[tokio::test]
    async fn parses_plain_security() {
        let args = ConnectionArgs {
            bootstrap_server: vec!["broker.example:9092".into()],
            bootstrap_controller: Vec::new(),
            command_config: None,
            client_id: None,
            request_timeout_ms: Some(1000),
            timeout: Time::from_millis(2000),
        };
        let mut props = BTreeMap::from([
            ("security.protocol".into(), "SASL_PLAINTEXT".into()),
            ("sasl.mechanism".into(), "PLAIN".into()),
            (
                "sasl.jaas.config".into(),
                "x required username=\"alice\" password=\"secret\";".into(),
            ),
        ]);
        let policy = security(&props, args.bootstrap_host()).unwrap().unwrap();
        assert!(policy.protocol == ListenerProtocol::SaslPlaintext);
        assert!(matches!(policy.sasl, Some(SaslCredentials::Plain { .. })));
        props.insert("security.protocol".into(), "PLAINTEXT".into());
        assert!(security(&BTreeMap::new(), "localhost").unwrap().is_none());
    }

    #[test]
    fn jaas_options_preserve_quoted_spaces_and_escapes() {
        assert!(
            jaas_options(r#"x required username="alice smith" password="two\" words";"#)
                == BTreeMap::from([
                    ("password".into(), "two\" words".into()),
                    ("username".into(), "alice smith".into()),
                ])
        );
    }

    #[test]
    fn bootstrap_host_preserves_ipv6() {
        let mut args = ConnectionArgs {
            bootstrap_server: vec!["[2001:db8::1]:9092".into()],
            bootstrap_controller: Vec::new(),
            command_config: None,
            client_id: None,
            request_timeout_ms: None,
            timeout: Time::from_millis(2000),
        };
        assert!(args.bootstrap_host() == "2001:db8::1");
        args.bootstrap_server = vec!["broker.example:9092".into()];
        assert!(args.bootstrap_host() == "broker.example");
    }

    #[tokio::test]
    async fn explicit_request_timeout_overrides_command_config() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "request.timeout.ms=9000\n").unwrap();
        let args = ConnectionArgs {
            bootstrap_server: vec!["broker.example:9092".into()],
            bootstrap_controller: Vec::new(),
            command_config: Some(file.path().into()),
            client_id: None,
            request_timeout_ms: Some(1234),
            timeout: Time::from_millis(2000),
        };
        let options = args.options("test").await.unwrap();
        assert!(options.request_timeout == Time::from_millis(1234));
    }
}
