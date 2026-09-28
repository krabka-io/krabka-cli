//! Kafka-compatible connection flags and the `--command-config` reader.
//!
//! Every networked command flattens [`ConnectionArgs`]. The flags have the
//! names of the JVM tools' flags, and `--command-config` reads the same
//! `java.util.Properties` file that `kafka-topics --command-config` reads, so
//! one file serves both tools.
//!
//! # Secrets
//!
//! [`Secret`] renders `[redacted]` for `Debug` and `Display`, and [`Properties`]
//! renders only its keys. That protection stops at this crate's boundary:
//! `ConnectionOptions`, `ClientSecurity` and `SaslCredentials` in
//! `krabka-client-core` derive `Debug`, and a `Debug` render of any of them
//! prints the password. Never format one of those values with `{:?}`. A
//! redaction wrapper in `krabka-client-core` is the fix.

use std::{collections::BTreeMap, fmt, path::PathBuf};

use clap::Args;
use krabka_client_admin::{AdminClient, AdminError};
use krabka_client_core::{
    ConnectionOptions, OAuthBearerTokenSource,
    security::{ClientSecurity, KeyStore, SaslCredentials, TlsConnectorConfig, TrustStore},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use krabka_units::{Time, convert::TimeExt as _};
use thiserror::Error;

/// The environment variable that names the Kerberos KDC for GSSAPI, for
/// example `tcp://kdc.example:88`.
///
/// The JVM reads the KDC from `krb5.conf`, not from a client property, and
/// `krabka-security` reads this variable on the broker side.
pub const KDC_URL_ENV: &str = "SSPI_KDC_URL";

/// Connection flags, with the names that the JVM tools use.
#[derive(Debug, Args, Clone, PartialEq)]
pub struct ConnectionArgs {
    /// The brokers to bootstrap from, `host:port`. Comma-separated, and the
    /// flag can repeat.
    #[arg(
        long,
        env = "KRABKA_BOOTSTRAP_SERVER",
        value_delimiter = ',',
        conflicts_with = "bootstrap_controller"
    )]
    pub bootstrap_server: Vec<String>,
    /// The controllers to bootstrap from, `host:port` (KIP-919).
    /// Comma-separated, and the flag can repeat.
    #[arg(
        long,
        env = "KRABKA_BOOTSTRAP_CONTROLLER",
        value_delimiter = ',',
        conflicts_with = "bootstrap_server"
    )]
    pub bootstrap_controller: Vec<String>,
    /// A properties file of admin client settings, as for
    /// `kafka-topics --command-config`.
    #[arg(long)]
    pub command_config: Option<PathBuf>,
    /// The client id. Overrides `client.id` in the command config.
    #[arg(long)]
    pub client_id: Option<String>,
    /// The deadline of one request, in milliseconds. Overrides
    /// `request.timeout.ms` in the command config.
    #[arg(long)]
    pub request_timeout_ms: Option<i64>,
    /// The deadline of the whole command, for example `30s` or `2m`.
    #[arg(long, env = "KRABKA_TIMEOUT", default_value = "30s", value_parser = parse_time)]
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

impl From<ConnectionError> for crate::output::CommandError {
    fn from(error: ConnectionError) -> Self {
        match error {
            ConnectionError::Admin(error) => error.into(),
            other => Self::Other(other.to_string()),
        }
    }
}

/// Why a connection could not be set up.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConnectionError {
    /// Neither bootstrap flag was given.
    #[error("one of --bootstrap-server or --bootstrap-controller is required")]
    MissingBootstrap,
    /// The command config file could not be read.
    #[error("read command config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The command config holds a value that cannot be used.
    #[error("command config: {0}")]
    Config(#[from] ConfigError),
    /// The admin client failed to connect.
    #[error(transparent)]
    Admin(#[from] AdminError),
}

/// A property that the command config sets to a value that cannot be used.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The file is not a valid properties file.
    #[error("malformed properties file: {0}")]
    Malformed(String),
    /// A required property is not set.
    #[error("{property} is required: {reason}")]
    Missing { property: String, reason: String },
    /// A property has a value that Kafka also refuses.
    #[error("{property} is invalid: {reason}")]
    Invalid { property: String, reason: String },
    /// A property has a value that Kafka accepts and krabka cannot use yet.
    #[error("{property} is not supported: {reason}")]
    Unsupported { property: String, reason: String },
}

impl ConfigError {
    fn missing(property: &str, reason: impl Into<String>) -> Self {
        Self::Missing {
            property: property.into(),
            reason: reason.into(),
        }
    }

    fn invalid(property: &str, reason: impl Into<String>) -> Self {
        Self::Invalid {
            property: property.into(),
            reason: reason.into(),
        }
    }

    fn unsupported(property: &str, reason: impl Into<String>) -> Self {
        Self::Unsupported {
            property: property.into(),
            reason: reason.into(),
        }
    }
}

/// A value that must not reach a log. `Debug` and `Display` both render
/// `[redacted]`.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps a secret value.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Returns the secret value.
    pub fn expose(self) -> T {
        self.0
    }
}

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

/// The key-value pairs of a `java.util.Properties` file.
///
/// `Debug` renders the keys only, because a command config holds passwords.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Properties(BTreeMap<String, String>);

impl fmt::Debug for Properties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Properties")
            .field("keys", &self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Properties {
    /// Parses the bytes of a properties file as `Properties.load(InputStream)`
    /// does.
    ///
    /// Each byte is one ISO 8859-1 character, as the JVM reads the file, so a
    /// non-ASCII value reaches the broker as the JVM tools send it. A logical
    /// line continues onto the next physical line after an odd number of
    /// trailing backslashes, and the next line loses its leading whitespace.
    /// `#` and `!` start a comment only at the start of a logical line. The
    /// key ends at the first unescaped `=`, `:`, space, tab or form feed.
    /// Escapes are `\t`, `\n`, `\r`, `\f`, `\uXXXX`, and a backslash before any
    /// other character, which stands for that character. A later key replaces
    /// an earlier one.
    ///
    /// # Errors
    /// Returns [`ConfigError::Malformed`] for a malformed `\uXXXX` escape, as
    /// the JVM throws `IllegalArgumentException`, and for an escape of an
    /// unpaired UTF-16 surrogate, which a Rust string cannot hold.
    pub fn parse(bytes: &[u8]) -> Result<Self, ConfigError> {
        let text = bytes
            .iter()
            .map(|&byte| char::from(byte))
            .collect::<Vec<_>>();
        let mut properties = BTreeMap::new();
        for line in logical_lines(&text) {
            let (key, value) = split_key_value(&line);
            properties.insert(unescape(key)?, unescape(value)?);
        }
        Ok(Self(properties))
    }

    /// The value of `key`, trimmed as Kafka's `ConfigDef` trims every value,
    /// or `None` when the file does not set it.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.trim())
    }

    fn contains(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

const fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\u{c}')
}

/// Splits the text into logical lines, as `Properties.LineReader` does.
fn logical_lines(text: &[char]) -> Vec<Vec<char>> {
    let mut lines = Vec::new();
    let mut line = Vec::new();
    let mut skip_blank = true;
    let mut continued = false;
    let mut backslash = false;
    let mut chars = text.iter().copied().peekable();
    while let Some(c) = chars.next() {
        if skip_blank {
            if is_blank(c) || (!continued && matches!(c, '\r' | '\n')) {
                continue;
            }
            skip_blank = false;
            continued = false;
        }
        if line.is_empty() && matches!(c, '#' | '!') {
            while chars.next_if(|c| !matches!(c, '\r' | '\n')).is_some() {}
            skip_blank = true;
            continue;
        }
        if !matches!(c, '\r' | '\n') {
            line.push(c);
            backslash = c == '\\' && !backslash;
            continue;
        }
        if line.is_empty() {
            skip_blank = true;
            continue;
        }
        if backslash {
            line.pop();
            skip_blank = true;
            continued = true;
            backslash = false;
            if c == '\r' {
                chars.next_if_eq(&'\n');
            }
        } else {
            lines.push(std::mem::take(&mut line));
            skip_blank = true;
        }
    }
    if !line.is_empty() {
        if backslash {
            line.pop();
        }
        lines.push(line);
    }
    lines
}

/// Splits one logical line into its raw key and value, as `Properties.load0`
/// does.
fn split_key_value(line: &[char]) -> (&[char], &[char]) {
    let mut key_len = 0;
    let mut value_start = line.len();
    let mut has_separator = false;
    let mut backslash = false;
    while key_len < line.len() {
        let c = line[key_len];
        if matches!(c, '=' | ':') && !backslash {
            value_start = key_len + 1;
            has_separator = true;
            break;
        }
        if is_blank(c) && !backslash {
            value_start = key_len + 1;
            break;
        }
        backslash = c == '\\' && !backslash;
        key_len += 1;
    }
    while value_start < line.len() {
        let c = line[value_start];
        if !is_blank(c) {
            if !has_separator && matches!(c, '=' | ':') {
                has_separator = true;
            } else {
                break;
            }
        }
        value_start += 1;
    }
    (&line[..key_len], &line[value_start..])
}

/// Resolves the escapes of a key or value, as `Properties.loadConvert` does.
fn unescape(raw: &[char]) -> Result<String, ConfigError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.iter().copied();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('u') => {
                let digits = chars.by_ref().take(4).collect::<String>();
                let code = (digits.len() == 4)
                    .then(|| u32::from_str_radix(&digits, 16).ok())
                    .flatten()
                    .ok_or_else(|| ConfigError::Malformed("malformed \\uxxxx encoding".into()))?;
                out.push(char::from_u32(code).ok_or_else(|| {
                    ConfigError::Malformed(format!("\\u{digits} is an unpaired surrogate"))
                })?);
            }
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\u{c}'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    Ok(out)
}

/// One login module entry of `sasl.jaas.config`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct JaasEntry {
    module: String,
    options: BTreeMap<String, Secret<String>>,
}

impl JaasEntry {
    fn option(&self, key: &str) -> Option<String> {
        self.options.get(key).cloned().map(Secret::expose)
    }

    fn required(&self, key: &str) -> Result<String, ConfigError> {
        self.option(key).ok_or_else(|| {
            ConfigError::missing(
                SASL_JAAS_CONFIG,
                format!("{} needs the `{key}` option", self.module),
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JaasToken {
    Word(String),
    Quoted(String),
    Number,
    Char(char),
}

impl JaasToken {
    /// The token's `StreamTokenizer.sval`, which is set for a word and for a
    /// quoted string.
    fn sval(self) -> Option<String> {
        match self {
            Self::Word(value) | Self::Quoted(value) => Some(value),
            Self::Number | Self::Char(_) => None,
        }
    }
}

/// Tokenizes as Kafka's `JaasConfig` configures `java.io.StreamTokenizer`:
/// `//` and `/* */` are comments, words take letters, digits, `.`, `-`, `_`
/// and `$`, a token that starts with a digit or `.` is a number, and `"` and
/// `'` quote a string that ends at the quote or at the end of the line.
fn jaas_tokens(text: &str) -> Vec<JaasToken> {
    let is_word_start =
        |c: char| c.is_ascii_alphabetic() || matches!(c, '-' | '_' | '$') || u32::from(c) >= 0xA0;
    let is_word = |c: char| is_word_start(c) || c.is_ascii_digit() || c == '.';
    let mut tokens = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c <= ' ' {
            continue;
        }
        if c == '/' && chars.next_if_eq(&'*').is_some() {
            let mut previous = '\0';
            for c in chars.by_ref() {
                if previous == '*' && c == '/' {
                    break;
                }
                previous = c;
            }
        } else if c == '/' && chars.next_if_eq(&'/').is_some() {
            while chars.next_if(|c| !matches!(c, '\r' | '\n')).is_some() {}
        } else if c == '"' || c == '\'' {
            tokens.push(JaasToken::Quoted(quoted(&mut chars, c)));
        } else if is_word_start(c) {
            let mut word = String::from(c);
            while let Some(c) = chars.next_if(|c| is_word(*c)) {
                word.push(c);
            }
            tokens.push(JaasToken::Word(word));
        } else if c.is_ascii_digit() || c == '.' {
            while chars.next_if(|c| c.is_ascii_digit() || *c == '.').is_some() {}
            tokens.push(JaasToken::Number);
        } else {
            tokens.push(JaasToken::Char(c));
        }
    }
    tokens
}

/// The rest of a quoted string, with `StreamTokenizer`'s escapes resolved.
fn quoted(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, quote: char) -> String {
    let mut value = String::new();
    while let Some(c) = chars.next_if(|c| !matches!(c, '\r' | '\n')) {
        if c == quote {
            break;
        }
        if c != '\\' {
            value.push(c);
            continue;
        }
        let Some(escaped) = chars.next() else { break };
        let resolved = match escaped {
            'a' => '\u{7}',
            'b' => '\u{8}',
            'f' => '\u{c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'v' => '\u{b}',
            '0'..='7' => {
                let mut code = escaped.to_digit(8).unwrap_or(0);
                let digits = if escaped <= '3' { 2 } else { 1 };
                for _ in 0..digits {
                    match chars.next_if(|c| matches!(c, '0'..='7')) {
                        Some(digit) => code = code * 8 + digit.to_digit(8).unwrap_or(0),
                        None => break,
                    }
                }
                char::from_u32(code).unwrap_or('\0')
            }
            other => other,
        };
        value.push(resolved);
    }
    value
}

const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";

/// Parses `sasl.jaas.config` as Kafka's `JaasConfig` does, and requires the
/// one login module that `JaasContext.loadClientContext` requires.
fn parse_jaas(text: &str) -> Result<JaasEntry, ConfigError> {
    let invalid = |reason: &str| ConfigError::invalid(SASL_JAAS_CONFIG, reason);
    let mut tokens = jaas_tokens(text).into_iter();
    let mut entries = Vec::new();
    while let Some(token) = tokens.next() {
        let module = token
            .sval()
            .ok_or_else(|| invalid("login module not specified"))?;
        let flag = tokens
            .next()
            .ok_or_else(|| invalid("login module control flag not specified"))?
            .sval()
            .ok_or_else(|| invalid("login module control flag is not available"))?;
        if !["required", "requisite", "sufficient", "optional"]
            .contains(&flag.to_ascii_lowercase().as_str())
        {
            return Err(invalid(&format!(
                "invalid login module control flag '{flag}'"
            )));
        }
        let mut options = BTreeMap::new();
        let mut terminated = false;
        while let Some(token) = tokens.next() {
            if token == JaasToken::Char(';') {
                terminated = true;
                break;
            }
            let key = token.sval().unwrap_or_default();
            let value = match (tokens.next(), tokens.next()) {
                (Some(JaasToken::Char('=')), Some(value)) => value.sval(),
                _ => None,
            }
            .ok_or_else(|| invalid(&format!("value not specified for key '{key}'")))?;
            options.insert(key, Secret::new(value));
        }
        if !terminated {
            return Err(invalid("entry not terminated by semi-colon"));
        }
        entries.push(JaasEntry { module, options });
    }
    match <[JaasEntry; 1]>::try_from(entries) {
        Ok([entry]) => Ok(entry),
        Err(entries) => Err(invalid(&format!(
            "holds {} login modules, and a client needs exactly 1",
            entries.len()
        ))),
    }
}

const PLAIN_LOGIN_MODULE: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";
const SCRAM_LOGIN_MODULE: &str = "org.apache.kafka.common.security.scram.ScramLoginModule";
const KRB5_LOGIN_MODULE: &str = "com.sun.security.auth.module.Krb5LoginModule";
const OAUTHBEARER_LOGIN_MODULE: &str =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
const OAUTHBEARER_LOGIN_CALLBACK_HANDLER: &str =
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler";

/// The inputs to the command-config mapping that do not come from the file.
#[derive(Debug, Clone, Copy)]
struct Context<'a> {
    /// The host of the first bootstrap address. It is the TLS server name
    /// unless `ssl.server.name` names another.
    bootstrap_host: &'a str,
    /// The Kerberos KDC, from [`KDC_URL_ENV`].
    kdc_url: Option<&'a str>,
}

/// Maps `security.protocol` and its TLS and SASL properties onto the client's
/// security policy. `PLAINTEXT` gives `None`, and under it Kafka ignores the
/// TLS and SASL properties too.
fn security(
    properties: &Properties,
    context: Context<'_>,
) -> Result<Option<ClientSecurity>, ConfigError> {
    let protocol = match properties
        .get("security.protocol")
        .unwrap_or("PLAINTEXT")
        .to_ascii_uppercase()
        .as_str()
    {
        "PLAINTEXT" => return Ok(None),
        "SSL" => ListenerProtocol::Ssl,
        "SASL_PLAINTEXT" => ListenerProtocol::SaslPlaintext,
        "SASL_SSL" => ListenerProtocol::SaslSsl,
        other => {
            return Err(ConfigError::invalid(
                "security.protocol",
                format!("{other} is not one of PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"),
            ));
        }
    };
    let tls = if protocol.requires_tls() {
        Some(tls(properties, context.bootstrap_host)?)
    } else {
        None
    };
    let sasl = if protocol.requires_sasl() {
        Some(sasl(properties, context.kdc_url)?)
    } else {
        None
    };
    Ok(Some(ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host: None,
    }))
}

/// Refuses a store type other than PEM. Kafka's default store type is JKS.
fn require_pem(properties: &Properties, key: &str) -> Result<(), ConfigError> {
    match properties.get(key) {
        Some("PEM") => Ok(()),
        kind => Err(ConfigError::unsupported(
            key,
            format!(
                "{} is not supported; krabka reads PEM stores only, so set {key}=PEM",
                kind.unwrap_or("JKS (the default)")
            ),
        )),
    }
}

fn tls(properties: &Properties, bootstrap_host: &str) -> Result<TlsConnectorConfig, ConfigError> {
    for key in [
        "ssl.truststore.certificates",
        "ssl.keystore.key",
        "ssl.keystore.certificate.chain",
    ] {
        if properties.contains(key) {
            return Err(ConfigError::unsupported(
                key,
                "PEM content in the config needs krabka-client-core support; name a PEM file with the .location property",
            ));
        }
    }
    let trust_store = properties.get("ssl.truststore.location").ok_or_else(|| {
        ConfigError::unsupported(
            "ssl.truststore.location",
            "the platform trust store needs krabka-client-core support; name a PEM file of CA certificates",
        )
    })?;
    require_pem(properties, "ssl.truststore.type")?;
    if properties.contains("ssl.truststore.password") {
        return Err(ConfigError::invalid(
            "ssl.truststore.password",
            "a PEM trust store has no password",
        ));
    }
    let key_store = match properties.get("ssl.keystore.location") {
        None => None,
        Some(key_store) => {
            require_pem(properties, "ssl.keystore.type")?;
            if properties.contains("ssl.keystore.password") {
                return Err(ConfigError::invalid(
                    "ssl.keystore.password",
                    "a PEM key store takes only ssl.key.password",
                ));
            }
            if properties.contains("ssl.key.password") {
                return Err(ConfigError::unsupported(
                    "ssl.key.password",
                    "an encrypted private key needs krabka-client-core support",
                ));
            }
            // A Kafka PEM key store is one file that holds the private key
            // and the certificate chain. The client reads each from it.
            Some(KeyStore::PemFile {
                path: PathBuf::from(key_store),
                key_password: None,
            })
        }
    };
    match properties
        .get("ssl.endpoint.identification.algorithm")
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("https") => {}
        Some("") => {
            return Err(ConfigError::unsupported(
                "ssl.endpoint.identification.algorithm",
                "turning hostname verification off needs krabka-client-core support",
            ));
        }
        Some(other) => {
            return Err(ConfigError::invalid(
                "ssl.endpoint.identification.algorithm",
                format!("{other} is not https or empty"),
            ));
        }
    }
    // `TlsConnectorConfig` caches its built rustls config in a private
    // field, so it is built from its default rather than a struct literal.
    let mut config = TlsConnectorConfig::default();
    config.trust_store = TrustStore::PemFile(PathBuf::from(trust_store));
    config.key_store = key_store;
    properties
        .get("ssl.server.name")
        .unwrap_or(bootstrap_host)
        .clone_into(&mut config.server_name);
    Ok(config)
}

fn sasl(properties: &Properties, kdc_url: Option<&str>) -> Result<SaslCredentials, ConfigError> {
    // Kafka's `sasl.mechanism` default.
    let mechanism = properties.get("sasl.mechanism").unwrap_or("GSSAPI");
    let jaas = parse_jaas(properties.get(SASL_JAAS_CONFIG).ok_or_else(|| {
        ConfigError::missing(
            SASL_JAAS_CONFIG,
            "krabka reads no JVM-wide JAAS file, so the login module must be in the command config",
        )
    })?)?;
    let expect_module = |modules: &[&str]| {
        if modules.contains(&jaas.module.as_str()) {
            Ok(())
        } else {
            Err(ConfigError::invalid(
                SASL_JAAS_CONFIG,
                format!("{} does not log in for {mechanism}", jaas.module),
            ))
        }
    };
    match mechanism {
        "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" => {
            expect_module(&[PLAIN_LOGIN_MODULE, SCRAM_LOGIN_MODULE])?;
            // Kafka's `ScramLoginModule` logs in with a delegation token
            // (KIP-48) when `tokenauth=true`; `PlainLoginModule` ignores it.
            let delegation_token = jaas
                .option("tokenauth")
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            let username = jaas.required("username")?;
            let password = Secret::new(jaas.required("password")?);
            Ok(match mechanism {
                "PLAIN" => SaslCredentials::Plain {
                    username,
                    password: password.expose(),
                },
                "SCRAM-SHA-256" => SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha256,
                    username,
                    password: password.expose(),
                    delegation_token,
                },
                _ => SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha512,
                    username,
                    password: password.expose(),
                    delegation_token,
                },
            })
        }
        "GSSAPI" => {
            expect_module(&[KRB5_LOGIN_MODULE])?;
            gssapi(properties, &jaas, kdc_url)
        }
        "OAUTHBEARER" => {
            expect_module(&[OAUTHBEARER_LOGIN_MODULE])?;
            if properties.get("sasl.login.callback.handler.class")
                != Some(OAUTHBEARER_LOGIN_CALLBACK_HANDLER)
            {
                return Err(ConfigError::unsupported(
                    "sasl.login.callback.handler.class",
                    format!(
                        "the default is Kafka's unsecured-token handler; set {OAUTHBEARER_LOGIN_CALLBACK_HANDLER}"
                    ),
                ));
            }
            Ok(SaslCredentials::OAuthBearer {
                token: OAuthBearerTokenSource::File(oauthbearer_token_file(properties)?),
                extensions: BTreeMap::new(),
            })
        }
        other => Err(ConfigError::invalid(
            "sasl.mechanism",
            format!(
                "{other} is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER"
            ),
        )),
    }
}

fn gssapi(
    properties: &Properties,
    jaas: &JaasEntry,
    kdc_url: Option<&str>,
) -> Result<SaslCredentials, ConfigError> {
    const SERVICE_NAME: &str = "sasl.kerberos.service.name";
    if properties.contains("sasl.kerberos.kdc") {
        return Err(ConfigError::unsupported(
            "sasl.kerberos.kdc",
            format!(
                "the JVM reads the KDC from krb5.conf; krabka reads it from {KDC_URL_ENV} until krabka-client-rs decides the property"
            ),
        ));
    }
    if !jaas
        .option("useKeyTab")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
    {
        return Err(ConfigError::unsupported(
            SASL_JAAS_CONFIG,
            "Krb5LoginModule without useKeyTab=true reads a ticket cache, which krabka-client-core does not",
        ));
    }
    // `SaslChannelBuilder` takes the JAAS `serviceName` first, refuses one
    // that disagrees with the Kafka property, and requires one of the two.
    let service_name = match (jaas.option("serviceName"), properties.get(SERVICE_NAME)) {
        (Some(jaas_name), Some(config_name)) if jaas_name != config_name => {
            return Err(ConfigError::invalid(
                SERVICE_NAME,
                format!("conflicts with serviceName={jaas_name} in {SASL_JAAS_CONFIG}"),
            ));
        }
        (Some(name), _) => name,
        (None, Some(name)) => name.to_owned(),
        (None, None) => {
            return Err(ConfigError::missing(
                SERVICE_NAME,
                "no serviceName is defined in either the JAAS or the Kafka config",
            ));
        }
    };
    Ok(SaslCredentials::Gssapi {
        keytab_path: PathBuf::from(jaas.required("keyTab")?),
        client_principal: jaas.required("principal")?,
        service_name,
        kdc_url: kdc_url
            .ok_or_else(|| {
                ConfigError::missing(
                    KDC_URL_ENV,
                    "GSSAPI needs the KDC address, for example tcp://kdc.example:88",
                )
            })?
            .to_owned(),
    })
}

/// The token file of `sasl.oauthbearer.token.endpoint.url=file:...`, the form
/// that Kafka's `FileJwtRetriever` reads. Like Kafka, it takes the raw URL
/// path and does not percent-decode it.
fn oauthbearer_token_file(properties: &Properties) -> Result<PathBuf, ConfigError> {
    const KEY: &str = "sasl.oauthbearer.token.endpoint.url";
    let url = properties.get(KEY).ok_or_else(|| {
        ConfigError::missing(
            KEY,
            "OAuthBearerLoginCallbackHandler needs a token endpoint",
        )
    })?;
    let (scheme, rest) = url
        .split_once(':')
        .ok_or_else(|| ConfigError::invalid(KEY, format!("{url} is a URL with no protocol")))?;
    match scheme.to_ascii_lowercase().as_str() {
        "file" => {
            let path = match rest.strip_prefix("//") {
                Some(authority_and_path) => authority_and_path
                    .find('/')
                    .map_or("", |start| &authority_and_path[start..]),
                None => rest,
            };
            if path.is_empty() {
                return Err(ConfigError::invalid(KEY, format!("{url} names no file")));
            }
            Ok(PathBuf::from(path))
        }
        "http" | "https" => Err(ConfigError::unsupported(
            KEY,
            "the client-credentials grant needs krabka-client-core support; use a file: URL",
        )),
        other => Err(ConfigError::invalid(
            KEY,
            format!("{other} is not one of http, https, file"),
        )),
    }
}

fn positive_millis(properties: &Properties, key: &str) -> Result<Option<Time>, ConfigError> {
    properties
        .get(key)
        .map(|value| match value.parse::<i64>() {
            Ok(millis) if millis > 0 => Ok(Time::from_millis(millis)),
            _ => Err(ConfigError::invalid(
                key,
                format!("{value} is not a positive number of milliseconds"),
            )),
        })
        .transpose()
}

impl ConnectionArgs {
    /// Connects an admin client through `--bootstrap-server` with
    /// `connect_with_options`, or through `--bootstrap-controller` with
    /// `connect_controller_with_options`.
    ///
    /// # Errors
    /// Returns [`ConnectionError::MissingBootstrap`] when neither flag is
    /// given, [`ConnectionError::Read`] or [`ConnectionError::Config`] for the
    /// command config, and [`ConnectionError::Admin`] when the client cannot
    /// connect.
    pub async fn connect(&self, command: &str) -> Result<AdminClient, ConnectionError> {
        let options = self.options(command).await?;
        if !self.bootstrap_controller.is_empty() {
            return Ok(AdminClient::connect_controller_with_options(
                &self.bootstrap_controller,
                options,
            )
            .await?);
        }
        if self.bootstrap_server.is_empty() {
            return Err(ConnectionError::MissingBootstrap);
        }
        Ok(AdminClient::connect_with_options(&self.bootstrap_server, options).await?)
    }

    /// The connection options for `command`: the command config, the flags
    /// that override it, and [`KDC_URL_ENV`].
    ///
    /// # Errors
    /// Returns [`ConnectionError::Read`] when the command config cannot be
    /// read, and [`ConnectionError::Config`] when it cannot be used.
    pub async fn options(&self, command: &str) -> Result<ConnectionOptions, ConnectionError> {
        let properties = match &self.command_config {
            Some(path) => {
                let bytes =
                    tokio::fs::read(path)
                        .await
                        .map_err(|source| ConnectionError::Read {
                            path: path.clone(),
                            source,
                        })?;
                Properties::parse(&bytes)?
            }
            None => Properties::default(),
        };
        let kdc_url = std::env::var(KDC_URL_ENV).ok();
        Ok(self.options_from(&properties, command, kdc_url.as_deref())?)
    }

    fn options_from(
        &self,
        properties: &Properties,
        command: &str,
        kdc_url: Option<&str>,
    ) -> Result<ConnectionOptions, ConfigError> {
        let mut options = ConnectionOptions {
            client_id: self
                .client_id
                .clone()
                .or_else(|| {
                    properties
                        .get("client.id")
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| format!("krabka-cli/{} {command}", env!("CARGO_PKG_VERSION"))),
            ..ConnectionOptions::default()
        };
        let request_timeout = match self.request_timeout_ms {
            Some(millis) if millis > 0 => Some(Time::from_millis(millis)),
            Some(millis) => {
                return Err(ConfigError::invalid(
                    "--request-timeout-ms",
                    format!("{millis} is not a positive number of milliseconds"),
                ));
            }
            None => positive_millis(properties, "request.timeout.ms")?,
        };
        if let Some(timeout) = request_timeout {
            options.request_timeout = timeout;
        }
        if let Some(timeout) = positive_millis(properties, "socket.connection.setup.timeout.ms")? {
            options.socket_connection_setup_timeout = timeout;
        }
        let context = Context {
            bootstrap_host: self.bootstrap_host(),
            kdc_url,
        };
        options.security = security(properties, context)?.map(Box::new);
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

#[cfg(test)]
mod tests;
