use assert2::{assert, check};
use krabka_client_core::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT;

use super::*;

fn args() -> ConnectionArgs {
    ConnectionArgs {
        bootstrap_server: vec!["broker.example:9092".into()],
        bootstrap_controller: Vec::new(),
        command_config: None,
        client_id: None,
        request_timeout_ms: None,
        timeout: Time::from_millis(30_000),
    }
}

fn base() -> ConnectionOptions {
    ConnectionOptions {
        client_id: format!("krabka-cli/{} test", env!("CARGO_PKG_VERSION")),
        ..ConnectionOptions::default()
    }
}

fn with_security(security: ClientSecurity) -> ConnectionOptions {
    ConnectionOptions {
        security: Some(Box::new(security)),
        ..base()
    }
}

fn map(text: &str) -> Result<ConnectionOptions, ConfigError> {
    args().options_from(
        &Properties::parse(text.as_bytes())?,
        "test",
        Some("tcp://kdc.example:88"),
    )
}

// `ConnectionOptions` and the security types derive `Debug` and not
// `PartialEq`, so the whole value is compared through its `Debug` render.
fn same(actual: &ConnectionOptions, expected: &ConnectionOptions) -> bool {
    format!("{actual:?}") == format!("{expected:?}")
}

// The key-value pairs that a table row expects.
type Pairs = &'static [(&'static str, &'static str)];

fn properties(pairs: &[(&str, &str)]) -> Properties {
    Properties(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    )
}

#[test]
fn the_properties_reader_matches_java_util_properties() {
    let cases: &[(&str, &[u8], Pairs)] = &[
        ("equals separator", b"a=b\n", &[("a", "b")]),
        ("colon separator", b"a:b\n", &[("a", "b")]),
        ("space separator", b"a b\n", &[("a", "b")]),
        (
            "blanks around the separator",
            b"  a \t = \x0c b c  \n",
            &[("a", "b c  ")],
        ),
        ("a second separator is value", b"a==b\n", &[("a", "=b")]),
        ("space then separator", b"a = :b\n", &[("a", ":b")]),
        ("key with no value", b"lonely\n", &[("lonely", "")]),
        ("hash comment", b"# a=b\nc=d\n", &[("c", "d")]),
        ("bang comment", b"  ! a=b\nc=d\n", &[("c", "d")]),
        (
            "hash after the start of a line is data",
            b"a=b # not a comment\n",
            &[("a", "b # not a comment")],
        ),
        ("escaped colon in a key", b"a\\:b=c\n", &[("a:b", "c")]),
        ("escaped equals in a key", b"a\\=b=c\n", &[("a=b", "c")]),
        ("escaped space in a key", b"a\\ b=c\n", &[("a b", "c")]),
        (
            "value over three continued lines",
            b"a=one \\\n    two \\\n\t three\n",
            &[("a", "one two three")],
        ),
        (
            "an even run of backslashes does not continue",
            b"a=b\\\\\nc=d\n",
            &[("a", "b\\"), ("c", "d")],
        ),
        (
            "a continuation over CRLF",
            b"a=b\\\r\n  c\r\nd=e\r\n",
            &[("a", "bc"), ("d", "e")],
        ),
        (
            "a continued line that looks like a comment is data",
            b"a=b\\\n#c\n",
            &[("a", "b#c")],
        ),
        (
            "escapes",
            b"a=\\t\\n\\r\\f\\\\\\q\n",
            &[("a", "\t\n\r\u{c}\\q")],
        ),
        ("unicode escape", b"a=\\u0041\\u00e9\n", &[("a", "A\u{e9}")]),
        ("latin-1 bytes", b"a=caf\xe9\n", &[("a", "caf\u{e9}")]),
        (
            "a trailing backslash at the end of the file is dropped",
            b"a=b\\",
            &[("a", "b")],
        ),
        ("a later key wins", b"a=1\na=2\n", &[("a", "2")]),
        ("blank lines", b"\n\n  \n\ta=b\n\n", &[("a", "b")]),
    ];
    for (name, text, expected) in cases {
        check!(
            Properties::parse(text) == Ok(properties(expected)),
            "{name}"
        );
    }
}

#[test]
fn a_malformed_unicode_escape_is_refused() {
    for text in [&b"a=\\u12"[..], b"a=\\u12zz", b"a=\\ud800"] {
        check!(matches!(
            Properties::parse(text),
            Err(ConfigError::Malformed(_))
        ));
    }
}

#[test]
fn the_jaas_parser_matches_kafka_jaas_config() {
    let cases: &[(&str, &str, Pairs)] = &[
        (
            "quoted values",
            r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="s3cret";"#,
            &[("username", "alice"), ("password", "s3cret")],
        ),
        (
            "unquoted word values and odd spacing",
            "x.Module  required\n\tuseKeyTab = true principal='a/b@EXAMPLE' ;",
            &[("useKeyTab", "true"), ("principal", "a/b@EXAMPLE")],
        ),
        (
            "escapes and spaces inside quotes",
            r#"x.Module optional password="two \"words\" \\ \101";"#,
            &[("password", r#"two "words" \ A"#)],
        ),
        (
            "comments",
            "/* a */ x.Module required // b\n key=\"v\";",
            &[("key", "v")],
        ),
        ("a control flag in any case", "x.Module REQUIRED;", &[]),
    ];
    for (name, text, options) in cases {
        let entry = parse_jaas(text).expect(name);
        let actual = entry
            .options
            .into_iter()
            .map(|(key, value)| (key, value.expose()))
            .collect::<BTreeMap<_, _>>();
        let expected = options
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<BTreeMap<_, _>>();
        check!(actual == expected, "{name}");
    }
}

#[test]
fn malformed_jaas_is_refused() {
    for text in [
        "",
        "x.Module;",
        "x.Module sometimes;",
        "x.Module required key=\"v\"",
        "x.Module required key;",
        "x.Module required key=1;",
        "a.Module required; b.Module required;",
    ] {
        check!(
            matches!(parse_jaas(text), Err(ConfigError::Invalid { property, .. }) if property == SASL_JAAS_CONFIG),
            "{text}"
        );
    }
}

#[test]
fn a_command_config_maps_onto_whole_connection_options() {
    let tls = |key_store: Option<&str>, server_name: &str| {
        let mut config = TlsConnectorConfig::default();
        config.trust_store = TrustStore::PemFile(PathBuf::from("/etc/kafka/ca.pem"));
        config.key_store = key_store.map(|path| KeyStore::PemFile {
            path: path.into(),
            key_password: None,
        });
        config.server_name = server_name.into();
        config
    };
    let cases = [
        ("no config", String::new(), base()),
        (
            "PLAINTEXT ignores SASL settings",
            "security.protocol=PLAINTEXT\nsasl.mechanism=PLAIN\n".into(),
            base(),
        ),
        (
            "client id and timeouts",
            "client.id=ops\nrequest.timeout.ms=9000\nsocket.connection.setup.timeout.ms=500\n"
                .into(),
            ConnectionOptions {
                client_id: "ops".into(),
                request_timeout: Time::from_millis(9000),
                socket_connection_setup_timeout: Time::from_millis(500),
                ..base()
            },
        ),
        (
            "SSL",
            "security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/ca.pem\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::Ssl,
                tls: Some(tls(None, "broker.example")),
                sasl: None,
                sasl_host: None,
            }),
        ),
        (
            "SSL with a lower-case protocol, mTLS and a server name",
            "security.protocol=ssl\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/ca.pem\nssl.keystore.type=PEM\nssl.keystore.location=/etc/kafka/client.pem\nssl.server.name=kafka.internal\nssl.endpoint.identification.algorithm=HTTPS\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::Ssl,
                tls: Some(tls(Some("/etc/kafka/client.pem"), "kafka.internal")),
                sasl: None,
                sasl_host: None,
            }),
        ),
        (
            "SASL_PLAINTEXT with PLAIN",
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\\n    username=\"alice\" \\\n    password=\"alice-secret\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: "alice".into(),
                    password: "alice-secret".into(),
                }),
                sasl_host: None,
            }),
        ),
        (
            "SASL_SSL with SCRAM-SHA-256",
            "security.protocol=SASL_SSL\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/ca.pem\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bob\" password=\"bob-secret\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslSsl,
                tls: Some(tls(None, "broker.example")),
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha256,
                    username: "bob".into(),
                    password: "bob-secret".into(),
                    delegation_token: false,
                }),
                sasl_host: None,
            }),
        ),
        (
            "SASL_PLAINTEXT with SCRAM-SHA-512",
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"carol\" password=\"carol-secret\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha512,
                    username: "carol".into(),
                    password: "carol-secret".into(),
                    delegation_token: false,
                }),
                sasl_host: None,
            }),
        ),
        (
            "SCRAM with tokenauth=true logs in with a delegation token",
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"token-id\" password=\"token-hmac\" tokenauth=\"true\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha256,
                    username: "token-id".into(),
                    password: "token-hmac".into(),
                    delegation_token: true,
                }),
                sasl_host: None,
            }),
        ),
        (
            "SASL_PLAINTEXT with GSSAPI, the default mechanism",
            "security.protocol=SASL_PLAINTEXT\nsasl.kerberos.service.name=kafka\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/etc/security/ops.keytab\" principal=\"ops@EXAMPLE.COM\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Gssapi {
                    keytab_path: "/etc/security/ops.keytab".into(),
                    client_principal: "ops@EXAMPLE.COM".into(),
                    service_name: "kafka".into(),
                    kdc_url: "tcp://kdc.example:88".into(),
                }),
                sasl_host: None,
            }),
        ),
        (
            "GSSAPI takes the service name from the JAAS entry",
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=GSSAPI\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true serviceName=\"kafka-svc\" keyTab=\"/k\" principal=\"p@R\";\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Gssapi {
                    keytab_path: "/k".into(),
                    client_principal: "p@R".into(),
                    service_name: "kafka-svc".into(),
                    kdc_url: "tcp://kdc.example:88".into(),
                }),
                sasl_host: None,
            }),
        ),
        (
            "SASL_SSL with OAUTHBEARER from a token file",
            "security.protocol=SASL_SSL\nssl.truststore.type=PEM\nssl.truststore.location=/etc/kafka/ca.pem\nsasl.mechanism=OAUTHBEARER\nsasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler\nsasl.oauthbearer.token.endpoint.url=file:///var/run/secrets/token.jwt\nsasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required ;\n".into(),
            with_security(ClientSecurity {
                protocol: ListenerProtocol::SaslSsl,
                tls: Some(tls(None, "broker.example")),
                sasl: Some(SaslCredentials::OAuthBearer {
                    token: OAuthBearerTokenSource::File("/var/run/secrets/token.jwt".into()),
                    extensions: BTreeMap::new(),
                }),
                sasl_host: None,
            }),
        ),
    ];
    for (name, text, expected) in cases {
        let actual = map(&text).unwrap_or_else(|error| panic!("{name}: {error}"));
        check!(same(&actual, &expected), "{name}");
    }
}

#[test]
fn a_config_that_cannot_be_used_names_the_property() {
    const PLAIN: &str = "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"b\";\n";
    const TRUST: &str = "ssl.truststore.type=PEM\nssl.truststore.location=/ca.pem\n";
    const KRB5: &str = "sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/k\" principal=\"p@R\";\nsasl.kerberos.service.name=kafka\n";
    const OAUTH: &str = "sasl.mechanism=OAUTHBEARER\nsasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler\nsasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;\n";
    enum Kind {
        Missing,
        Invalid,
        Unsupported,
    }
    let cases = [
        ("security.protocol=TLS\n".to_owned(), "security.protocol", Kind::Invalid),
        ("request.timeout.ms=0\n".into(), "request.timeout.ms", Kind::Invalid),
        ("request.timeout.ms=soon\n".into(), "request.timeout.ms", Kind::Invalid),
        ("security.protocol=SSL\n".into(), "ssl.truststore.location", Kind::Unsupported),
        (
            "security.protocol=SSL\nssl.truststore.location=/ca.jks\n".into(),
            "ssl.truststore.type",
            Kind::Unsupported,
        ),
        (
            "security.protocol=SSL\nssl.truststore.type=JKS\nssl.truststore.location=/ca.jks\n"
                .into(),
            "ssl.truststore.type",
            Kind::Unsupported,
        ),
        (
            format!("security.protocol=SSL\n{TRUST}ssl.truststore.password=x\n"),
            "ssl.truststore.password",
            Kind::Invalid,
        ),
        (
            format!("security.protocol=SSL\n{TRUST}ssl.keystore.location=/k.p12\nssl.keystore.type=PKCS12\n"),
            "ssl.keystore.type",
            Kind::Unsupported,
        ),
        (
            format!("security.protocol=SSL\n{TRUST}ssl.keystore.location=/k.pem\nssl.keystore.type=PEM\nssl.key.password=x\n"),
            "ssl.key.password",
            Kind::Unsupported,
        ),
        (
            format!("security.protocol=SSL\n{TRUST}ssl.endpoint.identification.algorithm=\n"),
            "ssl.endpoint.identification.algorithm",
            Kind::Unsupported,
        ),
        (
            format!("security.protocol=SSL\n{TRUST}ssl.truststore.certificates=-----BEGIN\n"),
            "ssl.truststore.certificates",
            Kind::Unsupported,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\n".into(),
            "sasl.jaas.config",
            Kind::Missing,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=DIGEST-MD5\nsasl.jaas.config=x.M required;\n".into(),
            "sasl.mechanism",
            Kind::Invalid,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\";\n".into(),
            "sasl.jaas.config",
            Kind::Missing,
        ),
        (
            format!("security.protocol=SASL_PLAINTEXT\nsasl.mechanism=GSSAPI\n{PLAIN}"),
            "sasl.jaas.config",
            Kind::Invalid,
        ),
        (
            format!("security.protocol=SASL_PLAINTEXT\n{KRB5}sasl.kerberos.kdc=kdc.example\n"),
            "sasl.kerberos.kdc",
            Kind::Unsupported,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.kerberos.service.name=kafka\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;\n".into(),
            "sasl.jaas.config",
            Kind::Unsupported,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/k\" principal=\"p@R\";\n".into(),
            "sasl.kerberos.service.name",
            Kind::Missing,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.kerberos.service.name=kafka\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true serviceName=\"other\" keyTab=\"/k\" principal=\"p@R\";\n".into(),
            "sasl.kerberos.service.name",
            Kind::Invalid,
        ),
        (
            format!("security.protocol=SASL_PLAINTEXT\n{OAUTH}sasl.oauthbearer.token.endpoint.url=https://idp.example/token\n"),
            "sasl.oauthbearer.token.endpoint.url",
            Kind::Unsupported,
        ),
        (
            format!("security.protocol=SASL_PLAINTEXT\n{OAUTH}sasl.oauthbearer.token.endpoint.url=ftp://idp.example/token\n"),
            "sasl.oauthbearer.token.endpoint.url",
            Kind::Invalid,
        ),
        (
            format!("security.protocol=SASL_PLAINTEXT\n{OAUTH}"),
            "sasl.oauthbearer.token.endpoint.url",
            Kind::Missing,
        ),
        (
            "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=OAUTHBEARER\nsasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;\n".into(),
            "sasl.login.callback.handler.class",
            Kind::Unsupported,
        ),
    ];
    for (text, property, kind) in cases {
        let error = map(&text).expect_err(&text);
        let matched = match (&error, kind) {
            (ConfigError::Missing { property: p, .. }, Kind::Missing)
            | (ConfigError::Invalid { property: p, .. }, Kind::Invalid)
            | (ConfigError::Unsupported { property: p, .. }, Kind::Unsupported) => p == property,
            _ => false,
        };
        check!(matched, "{text} gave {error}");
    }
}

#[test]
fn gssapi_without_a_kdc_names_the_environment_variable() {
    let properties = Properties::parse(
        b"security.protocol=SASL_PLAINTEXT\nsasl.kerberos.service.name=kafka\nsasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab=\"/k\" principal=\"p@R\";\n",
    )
    .unwrap();
    assert!(matches!(
        args().options_from(&properties, "test", None),
        Err(ConfigError::Missing { property, .. }) if property == KDC_URL_ENV
    ));
}

#[test]
fn flags_override_the_command_config() {
    let properties = Properties::parse(b"client.id=from-file\nrequest.timeout.ms=9000\n").unwrap();
    let args = ConnectionArgs {
        client_id: Some("from-flag".into()),
        request_timeout_ms: Some(1234),
        ..args()
    };
    let expected = ConnectionOptions {
        client_id: "from-flag".into(),
        request_timeout: Time::from_millis(1234),
        socket_connection_setup_timeout: DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT,
        ..base()
    };
    assert!(same(
        &args.options_from(&properties, "test", None).unwrap(),
        &expected
    ));
    let zero = ConnectionArgs {
        request_timeout_ms: Some(0),
        ..args
    };
    assert!(matches!(
        zero.options_from(&properties, "test", None),
        Err(ConfigError::Invalid { property, .. }) if property == "--request-timeout-ms"
    ));
}

#[tokio::test]
async fn options_read_the_command_config_file() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "client.id=from-file\n").unwrap();
    let args = ConnectionArgs {
        command_config: Some(file.path().into()),
        ..args()
    };
    let options = args.options("test").await.unwrap();
    assert!(options.client_id == "from-file");

    let missing = ConnectionArgs {
        command_config: Some("/nonexistent/krabka.properties".into()),
        ..args
    };
    assert!(matches!(
        missing.options("test").await,
        Err(ConnectionError::Read { .. })
    ));
}

#[test]
fn bootstrap_host_strips_the_port_and_ipv6_brackets() {
    for (address, host) in [
        ("[2001:db8::1]:9092", "2001:db8::1"),
        ("broker.example:9092", "broker.example"),
        ("broker.example", "broker.example"),
    ] {
        let args = ConnectionArgs {
            bootstrap_server: vec![address.into()],
            ..args()
        };
        check!(args.bootstrap_host() == host);
    }
}

#[test]
fn secrets_never_render_their_value() {
    const PASSWORD: &str = "hunter2-fixture";
    let file = tempfile::NamedTempFile::new().unwrap();
    let text = format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"{PASSWORD}\";\n"
    );
    std::fs::write(file.path(), &text).unwrap();
    let args = ConnectionArgs {
        command_config: Some(file.path().into()),
        ..args()
    };
    let secret = Secret::new(PASSWORD.to_owned());
    let properties = Properties::parse(text.as_bytes()).unwrap();
    let jaas = parse_jaas(properties.get(SASL_JAAS_CONFIG).unwrap()).unwrap();
    check!(format!("{secret}") == "[redacted]");
    check!(format!("{secret:?}") == "[redacted]");
    for rendered in [
        format!("{args:?}"),
        format!("{properties:?}"),
        format!("{jaas:?}"),
    ] {
        check!(!rendered.contains(PASSWORD), "{rendered}");
    }
    check!(format!("{jaas:?}").contains("[redacted]"));
}
