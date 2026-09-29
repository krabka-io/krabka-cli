use assert2::{assert, check};
use clap::Parser;

use super::*;

const HMAC_BASE64: &str = "c2VjcmV0LWhtYWMtdmFsdWU=";

type PrincipalCase = (&'static [&'static str], Result<Vec<KafkaPrincipal>, String>);

#[derive(Debug, Parser)]
struct Command {
    #[command(flatten)]
    args: DelegationTokensArgs,
}

fn parse(argv: &[&str]) -> DelegationTokensArgs {
    Command::try_parse_from(std::iter::once("delegation-tokens").chain(argv.iter().copied()))
        .unwrap()
        .args
}

fn with_connection(argv: &[&str]) -> DelegationTokensArgs {
    parse(
        &[
            &["--bootstrap-server", "h:1", "--command-config", "/dev/null"][..],
            argv,
        ]
        .concat(),
    )
}

fn token() -> TokenRow {
    TokenRow {
        token_id: "tok-1".into(),
        hmac: Secret::new(b"secret-hmac-value".to_vec()),
        owner: "User:alice".into(),
        requester: Some("User:admin".into()),
        renewers: vec!["User:bob".into(), "User:carol".into()],
        issue_timestamp_ms: 1_700_000_000_000,
        expiry_timestamp_ms: 1_700_086_400_000,
        max_timestamp_ms: 1_700_604_800_000,
    }
}

#[test]
fn no_debug_rendering_carries_an_hmac() {
    let args = with_connection(&[
        "--renew",
        "--renew-time-period",
        "-1",
        "--hmac",
        HMAC_BASE64,
    ]);
    let token = token();
    for rendered in [format!("{args:?}"), format!("{token:?}")] {
        check!(!rendered.contains(HMAC_BASE64), "{rendered}");
        check!(!rendered.contains("secret-hmac-value"), "{rendered}");
        check!(!rendered.contains("115, 101, 99"), "{rendered}");
        check!(rendered.contains("[redacted]"), "{rendered}");
    }
}

#[test]
fn flag_checks_match_kafkas_order_and_messages() {
    let cases: [(&[&str], Result<(), &str>); 13] = [
        (&["--create", "--max-life-time-period", "-1"], Ok(())),
        (&["--describe"], Ok(())),
        (&["--describe", "--owner-principal", "User:a"], Ok(())),
        (
            &["--renew", "--hmac", "x", "--renew-time-period", "-1"],
            Ok(()),
        ),
        (
            &[
                "--expire",
                "--hmac-file",
                "/f",
                "--expiry-time-period",
                "-1",
                "--dry-run",
            ],
            Ok(()),
        ),
        (
            &[],
            Err(
                "Command must include exactly one action: --create, --renew, --expire or --describe",
            ),
        ),
        (
            &["--create", "--describe"],
            Err(
                "Command must include exactly one action: --create, --renew, --expire or --describe",
            ),
        ),
        (
            &["--create"],
            Err("Missing required argument \"[max-life-time-period]\""),
        ),
        (
            &["--renew", "--renew-time-period", "1"],
            Err("Missing required argument \"[hmac]\""),
        ),
        (
            &["--create", "--max-life-time-period", "1", "--hmac", "x"],
            Err("Option \"[create]\" can't be used with option \"[hmac]\""),
        ),
        (
            &[
                "--renew",
                "--hmac",
                "x",
                "--renew-time-period",
                "1",
                "--owner-principal",
                "User:a",
            ],
            Err("Option \"[renew]\" can't be used with option \"[owner-principal]\""),
        ),
        (
            &["--describe", "--expiry-time-period", "1"],
            Err("Option \"[describe]\" can't be used with option \"[expiry-time-period]\""),
        ),
        (
            &["--describe", "--dry-run"],
            Err("--dry-run is only valid with --expire"),
        ),
    ];
    for (argv, expected) in cases {
        check!(
            with_connection(argv).check(false).map(|_| ()) == expected.map_err(str::to_owned),
            "{argv:?}"
        );
    }
}

#[test]
fn bootstrap_server_and_command_config_are_required() {
    let cases: [(&[&str], &str); 2] = [
        (
            &["--describe", "--command-config", "/dev/null"],
            "Missing required argument \"[bootstrap-server]\"",
        ),
        (
            &["--describe", "--bootstrap-server", "h:1"],
            "Missing required argument \"[command-config]\"",
        ),
    ];
    for (argv, expected) in cases {
        check!(
            parse(argv).check(false) == Err(expected.to_owned()),
            "{argv:?}"
        );
    }
}

#[test]
fn the_hmac_environment_variable_answers_the_hmac_requirement() {
    let args = with_connection(&["--expire", "--expiry-time-period", "-1"]);
    check!(args.check(false) == Err("Missing required argument \"[hmac]\"".to_owned()));
    check!(args.check(true) == Ok(Action::Expire));
}

#[tokio::test]
async fn the_hmac_comes_from_the_flag_then_the_file_then_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("hmac");
    std::fs::write(&file, "from-file\n").unwrap();
    let file = file.to_str().unwrap();
    let env = || Some(Secret::new(" from-env ".to_owned()));
    let cases: [(&[&str], Result<&str, &str>); 3] = [
        (&["--hmac", "from-flag"], Ok("from-flag")),
        (&["--hmac-file", file], Ok("from-file")),
        (&[], Ok("from-env")),
    ];
    for (argv, expected) in cases {
        let args = with_connection(&[&["--renew", "--renew-time-period", "-1"][..], argv].concat());
        let got = args.hmac_value(env()).await.map(Secret::expose);
        check!(
            got == expected.map(str::to_owned).map_err(str::to_owned),
            "{argv:?}"
        );
    }
    let args = with_connection(&["--renew", "--renew-time-period", "-1"]);
    check!(args.hmac_value(None).await.is_err());
}

#[test]
fn hmac_and_hmac_file_conflict_in_the_parser() {
    let argv = [
        "delegation-tokens",
        "--renew",
        "--hmac",
        "x",
        "--hmac-file",
        "/f",
    ];
    assert!(Command::try_parse_from(argv).is_err());
}

#[test]
fn principals_parse_as_kafkas_security_utils_does() {
    let principal = |principal_type: &str, name: &str| KafkaPrincipal {
        principal_type: principal_type.into(),
        name: name.into(),
    };
    let cases: [PrincipalCase; 4] = [
        (
            &[" User:alice ", "Group:ops"],
            Ok(vec![principal("User", "alice"), principal("Group", "ops")]),
        ),
        (&["User:a:b"], Ok(vec![principal("User", "a:b")])),
        (
            &["bad"],
            Err("expected a string in format principalType:principalName but got bad".into()),
        ),
        (
            &[""],
            Err("expected a string in format principalType:principalName but got ".into()),
        ),
    ];
    for (values, expected) in cases {
        let values = values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        check!(principals(&values) == expected, "{values:?}");
    }
}

#[test]
fn base64_round_trips_as_javas_codec_does() {
    for (bytes, encoded) in [
        (&b""[..], ""),
        (&b"f"[..], "Zg=="),
        (&b"fo"[..], "Zm8="),
        (&b"foo"[..], "Zm9v"),
        (&b"secret-hmac-value"[..], HMAC_BASE64),
        (&[0xfb, 0xff, 0xbf][..], "+/+/"),
    ] {
        check!(encode_base64(bytes) == encoded);
        check!(decode_base64(encoded.as_bytes()) == Ok(bytes.to_vec()));
    }
    check!(decode_base64(b"Zg") == Ok(b"f".to_vec()));
    check!(decode_base64(b"Zm8") == Ok(b"fo".to_vec()));
}

#[test]
fn malformed_base64_fails_with_javas_messages() {
    for (input, expected) in [
        (
            &b"Z"[..],
            "Input byte[] should at least have 2 bytes for base64 bytes",
        ),
        (&b"Zm9v!"[..], "Illegal base64 character 21"),
        (&b"Zm9vZ"[..], "Last unit does not have enough valid bits"),
        (
            &b"Zg=x"[..],
            "Input byte array has wrong 4-byte ending unit",
        ),
        (
            &b"Zg==Zg=="[..],
            "Input byte array has incorrect ending byte at 4",
        ),
        (
            &b"=abc"[..],
            "Input byte array has wrong 4-byte ending unit",
        ),
    ] {
        check!(
            decode_base64(input) == Err(expected.to_owned()),
            "{input:?}"
        );
    }
}

#[test]
fn dates_print_as_simple_date_format_does_in_utc() {
    for (epoch_ms, expected) in [
        (0, "1970-01-01T00:00"),
        (1_700_000_000_000, "2023-11-14T22:13"),
        (951_782_400_000, "2000-02-29T00:00"),
        (-1, "1969-12-31T23:59"),
        (253_402_300_799_000, "9999-12-31T23:59"),
    ] {
        check!(format_date(epoch_ms) == expected, "{epoch_ms}");
    }
}

#[test]
fn describe_prints_kafkas_header_and_token_rows() {
    let owners = [KafkaPrincipal {
        principal_type: "User".into(),
        name: "alice".into(),
    }];
    let result = described(
        &owners,
        &[TokenRow {
            requester: None,
            ..token()
        }],
    );
    check!(
        result.human
            == [
                "Calling describe token operation for owners: [User:alice]".to_owned(),
                "Total number of tokens : 1".to_owned(),
                "TOKENID         HMAC                           OWNER           REQUESTER       \
                 RENEWERS                  ISSUEDATE       EXPIRYDATE      MAXDATE        "
                    .to_owned(),
                String::new(),
                "tok-1           c2VjcmV0LWhtYWMtdmFsdWU=       User:alice      :               \
                 [User:bob, User:carol]    2023-11-14T22:13 2023-11-15T22:13 2023-11-21T22:13"
                    .to_owned(),
            ]
    );
    check!(
        result.data
            == json!([{
                "token_id": "tok-1",
                "hmac": HMAC_BASE64,
                "owner": "User:alice",
                "requester": null,
                "renewers": ["User:bob", "User:carol"],
                "issue_timestamp_ms": 1_700_000_000_000_i64,
                "expiry_timestamp_ms": 1_700_086_400_000_i64,
                "max_timestamp_ms": 1_700_604_800_000_i64,
            }])
    );
}

#[test]
fn create_prints_kafkas_calling_line_and_the_new_token() {
    let result = created(&["User:bob".to_owned()], -1, &token());
    check!(
        result.human[..3]
            == [
                "Calling create token operation with renewers :[User:bob] , max-life-time-period :-1",
                "Created delegation token with tokenId : tok-1",
                "",
            ]
    );
    check!(result.human[5].starts_with(
        "tok-1           c2VjcmV0LWhtYWMtdmFsdWU=       User:alice      User:admin      "
    ));
}
