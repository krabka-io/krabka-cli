//! `krabka delegation-tokens`, the counterpart of `kafka-delegation-tokens`.
//!
//! The flags, their checks, their messages and the report are those of
//! `DelegationTokenCommand` in Kafka 4.3.1. `--bootstrap-server` and
//! `--command-config` are required, as they are there.
//!
//! An HMAC is a bearer credential. It is held as a [`Secret`], so no `Debug`
//! rendering, log line or error message carries it. The report prints it on
//! stdout, base64-encoded, where the JVM tool prints it. `--hmac-file` and the
//! [`HMAC_ENV`] environment variable are krabka additions that keep the HMAC
//! out of the shell history and the process listing; `--hmac` wins over
//! `--hmac-file`, which wins over the environment.
//!
//! Dates print as `yyyy-MM-dd'T'HH:mm` in UTC. The JVM tool prints them in the
//! JVM's default time zone, which is UTC in Kafka's container images.

use std::path::PathBuf;

use clap::{
    Args,
    builder::{StringValueParser, TypedValueParser as _},
};
use krabka_client_admin::{
    AdminClient, CreateDelegationTokenOptions, DelegationToken, DescribeDelegationTokenOptions,
    ExpireDelegationTokenOptions, RenewDelegationTokenOptions,
};
use krabka_security::KafkaPrincipal;
use krabka_units::{Time, convert::TimeExt as _};
use serde_json::{Value, json};

use crate::{
    connection::{ConnectionArgs, Secret},
    kafka_errors::admin_error,
    output::{CommandError, CommandResult},
    safety::{ConfirmArgs, Impact, confirm},
};

/// The environment variable that `--renew` and `--expire` read the HMAC from
/// when neither `--hmac` nor `--hmac-file` is given.
pub const HMAC_ENV: &str = "KRABKA_DELEGATION_TOKEN_HMAC";

/// The `-1` that the JVM tool's time-period flags take for "the broker's
/// default" or "now".
const BROKER_DEFAULT: i64 = -1;

#[derive(Debug, Args)]
pub struct DelegationTokensArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Create a new delegation token. Use --renewer-principal option to pass
    /// renewer principals.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    create: Option<bool>,
    /// Renew delegation token. Use --renew-time-period option to set renew
    /// time period.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    renew: Option<bool>,
    /// Expire delegation token. Use --expiry-time-period option to expire the
    /// token.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    expire: Option<bool>,
    /// Describe delegation tokens for the given principals. Use
    /// --owner-principal to pass owner/renewer principals.
    #[arg(long, num_args = 0, default_missing_value = "true")]
    describe: Option<bool>,
    /// Owner is a Kafka principal. They should be in principalType:name
    /// format.
    #[arg(long)]
    owner_principal: Vec<String>,
    /// Renewer is a Kafka principal. They should be in principalType:name
    /// format.
    #[arg(long)]
    renewer_principal: Vec<String>,
    /// Max life period for the token in milliseconds. If the value is -1, then
    /// token max life time will default to the server side config value of
    /// (delegation.token.max.lifetime.ms).
    #[arg(long, allow_negative_numbers = true)]
    max_life_time_period: Option<i64>,
    /// Renew time period in milliseconds. If the value is -1, then the renew
    /// time period will default to the server side config value of
    /// (delegation.token.expiry.time.ms).
    #[arg(long, allow_negative_numbers = true)]
    renew_time_period: Option<i64>,
    /// Expiry time period in milliseconds. If the value is -1, then the token
    /// will get invalidated immediately.
    #[arg(long, allow_negative_numbers = true)]
    expiry_time_period: Option<i64>,
    /// HMAC of the delegation token, base64-encoded.
    #[arg(long, value_parser = StringValueParser::new().map(Secret::new))]
    hmac: Option<Secret<String>>,
    /// A file that holds the HMAC of the delegation token, base64-encoded.
    #[arg(long, conflicts_with = "hmac")]
    hmac_file: Option<PathBuf>,
    #[command(flatten)]
    confirm: ConfirmArgs,
}

/// The one action of a `delegation-tokens` command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Create,
    Renew,
    Expire,
    Describe,
}

/// One delegation token, as the JVM tool's table prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenRow {
    token_id: String,
    hmac: Secret<Vec<u8>>,
    owner: String,
    /// `None` when the answer does not name the requester.
    requester: Option<String>,
    renewers: Vec<String>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
}

impl DelegationTokensArgs {
    pub async fn run(self) -> Result<CommandResult, CommandError> {
        let env_hmac = std::env::var(HMAC_ENV).ok().map(Secret::new);
        let action = self.check(env_hmac.is_some())?;
        if action == Action::Create {
            let renewers = principals(&self.renewer_principal)?;
            let owner = principals(&self.owner_principal)?.into_iter().next();
            let max_life_time = self.max_life_time_period.unwrap_or(BROKER_DEFAULT);
            let client = self.connect().await?;
            return create(&client, &renewers, owner.as_ref(), max_life_time).await;
        }
        if action == Action::Describe {
            let owners = principals(&self.owner_principal)?;
            let client = self.connect().await?;
            return describe(&client, &owners).await;
        }
        let encoded = self.hmac_value(env_hmac).await?;
        let hmac = decode_base64(encoded.clone().expose().as_bytes())?;
        if action == Action::Renew {
            let period = self.renew_time_period.unwrap_or(BROKER_DEFAULT);
            let client = self.connect().await?;
            let expiry = client
                .renew_delegation_token(
                    &hmac,
                    RenewDelegationTokenOptions {
                        renew_time_period: time_period(period),
                    },
                )
                .await
                .map_err(admin_error)?;
            return Ok(CommandResult::success(
                vec![
                    format!(
                        "Calling renew token operation with hmac :{} , renew-time-period :{period}",
                        encoded.expose()
                    ),
                    format!(
                        "Completed renew operation. New expiry date : {}",
                        format_date(expiry)
                    ),
                ],
                json!({"expiry_timestamp_ms": expiry}),
            ));
        }
        let period = self.expiry_time_period.unwrap_or(BROKER_DEFAULT);
        let calling = format!(
            "Calling expire token operation with hmac :{} , expire-time-period :{period}",
            encoded.expose()
        );
        let client = self.connect().await?;
        if self.confirm.dry_run {
            return Ok(CommandResult::success(
                vec![calling],
                json!({"expiry_timestamp_ms": Value::Null}),
            )
            .into_dry_run());
        }
        let summary = if period < 0 {
            "expire the delegation token of the given HMAC now".to_owned()
        } else {
            format!("expire the delegation token of the given HMAC in {period} ms")
        };
        confirm(
            self.confirm.yes,
            "krabka delegation-tokens",
            Impact {
                summary,
                resources: Vec::new(),
            },
        )
        .await?;
        let expiry = client
            .expire_delegation_token(
                &hmac,
                ExpireDelegationTokenOptions {
                    expiry_time_period: time_period(period),
                },
            )
            .await
            .map_err(admin_error)?;
        Ok(CommandResult::success(
            vec![
                calling,
                format!(
                    "Completed expire operation. New expiry date : {}",
                    format_date(expiry)
                ),
            ],
            json!({"expiry_timestamp_ms": expiry}),
        ))
    }

    /// Kafka's checks of the flags, in Kafka's order and with Kafka's
    /// messages. `env_hmac` tells whether [`HMAC_ENV`] holds an HMAC.
    fn check(&self, env_hmac: bool) -> Result<Action, String> {
        let given = [
            (self.create, Action::Create),
            (self.renew, Action::Renew),
            (self.expire, Action::Expire),
            (self.describe, Action::Describe),
        ]
        .into_iter()
        .filter_map(|(flag, action)| flag.map(|_| action))
        .collect::<Vec<_>>();
        let [action] = given[..] else {
            return Err(
                "Command must include exactly one action: --create, --renew, --expire or \
                 --describe"
                    .into(),
            );
        };
        let create = action == Action::Create;
        let renew = action == Action::Renew;
        let expire = action == Action::Expire;
        let describe = action == Action::Describe;
        let hmac_flag = self.hmac.is_some() || self.hmac_file.is_some();
        let owner = !self.owner_principal.is_empty();
        let renewer = !self.renewer_principal.is_empty();
        let max_life = self.max_life_time_period.is_some();
        let renew_period = self.renew_time_period.is_some();
        let expiry_period = self.expiry_time_period.is_some();
        let required = [
            (
                true,
                "bootstrap-server",
                !self.connection.bootstrap_server.is_empty(),
            ),
            (
                true,
                "command-config",
                self.connection.command_config.is_some(),
            ),
            (create, "max-life-time-period", max_life),
            (renew, "hmac", hmac_flag || env_hmac),
            (renew, "renew-time-period", renew_period),
            (expire, "hmac", hmac_flag || env_hmac),
            (expire, "expiry-time-period", expiry_period),
        ];
        if let Some((_, name, _)) = required
            .iter()
            .find(|(applies, _, present)| *applies && !present)
        {
            return Err(format!("Missing required argument \"[{name}]\""));
        }
        let invalid = [
            (create, "create", "hmac", hmac_flag),
            (create, "create", "renew-time-period", renew_period),
            (create, "create", "expiry-time-period", expiry_period),
            (renew, "renew", "renewer-principal", renewer),
            (renew, "renew", "max-life-time-period", max_life),
            (renew, "renew", "expiry-time-period", expiry_period),
            (renew, "renew", "owner-principal", owner),
            (expire, "expire", "max-life-time-period", max_life),
            (expire, "expire", "renew-time-period", renew_period),
            (expire, "expire", "owner-principal", owner),
            (describe, "describe", "renew-time-period", renew_period),
            (describe, "describe", "max-life-time-period", max_life),
            (describe, "describe", "hmac", hmac_flag),
            (describe, "describe", "expiry-time-period", expiry_period),
        ];
        if let Some((_, used, other, _)) = invalid
            .iter()
            .find(|(action, _, _, present)| *action && *present)
        {
            return Err(format!(
                "Option \"[{used}]\" can't be used with option \"[{other}]\""
            ));
        }
        if self.confirm.dry_run && !expire {
            return Err("--dry-run is only valid with --expire".into());
        }
        Ok(action)
    }

    async fn connect(&self) -> Result<AdminClient, CommandError> {
        Ok(self.connection.connect("delegation-tokens").await?)
    }

    /// The HMAC from `--hmac`, else from `--hmac-file`, else from `env`.
    async fn hmac_value(&self, env: Option<Secret<String>>) -> Result<Secret<String>, String> {
        if let Some(hmac) = &self.hmac {
            return Ok(hmac.clone());
        }
        if let Some(path) = &self.hmac_file {
            let text = tokio::fs::read_to_string(path)
                .await
                .map_err(|error| format!("read HMAC file {}: {error}", path.display()))?;
            return Ok(Secret::new(text.trim().to_owned()));
        }
        env.map(|value| Secret::new(value.expose().trim().to_owned()))
            .ok_or_else(|| "Missing required argument \"[hmac]\"".to_owned())
    }
}

/// A time-period flag as the admin options take it: Kafka's `-1`, and any
/// other negative value, is `None`, which sends `-1`.
fn time_period(millis: i64) -> Option<Time> {
    (millis >= 0).then(|| Time::from_millis(millis))
}

async fn create(
    client: &AdminClient,
    renewers: &[KafkaPrincipal],
    owner: Option<&KafkaPrincipal>,
    lifetime_ms: i64,
) -> Result<CommandResult, CommandError> {
    let renewer_names = renewers.iter().map(ToString::to_string).collect::<Vec<_>>();
    // Kafka's `CreateDelegationTokenOptions.maxLifetimeMs` sends the value
    // as given; the broker uses its maximum for zero or less.
    let options = CreateDelegationTokenOptions {
        owner: owner.cloned(),
        renewers: renewers.to_vec(),
        max_lifetime: (lifetime_ms != BROKER_DEFAULT).then(|| Time::from_millis(lifetime_ms)),
    };
    let token = client
        .create_delegation_token(&options)
        .await
        .map_err(admin_error)?;
    Ok(created(&renewer_names, lifetime_ms, &token_row(token)))
}

fn token_row(token: DelegationToken) -> TokenRow {
    TokenRow {
        token_id: token.token_id,
        hmac: Secret::new(token.hmac),
        owner: token.owner.to_string(),
        requester: Some(token.token_requester.to_string()),
        renewers: token.renewers.iter().map(ToString::to_string).collect(),
        issue_timestamp_ms: token.issue_timestamp_ms,
        expiry_timestamp_ms: token.expiry_timestamp_ms,
        max_timestamp_ms: token.max_timestamp_ms,
    }
}

async fn describe(
    client: &AdminClient,
    owners: &[KafkaPrincipal],
) -> Result<CommandResult, CommandError> {
    // Kafka's command passes the parsed owner list, empty when no
    // --owner-principal is given, to `DescribeDelegationTokenOptions.owners`.
    let options = DescribeDelegationTokenOptions {
        owners: (!owners.is_empty()).then(|| owners.to_vec()),
    };
    let tokens = client
        .describe_delegation_token(&options)
        .await
        .map_err(admin_error)?
        .into_iter()
        .map(token_row)
        .collect::<Vec<_>>();
    Ok(described(owners, &tokens))
}

/// Parses principals as Kafka's `SecurityUtils.parseKafkaPrincipal` does,
/// after trimming each value as the JVM tool does.
fn principals(values: &[String]) -> Result<Vec<KafkaPrincipal>, String> {
    values
        .iter()
        .map(|value| {
            let value = value.trim();
            value
                .split_once(':')
                .map(|(principal_type, name)| KafkaPrincipal {
                    principal_type: principal_type.to_owned(),
                    name: name.to_owned(),
                })
                .ok_or_else(|| {
                    format!(
                        "expected a string in format principalType:principalName but got {value}"
                    )
                })
        })
        .collect()
}

/// A Java `List.toString`: `[a, b]`.
fn java_list(items: &[String]) -> String {
    format!("[{}]", items.join(", "))
}

/// The report of `--create`.
fn created(renewers: &[String], lifetime_ms: i64, token: &TokenRow) -> CommandResult {
    let mut human = vec![
        format!(
            "Calling create token operation with renewers :{} , max-life-time-period \
             :{lifetime_ms}",
            java_list(renewers)
        ),
        format!("Created delegation token with tokenId : {}", token.token_id),
        String::new(),
    ];
    human.extend(token_table(std::slice::from_ref(token)));
    CommandResult::success(human, token_json(token))
}

/// The report of `--describe`.
fn described(owners: &[KafkaPrincipal], tokens: &[TokenRow]) -> CommandResult {
    let owners = owners.iter().map(ToString::to_string).collect::<Vec<_>>();
    let calling = if owners.is_empty() {
        "Calling describe token operation for current user.".to_owned()
    } else {
        format!(
            "Calling describe token operation for owners: {}",
            java_list(&owners)
        )
    };
    let mut human = vec![
        calling,
        format!("Total number of tokens : {}", tokens.len()),
    ];
    human.extend(token_table(tokens));
    CommandResult::success(human, tokens.iter().map(token_json).collect::<Vec<_>>())
}

/// Pads `value` to `width` as Java's `%-<width>s` does, counting UTF-16 code
/// units.
fn pad(value: &str, width: usize) -> String {
    let len = value.encode_utf16().count();
    format!("{value}{}", " ".repeat(width.saturating_sub(len)))
}

/// The table of Kafka's `printToken`: the header, then a blank line before
/// each token.
fn token_table(tokens: &[TokenRow]) -> Vec<String> {
    const WIDTHS: [usize; 8] = [15, 30, 15, 15, 25, 15, 15, 15];
    let line = |cells: [String; 8]| {
        cells
            .iter()
            .zip(WIDTHS)
            .map(|(cell, width)| pad(cell, width))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let header = [
        "TOKENID",
        "HMAC",
        "OWNER",
        "REQUESTER",
        "RENEWERS",
        "ISSUEDATE",
        "EXPIRYDATE",
        "MAXDATE",
    ]
    .map(str::to_owned);
    let mut lines = vec![line(header)];
    for token in tokens {
        lines.push(String::new());
        lines.push(line([
            token.token_id.clone(),
            encode_base64(&token.hmac.clone().expose()),
            token.owner.clone(),
            // Kafka prints an empty principal, `:`, when the answer does not
            // name the requester.
            token.requester.clone().unwrap_or_else(|| ":".to_owned()),
            java_list(&token.renewers),
            format_date(token.issue_timestamp_ms),
            format_date(token.expiry_timestamp_ms),
            format_date(token.max_timestamp_ms),
        ]));
    }
    lines
}

fn token_json(token: &TokenRow) -> Value {
    json!({
        "token_id": token.token_id,
        "hmac": encode_base64(&token.hmac.clone().expose()),
        "owner": token.owner,
        "requester": token.requester,
        "renewers": token.renewers,
        "issue_timestamp_ms": token.issue_timestamp_ms,
        "expiry_timestamp_ms": token.expiry_timestamp_ms,
        "max_timestamp_ms": token.max_timestamp_ms,
    })
}

/// `yyyy-MM-dd'T'HH:mm` in UTC, as Java's `SimpleDateFormat` prints an epoch
/// millisecond value in a UTC JVM.
fn format_date(epoch_ms: i64) -> String {
    let seconds = epoch_ms.div_euclid(1_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let second_of_day = seconds.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}",
        second_of_day / 3_600,
        second_of_day % 3_600 / 60
    )
}

/// The proleptic Gregorian date of a day count from 1970-01-01, by Howard
/// Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding, as Java's `Base64.getEncoder()` writes it.
fn encode_base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let bits = chunk
            .iter()
            .enumerate()
            .fold(0_u32, |bits, (index, &byte)| {
                bits | u32::from(byte) << (16 - 8 * index)
            });
        for index in 0..4 {
            if index <= chunk.len() {
                let sextet = (bits >> (18 - 6 * index)) & 0x3f;
                out.push(char::from(BASE64_ALPHABET[sextet as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Decodes base64 as Java's `Base64.getDecoder()` does, with its messages.
/// Padding is optional; a character outside the alphabet, a misplaced `=`, or
/// a final unit of one character is refused.
fn decode_base64(input: &[u8]) -> Result<Vec<u8>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    if input.len() < 2 {
        return Err("Input byte[] should at least have 2 bytes for base64 bytes".into());
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3 + 2);
    let mut bits = 0_u32;
    let mut shift = 18_i32;
    let mut position = 0;
    while position < input.len() {
        let byte = input[position];
        position += 1;
        if byte == b'=' {
            let complete = match shift {
                6 => {
                    let next = input.get(position) == Some(&b'=');
                    position += 1;
                    next
                }
                18 => false,
                _ => true,
            };
            if !complete {
                return Err("Input byte array has wrong 4-byte ending unit".into());
            }
            break;
        }
        let Some(sextet) = BASE64_ALPHABET.iter().position(|&symbol| symbol == byte) else {
            let signed = i32::from(i8::from_ne_bytes([byte]));
            let hex = if signed < 0 {
                format!("-{:x}", -signed)
            } else {
                format!("{signed:x}")
            };
            return Err(format!("Illegal base64 character {hex}"));
        };
        bits |= u32::try_from(sextet).expect("a base64 sextet fits in u32") << shift;
        shift -= 6;
        if shift < 0 {
            out.extend_from_slice(&bits.to_be_bytes()[1..]);
            bits = 0;
            shift = 18;
        }
    }
    match shift {
        6 => out.push(bits.to_be_bytes()[1]),
        0 => out.extend_from_slice(&bits.to_be_bytes()[1..3]),
        12 => return Err("Last unit does not have enough valid bits".into()),
        _ => {}
    }
    if position < input.len() {
        return Err(format!(
            "Input byte array has incorrect ending byte at {position}"
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
