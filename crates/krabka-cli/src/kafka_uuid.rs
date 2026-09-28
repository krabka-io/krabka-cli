//! Kafka's `Uuid` text form: the 16 bytes in URL-safe base64 without padding.
//!
//! Kafka tools print a directory ID or a cluster ID in this form, and
//! `Uuid.fromString` reads it back. [`KafkaUuid::parse`] refuses what
//! `Uuid.fromString` refuses, with the same message, because an operator's
//! runbook quotes those messages.

use std::fmt;

/// A 128-bit Kafka `Uuid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KafkaUuid([u8; 16]);

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

impl KafkaUuid {
    /// Kafka's `Uuid.ZERO_UUID`.
    pub const ZERO: Self = Self([0; 16]);

    /// The UUID of these bytes, most significant first.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Parses Kafka's base64 form as `Uuid.fromString` does.
    ///
    /// # Errors
    /// Returns the message of the `IllegalArgumentException` that
    /// `Uuid.fromString` throws for the same text.
    pub fn parse(text: &str) -> Result<Self, String> {
        let chars = text.chars().collect::<Vec<_>>();
        if chars.len() > 24 {
            let prefix = chars[..24].iter().collect::<String>();
            return Err(format!(
                "Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID"
            ));
        }
        let bytes = decode_url(text)?;
        <[u8; 16]>::try_from(bytes.as_slice())
            .map(Self)
            .map_err(|_| {
                format!(
                    "Input string `{text}` decoded as {} bytes, which is not equal to the \
                     expected 16 bytes of a base64-encoded UUID",
                    bytes.len()
                )
            })
    }

    /// Parses Kafka's base64 form or the canonical hyphenated form.
    ///
    /// The hyphenated form is a krabka addition: `krabka format` writes it,
    /// and Kafka refuses it as too long.
    ///
    /// # Errors
    /// Returns the [`KafkaUuid::parse`] message for text that is neither.
    pub fn parse_either(text: &str) -> Result<Self, String> {
        parse_hyphenated(text).map_or_else(|| Self::parse(text), Ok)
    }
}

fn parse_hyphenated(text: &str) -> Option<KafkaUuid> {
    let groups = text.split('-').map(str::len).collect::<Vec<_>>();
    if groups != [8, 4, 4, 4, 12] {
        return None;
    }
    let hex = text.replace('-', "");
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(KafkaUuid(bytes))
}

impl fmt::Display for KafkaUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::with_capacity(22);
        for chunk in self.0.chunks(3) {
            let bits = chunk.iter().enumerate().fold(0_u32, |bits, (index, byte)| {
                bits | u32::from(*byte) << (16 - 8 * index)
            });
            for position in 0..=chunk.len() {
                let index = (bits >> (18 - 6 * position)) & 0x3f;
                out.push(char::from(ALPHABET[index as usize]));
            }
        }
        f.write_str(&out)
    }
}

/// The value of one base64url character, or `None`.
fn value(byte: u8) -> Option<u32> {
    ALPHABET
        .iter()
        .position(|&c| c == byte)
        .and_then(|index| u32::try_from(index).ok())
}

/// Decodes as `Base64.getUrlDecoder().decode(String)` does, with the
/// messages of its `IllegalArgumentException`.
fn decode_url(text: &str) -> Result<Vec<u8>, String> {
    // The JVM encodes the string as ISO-8859-1 first; a character outside it
    // becomes `?`.
    let src = text
        .chars()
        .map(|c| u8::try_from(u32::from(c)).unwrap_or(b'?'))
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    let mut bits = 0_u32;
    let mut shift = 18_i32;
    let mut position = 0;
    while position < src.len() {
        let byte = src[position];
        position += 1;
        let Some(value) = value(byte) else {
            if byte == b'=' {
                if (shift == 6 && (position == src.len() || src[position] != b'=')) || shift == 18 {
                    return Err("Input byte array has wrong 4-byte ending unit".into());
                }
                if shift == 6 {
                    position += 1;
                }
                break;
            }
            // Java prints the signed byte in hex.
            let signed = i32::from(i8::from_ne_bytes([byte]));
            let hex = if signed < 0 {
                format!("-{:x}", -signed)
            } else {
                format!("{signed:x}")
            };
            return Err(format!("Illegal base64 character {hex}"));
        };
        bits |= value << shift;
        shift -= 6;
        if shift < 0 {
            out.extend_from_slice(&bits.to_be_bytes()[1..]);
            shift = 18;
            bits = 0;
        }
    }
    match shift {
        6 => out.push(bits.to_be_bytes()[1]),
        0 => out.extend_from_slice(&bits.to_be_bytes()[1..3]),
        12 => return Err("Last unit does not have enough valid bits".into()),
        _ => {}
    }
    if position < src.len() {
        return Err(format!(
            "Input byte array has incorrect ending byte at {position}"
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn uuids_render_and_parse_as_kafka_does() {
        let one = KafkaUuid::from_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        check!(KafkaUuid::ZERO.to_string() == "AAAAAAAAAAAAAAAAAAAAAA");
        check!(one.to_string() == "AAAAAAAAAAAAAAAAAAAAAQ");
        check!(KafkaUuid::parse("AAAAAAAAAAAAAAAAAAAAAQ") == Ok(one));
        let bytes = [
            0xe4, 0xbe, 0xa0, 0xdd, 0x7b, 0x3a, 0x4f, 0xe3, 0x8c, 0x0a, 0xd2, 0xbe, 0x3f, 0xf5,
            0xf3, 0xab,
        ];
        let id = KafkaUuid::from_bytes(bytes);
        check!(id.to_string() == "5L6g3Xs6T-OMCtK-P_Xzqw");
        check!(KafkaUuid::parse(&id.to_string()) == Ok(id));
        check!(KafkaUuid::parse_either("e4bea0dd-7b3a-4fe3-8c0a-d2be3ff5f3ab") == Ok(id));
        check!(KafkaUuid::parse_either("5L6g3Xs6T-OMCtK-P_Xzqw") == Ok(id));
    }

    #[test]
    fn malformed_uuids_fail_with_kafkas_messages() {
        let cases = [
            ("bogus", "Last unit does not have enough valid bits"),
            (
                "AAAAAAAAAAAAAAAAAAAAAAAAA",
                "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAAAA` is too long to be decoded \
                 as a base64 UUID",
            ),
            (
                "AAAA",
                "Input string `AAAA` decoded as 3 bytes, which is not equal to the expected 16 \
                 bytes of a base64-encoded UUID",
            ),
            ("AA+A", "Illegal base64 character 2b"),
            ("AA\u{e9}A", "Illegal base64 character -17"),
            ("A===", "Last unit does not have enough valid bits"),
            ("AA=", "Input byte array has wrong 4-byte ending unit"),
            ("AA==A", "Input byte array has incorrect ending byte at 4"),
            (
                "AAA=",
                "Input string `AAA=` decoded as 2 bytes, which is not equal to the expected 16 \
                 bytes of a base64-encoded UUID",
            ),
            (
                "e4bea0dd-7b3a-4fe3-8c0a-d2be3ff5f3ab",
                "Input string with prefix `e4bea0dd-7b3a-4fe3-8c0a-` is too long to be decoded \
                 as a base64 UUID",
            ),
        ];
        for (text, message) in cases {
            check!(KafkaUuid::parse(text) == Err(message.to_owned()), "{text}");
        }
    }
}
