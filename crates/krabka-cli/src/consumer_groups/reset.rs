//! `--reset-offsets` planning, as Kafka's `OffsetsUtils` computes a plan.
//!
//! [`plan`] turns a scenario, the partitions in scope and the group's committed
//! offsets into the new offset of each partition. Log offsets come through
//! [`OffsetLookup`], the seam where `AdminClient::list_offsets` goes once the
//! pinned `krabka-client-rs` has it. Until then every lookup fails with a
//! "not supported by this build" error, and the scenarios that cannot do
//! without one fail with that error.

use std::collections::BTreeMap;

use crate::{
    compat::{KafkaException, default_capacity, hash_order, topic_partition_hash},
    get_offsets::{OffsetLookup, OffsetSpec, PartitionOffset},
    output::CommandError,
};

/// A partition, `(topic, partition)`.
pub type Partition = (String, i32);

/// The new offset of each partition of one group, in the order Kafka prints
/// them.
pub type Plan = Vec<(Partition, i64)>;

/// The reset scenario: exactly one `--to-*`, `--shift-by`, `--by-duration` or
/// `--from-file`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scenario {
    ToOffset(i64),
    ToEarliest,
    ToLatest,
    ToCurrent,
    ShiftBy(i64),
    /// `--to-datetime`, as Kafka epoch milliseconds.
    ToDatetime(i64),
    /// `--by-duration`, as milliseconds before now.
    ByDuration(i64),
    /// `--from-file`: the offsets that the file names for this group, or
    /// `None` when the file names none.
    FromFile(Option<BTreeMap<Partition, i64>>),
}

/// A plan and what the JVM tool prints while it computes one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Planned {
    pub plan: Plan,
    /// Lines that the JVM tool prints to stdout before the table, such as
    /// `Warn: Partition 0 from topic t is empty. ...`.
    pub stdout: Vec<String>,
    /// Lines for stderr, such as the log warnings of a clamped offset.
    pub notices: Vec<String>,
}

fn name((topic, partition): &Partition) -> String {
    format!("{topic}-{partition}")
}

/// A plan in the order of the `HashMap` that Kafka collects it into.
fn ordered(offsets: impl IntoIterator<Item = (Partition, i64)>) -> Plan {
    let offsets = offsets.into_iter().collect::<Vec<_>>();
    let capacity = default_capacity(offsets.len());
    hash_order(offsets, capacity, |((topic, partition), _)| {
        topic_partition_hash(topic, *partition)
    })
}

/// The offsets of a lookup, or the error of its first failed partition, as
/// `ListOffsetsResult.all()` fails.
fn known(
    answers: BTreeMap<Partition, PartitionOffset>,
) -> Result<BTreeMap<Partition, i64>, CommandError> {
    answers
        .into_iter()
        .map(|(partition, answer)| {
            answer
                .map(|offset| (partition, offset))
                .map_err(|code| KafkaException::for_code(code).to_java_string().into())
        })
        .collect()
}

/// The offset of each partition from `lookup`, or `missing(partition)` as the
/// error for the first partition the lookup does not know.
async fn required(
    lookup: &impl OffsetLookup,
    partitions: &[Partition],
    spec: OffsetSpec,
    missing: &str,
) -> Result<BTreeMap<Partition, i64>, CommandError> {
    let offsets = known(lookup.offsets(partitions, spec).await?)?;
    for partition in partitions {
        if !offsets.contains_key(partition) {
            return Err(format!("{missing}{}", name(partition)).into());
        }
    }
    Ok(offsets)
}

/// `checkOffsetsRange`: each requested offset clamped into the partition's
/// log, with a warning for each clamp.
///
/// Without a working lookup the requested offsets pass through unchecked,
/// with one warning that says so, and a negative request is refused, because
/// only the log start offset can say what it means.
async fn clamped(
    lookup: &impl OffsetLookup,
    requested: BTreeMap<Partition, i64>,
    planned: &mut Planned,
) -> Result<Plan, CommandError> {
    let partitions = requested.keys().cloned().collect::<Vec<_>>();
    let bounds = match lookup.offsets(&partitions, OffsetSpec::Earliest).await {
        Ok(starts) => Some((
            known(starts)?,
            known(lookup.offsets(&partitions, OffsetSpec::Latest).await?)?,
        )),
        Err(CommandError::Unsupported(reason)) => {
            if let Some((partition, offset)) = requested.iter().find(|(_, offset)| **offset < 0) {
                return Err(CommandError::Unsupported(format!(
                    "New offset ({offset}) for topic partition {} is negative, and {reason}",
                    name(partition)
                )));
            }
            planned.notices.push(format!(
                "WARN New offsets are not checked against the log start and end offsets: {reason}"
            ));
            None
        }
        Err(error) => return Err(error),
    };
    let Some((starts, ends)) = bounds else {
        return Ok(ordered(requested));
    };
    let mut plan = Vec::new();
    for (partition, offset) in requested {
        let end = ends.get(&partition).copied().ok_or_else(|| {
            format!(
                "Unexpected non-existing offset value for topic partition {}",
                name(&partition)
            )
        })?;
        let offset = match starts.get(&partition).copied() {
            _ if offset > end => {
                planned.notices.push(format!(
                    "WARN New offset ({offset}) is higher than latest offset for topic partition {}. Value will be set to {end}",
                    name(&partition)
                ));
                end
            }
            Some(start) if offset < start => {
                planned.notices.push(format!(
                    "WARN New offset ({offset}) is lower than earliest offset for topic partition {}. Value will be set to {start}",
                    name(&partition)
                ));
                start
            }
            _ => offset,
        };
        plan.push((partition, offset));
    }
    Ok(ordered(plan))
}

/// `getLogTimestampOffsets`: the first offset at or after `timestamp`, or the
/// log-end offset, with a warning, for a partition with no such record.
async fn by_timestamp(
    lookup: &impl OffsetLookup,
    partitions: &[Partition],
    timestamp: i64,
    planned: &mut Planned,
) -> Result<Plan, CommandError> {
    let found = required(
        lookup,
        partitions,
        OffsetSpec::Timestamp(timestamp),
        "Error getting offset by timestamp of topic partition: ",
    )
    .await?;
    let empty = partitions
        .iter()
        .filter(|partition| found.get(*partition) == Some(&-1))
        .cloned()
        .collect::<Vec<_>>();
    for (topic, partition) in &empty {
        planned.stdout.push(String::new());
        planned.stdout.push(format!(
            "Warn: Partition {partition} from topic {topic} is empty. Falling back to latest known offset."
        ));
    }
    let ends = if empty.is_empty() {
        BTreeMap::new()
    } else {
        required(
            lookup,
            &empty,
            OffsetSpec::Latest,
            "Error getting offset by timestamp of topic partition: ",
        )
        .await?
    };
    Ok(ordered(partitions.iter().map(|partition| {
        let offset = match found[partition] {
            -1 => ends[partition],
            offset => offset,
        };
        (partition.clone(), offset)
    })))
}

/// The plan of one group, as `prepareOffsetsToReset` computes it.
///
/// # Errors
/// Returns the error that the JVM tool ends with, such as a partition with no
/// committed offset under `--shift-by`, or the failure of the lookup.
pub async fn plan(
    scenario: &Scenario,
    group: &str,
    partitions: &[Partition],
    committed: &BTreeMap<Partition, i64>,
    lookup: &impl OffsetLookup,
) -> Result<Planned, CommandError> {
    let mut planned = Planned::default();
    planned.plan = match scenario {
        Scenario::ToOffset(offset) => {
            let requested = partitions
                .iter()
                .map(|partition| (partition.clone(), *offset))
                .collect();
            clamped(lookup, requested, &mut planned).await?
        }
        Scenario::ToEarliest => {
            let starts = required(
                lookup,
                partitions,
                OffsetSpec::Earliest,
                "Error getting starting offset of topic partition: ",
            )
            .await?;
            ordered(
                starts
                    .into_iter()
                    .filter(|(partition, _)| partitions.contains(partition)),
            )
        }
        Scenario::ToLatest => {
            let ends = required(
                lookup,
                partitions,
                OffsetSpec::Latest,
                "Error getting ending offset of topic partition: ",
            )
            .await?;
            ordered(
                ends.into_iter()
                    .filter(|(partition, _)| partitions.contains(partition)),
            )
        }
        Scenario::ShiftBy(shift) => {
            let mut requested = BTreeMap::new();
            for partition in partitions {
                let current = committed.get(partition).ok_or_else(|| {
                    format!(
                        "Cannot shift offset for partition {} since there is no current committed offset",
                        name(partition)
                    )
                })?;
                requested.insert(partition.clone(), current.saturating_add(*shift));
            }
            clamped(lookup, requested, &mut planned).await?
        }
        Scenario::ToDatetime(timestamp) | Scenario::ByDuration(timestamp) => {
            by_timestamp(lookup, partitions, *timestamp, &mut planned).await?
        }
        Scenario::FromFile(None) => {
            planned.stdout.push(String::new());
            planned
                .stdout
                .push(format!("Error: No reset plan for group {group} found"));
            Vec::new()
        }
        Scenario::FromFile(Some(requested)) => {
            clamped(lookup, requested.clone(), &mut planned).await?
        }
        Scenario::ToCurrent => {
            let (with, without): (Vec<_>, Vec<_>) = partitions
                .iter()
                .cloned()
                .partition(|partition| committed.contains_key(partition));
            let mut offsets = with
                .into_iter()
                .map(|partition| {
                    let offset = committed[&partition];
                    (partition, offset)
                })
                .collect::<Vec<_>>();
            if !without.is_empty() {
                let ends = required(
                    lookup,
                    &without,
                    OffsetSpec::Latest,
                    "Error getting ending offset of topic partition: ",
                )
                .await?;
                offsets.extend(
                    ends.into_iter()
                        .filter(|(partition, _)| without.contains(partition)),
                );
            }
            ordered(offsets)
        }
    };
    Ok(planned)
}

/// `String.split` as Java does it, dropping trailing empty strings.
fn java_split(value: &str, separator: char) -> Vec<&str> {
    let mut parts = value.split(separator).collect::<Vec<_>>();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    parts
}

/// One `--topic` of `--reset-offsets`: a topic, or `topic:0,1,2`.
///
/// # Errors
/// Returns Kafka's message for a malformed argument.
pub fn topic_arg(arg: &str) -> Result<(String, Option<Vec<i32>>), CommandError> {
    if !arg.contains(':') {
        return Ok((arg.to_owned(), None));
    }
    let parts = java_split(arg, ':');
    let [topic, partitions] = parts[..] else {
        return Err(
            format!("Invalid topic arg '{arg}', expected topic name and partitions").into(),
        );
    };
    let partitions = java_split(partitions, ',')
        .into_iter()
        .map(|partition| {
            partition.parse::<i32>().map_err(|_| {
                CommandError::from(format!(
                    "Invalid partition '{partition}' specified in topic arg '{arg}''"
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    Ok((topic.to_owned(), Some(partitions)))
}

/// Parses a `--from-file` plan, as `parseResetPlan` reads the CSV that
/// `--export` writes: `topic,partition,offset` for a single `--group`, else
/// `group,topic,partition,offset`.
///
/// # Errors
/// Returns an error naming the line that does not parse.
pub fn parse_reset_file(
    csv: &str,
    groups: &[String],
) -> Result<BTreeMap<String, BTreeMap<Partition, i64>>, CommandError> {
    let lines = java_split(csv, '\n');
    let record = |line: &str, fields: usize| -> Option<Vec<String>> {
        let values = line.split(',').map(str::to_owned).collect::<Vec<_>>();
        (values.len() == fields
            && values[fields - 2].parse::<i32>().is_ok()
            && values[fields - 1].parse::<i64>().is_ok())
        .then_some(values)
    };
    let without_group =
        groups.len() == 1 && lines.first().is_some_and(|line| record(line, 3).is_some());
    let mut plans = BTreeMap::<String, BTreeMap<Partition, i64>>::new();
    for line in lines {
        let (group, values) = if without_group {
            (groups[0].clone(), record(line, 3))
        } else {
            let values = record(line, 4);
            (
                values
                    .as_ref()
                    .map(|values| values[0].clone())
                    .unwrap_or_default(),
                values.map(|values| values[1..].to_vec()),
            )
        };
        let values =
            values.ok_or_else(|| format!("Unable to parse the reset plan line: {line}"))?;
        plans.entry(group).or_default().insert(
            (values[0].clone(), values[1].parse().unwrap_or_default()),
            values[2].parse().unwrap_or_default(),
        );
    }
    Ok(plans)
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Reads exactly `width` ASCII digits.
fn digits(text: &str, width: usize) -> Option<i64> {
    (text.len() == width && text.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// A zone offset in minutes: `Z`, `+hh`, `+hhmm` or `+hh:mm`.
fn zone_minutes(zone: &str) -> Option<i64> {
    if zone == "Z" {
        return Some(0);
    }
    let (sign, rest) = match zone.as_bytes().first()? {
        b'+' => (1, &zone[1..]),
        b'-' => (-1, &zone[1..]),
        _ => return None,
    };
    let rest = rest.replacen(':', "", 1);
    let (hours, minutes) = match rest.len() {
        2 => (digits(&rest, 2)?, 0),
        4 => (digits(&rest[..2], 2)?, digits(&rest[2..], 2)?),
        _ => return None,
    };
    (hours < 24 && minutes < 60).then_some(sign * (hours * 60 + minutes))
}

/// `Utils.getDateTime`: `yyyy-MM-ddTHH:mm:ss.SSS`, with an optional zone
/// that defaults to UTC, as Kafka epoch milliseconds.
///
/// # Errors
/// Returns the `ParseException` text that Kafka reports.
pub fn parse_datetime(value: &str) -> Result<i64, CommandError> {
    let Some((_, time)) = value.split_once('T') else {
        return Err("java.text.ParseException: Error parsing timestamp. It does not contain a 'T' according to ISO8601 format".into());
    };
    let value = if time.contains(['+', '-', 'Z']) {
        value.to_owned()
    } else {
        format!("{value}Z")
    };
    let unparseable = || {
        CommandError::from(format!(
            "java.text.ParseException: Unparseable date: \"{value}\""
        ))
    };
    let parse = || -> Option<i64> {
        let (date, time) = value.split_once('T')?;
        let mut date = date.split('-');
        let (year, month, day) = (
            digits(date.next()?, 4)?,
            digits(date.next()?, 2)?,
            digits(date.next()?, 2)?,
        );
        if date.next().is_some()
            || !(1..=12).contains(&month)
            || day < 1
            || day > days_in_month(year, month)
        {
            return None;
        }
        let zone_at = time.find(['+', '-', 'Z'])?;
        let (clock, zone) = time.split_at(zone_at);
        let (hms, millis) = clock.split_once('.')?;
        let mut hms = hms.split(':');
        let (hour, minute, second) = (
            digits(hms.next()?, 2)?,
            digits(hms.next()?, 2)?,
            digits(hms.next()?, 2)?,
        );
        let millis = (!millis.is_empty()
            && millis.len() <= 3
            && millis.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| millis.parse::<i64>().ok())
        .flatten()?;
        if hms.next().is_some() || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let local = ((days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60 + second;
        Some((local - zone_minutes(zone)? * 60) * 1000 + millis)
    };
    parse().ok_or_else(unparseable)
}

/// `java.time.Duration.parse`: `PnDTnHnMn.nS`, with optional signs, as
/// milliseconds.
///
/// # Errors
/// Returns Kafka's message for text that is not a duration.
pub fn parse_duration(value: &str) -> Result<i64, CommandError> {
    let invalid = || CommandError::from("Text cannot be parsed to a Duration");
    let upper = value.to_ascii_uppercase();
    let (negate, rest) = match upper.as_bytes().first() {
        Some(b'-') => (true, &upper[1..]),
        Some(b'+') => (false, &upper[1..]),
        _ => (false, upper.as_str()),
    };
    let rest = rest.strip_prefix('P').ok_or_else(invalid)?;
    let (date, time) = match rest.split_once('T') {
        Some((date, time)) if !time.is_empty() => (date, Some(time)),
        Some(_) => return Err(invalid()),
        None => (rest, None),
    };
    let number = |text: &str| -> Result<i128, CommandError> {
        let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        text.parse::<i128>().map_err(|_| invalid())
    };
    let mut millis = 0_i128;
    let mut any = false;
    if !date.is_empty() {
        let days = date.strip_suffix('D').ok_or_else(invalid)?;
        millis += number(days)? * 86_400_000;
        any = true;
    }
    if let Some(mut time) = time {
        for (unit, scale) in [('H', 3_600_000), ('M', 60_000)] {
            if let Some((amount, tail)) = time.split_once(unit) {
                millis += number(amount)? * scale;
                time = tail;
                any = true;
            }
        }
        if !time.is_empty() {
            let seconds = time.strip_suffix('S').ok_or_else(invalid)?;
            let (whole, fraction) = seconds.split_once(['.', ',']).unwrap_or((seconds, ""));
            if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            let whole_value = number(whole)?;
            let fraction_millis = format!("{fraction:0<3}")[..3].parse::<i128>().unwrap_or(0);
            let sign = if whole.starts_with('-') { -1 } else { 1 };
            millis += whole_value * 1000 + sign * fraction_millis;
            any = true;
        }
    }
    if !any {
        return Err(invalid());
    }
    let millis = if negate { -millis } else { millis };
    i64::try_from(millis).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Log offsets from fixed tables.
    struct Fixed {
        starts: BTreeMap<Partition, i64>,
        ends: BTreeMap<Partition, i64>,
        at: BTreeMap<Partition, i64>,
    }

    impl OffsetLookup for Fixed {
        fn offsets(
            &self,
            partitions: &[Partition],
            spec: OffsetSpec,
        ) -> impl Future<Output = Result<BTreeMap<Partition, PartitionOffset>, CommandError>>
        {
            let table = match spec {
                OffsetSpec::Earliest => &self.starts,
                OffsetSpec::Latest => &self.ends,
                _ => &self.at,
            };
            std::future::ready(Ok(partitions
                .iter()
                .filter_map(|partition| {
                    table
                        .get(partition)
                        .map(|offset| (partition.clone(), Ok(*offset)))
                })
                .collect()))
        }
    }

    fn p(topic: &str, partition: i32) -> Partition {
        (topic.to_owned(), partition)
    }

    fn fixed() -> Fixed {
        Fixed {
            starts: BTreeMap::from([(p("orders", 0), 2), (p("orders", 1), 0)]),
            ends: BTreeMap::from([(p("orders", 0), 10), (p("orders", 1), 0)]),
            at: BTreeMap::from([(p("orders", 0), 5), (p("orders", 1), -1)]),
        }
    }

    fn run(
        scenario: &Scenario,
        committed: &[(Partition, i64)],
        lookup: &impl OffsetLookup,
    ) -> Result<Planned, String> {
        let partitions = [p("orders", 0), p("orders", 1)];
        let committed = committed.iter().cloned().collect();
        futures_lite(plan(scenario, "g", &partitions, &committed, lookup))
            .map_err(|error| error.to_string())
    }

    fn futures_lite<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn every_scenario_plans_what_offsets_utils_plans() {
        struct Case {
            scenario: Scenario,
            committed: Vec<(Partition, i64)>,
            expected: Result<Planned, &'static str>,
        }
        let planned = |plan: Plan, stdout: Vec<&str>, notices: Vec<&str>| Planned {
            plan,
            stdout: stdout.into_iter().map(ToOwned::to_owned).collect(),
            notices: notices.into_iter().map(ToOwned::to_owned).collect(),
        };
        let cases = vec![
            Case {
                scenario: Scenario::ToOffset(3),
                committed: vec![],
                expected: Ok(planned(
                    vec![(p("orders", 0), 3), (p("orders", 1), 0)],
                    vec![],
                    vec![
                        "WARN New offset (3) is higher than latest offset for topic partition orders-1. Value will be set to 0",
                    ],
                )),
            },
            Case {
                scenario: Scenario::ToOffset(1),
                committed: vec![],
                expected: Ok(planned(
                    vec![(p("orders", 0), 2), (p("orders", 1), 0)],
                    vec![],
                    vec![
                        "WARN New offset (1) is lower than earliest offset for topic partition orders-0. Value will be set to 2",
                        "WARN New offset (1) is higher than latest offset for topic partition orders-1. Value will be set to 0",
                    ],
                )),
            },
            Case {
                scenario: Scenario::ToEarliest,
                committed: vec![],
                expected: Ok(planned(
                    vec![(p("orders", 0), 2), (p("orders", 1), 0)],
                    vec![],
                    vec![],
                )),
            },
            Case {
                scenario: Scenario::ToLatest,
                committed: vec![],
                expected: Ok(planned(
                    vec![(p("orders", 0), 10), (p("orders", 1), 0)],
                    vec![],
                    vec![],
                )),
            },
            Case {
                scenario: Scenario::ShiftBy(-3),
                committed: vec![(p("orders", 0), 9), (p("orders", 1), 0)],
                expected: Ok(planned(
                    vec![(p("orders", 0), 6), (p("orders", 1), 0)],
                    vec![],
                    vec![
                        "WARN New offset (-3) is lower than earliest offset for topic partition orders-1. Value will be set to 0",
                    ],
                )),
            },
            Case {
                scenario: Scenario::ShiftBy(1),
                committed: vec![(p("orders", 1), 0)],
                expected: Err(
                    "Cannot shift offset for partition orders-0 since there is no current committed offset",
                ),
            },
            Case {
                scenario: Scenario::ToDatetime(1_700_000_000_000),
                committed: vec![],
                expected: Ok(planned(
                    vec![(p("orders", 0), 5), (p("orders", 1), 0)],
                    vec![
                        "",
                        "Warn: Partition 1 from topic orders is empty. Falling back to latest known offset.",
                    ],
                    vec![],
                )),
            },
            Case {
                scenario: Scenario::ToCurrent,
                committed: vec![(p("orders", 0), 7)],
                expected: Ok(planned(
                    vec![(p("orders", 0), 7), (p("orders", 1), 0)],
                    vec![],
                    vec![],
                )),
            },
            Case {
                scenario: Scenario::FromFile(Some(BTreeMap::from([(p("orders", 0), 4)]))),
                committed: vec![],
                expected: Ok(planned(vec![(p("orders", 0), 4)], vec![], vec![])),
            },
            Case {
                scenario: Scenario::FromFile(None),
                committed: vec![],
                expected: Ok(planned(
                    vec![],
                    vec!["", "Error: No reset plan for group g found"],
                    vec![],
                )),
            },
        ];
        for case in cases {
            let actual = run(&case.scenario, &case.committed, &fixed());
            check!(
                actual == case.expected.map_err(ToOwned::to_owned),
                "{:?}",
                case.scenario
            );
        }
    }

    #[test]
    fn without_list_offsets_only_lookup_free_scenarios_proceed() {
        let unsupported = |what: &str| {
            format!(
                "{what} is not supported by this build: it needs AdminClient::list_offsets, which the pinned krabka-client-rs revision does not have"
            )
        };
        let unchecked = format!(
            "WARN New offsets are not checked against the log start and end offsets: {}",
            unsupported("reading log offsets (ListOffsets timestamp -2)")
        );
        let cases = [
            (
                Scenario::ToCurrent,
                vec![(p("orders", 0), 7), (p("orders", 1), 1)],
                Ok((vec![(p("orders", 0), 7), (p("orders", 1), 1)], vec![])),
            ),
            (
                Scenario::ToOffset(42),
                vec![],
                Ok((
                    vec![(p("orders", 0), 42), (p("orders", 1), 42)],
                    vec![unchecked.clone()],
                )),
            ),
            (
                Scenario::ShiftBy(2),
                vec![(p("orders", 0), 7), (p("orders", 1), 1)],
                Ok((
                    vec![(p("orders", 0), 9), (p("orders", 1), 3)],
                    vec![unchecked],
                )),
            ),
            (
                Scenario::ToOffset(-1),
                vec![],
                Err(format!(
                    "New offset (-1) for topic partition orders-0 is negative, and {}",
                    unsupported("reading log offsets (ListOffsets timestamp -2)")
                )),
            ),
            (
                Scenario::ToEarliest,
                vec![],
                Err(unsupported(
                    "reading log offsets (ListOffsets timestamp -2)",
                )),
            ),
            (
                Scenario::ToLatest,
                vec![],
                Err(unsupported(
                    "reading log offsets (ListOffsets timestamp -1)",
                )),
            ),
            (
                Scenario::ToCurrent,
                vec![(p("orders", 0), 7)],
                Err(unsupported(
                    "reading log offsets (ListOffsets timestamp -1)",
                )),
            ),
            (
                Scenario::ByDuration(5),
                vec![],
                Err(unsupported("reading log offsets (ListOffsets timestamp 5)")),
            ),
        ];
        for (scenario, committed, expected) in cases {
            let actual = run(&scenario, &committed, &crate::get_offsets::Unavailable)
                .map(|planned| (planned.plan, planned.notices));
            check!(actual == expected, "{scenario:?}");
        }
    }

    #[test]
    fn topic_args_parse_as_kafka_reads_them() {
        let cases = [
            ("orders", Ok(("orders".to_owned(), None))),
            ("orders:0,2", Ok(("orders".to_owned(), Some(vec![0, 2])))),
            ("orders:-1", Ok(("orders".to_owned(), Some(vec![-1])))),
            (
                "orders:x",
                Err("Invalid partition 'x' specified in topic arg 'orders:x''".to_owned()),
            ),
            (
                "orders:",
                Err("Invalid topic arg 'orders:', expected topic name and partitions".to_owned()),
            ),
            (
                "a:b:1",
                Err("Invalid topic arg 'a:b:1', expected topic name and partitions".to_owned()),
            ),
        ];
        for (arg, expected) in cases {
            check!(
                topic_arg(arg).map_err(|error| error.to_string()) == expected,
                "{arg}"
            );
        }
    }

    #[test]
    fn reset_files_read_both_csv_shapes() {
        let one = vec!["g1".to_owned()];
        let two = vec!["g1".to_owned(), "g2".to_owned()];
        let plan = |rows: &[(&str, &str, i32, i64)]| {
            let mut plans = BTreeMap::<String, BTreeMap<Partition, i64>>::new();
            for (group, topic, partition, offset) in rows {
                plans
                    .entry((*group).to_owned())
                    .or_default()
                    .insert(p(topic, *partition), *offset);
            }
            plans
        };
        let cases = [
            (
                "orders,0,3\norders,1,4\n\n",
                &one,
                Ok(plan(&[("g1", "orders", 0, 3), ("g1", "orders", 1, 4)])),
            ),
            (
                "g1,orders,0,3\ng2,orders,1,4\n",
                &two,
                Ok(plan(&[("g1", "orders", 0, 3), ("g2", "orders", 1, 4)])),
            ),
            ("g2,orders,1,4\n", &one, Ok(plan(&[("g2", "orders", 1, 4)]))),
            (
                "orders,0,x\n",
                &one,
                Err("Unable to parse the reset plan line: orders,0,x".to_owned()),
            ),
        ];
        for (csv, groups, expected) in cases {
            check!(
                parse_reset_file(csv, groups).map_err(|error| error.to_string()) == expected,
                "{csv:?}"
            );
        }
    }

    #[test]
    fn datetimes_parse_as_utils_get_date_time() {
        let cases = [
            ("2024-01-01T00:00:00.000", Ok(1_704_067_200_000)),
            ("2024-01-01T00:00:00.5", Ok(1_704_067_200_005)),
            ("2024-02-29T12:30:15.250Z", Ok(1_709_209_815_250)),
            ("2024-01-01T02:00:00.000+02:00", Ok(1_704_067_200_000)),
            ("2024-01-01T00:00:00.000-0130", Ok(1_704_072_600_000)),
            ("1969-12-31T23:59:59.999", Ok(-1)),
            (
                "2024-01-01",
                Err(
                    "java.text.ParseException: Error parsing timestamp. It does not contain a 'T' according to ISO8601 format",
                ),
            ),
            (
                "2024-01-01T00:00:00",
                Err("java.text.ParseException: Unparseable date: \"2024-01-01T00:00:00Z\""),
            ),
            (
                "2023-02-29T00:00:00.000",
                Err("java.text.ParseException: Unparseable date: \"2023-02-29T00:00:00.000Z\""),
            ),
        ];
        for (value, expected) in cases {
            check!(
                parse_datetime(value).map_err(|error| error.to_string())
                    == expected.map_err(ToOwned::to_owned),
                "{value}"
            );
        }
    }

    #[test]
    fn durations_parse_as_java_time_duration() {
        let invalid = Err("Text cannot be parsed to a Duration".to_owned());
        let cases = [
            ("PT1H", Ok(3_600_000)),
            ("P1DT2H3M4.5S", Ok(93_784_500)),
            ("pt15m", Ok(900_000)),
            ("-PT1S", Ok(-1000)),
            ("PT-1.5S", Ok(-1500)),
            ("P2D", Ok(172_800_000)),
            ("PT", invalid.clone()),
            ("P", invalid.clone()),
            ("XX", invalid.clone()),
            ("PT1X", invalid),
        ];
        for (value, expected) in cases {
            check!(
                parse_duration(value).map_err(|error| error.to_string()) == expected,
                "{value}"
            );
        }
    }
}
