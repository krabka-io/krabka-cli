//! The tables that `kafka-consumer-groups` prints, column for column.
//!
//! Each function takes the rows of one table and returns its text as the JVM
//! tool's `printf` formats lay it out. The JVM tool begins most tables with a
//! blank line and pads with `%-Ns`, so the text keeps those leading newlines
//! and the trailing spaces of padded last columns.

use std::fmt::Write as _;

/// The value of an empty column.
pub const MISSING: &str = "-";

/// `%-Ns`: `value` padded with spaces to at least `width` characters.
fn pad(value: &str, width: usize) -> String {
    format!("{value:<width$}")
}

/// One `--list --state`/`--type` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    pub group_id: String,
    pub group_type: String,
    pub state: String,
}

/// `--list` with `--state` or `--type`: `GROUP`, then `TYPE` and `STATE`
/// columns as asked for.
#[must_use]
pub fn list_table(groups: &[ListedGroup], include_type: bool, include_state: bool) -> String {
    let group_width = groups
        .iter()
        .map(|group| group.group_id.chars().count().max(15))
        .max()
        .unwrap_or(15)
        + 10;
    let line = |id: &str, kind: &str, state: &str| {
        let mut line = pad(id, group_width);
        if include_type {
            line.push(' ');
            line.push_str(&pad(kind, 20));
        }
        if include_state {
            line.push(' ');
            line.push_str(&pad(state, 20));
        }
        line.push('\n');
        line
    };
    let mut out = line("GROUP", "TYPE", "STATE");
    for group in groups {
        out.push_str(&line(&group.group_id, &group.group_type, &group.state));
    }
    out
}

/// One row of `--describe --offsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRow {
    pub group: String,
    pub topic: Option<String>,
    pub partition: Option<i32>,
    pub leader_epoch: Option<i32>,
    pub offset: Option<i64>,
    pub log_end_offset: Option<i64>,
    pub lag: Option<i64>,
    pub consumer_id: Option<String>,
    pub host: Option<String>,
    pub client_id: Option<String>,
}

fn or_missing<T: ToString>(value: Option<&T>) -> String {
    value.map_or_else(|| MISSING.to_owned(), ToString::to_string)
}

/// `CURRENT-OFFSET` less the log-end offset's lag: `log_end - offset`, or
/// nothing when either is unknown or the offset is `-1`.
#[must_use]
pub fn lag(offset: Option<i64>, log_end_offset: Option<i64>) -> Option<i64> {
    offset
        .filter(|offset| *offset != -1)
        .and_then(|offset| log_end_offset.map(|end| end - offset))
}

/// `--describe --offsets` for one group. `verbose` adds `LEADER-EPOCH`.
#[must_use]
pub fn offsets_table(rows: &[OffsetRow], verbose: bool) -> String {
    let width = |values: &mut dyn Iterator<Item = usize>| values.fold(15, usize::max);
    let group_width = width(&mut rows.iter().map(|row| row.group.chars().count()));
    let topic_width = width(
        &mut rows
            .iter()
            .map(|row| row.topic.as_deref().unwrap_or(MISSING).chars().count()),
    );
    let consumer_width = width(&mut rows.iter().map(|row| {
        row.consumer_id
            .as_deref()
            .unwrap_or(MISSING)
            .chars()
            .count()
    }));
    let host_width = width(
        &mut rows
            .iter()
            .map(|row| row.host.as_deref().unwrap_or(MISSING).chars().count()),
    );
    let line = |columns: [&str; 10]| {
        let [
            group,
            topic,
            partition,
            epoch,
            offset,
            end,
            lag,
            consumer,
            host,
            client,
        ] = columns;
        let mut line = format!(
            "\n{} {} {} ",
            pad(group, group_width),
            pad(topic, topic_width),
            pad(partition, 10)
        );
        if verbose {
            line.push_str(&pad(epoch, 15));
            line.push(' ');
        }
        let _ = write!(
            line,
            "{} {} {} {} {} {client}",
            pad(offset, 15),
            pad(end, 15),
            pad(lag, 15),
            pad(consumer, consumer_width),
            pad(host, host_width),
        );
        line
    };
    let mut out = line([
        "GROUP",
        "TOPIC",
        "PARTITION",
        "LEADER-EPOCH",
        "CURRENT-OFFSET",
        "LOG-END-OFFSET",
        "LAG",
        "CONSUMER-ID",
        "HOST",
        "CLIENT-ID",
    ]);
    for row in rows {
        out.push_str(&line([
            &row.group,
            row.topic.as_deref().unwrap_or(MISSING),
            &or_missing(row.partition.as_ref()),
            &or_missing(row.leader_epoch.as_ref()),
            &or_missing(row.offset.as_ref()),
            &or_missing(row.log_end_offset.as_ref()),
            &or_missing(row.lag.as_ref()),
            row.consumer_id.as_deref().unwrap_or(MISSING),
            row.host.as_deref().unwrap_or(MISSING),
            row.client_id.as_deref().unwrap_or(MISSING),
        ]));
    }
    out.push('\n');
    out
}

/// One row of `--describe --members`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRow {
    pub group: String,
    pub consumer_id: String,
    pub group_instance_id: String,
    pub host: String,
    pub client_id: String,
    pub assignment: Vec<(String, i32)>,
    pub target_assignment: Vec<(String, i32)>,
    pub current_epoch: Option<i32>,
    pub target_epoch: Option<i32>,
    pub upgraded: Option<bool>,
}

/// `topic:0,1;other:2`: the partitions of each topic sorted, the topics
/// sorted, as `getAssignmentString` writes them.
#[must_use]
pub fn assignment_string(assignment: &[(String, i32)]) -> String {
    let mut topics = std::collections::BTreeMap::<&str, Vec<i32>>::new();
    for (topic, partition) in assignment {
        topics.entry(topic).or_default().push(*partition);
    }
    let mut entries = topics
        .into_iter()
        .map(|(topic, mut partitions)| {
            partitions.sort_unstable();
            let partitions = partitions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("{topic}:{partitions}")
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries.join(";")
}

fn assignment_or_missing(assignment: &[(String, i32)]) -> String {
    if assignment.is_empty() {
        MISSING.to_owned()
    } else {
        assignment_string(assignment)
    }
}

/// `--describe --members` for one group. `verbose` adds the epoch and
/// assignment columns, and `UPGRADED` when the group mixes classic and
/// consumer-protocol members.
#[must_use]
pub fn members_table(rows: &[MemberRow], verbose: bool) -> String {
    let mut group_width = 15;
    let mut consumer_width = 15;
    let mut instance_width = 17;
    let mut host_width = 15;
    let mut client_width = 15;
    let mut current_width = 20;
    let mut target_width = 20;
    let mut include_instance = false;
    let mut has_classic = false;
    let mut has_consumer = false;
    for row in rows {
        group_width = group_width.max(row.group.chars().count());
        consumer_width = consumer_width.max(row.consumer_id.chars().count());
        instance_width = instance_width.max(row.group_instance_id.chars().count());
        host_width = host_width.max(row.host.chars().count());
        client_width = client_width.max(row.client_id.chars().count());
        include_instance |= !row.group_instance_id.is_empty();
        current_width = current_width.max(assignment_or_missing(&row.assignment).chars().count());
        target_width = target_width.max(
            assignment_or_missing(&row.target_assignment)
                .chars()
                .count(),
        );
        has_classic |= row.upgraded == Some(false);
        has_consumer |= row.upgraded == Some(true);
    }
    let migrating = has_classic && has_consumer;
    let identity =
        |group: &str, consumer: &str, instance: &str, host: &str, client: &str, count: &str| {
            let mut line = format!(
                "{} {} ",
                pad(group, group_width),
                pad(consumer, consumer_width)
            );
            if include_instance {
                line.push_str(&pad(instance, instance_width));
                line.push(' ');
            }
            let _ = write!(
                line,
                "{} {} {} ",
                pad(host, host_width),
                pad(client, client_width),
                pad(count, 15)
            );
            line
        };
    let detail =
        |current_epoch: &str, current: &str, target_epoch: &str, target: &str, upgraded: &str| {
            let mut line = format!(
                "{} {} {} {}",
                pad(current_epoch, 15),
                pad(current, current_width),
                pad(target_epoch, 15),
                pad(target, target_width)
            );
            if migrating {
                line.push(' ');
                line.push_str(upgraded);
            }
            line
        };
    let mut out = String::from("\n");
    out.push_str(&identity(
        "GROUP",
        "CONSUMER-ID",
        "GROUP-INSTANCE-ID",
        "HOST",
        "CLIENT-ID",
        "#PARTITIONS",
    ));
    if verbose {
        out.push_str(&detail(
            "CURRENT-EPOCH",
            "CURRENT-ASSIGNMENT",
            "TARGET-EPOCH",
            "TARGET-ASSIGNMENT",
            "UPGRADED",
        ));
    }
    out.push('\n');
    for row in rows {
        out.push_str(&identity(
            &row.group,
            &row.consumer_id,
            &row.group_instance_id,
            &row.host,
            &row.client_id,
            &row.assignment.len().to_string(),
        ));
        if verbose {
            out.push_str(&detail(
                &or_missing(row.current_epoch.as_ref()),
                &assignment_or_missing(&row.assignment),
                &or_missing(row.target_epoch.as_ref()),
                &assignment_or_missing(&row.target_assignment),
                &or_missing(row.upgraded.as_ref()),
            ));
        }
        out.push('\n');
    }
    out
}

/// The row of `--describe --state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRow {
    pub group: String,
    /// `host:port  (id)`.
    pub coordinator: String,
    pub assignment_strategy: String,
    pub state: String,
    pub members: usize,
    pub group_epoch: Option<i32>,
    pub target_assignment_epoch: Option<i32>,
}

/// `--describe --state` for one group. `verbose` adds `GROUP-EPOCH` and
/// `TARGET-ASSIGNMENT-EPOCH`.
#[must_use]
pub fn state_table(row: &StateRow, verbose: bool) -> String {
    let coordinator_width = row.coordinator.chars().count().max(25);
    let group_width = row.group.chars().count().max(15);
    let strategy = if row.assignment_strategy.is_empty() {
        MISSING
    } else {
        &row.assignment_strategy
    };
    let line = |group: &str,
                coordinator: &str,
                strategy: &str,
                state: &str,
                epoch: &str,
                target: &str,
                members: &str| {
        let mut line = format!(
            "\n{} {} {} {} ",
            pad(group, group_width),
            pad(coordinator, coordinator_width),
            pad(strategy, 20),
            pad(state, 20)
        );
        if verbose {
            let _ = write!(line, "{} {} ", pad(epoch, 15), pad(target, 25));
        }
        line.push_str(members);
        line
    };
    let mut out = line(
        "GROUP",
        "COORDINATOR (ID)",
        "ASSIGNMENT-STRATEGY",
        "STATE",
        "GROUP-EPOCH",
        "TARGET-ASSIGNMENT-EPOCH",
        "#MEMBERS",
    );
    out.push_str(&line(
        &row.group,
        &row.coordinator,
        strategy,
        &row.state,
        &or_missing(row.group_epoch.as_ref()),
        &or_missing(row.target_assignment_epoch.as_ref()),
        &row.members.to_string(),
    ));
    out.push('\n');
    out
}

/// A group and the new offset of each of its partitions.
pub type GroupPlan = (String, Vec<((String, i32), i64)>);

/// `--reset-offsets`: the plan of each group, `GROUP TOPIC PARTITION
/// NEW-OFFSET`. With no group at all the JVM tool prints only a newline.
#[must_use]
pub fn reset_table(plans: &[GroupPlan]) -> String {
    let group_width = plans
        .iter()
        .map(|(group, _)| group.chars().count())
        .fold(15, usize::max);
    let topic_width = plans
        .iter()
        .flat_map(|(_, plan)| plan.iter().map(|((topic, _), _)| topic.chars().count()))
        .fold(15, usize::max);
    let line = |group: &str, topic: &str, partition: &str, offset: &str| {
        format!(
            "\n{} {} {} {offset}",
            pad(group, group_width),
            pad(topic, topic_width),
            pad(partition, 10)
        )
    };
    let mut out = String::new();
    if !plans.is_empty() {
        out.push_str(&line("GROUP", "TOPIC", "PARTITION", "NEW-OFFSET"));
    }
    for (group, plan) in plans {
        for ((topic, partition), offset) in plan {
            out.push_str(&line(
                group,
                topic,
                &partition.to_string(),
                &offset.to_string(),
            ));
        }
    }
    out.push('\n');
    out
}

/// `--reset-offsets --export`: one CSV record per partition, with the group
/// column only when more than one `--group` was given, then the newline that
/// `println` adds.
#[must_use]
pub fn export_csv(plans: &[GroupPlan], single_group: bool) -> String {
    let mut out = String::new();
    for (group, plan) in plans {
        for ((topic, partition), offset) in plan {
            if single_group {
                let _ = writeln!(out, "{},{partition},{offset}", csv_field(topic));
            } else {
                let _ = writeln!(
                    out,
                    "{},{},{partition},{offset}",
                    csv_field(group),
                    csv_field(topic)
                );
            }
        }
    }
    out.push('\n');
    out
}

/// A CSV field as Jackson's `CsvMapper` writes it: quoted, with quotes
/// doubled, when it holds a separator, a quote or a line break.
fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// One `--delete-offsets` row: a partition, or `None` for a topic that could
/// not be described, and its error text.
pub type DeleteOffsetRow = ((String, Option<i32>), Option<String>);

/// `--delete-offsets`: `TOPIC PARTITION STATUS`, one row per partition sorted
/// by topic name and partition text, as the JVM tool sorts them.
#[must_use]
pub fn delete_offsets_table(rows: &[DeleteOffsetRow]) -> String {
    let topic_width = rows
        .iter()
        .map(|((topic, _), _)| topic.chars().count())
        .fold(15, usize::max);
    let mut sorted = rows.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|((topic, partition), _)| format!("{topic}{}", partition.unwrap_or(-1)));
    let line = |topic: &str, partition: &str, status: &str| {
        format!(
            "\n{} {} {}",
            pad(topic, topic_width),
            pad(partition, 10),
            pad(status, 15)
        )
    };
    let mut out = line("TOPIC", "PARTITION", "STATUS");
    for ((topic, partition), error) in sorted {
        let partition =
            partition.map_or_else(|| MISSING.to_owned(), |partition| partition.to_string());
        let status = error.as_ref().map_or_else(
            || "Successful".to_owned(),
            |error| format!("Error: {error}"),
        );
        out.push_str(&line(topic, &partition, &status));
    }
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn offset_row(
        group: &str,
        topic: &str,
        partition: i32,
        offset: Option<i64>,
        end: Option<i64>,
    ) -> OffsetRow {
        OffsetRow {
            group: group.into(),
            topic: Some(topic.into()),
            partition: Some(partition),
            leader_epoch: None,
            offset,
            log_end_offset: end,
            lag: lag(offset, end),
            consumer_id: None,
            host: None,
            client_id: None,
        }
    }

    #[test]
    fn lag_is_log_end_less_committed_and_unknown_without_either() {
        let cases = [
            (Some(3), Some(10), Some(7)),
            (Some(10), Some(10), Some(0)),
            (Some(12), Some(10), Some(-2)),
            (None, Some(10), None),
            (Some(-1), Some(10), None),
            (Some(3), None, None),
            (None, None, None),
        ];
        for (offset, end, expected) in cases {
            check!(lag(offset, end) == expected, "{offset:?} {end:?}");
        }
    }

    // `kafka-consumer-groups --describe --group g2` on Kafka 4.3.1.
    #[test]
    fn the_offsets_table_matches_kafka() {
        let rows = [
            offset_row(
                "g2",
                "a-very-long-topic-name-over-24-chars",
                0,
                Some(0),
                Some(0),
            ),
            offset_row("g2", "orders", 0, Some(10), Some(10)),
            offset_row("g2", "orders", 1, None, Some(4)),
        ];
        check!(
            offsets_table(&rows, false)
                == concat!(
                    "\nGROUP           TOPIC                                PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID",
                    "\ng2              a-very-long-topic-name-over-24-chars 0          0               0               0               -               -               -",
                    "\ng2              orders                               0          10              10              0               -               -               -",
                    "\ng2              orders                               1          -               4               -               -               -               -",
                    "\n",
                )
        );
        let active = OffsetRow {
            consumer_id: Some("cli-a-2ddd1d95-0ed6-47aa-8864-681ee912ad2c".into()),
            host: Some("/127.0.0.1".into()),
            client_id: Some("cli-a".into()),
            ..offset_row("active1", "events", 0, Some(0), Some(0))
        };
        check!(
            offsets_table(&[active], true)
                == concat!(
                    "\nGROUP           TOPIC           PARTITION  LEADER-EPOCH    CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                HOST            CLIENT-ID",
                    "\nactive1         events          0          -               0               0               0               cli-a-2ddd1d95-0ed6-47aa-8864-681ee912ad2c /127.0.0.1      cli-a",
                    "\n",
                )
        );
    }

    #[test]
    fn the_list_tables_match_kafka() {
        let groups = [
            ListedGroup {
                group_id: "g1".into(),
                group_type: "Classic".into(),
                state: "Empty".into(),
            },
            ListedGroup {
                group_id: "g2".into(),
                group_type: "Consumer".into(),
                state: "Stable".into(),
            },
        ];
        let cases = [
            (
                false,
                true,
                "GROUP                     STATE               \ng1                        Empty               \ng2                        Stable              \n",
            ),
            (
                true,
                false,
                "GROUP                     TYPE                \ng1                        Classic             \ng2                        Consumer            \n",
            ),
            (
                true,
                true,
                "GROUP                     TYPE                 STATE               \ng1                        Classic              Empty               \ng2                        Consumer             Stable              \n",
            ),
        ];
        for (include_type, include_state, expected) in cases {
            check!(list_table(&groups, include_type, include_state) == expected);
        }
        check!(list_table(&[], false, true) == "GROUP                     STATE               \n");
    }

    #[test]
    fn the_members_tables_match_kafka() {
        let member = MemberRow {
            group: "active1".into(),
            consumer_id: "cli-a-2ddd1d95-0ed6-47aa-8864-681ee912ad2c".into(),
            group_instance_id: String::new(),
            host: "/127.0.0.1".into(),
            client_id: "cli-a".into(),
            assignment: (0..20)
                .map(|partition| ("events".to_owned(), partition))
                .collect(),
            target_assignment: vec![],
            current_epoch: None,
            target_epoch: None,
            upgraded: None,
        };
        check!(
            members_table(std::slice::from_ref(&member), false)
                == concat!(
                    "\nGROUP           CONSUMER-ID                                HOST            CLIENT-ID       #PARTITIONS     \n",
                    "active1         cli-a-2ddd1d95-0ed6-47aa-8864-681ee912ad2c /127.0.0.1      cli-a           20              \n",
                )
        );
        check!(
            members_table(&[member], true)
                == concat!(
                    "\nGROUP           CONSUMER-ID                                HOST            CLIENT-ID       #PARTITIONS     CURRENT-EPOCH   CURRENT-ASSIGNMENT                                       TARGET-EPOCH    TARGET-ASSIGNMENT   \n",
                    "active1         cli-a-2ddd1d95-0ed6-47aa-8864-681ee912ad2c /127.0.0.1      cli-a           20              -               events:0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 -               -                   \n",
                )
        );
        check!(
            members_table(&[], false)
                == "\nGROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     \n"
        );
    }

    #[test]
    fn assignments_group_by_topic_and_sort() {
        let assignment = [
            ("b".to_owned(), 2),
            ("a".to_owned(), 10),
            ("b".to_owned(), 0),
            ("a".to_owned(), 9),
        ];
        check!(assignment_string(&assignment) == "a:9,10;b:0,2");
    }

    #[test]
    fn the_state_tables_match_kafka() {
        let row = StateRow {
            group: "active1".into(),
            coordinator: "localhost:9092  (1)".into(),
            assignment_strategy: "range".into(),
            state: "Stable".into(),
            members: 1,
            group_epoch: None,
            target_assignment_epoch: None,
        };
        check!(
            state_table(&row, false)
                == "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS\nactive1         localhost:9092  (1)       range                Stable               1\n"
        );
        check!(
            state_table(&row, true)
                == "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                GROUP-EPOCH     TARGET-ASSIGNMENT-EPOCH   #MEMBERS\nactive1         localhost:9092  (1)       range                Stable               -               -                         1\n"
        );
        let empty = StateRow {
            group: "g1".into(),
            assignment_strategy: String::new(),
            state: "Empty".into(),
            members: 0,
            ..row
        };
        check!(
            state_table(&empty, false)
                == "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS\ng1              localhost:9092  (1)       -                    Empty                0\n"
        );
    }

    #[test]
    fn the_reset_table_and_export_match_kafka() {
        let plans = vec![
            (
                "g1".to_owned(),
                vec![(("orders".to_owned(), 0), 0), (("orders".to_owned(), 1), 0)],
            ),
            ("g2".to_owned(), vec![(("orders".to_owned(), 0), 0)]),
        ];
        check!(
            reset_table(&plans)
                == "\nGROUP           TOPIC           PARTITION  NEW-OFFSET\ng1              orders          0          0\ng1              orders          1          0\ng2              orders          0          0\n"
        );
        check!(
            reset_table(&[("active1".to_owned(), vec![])])
                == "\nGROUP           TOPIC           PARTITION  NEW-OFFSET\n"
        );
        check!(reset_table(&[]) == "\n");
        check!(export_csv(&plans[..1], true) == "orders,0,0\norders,1,0\n\n");
        check!(export_csv(&plans, false) == "g1,orders,0,0\ng1,orders,1,0\ng2,orders,0,0\n\n");
        check!(
            export_csv(
                &[("g,1".to_owned(), vec![(("a\"b".to_owned(), 0), 1)])],
                false
            ) == "\"g,1\",\"a\"\"b\",0,1\n\n"
        );
    }

    #[test]
    fn the_delete_offsets_table_matches_kafka() {
        let rows = vec![
            (("orders".to_owned(), Some(1)), None),
            (
                ("nosuch".to_owned(), None),
                Some("org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.".to_owned()),
            ),
        ];
        check!(
            delete_offsets_table(&rows)
                == concat!(
                    "\nTOPIC           PARTITION  STATUS         ",
                    "\nnosuch          -          Error: org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.",
                    "\norders          1          Successful     ",
                    "\n",
                )
        );
    }
}
