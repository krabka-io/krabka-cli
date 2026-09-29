//! A Kafka oracle for conformance tests: one Kafka broker in a container,
//! and two runners pointed at it, the JVM tool inside the container and
//! `krabka` on the host.
//!
//! # Topology
//!
//! Both tools talk to the same Kafka broker. A row that agrees therefore
//! proves that `krabka` accepts the same command line, sends requests that
//! Kafka answers the same way, and renders the answer as the JVM tool does:
//! wire behaviour and output behaviour of the CLI. It proves nothing about
//! the krabka broker. A lane that points both tools at a krabka broker, which
//! would prove product behaviour, is a separate lane.
//!
//! # Oracle
//!
//! The broker is `apache/kafka` 4.3.1, the upstream image of that release,
//! pinned by digest in [`IMAGE`]. Every expectation in the suites that use
//! this module was captured against it. The broker runs as a single `KRaft`
//! node, broker and controller in one process, with the `StandardAuthorizer`
//! on and `User:ANONYMOUS` as a super user, so ACL rows reach a real
//! authorizer while every other row runs unrestricted. It advertises
//! `127.0.0.1:<port>` on a port that is mapped to the same port on the host,
//! so the tool inside the container and `krabka` on the host reach the same
//! listener at the same address.
//!
//! A work directory is mounted into the container at its host path, and the
//! JVM tools run as the host user. A file that one tool writes is then a
//! file that the harness reads at the same path, and a directory that
//! `kafka-storage format` writes is one that the harness can delete.
//!
//! Under Bazel, `KRABKA_ORACLE_IMAGE` names the tag that `//bazel/images`
//! loaded from its digest-pinned tarball, and the container starts with
//! `--pull=never`, so nothing is fetched while the test runs.
//!
//! # Verdicts
//!
//! One run of a row produces a [`View`] per tool with four comparison
//! categories: the flag surface (whether the tool accepted the command line),
//! the exit code, the stdout shape with the stderr message, and the resolved
//! state, which is what the cluster or the disk holds afterwards as a neutral
//! reader sees it. [`judge`] compares the two views layer by layer. A row
//! whose views differ passes only when the suite declares a difference that
//! covers every differing [`Layer`]. A declared difference that no longer
//! shows fails as stale, so the list cannot hide a closed gap.

use std::{
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::MetadataExt,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// The oracle image: `apache/kafka:4.3.1`, pinned by the digest of its
/// multi-platform index.
pub const IMAGE: &str =
    "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837";

/// The Kafka release that [`IMAGE`] carries.
pub const KAFKA_VERSION: &str = "4.3.1";

/// What one process produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capture {
    pub exit: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// How long one process may run before it is killed. A row that waits for
/// records that never come would otherwise hang the suite; a killed process
/// has no exit code, which fails the row.
const PROCESS_DEADLINE: Duration = Duration::from_mins(2);

fn capture(command: &mut Command, stdin: &str) -> Capture {
    let mut child = command
        .stdin(if stdin.is_empty() {
            Stdio::null()
        } else {
            Stdio::piped()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("run {command:?}: {error}"));
    if let Some(mut pipe) = child.stdin.take() {
        pipe.write_all(stdin.as_bytes())
            .unwrap_or_else(|error| panic!("write the stdin of {command:?}: {error}"));
    }
    let drain = |pipe: Option<Box<dyn Read + Send>>| {
        thread::spawn(move || {
            let mut bytes = Vec::new();
            if let Some(mut pipe) = pipe {
                let _ = pipe.read_to_end(&mut bytes);
            }
            String::from_utf8_lossy(&bytes).into_owned()
        })
    };
    let stdout = drain(
        child
            .stdout
            .take()
            .map(|pipe| Box::new(pipe) as Box<dyn Read + Send>),
    );
    let stderr = drain(
        child
            .stderr
            .take()
            .map(|pipe| Box::new(pipe) as Box<dyn Read + Send>),
    );
    let deadline = Instant::now() + PROCESS_DEADLINE;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            Ok(None) => {
                let _ = child.kill();
                break child
                    .wait()
                    .unwrap_or_else(|error| panic!("reap {command:?}: {error}"));
            }
            Err(error) => panic!("wait for {command:?}: {error}"),
        }
    };
    Capture {
        exit: status.code(),
        stdout: stdout.join().expect("the stdout reader"),
        stderr: stderr.join().expect("the stderr reader"),
    }
}

/// A running oracle broker. Dropping it removes the container, and then the
/// work directory.
pub struct Oracle {
    container: String,
    port: u16,
    user: String,
    work: tempfile::TempDir,
}

impl Oracle {
    /// Starts the broker and waits until it answers the JVM tool.
    ///
    /// # Panics
    /// Panics when Docker cannot start the container, or the broker does
    /// not answer within two minutes.
    pub fn start() -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("reserve a port")
            .port();
        let work = tempfile::tempdir().expect("create the work directory");
        let owner = std::fs::metadata(work.path()).expect("stat the work directory");
        let user = format!("{}:{}", owner.uid(), owner.gid());
        let container = format!("krabka-oracle-{}-{port}", std::process::id());
        let env = [
            "KAFKA_NODE_ID=1".to_owned(),
            "KAFKA_PROCESS_ROLES=broker,controller".to_owned(),
            format!("KAFKA_LISTENERS=PLAINTEXT://:{port},CONTROLLER://:9093"),
            format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}"),
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER".to_owned(),
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                .to_owned(),
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093".to_owned(),
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1".to_owned(),
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1".to_owned(),
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1".to_owned(),
            "KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
                .to_owned(),
            "KAFKA_SUPER_USERS=User:ANONYMOUS".to_owned(),
        ];
        let mount = work.path().display().to_string();
        let mut run = Command::new("docker");
        run.args(["run", "-d", "--rm", "--name", &container]);
        run.args(["-p", &format!("127.0.0.1:{port}:{port}")]);
        run.args(["-v", &format!("{mount}:{mount}")]);
        for variable in &env {
            run.args(["-e", variable]);
        }
        match std::env::var("KRABKA_ORACLE_IMAGE") {
            Ok(image) => run.args(["--pull=never", &image]),
            Err(_) => run.arg(IMAGE),
        };
        let started = capture(&mut run, "");
        assert!(started.exit == Some(0), "docker run: {started:?}");
        let oracle = Self {
            container,
            port,
            user,
            work,
        };
        let deadline = Instant::now() + Duration::from_mins(2);
        loop {
            let probe = ["--list".to_owned()];
            if oracle.jvm("kafka-topics.sh", &probe, true, "").exit == Some(0) {
                return oracle;
            }
            assert!(Instant::now() < deadline, "the oracle broker did not start");
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// The broker's address, as both tools reach it.
    pub fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// The directory that both tools see at the same path.
    pub fn work(&self) -> &Path {
        self.work.path()
    }

    /// Runs a JVM tool script from `/opt/kafka/bin` inside the container, as
    /// the host user, with `--bootstrap-server` first when `bootstrap` is set
    /// and `stdin` on its standard input.
    pub fn jvm(&self, script: &str, args: &[String], bootstrap: bool, stdin: &str) -> Capture {
        let mut command = Command::new("docker");
        command.args(["exec", "-u", &self.user]);
        if !stdin.is_empty() {
            command.arg("-i");
        }
        command.args([&self.container, &format!("/opt/kafka/bin/{script}")]);
        if bootstrap {
            command.args(["--bootstrap-server", &self.bootstrap()]);
        }
        capture(command.args(args), stdin)
    }

    /// Runs `krabka <subcommand>` on the host, with `--bootstrap-server`
    /// after the subcommand when `bootstrap` is set and `stdin` on its
    /// standard input. The environment holds no `KRABKA_*` variable but those
    /// in `env`.
    pub fn krabka(
        &self,
        subcommand: &[&str],
        args: &[String],
        bootstrap: bool,
        env: &[(&str, &str)],
        stdin: &str,
    ) -> Capture {
        let mut command = Command::new(env!("CARGO_BIN_EXE_krabka"));
        for (key, _) in std::env::vars() {
            if key.starts_with("KRABKA_") {
                command.env_remove(key);
            }
        }
        command.env("RUST_LOG", "off").envs(env.iter().copied());
        command.args(subcommand);
        if bootstrap {
            command.args(["--bootstrap-server", &self.bootstrap()]);
        }
        capture(command.args(args), stdin)
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// What the JVM tool does with a row's command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The JVM tool exits 0. Exit code, stdout and stderr must agree.
    Accepted,
    /// The JVM tool refuses it with exit 1. Exit code and message must agree.
    Rejected,
}

impl Expect {
    /// The exit code that the JVM tool returns for the row.
    pub const fn exit(self) -> i32 {
        match self {
            Self::Accepted => 0,
            Self::Rejected => 1,
        }
    }
}

/// One part of a [`View`] that can differ between the two tools.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Layer {
    /// One tool accepts the command line and the other refuses it.
    Surface,
    /// Both refuse it, with different exit codes.
    Exit,
    /// What the tools print on stdout.
    Stdout,
    /// The stderr message.
    Stderr,
    /// The stderr message holds the same characters in another order: the
    /// JVM tool prints a `Set.of`, whose order changes between JVM runs.
    UnorderedStderr,
    /// What the cluster or the disk holds afterwards.
    Resolved,
}

/// The layers of a different outcome: exit code and output.
pub const OUTCOME: &[Layer] = &[Layer::Surface, Layer::Exit, Layer::Stdout, Layer::Stderr];

/// The layers of a different outcome that also leaves a different state.
pub const OUTCOME_AND_STATE: &[Layer] = &[
    Layer::Surface,
    Layer::Exit,
    Layer::Stdout,
    Layer::Stderr,
    Layer::Resolved,
];

/// Why a declared difference exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// krabka differs on purpose.
    Intended,
    /// krabka differs from Kafka and should not. The row stays in the matrix,
    /// so the day the defect is fixed the declaration goes stale and fails.
    Defect,
}

/// A declared difference between the two tools on one row, as a suite
/// writes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Difference {
    /// The layers that may differ. Every other layer must agree.
    pub layers: &'static [Layer],
    pub kind: Kind,
    pub reason: &'static str,
}

impl Difference {
    /// An intended difference in `layers`.
    pub const fn intended(layers: &'static [Layer], reason: &'static str) -> Self {
        Self {
            layers,
            kind: Kind::Intended,
            reason,
        }
    }

    /// A defect in `layers`.
    pub const fn defect(layers: &'static [Layer], reason: &'static str) -> Self {
        Self {
            layers,
            kind: Kind::Defect,
            reason,
        }
    }
}

/// Everything declared for one row, merged from its declarations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declared {
    pub layers: Vec<Layer>,
    pub kind: Kind,
    pub reasons: Vec<&'static str>,
}

impl Declared {
    /// Merges `differences`, or `None` when there are none. The merge is a
    /// defect when any of them is.
    pub fn merge(differences: impl IntoIterator<Item = Difference>) -> Option<Self> {
        differences.into_iter().fold(None, |merged, difference| {
            let mut merged = merged.unwrap_or(Self {
                layers: Vec::new(),
                kind: Kind::Intended,
                reasons: Vec::new(),
            });
            merged.layers.extend_from_slice(difference.layers);
            merged.layers.sort_unstable();
            merged.layers.dedup();
            if difference.kind == Kind::Defect {
                merged.kind = Kind::Defect;
            }
            merged.reasons.push(difference.reason);
            Some(merged)
        })
    }
}

/// Whether a tool accepted the command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Accepted,
    Refused,
}

/// The part of a run that a row compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub surface: Surface,
    pub exit: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub resolved: String,
}

/// Which tool produced a capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    Jvm,
    Krabka,
}

/// The prefix of a failure that `TopicCommand` prints on stdout.
const JVM_FAILURE: &str = "Error while executing topic command : ";

/// The prefix of an exception that the JVM does not catch.
const UNCAUGHT: &str = "Exception in thread \"main\" ";

/// The prefix with which a JVM tool introduces its failure message, as
/// `GetOffsetShell` does. krabka's output layer introduces the same message
/// with `krabka <command>: ` instead, so both prefixes are envelope.
const JVM_ERROR: &str = "Error occurred: ";

/// `line` without a leading `<package>.<Class>: `, the form in which the JVM
/// prints an exception.
fn strip_exception(line: &str) -> &str {
    line.split_once(": ")
        .filter(|(class, _)| {
            class.contains('.')
                && !class.contains(' ')
                && (class.ends_with("Exception") || class.ends_with("Error"))
        })
        .map_or(line, |(_, message)| message)
}

/// Whether `line` is a log4j line, `[2026-01-01 00:00:00,000] LEVEL ...`,
/// which carries a timestamp and is not the tool's own message.
fn is_log_line(line: &str) -> bool {
    line.strip_prefix('[')
        .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
}

/// The message of a refusal: the line after [`JVM_FAILURE`] on stdout, as
/// both tools print a broker's per-topic failure, or the first line of
/// stderr that is not a log line. The `krabka <command>: ` prefix of krabka's
/// output layer and the uncaught-exception prefix of the JVM are removed.
fn message(capture: &Capture, tool: Tool, command: &str) -> String {
    if let Some(line) = capture
        .stdout
        .lines()
        .find_map(|line| line.strip_prefix(JVM_FAILURE))
    {
        return line.to_owned();
    }
    let first = capture
        .stderr
        .lines()
        .find(|line| !is_log_line(line))
        .unwrap_or_default();
    match tool {
        Tool::Jvm => {
            let first = first.strip_prefix(UNCAUGHT).unwrap_or(first);
            strip_exception(first.strip_prefix(JVM_ERROR).unwrap_or(first)).to_owned()
        }
        Tool::Krabka => first
            .strip_prefix(&format!("krabka {command}: "))
            .unwrap_or(first)
            .to_owned(),
    }
}

/// A rewrite of volatile output, applied to both tools alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scrub {
    /// The word after `label` on each line that holds it, such as a high
    /// watermark or a feature epoch that moves between the two runs.
    Field(&'static str),
    /// The tab-separated column `n` of every line after the header, such as
    /// a fetch timestamp.
    TabColumn(usize),
    /// Every 22-character base64url word: a Kafka `Uuid` generated per run.
    Uuid,
}

fn is_uuid(word: &str) -> bool {
    word.len() == 22
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

impl Scrub {
    /// `text` with this rewrite applied.
    pub fn apply(self, text: &str) -> String {
        let lines = text.split_inclusive('\n');
        match self {
            Self::Field(label) => lines
                .map(|line| match line.split_once(label) {
                    Some((head, rest)) => {
                        let pad = rest.len() - rest.trim_start().len();
                        let value = rest[pad..]
                            .find(char::is_whitespace)
                            .map_or(rest.len(), |end| pad + end);
                        format!("{head}{label}{}#{}", &rest[..pad], &rest[value..])
                    }
                    None => line.to_owned(),
                })
                .collect(),
            Self::TabColumn(column) => lines
                .enumerate()
                .map(|(index, line)| {
                    if index == 0 {
                        return line.to_owned();
                    }
                    line.split('\t')
                        .enumerate()
                        .map(|(at, cell)| {
                            if at == column {
                                format!("{:<width$}", "#", width = cell.len())
                            } else {
                                cell.to_owned()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\t")
                })
                .collect(),
            Self::Uuid => lines
                .map(|line| {
                    line.split(' ')
                        .map(|word| {
                            let trimmed = word.trim_end();
                            if is_uuid(trimmed) {
                                word.replacen(trimmed, "<uuid>", 1)
                            } else {
                                word.to_owned()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect(),
        }
    }
}

/// How the output of one run is cleaned before it is compared.
pub struct Clean<'a> {
    /// Each name replaced by its placeholder, such as a row's own topic.
    pub rename: &'a [(String, &'a str)],
    /// Rewrites of volatile output.
    pub scrub: &'a [Scrub],
}

impl Clean<'_> {
    fn apply(&self, text: &str) -> String {
        let renamed = self
            .rename
            .iter()
            .fold(text.to_owned(), |text, (name, placeholder)| {
                text.replace(name.as_str(), placeholder)
            });
        self.scrub
            .iter()
            .fold(renamed, |text, rewrite| rewrite.apply(&text))
    }
}

/// The comparable part of `capture`, cleaned by `clean`, with `resolved` as
/// the neutral reader saw it.
pub fn view(
    expect: Expect,
    capture: &Capture,
    tool: Tool,
    command: &str,
    clean: &Clean<'_>,
    resolved: &str,
) -> View {
    let surface = if capture.exit == Some(0) {
        Surface::Accepted
    } else {
        Surface::Refused
    };
    let (stdout, stderr) = match expect {
        Expect::Accepted => (clean.apply(&capture.stdout), clean.apply(&capture.stderr)),
        Expect::Rejected => (String::new(), clean.apply(&message(capture, tool, command))),
    };
    View {
        surface,
        exit: capture.exit,
        stdout,
        stderr,
        resolved: clean.apply(resolved),
    }
}

/// How one row came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The two tools agree.
    Agree,
    /// The two tools differ as declared, on purpose.
    ExpectedDifference,
    /// The two tools differ as declared, through a krabka defect.
    KnownDefect,
    /// The JVM tool did not return the exit code the row expects of it, so
    /// the row is wrong.
    Mislabeled { expected: i32, jvm: View },
    /// The two tools differ in `layers`, which no declaration covers.
    Undeclared {
        layers: Vec<Layer>,
        jvm: View,
        krabka: View,
    },
    /// The row declares a difference that did not show.
    Stale { view: View },
}

/// The layers in which `jvm` and `krabka` differ.
pub fn differing(jvm: &View, krabka: &View) -> Vec<Layer> {
    let sorted = |text: &str| {
        let mut chars = text.chars().collect::<Vec<_>>();
        chars.sort_unstable();
        chars
    };
    let mut layers = Vec::new();
    if jvm.surface != krabka.surface {
        layers.push(Layer::Surface);
    } else if jvm.exit != krabka.exit {
        layers.push(Layer::Exit);
    }
    if jvm.stdout != krabka.stdout {
        layers.push(Layer::Stdout);
    }
    if jvm.stderr != krabka.stderr {
        if sorted(&jvm.stderr) == sorted(&krabka.stderr) {
            layers.push(Layer::UnorderedStderr);
        } else {
            layers.push(Layer::Stderr);
        }
    }
    if jvm.resolved != krabka.resolved {
        layers.push(Layer::Resolved);
    }
    layers
}

/// Judges one row from both tools' views.
pub fn judge(expect: Expect, declared: Option<&Declared>, jvm: &View, krabka: &View) -> Verdict {
    if jvm.exit != Some(expect.exit()) {
        return Verdict::Mislabeled {
            expected: expect.exit(),
            jvm: jvm.clone(),
        };
    }
    let allowed = declared.map_or(&[][..], |declared| declared.layers.as_slice());
    // A declared `Stderr` also covers a reordering of the same message.
    let covered = |layer: &Layer| {
        allowed.contains(layer)
            || (*layer == Layer::UnorderedStderr && allowed.contains(&Layer::Stderr))
    };
    let differ = differing(jvm, krabka);
    let undeclared = differ
        .iter()
        .copied()
        .filter(|layer| !covered(layer))
        .collect::<Vec<_>>();
    if !undeclared.is_empty() {
        return Verdict::Undeclared {
            layers: undeclared,
            jvm: jvm.clone(),
            krabka: krabka.clone(),
        };
    }
    // A reordering is a tolerance, not a difference: the JVM prints the same
    // order now and then, so a declaration of it alone is never stale.
    let tolerance = allowed.iter().all(|layer| *layer == Layer::UnorderedStderr);
    match declared.map(|declared| declared.kind) {
        None => Verdict::Agree,
        Some(_) if differ.is_empty() && !tolerance => Verdict::Stale { view: jvm.clone() },
        Some(Kind::Intended) => Verdict::ExpectedDifference,
        Some(Kind::Defect) => Verdict::KnownDefect,
    }
}

/// The verdict that a row passes with: [`Verdict::Agree`] for an undeclared
/// row, and the verdict of its declaration's kind for a declared one.
pub fn wanted(declared: Option<&Declared>) -> Verdict {
    match declared.map(|declared| declared.kind) {
        None => Verdict::Agree,
        Some(Kind::Intended) => Verdict::ExpectedDifference,
        Some(Kind::Defect) => Verdict::KnownDefect,
    }
}

/// `text` without the whitespace-separated column `column` of each line: the
/// stdout shape an expectation would hold with that column removed.
pub fn remove_column(text: &str, column: usize) -> String {
    text.lines()
        .map(|line| {
            line.split_whitespace()
                .enumerate()
                .filter(|(at, _)| *at != column)
                .map(|(_, word)| word)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        Capture, Declared, Difference, Expect, Layer, OUTCOME, Scrub, Surface, Tool, Verdict, View,
        judge, message, remove_column,
    };

    fn accepted(stdout: &str, resolved: &str) -> View {
        View {
            surface: Surface::Accepted,
            exit: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            resolved: resolved.to_owned(),
        }
    }

    fn refused(exit: i32, stderr: &str) -> View {
        View {
            surface: Surface::Refused,
            exit: Some(exit),
            stdout: String::new(),
            stderr: stderr.to_owned(),
            resolved: String::new(),
        }
    }

    fn declare(difference: Difference) -> Option<Declared> {
        Declared::merge([difference])
    }

    const TABLE: &str = "GROUP  TOPIC  PARTITION\ng      t      0\n";
    const EXIT: Difference = Difference::intended(&[Layer::Exit], "exit codes");

    #[test]
    fn judge_reports_each_layer_that_differs() {
        #[rustfmt::skip]
        let cases = [
            ("agree", Expect::Accepted, None, accepted(TABLE, ""), accepted(TABLE, ""), Verdict::Agree),
            ("declared exit", Expect::Rejected, declare(EXIT), refused(1, "m"), refused(3, "m"), Verdict::ExpectedDifference),
            ("stale", Expect::Rejected, declare(EXIT), refused(1, "m"), refused(1, "m"), Verdict::Stale { view: refused(1, "m") }),
            (
                "exit declared, message not",
                Expect::Rejected,
                declare(EXIT),
                refused(1, "m"),
                refused(3, "n"),
                Verdict::Undeclared { layers: vec![Layer::Stderr], jvm: refused(1, "m"), krabka: refused(3, "n") },
            ),
            ("defect", Expect::Accepted, declare(Difference::defect(&[Layer::Stdout], "bug")), accepted("a", ""), accepted("b", ""), Verdict::KnownDefect),
            (
                "unordered",
                Expect::Rejected,
                declare(Difference::intended(&[Layer::UnorderedStderr], "Set.of")),
                refused(1, "[A, B]"),
                refused(1, "[B, A]"),
                Verdict::ExpectedDifference,
            ),
            (
                "unordered, same order this run",
                Expect::Rejected,
                declare(Difference::intended(&[Layer::UnorderedStderr], "Set.of")),
                refused(1, "[A, B]"),
                refused(1, "[A, B]"),
                Verdict::ExpectedDifference,
            ),
            ("outcome covers the surface", Expect::Rejected, declare(Difference::intended(OUTCOME, "clap")), refused(1, "m"), accepted("help", ""), Verdict::ExpectedDifference),
        ];
        for (name, expect, declared, jvm, krabka, want) in cases {
            assert!(
                judge(expect, declared.as_ref(), &jvm, &krabka) == want,
                "{name}"
            );
        }
    }

    #[test]
    fn a_merge_is_a_defect_when_any_part_is() {
        let merged = Declared::merge([EXIT, Difference::defect(&[Layer::Stderr], "wording")]);
        assert!(
            merged
                == Some(Declared {
                    layers: vec![Layer::Exit, Layer::Stderr],
                    kind: super::Kind::Defect,
                    reasons: vec!["exit codes", "wording"],
                })
        );
        assert!(Declared::merge([]) == None);
    }

    /// An expected exit code that the JVM tool does not return fails the row
    /// before anything is compared.
    #[test]
    fn a_wrong_expected_exit_code_fails() {
        let jvm = refused(1, "Option [describe] takes one of these options");
        assert!(
            judge(Expect::Accepted, None, &jvm, &jvm)
                == Verdict::Mislabeled {
                    expected: 0,
                    jvm: jvm.clone()
                }
        );
    }

    /// An expected stdout shape with one column removed no longer matches.
    #[test]
    fn a_removed_column_fails() {
        let jvm = accepted(TABLE, "");
        let expected = View {
            stdout: remove_column(TABLE, 1),
            ..jvm.clone()
        };
        assert!(expected != jvm);
        assert!(matches!(
            judge(Expect::Accepted, None, &expected, &jvm),
            Verdict::Undeclared { layers, .. } if layers == [Layer::Stdout]
        ));
    }

    /// A changed feature level in the resolved state fails, even where a
    /// difference in the printed output is declared.
    #[test]
    fn a_changed_feature_level_fails() {
        let jvm = accepted(
            "Formatting a",
            "metadata.version=25\ntransaction.version=2\n",
        );
        let krabka = accepted(
            "Formatting b",
            "metadata.version=25\ntransaction.version=1\n",
        );
        let declared = declare(Difference::intended(&[Layer::Stdout], "progress output"));
        assert!(matches!(
            judge(Expect::Accepted, declared.as_ref(), &jvm, &krabka),
            Verdict::Undeclared { layers, .. } if layers == [Layer::Resolved]
        ));
    }

    #[test]
    fn scrubs_rewrite_only_the_volatile_part() {
        #[rustfmt::skip]
        let cases = [
            (Scrub::Field("HighWatermark:"), "LeaderId: 1\nHighWatermark:    1065\n", "LeaderId: 1\nHighWatermark:    #\n"),
            (Scrub::Field("Epoch:"), "Feature: a    FinalizedVersionLevel: 1    Epoch: 932\n", "Feature: a    FinalizedVersionLevel: 1    Epoch: #\n"),
            (Scrub::TabColumn(1), "A\tB\tC\n1\t22\t3\n", "A\tB\tC\n1\t# \t3\n"),
            (Scrub::Uuid, "Kjvu9Y8aSU-aFXBD3jhKSA\nshort\n", "<uuid>\nshort\n"),
        ];
        for (scrub, input, want) in cases {
            assert!(scrub.apply(input) == want, "{scrub:?}");
        }
    }

    #[test]
    fn the_message_skips_log_lines_and_exception_prefixes() {
        let capture = |stdout: &str, stderr: &str| Capture {
            exit: Some(1),
            stdout: stdout.to_owned(),
            stderr: stderr.to_owned(),
        };
        #[rustfmt::skip]
        let cases = [
            (Tool::Jvm, capture("", "[2026-09-29 04:51:07,713] ERROR x\norg.apache.kafka.common.errors.BrokerIdNotRegisteredException: Broker ID 5 is not currently registered\n"), "Broker ID 5 is not currently registered"),
            (Tool::Jvm, capture("", "Exception in thread \"main\" java.lang.RuntimeException: Invalid cluster.id\n"), "Invalid cluster.id"),
            (Tool::Jvm, capture("Error while executing topic command : Topic 'x' already exists.\n", ""), "Topic 'x' already exists."),
            (Tool::Jvm, capture("", "Missing required argument: see --help\n"), "Missing required argument: see --help"),
            (Tool::Jvm, capture("", "Error occurred: Could not match any topic-partitions"), "Could not match any topic-partitions"),
            (Tool::Krabka, capture("", "krabka cluster: Broker ID 5\n"), "Broker ID 5"),
        ];
        for (tool, capture, want) in cases {
            assert!(message(&capture, tool, "cluster") == want);
        }
    }
}
