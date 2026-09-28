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
//! this module was captured against it. The broker runs as a single
//! `KRaft` node, broker and controller in one process, and advertises
//! `127.0.0.1:<port>` on a port that is mapped to the same port on the host,
//! so the tool inside the container and `krabka` on the host reach the same
//! listener at the same address.
//!
//! # Verdicts
//!
//! Each matrix row runs once under each tool. A row whose tool outcomes
//! differ passes only when the row appears in the suite's expected-difference
//! list, with a reason. A declared difference that no longer shows fails as
//! stale, so the list cannot hide a closed gap.

use std::{
    net::TcpListener,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// The oracle image: `apache/kafka:4.3.1`, pinned by digest.
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

fn capture(command: &mut Command) -> Capture {
    let out = command
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|error| panic!("run {command:?}: {error}"));
    Capture {
        exit: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// A running oracle broker. Dropping it removes the container.
pub struct Oracle {
    container: String,
    port: u16,
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
        ];
        let mut run = Command::new("docker");
        run.args(["run", "-d", "--rm", "--name", &container]);
        run.args(["-p", &format!("127.0.0.1:{port}:{port}")]);
        for variable in &env {
            run.args(["-e", variable]);
        }
        run.arg(IMAGE);
        let started = capture(&mut run);
        assert!(started.exit == Some(0), "docker run: {started:?}");
        let oracle = Self { container, port };
        let deadline = Instant::now() + Duration::from_mins(2);
        loop {
            if oracle
                .jvm("kafka-topics.sh", &["--list".to_owned()], true)
                .exit
                == Some(0)
            {
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

    /// Runs a JVM tool script from `/opt/kafka/bin` inside the container,
    /// with `--bootstrap-server` first when `bootstrap` is set.
    pub fn jvm(&self, script: &str, args: &[String], bootstrap: bool) -> Capture {
        let mut command = Command::new("docker");
        command.args(["exec", &self.container, &format!("/opt/kafka/bin/{script}")]);
        if bootstrap {
            command.args(["--bootstrap-server", &self.bootstrap()]);
        }
        capture(command.args(args))
    }

    /// Runs `krabka <subcommand>` on the host, with `--bootstrap-server`
    /// after the subcommand when `bootstrap` is set. The environment holds
    /// no `KRABKA_*` variable but those in `env`.
    pub fn krabka(
        &self,
        subcommand: &str,
        args: &[String],
        bootstrap: bool,
        env: &[(&str, &str)],
    ) -> Capture {
        let mut command = Command::new(env!("CARGO_BIN_EXE_krabka"));
        for (key, _) in std::env::vars() {
            if key.starts_with("KRABKA_") {
                command.env_remove(key);
            }
        }
        command.env("RUST_LOG", "off").envs(env.iter().copied());
        command.arg(subcommand);
        if bootstrap {
            command.args(["--bootstrap-server", &self.bootstrap()]);
        }
        capture(command.args(args))
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

/// Whether the JVM tool accepts a row's command line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The JVM tool exits 0. Exit code, stdout and stderr must agree.
    Accepted,
    /// The JVM tool refuses it. Exit code and message must agree.
    Rejected,
}

/// A declared difference between the two tools on one row, with its reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Difference {
    /// The exit codes agree and the output differs.
    Message(&'static str),
    /// The exit code may differ too.
    Outcome(&'static str),
    /// The JVM message lists a `Set.of`, whose order changes from one JVM run
    /// to the next. The exit codes and stdout must agree, and the messages
    /// must hold the same characters.
    Unordered(&'static str),
}

impl Difference {
    /// Why the tools differ on the row.
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Message(reason) | Self::Outcome(reason) | Self::Unordered(reason) => reason,
        }
    }
}

/// The part of a capture that a row compares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    pub exit: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Which tool produced a capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    Jvm,
    Krabka,
}

/// The prefix of a failure that `TopicCommand` prints on stdout.
const JVM_FAILURE: &str = "Error while executing topic command : ";

/// The message of a refusal: the line after [`JVM_FAILURE`] on stdout, as
/// both tools print a broker's per-topic failure, or the first line of
/// stderr, without the `krabka <command>: ` prefix of krabka's output layer.
fn message(capture: &Capture, tool: Tool, command: &str) -> String {
    if let Some(line) = capture
        .stdout
        .lines()
        .find_map(|line| line.strip_prefix(JVM_FAILURE))
    {
        return line.to_owned();
    }
    let first = capture.stderr.lines().next().unwrap_or_default();
    match tool {
        Tool::Jvm => first.to_owned(),
        Tool::Krabka => first
            .strip_prefix(&format!("krabka {command}: "))
            .unwrap_or(first)
            .to_owned(),
    }
}

/// The comparable part of `capture`, with each name in `rename` replaced by
/// its placeholder.
pub fn view(
    expect: Expect,
    capture: &Capture,
    tool: Tool,
    command: &str,
    rename: &[(String, &str)],
) -> View {
    let rename = |text: &str| {
        rename
            .iter()
            .fold(text.to_owned(), |text, (name, placeholder)| {
                text.replace(name.as_str(), placeholder)
            })
    };
    match expect {
        Expect::Accepted => View {
            exit: capture.exit,
            stdout: rename(&capture.stdout),
            stderr: rename(&capture.stderr),
        },
        Expect::Rejected => View {
            exit: capture.exit,
            stdout: String::new(),
            stderr: rename(&message(capture, tool, command)),
        },
    }
}

/// How one row came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The two tools agree.
    Agree,
    /// The two tools differ as declared.
    ExpectedDifference,
    /// The JVM tool did not do what the row expects of it, so the row is
    /// wrong.
    Mislabeled { jvm: View },
    /// The two tools differ, and the row declares no difference, or the exit
    /// codes differ where only the message may.
    Undeclared { jvm: View, krabka: View },
    /// The row declares a difference that did not show.
    Stale { view: View },
}

/// Judges one row from both tools' views.
pub fn judge(expect: Expect, declared: Option<Difference>, jvm: &View, krabka: &View) -> Verdict {
    let jvm_accepted = jvm.exit == Some(0);
    if jvm_accepted != (expect == Expect::Accepted) {
        return Verdict::Mislabeled { jvm: jvm.clone() };
    }
    let undeclared = || Verdict::Undeclared {
        jvm: jvm.clone(),
        krabka: krabka.clone(),
    };
    let same_characters = |a: &str, b: &str| {
        let sorted = |text: &str| {
            let mut chars = text.chars().collect::<Vec<_>>();
            chars.sort_unstable();
            chars
        };
        sorted(a) == sorted(b)
    };
    let unordered_match = (jvm.exit, &jvm.stdout) == (krabka.exit, &krabka.stdout)
        && same_characters(&jvm.stderr, &krabka.stderr);
    match declared {
        None if jvm == krabka => Verdict::Agree,
        Some(Difference::Unordered(_)) if unordered_match => Verdict::ExpectedDifference,
        Some(Difference::Message(_) | Difference::Outcome(_)) if jvm == krabka => {
            Verdict::Stale { view: jvm.clone() }
        }
        Some(Difference::Message(_)) if jvm.exit == krabka.exit => Verdict::ExpectedDifference,
        Some(Difference::Outcome(_)) => Verdict::ExpectedDifference,
        None | Some(Difference::Message(_) | Difference::Unordered(_)) => undeclared(),
    }
}
