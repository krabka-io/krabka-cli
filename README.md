# krabka-cli

The [krabka](https://github.com/krabka-io) operator CLI, `krabka`.

## Subcommands

### Built in

`krabka format` prepares a fresh log directory, optionally seeding SCRAM
credentials and KIP-1022 feature levels. `krabka gres` operates the Gres tenant
registry and range layout, including tenant creation, inspection, balancing,
and PgDog configuration rendering.

### Plugins

Everything else is a plugin. An unrecognised subcommand is delegated to
`krabka-<name>` on `PATH`, the way `git` finds `git-foo` and `cargo` finds
`cargo-foo`:

```bash
krabka restore --help   # runs krabka-restore --help
```

Each plugin is a separate install, and `krabka --help` lists the known ones:

- `krabka admin-ui` runs `krabka-admin-ui`, the operator web UI. This repository builds it; see [Admin UI](#admin-ui) below.
- `krabka backup` runs `krabka-backup`, which captures the inputs of a point-in-time restore.
- `krabka barrier` runs `krabka-barrier`, which defines barrier groups and triggers and verifies cuts.
- `krabka guard` runs `krabka-guard`, which freezes and thaws topic writes under a two-person break-glass rule.
- `krabka restore` runs `krabka-restore`, a point-in-time restore of a cluster data directory from a tiered-storage archive.

The last four ship from [`krabka-broker`](https://github.com/krabka-io/krabka-broker), so install them separately.

The Gres operator command is built in because the demo uses it to provision its
tenant before the Gres server starts accepting PostgreSQL connections.

A built-in always wins, so a stray `krabka-format` on `PATH` cannot shadow the compiled-in one. `krabka` resolves the plugin on `PATH` itself and runs the first executable match. A subcommand that is neither built in nor on `PATH` exits 127, and one that is found but cannot be run exits 126, naming the file it found. These are the codes a shell uses for the same two cases. The plugin's own exit code passes through unchanged, and a plugin killed by signal n exits 128 + n. While a plugin runs, Ctrl-C goes to the plugin, and `krabka` forwards SIGTERM to it.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | The operation failed, or at least one row of it failed |
| 2 | Usage error: clap's code, and a refusal such as a missing `--yes` |
| 126 | A `krabka-<name>` plugin is on `PATH` but cannot be run |
| 127 | No built-in and no `krabka-<name>` on `PATH` |
| 128 + n | The plugin died from signal n |
| 130 | Cancelled with Ctrl-C, or a declined confirmation |

`krabka format` returns the codes of `krabka-format`, and a plugin returns its own.

## Admin UI

`crates/admin-ui` builds `krabka-admin-ui`, the second operator-facing surface
in this repository. It is a web UI, not a terminal UI: a [Dioxus](https://dioxuslabs.com)
component tree that the server renders to HTML with `dioxus-ssr` and serves
over [axum](https://github.com/tokio-rs/axum). Every page is rendered on the
server, so there is no WebAssembly bundle, no JavaScript, and no static asset
directory to build or ship. The binary is the whole deployment.

```bash
KRABKA_ADMIN_UI_BOOTSTRAP=localhost:9092 krabka admin-ui
```

It signs an operator in with SASL/SCRAM-SHA-512, holds the session server-side
behind an `HttpOnly` cookie, and reads the operator's ACLs to decide which
pages and actions to show. The pages are overview, topics, groups, ACLs, users,
quotas and log directories.

Each mutation is a plain HTML form that the browser posts. The server refuses a
mutation that carries no CSRF token, because the session cookie alone does not
show that the operator asked for it. A form sends the token in its `csrf_token`
field. A JSON client sends the same token in the `x-krabka-csrf` header. The
token belongs to one session, so a page on another origin cannot supply it.

It stays a separate binary rather than a subcommand module of `krabka`, for the
reason given under [Plugins](#plugins): the component framework is a large
dependency graph, and the CLI does not take it on. `krabka admin-ui` finds it on
PATH like any other plugin.

The UI talks to one service, the broker, through the Kafka admin protocol.
[`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) supplies
that client: `krabka-client-admin` for metadata, groups, log directories, ACLs,
SCRAM users, quotas and configuration, and `krabka-client-core` for the
security handshake. It reaches no HTTP endpoint of its own and no other
krabka-io service.

## Layering

Depends on four sibling repositories, pinned by revision in
[`Cargo.toml`](Cargo.toml)'s `[patch.crates-io]`:

| Repository | What it supplies |
| --- | --- |
| [`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) | Wire types, security, metadata, units |
| [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) | The admin and core clients |
| [`krabka-broker`](https://github.com/krabka-io/krabka-broker) | Raft, and the bootstrap records `format` writes |
| [`gres`](https://github.com/krabka-io/gres) | Gres registry, range planning, and tenant provisioning |

`crates/admin-ui` uses two of the three: the admin and core clients, and the
wire-layer security and unit types. It does not use `krabka-broker`.

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are supported and both are gated in CI. Cargo stays the dependency source
of truth; Bazel reads the same `Cargo.toml` and `Cargo.lock`.

```bash
bazel run //:krabka -- format --help
```

## Candidate-broker qualification

The ignored `candidate_broker` test runs the authenticated admin command matrix
against a deployed candidate and emits one JSON evidence document. Run it from
an immutable CLI revision and archive both the output and its digest:

```bash
export KRABKA_CLI_REVISION="$(git rev-parse HEAD)"
export KRABKA_CANDIDATE_REVISION='<40-character broker commit>'
export KRABKA_CANDIDATE_IMAGE='<registry/repository@sha256:digest>'
export KRABKA_CANDIDATE_BOOTSTRAP='<host:port>'
export KRABKA_COMMAND_CONFIG='<authenticated Kafka properties file>'
export KRABKA_DENIED_COMMAND_CONFIG='<properties file for the denied principal>'
export KRABKA_DENIED_PRINCIPAL='User:<principal named by the denied config>'
set -o pipefail
cargo test -p krabka-cli --test candidate_broker \
  authenticated_admin_matrix_matches_real_broker_state \
  -- --ignored --exact --nocapture \
  | tee candidate-broker.jsonl
sha256sum candidate-broker.jsonl
```

The properties files support `security.protocol`, PEM TLS, SASL PLAIN, SCRAM,
GSSAPI, and a file-backed OAuth bearer token. They must not be archived with
the evidence. The candidate must have at least two brokers. The denied principal
must initially be able to describe the test topic so the lane proves that the
ACL created by the matrix causes the recorded authorization failure.

The matrix records arguments, credential label, structured response, and exit
status for every operation. It also observes broker state after topic creation,
configuration, offset reset, reassignment, and deletion. Reassignment evidence
contains every progress check through completion; invalid and unauthorized
requests must return structured errors with a nonzero exit status.
