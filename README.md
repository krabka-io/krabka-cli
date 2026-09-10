# krabka-cli

The [krabka](https://github.com/krabka-io) operator CLI, `krabka`.

## Subcommands

### Built in

`krabka format` prepares a fresh log directory, optionally seeding SCRAM
credentials and KIP-1022 feature levels.

### Plugins

Everything else is a plugin. An unrecognised subcommand is delegated to
`krabka-<name>` on `PATH`, the way `git` finds `git-foo` and `cargo` finds
`cargo-foo`:

```bash
krabka gres list-tenants --bootstrap localhost:9092
# runs: krabka-gres list-tenants --bootstrap localhost:9092
```

Each plugin is a separate install, and `krabka --help` lists the known ones:

- `krabka restore` runs `krabka-restore`, a point-in-time restore of a cluster data directory from a tiered-storage archive. It ships from [`krabka-broker`](https://github.com/krabka-io/krabka-broker), so install it separately.
- `krabka gres` runs `krabka-gres`, which ships from the gres repository.

That is what lets a subcommand live in the repository that owns the thing it
operates on. Compiling them in instead would make this binary's dependency
graph the union of every product in the organisation -- the `gres` subcommand
alone reaches about 21k lines of storage engine.

A built-in always wins, so a stray `krabka-format` on `PATH` cannot shadow the
compiled-in one. A subcommand that is neither built in nor on `PATH` exits 127,
and one that is found but cannot be run exits 126 -- the codes a shell uses for
the same two cases.

## Layering

Depends on three sibling repositories, pinned by revision in
[`Cargo.toml`](Cargo.toml)'s `[patch.crates-io]`:

| Repository | What it supplies |
| --- | --- |
| [`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) | Wire types, security, metadata, units |
| [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) | The admin and core clients |
| [`krabka-broker`](https://github.com/krabka-io/krabka-broker) | Raft, and the bootstrap records `format` writes |

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
