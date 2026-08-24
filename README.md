# krabka-cli

The [krabka](https://github.com/krabka-io) operator CLI, `krabka`.

## Subcommands

`krabka format` prepares a fresh log directory, optionally seeding SCRAM
credentials and KIP-1022 feature levels.

Everything else is a plugin. An unrecognised subcommand is delegated to
`krabka-<name>` on `PATH`, the way `git` finds `git-foo` and `cargo` finds
`cargo-foo`:

```bash
krabka gres list-tenants --bootstrap localhost:9092
# runs: krabka-gres list-tenants --bootstrap localhost:9092
```

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
