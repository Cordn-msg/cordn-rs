# cordn-rs

A native Rust port of the [`cordn`](https://github.com/ContextVM/cordn) MLS delivery
coordinator — a ContextVM/MCP server that stores and delivers MLS key packages,
welcomes, join requests, and per-group opaque message streams.

> **Status:** port in progress. The TypeScript coordinator (reference
> implementation, currently `0.4.0`) is the source of truth for behavior and
> wire/DB compatibility. See [`AGENTS.md`](./AGENTS.md) for the full design
> decisions and conventions.

## Goals

- **Drop-in replacement** for the TS coordinator, including the existing SQLite
  database — a DB written by the TS coordinator must be readable by this one and
  vice-versa, with no migration.
- **Behavioral parity** with the TS coordinator across all storage backends and
  the MCP tool surface.
- Native performance and a smaller runtime footprint.

## Non-goals

- This is **not** an MLS implementation. The coordinator is opaque to MLS
  payload contents by design (spec `references/cordn/spec/00.md` §7-8). It parses
  key packages only far enough to read the BasicCredential identity and detect
  the last-resort extension; everything else is stored and returned verbatim.

## Workspace layout

```text
cordn-rs/
  Cargo.toml                 # [workspace] resolver = "2"
  crates/
    cordn-core/              # lib: types, storage, coordinator, mls_parse,
                             #      last_resort, ratelimit, contracts.
                             #      Zero network dependencies.
    cordn-server/            # bin: depends on cordn-core + contextvm-sdk +
                             #      rmcp + tokio. The runnable coordinator.
  references/                # git-ignored reference trees (read-only)
    cordn/                   # the TS coordinator (parity source of truth)
    rs-sdk/                  # contextvm-sdk Rust SDK
```

`cordn-core` has no network or async-runtime dependencies, so it compiles and
tests fast and is the natural artifact to publish if an SDK is released later.

## Toolchain

- Rust stable, MSRV **1.88** (matches `contextvm-sdk`).
- Workspace package manager: `cargo`.

## Quick start

```bash
cargo build                 # build everything
cargo test                  # run all tests
cargo run -p cordn-server   # run the coordinator server
```

## References

- `references/cordn/` — the TS coordinator. Behavior, contracts, schema, and
  tests are ported from here.
- `references/cordn/spec/` — the protocol specifications.
- `references/rs-sdk/` — the ContextVM Rust SDK used by `cordn-server`.

See [`AGENTS.md`](./AGENTS.md) for development conventions and the locked
architectural decisions.
