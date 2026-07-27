# cordn-rs

A native Rust port of the [`cordn`](https://github.com/ContextVM/cordn) MLS
delivery coordinator — a ContextVM/MCP server that stores and delivers MLS key
packages, welcomes, join requests, and per-group opaque message streams.

> **Status:** the port is **complete and parity-asserted**. It is a drop-in
> replacement for the TypeScript coordinator (`cordn` `0.4.0`): the same wire
> shapes, the same SQLite schema, and a database written by the TS coordinator is
> readable byte-for-byte by this one (proven by
> [`crates/cordn-core/tests/db_cross_read.rs`](./crates/cordn-core/tests/db_cross_read.rs)).
> This release tracks `cordn` `0.4.0` protocol/DB compatibility.

## Highlights

- **Drop-in replacement** for the TS coordinator, including the existing SQLite
  database — no migration.
- **Behavioral parity** across storage backends and the full MCP tool surface.
- **Native performance.** ~6.8× faster core throughput, ~35× smaller baseline
  memory than the TS server. See
  [`docs/benchmark-results.md`](./docs/benchmark-results.md) for the full
  measurements and how to reproduce them.

## Non-goals

This is **not** an MLS implementation. The coordinator is opaque to MLS payload
contents by design (spec `references/cordn/spec/00.md` §7-8). It parses key
packages only far enough to read the BasicCredential identity and detect the
last-resort extension; everything else is stored and returned verbatim.

## Install

### Prebuilt binary

Download the archive for your platform from the
[Releases page](https://github.com/Cordn-msg/cordn-rs/releases), verify the
checksum against `SHA256SUMS`, and extract `cordn-server`:

```bash
tar xzf cordn-server-<target>.tar.gz
./cordn-server
```

Or use the one-line installer (verifies the SHA256 checksum; linux/amd64 +
linux/arm64). Set `PREFIX` to install elsewhere, e.g. `PREFIX=$HOME/.local/bin`:

```bash
sh scripts/install.sh
```

### Docker

A multi-arch image (`linux/amd64`, `linux/arm64`) is published to GHCR on every
release tag. The server dials relays over wss — there is no port to publish.

```bash
docker run -d --name cordn \
  -e CORDN_SERVER_PRIVATE_KEY=<hex> \
  -e CORDN_RELAY_URLS=wss://relay.contextvm.org \
  -e CORDN_STORAGE_BACKEND=sqlite \
  -e CORDN_SQLITE_PATH=/data/cordn.sqlite \
  -v cordn-data:/data \
  ghcr.io/cordn-msg/cordn-rs/cordn-server:latest
```

Defaults (safe to omit): ephemeral server key, in-memory storage.

### Build from source

Requires Rust stable (MSRV **1.88**).

```bash
git clone https://github.com/Cordn-msg/cordn-rs.git
cd cordn-rs
cargo build --release -p cordn-server --features server
# binary: target/release/cordn-server
```

## Quick start

```bash
# Minimal run: ephemeral server key, in-memory storage, default relay.
cargo run -p cordn-server --features server

# Persistent identity + SQLite storage + your relays:
CORDN_SERVER_PRIVATE_KEY=<hex> \
CORDN_RELAY_URLS=wss://relay.example.com,wss://relay.contextvm.org \
CORDN_STORAGE_BACKEND=sqlite \
CORDN_SQLITE_PATH=./cordn.sqlite \
cargo run -p cordn-server --features server
```

On startup the server prints its public key and configured relays. See
[`AGENTS.md`](./AGENTS.md) for the locked design decisions.

## Configuration

The server loads `.env` then `.env.local` (first-write-wins per key), then the
process environment. Unset values use the defaults below.

| Variable | Default | Description |
|---|---|---|
| `CORDN_SERVER_PRIVATE_KEY` | _(ephemeral)_ | Hex Nostr private key. Unset → a new key is generated each start. |
| `CORDN_RELAY_URLS` | `wss://relay.contextvm.org` | Comma-separated relay URLs. |
| `CORDN_SERVER_NAME` | `cordn-server` | Server name (kind 0 metadata). |
| `CORDN_SERVER_ABOUT` | _(none)_ | About text. |
| `CORDN_SERVER_WEBSITE` | _(none)_ | Website. |
| `CORDN_ANNOUNCED` | `false` | Announce the server on the relay. |
| `CORDN_STORAGE_BACKEND` | `memory` | `memory` \| `sqlite`. |
| `CORDN_SQLITE_PATH` | `./cordn.sqlite` | _(sqlite only)_ DB file path. |
| `CORDN_SQLITE_SYNCHRONOUS` | `normal` | _(sqlite only)_ `normal` \| `full`. `normal` is ~30–40× faster than `full`; crash-safe, no corruption (power loss can drop the last committed txn). The TS server defaults to `full`. |
| `CORDN_MAX_AGE_DAYS` | `30` | Max age for welcome/join-request cleanup. `0` keeps forever. |
| `CORDN_RATE_LIMIT_ENABLED` | `true` | Per-identity token-bucket rate limiting. |
| `CORDN_RATE_LIMIT_REFILL_PER_MINUTE` | `500` | Tokens added per minute. |
| `CORDN_RATE_LIMIT_BURST` | `160` | Bucket capacity. |
| `CORDN_RATE_LIMIT_IDLE_TTL_SECONDS` | `3600` | Idle-identity state retention. |
| `CORDN_MAX_KEY_PACKAGES_PER_IDENTITY` | `50` | Max published key packages per identity. |
| `CORDN_MAX_LAST_RESORT_KEY_PACKAGES_PER_IDENTITY` | `1` | Max last-resort key packages per identity. |
| `CORDN_LOG_ABUSE_REJECTIONS` | `true` | Log rate-limit/quota rejections. |

## Workspace layout

```text
cordn-rs/
  Cargo.toml                 # [workspace] resolver = "2", single source of version
  crates/
    cordn-core/              # lib: types, storage, coordinator, mls_parse,
                             #      ratelimit, contracts. Zero network deps.
    cordn-server/            # bin: depends on cordn-core + contextvm-sdk +
                             #      rmcp + tokio. The runnable coordinator.
  docs/                      # benchmark proposal + results
  references/                # git-ignored reference trees (read-only; dev only)
    cordn/                   # the TS coordinator (parity source of truth)
    rs-sdk/                  # contextvm-sdk Rust SDK
```

`cordn-core` has no network or async-runtime dependencies, so it compiles and
tests fast and is the natural artifact to publish if an SDK is released later.

`references/` is **git-ignored** and not required to build or test the port. It
is only needed to regenerate the parser/parity fixtures and to run the
cross-language benchmarks — see [`docs/`](./docs/) for how to populate it.

## Storage & parity

The SQLite schema is fixed and mirrors
`references/cordn/src/coordinator/storage/sqliteStorage.ts` byte-for-byte,
including per-group cursor allocation, the legacy-column migrations, and
production pragmas (`journal_mode = WAL`, `foreign_keys = ON`,
`busy_timeout = 5000`). Key packages, welcomes, and opaque messages are stored as
the exact incoming wire bytes (no re-encoding).

The drop-in guarantee is tested in four layers (unit, integration with MLS
fixtures, cross-impl smoke, and DB cross-read) — see
[`AGENTS.md`](./AGENTS.md) for details.

## Development

```bash
cargo build                              # build workspace (default features)
cargo build -p cordn-server --features server   # build the runnable server
cargo check --all-targets                # fast type-check
cargo fmt --all                          # format
cargo clippy --all-targets -- -D warnings   # lint
cargo test                               # all tests
```

### Releasing

Releases are tag-driven. From a clean working tree:

```bash
make patch   # 0.4.0 → 0.4.1
make minor   # 0.4.0 → 0.5.0
make major   # 0.4.0 → 1.0.0
```

Each target bumps the workspace version, refreshes `Cargo.lock`, commits, tags
`vX.Y.Z`, and pushes — which triggers `.github/workflows/release.yml` to build
cross-platform binaries and publish a GitHub Release.

## References

- `references/cordn/` — the TS coordinator. Behavior, contracts, schema, and
  tests are ported from here.
- `references/cordn/spec/` — the protocol specifications.
- `references/rs-sdk/` — the ContextVM Rust SDK used by `cordn-server`.

See [`AGENTS.md`](./AGENTS.md) for development conventions and the locked
architectural decisions.

## License

[MIT](./LICENSE).
