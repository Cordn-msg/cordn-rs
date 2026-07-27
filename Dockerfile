# syntax=docker/dockerfile:1
#
# cordn-server — MLS delivery coordinator (ContextVM/MCP delivery service).
#
# It dials Nostr relays over wss; there is NO inbound listening port, so there
# is nothing to EXPOSE. Runtime needs only glibc: SQLite is compiled in via
# rusqlite's "bundled" feature and TLS uses rustls + webpki-roots (Mozilla CA
# roots baked into the binary). ca-certificates is belt-and-suspenders for any
# future dep that reads the system store.

FROM rust:1-bookworm AS builder
WORKDIR /build
COPY . .
# --mount caches the cargo registry + target across builds (buildx/CI).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --features server -p cordn-server && \
    cp target/release/cordn-server /cordn-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --no-create-home --uid 10001 cordn \
 && mkdir -p /data && chown cordn:cordn /data
COPY --from=builder /cordn-server /usr/local/bin/cordn-server
USER cordn
# Safe defaults: ephemeral server key, in-memory storage. For SQLite persistence
# set CORDN_STORAGE_BACKEND=sqlite; the DB path defaults to /data/cordn.sqlite
# (below), and /data is owned by the cordn user so a named volume mounted there
# is writable. For a bind mount, the host dir must be writable by uid 10001
# (or run with --user $(id -u):$(id -g)).
ENV CORDN_STORAGE_BACKEND=memory
ENV CORDN_SQLITE_PATH=/data/cordn.sqlite
ENTRYPOINT ["cordn-server"]
