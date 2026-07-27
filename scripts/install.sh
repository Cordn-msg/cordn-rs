#!/usr/bin/env sh
# cordn-server install — downloads the latest prebuilt binary from GitHub
# Releases, verifies the SHA256 checksum, and extracts it to $PREFIX.
#
#   sh scripts/install.sh                     # binary only
#   sh scripts/install.sh --service           # binary + hardened systemd unit
#   PREFIX=$HOME/.local/bin sh scripts/install.sh
#   CORDN_SERVER_PRIVATE_KEY=<hex> sh scripts/install.sh --service
#
# --service (Linux/systemd, root): installs a non-root system user, a data dir
# at /var/lib/cordn, an env file at /etc/cordn/cordn.env, and a hardened systemd
# unit, then enables + starts `cordn`. It persists a server key (generated via
# `openssl rand -hex 32` unless CORDN_SERVER_PRIVATE_KEY is set) and SQLite
# storage, so the identity and data survive restarts — i.e. production-ready.
# No interactive wizard: the server auto-handles the rest; edit cordn.env to
# tune. (ponytail: a bootstrap TUI would reimplement `editor cordn.env`.)
#
# Functions are defined BEFORE the main flow: POSIX sh (dash) needs a function
# defined before first call, unlike bash.
set -eu

REPO="Cordn-msg/cordn-rs"
PREFIX="${PREFIX:-/usr/local/bin}"
SERVICE=0

usage() {
  cat <<EOF
Usage: sh scripts/install.sh [--service]
  --service   also install + enable a hardened systemd unit (Linux, root)
Env:
  PREFIX                      binary install dir (default /usr/local/bin)
  CORDN_SERVER_PRIVATE_KEY    seed the service env file (else generated)
EOF
}

# Requires root + systemd. Run AFTER the binary is installed at $1.
install_service() {
  BIN="$1"
  USER=cordn
  DATA=/var/lib/cordn
  ENVFILE=/etc/cordn/cordn.env
  UNIT=/etc/systemd/system/cordn.service

  # Dedicated non-root system user + data dir.
  id "$USER" >/dev/null 2>&1 || \
    useradd --system --no-create-home --home-dir "$DATA" --shell /usr/sbin/nologin --user-group "$USER"
  mkdir -p "$DATA"
  chown "$USER:$USER" "$DATA"
  chmod 750 "$DATA"

  # Env file — idempotent: never overwrite an existing one (preserves edits +
  # the persisted key across re-installs).
  mkdir -p /etc/cordn
  if [ ! -f "$ENVFILE" ]; then
    KEY="${CORDN_SERVER_PRIVATE_KEY:-}"
    if [ -z "$KEY" ]; then
      command -v openssl >/dev/null 2>&1 || \
        { echo "openssl not found: need it to generate a key, or set CORDN_SERVER_PRIVATE_KEY" >&2; exit 1; }
      KEY="$(openssl rand -hex 32)"
    fi
    cat > "$ENVFILE" <<EOF
# /etc/cordn/cordn.env — created by 'install.sh --service'. Edit freely.
# Persistent server identity. KEEP THIS SECRET + backed up — clients address
# the server by the public key derived from it. Losing it = new identity.
CORDN_SERVER_PRIVATE_KEY=$KEY

# Nostr relays (comma-separated).
CORDN_RELAY_URLS=wss://relay.contextvm.org

# Persistent storage (data dir is $DATA).
CORDN_STORAGE_BACKEND=sqlite
CORDN_SQLITE_PATH=$DATA/cordn.sqlite
EOF
    chmod 640 "$ENVFILE"
    chown root:"$USER" "$ENVFILE"
    echo "→ wrote $ENVFILE (generated + persisted a fresh server key)"
  else
    echo "→ kept existing $ENVFILE"
  fi

  cat > "$UNIT" <<EOF
[Unit]
Description=cordn-server - MLS delivery coordinator
Documentation=https://github.com/Cordn-msg/cordn-rs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
Group=$USER
EnvironmentFile=$ENVFILE
ExecStart=$BIN
Restart=on-failure
RestartSec=5s
TimeoutStopSec=15s
# Hardening: the server reads its env + data dir and dials out to relays (wss).
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
LockPersonality=true
MemoryDenyWriteExecute=true
RestrictRealtime=true
RestrictSUIDSGID=true
SystemCallArchitectures=native
CapabilityBoundingSet=
AmbientCapabilities=

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now cordn
  echo
  systemctl --no-pager --full status cordn || true
  cat <<EOF

✓ cordn service enabled and started.
  config:  $ENVFILE
  data:    $DATA
  logs:    journalctl -u cordn -f
  apply config edits: systemctl restart cordn   # no reload: env is read once at startup
  remove:  systemctl disable --now cordn && rm -f $UNIT && systemctl daemon-reload
EOF
}

# --- main flow ---

for arg in "$@"; do
  case "$arg" in
    --service) SERVICE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$arg" >&2; usage >&2; exit 1 ;;
  esac
done

# Minimal server images (e.g. plain debian) ship without curl; fail clearly
# instead of a cryptic error mid-download. (openssl is checked only inside
# --service, when a key actually needs generating.)
command -v curl >/dev/null 2>&1 || { echo "curl not found — install it (e.g. apt-get install curl)" >&2; exit 1; }

if [ "$SERVICE" = 1 ]; then
  [ "$(id -u)" -eq 0 ] || { echo "--service requires root (system unit + system user)" >&2; exit 1; }
  command -v systemctl >/dev/null 2>&1 || { echo "--service needs systemd (systemctl not found)" >&2; exit 1; }
  PREFIX=/usr/local/bin        # the unit references this absolute path
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "$(uname -sm)" in
  Linux\ x86_64)  TARGET=x86_64-unknown-linux-gnu ;;
  Linux\ aarch64) TARGET=aarch64-unknown-linux-gnu ;;
  *) echo "unsupported platform: $(uname -sm) — prebuilt binaries are linux/amd64 + linux/arm64" >&2; exit 1 ;;
esac

TAG="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
        | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
[ -n "$TAG" ] || { echo "could not resolve latest release for $REPO" >&2; exit 1; }
BASE="https://github.com/$REPO/releases/download/$TAG"

echo "→ installing cordn-server $TAG ($TARGET) to $PREFIX"
cd "$TMP"
curl -fsSL -O "$BASE/cordn-server-$TARGET.tar.gz"
curl -fsSL -O "$BASE/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
tar xzf "cordn-server-$TARGET.tar.gz"

mkdir -p "$PREFIX" 2>/dev/null || true
if [ ! -w "$PREFIX" ]; then
  echo "$PREFIX is not writable — re-run with sudo, or set PREFIX=\$HOME/.local/bin" >&2
  exit 1
fi
install -m 0755 cordn-server "$PREFIX/cordn-server"
echo "✓ binary: $PREFIX/cordn-server"

[ "$SERVICE" = 1 ] && install_service "$PREFIX/cordn-server"
echo "done — set CORDN_* env vars (see .env.example) and run: cordn-server"
