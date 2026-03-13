#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env.local ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env.local
  set +a
fi

WATCH_CONFIG="${WATCH_CONFIG:-}"
WATCH_CONFIG_DIR="${WATCH_CONFIG_DIR:-config/oneshot-tasks}"
WATCH_POLL_SECONDS="${WATCH_POLL_SECONDS:-5}"

if [[ -n "$WATCH_CONFIG" ]]; then
  CONFIG_ARGS=(--config "$WATCH_CONFIG")
else
  CONFIG_ARGS=(--config-dir "$WATCH_CONFIG_DIR")
fi

exec "$ROOT_DIR/target/release/tradebot-cli" \
  watch \
  "${CONFIG_ARGS[@]}" \
  --poll-seconds "$WATCH_POLL_SECONDS"
