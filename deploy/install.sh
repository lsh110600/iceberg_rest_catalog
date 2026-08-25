#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

verify_checksums() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check SHA256SUMS
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 --check SHA256SUMS
  else
    echo "sha256sum or shasum is required to verify the bundle." >&2
    exit 1
  fi
}

command -v docker >/dev/null 2>&1 || {
  echo "Docker Engine and Docker Compose v2 are required." >&2
  exit 1
}
docker compose version >/dev/null

verify_checksums
docker image load --input images.tar

if [[ ! -f .env ]]; then
  cp .env.airgap.example .env
  chmod 600 .env
  echo "Created $SCRIPT_DIR/.env. Set a strong POSTGRES_PASSWORD and deployment addresses, then run ./install.sh again."
  exit 2
fi

PASSWORD_LINE="$(grep -E '^POSTGRES_PASSWORD=' .env || true)"
POSTGRES_PASSWORD="${PASSWORD_LINE#POSTGRES_PASSWORD=}"
PLAYGROUND_LINE="$(grep -E '^PLAYGROUND_ENABLED=' .env || true)"
PLAYGROUND_ENABLED="${PLAYGROUND_LINE#PLAYGROUND_ENABLED=}"
RUNNER_TOKEN_LINE="$(grep -E '^PLAYGROUND_RUNNER_TOKEN=' .env || true)"
PLAYGROUND_RUNNER_TOKEN="${RUNNER_TOKEN_LINE#PLAYGROUND_RUNNER_TOKEN=}"
PROFILE_ARGS=()

if [[ "$PLAYGROUND_ENABLED" == "true" ]]; then
  if [[ -z "$PLAYGROUND_RUNNER_TOKEN" || "$PLAYGROUND_RUNNER_TOKEN" == REPLACE_WITH_* ]]; then
    echo "Refusing to start Playground with the placeholder PLAYGROUND_RUNNER_TOKEN in .env." >&2
    exit 1
  fi
  PROFILE_ARGS=(--profile playground)
fi

if [[ -z "$POSTGRES_PASSWORD" || "$POSTGRES_PASSWORD" == REPLACE_WITH_* ]]; then
  echo "Refusing to start with the placeholder POSTGRES_PASSWORD in .env." >&2
  exit 1
fi

docker compose \
  --env-file .env \
  --file compose.airgap.yaml \
  "${PROFILE_ARGS[@]}" \
  config --quiet

docker compose \
  --env-file .env \
  --file compose.airgap.yaml \
  "${PROFILE_ARGS[@]}" \
  up --detach --pull never

docker compose \
  --env-file .env \
  --file compose.airgap.yaml \
  "${PROFILE_ARGS[@]}" \
  ps
