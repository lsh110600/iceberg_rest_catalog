#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${1:-0.1.0}"
OUTPUT_DIR="${2:-$ROOT_DIR/dist}"
REQUESTED_ARCH="${3:-$(uname -m)}"
INCLUDE_PLAYGROUND="${INCLUDE_PLAYGROUND:-false}"

case "$VERSION" in
  ""|*[!A-Za-z0-9._-]*)
    echo "Version may contain only letters, numbers, dots, underscores, and hyphens." >&2
    exit 1
    ;;
esac

case "$REQUESTED_ARCH" in
  x86_64|amd64) TARGET_ARCH=amd64 ;;
  arm64|aarch64) TARGET_ARCH=arm64 ;;
  *)
    echo "Unsupported architecture: $REQUESTED_ARCH (use amd64 or arm64)." >&2
    exit 1
    ;;
esac

if [[ "$INCLUDE_PLAYGROUND" != "true" && "$INCLUDE_PLAYGROUND" != "false" ]]; then
  echo "INCLUDE_PLAYGROUND must be true or false." >&2
  exit 1
fi

command -v docker >/dev/null 2>&1 || {
  echo "Docker is required to build the air-gapped bundle." >&2
  exit 1
}

checksum_files() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$@"
  else
    echo "sha256sum or shasum is required." >&2
    exit 1
  fi
}

mkdir -p "$OUTPUT_DIR"
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/iceberg-ops-airgap.XXXXXX")"
trap 'rm -rf "$STAGING_DIR"' EXIT

BUNDLE_NAME="iceberg-ops-airgap-${VERSION}-linux-${TARGET_ARCH}"
BUNDLE_DIR="$STAGING_DIR/$BUNDLE_NAME"
mkdir -p "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR/spark-jobs"

BACKEND_IMAGE="iceberg-ops/backend:$VERSION"
FRONTEND_IMAGE="iceberg-ops/frontend:$VERSION"
POSTGRES_SOURCE_IMAGE="postgres:18.6-alpine"
POSTGRES_BUNDLE_IMAGE="iceberg-ops/postgres:18.6"
PLAYGROUND_IMAGE="iceberg-ops/playground-spark:$VERSION"
PLAYGROUND_LIVY_IMAGE="iceberg-ops/playground-livy:$VERSION"
HADOOP_SOURCE_IMAGE="ghcr.io/apache/hadoop:3.5.0"
HADOOP_BUNDLE_IMAGE="iceberg-ops/hadoop:3.5.0"
PLATFORM="linux/$TARGET_ARCH"
IMAGES=("$BACKEND_IMAGE" "$FRONTEND_IMAGE" "$POSTGRES_BUNDLE_IMAGE")

echo "Building $BACKEND_IMAGE for $PLATFORM"
docker build --platform "$PLATFORM" --tag "$BACKEND_IMAGE" "$ROOT_DIR/backend"

echo "Building $FRONTEND_IMAGE for $PLATFORM"
docker build --platform "$PLATFORM" --tag "$FRONTEND_IMAGE" "$ROOT_DIR/frontend"

echo "Fetching PostgreSQL runtime image for $PLATFORM"
docker pull --platform "$PLATFORM" "$POSTGRES_SOURCE_IMAGE"
docker tag "$POSTGRES_SOURCE_IMAGE" "$POSTGRES_BUNDLE_IMAGE"

if [[ "$INCLUDE_PLAYGROUND" == "true" ]]; then
  echo "Building $PLAYGROUND_IMAGE for $PLATFORM"
  docker build --platform "$PLATFORM" --tag "$PLAYGROUND_IMAGE" "$ROOT_DIR/playground/spark"

  echo "Building $PLAYGROUND_LIVY_IMAGE for $PLATFORM"
  docker build --platform "$PLATFORM" --file "$ROOT_DIR/playground/livy/Dockerfile" \
    --tag "$PLAYGROUND_LIVY_IMAGE" "$ROOT_DIR"

  echo "Fetching Hadoop runtime image for $PLATFORM"
  docker pull --platform "$PLATFORM" "$HADOOP_SOURCE_IMAGE"
  docker tag "$HADOOP_SOURCE_IMAGE" "$HADOOP_BUNDLE_IMAGE"
  IMAGES+=("$PLAYGROUND_IMAGE" "$PLAYGROUND_LIVY_IMAGE" "$HADOOP_BUNDLE_IMAGE")
fi

docker image save --output "$BUNDLE_DIR/images.tar" "${IMAGES[@]}"

cp "$ROOT_DIR/deploy/compose.airgap.yaml" "$BUNDLE_DIR/compose.airgap.yaml"
cp "$ROOT_DIR/deploy/.env.airgap.example" "$BUNDLE_DIR/.env.airgap.example"
cp "$ROOT_DIR/deploy/install.sh" "$BUNDLE_DIR/install.sh"
cp "$ROOT_DIR/AIR_GAPPED.md" "$BUNDLE_DIR/AIR_GAPPED.md"
cp "$ROOT_DIR/spark-jobs/iceberg_maintenance.py" "$BUNDLE_DIR/spark-jobs/iceberg_maintenance.py"
chmod +x "$BUNDLE_DIR/install.sh"
awk -v version="$VERSION" '
  /^APP_VERSION=/ { print "APP_VERSION=" version; next }
  { print }
' "$BUNDLE_DIR/.env.airgap.example" > "$BUNDLE_DIR/.env.airgap.example.tmp"
mv "$BUNDLE_DIR/.env.airgap.example.tmp" "$BUNDLE_DIR/.env.airgap.example"

{
  echo "bundle=$BUNDLE_NAME"
  echo "application_version=$VERSION"
  echo "target_platform=$PLATFORM"
  echo "playground_included=$INCLUDE_PLAYGROUND"
  echo "created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  docker image inspect \
    --format 'image={{index .RepoTags 0}} id={{.Id}} os={{.Os}} architecture={{.Architecture}}' \
    "${IMAGES[@]}"
} > "$BUNDLE_DIR/MANIFEST.txt"

(
  cd "$BUNDLE_DIR"
  checksum_files images.tar compose.airgap.yaml .env.airgap.example install.sh AIR_GAPPED.md \
    MANIFEST.txt spark-jobs/iceberg_maintenance.py > SHA256SUMS
)

ARCHIVE_PATH="$OUTPUT_DIR/$BUNDLE_NAME.tar.gz"
tar --create --gzip --file "$ARCHIVE_PATH" --directory "$STAGING_DIR" "$BUNDLE_NAME"
(
  cd "$OUTPUT_DIR"
  checksum_files "$BUNDLE_NAME.tar.gz" > "$BUNDLE_NAME.tar.gz.sha256"
)

echo "Created: $ARCHIVE_PATH"
echo "Checksum: $ARCHIVE_PATH.sha256"
