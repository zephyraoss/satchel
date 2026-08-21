#!/usr/bin/env sh
set -eu
PLUGIN="${PLUGIN:-zephyraoss/satchel}"
TAG="${TAG:-dev}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

docker build -f "$ROOT/deploy/plugin/Dockerfile" -t satchel-rootfs:"$TAG" "$ROOT"
cid="$(docker create satchel-rootfs:"$TAG" true)"
mkdir -p "$WORK/rootfs"
docker export "$cid" | tar -x -C "$WORK/rootfs"
docker rm -f "$cid" >/dev/null
cp "$ROOT/deploy/plugin/config.json" "$WORK/config.json"

docker plugin rm -f "$PLUGIN:$TAG" 2>/dev/null || true
docker plugin create "$PLUGIN:$TAG" "$WORK"
echo "created plugin $PLUGIN:$TAG"
echo "enable with: docker plugin set $PLUGIN:$TAG SATCHEL_S3_ENDPOINT=... SATCHEL_S3_BUCKET=... SATCHEL_S3_ACCESS_KEY=... SATCHEL_S3_SECRET_KEY=... && docker plugin enable $PLUGIN:$TAG"
