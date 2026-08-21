#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
docker compose up -d --wait
trap 'docker compose down -v' EXIT
cd ../..
SATCHEL_E2E_S3_ENDPOINT=http://127.0.0.1:9000 go test ./test/e2e/ -count=1 -v "$@"
