#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
compose_file=$(pwd)/docker-compose.yml
trap 'docker compose -f "$compose_file" down -v' EXIT
docker compose -f "$compose_file" up -d --wait minio
docker compose -f "$compose_file" run --rm createbucket
cd ../..
SATCHEL_E2E_S3_ENDPOINT=http://127.0.0.1:9000 go test ./test/e2e/ -count=1 -v "$@"
