#!/usr/bin/env bash
set -euo pipefail

dsn="${1:?usage: record_postgres_versions.sh POSTGRES_DSN}"

docker run --rm --network host postgres:16-alpine sh -c '
  set -eu
  psql --version
  psql "$1" -Atc "select version()"
' sh "$dsn"
