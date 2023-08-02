#!/usr/bin/env bash

SOURCE=postgres://postgres@localhost:6506/source

docker stop source
docker rm source

set -e

docker pull --platform linux/amd64 timescale/timescaledb-ha:pg15-ts2.11

docker run -d \
  --name source \
  --platform linux/amd64 \
  --memory=2GB --memory-swap=4GB --shm-size=1gb \
  -v "$(pwd)/alter.sql":/docker-entrypoint-initdb.d/099-alter-system.sql \
  -p 6506:5432 \
  -e POSTGRES_DB=source \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  timescale/timescaledb-ha:pg15-ts2.11

sleep 10

until pg_isready -d "$SOURCE" ; do sleep 1 ; done

psql -d "$SOURCE" -v ON_ERROR_STOP=1 --echo-errors -f source.sql

echo 'DONE!'
