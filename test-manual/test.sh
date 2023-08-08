#!/usr/bin/env bash

UNTIL='2023-08-28 15:30:00+00'
SOURCE=postgres://postgres@localhost:6506/source
TARGET=postgres://postgres@localhost:6599/target

rm dump.sql
rm source.out
rm target.out

docker stop target
docker rm target

set -e

docker pull --platform linux/amd64 timescale/timescaledb-ha:pg15-ts2.11

echo creating target database...
docker run -d \
  --name target \
  --platform linux/amd64 \
  --memory=2GB --memory-swap=4GB --shm-size=1gb \
  -v "$(pwd)/alter.sql":/docker-entrypoint-initdb.d/099-alter-system.sql \
  -p 6599:5432 \
  -e POSTGRES_DB=target \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  timescale/timescaledb-ha:pg15-ts2.11

sleep 10

until pg_isready -d "$SOURCE" ; do sleep 1 ; done
until pg_isready -d "$TARGET" ; do sleep 1 ; done

echo dumping schema...
pg_dump -d "$SOURCE" \
  --format=plain \
  --quote-all-identifiers \
  --no-tablespaces \
  --no-owner \
  --no-privileges \
  --disable-triggers \
  --exclude-table-data='_timescaledb_internal.*' \
  --file=dump.sql

echo restoring schema...
psql -d "$TARGET" -v ON_ERROR_STOP=1 --echo-errors \
  -c 'select public.timescaledb_pre_restore();' \
  -f dump.sql \
  -f - <<'EOF'
begin;
select public.timescaledb_post_restore();

-- disable all background jobs
select public.alter_job(id::integer, scheduled=>false)
from _timescaledb_config.bgw_job
where id >= 1000
;
commit;
EOF

echo staging...
../target/release/timescaledb-backfill stage --source="$SOURCE" --target="$TARGET" --until="$UNTIL"

echo copying...
../target/release/timescaledb-backfill copy --source="$SOURCE" --target="$TARGET"

psql -d "$TARGET" -v ON_ERROR_STOP=1 --echo-errors -c 'select public.timescaledb_post_restore();'

echo dumping stats...
psql -d "$SOURCE" -o source.out -f - <<'EOF'
\pset title metric_1
select date_trunc('day', time), min(time), max(time), count(*), sum(val2) from metric_1 group by 1 order by 1;

\pset title metric_1_6h
select date_trunc('day', time), min(time), max(time), count(*), sum(sum_val2) from metric_1_6h group by 1 order by 1;

\pset title metric_1_24h
select date_trunc('day', time), min(time), max(time), count(*), sum(sum_val2) from metric_1_24h group by 1 order by 1;

\pset title chunks
select hypertable_schema, hypertable_name, min(range_start), max(range_end), count(*), count(*) filter (where is_compressed)
from timescaledb_information.chunks group by 1, 2 order by 1, 2;
EOF

psql -d "$TARGET" -o target.out -f - <<'EOF'
\pset title metric_1
select date_trunc('day', time), min(time), max(time), count(*), sum(val2) from metric_1 group by 1 order by 1;

\pset title metric_1_6h
select date_trunc('day', time), min(time), max(time), count(*), sum(sum_val2) from metric_1_6h group by 1 order by 1;

\pset title metric_1_24h
select date_trunc('day', time), min(time), max(time), count(*), sum(sum_val2) from metric_1_24h group by 1 order by 1;

\pset title chunks
select hypertable_schema, hypertable_name, min(range_start), max(range_end), count(*), count(*) filter (where is_compressed)
from timescaledb_information.chunks group by 1, 2 order by 1, 2;
EOF

echo 'DONE!'
