create schema __backfill;

-- a list of source chunks to backfill
create table __backfill.task
( priority bigint not null generated by default as identity primary key
, worked tstzrange
, copy_message text
, verified tstzrange
, verify_message text
, chunk_schema name not null
, chunk_name name not null
, hypertable_schema name not null
, hypertable_name name not null
, dimensions jsonb not null
, filter text -- e.g. `where "time" < '2023-01-06'::timestamptz`
, snapshot text
);

create unique index on __backfill.task (priority) where (worked is null);
create unique index on __backfill.task (chunk_schema, chunk_name);
create unique index on __backfill.task (priority) where (verified is null and worked is not null);
