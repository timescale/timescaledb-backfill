insert into __backfill.task
( chunk_schema
, chunk_name
, hypertable_schema
, hypertable_name
, dimensions
, filter
, snapshot
)
values
( $1
, $2
, $3
, $4
, $5::text::jsonb
, $6
, $7
)
on conflict do nothing
returning true as inserted
