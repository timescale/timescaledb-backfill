insert into __backfill.task
( chunk_id
, chunk_schema
, chunk_name
, hypertable_id
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
, $5
, $6
, $7
, $8
, $9
)