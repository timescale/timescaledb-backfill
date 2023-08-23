select
  priority
, chunk_schema
, chunk_name
, hypertable_schema
, hypertable_name
, dimensions
, filter
, snapshot
from __backfill.task
where worked is not null and verified is null
order by priority
limit 1
for no key update
skip locked
;
