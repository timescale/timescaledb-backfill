select
  priority
, worked
, chunk_schema
, chunk_name
, hypertable_schema
, hypertable_name
, target_hypertable_dimensions target_dimensions
, dimension_slices
, filter
, snapshot
from __backfill.task
where worked is null
order by priority
limit 1
for no key update
skip locked
;
