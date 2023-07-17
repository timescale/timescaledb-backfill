update __backfill.task
set worked = tstzrange(now(), clock_timestamp(), '[)')
where priority = $1
;