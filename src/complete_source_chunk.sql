update __backfill.task
set worked = tstzrange(now(), clock_timestamp(), '[)'), copy_message = $2
where priority = $1
;
