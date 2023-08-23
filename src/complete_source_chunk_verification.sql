update __backfill.task
set verified = tstzrange(now(), clock_timestamp(), '[)'), verify_message = $2
where priority = $1
;
