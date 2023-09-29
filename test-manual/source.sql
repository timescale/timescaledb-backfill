-- Copyright 2023 Timescale, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
create extension if not exists timescaledb;

create view params as
select
  x.var
, x.val
from jsonb_to_recordset($$
[ {"var": "num_metrics", "val": "3"}
, {"var": "start", "val": "2023-08-01"}
, {"var": "end",   "val": "2023-08-31"}
, {"var": "compress_days", "val": "10"}
, {"var": "refresh_days", "val": "20"}
]
$$) x(var text, val text)
;

create table customer
( id bigint not null generated always as identity primary key
, customer_name text not null unique
, created_at timestamptz not null default clock_timestamp()
);

insert into customer (customer_name)
select format('customer_%s_%s', x, md5(random()::text || clock_timestamp()::text))
from generate_series(1, 10) x
;

create table device_type
( id bigint not null generated always as identity primary key
, description text not null
, manufacturer text not null
, created_at timestamptz not null default clock_timestamp()
);

insert into device_type (description, manufacturer)
select
  format('model_%s %s', x, md5(random()::text || clock_timestamp()::text))
, format('manufacturer %s', floor(random() * 3.0))
from generate_series(1, 10) x
;

create table device
( id bigint not null generated always as identity primary key
, customer_id bigint not null references customer (id) on delete cascade
, device_name text not null
, device_type_id bigint not null references device_type (id) on delete cascade
, created_at timestamptz not null default clock_timestamp()
);

insert into device (customer_id, device_name, device_type_id)
select
  c.id
, md5(random()::text || clock_timestamp()::text)
, y.id
from generate_series(1, 10) x
cross join customer c
cross join device_type y
;

create index on device (customer_id);
create index on device (device_type_id);

do $block$
declare
    _m bigint;
    _num_metrics int;
begin
    select val::int into strict _num_metrics from params where var = 'num_metrics';

    for _m in 1.._num_metrics
    loop
        execute format(
        $sql$
        create table metric_%s
        ( device_id bigint not null references device (id) on delete cascade
        , "time" timestamptz not null default clock_timestamp()
        , val0 float8
        , val1 float8
        , val2 int
        , val3 bool
        , val4 smallint
        )
        $sql$
        , _m
        );
        execute format(
        $sql$
        create unique index on metric_%s (device_id, "time")
        $sql$
        , _m
        );
        execute format(
        $sql$
        select create_hypertable('metric_%s', 'time', 'device_id', 4, chunk_time_interval => INTERVAL '1 day');
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_retention_policy('metric_%s', interval '7 years');
        $sql$
        , _m
        );
        execute format(
        $sql$
        alter table metric_%s set (timescaledb.compress, timescaledb.compress_orderby = 'time desc', timescaledb.compress_segmentby = 'device_id')
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_compression_policy('metric_%s', interval '8d')
        $sql$
        , _m
        );
        -- 6h cagg
        execute format(
        $sql$
        create materialized view metric_%s_6h
        with (timescaledb.continuous) as
        select
          time_bucket('6h', "time") as "time"
        , device_id
        , count(*) as nbr_obs
        , min(val0) as min_val0
        , max(val0) as max_val0
        , min(val1) as min_val1
        , max(val1) as max_val1
        , sum(val2) as sum_val2
        , bool_and(val3) as and_val3
        , avg(val4) as avg_val4
        from metric_%s
        group by device_id, time_bucket('6h', "time")
        with no data
        $sql$
        , _m
        , _m
        );
        execute format(
        $sql$
        select set_chunk_time_interval(format('%%I.%%I', materialization_hypertable_schema, materialization_hypertable_name), interval '1d')
        from timescaledb_information.continuous_aggregates
        where view_name = 'metric_%s_6h'
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_continuous_aggregate_policy('metric_%s_6h',
          start_offset => interval '3d',
          end_offset   => interval '1d',
          schedule_interval => interval '30m' + (random() * 5.0 * interval '1m'));
        $sql$
        , _m
        );
        execute format(
        $sql$
        alter materialized view metric_%s_6h set (timescaledb.compress = true);
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_compression_policy('metric_%s_6h', compress_after=>'1 year'::interval);
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_retention_policy('metric_%s_6h', interval '7 years');
        $sql$
        , _m
        );
        -- 24h cagg
        execute format(
        $sql$
        create materialized view metric_%s_24h
        with (timescaledb.continuous) as
        select
          time_bucket('24h', "time") as "time"
        , device_id
        , count(*) as nbr_obs
        , min(min_val0) as min_val0
        , max(max_val0) as max_val0
        , min(min_val1) as min_val1
        , max(max_val1) as max_val1
        , sum(sum_val2) as sum_val2
        , bool_and(and_val3) as and_val3
        , avg(avg_val4) as avg_val4
        from metric_%s_6h
        group by device_id, time_bucket('24h', "time")
        with no data
        $sql$
        , _m
        , _m
        );
        execute format(
        $sql$
        select set_chunk_time_interval(format('%%I.%%I', materialization_hypertable_schema, materialization_hypertable_name), interval '1d')
        from timescaledb_information.continuous_aggregates
        where view_name = 'metric_%s_24h'
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_continuous_aggregate_policy('metric_%s_24h',
          start_offset => interval '3d',
          end_offset   => interval '1d',
          schedule_interval => interval '30m' + (random() * 5.0 * interval '1m'));
        $sql$
        , _m
        );
        execute format(
        $sql$
        alter materialized view metric_%s_24h set (timescaledb.compress = true);
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_compression_policy('metric_%s_24h', compress_after=>'1 year'::interval);
        $sql$
        , _m
        );
        execute format(
        $sql$
        select add_retention_policy('metric_%s_24h', interval '7 years');
        $sql$
        , _m
        );
    end loop;
end;
$block$
;

-- disable the background jobs so we have predictable results
select public.alter_job(id::integer, scheduled=>false)
from _timescaledb_config.bgw_job
where id >= 1000
;

-- mock up a day's worth of data
create table scratch as
select
  d.id as device_id
, m * interval '1m' as "offset"
, (extract(microsecond from clock_timestamp()) * random())::float8 as val0
, (extract(second from clock_timestamp()) * random())::float8 as val1
, (extract(milliseconds from clock_timestamp()) * random())::int as val2
, mod(extract(microsecond from clock_timestamp()) + d.id, 2) = 0 as val3
, (random() * 168)::smallint as val4
from device d
cross join generate_series(0, 1440, 10) m
where m * interval '1m' < interval '1d'
;

-- load the metric tables by copying the scratch table
do $block$
declare
    _rec record;
    _num_metrics int;
    _start timestamptz;
    _end timestamptz;
begin
    select val::int into strict _num_metrics from params where var = 'num_metrics';
    select val::timestamptz into strict _start from params where var = 'start';
    select val::timestamptz into strict _end from params where var = 'end';

    for _rec in
    (
        select
          format('metric_%s', m) as metric
        , d as day
        from generate_series(_start, _end, interval '1d') d
        cross join generate_series(1, _num_metrics) m
    )
    loop
        raise notice '% %', _rec.day, _rec.metric;
        execute format(
        $sql$
        insert into %I (device_id, "time", val0, val1, val2, val3, val4)
        select
          device_id
        , %L::timestamptz + "offset"
        , val0
        , val1
        , val2
        , val3
        , val4
        from scratch
        ;
        $sql$
        , _rec.metric
        , _rec.day
        );
        commit;
    end loop;
end;
$block$
;

-- refresh the caggs
do $block$
declare
    _i int;
    _num_metrics int;
    _start timestamptz;
    _refresh_days int;
begin
    select val::int into strict _num_metrics from params where var = 'num_metrics';
    select val::timestamptz into strict _start from params where var = 'start';
    select val::int into strict _refresh_days from params where var = 'refresh_days';

    for _i in 1.._num_metrics
    loop
        call public.refresh_continuous_aggregate(
            format('public.metric_%s_6h', _i)::regclass,
            _start,
            _start + (interval '1 days' * _refresh_days)
            );
        call public.refresh_continuous_aggregate(
            format('public.metric_%s_24h', _i)::regclass,
            _start,
            _start + (interval '1 days' * _refresh_days)
            );
    end loop;
end;
$block$
;

-- compress some chunks
do $block$
declare
    _rec record;
    _num_metrics int;
    _start timestamptz;
    _compress_days int;
begin
    select val::int into strict _num_metrics from params where var = 'num_metrics';
    select val::timestamptz into strict _start from params where var = 'start';
    select val::int into strict _compress_days from params where var = 'compress_days';

    for _rec in
    (
        select
          h.id as hypertable_id
        , c.id as chunk_id
        , c.schema_name as chunk_schema
        , c.table_name as chunk_table
        , d.id as dimension_id
        , _timescaledb_internal.to_timestamp(ds.range_start) as range_start
        , _timescaledb_internal.to_timestamp(ds.range_end) as range_end
        from _timescaledb_catalog.hypertable h
        inner join lateral
        (
            select *
            from _timescaledb_catalog.dimension d
            where h.id = d.hypertable_id
            order by d.id
            limit 1
        ) d on (true)
        inner join _timescaledb_catalog.chunk c on (h.id = c.hypertable_id)
        inner join _timescaledb_catalog.chunk_constraint cc on (c.id = cc.chunk_id)
        inner join _timescaledb_catalog.dimension_slice ds on (cc.dimension_slice_id = ds.id and ds.dimension_id = d.id)
        where h.compression_state = 1 -- compression enabled
        and c.dropped is false
        and c.compressed_chunk_id is null
        and _timescaledb_internal.to_timestamp(ds.range_start) < _start + (interval '1 day' * _compress_days)
        order by _timescaledb_internal.to_timestamp(ds.range_start)
    )
    loop
        perform compress_chunk(format('%I.%I', _rec.chunk_schema, _rec.chunk_table), if_not_compressed=>true);
    end loop;
end
$block$
;

checkpoint;

vacuum analyze;

select pg_size_pretty(pg_database_size(current_database()));
