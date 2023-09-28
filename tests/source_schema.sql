-- create schema and load data in source
create table t1(time timestamptz not null, key int not null, value float);

select create_hypertable('t1', 'time', chunk_time_interval => interval '1 day');

-- One cagg with timestamptz
create materialized view "caggs""_T1"
  with (timescaledb.continuous) as
  select
    time_bucket('1 day', "time") as day,
    key,
    max(value) as high
  from t1
  group by day, key;

-- A heriarchical cagg over the previous
create materialized view caggs_t1_2
  with (timescaledb.continuous) as
  select
    time_bucket('1 month', "day") as month,
    key,
    max(high) as high
  from "caggs""_T1"
  group by month, key;

-- A heriarchical cagg over the previous
create materialized view caggs_t1_3
  with (timescaledb.continuous) as
  select
    time_bucket('2 month', "month") as bimonth,
    key,
    max(high) as high
  from "caggs_t1_2"
  group by bimonth, key;

insert into t1 values ('2023-09-06 19:27:30.024001+02', 1, 1);

call refresh_continuous_aggregate('"caggs""_T1"', null, '2023-09-07 19:27:30.024001+02');
call refresh_continuous_aggregate('caggs_t1_2', null, '2023-10-30 02:00:00+02');
call refresh_continuous_aggregate('caggs_t1_3', null, '2023-12-01 02:00:00+02');

create table t2(
    time bigint not null,
    key int not null,
    value float
);

select create_hypertable('t2', 'time', chunk_time_interval => 10);

create function latest_time_t2()
returns bigint
language sql
stable
as $$
SELECT max(time) from t2;$$
;

select set_integer_now_func('t2', 'latest_time_t2');

-- Cagg with time dimension as integer
create materialized view caggs_t2
  with (timescaledb.continuous) as
  select
    time_bucket(10, "time") as day,
    key,
    max(value) as high
  from t2
  group by day, key;

insert into t2 values (1, 1, 1);

call refresh_continuous_aggregate('caggs_t2', 0, 10);
