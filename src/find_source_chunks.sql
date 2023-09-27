/*
$1 is a case insensitive posix regular expression filtering on hypertable schema.table
$2 is a string that represents the upper bound on "time" dimension values
$3 is a string that represents the lower bound on "time" dimension values
$4 is a bool controlling whether or not filter matches cascade up the cagg hierarchy
$5 is a bool controlling whether or not filter matches cascade down the cagg hierarchy

TimescaleDB 2.12 changed the schema where internal functions are installed from
_timescaledb_internal to _timescaledb_functions, to support both we add the
@extschema@ placeholder and replace it before running the query.
*/
with recursive f as
(
    select -- all hypertables OR hypertables that match the filter
      h.id
    , h.schema_name
    , h.table_name
    , h.num_dimensions
    from _timescaledb_catalog.hypertable h
    where $1::text is null or format('%I.%I', h.schema_name, h.table_name) ~* $1::text
    union
    select -- if filter provided, materialized hypertables for caggs matching the filter
      h.id
    , h.schema_name
    , h.table_name
    , h.num_dimensions
    from _timescaledb_catalog.continuous_agg c
    inner join _timescaledb_catalog.hypertable h on (c.mat_hypertable_id = h.id)
    where $1::text is not null and format('%I.%I', c.user_view_schema, c.user_view_name) ~* $1::text
)
, up as
(
    select
      f.id
    , f.schema_name
    , f.table_name
    , f.num_dimensions
    , c.mat_hypertable_id
    from f
    inner join _timescaledb_catalog.continuous_agg c on (f.id = c.raw_hypertable_id)
    where $4::bool -- cascade up?
    union all
    select
      h.id
    , h.schema_name
    , h.table_name
    , h.num_dimensions
    , c.mat_hypertable_id
    from up
    inner join _timescaledb_catalog.hypertable h on (h.id = up.mat_hypertable_id)
    left outer join _timescaledb_catalog.continuous_agg c on (h.id = c.raw_hypertable_id)
)
, down as
(
    select
      f.id
    , f.schema_name
    , f.table_name
    , f.num_dimensions
    , c.raw_hypertable_id
    from f
    inner join _timescaledb_catalog.continuous_agg c on (f.id = c.mat_hypertable_id)
    where $5::bool -- cascade down ?
    union all
    select
      h.id
    , h.schema_name
    , h.table_name
    , h.num_dimensions
    , c.raw_hypertable_id
    from down
    inner join _timescaledb_catalog.hypertable h on (h.id = down.raw_hypertable_id)
    left outer join _timescaledb_catalog.continuous_agg c on (h.id = c.mat_hypertable_id)
)
, h as -- final distinct list of hypertables
(
    select
      id
    , schema_name
    , table_name
    , num_dimensions
    from f
    union
    select
      id
    , schema_name
    , table_name
    , num_dimensions
    from up
    union
    select
      id
    , schema_name
    , table_name
    , num_dimensions
    from down
)
select
  c.schema_name as chunk_schema
, c.table_name as chunk_name
, h.schema_name as hypertable_schema
, h.table_name as hypertable_name
, (
    select json_agg
    (
        json_build_object
        ( 'column_name', d2.column_name
        , 'column_type', d2.column_type
        , 'range_start', ds2.range_start
        , 'range_end', ds2.range_end
        )
        order by d2.id
    )
    from _timescaledb_catalog.chunk_constraint cc2
    inner join _timescaledb_catalog.dimension_slice ds2 on (cc2.chunk_id = c.id and cc2.dimension_slice_id = ds2.id)
    inner join _timescaledb_catalog.dimension d2 on (ds2.dimension_id = d2.id and d2.hypertable_id = h.id)
  )::text as dimensions
, nullif(
    concat_ws
    ( ' and '
    , case when ds.range_start < d.upper_value and d.upper_value < ds.range_end
          then format('%I < %s', d.column_name, d.upper_literal)
          else null
      end
    , case when d.lower_value is not null and ds.range_start <= d.lower_value and d.lower_value < ds.range_end
          then format('%I >= %s', d.column_name, d.lower_literal)
          else null
      end
    ),
    ''
  ) as filter
from h
inner join lateral
(
    select
      d.id
    , d.column_name
    , d.column_type
    , case
        when d.column_type = 'timestamp'::regtype then
            @extschema@.time_to_internal($2::text::timestamp)
        when d.column_type = 'timestamptz'::regtype then
            @extschema@.time_to_internal($2::text::timestamptz)
        when d.column_type = 'date'::regtype then
            @extschema@.time_to_internal($2::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) and $2::text ~ '^[0-9]+$' then
            @extschema@.time_to_internal(cast($2::text as bigint))
      end as upper_value
    , case
        when d.column_type = 'timestamp'::regtype then
            format('%L', $2::text::timestamp)
        when d.column_type = 'timestamptz'::regtype then
            format('%L', $2::text::timestamptz)
        when d.column_type = 'date'::regtype then
            format('%L', $2::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) and $2::text ~ '^[0-9]+$' then
            format('%L', cast($2::text as bigint))
      end as upper_literal
    , case
        when d.column_type = 'timestamp'::regtype then
            @extschema@.time_to_internal($3::text::timestamp)
        when d.column_type = 'timestamptz'::regtype then
            @extschema@.time_to_internal($3::text::timestamptz)
        when d.column_type = 'date'::regtype then
            @extschema@.time_to_internal($3::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) and $3::text ~ '^[0-9]+$' then
            @extschema@.time_to_internal(cast($3::text as bigint))
      end as lower_value
    , case
        when d.column_type = 'timestamp'::regtype then
            format('%L', $3::text::timestamp)
        when d.column_type = 'timestamptz'::regtype then
            format('%L', $3::text::timestamptz)
        when d.column_type = 'date'::regtype then
            format('%L', $3::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) and $3::text ~ '^[0-9]+$' then
            format('%L', cast($3::text as bigint))
      end as lower_literal
    from _timescaledb_catalog.dimension d
    where d.hypertable_id = h.id
    order by d.id
    limit 1
) d on (true)
inner join _timescaledb_catalog.dimension_slice ds
on (d.id = ds.dimension_id and ds.range_start < d.upper_value and (d.lower_value is null or d.lower_value <= ds.range_end))
inner join _timescaledb_catalog.chunk_constraint cc on (ds.id = cc.dimension_slice_id)
inner join _timescaledb_catalog.chunk c on (cc.chunk_id = c.id and h.id = c.hypertable_id)
where c.dropped = false
order by ds.range_start desc
;
