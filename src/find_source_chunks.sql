/*
$1 is a case insensitive posix regular expression filtering on hypertable schema.table
$2 is a string that represents the upper bound on "time" dimension values

TimescaleDB 2.12 changed the schema where internal functions are installed from
_timescaledb_internal to _timescaledb_functions, to support both we add the
@extschema@ placeholder and replace it before running the query.
*/
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
  )::TEXT as dimensions
, case when ds.range_start <= d.filter_value and d.filter_value < ds.range_end
    then format('%I <= %s', d.column_name, d.filter_literal)
  end as filter
from
(
    select
      h.id
    , h.schema_name
    , h.table_name
    , h.num_dimensions
    from _timescaledb_catalog.hypertable h
    where $1::text is null or format('%I.%I', h.schema_name, h.table_name) ~* $1::text
) h
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
      end as filter_value
    , case
        when d.column_type = 'timestamp'::regtype then
            format('%L', $2::text::timestamp)
        when d.column_type = 'timestamptz'::regtype then
            format('%L', $2::text::timestamptz)
        when d.column_type = 'date'::regtype then
            format('%L', $2::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) and $2::text ~ '^[0-9]+$' then
            format('%L', cast($2::text as bigint))
      end as filter_literal
    from _timescaledb_catalog.dimension d
    where d.hypertable_id = h.id
    order by d.id
    limit 1
) d on (true)
inner join _timescaledb_catalog.dimension_slice ds
on (d.id = ds.dimension_id and ds.range_start <= d.filter_value)
inner join _timescaledb_catalog.chunk_constraint cc on (ds.id = cc.dimension_slice_id)
inner join _timescaledb_catalog.chunk c on (cc.chunk_id = c.id and h.id = c.hypertable_id)
where c.dropped = false
order by ds.range_start desc
;
