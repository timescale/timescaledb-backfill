/*
$1 is a case insensitive posix regular expression filtering on hypertable schema.table
$2 is an optional text value representing an upper bound on "time" dimension values
*/
select
  c.id as chunk_id
, c.schema_name as chunk_schema
, c.table_name as chunk_name
, h.id as hypertable_id
, h.schema_name as hypertable_schema
, h.table_name as hypertable_name
, (
    select json_agg
    (
        json_build_object
        ( 'dimension_id', d2.id
        , 'column_name', d2.column_name
        , 'column_type', d2.column_type
        , 'range_start', ds2.range_start
        , 'range_end', ds2.range_end
        )
        order by d2.id
    )
    from _timescaledb_catalog.chunk_constraint cc2
    inner join _timescaledb_catalog.dimension_slice ds2 on (cc2.chunk_id = c.id and cc2.dimension_slice_id = ds2.id)
    inner join _timescaledb_catalog.dimension d2 on (ds2.dimension_id = d2.id and d2.hypertable_id = h.id)
  ) as dimensions
, case when $2::text is not null and ds.range_start <= d.filter_value and d.filter_value < ds.range_end
    then format('where %I <= %s', d.column_name, d.filter_literal)
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
        when d.column_type = 'tstzrange'::regtype then
            _timescaledb_internal.time_to_internal($2::text::timestamptz)
        when d.column_type = 'timestamp'::regtype then
            _timescaledb_internal.time_to_internal($2::text::timestamp)
        when d.column_type = 'date'::regtype then
            _timescaledb_internal.time_to_internal($2::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) then
            $2::text::bigint
      end as filter_value
    , case
        when d.column_type = 'tstzrange'::regtype then
            format('%L', $2::text::timestamptz)
        when d.column_type = 'timestamp'::regtype then
            format('%L', $2::text::timestamp)
        when d.column_type = 'date'::regtype then
            format('%L', $2::text::date)
        when d.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) then
            format('%L', $2::text::bigint)
      end as filter_literal
    from _timescaledb_catalog.dimension d
    where d.hypertable_id = h.id
    order by d.id
    limit 1
) d on (true)
inner join _timescaledb_catalog.dimension_slice ds
on (d.id = ds.dimension_id and ($2::text is null or ds.range_start <= d.filter_value))
inner join _timescaledb_catalog.chunk_constraint cc on (ds.id = cc.dimension_slice_id)
inner join _timescaledb_catalog.chunk c on (cc.chunk_id = c.id and h.id = c.hypertable_id)
order by ds.range_start desc
;