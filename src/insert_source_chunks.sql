INSERT INTO __backfill.task (
  chunk_schema
, chunk_name
, hypertable_schema
, hypertable_name
, source_hypertable_dimensions
, target_hypertable_dimensions
, dimension_slices
, filter
, snapshot
)
SELECT
  $1 as chunk_schema
, $2 as chunk_name
, $3 as hypertable_schema
, $4 as hypertable_name
, $5::text::text[] as source_hypertable_dimensions
, array_agg(d.column_name order by d.id) as target_hypertable_dimensions
, $6::text::jsonb as dimension_slices
, $7 as filter
, $8 as snapshot
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON h.id = d.hypertable_id
WHERE h.schema_name = $3
  AND h.table_name = $4;
