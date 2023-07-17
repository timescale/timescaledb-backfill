use crate::connect::Source;
use crate::timescale::{Chunk, CompressionState, Dimension, Hypertable};
use anyhow::Result;
use chrono::{DateTime, Utc};
use multimap::MultiMap;

// TODO: handle time dimensions other than timestamptz
static CHUNK_INFORMATION_QUERY: &str = r#"
WITH chunks AS (
    SELECT
        h.schema_name AS hypertable_schema,
        h.table_name AS hypertable_name,
        ch.schema_name AS chunk_schema,
        ch.table_name AS chunk_name,
        jsonb_agg(
            jsonb_build_object(
                'column_name',
                di.column_name,
                'column_type',
                di.column_type,
                'range_start',
                ds.range_start,
                'range_end',
                ds.range_end
            ) ORDER BY di.id -- Sort by ID because the lowest ID is the time dimension.
        ) AS dimensions
    FROM
        _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.chunk_constraint cons ON cons.chunk_id = ch.id
    JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cons.dimension_slice_id
    JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id
    JOIN _timescaledb_catalog.dimension di ON h.id = di.hypertable_id AND di.id = ds.dimension_id
    GROUP BY 1, 2, 3, 4
)
SELECT
    *,
    _timescaledb_internal.to_timestamp((dimensions->0->'range_start')::bigint)::timestamptz < $1
        AND _timescaledb_internal.to_timestamp((dimensions->0->'range_end')::bigint)::timestamptz > $1 AS active_chunk
FROM
    chunks
WHERE -- Dimensions are sorted by ID and the lowest ID is the time dimension.
    _timescaledb_internal.to_timestamp((dimensions->0->'range_start')::bigint)::timestamptz < $1;
"#;

static HYPERTABLE_INFORMATION_QUERY: &str = r#"
    SELECT
        h.id as id
      , h.schema_name as schema
      , h.table_name as table
      , h.compression_state as compression_state
    FROM _timescaledb_catalog.hypertable h;
"#;

static HYPERTABLE_DIMENSION_QUERY: &str = r"
    SELECT
        d.hypertable_id as hypertable_id
      , d.column_name as column_name
      , d.column_type::text as column_type
    FROM _timescaledb_catalog.dimension d;
";

pub async fn get_chunk_information(
    source: &mut Source,
    until: &DateTime<Utc>,
) -> Result<Vec<Chunk>> {
    let rows = source
        .transaction()
        .await?
        .query(CHUNK_INFORMATION_QUERY, &[until])
        .await?;
    rows.iter()
        .map(|r| {
            Ok(Chunk {
                hypertable_schema: r.get("hypertable_schema"),
                hypertable_name: r.get("hypertable_name"),
                chunk_schema: r.get("chunk_schema"),
                chunk_name: r.get("chunk_name"),
                dimensions: serde_json::from_value(r.get("dimensions"))?,
                active_chunk: r.get("active_chunk"),
            })
        })
        .collect()
}

pub async fn get_hypertable_information(source: &mut Source) -> Result<Vec<Hypertable>> {
    let tx = source.transaction().await?;
    let hypertable_rows = tx.query(HYPERTABLE_INFORMATION_QUERY, &[]).await?;
    let dimension_rows = tx.query(HYPERTABLE_DIMENSION_QUERY, &[]).await?;
    let mut hts = Vec::with_capacity(hypertable_rows.len());
    let mut dimension_by_ht_id = MultiMap::new();
    for dimension_row in dimension_rows {
        let ht_id: i32 = dimension_row.get("hypertable_id");
        let dimension = Dimension {
            column_name: dimension_row.get("column_name"),
            column_type: dimension_row.get("column_type"),
        };
        dimension_by_ht_id.insert(ht_id, dimension);
    }
    for hypertable_row in hypertable_rows {
        let ht_id: i32 = hypertable_row.get("id");
        let hypertable = Hypertable {
            schema: hypertable_row.get("schema"),
            table: hypertable_row.get("table"),
            dimensions: dimension_by_ht_id.remove(&ht_id).unwrap_or_default(),
            compression_state: CompressionState::try_from(
                hypertable_row.get::<'_, _, i16>("compression_state"),
            )?,
        };
        hts.push(hypertable);
    }
    Ok(hts)
}
