use crate::connect::Source;
use anyhow::Result;
use chrono::{DateTime, Utc};

static CHUNK_INFORMATION_QUERY: &str = r#"
    SELECT
        h.schema_name as hypertable_schema
        , h.table_name as hypertable_name
        , ch.schema_name as chunk_schema
        , ch.table_name as chunk_name
        , ds.range_start as dimension_start
        , ds.range_end as dimension_end
        , _timescaledb_internal.to_timestamp(ds.range_start) < $1 AND _timescaledb_internal.to_timestamp(ds.range_end) > $1 as active_chunk
    FROM
         _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.chunk_constraint cons ON cons.chunk_id = ch.id
    JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cons.dimension_slice_id
    JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id
    WHERE _timescaledb_internal.to_timestamp(ds.range_start) < $1;
"#;

#[derive(Debug)]
pub struct Chunk {
    pub hypertable_schema: String,
    pub hypertable_name: String,
    pub chunk_schema: String,
    pub chunk_name: String,
    pub dimension_start: i64,
    pub dimension_end: i64,
    pub active_chunk: bool,
}

pub async fn get_chunk_information(
    source: &mut Source,
    until: &DateTime<Utc>,
) -> Result<Vec<Chunk>> {
    let rows = source
        .transaction()
        .await?
        .query(CHUNK_INFORMATION_QUERY, &[until])
        .await?;
    Ok(rows
        .iter()
        .map(|r| Chunk {
            hypertable_schema: r.get("hypertable_schema"),
            hypertable_name: r.get("hypertable_name"),
            chunk_schema: r.get("chunk_schema"),
            chunk_name: r.get("chunk_name"),
            dimension_start: r.get("dimension_start"),
            dimension_end: r.get("dimension_end"),
            active_chunk: r.get("active_chunk"),
        })
        .collect())
}
