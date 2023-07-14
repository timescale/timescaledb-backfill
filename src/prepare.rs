use crate::connect::Source;
use crate::prepare::CompressionState::{CompressedHypertable, CompressionOff, CompressionOn};
use anyhow::Result;
use chrono::{DateTime, Utc};
use multimap::MultiMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

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

#[derive(Debug, PartialEq, Eq)]
pub enum CompressionState {
    CompressionOff,
    CompressionOn,
    CompressedHypertable,
}

#[derive(Debug)]
pub struct InvalidCompressionStatusError;

impl Display for InvalidCompressionStatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for InvalidCompressionStatusError {}

impl TryFrom<i16> for CompressionState {
    type Error = InvalidCompressionStatusError;

    fn try_from(value: i16) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionOff),
            1 => Ok(CompressionOn),
            2 => Ok(CompressedHypertable),
            _ => Err(InvalidCompressionStatusError),
        }
    }
}

#[derive(Debug)]
pub struct Hypertable {
    pub schema: String,
    pub table: String,
    pub dimensions: Vec<Dimension>,
    pub compression_state: CompressionState,
}

#[derive(Debug)]
pub struct Dimension {
    pub column_name: String,
    pub column_type: String,
}

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
