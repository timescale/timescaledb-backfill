use crate::connect::{Source, Target};
use crate::sql::quote_table_name;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt::Debug, sync::OnceLock};
use tokio_postgres::GenericClient;

static SOURCE_PROC_SCHEMA: OnceLock<String> = OnceLock::new();
static TARGET_PROC_SCHEMA: OnceLock<String> = OnceLock::new();
static EXTSCHEMA: &str = "@extschema@";
static FUNCTIONS_SCHEMA: &str = "_timescaledb_functions";
static INTERNAL_SCHEMA: &str = "_timescaledb_internal";

pub async fn initialize_source_proc_schema(source: &mut Source) -> Result<()> {
    let schema = fetch_proc_schema(&source.client).await?;
    // When running the `all` command the variable might already be set. In
    // that case set will return Err(already_set_value) which we can discard.
    let _ = SOURCE_PROC_SCHEMA.set(schema);
    Ok(())
}

pub async fn initialize_target_proc_schema(target: &Target) -> Result<()> {
    let schema = fetch_proc_schema(&target.client).await?;
    // When running the `all` command the variable might already be set. In
    // that case set will return Err(already_set_value) which we can discard.
    let _ = TARGET_PROC_SCHEMA.set(schema);
    Ok(())
}

async fn fetch_proc_schema<T>(client: &T) -> Result<String>
where
    T: GenericClient,
{
    let query = r"
    SELECT exists(
        SELECT 1
        FROM pg_proc
        WHERE
            proname = 'create_chunk' AND
            pronamespace::regnamespace::text = $1
    )
    ";
    let row = client.query_one(query, &[&FUNCTIONS_SCHEMA]).await?;

    if row.get(0) {
        Ok(FUNCTIONS_SCHEMA.into())
    } else {
        Ok(INTERNAL_SCHEMA.into())
    }
}

pub fn set_query_source_proc_schema(query: &str) -> String {
    let schema = SOURCE_PROC_SCHEMA
        .get()
        .expect("source proc schema is not set");
    query.replace(EXTSCHEMA, schema)
}

pub fn set_query_target_proc_schema(query: &str) -> String {
    let schema = TARGET_PROC_SCHEMA
        .get()
        .expect("target proc schema is not set");
    query.replace(EXTSCHEMA, schema)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionRange {
    pub column_name: String,
    pub column_type: String,
    pub range_start: i64,
    pub range_end: i64,
}

#[derive(Debug, Clone)]
pub struct Hypertable {
    pub schema: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub schema: String,
    pub table: String,
    pub hypertable: Hypertable,
    /// The dimensions need to be sorted by ID ASC. The order is important
    /// because it's used to match against other Chunk's dimensions.
    pub dimensions: Vec<DimensionRange>,
}

pub type SourceChunk = Chunk;
pub type TargetChunk = Chunk;

impl Chunk {
    /// Returns the slices (dimensions) of the Chunk, in the format required
    /// by the `_timescaledb_functions.create_chunk` function.
    ///
    /// ```
    /// {
    ///   "time": [100000000, 200000000],
    ///   "device_id": [10, 50]
    ///   ...
    /// }
    /// ```
    pub fn slices(&self) -> Result<String, serde_json::Error> {
        let mut dimensions_json = serde_json::Map::new();
        for dimension in &self.dimensions {
            let column_name = &dimension.column_name;
            let range_start = dimension.range_start;
            let range_end = dimension.range_end;

            let dimension_value =
                Value::Array(vec![Value::from(range_start), Value::from(range_end)]);
            dimensions_json.insert(column_name.clone(), dimension_value);
        }
        serde_json::to_string(&dimensions_json)
    }

    pub fn quoted_name(&self) -> String {
        quote_table_name(&self.schema, &self.table)
    }
}

pub type SourceCompressedChunk = CompressedChunk;
pub type TargetCompressedChunk = CompressedChunk;

pub struct CompressedChunk {
    pub schema: String,
    pub table: String,
}

impl CompressedChunk {
    pub fn quoted_name(&self) -> String {
        quote_table_name(&self.schema, &self.table)
    }
}

#[derive(Debug, Deserialize)]
pub struct CompressionSize {
    pub uncompressed_heap_size: i64,
    pub uncompressed_toast_size: i64,
    pub uncompressed_index_size: i64,
    pub compressed_heap_size: i64,
    pub compressed_toast_size: i64,
    pub compressed_index_size: i64,
    pub numrows_pre_compression: i64,
    pub numrows_post_compression: i64,
}
