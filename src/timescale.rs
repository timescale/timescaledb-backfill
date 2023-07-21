use crate::sql::quote_table_name;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;

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
    /// by the `_timescaledb_internal.create_chunk` function.
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
