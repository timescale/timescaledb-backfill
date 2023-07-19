use crate::timescale::CompressionState::{CompressedHypertable, CompressionOff, CompressionOn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct DimensionRange {
    pub column_name: String,
    pub column_type: String,
    pub range_start: i64,
    pub range_end: i64,
}

#[derive(Debug)]
pub struct Chunk {
    pub hypertable_schema: String,
    pub hypertable_name: String,
    pub chunk_schema: String,
    pub chunk_name: String,
    /// The dimensions need to be sorted by ID ASC. The order is important
    /// because it's used to match against other Chunk's dimensions.
    pub dimensions: Vec<DimensionRange>,
    pub active_chunk: bool,
}

impl Chunk {
    /// Returns the dimensions of the Chunk as a JSON string with the format:
    ///
    /// ```
    /// [{
    ///     "column_name": "time",
    ///     "column_type": "timestamp with timezone",
    ///     "range_start": 100000000,
    ///     "range_end": 200000000,
    /// },{
    ///     "column_name": "device_id",
    ///     "column_type": "bigint",
    ///     "range_start": 10,
    ///     "range_end": 50,
    /// },...]
    /// ```
    ///
    /// The dimensions are expected to be a list sorted by dimension ID. The
    /// purpose is to be able to use the list of dimensions from a source
    /// Chunk and match them against a target Chunk.
    pub fn dimension_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.dimensions)
    }

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
        quote_table_name(&self.chunk_schema, &self.chunk_name)
    }
}

pub struct CompressedChunk {
    pub chunk_schema: String,
    pub chunk_name: String,
}

impl CompressedChunk {
    pub fn quoted_name(&self) -> String {
        quote_table_name(&self.chunk_schema, &self.chunk_name)
    }
}

pub fn quote_table_name(schema_name: &str, table_name: &str) -> String {
    format!("{}.{}", quote_ident(schema_name), quote_ident(table_name))
}

/// `quote_ident` quotes the given value as an identifier (table, schema) safely for use in a `simple_query` call.
/// Implementation matches that of `quote_identifier` in ruleutils.c of the `PostgreSQL` code,
/// with `quote_all_identifiers` = true.
pub fn quote_ident(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 4);
    result.push('"');
    for c in value.chars() {
        if c == '"' {
            result.push(c);
        }
        result.push(c);
    }
    result.push('"');
    result
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
