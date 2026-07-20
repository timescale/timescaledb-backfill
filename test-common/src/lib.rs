use postgres::{Config, NoTls};
use std::str::FromStr;

mod assert_within;
mod cloud;
mod db_assert;
mod json_assert;
mod psql;
mod test_connection_string;
mod timescale_docker;

pub use crate::cloud::*;
pub use crate::db_assert::*;
pub use crate::json_assert::*;
pub use crate::psql::*;
pub use crate::test_connection_string::*;
pub use crate::timescale_docker::*;

pub fn get_ts_version(dsn: &TestConnectionString) -> Result<String, tokio_postgres::Error> {
    let config = Config::from_str(dsn.connection_string().as_str())?;
    let mut client = config.connect(NoTls)?;
    let row = client.query_one(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'",
        &[],
    )?;
    Ok(row.get(0))
}

/// Whether `_timescaledb_catalog.chunk` still has the `dropped` column. It was
/// removed in TimescaleDB 2.26. Detecting the column is more robust than a
/// version check, which is awkward for nightly prerelease versions.
pub fn chunk_has_dropped_column(dsn: &TestConnectionString) -> Result<bool, tokio_postgres::Error> {
    let config = Config::from_str(dsn.connection_string().as_str())?;
    let mut client = config.connect(NoTls)?;
    let row = client.query_one(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = '_timescaledb_catalog'
              AND table_name = 'chunk'
              AND column_name = 'dropped'
        )",
        &[],
    )?;
    Ok(row.get(0))
}
