use anyhow::Result;
use postgres::{Config, NoTls};
use std::str::FromStr;

mod assert_within;
mod db_assert;
mod json_assert;
mod psql;
mod test_connection_string;
mod timescale_docker;

pub use crate::assert_within::*;
pub use crate::db_assert::*;
pub use crate::json_assert::*;
pub use crate::psql::*;
pub use crate::test_connection_string::*;
pub use crate::timescale_docker::*;

pub fn get_ts_version(dsn: &TestConnectionString) -> Result<String> {
    let config = Config::from_str(dsn.connection_string().as_str())?;
    let mut client = config.connect(NoTls)?;
    let row = client.query_one(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'",
        &[],
    )?;
    Ok(row.get(0))
}
