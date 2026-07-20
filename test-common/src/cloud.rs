use std::io::Write;
use tempfile::NamedTempFile;

use crate::psql::{psql, PsqlError, PsqlInput};
use crate::test_connection_string::HasConnectionString;

const CLOUD_INIT: &[u8] = include_bytes!("cloud_init.sql");

/// Timescale cloud has special configuration which restricts which actions can
/// be performed in the database instance. This function performs the following
/// actions:
/// - Creates the `tsdbadmin` role
/// - Creates the `tsdb` database, with owner `tsdbadmin`
/// - Applies most (?) of the restrictions which Timescale cloud does
///   Note: it's somewhat non-trivial to know exactly which restrictions are
///   applied. We cherry-picked these from: https://github.com/timescale/timescaledb-operator/blob/6b99a24ff1d72751249e4238db54b84e54e351a3/operator/pkg/options/scripts/after-create.sql
pub fn configure_cloud_setup<C: HasConnectionString>(container: &C) -> Result<(), PsqlError> {
    let mut script_file = NamedTempFile::new()?;
    script_file.write_all(CLOUD_INIT)?;
    let path = script_file.path();
    psql(&container, PsqlInput::File(path))?;
    Ok(())
}
