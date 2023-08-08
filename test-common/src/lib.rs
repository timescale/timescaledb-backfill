use anyhow::{Context, Result};
use assert_cmd::prelude::*;
use std::process::{Child, Command, Output, Stdio};

use log::debug;
use std::fmt::{Display, Formatter};
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;

mod assert_within;
mod config;
mod db_assert;
mod json_assert;
pub use crate::assert_within::*;
pub use crate::config::*;
pub use crate::db_assert::*;
pub use crate::json_assert::*;
mod psql;
mod test_connection_string;
pub use crate::assert_within::*;
pub use crate::db_assert::*;
pub use crate::json_assert::*;
pub use crate::psql::*;
pub use crate::test_connection_string::*;

#[allow(dead_code)]
pub enum PgVersion {
    PG11,
    PG12,
    PG13,
    PG14,
    PG15,
}

impl Display for PgVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PgVersion::PG11 => f.write_str("11"),
            PgVersion::PG12 => f.write_str("12"),
            PgVersion::PG13 => f.write_str("13"),
            PgVersion::PG14 => f.write_str("14"),
            PgVersion::PG15 => f.write_str("15"),
        }
    }
}

pub const TIMESCALEDB_IMAGE: &str = "timescale/timescaledb-ha";

/// Prepares a testcontainer image object for a given version of PostgreSQL
pub fn postgres(version: PgVersion) -> GenericImage {
    generic_postgres("postgres", version.to_string().as_str())
}

/// Prepares a testcontainer image object for the latest version of
/// TimescaleDB and a given version of PostgreSQL
pub fn timescaledb(pg_version: PgVersion) -> GenericImage {
    let version_tag = format!("pg{}", pg_version);
    generic_postgres(TIMESCALEDB_IMAGE, version_tag.as_str())
}

/// Prepares a testcontainer image object for a given image name and tag
pub fn generic_postgres(name: &str, tag: &str) -> GenericImage {
    GenericImage::new(name, tag)
        .with_exposed_port(5432)
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
}

/// Spawns a backfill process with the specified test configuration [`TestConfig`],
/// returning the associated [`std::process::Child`]
pub fn spawn_backfill(config: impl TestConfig) -> Result<Child> {
    Command::cargo_bin("timescaledb-backfill")?
        .arg(config.action())
        .args(config.args())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("couldn't spawn timescaledb-backfill")
}

/// Runs backfill with the specified test configuration [`TestConfig`]
/// waits for it to finish and returns its [`std::process::Output`].
pub fn run_backfill(config: impl TestConfig) -> Result<Output> {
    debug!("running backfill");
    let child = spawn_backfill(config).expect("Couldn't launch timescaledb-backfill");

    child.wait_with_output().context("backfill process failed")
}

pub fn copy_skeleton_schema<C: HasConnectionString>(source: C, target: C) -> Result<()> {
    let pg_dump = Command::new("pg_dump")
        .args(["-d", source.connection_string().as_str()])
        .args(["--format", "plain"])
        .args(["--exclude-table-data", "_timescaledb_internal.*"])
        .arg("--quote-all-identifiers")
        .arg("--no-tablespaces")
        .arg("--no-owner")
        .arg("--no-privileges")
        .stdout(Stdio::piped())
        .spawn()?;

    let pg_dump_stdout = pg_dump.stdout.unwrap();

    psql(
        &target,
        PsqlInput::Sql("select public.timescaledb_pre_restore()"),
    )?;

    let restore = Command::new("psql")
        .arg(target.connection_string().as_str())
        .stdin(Stdio::from(pg_dump_stdout))
        .stdout(Stdio::piped())
        .spawn()?;

    restore.wait_with_output()?;

    psql(
        &target,
        PsqlInput::Sql("select public.timescaledb_post_restore()"),
    )?;
    Ok(())
}
