use anyhow::{Context, Result};
use assert_cmd::cargo::CargoError;
use assert_cmd::prelude::*;
use std::env;
use std::ffi::OsString;
use std::process::{Command, Output, Stdio};

use log::{debug, trace};
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
const PSQL_IMAGE: &str = "postgres:15";

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

pub fn run_backfill_(config: TestConfig, action: &str) -> Result<Output> {
    debug!("running backfill");
    let child = Command::cargo_bin("timescaledb-backfill")?
        .arg(action)
        .args(config.args())
        .spawn()
        .expect("Couldn't launch timescaledb-backfill");

    child.wait_with_output().context("backfill process failed")
}

/// Runs backfill with the specified test configuration [`TestConfig`]
/// waits for it to finish and returns its [`std::process::Output`].
///
/// The timescaledb-backfill process could be either run as a regular child
/// process or, if `USE_DOCKER` environment variable is set to `true`,
/// as a docker container. In the latter case `DOCKER_IMAGE` determines
/// the docker image that will be used.
/// (The default is `timescale/timescaledb-backfill:edge`)
///
/// If `DOCKER_PLATFORM` is set its value will be passed to the docker
/// command as `--platform=<value>`
pub fn run_backfill(config: TestConfig, action: &str) -> std::io::Result<Output> {
    backfill_cmd(action, config.args().to_vec())
        .expect("Couldn't build a Command to launch backfill")
        .spawn()
        .expect("Couldn't launch backfill")
        .wait_with_output()
}

fn use_docker() -> bool {
    env::var("USE_DOCKER")
        .map(|val| val.to_ascii_lowercase() == "true")
        .unwrap_or(false)
}

fn use_psql_docker() -> bool {
    env::var("PSQL_DOCKER")
        .map(|val| val.to_ascii_lowercase() == "true")
        .unwrap_or_else(|_| use_docker())
}

fn backfill_cmd(action: &str, args: Vec<OsString>) -> Result<Command, CargoError> {
    if use_docker() {
        let backfill_image = env::var("DOCKER_IMAGE")
            .unwrap_or_else(|_| "timescale/timescaledb-backfill:edge".into());

        let mut cmd = Command::new("docker");
        cmd.args([
            "run",
            "--add-host",
            "host.docker.internal:host-gateway",
            "--rm",
            "-i",
        ]);

        if let Some(image_platform) = env::var("DOCKER_PLATFORM").ok().filter(|p| !p.is_empty()) {
            cmd.args(["--platform", &image_platform]);
        }

        cmd.arg(backfill_image).arg(action).args(args);

        Ok(cmd)
    } else {
        let mut cmd = Command::cargo_bin("timescaledb-backfill")?;
        cmd.arg(action).args(args);
        Ok(cmd)
    }
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

    let restore = Command::new("psql")
        .arg(target.connection_string().as_str())
        .stdin(Stdio::from(pg_dump_stdout))
        .stdout(Stdio::piped())
        .spawn()?;

    restore.wait_with_output()?;
    Ok(())
}
