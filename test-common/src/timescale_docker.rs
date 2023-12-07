use std::fmt::{Display, Formatter};
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;

#[allow(dead_code)]
#[derive(PartialEq, Eq)]
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

impl<T: AsRef<str>> From<T> for PgVersion {
    fn from(value: T) -> Self {
        match value.as_ref() {
            "11" => PgVersion::PG11,
            "12" => PgVersion::PG12,
            "13" => PgVersion::PG13,
            "14" => PgVersion::PG14,
            "15" => PgVersion::PG15,
            _ => unimplemented!(),
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
