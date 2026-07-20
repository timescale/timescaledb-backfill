use crate::TsVersion::{
    Nightly, TS216, TS217, TS218, TS219, TS220, TS221, TS222, TS223, TS224, TS225, TS226, TS227,
    TS228,
};
use std::fmt::{Display, Formatter};
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;

#[allow(dead_code)]
#[derive(PartialEq, Eq)]
pub enum PgVersion {
    PG15,
    PG16,
    PG17,
    PG18,
}

impl Display for PgVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PgVersion::PG15 => f.write_str("15"),
            PgVersion::PG16 => f.write_str("16"),
            PgVersion::PG17 => f.write_str("17"),
            PgVersion::PG18 => f.write_str("18"),
        }
    }
}

impl<T: AsRef<str>> From<T> for PgVersion {
    fn from(value: T) -> Self {
        match value.as_ref() {
            "15" => PgVersion::PG15,
            "16" => PgVersion::PG16,
            "17" => PgVersion::PG17,
            "18" => PgVersion::PG18,
            _ => unimplemented!(),
        }
    }
}

#[allow(dead_code)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum TsVersion {
    TS216,
    TS217,
    TS218,
    TS219,
    TS220,
    TS221,
    TS222,
    TS223,
    TS224,
    TS225,
    TS226,
    TS227,
    TS228,
    /// The nightly build of TimescaleDB. Tracks the next unreleased version
    /// (currently 2.29). Ordered last so it compares greater than any released
    /// version.
    Nightly,
}

impl From<String> for TsVersion {
    fn from(value: String) -> Self {
        match value.as_str() {
            "2.16" => TS216,
            "2.17" => TS217,
            "2.18" => TS218,
            "2.19" => TS219,
            "2.20" => TS220,
            "2.21" => TS221,
            "2.22" => TS222,
            "2.23" => TS223,
            "2.24" => TS224,
            "2.25" => TS225,
            "2.26" => TS226,
            "2.27" => TS227,
            "2.28" => TS228,
            "2.29" | "nightly" => Nightly,
            _ => unimplemented!(),
        }
    }
}

impl Display for TsVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TS216 => write!(f, "2.16"),
            TS217 => write!(f, "2.17"),
            TS218 => write!(f, "2.18"),
            TS219 => write!(f, "2.19"),
            TS220 => write!(f, "2.20"),
            TS221 => write!(f, "2.21"),
            TS222 => write!(f, "2.22"),
            TS223 => write!(f, "2.23"),
            TS224 => write!(f, "2.24"),
            TS225 => write!(f, "2.25"),
            TS226 => write!(f, "2.26"),
            TS227 => write!(f, "2.27"),
            TS228 => write!(f, "2.28"),
            Nightly => write!(f, "2.29"),
        }
    }
}

pub const TIMESCALEDB_IMAGE: &str = "timescale/timescaledb-ha";

/// Nightly builds live in a separate repository and are tagged only by the
/// PostgreSQL version (e.g. `nightly-pg18`).
pub const TIMESCALEDB_NIGHTLY_IMAGE: &str = "timescaledev/timescaledb";

/// Prepares a testcontainer image object for a given version of PostgreSQL
pub fn postgres(version: PgVersion) -> GenericImage {
    generic_postgres("postgres", version.to_string().as_str())
}

/// Prepares a testcontainer image object for the latest version of
/// TimescaleDB and a given version of PostgreSQL
pub fn timescaledb(pg_version: PgVersion, ts_version: TsVersion) -> GenericImage {
    let (image, tag) = match ts_version {
        Nightly => (TIMESCALEDB_NIGHTLY_IMAGE, format!("nightly-pg{pg_version}")),
        _ => (TIMESCALEDB_IMAGE, format!("pg{pg_version}-ts{ts_version}")),
    };
    generic_postgres(image, tag.as_str()).with_env_var("TIMESCALEDB_TELEMETRY", "off")
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
