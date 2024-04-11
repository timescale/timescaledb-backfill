use crate::connect::Target;
use crate::postgres::fetch_pg_version_number;
use crate::timescale::fetch_tsdb_version;
use anyhow::{anyhow, Result};
use semver::{Version, VersionReq};
use std::sync::OnceLock;

static PER_CHUNK_COMPRESSION_SETTINGS: OnceLock<bool> = OnceLock::new();
static STORAGE_TYPE_IN_CREATE_TABLE: OnceLock<bool> = OnceLock::new();
static MUTATION_OF_COMPRESSED_HYPERTABLES: OnceLock<bool> = OnceLock::new();

pub async fn initialize_features(target: &Target) -> Result<()> {
    let ts_version = &Version::parse(&fetch_tsdb_version(&target.client).await?)?;
    let pg_version = fetch_pg_version_number(&target.client).await?;

    let ts_ge_214 = VersionReq::parse(">=2.14.0").unwrap().matches(ts_version);
    let ts_ge_211 = VersionReq::parse(">=2.11.0").unwrap().matches(ts_version);
    let pg_ge_16 = pg_version >= 160000;
    let pg_ge_14 = pg_version >= 140000;

    PER_CHUNK_COMPRESSION_SETTINGS
        .set(ts_ge_214)
        .map_err(|e| anyhow!("PER_CHUNK_COMPRESSION_SETTINGS already set to {}", e))?;

    STORAGE_TYPE_IN_CREATE_TABLE
        .set(pg_ge_16)
        .map_err(|e| anyhow!("STORAGE_TYPE_IN_CREATE_TABLE already set to {}", e))?;

    MUTATION_OF_COMPRESSED_HYPERTABLES
        .set(pg_ge_14 && ts_ge_211)
        .map_err(|e| anyhow!("MUTATION_OF_COMPRESSED_HYPERTABLES already set to {}", e))?;
    Ok(())
}

// Supported from TS >= 2.14.0
pub fn per_chunk_compression() -> bool {
    *PER_CHUNK_COMPRESSION_SETTINGS
        .get()
        .expect("PER_CHUNK_COMPRESSION_SETTINGS is not set")
}

// Supported from PG >= 16
pub fn storage_type_in_create_table() -> bool {
    *STORAGE_TYPE_IN_CREATE_TABLE
        .get()
        .expect("STORAGE_TYPE_IN_CREATE_TABLE is not set")
}

// Supported from TS >= 2.11.0 and PG >= 14
pub fn mutation_of_compressed_hypertables() -> bool {
    *MUTATION_OF_COMPRESSED_HYPERTABLES
        .get()
        .expect("STORAGE_TYPE_IN_CREATE_TABLE is not set")
}
