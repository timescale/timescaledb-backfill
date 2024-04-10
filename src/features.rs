use crate::connect::Target;
use crate::postgres::fetch_pg_version_number;
use crate::timescale::fetch_tsdb_version;
use anyhow::{anyhow, Result};
use semver::{Version, VersionReq};
use std::sync::OnceLock;

static PER_CHUNK_COMPRESSION_SETTINGS: OnceLock<bool> = OnceLock::new();
static STORAGE_TYPE_IN_CREATE_TABLE: OnceLock<bool> = OnceLock::new();

pub async fn initialize_features(target: &Target) -> Result<()> {
    let ts_version = fetch_tsdb_version(&target.client).await?;
    let ge_214 = VersionReq::parse(">=2.14.0").unwrap();

    PER_CHUNK_COMPRESSION_SETTINGS
        .set(ge_214.matches(&Version::parse(&ts_version)?))
        .map_err(|e| anyhow!("PER_CHUNK_COMPRESSION_SETTINGS already set to {}", e))?;

    let pg_version = fetch_pg_version_number(&target.client).await?;
    let ge_16 = pg_version >= 160000;
    STORAGE_TYPE_IN_CREATE_TABLE
        .set(ge_16)
        .map_err(|e| anyhow!("STORAGE_TYPE_IN_CREATE_TABLE already set to {}", e))?;
    Ok(())
}

pub fn per_chunk_compression() -> bool {
    *PER_CHUNK_COMPRESSION_SETTINGS
        .get()
        .expect("PER_CHUNK_COMPRESSION_SETTINGS is not set")
}

pub fn storage_type_in_create_table() -> bool {
    *STORAGE_TYPE_IN_CREATE_TABLE
        .get()
        .expect("STORAGE_TYPE_IN_CREATE_TABLE is not set")
}
