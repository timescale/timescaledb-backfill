use crate::connect::Target;
use crate::postgres::fetch_pg_version_number;
use crate::timescale::fetch_tsdb_version;
use anyhow::{anyhow, Result};
use semver::{Version, VersionReq};
use std::sync::OnceLock;

static PER_CHUNK_COMPRESSION_SETTINGS: OnceLock<bool> = OnceLock::new();
static STORAGE_TYPE_IN_CREATE_TABLE: OnceLock<bool> = OnceLock::new();
static MUTATION_OF_COMPRESSED_HYPERTABLES: OnceLock<bool> = OnceLock::new();
static NO_SEQUENCE_NUMBER_IN_COMPRESSED_HYPERTABLES: OnceLock<bool> = OnceLock::new();
static COMPRESSION_SETTINGS_WITH_COMPRESS_RELID: OnceLock<bool> = OnceLock::new();
static HYPERCORE_TAM: OnceLock<bool> = OnceLock::new();

pub async fn initialize_features(target: &Target) -> Result<()> {
    let ts_version = &Version::parse(&fetch_tsdb_version(&target.client).await?)?;
    let pg_version = fetch_pg_version_number(&target.client).await?;

    let ts_lt_222 = VersionReq::parse("<2.22.0").unwrap().matches(ts_version);
    let ts_ge_219 = VersionReq::parse(">=2.19.0").unwrap().matches(ts_version);
    let ts_ge_218 = VersionReq::parse(">=2.18.0").unwrap().matches(ts_version);
    let ts_ge_217 = VersionReq::parse(">=2.17.0").unwrap().matches(ts_version);
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

    NO_SEQUENCE_NUMBER_IN_COMPRESSED_HYPERTABLES
        .set(ts_ge_217)
        .map_err(|e| {
            anyhow!(
                "NO_SEQUENCE_NUMBER_IN_COMPRESSED_HYPERTABLES already set to {}",
                e
            )
        })?;

    COMPRESSION_SETTINGS_WITH_COMPRESS_RELID
        .set(ts_ge_219)
        .map_err(|e| {
            anyhow!(
                "COMPRESSION_SETTINGS_WITH_COMPRESS_RELID already set to {}",
                e
            )
        })?;

    HYPERCORE_TAM
        .set(ts_ge_218 && ts_lt_222)
        .map_err(|e| anyhow!("HYPERCORE_TAM already set to {}", e))?;
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

// Supported from TS >= 2.17.0
pub fn no_sequence_number_in_compressed_hypertables() -> bool {
    *NO_SEQUENCE_NUMBER_IN_COMPRESSED_HYPERTABLES
        .get()
        .expect("NO_SEQUENCE_NUMBER_IN_COMPRESSED_HYPERTABLES is not set")
}

// Supported from TS >= 2.19.0
pub fn compression_settings_with_compress_relid() -> bool {
    *COMPRESSION_SETTINGS_WITH_COMPRESS_RELID
        .get()
        .expect("COMPRESSION_SETTINGS_WITH_COMPRESS_RELID is not set")
}

// Supported from TS >= 2.18.0 to < 2.22.0
pub fn hypercore_tam() -> bool {
    *HYPERCORE_TAM.get().expect("HYPERCORE_TAM is not set")
}
