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
static CHUNK_CATALOG_USES_RELID: OnceLock<bool> = OnceLock::new();
static CHUNK_HAS_DROPPED_COLUMN: OnceLock<bool> = OnceLock::new();

/// Detects the shape of `_timescaledb_catalog.chunk`. Two independent columns
/// changed across the supported range, at different versions, so we detect them
/// directly instead of hard-coding version cut-offs:
/// - `dropped` was removed around TS 2.26 (dropped chunks are no longer kept as
///   catalog tombstones)
/// - `schema_name`/`table_name`/`compressed_chunk_id` were replaced by `relid`
///   in TS 2.29
async fn fetch_chunk_catalog_columns(target: &Target) -> Result<(bool, bool)> {
    let row = target
        .client
        .query_one(
            r"
            SELECT
                bool_or(column_name = 'relid')   AS has_relid,
                bool_or(column_name = 'dropped') AS has_dropped
            FROM information_schema.columns
            WHERE table_schema = '_timescaledb_catalog'
              AND table_name = 'chunk'
            ",
            &[],
        )
        .await?;
    Ok((row.get("has_relid"), row.get("has_dropped")))
}

pub async fn initialize_features(target: &Target) -> Result<()> {
    let mut ts_version = Version::parse(&fetch_tsdb_version(&target.client).await?)?;
    // Nightly builds report a prerelease version (e.g. `2.29.0-dev`). By semver
    // rules a prerelease only satisfies a comparator that itself carries a
    // prerelease with the same major.minor.patch, so a plain `>=X.Y.Z` would
    // match none of the checks below. Drop the prerelease so a nightly is
    // treated as its target release for feature gating.
    ts_version.pre = semver::Prerelease::EMPTY;
    let ts_version = &ts_version;
    let pg_version = fetch_pg_version_number(&target.client).await?;
    let (chunk_has_relid, chunk_has_dropped) = fetch_chunk_catalog_columns(target).await?;

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

    CHUNK_CATALOG_USES_RELID
        .set(chunk_has_relid)
        .map_err(|e| anyhow!("CHUNK_CATALOG_USES_RELID already set to {}", e))?;

    CHUNK_HAS_DROPPED_COLUMN
        .set(chunk_has_dropped)
        .map_err(|e| anyhow!("CHUNK_HAS_DROPPED_COLUMN already set to {}", e))?;
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
        .expect("MUTATION_OF_COMPRESSED_HYPERTABLES is not set")
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

// Detected, not version-gated. Starting with TS 2.29 the
// `_timescaledb_catalog.chunk` table stores the chunk's relation as a `relid`
// (regclass) instead of the `schema_name` / `table_name` pair and no longer
// tracks `compressed_chunk_id`. The compressed relation for a chunk is found
// through `compression_settings.compress_relid` and its size through
// `compression_chunk_size.chunk_id`.
pub fn chunk_catalog_uses_relid() -> bool {
    *CHUNK_CATALOG_USES_RELID
        .get()
        .expect("CHUNK_CATALOG_USES_RELID is not set")
}

// Detected, not version-gated. The `dropped` column was removed from
// `_timescaledb_catalog.chunk` around TS 2.26; from then on dropped chunks are
// removed from the catalog rather than kept as tombstones, so no filter is
// needed.
pub fn chunk_has_dropped_column() -> bool {
    *CHUNK_HAS_DROPPED_COLUMN
        .get()
        .expect("CHUNK_HAS_DROPPED_COLUMN is not set")
}
