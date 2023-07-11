use anyhow::Result;
use chrono::Utc;
use test_common::PgVersion::PG15;
use test_common::*;
use testcontainers::*;

static SETUP_HYPERTABLE_WITH_COMPRESSED_CHUNKS: &str = r"
    CREATE TABLE public.metrics(
        time TIMESTAMPTZ,
        device_id TEXT,
        val FLOAT8);
    SELECT create_hypertable('public.metrics', 'time');
    INSERT INTO metrics (time, device_id, val)
    SELECT time, 1, random()
    FROM generate_series('2023-06-01T00:00:00Z'::timestamptz, '2023-06-30T23:30:00Z'::timestamptz, '1 hour'::interval) time;

    ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');

    SELECT compress_chunk(format('%I.%I', schema_name, table_name)) FROM _timescaledb_catalog.chunk WHERE id = 1;
";

#[test]
fn copy_data_from_compressed_chunks() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = clients::Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

    psql(
        &source_container,
        PsqlInput::Sql(SETUP_HYPERTABLE_WITH_COMPRESSED_CHUNKS),
    )?;

    copy_skeleton_schema(&source_container, &target_container)?;
    run_backfill(
        TestConfig::new(&source_container, &target_container, Utc::now()),
        "copy",
    )?;

    for db in &[&source_container, &target_container] {
        DbAssert::new(db)?
            .has_table_count("public", "metrics", 720)
            .has_chunk_count("public", "metrics", 5)
            .has_compressed_chunk_count("public", "metrics", 1);
    }
    Ok(())
}
