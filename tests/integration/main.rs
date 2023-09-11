use anyhow::{bail, Result};
use assert_cmd::prelude::*;
use predicates::prelude::*;
use predicates::str::contains;
use std::env;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Read};
use std::process::Command;
use strip_ansi_escapes::strip;
use tap_reader::Tap;
use test_common::*;
use testcontainers::clients::Cli;

static SETUP_HYPERTABLE: &str = r"
    CREATE TABLE public.metrics(
        time TIMESTAMPTZ,
        device_id TEXT,
        val FLOAT8);
    SELECT create_hypertable('public.metrics', 'time');
";

static INSERT_DATA_FOR_MAY: &str = r"
    INSERT INTO metrics (time, device_id, val)
    SELECT time, 1, random()
    FROM generate_series('2023-05-01T00:00:00Z'::timestamptz, '2023-05-31T23:30:00Z'::timestamptz, '1 hour'::interval) time;
";

static INSERT_DATA_FOR_JUNE: &str = r"
    INSERT INTO metrics (time, device_id, val)
    SELECT time, 1, random()
    FROM generate_series('2023-06-01T00:00:00Z'::timestamptz, '2023-06-30T23:30:00Z'::timestamptz, '1 hour'::interval) time;
";

static ENABLE_HYPERTABLE_COMPRESSION: &str = r"
    ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');
";

static COMPRESS_ONE_CHUNK: &str = r"
    SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)) FROM timescaledb_information.chunks WHERE is_compressed = false LIMIT 1;
";

static COMPRESS_ALL_CHUNKS: &str = r"
    SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)) FROM timescaledb_information.chunks WHERE is_compressed = false;
";

static DECOMPRESS_ONE_CHUNK: &str = r"
    SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)) FROM timescaledb_information.chunks WHERE is_compressed = true LIMIT 1;
";

static CREATE_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW cagg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', time) as time, device_id, max(val) FROM metrics
    GROUP BY time_bucket('1 day', time), device_id;
";

static SETUP_BIGINT_HYPERTABLE: &str = r"
    CREATE TABLE public.metrics(
        time BIGINT,
        device_id TEXT,
        val FLOAT8);
    SELECT create_hypertable('public.metrics', 'time', chunk_time_interval => 86400000); -- 1 day in milliseconds
";

static INSERT_7_DAYS_OF_BIGINT_DATA: &str = r"
    INSERT INTO metrics (time, device_id, val)
    SELECT time, device_id, random()
    FROM generate_series(1, 604800000, 3600000) time
    CROSS JOIN generate_series(1, 300) device_id;
";

static ADD_SPACE_DIMENSION_TO_HYPERTABLE: &str = r"
    SELECT add_dimension('public.metrics', 'device_id', number_partitions => 15)
";

static SETUP_OTHER_HYPERTABLE: &str = r"
    CREATE SCHEMA other;
    CREATE TABLE other.metrics(
        time TIMESTAMPTZ,
        device_id TEXT,
        val FLOAT8);
    SELECT create_hypertable('other.metrics', 'time');
";

static INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY: &str = r"
    INSERT INTO other.metrics (time, device_id, val)
    SELECT time, 1, random()
    FROM generate_series('2023-05-01T00:00:00Z'::timestamptz, '2023-05-31T23:30:00Z'::timestamptz, '1 hour'::interval) time;
";

#[derive(Debug)]
struct TestCase<'a, S, F>
where
    S: AsRef<OsStr>,
    F: Fn(&mut DbAssert, &mut DbAssert),
{
    setup_sql: Vec<PsqlInput<S>>,
    completion_time: &'a str,
    filter: Option<String>,
    post_skeleton_source_sql: Vec<PsqlInput<S>>,
    post_skeleton_target_sql: Vec<PsqlInput<S>>,
    asserts: Box<F>,
}

macro_rules! generate_tests {
    ($(($func:ident, $testcase:expr),)*) => {
        $(
            #[test]
            fn $func() -> Result<()> {
                run_test($testcase)
            }
        )*
    }
}

fn external_version() -> Option<PgVersion> {
    env::var("BF_TEST_PG_VERSION").ok().map(PgVersion::from)
}

fn pg_version() -> PgVersion {
    external_version().unwrap_or(PgVersion::PG15)
}

fn run_test<S: AsRef<OsStr>, F: Fn(&mut DbAssert, &mut DbAssert)>(
    test_case: TestCase<S, F>,
) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    for sql in test_case.setup_sql {
        psql(&source_container, sql)?;
    }

    copy_skeleton_schema(&source_container, &target_container)?;

    for sql in test_case.post_skeleton_source_sql {
        psql(&source_container, sql)?;
    }

    for sql in test_case.post_skeleton_target_sql {
        psql(&target_container, sql)?;
    }

    let mut stage_config = TestConfigStage::new(
        &source_container,
        &target_container,
        test_case.completion_time,
    );

    if let Some(filter) = test_case.filter {
        stage_config = stage_config.with_filter(&filter);
    }

    run_backfill(stage_config).unwrap().assert().success();

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success();

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    (test_case.asserts)(&mut source_dbassert, &mut target_dbassert);

    run_backfill(TestConfigVerify::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success();

    Ok(())
}

generate_tests!(
    (
        copy_data_from_chunks,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            completion_time: "2023-06-01T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_data_from_chunks_filter_table,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
            ],
            completion_time: "2023-06-01T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("other", "metrics", 744)
                    .has_chunk_count("other", "metrics", 5);
                target
                    .has_table_count("other", "metrics", 0)
                    .has_chunk_count("other", "metrics", 5);
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
            }),
            filter: Some("public.metrics".into()),
        }
    ),
    (
        copy_data_from_chunks_filter_schema,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
            ],
            completion_time: "2023-06-01T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("other", "metrics", 744)
                    .has_chunk_count("other", "metrics", 5);
                target
                    .has_table_count("other", "metrics", 0)
                    .has_chunk_count("other", "metrics", 5);
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
            }),
            filter: Some("public.*".into()),
        }
    ),
    (
        copy_data_from_compressed_chunks,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "2023-06-01T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_correctly_delete_rows_in_active_chunk_in_target,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            // Completion time is just inside of a chunk boundary, so if we implement this incorrectly,
            // the chunk will end up with rows from the source with time > completion_time.
            completion_time: "2023-05-26T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![
                // Simulate beginning dual-write at 2023-05-20T00:00:00Z
                PsqlInput::Sql(
                    r"
                    INSERT INTO metrics (time, device_id, val)
                    SELECT time, 2, random()
                    FROM generate_series('2023-05-20T00:00:00Z'::timestamptz, '2023-06-10T23:30:00Z'::timestamptz, '1 hour'::interval) time;
                "
                ),
            ],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5);
                target
                    .has_table_count("public", "metrics", 984)
                    .has_chunk_count("public", "metrics", 7);
            }),
            filter: None,
        }
    ),
    (
        copy_continuous_aggregates,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-06-01T00:00:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_cagg_mt_chunk_count("public", "cagg", 1)
                        .has_table_count(
                            "_timescaledb_catalog",
                            "continuous_aggs_hypertable_invalidation_log",
                            0,
                        );
                }
            }),
            filter: None,
        }
    ),
    (
        copy_source_has_chunk_not_present_in_target,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            completion_time: "2023-07-01T00:00:00",
            post_skeleton_source_sql: vec![PsqlInput::Sql(INSERT_DATA_FOR_JUNE)],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1464)
                        .has_chunk_count("public", "metrics", 10);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_source_has_compressed_chunk_not_present_in_target,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "2023-07-01T00:00:00",
            post_skeleton_source_sql: vec![PsqlInput::Sql(COMPRESS_ONE_CHUNK),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 2);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_bigint_table,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_BIGINT_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_7_DAYS_OF_BIGINT_DATA),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "604800000",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 7)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_bigint_table_with_space_dimension,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_BIGINT_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(ADD_SPACE_DIMENSION_TO_HYPERTABLE),
                PsqlInput::Sql(INSERT_7_DAYS_OF_BIGINT_DATA),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "604800000",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 105)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_until_falls_within_compressed_chunk_in_source,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ALL_CHUNKS),
            ],
            completion_time: "2023-05-05T23:30:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
                target
                    .has_table_count("public", "metrics", 120)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
            }),
            filter: None,
        }
    ),
    (
        copy_until_falls_within_chunk_in_source_which_after_skeleton_copy_became_compressed,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            completion_time: "2023-05-05T23:30:00",
            post_skeleton_source_sql: vec![PsqlInput::Sql(COMPRESS_ALL_CHUNKS),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
                target
                    .has_table_count("public", "metrics", 120)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 1);
            }),
            filter: None,
        }
    ),
    (
        copy_until_falls_within_compressed_chunk_in_source_and_target,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ALL_CHUNKS),
            ],
            completion_time: "2023-05-05T23:30:00",
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![
                // this simulates a dual-write which has produced data in compressed chunks
                PsqlInput::Sql(
                    r"
                    INSERT INTO metrics (time, device_id, val)
                    SELECT time, 1, random()
                    FROM generate_series('2023-05-01T00:00:00Z'::timestamptz, '2023-05-06T23:30:00Z'::timestamptz, '1 hour'::interval) time;
                "
                ),
                PsqlInput::Sql(COMPRESS_ALL_CHUNKS),
            ],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
                target.has_table_count("public", "metrics", 144);
            }),
            filter: None,
        }
    ),
    (
        copy_chunk_which_was_uncompressed_and_after_skeleton_copy_became_partial,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            completion_time: "2023-07-01T00:00:00",
            post_skeleton_source_sql: vec![
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1488)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_chunk_which_was_compressed_and_after_skeleton_copy_became_partial,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "2023-07-01T00:00:00",
            post_skeleton_source_sql: vec![PsqlInput::Sql(INSERT_DATA_FOR_MAY),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1488)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
            filter: None,
        }
    ),
    (
        copy_chunk_which_was_compressed_and_after_skeleton_copy_was_uncompressed,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            ],
            completion_time: "2023-07-01T00:00:00",
            post_skeleton_source_sql: vec![PsqlInput::Sql(DECOMPRESS_ONE_CHUNK),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 0);
                target
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 0);
            }),
            filter: None,
        }
    ),
);

#[test]
fn copy_without_stage_error() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    run_backfill(
        TestConfigCopy::new(&source_container, &target_container),
    )
    .unwrap()
    .assert()
    .failure()
    .stderr(contains(
            "Error: administrative schema `__backfill` not found. Run the `stage` command once before running `copy`."
        ));

    Ok(())
}

#[test]
fn copy_without_available_tasks_error() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "20160201",
    ))
    .unwrap()
    .assert()
    .success();

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .failure()
        .stderr(contains(
            "there are no pending copy tasks. Use the `stage` command to add more.",
        ));

    Ok(())
}

#[test]
fn clean_removes_schema() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "20160201",
    ))
    .unwrap()
    .assert()
    .success();

    DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target")
        .has_schema("__backfill");

    run_backfill(TestConfigClean::new(&target_container))
        .unwrap()
        .assert()
        .success();

    DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target")
        .not_has_schema("__backfill");

    Ok(())
}

#[test]
fn ctrl_c_stops_gracefully() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
            INSERT INTO metrics (time, device_id, val)
            SELECT time, device_id, random()
            FROM generate_series('2023-05-04T00:00:00Z'::timestamptz, '2023-05-10T23:59:00Z'::timestamptz, '1 minute'::interval) time
            CROSS JOIN generate_series(1, 10) device_id;
        ",
        ),
    )?;

    copy_skeleton_schema(&source_container, &target_container)?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2023-05-10T23:59:00Z",
    ))
    .unwrap();

    let mut child = spawn_backfill(
        TestConfigCopy::new(&source_container, &target_container)
            .with_envs(vec![(String::from("RUST_LOG"), String::from("debug"))]),
    )
    .unwrap();

    let mut tapped_stdout = Tap::new(child.stdout.take().unwrap());

    wait_for_message_in_output(&mut tapped_stdout, "Copying uncompressed chunk")?;

    let mut kill = Command::new("kill")
        .args(["-s", "INT", &child.id().to_string()])
        .spawn()?;
    kill.wait()?;

    let mut output = child.wait_with_output().unwrap();

    // get the rest of the stdout output
    tapped_stdout.read_to_end(&mut Vec::new())?;

    output.stdout.append(&mut tapped_stdout.bytes);

    output.assert().success().stdout(
        contains("Copying 1 chunks with 8 workers")
            .and(contains(
                "Shutting down, waiting for in-progress copies to complete...",
            ))
            .and(contains(
                "[1/1] Copied chunk \"_timescaledb_internal\".\"_hyper_1_1_chunk\"",
            ))
            .and(contains("Copied 2.1MB from 1 chunks")),
    );

    Ok(())
}

fn wait_for_message_in_output<T: Read>(output: &mut T, message: &str) -> Result<()> {
    let logs = BufReader::new(output);

    for line in logs.lines() {
        let line = line?;
        if line.contains(message) {
            return Ok(());
        }
    }

    bail!("message '{message}' not found in output")
}

// This test fails under macos, because double ctrl-c does not actually
// hard-stop the program on that platform. Inserting a `yield_now` into the
// tight loop in `copy_from_source_to_sink` fixes the problem, but it needs
// further investigation.
#[cfg(not(target_os = "macos"))]
#[test]
fn double_ctrl_c_stops_hard() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
            INSERT INTO metrics (time, device_id, val)
            SELECT time, device_id, random()
            FROM generate_series('2023-05-04T00:00:00Z'::timestamptz, '2023-05-10T23:59:00Z'::timestamptz, '1 minute'::interval) time
            CROSS JOIN generate_series(1, 10) device_id;
        ",
        ),
    )?;

    copy_skeleton_schema(&source_container, &target_container)?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2023-05-10T23:59:00Z",
    ))
    .unwrap();

    let mut child = spawn_backfill(
        TestConfigCopy::new(&source_container, &target_container)
            .with_envs(vec![(String::from("RUST_LOG"), String::from("debug"))]),
    )
    .unwrap();

    let mut tapped_stdout = Tap::new(child.stdout.take().unwrap());

    wait_for_message_in_output(&mut tapped_stdout, "Copying uncompressed chunk")?;

    let mut kill = Command::new("kill")
        .args(["-s", "INT", &child.id().to_string()])
        .spawn()?;
    kill.wait()?;
    let mut kill = Command::new("kill")
        .args(["-s", "INT", &child.id().to_string()])
        .spawn()?;
    kill.wait()?;

    let mut output = child.wait_with_output().unwrap();

    // get the rest of the stdout output
    tapped_stdout.read_to_end(&mut Vec::new())?;

    output.stdout.append(&mut tapped_stdout.bytes);

    output.assert().success().stdout(
        contains("Copying 1 chunks with 8 workers")
            .and(contains(
                "Shutting down, waiting for in-progress copies to complete...",
            ))
            .and(
                contains("[1/1] Copied chunk \"_timescaledb_internal\".\"_hyper_1_1_chunk\"").not(),
            )
            .and(contains("Copied 0B from 0 chunks")),
    );
    Ok(())
}

#[test]
fn copy_task_with_deleted_source_chunk_skips_it() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    // Given 2 chunks
    psql(
        &source_container,
        vec![
            PsqlInput::Sql(SETUP_HYPERTABLE),
            PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE), // hypertables with a cagg behave differently on drop
            PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
        ],
    )?;
    psql(
        &source_container,
        vec![
            PsqlInput::Sql(
                r"
                INSERT INTO metrics(time, device_id, val)
                VALUES
                    ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '6 month', 88, 43),
                    ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '3 month', 42, 24),
                    ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '1 month', 7, 21)",
            ),
            PsqlInput::Sql(COMPRESS_ONE_CHUNK),
            PsqlInput::Sql(
                r"
                SELECT public.drop_chunks(
                    'public.metrics',
                    '2016-01-02T00:00:00Z'::timestamptz - INTERVAL '4 month'
                )",
            ),
        ],
    )?;

    copy_skeleton_schema(&source_container, &target_container)?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("Staged 2 chunks to copy"));

    // When we delete a chunk that has already been staged
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz - INTERVAL '2 month'
        )",
        ),
    )?;

    // Then the chunk is skipped and all the other task execute regularly
    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success()
        .stdout(contains(r#"Skipping chunk "_timescaledb_internal"."_hyper_1_2_chunk" because it no longer exists on source"#)
                .and(contains(r#"Copied chunk "_timescaledb_internal"."_hyper_1_3_chunk" in"#)));

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    source_dbassert.has_chunk_count("public", "metrics", 3);
    target_dbassert.has_chunk_count("public", "metrics", 3);

    Ok(())
}

#[test]
fn stage_skips_chunks_marked_as_dropped() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    psql(
        &source_container,
        vec![
            PsqlInput::Sql(SETUP_HYPERTABLE),
            PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE), // hypertables with a cagg behave differently on drop
            // Given 3 chunks
            PsqlInput::Sql(
                r"
            INSERT INTO metrics(time, device_id, val)
            VALUES
                ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '6 month', 88, 43),
                ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '3 month', 42, 24),
                ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '1 month', 7, 21)",
            ),
            // Mark two of three chunks as dropped
            PsqlInput::Sql(
                r"SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz - INTERVAL '2 month')
            ",
            ),
        ],
    )?;

    DbAssert::new(&source_container.connection_string())
        .unwrap()
        .has_chunk_count("public", "metrics", 3);

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("Staged 1 chunks to copy"));

    Ok(())
}

#[test]
fn duplicated_stage_task_is_skipped() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    copy_skeleton_schema(&source_container, &target_container)?;
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        INSERT INTO metrics(time, device_id, val)
        VALUES
            ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '1 month', 7, 21)",
        ),
    )?;
    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("Staged 1 chunks to copy"));

    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    target_dbassert.has_task_count(1);

    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        INSERT INTO metrics(time, device_id, val)
        VALUES
            ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '1 day', 7, 21),
            ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '2 month', 7, 21)",
        ),
    )?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("Staged 2 chunks"))
    .stdout(contains(
        "Skipping 1 chunks that were already staged. To re-stage run the `clean` command first",
    ));

    target_dbassert.has_task_count(3);

    Ok(())
}

fn stage_and_copy_a_single_chunk<C: HasConnectionString>(
    source_container: &C,
    target_container: &C,
) -> Result<()> {
    psql(source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    copy_skeleton_schema(source_container, target_container)?;
    psql(
        source_container,
        PsqlInput::Sql(
            r"
        INSERT INTO metrics(time, device_id, val)
        VALUES
            ('2016-01-02T00:00:00Z'::timestamptz - INTERVAL '3 month', 7, 21)",
        ),
    )?;
    run_backfill(TestConfigStage::new(
        source_container,
        target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("Staged 1 chunks to copy"));
    run_backfill(TestConfigCopy::new(source_container, target_container))
        .unwrap()
        .assert()
        .success()
        .stdout(contains(
            r#"Copied chunk "_timescaledb_internal"."_hyper_1_1_chunk" in"#,
        ));

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    source_dbassert.has_chunk_count("public", "metrics", 1);
    target_dbassert.has_chunk_count("public", "metrics", 1);
    Ok(())
}

#[test]
fn verify_task_with_deleted_source_chunk() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When the source chunk is dropped
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz - INTERVAL '2 month'
        )",
        ),
    )?;

    // Then verify will output a message saying that the chunk no longer exists
    run_backfill(TestConfigVerify::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success()
        .stdout(contains(
            r#"source chunk "_timescaledb_internal"."_hyper_1_1_chunk" no longer exists"#,
        ));

    Ok(())
}

#[test]
fn verify_task_with_deleted_target_chunk() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When the target chunk is dropped
    psql(
        &target_container,
        PsqlInput::Sql(
            r"
        SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz - INTERVAL '2 month'
        )",
        ),
    )?;

    // Then verify will output a message saying that the chunk no longer exists
    run_backfill(TestConfigVerify::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success()
        .stdout(contains(r#"target chunk does not exist"#));

    Ok(())
}

#[test]
fn verify_task_with_extra_rows_in_source() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version()));
    let target_container = docker.run(timescaledb(pg_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When there's extra rows in the source table
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        INSERT INTO metrics(time, device_id, val)
        VALUES
            ('2016-01-01T23:59:59Z'::timestamptz - INTERVAL '3 month', 1, 11),
            ('2016-01-02T00:00:02Z'::timestamptz - INTERVAL '3 month', 8, 31)",
        ),
    )?;

    let expected_diff = r#"Verifying 1 chunks with 8 workers
[1/1] Chunk verification failed, source="_timescaledb_internal"."_hyper_1_1_chunk" target="_timescaledb_internal"."_hyper_1_1_chunk" diff
```diff
--- original
+++ modified
@@ -1,14 +1,14 @@
 min:
-  device_id: '7'
-  time: 2015-10-02 00:00:00+00
-  val: '21'
+  device_id: '1'
+  time: 2015-10-01 23:59:59+00
+  val: '11'
 max:
-  device_id: '7'
-  time: 2015-10-02 00:00:00+00
-  val: '21'
+  device_id: '8'
+  time: 2015-10-02 00:00:02+00
+  val: '31'
 sum: {}
 count:
-  device_id: 1
-  time: 1
-  val: 1
-total_count: 1
+  device_id: 3
+  time: 3
+  val: 3
+total_count: 3
```
Verifed 1 chunks in"#;

    // Then verify will output a message saying that the chunk no longer exists
    let success = run_backfill(TestConfigVerify::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success();

    // The output has some control characters for colors which make it hard to
    // compare against.
    let stripped_actual_output = String::from_utf8(strip(&success.get_output().stdout))?;
    assert!(stripped_actual_output.contains(expected_diff));
    Ok(())
}
