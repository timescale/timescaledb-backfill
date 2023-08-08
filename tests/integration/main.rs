use anyhow::Result;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::ffi::OsStr;
use test_common::PgVersion::PG15;
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

#[derive(Debug)]
struct TestCase<'a, S, F>
where
    S: AsRef<OsStr>,
    F: Fn(&mut DbAssert, &mut DbAssert),
{
    setup_sql: Vec<PsqlInput<S>>,
    completion_time: &'a str,
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

fn run_test<S: AsRef<OsStr>, F: Fn(&mut DbAssert, &mut DbAssert)>(
    test_case: TestCase<S, F>,
) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

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

    run_backfill(
        TestConfigStage::new(&source_container, &target_container)
            .with_completion_time(test_case.completion_time),
    )
    .unwrap()
    .assert()
    .success();

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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
            }),
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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
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
                for dbassert in vec![source, target] {
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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1464)
                        .has_chunk_count("public", "metrics", 10);
                }
            }),
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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 2);
                }
            }),
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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 7)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
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
                for dbassert in vec![source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 105)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
            }),
        }
    ),
);

#[test]
fn copy_without_stage_error() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

    run_backfill(
        TestConfigCopy::new(&source_container, &target_container),
    )
    .unwrap()
    .assert()
    .failure()
    .stderr(predicate::str::contains(
            "Error: administrative schema `__backfill` not found. Run the `stage` command once before running `copy`."
        ));

    Ok(())
}

#[test]
fn copy_without_available_tasks_error() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

    run_backfill(TestConfigStage::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success();

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "there are no pending copy tasks. Use the `stage` command to add more.",
        ));

    Ok(())
}

#[test]
fn clean_removes_schema() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;

    run_backfill(TestConfigStage::new(&source_container, &target_container))
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
