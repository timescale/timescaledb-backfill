use anyhow::Result;
use assert_cmd::prelude::*;
use chrono::{DateTime, Utc};
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

static ENABLE_HYPERTABLE_COMPRESSION: &str = r"
    ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');
";

static COMPRESS_ONE_CHUNK: &str = r"
    SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)) FROM timescaledb_information.chunks WHERE is_compressed = false LIMIT 1;
";

static CREATE_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW cagg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', time), device_id, max(val) FROM metrics
    GROUP BY time_bucket('1 day', time), device_id;
";

#[derive(Debug)]
struct TestCase<S, F>
where
    S: AsRef<OsStr>,
    F: Fn(&mut DbAssert),
{
    setup_sql: Vec<PsqlInput<S>>,
    completion_time: DateTime<Utc>,
    post_skeleton_sql: Vec<PsqlInput<S>>,
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

fn run_test<S: AsRef<OsStr>, F: Fn(&mut DbAssert)>(test_case: TestCase<S, F>) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(PG15));
    let target_container = docker.run(timescaledb(PG15));

    for sql in test_case.setup_sql {
        psql(&source_container, sql)?;
    }

    copy_skeleton_schema(&source_container, &target_container)?;

    for sql in test_case.post_skeleton_sql {
        psql(&source_container, sql)?;
    }

    run_backfill(
        TestConfig::new(
            &source_container,
            &target_container,
            test_case.completion_time,
        ),
        "copy",
    )
    .unwrap()
    .assert()
    .success();

    for (db, name) in &[(&source_container, "source"), (&target_container, "target")] {
        let mut dbassert = DbAssert::new(&db.connection_string())
            .unwrap()
            .with_name(name);
        (test_case.asserts)(&mut dbassert);
    }
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
            completion_time: Utc::now(),
            post_skeleton_sql: vec![],
            asserts: Box::new(|dbassert: &mut DbAssert| {
                dbassert
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5);
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
            completion_time: Utc::now(),
            post_skeleton_sql: vec![],
            asserts: Box::new(|dbassert: &mut DbAssert| {
                dbassert
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 1);
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
            completion_time: Utc::now(),
            post_skeleton_sql: vec![],
            asserts: Box::new(|dbassert: &mut DbAssert| {
                dbassert
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_cagg_mt_chunk_count("public", "cagg", 1)
                    .has_table_count(
                        "_timescaledb_catalog",
                        "continuous_aggs_hypertable_invalidation_log",
                        0,
                    );
            }),
        }
    ),
);
