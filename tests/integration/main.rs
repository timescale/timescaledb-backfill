use crate::config::{
    TestConfig, TestConfigClean, TestConfigCopy, TestConfigRefreshCaggs, TestConfigStage,
    TestConfigVerify,
};
use anyhow::{bail, Context, Result};
use assert_cmd::prelude::*;
use diffy::{create_patch, PatchFormatter};
use lazy_static::lazy_static;
use predicates::prelude::*;
use predicates::str::contains;
use semver::{Version, VersionReq};
use std::clone::Clone;
use std::env;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use strip_ansi_escapes::strip;
use tap_reader::Tap;
use test_common::TsVersion::{TS217, TS218};
use test_common::*;
use testcontainers::clients::Cli;
use tracing::debug;

pub mod config;

lazy_static! {
    static ref TS_LT_2_12: VersionReq = VersionReq::parse("<2.12").unwrap();
    static ref TS_HYPERCORE_SUPPORT: VersionReq = VersionReq::parse(">=2.18, <2.22").unwrap();
}

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

static ENABLE_HYPERCORE_ACCESS_METHOD: &str = r"
    ALTER TABLE metrics SET ACCESS METHOD hypercore;
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
    SELECT time_bucket('1 day', time) as time, device_id, max(val) as max_val FROM metrics
    GROUP BY time_bucket('1 day', time), device_id;
";

static CREATE_ANOTHER_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW cagg2
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', time) as time, device_id, avg(val) as avg_val FROM metrics
    GROUP BY time_bucket('1 day', time), device_id;
";

static CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW hcagg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('2 day', time) as time, device_id, max(max_val) as max_val FROM cagg
    GROUP BY time_bucket('2 day', time), device_id;
";

static CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW hcagg2
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('2 day', time) as time, device_id, avg(max_val) as avg_max_val FROM cagg
    GROUP BY time_bucket('2 day', time), device_id;
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

static CREATE_OTHER_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW other.cagg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 day', time) as time, device_id, max(val) as max_val FROM other.metrics
    GROUP BY time_bucket('1 day', time), device_id;
";

static CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE: &str = r"
    CREATE MATERIALIZED VIEW other.hcagg
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('2 day', time) as time, device_id, max(max_val) as max_val FROM other.cagg
    GROUP BY time_bucket('2 day', time), device_id;
";

#[derive(Debug, Eq, PartialEq)]
enum CascadeMode {
    None,
    Up,
    Down,
    Both,
}

#[derive(Debug, Eq, PartialEq)]
struct Filter<'a> {
    filter: &'a str,
    cascade: CascadeMode,
}

impl<'a> Filter<'a> {
    fn new(filter: &'a str, cascade: CascadeMode) -> Self {
        Filter { filter, cascade }
    }
}

#[derive(Debug)]
struct TestCase<'a, S, F>
where
    S: AsRef<OsStr>,
    F: Fn(&mut DbAssert, &mut DbAssert),
{
    setup_sql: Vec<PsqlInput<S>>,
    completion_time: &'a str,
    starting_time: Option<&'a str>,
    filter: Option<Filter<'a>>,
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
    external_version().unwrap_or(PgVersion::PG17)
}

fn ts_version() -> TsVersion {
    env::var("BF_TEST_TS_VERSION")
        .ok()
        .map(TsVersion::from)
        .unwrap_or(TS217)
}

/// Spawns a backfill process with the specified test configuration [`TestConfig`],
/// returning the associated [`std::process::Child`]
pub fn spawn_backfill(config: impl TestConfig) -> Result<Child> {
    Command::cargo_bin("timescaledb-backfill")?
        .arg(config.action())
        .args(config.args())
        .envs(config.envs())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context("couldn't spawn timescaledb-backfill")
}

/// Runs backfill with the specified test configuration [`TestConfig`]
/// waits for it to finish and returns its [`std::process::Output`].
pub fn run_backfill(config: impl TestConfig) -> Result<Output> {
    debug!("running backfill");
    let child = spawn_backfill(config).expect("Couldn't launch timescaledb-backfill");

    child.wait_with_output().context("backfill process failed")
}

pub fn copy_skeleton_schema<S: HasConnectionString, T: HasConnectionString>(
    source: S,
    target: T,
) -> Result<()> {
    let pg_dump = Command::new("pg_dump")
        .args(["-d", source.connection_string().as_str()])
        .args(["--format", "plain"])
        .args(["--exclude-table-data", "_timescaledb_internal.*"])
        .arg("--quote-all-identifiers")
        .arg("--no-tablespaces")
        .arg("--no-owner")
        .arg("--no-privileges")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let pg_dump_stdout = pg_dump.stdout.unwrap();

    psql(
        &target,
        PsqlInput::Sql("select public.timescaledb_pre_restore()"),
    )?;

    let restore = Command::new("psql")
        .arg(target.connection_string().as_str())
        .stdin(Stdio::from(pg_dump_stdout))
        .stdout(Stdio::piped())
        .spawn()?;

    restore.wait_with_output()?;

    psql(
        &target,
        PsqlInput::Sql("select public.timescaledb_post_restore()"),
    )?;
    Ok(())
}

fn run_test<S: AsRef<OsStr>, F: Fn(&mut DbAssert, &mut DbAssert)>(
    test_case: TestCase<S, F>,
) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    configure_cloud_setup(&target_container)?;

    let conn_tsdbadmin = target_container
        .connection_string()
        .user("tsdbadmin")
        .dbname("tsdb");

    for sql in test_case.setup_sql {
        psql(&source_container, sql)?;
    }

    copy_skeleton_schema(&source_container, &conn_tsdbadmin)?;

    for sql in test_case.post_skeleton_source_sql {
        psql(&source_container, sql)?;
    }

    for sql in test_case.post_skeleton_target_sql {
        psql(&conn_tsdbadmin, sql)?;
    }

    let mut stage_config = TestConfigStage::new(
        &source_container,
        &conn_tsdbadmin,
        test_case.completion_time,
    );

    if let Some(filter) = test_case.filter {
        stage_config = stage_config.with_filter(filter.filter);
        stage_config = match filter.cascade {
            CascadeMode::Up => stage_config.with_cascading_up(),
            CascadeMode::Down => stage_config.with_cascading_down(),
            CascadeMode::Both => stage_config.with_cascading_up().with_cascading_down(),
            CascadeMode::None => stage_config,
        };
    }

    if let Some(from) = test_case.starting_time {
        stage_config = stage_config.with_starting_time(from);
    }

    run_backfill(stage_config).unwrap().assert().success();

    run_backfill(TestConfigCopy::new(&source_container, &conn_tsdbadmin))
        .unwrap()
        .assert()
        .success();

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&conn_tsdbadmin.connection_string())
        .unwrap()
        .with_name("target");

    run_backfill(TestConfigVerify::new(&source_container, &conn_tsdbadmin))
        .unwrap()
        .assert()
        .success()
        .stdout(contains("Chunk verification failed").not());

    (test_case.asserts)(&mut source_dbassert, &mut target_dbassert);

    Ok(())
}

/// Timescale cloud has special configuration which restricts which actions can
/// be performed in the database instance. This function performs the following
/// actions:
/// - Creates the `tsdbadmin` role
/// - Creates the `tsdb` database, with owner `tsdbadmin`
/// - Applies most (?) of the restrictions which Timescale cloud does
///   Note: it's somewhat non-trivial to know exactly which restrictions are
///   applied. We cherry-picked these from: https://github.com/timescale/timescaledb-operator/blob/6b99a24ff1d72751249e4238db54b84e54e351a3/operator/pkg/options/scripts/after-create.sql
fn configure_cloud_setup<C: HasConnectionString>(container: &C) -> Result<()> {
    psql(
        &container,
        PsqlInput::File(PathBuf::from("tests/cloud_init.sql")),
    )?;
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
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
            }),
            filter: None,
        }
    ),
    (
        copy_data_from_chunks_different_column_ordinal,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql("ALTER TABLE public.metrics DROP COLUMN val"),
                PsqlInput::Sql("ALTER TABLE public.metrics ADD COLUMN val FLOAT8"),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            completion_time: "2023-06-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5);
                }
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
            }),
            filter: None,
        }
    ),
    (
        copy_data_from_chunks_with_from_flag,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_JUNE),
            ],
            completion_time: "2023-06-30T23:30:01Z",
            starting_time: Some("2023-06-01T00:00:00Z"),
            post_skeleton_source_sql: vec![],
            // Insert data on a previous date to assert that we are not
            // deleting it.
            post_skeleton_target_sql: vec![PsqlInput::Sql(INSERT_DATA_FOR_MAY),],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 720)
                    .has_chunk_count("public", "metrics", 5);
                target
                    .has_table_count("public", "metrics", 1464)
                    .has_chunk_count("public", "metrics", 10);
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
            }),
            filter: None,
        }
    ),
    (
        copy_data_from_a_single_chunk_with_from_flag,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            // Chunk size is 7 days
            completion_time: "2023-05-11 00:00:00+00",
            starting_time: Some("2023-05-04 00:00:00+00"),
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5);
                target
                    .has_table_count("public", "metrics", 168)
                    .has_chunk_count("public", "metrics", 5);
                let tasks = 1;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
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
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
            }),
            filter: Some(Filter::new("public.metrics", CascadeMode::None)),
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
            starting_time: None,
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
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
            }),
            filter: Some(Filter::new("public.*", CascadeMode::None)),
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
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
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

                let tasks = 5;
                target
                    .has_table_count("public", "metrics", 984)
                    .has_chunk_count("public", "metrics", 7)
                    .has_telemetry(vec![
                        assert_stage_telemetry(tasks),
                        assert_copy_telemetry(tasks),
                        assert_verify_telemetry(tasks, 0),
                    ]);
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
            starting_time: None,
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
                let tasks = 6;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![PsqlInput::Sql(INSERT_DATA_FOR_JUNE)],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1464)
                        .has_chunk_count("public", "metrics", 10);
                }
                let tasks = 10;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![PsqlInput::Sql(COMPRESS_ONE_CHUNK),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 744)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 2);
                }
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 7)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
                let tasks = 7;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 50400)
                        .has_chunk_count("public", "metrics", 105)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
                let tasks = 105;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
                let tasks = 2;
                target
                    .has_table_count("public", "metrics", 120)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5)
                    .has_telemetry(vec![
                        assert_stage_telemetry(tasks),
                        assert_copy_telemetry(tasks),
                        assert_verify_telemetry(tasks, 0),
                    ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![PsqlInput::Sql(COMPRESS_ALL_CHUNKS),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 5);
                let tasks = 2;
                target
                    .has_table_count("public", "metrics", 120)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 1)
                    .has_telemetry(vec![
                        assert_stage_telemetry(tasks),
                        assert_copy_telemetry(tasks),
                        assert_verify_telemetry(tasks, 0),
                    ]);
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
            starting_time: None,
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
                let tasks = 2;
                target
                    .has_table_count("public", "metrics", 144)
                    .has_telemetry(vec![
                        assert_stage_telemetry(tasks),
                        assert_copy_telemetry(tasks),
                        assert_verify_telemetry(tasks, 0),
                    ]);
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
            starting_time: None,
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
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![PsqlInput::Sql(INSERT_DATA_FOR_MAY),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                for dbassert in [source, target] {
                    dbassert
                        .has_table_count("public", "metrics", 1488)
                        .has_chunk_count("public", "metrics", 5)
                        .has_compressed_chunk_count("public", "metrics", 1);
                }
                let tasks = 5;
                target.has_telemetry(vec![
                    assert_stage_telemetry(tasks),
                    assert_copy_telemetry(tasks),
                    assert_verify_telemetry(tasks, 0),
                ]);
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
            starting_time: None,
            post_skeleton_source_sql: vec![PsqlInput::Sql(DECOMPRESS_ONE_CHUNK),],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|source: &mut DbAssert, target: &mut DbAssert| {
                source
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 0);
                let tasks = 5;
                target
                    .has_table_count("public", "metrics", 744)
                    .has_chunk_count("public", "metrics", 5)
                    .has_compressed_chunk_count("public", "metrics", 0)
                    .has_telemetry(vec![
                        assert_stage_telemetry(tasks),
                        assert_copy_telemetry(tasks),
                        assert_verify_telemetry(tasks, 0),
                    ]);
            }),
            filter: None,
        }
    ),
    (
        filter_match_one_ht,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 5;
                target
                    .has_task_count_for_table("public", "metrics", tasks)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.metrics$", CascadeMode::None)),
        }
    ),
    (
        filter_match_two_ht,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 10;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("other", "metrics", 5)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^(public|other)\\.metrics$", CascadeMode::None)),
        }
    ),
    (
        filter_match_one_cagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 1;
                target
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.cagg$", CascadeMode::None)),
        }
    ),
    (
        filter_match_two_cagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 2;
                target
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("other", "cagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^(public|other)\\.cagg$", CascadeMode::None)),
        }
    ),
    (
        filter_match_schema,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 7;
                target
                    .has_task_count_for_table("other", "metrics", 5)
                    .has_task_count_for_table("other", "cagg", 1)
                    .has_task_count_for_table("other", "hcagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^other\\..*$", CascadeMode::None)),
        }
    ),
    (
        filter_cascade_up_from_ht,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 9;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("public", "cagg2", 1)
                    .has_task_count_for_table("public", "hcagg", 1)
                    .has_task_count_for_table("public", "hcagg2", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.metrics$", CascadeMode::Up)),
        }
    ),
    (
        filter_cascade_up_from_cagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 3;
                target
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("public", "hcagg", 1)
                    .has_task_count_for_table("public", "hcagg2", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.cagg$", CascadeMode::Up)),
        }
    ),
    (
        filter_cascade_down_from_cagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 6;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.cagg$", CascadeMode::Down)),
        }
    ),
    (
        filter_cascade_up_and_down_from_cagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 8;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("public", "hcagg", 1)
                    .has_task_count_for_table("public", "hcagg2", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.cagg$", CascadeMode::Both)),
        }
    ),
    (
        filter_cascade_down_from_hcagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 7;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("public", "hcagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^public\\.hcagg$", CascadeMode::Down)),
        }
    ),
    (
        filter_cascade_down_from_two_hcagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 14;
                target
                    .has_task_count_for_table("public", "metrics", 5)
                    .has_task_count_for_table("public", "cagg", 1)
                    .has_task_count_for_table("public", "hcagg", 1)
                    .has_task_count_for_table("other", "metrics", 5)
                    .has_task_count_for_table("other", "cagg", 1)
                    .has_task_count_for_table("other", "hcagg", 1)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^(public|other)\\.hcagg$", CascadeMode::Down)),
        }
    ),
    (
        filter_cascade_up_from_hcagg,
        TestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_ANOTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(SETUP_OTHER_HYPERTABLE),
                PsqlInput::Sql(INSERT_OTHER_HYPERTABLE_DATA_FOR_MAY),
                PsqlInput::Sql(CREATE_OTHER_CONTINUOUS_AGGREGATE),
                PsqlInput::Sql(CREATE_OTHER_HIERARCHICAL_CONTINUOUS_AGGREGATE),
            ],
            completion_time: "2023-07-01T00:00:00",
            starting_time: None,
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![],
            asserts: Box::new(|_: &mut DbAssert, target: &mut DbAssert| {
                let tasks = 1;
                target
                    .has_task_count_for_table("other", "hcagg", tasks)
                    .has_task_count(tasks)
                    .has_telemetry(vec![
                        assert_stage_telemetry(usize::try_from(tasks).unwrap()),
                        assert_copy_telemetry(usize::try_from(tasks).unwrap()),
                        assert_verify_telemetry(usize::try_from(tasks).unwrap(), 0),
                    ]);
            }),
            filter: Some(Filter::new("^other\\.hcagg$", CascadeMode::Up)),
        }
    ),
);

#[test]
fn copy_without_stage_error() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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

    DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target")
        .has_telemetry(vec![
            assert_stage_telemetry(0),
            assert_error_telemetry(
                String::from("copy"),
                vec!["there are no pending copy tasks. Use the `stage` command to add more."],
            ),
        ]);

    Ok(())
}

#[test]
fn clean_removes_schema() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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
            .and(contains("Copied 3.1MB from 1 chunks")),
    );

    Ok(())
}

#[test]
fn abort_on_mismatching_timescaledb_version() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), TS217));
    let target_container = docker.run(timescaledb(pg_version(), TS218));

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

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2023-05-10T23:59:00Z",
    ))
    .unwrap()
    .assert()
    .failure()
    .stderr(contains(
        "timescaledb extension version is different in source and target",
    ));

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .failure()
        .stderr(contains(
            "timescaledb extension version is different in source and target",
        ));

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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Given 3 chunks
    psql(
        &source_container,
        vec![
            PsqlInput::Sql(SETUP_HYPERTABLE),
            PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE),
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

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    source_dbassert.has_chunk_count("public", "metrics", 2);
    target_dbassert.has_chunk_count("public", "metrics", 2);

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2016-01-02T00:00:00Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains(
        "Staged 2 chunks to copy.\nExecute the 'copy' command to migrate the data.",
    ));

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

    source_dbassert.has_chunk_count("public", "metrics", 1);
    // And the target has 2 chunks because it was created by skeleton_copy and
    // dropped in source before the COPY.
    target_dbassert.has_chunk_count("public", "metrics", 2);

    Ok(())
}

#[test]
fn stage_skips_chunks_marked_as_dropped() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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
            // Mark two of three chunks as dropped. This only happens when the
            // table has a CAGG, and the CAGG is still building
            // https://github.com/timescale/timescaledb/blob/b8057f90916dac54fd6f6400e1cab6eddd226a02/src/chunk.c#L4031
            PsqlInput::Sql(
                r"UPDATE _timescaledb_catalog.chunk
                SET dropped = true
                WHERE id in (1, 2)
                ",
            ),
        ],
    )?;

    psql(
        &target_container,
        vec![
            PsqlInput::Sql(SETUP_HYPERTABLE),
            PsqlInput::Sql(CREATE_CONTINUOUS_AGGREGATE), // hypertables with a cagg behave differently on drop
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
    .stdout(contains(
        "Staged 1 chunks to copy.\nExecute the 'copy' command to migrate the data.",
    ));

    Ok(())
}

#[test]
fn duplicated_stage_task_is_skipped() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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
    .stdout(contains(
        "Staged 1 chunks to copy.\nExecute the 'copy' command to migrate the data.",
    ));

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
            ('2016-01-02T00:00:00Z'::timestamptz, 7, 21)",
        ),
    )?;
    run_backfill(TestConfigStage::new(
        source_container,
        target_container,
        "2016-01-02T00:00:01Z",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains(
        "Staged 1 chunks to copy.\nExecute the 'copy' command to migrate the data.",
    ));
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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When the source chunk is dropped
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz + INTERVAL '2 month'
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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When the target chunk is dropped
    psql(
        &target_container,
        PsqlInput::Sql(
            r"
        SELECT public.drop_chunks(
            'public.metrics',
            '2016-01-02T00:00:00Z'::timestamptz + INTERVAL '2 month'
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

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    // When there's extra rows in the source table
    psql(
        &source_container,
        PsqlInput::Sql(
            r"
        INSERT INTO metrics(time, device_id, val)
        VALUES
            ('2016-01-01T23:59:59Z'::timestamptz, 1, 11),
            ('2016-01-02T00:00:02Z'::timestamptz, 8, 31)", // This row is discarded by until
        ),
    )?;

    let expected_diff = r#"Verifying 1 chunks with 8 workers
[1/1] Chunk verification failed, source="_timescaledb_internal"."_hyper_1_1_chunk" target="_timescaledb_internal"."_hyper_1_1_chunk" diff
```diff
--- original
+++ modified
@@ -1,7 +1,7 @@
 min:
-  device_id: '1'
-  time: 2016-01-01 23:59:59+00
-  val: '11'
+  device_id: '7'
+  time: 2016-01-02 00:00:00+00
+  val: '21'
 max:
   device_id: '7'
   time: 2016-01-02 00:00:00+00
@@ -8,7 +8,7 @@
   val: '21'
 sum: {}
 count:
-  device_id: 2
-  time: 2
-  val: 2
-total_count: 2
+  device_id: 1
+  time: 1
+  val: 1
+total_count: 1
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
    assert!(
        stripped_actual_output.contains(expected_diff),
        "\n=== Expected diff \n {} \n=== Actual diff \n {}",
        expected_diff,
        stripped_actual_output
    );

    DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target")
        .has_telemetry(vec![
            assert_stage_telemetry(1),
            assert_copy_telemetry(1),
            assert_verify_telemetry(1, 1),
        ]);
    Ok(())
}

#[test]
fn refresh_caggs_with_no_cagg() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Given a chunk that's staged and copied
    stage_and_copy_a_single_chunk(&source_container, &target_container)?;

    run_backfill(TestConfigRefreshCaggs::new(
        &source_container,
        &target_container,
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains("No continuous aggregates to refresh"));

    Ok(())
}

macro_rules! generate_refresh_caggs_tests {
    ($(($func:ident, $testcase:expr),)*) => {
        $(
            #[test]
            fn $func() -> Result<()> {
                run_refresh_caggs_test($testcase)
            }
        )*
    }
}

#[derive(Debug)]
struct RefreshCaggsTestCase<'a, F, G>
where
    F: Fn(&mut DbAssert, bool),
    G: Fn(Output, bool),
{
    filter: Option<Filter<'a>>,
    asserts: Box<F>,
    assert_output: Box<G>,
}

#[derive(Clone)]
struct CaggExpected {
    initial_watermak: i64,
    refreshed_watermark: i64,
    stdout: &'static str,
}

static CAGG_T1_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 1694044800000000,
    refreshed_watermark: 1704585600000000,
    stdout: "Refreshing continuous aggregate 'public'.'caggs\"_T1' in range [2023-09-07 00:00:00+00, 2024-01-07 00:00:00+00)",
};

static TS_LT_2_12_CAGG_T1_2_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 1695945600000000,
    refreshed_watermark: 1706313600000000,
    stdout: "Refreshing continuous aggregate 'public'.'caggs_t1_2' in range [2023-09-29 00:00:00+00, 2024-01-27 00:00:00+00)",
};
static TS_GTE_2_12_CAGG_T1_2_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 1696118400000000,
    refreshed_watermark: 1706745600000000,
    stdout: "Refreshing continuous aggregate 'public'.'caggs_t1_2' in range [2023-10-01 00:00:00+00, 2024-02-01 00:00:00+00)",
};

static TS_LT_2_12_CAGG_T1_3_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 1698537600000000,
    refreshed_watermark: 1708905600000000,
    stdout: "Refreshing continuous aggregate \'public\'.\'caggs_t1_3\' in range [2023-10-29 00:00:00+00, 2024-02-26 00:00:00+00)",
};

static TS_GTE_2_12_CAGG_T1_3_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 1698796800000000,
    refreshed_watermark: 1709251200000000,
    stdout: "Refreshing continuous aggregate 'public'.'caggs_t1_3' in range [2023-11-01 00:00:00+00, 2024-03-01 00:00:00+00)",
};

static CAGG_T2_EXPECTED: CaggExpected = CaggExpected {
    initial_watermak: 10,
    refreshed_watermark: 40,
    stdout: "Refreshing continuous aggregate 'public'.'caggs_t2' in range [10, 40)",
};

// Timescale 2.12 changed the way the hierarchical caggs interval are
// calculated. This affects the watermark calculation. Depending on the TS
// version running the tests we might get different results.
//
//#5860 Fix interval calculation for hierarchical CAggs
fn is_ts_version_lt_2_12(dsn: &TestConnectionString) -> Result<bool> {
    let ts_version = Version::parse(get_ts_version(dsn)?.as_ref())?;
    Ok(TS_LT_2_12.matches(&ts_version))
}

fn supports_hypercore(dsn: &TestConnectionString) -> Result<bool> {
    let ts_version = Version::parse(get_ts_version(dsn)?.as_ref())?;
    Ok(TS_HYPERCORE_SUPPORT.matches(&ts_version))
}

// Timescale 2.12 changed the way the hierarchical caggs interval are
// calculated. This affects the watermark calculation. Depending on the TS
// version running the tests we might get different results.
//
//#5860 Fix interval calculation for hierarchical CAggs
fn get_cagg_t1_2_expected(ts_version_lt_2_12: bool) -> CaggExpected {
    if ts_version_lt_2_12 {
        TS_LT_2_12_CAGG_T1_2_EXPECTED.clone()
    } else {
        TS_GTE_2_12_CAGG_T1_2_EXPECTED.clone()
    }
}

// Timescale 2.12 changed the way the hierarchical caggs interval are
// calculated. This affects the watermark calculation. Depending on the TS
// version running the tests we might get different results.
//
//#5860 Fix interval calculation for hierarchical CAggs
fn get_cagg_t1_3_expected(ts_version_lt_2_12: bool) -> CaggExpected {
    if ts_version_lt_2_12 {
        TS_LT_2_12_CAGG_T1_3_EXPECTED.clone()
    } else {
        TS_GTE_2_12_CAGG_T1_3_EXPECTED.clone()
    }
}

fn run_refresh_caggs_test<F, G>(test_case: RefreshCaggsTestCase<F, G>) -> Result<()>
where
    F: Fn(&mut DbAssert, bool),
    G: Fn(Output, bool),
{
    let _ = pretty_env_logger::try_init();
    let docker = testcontainers::clients::Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    psql(
        &source_container,
        PsqlInput::File(PathBuf::from("tests/source_schema.sql")),
    )?;
    copy_skeleton_schema(&source_container, &target_container)?;

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    let is_source_ts_lt_2_12 = is_ts_version_lt_2_12(&source_container.connection_string())?;

    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs\"_T1",
        CAGG_T1_EXPECTED.initial_watermak,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_2",
        get_cagg_t1_2_expected(is_source_ts_lt_2_12).initial_watermak,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_3",
        get_cagg_t1_3_expected(is_source_ts_lt_2_12).initial_watermak,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t2",
        CAGG_T2_EXPECTED.initial_watermak,
    );

    let is_target_ts_lt_2_12 = is_ts_version_lt_2_12(&source_container.connection_string())?;

    target_dbassert.has_cagg_with_watermark(
        "public",
        "caggs\"_T1",
        CAGG_T1_EXPECTED.initial_watermak,
    );
    target_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_2",
        get_cagg_t1_2_expected(is_target_ts_lt_2_12).initial_watermak,
    );
    target_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_3",
        get_cagg_t1_3_expected(is_target_ts_lt_2_12).initial_watermak,
    );
    target_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t2",
        CAGG_T2_EXPECTED.initial_watermak,
    );

    let dual_write_query: &str = r"
    insert into t1 values ('2024-01-06 19:27:30.024001+02', 1, 1);
    insert into t2 values (30, 1, 1);";

    psql(&source_container, PsqlInput::Sql(dual_write_query))?;
    psql(&target_container, PsqlInput::Sql(dual_write_query))?;

    psql(
        &source_container,
        PsqlInput::Sql(
            r#"call refresh_continuous_aggregate('"caggs""_T1"', null, '2024-10-31 02:00:00+02')"#,
        ),
    )?;
    psql(
        &source_container,
        PsqlInput::Sql(
            "call refresh_continuous_aggregate('caggs_t1_2', null, '2024-10-31 02:00:00+02')",
        ),
    )?;
    psql(
        &source_container,
        PsqlInput::Sql(
            "call refresh_continuous_aggregate('caggs_t1_3', null, '2024-10-31 02:00:00+02')",
        ),
    )?;
    psql(
        &source_container,
        PsqlInput::Sql("call refresh_continuous_aggregate('caggs_t2', 0, 40)"),
    )?;

    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs\"_T1",
        CAGG_T1_EXPECTED.refreshed_watermark,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_2",
        get_cagg_t1_2_expected(is_source_ts_lt_2_12).refreshed_watermark,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t1_3",
        get_cagg_t1_3_expected(is_source_ts_lt_2_12).refreshed_watermark,
    );
    source_dbassert.has_cagg_with_watermark(
        "public",
        "caggs_t2",
        CAGG_T2_EXPECTED.refreshed_watermark,
    );

    let mut config = TestConfigRefreshCaggs::new(&source_container, &target_container);

    if let Some(filter) = test_case.filter {
        config = config.with_filter(filter.filter);
        config = match filter.cascade {
            CascadeMode::Up => config.with_cascading_up(),
            CascadeMode::Down => config.with_cascading_down(),
            CascadeMode::Both => config.with_cascading_up().with_cascading_down(),
            CascadeMode::None => config,
        };
    }

    let output = run_backfill(config).unwrap();

    (test_case.assert_output)(output, is_target_ts_lt_2_12);

    (test_case.asserts)(&mut target_dbassert, is_target_ts_lt_2_12);
    Ok(())
}

generate_refresh_caggs_tests!(
    (
        refresh_caggs_all,
        RefreshCaggsTestCase {
            filter: None,
            asserts: Box::new(|target: &mut DbAssert, ts_version_lt_2_12: bool| {
                target.has_cagg_with_watermark(
                    "public",
                    "caggs\"_T1",
                    CAGG_T1_EXPECTED.refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_2",
                    get_cagg_t1_2_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_3",
                    get_cagg_t1_3_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t2",
                    CAGG_T2_EXPECTED.refreshed_watermark,
                );
                target.has_telemetry(vec![assert_refresh_caggs_telemetry(4)]);
            }),
            assert_output: Box::new(|output: Output, ts_version_lt_2_12: bool| {
                output.assert().success().stdout(
                    contains(CAGG_T1_EXPECTED.stdout)
                        .and(contains(get_cagg_t1_2_expected(ts_version_lt_2_12).stdout))
                        .and(contains(get_cagg_t1_3_expected(ts_version_lt_2_12).stdout))
                        .and(contains(CAGG_T2_EXPECTED.stdout)),
                );
            }),
        }
    ),
    (
        refresh_caggs_with_filter,
        RefreshCaggsTestCase {
            filter: Some(Filter {
                filter: "public.\"caggs\"\"_T1\"",
                cascade: CascadeMode::None
            }),
            asserts: Box::new(|target: &mut DbAssert, ts_version_lt_2_12: bool| {
                target.has_cagg_with_watermark(
                    "public",
                    "caggs\"_T1",
                    CAGG_T1_EXPECTED.refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_2",
                    get_cagg_t1_2_expected(ts_version_lt_2_12).initial_watermak,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_3",
                    get_cagg_t1_3_expected(ts_version_lt_2_12).initial_watermak,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t2",
                    CAGG_T2_EXPECTED.initial_watermak,
                );
                target.has_telemetry(vec![assert_refresh_caggs_telemetry(1)]);
            }),
            assert_output: Box::new(|output: Output, ts_version_lt_2_12: bool| {
                output.assert().success().stdout(
                    contains(CAGG_T1_EXPECTED.stdout)
                        .and(contains(get_cagg_t1_2_expected(ts_version_lt_2_12).stdout).not())
                        .and(contains(get_cagg_t1_3_expected(ts_version_lt_2_12).stdout).not())
                        .and(contains(CAGG_T2_EXPECTED.stdout).not()),
                );
            }),
        }
    ),
    (
        refresh_caggs_with_filter_cascade_up,
        RefreshCaggsTestCase {
            filter: Some(Filter {
                filter: "public.\"caggs\"\"_T1\"",
                cascade: CascadeMode::Up,
            }),
            asserts: Box::new(|target: &mut DbAssert, ts_version_lt_2_12: bool| {
                target.has_cagg_with_watermark(
                    "public",
                    "caggs\"_T1",
                    CAGG_T1_EXPECTED.refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_2",
                    get_cagg_t1_2_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_3",
                    get_cagg_t1_3_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t2",
                    CAGG_T2_EXPECTED.initial_watermak,
                );
                target.has_telemetry(vec![assert_refresh_caggs_telemetry(3)]);
            }),
            assert_output: Box::new(|output: Output, ts_version_lt_2_12: bool| {
                output.assert().success().stdout(
                    contains(CAGG_T1_EXPECTED.stdout)
                        .and(contains(get_cagg_t1_2_expected(ts_version_lt_2_12).stdout))
                        .and(contains(get_cagg_t1_3_expected(ts_version_lt_2_12).stdout))
                        .and(contains(CAGG_T2_EXPECTED.stdout).not()),
                );
            }),
        }
    ),
    (
        refresh_caggs_with_filter_cascade_down,
        RefreshCaggsTestCase {
            filter: Some(Filter {
                filter: "public.caggs_t1_3",
                cascade: CascadeMode::Down,
            }),
            asserts: Box::new(|target: &mut DbAssert, ts_version_lt_2_12: bool| {
                target.has_cagg_with_watermark(
                    "public",
                    "caggs\"_T1",
                    CAGG_T1_EXPECTED.refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_2",
                    get_cagg_t1_2_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t1_3",
                    get_cagg_t1_3_expected(ts_version_lt_2_12).refreshed_watermark,
                );
                target.has_cagg_with_watermark(
                    "public",
                    "caggs_t2",
                    CAGG_T2_EXPECTED.initial_watermak,
                );
                target.has_telemetry(vec![assert_refresh_caggs_telemetry(3)]);
            }),
            assert_output: Box::new(|output: Output, ts_version_lt_2_12: bool| {
                output.assert().success().stdout(
                    contains(CAGG_T1_EXPECTED.stdout)
                        .and(contains(get_cagg_t1_2_expected(ts_version_lt_2_12).stdout))
                        .and(contains(get_cagg_t1_3_expected(ts_version_lt_2_12).stdout))
                        .and(contains(CAGG_T2_EXPECTED.stdout).not()),
                );
            }),
        }
    ),
);

#[test]
fn telemetry_captures_error_reason() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    psql(&source_container, PsqlInput::Sql(INSERT_DATA_FOR_MAY))?;

    copy_skeleton_schema(&source_container, &target_container)?;

    psql(&target_container, PsqlInput::Sql("drop table metrics"))?;
    psql(
        &target_container,
        PsqlInput::Sql(
            r"CREATE TABLE public.metrics(time TIMESTAMPTZ, device_id TEXT, val FLOAT8);",
        ),
    )?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2023-06-01T00:00:00",
    ))
    .unwrap()
    .assert()
    .success();

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .failure()
        .stderr(contains("Error: worker pool error").and(contains(
            r#"Caused by:
    0: worker execution error
    1: db error: ERROR: table "metrics" is not a hypertable
    2: ERROR: table "metrics" is not a hypertable"#,
        )));

    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    target_dbassert.has_telemetry(vec![
        assert_stage_telemetry(5),
        assert_error_telemetry(
            "copy".into(),
            vec![
                "worker pool error",
                "worker execution error",
                r#"db error: ERROR: table "metrics" is not a hypertable"#,
                r#"ERROR: table "metrics" is not a hypertable"#,
            ],
        ),
    ]);

    Ok(())
}

fn assert_stage_telemetry(staged_tasks: usize) -> Box<dyn Fn(JsonAssert)> {
    Box::new(move |json_assert: JsonAssert| {
        json_assert.has_string("session_id");
        json_assert.has_string("session_created_at");
        json_assert.has(
            "timescaledb_backfill_version",
            env!("CARGO_PKG_VERSION").to_string(),
        );
        json_assert.has("debug_mode", true);
        json_assert.has("success", true);
        json_assert.has("command", "stage");
        json_assert.has_number("command_duration_secs");
        json_assert.has("staged_tasks", staged_tasks);
        json_assert.has_null("copy_tasks_finished");
        json_assert.has_null("copy_tasks_total_bytes");
        json_assert.has_null("verify_tasks_finished");
        json_assert.has_null("error_reason");
        json_assert.has_null("refreshed_caggs");
        json_assert.has_string("source_db_pg_version");
        json_assert.has_string("source_db_tsdb_version");
    })
}

fn assert_copy_telemetry(copy_tasks_finished: usize) -> Box<dyn Fn(JsonAssert)> {
    Box::new(move |json_assert: JsonAssert| {
        json_assert.has_string("session_id");
        json_assert.has_string("session_created_at");
        json_assert.has(
            "timescaledb_backfill_version",
            env!("CARGO_PKG_VERSION").to_string(),
        );
        json_assert.has("debug_mode", true);
        json_assert.has("success", true);
        json_assert.has("command", "copy");
        json_assert.has_number("command_duration_secs");
        json_assert.has("copy_tasks_finished", copy_tasks_finished);
        json_assert.has_number("copy_tasks_total_bytes");
        json_assert.has_null("staged_tasks");
        json_assert.has_null("verify_tasks_finished");
        json_assert.has_null("verify_tasks_failures");
        json_assert.has_null("error_reason");
        json_assert.has_null("refreshed_caggs");
        json_assert.has_string("source_db_pg_version");
        json_assert.has_string("source_db_tsdb_version");
    })
}

fn assert_error_telemetry(command: String, reason: Vec<&str>) -> Box<dyn Fn(JsonAssert) + '_> {
    Box::new(move |json_assert: JsonAssert| {
        json_assert.has_array_value("error_reason", reason.clone());
        json_assert.has_null("error_backtrace");
        json_assert.has_string("session_id");
        json_assert.has_string("session_created_at");
        json_assert.has(
            "timescaledb_backfill_version",
            env!("CARGO_PKG_VERSION").to_string(),
        );
        json_assert.has("debug_mode", true);
        json_assert.has("success", false);
        json_assert.has("command", command.clone());
        json_assert.has_number("command_duration_secs");
        json_assert.has_null("copy_tasks_finished");
        json_assert.has_null("copy_tasks_total_bytes");
        json_assert.has_null("staged_tasks");
        json_assert.has_null("verify_tasks_finished");
        json_assert.has_null("verify_tasks_failures");
        json_assert.has_null("refreshed_caggs");
        json_assert.has_string("source_db_pg_version");
        json_assert.has_string("source_db_tsdb_version");
    })
}

fn assert_verify_telemetry(
    verify_tasks_finished: usize,
    verify_tasks_failures: usize,
) -> Box<dyn Fn(JsonAssert)> {
    Box::new(move |json_assert: JsonAssert| {
        json_assert.has_string("session_id");
        json_assert.has_string("session_created_at");
        json_assert.has(
            "timescaledb_backfill_version",
            env!("CARGO_PKG_VERSION").to_string(),
        );
        json_assert.has("debug_mode", true);
        json_assert.has("success", true);
        json_assert.has("command", "verify");
        json_assert.has_number("command_duration_secs");
        json_assert.has_null("copy_tasks_finished");
        json_assert.has_null("copy_tasks_total_bytes");
        json_assert.has_null("staged_tasks");
        json_assert.has_null("refreshed_caggs");
        json_assert.has("verify_tasks_finished", verify_tasks_finished);
        json_assert.has("verify_tasks_failures", verify_tasks_failures);
        json_assert.has_null("error_reason");
        json_assert.has_string("source_db_pg_version");
        json_assert.has_string("source_db_tsdb_version");
    })
}

fn assert_refresh_caggs_telemetry(refreshed_caggs: usize) -> Box<dyn Fn(JsonAssert)> {
    Box::new(move |json_assert: JsonAssert| {
        json_assert.has_null("session_id");
        json_assert.has_null("session_created_at");
        json_assert.has(
            "timescaledb_backfill_version",
            env!("CARGO_PKG_VERSION").to_string(),
        );
        json_assert.has("debug_mode", true);
        json_assert.has("success", true);
        json_assert.has("command", "refresh_caggs");
        json_assert.has_number("command_duration_secs");
        json_assert.has_null("copy_tasks_finished");
        json_assert.has_null("copy_tasks_total_bytes");
        json_assert.has_null("staged_tasks");
        json_assert.has("refreshed_caggs", refreshed_caggs);
        json_assert.has_null("error_reason");
        json_assert.has_string("source_db_pg_version");
        json_assert.has_string("source_db_tsdb_version");
    })
}

#[derive(Debug)]
struct ValidateTestCase<'a, S>
where
    S: AsRef<OsStr>,
{
    setup_sql: Vec<PsqlInput<S>>,
    post_skeleton_source_sql: Vec<PsqlInput<S>>,
    post_skeleton_target_sql: Vec<PsqlInput<S>>,
    stderr: &'a str,
}

macro_rules! generate_validate_tests {
    ($(($func:ident, $testcase:expr),)*) => {
        $(
            #[test]
            fn $func() -> Result<()> {
                run_validate_test($testcase)
            }
        )*
    }
}

fn run_validate_test<S: AsRef<OsStr>>(test_case: ValidateTestCase<S>) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

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

    // Then verify will output a message saying that the chunk no longer exists
    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2025-01-01 00:00:00",
    ))
    .unwrap()
    .assert()
    .failure()
    .stderr(contains(test_case.stderr));

    Ok(())
}

generate_validate_tests!(
    (
        validate_hypertables_target_additional_column,
        ValidateTestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![PsqlInput::Sql(
                "ALTER TABLE metrics add column extra bigint"
            )],
            stderr: r#"- 'public.metrics' columns mismatch:
    * source columns: time timestamp with time zone (pg_catalog.timestamptz), device_id text (pg_catalog.text), val double precision (pg_catalog.float8)
    * target columns: time timestamp with time zone (pg_catalog.timestamptz), device_id text (pg_catalog.text), val double precision (pg_catalog.float8), extra bigint (pg_catalog.int8)"#
        }
    ),
    (
        validate_hypertables_source_additional_column,
        ValidateTestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            post_skeleton_source_sql: vec![PsqlInput::Sql(
                "ALTER TABLE metrics add column extra bigint"
            )],
            post_skeleton_target_sql: vec![],
            stderr: r#"- 'public.metrics' columns mismatch:
    * source columns: time timestamp with time zone (pg_catalog.timestamptz), device_id text (pg_catalog.text), val double precision (pg_catalog.float8), extra bigint (pg_catalog.int8)
    * target columns: time timestamp with time zone (pg_catalog.timestamptz), device_id text (pg_catalog.text), val double precision (pg_catalog.float8)"#
        }
    ),
    (
        validate_hypertables_table_not_in_target,
        ValidateTestCase {
            setup_sql: vec![
                PsqlInput::Sql(SETUP_HYPERTABLE),
                PsqlInput::Sql(INSERT_DATA_FOR_MAY),
            ],
            post_skeleton_source_sql: vec![],
            post_skeleton_target_sql: vec![PsqlInput::Sql("DROP TABLE metrics CASCADE")],
            stderr: r#"- 'public.metrics' not found in target:
    * source columns: time timestamp with time zone (pg_catalog.timestamptz), device_id text (pg_catalog.text), val double precision (pg_catalog.float8)"#,
        }
    ),
);

#[test]
fn validate_hypertables_hypercore_not_supported() -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();
    let source_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Skip test if hypercore is not supported in this TimescaleDB version
    if !supports_hypercore(&source_container.connection_string())? {
        println!("Skipping hypercore test - TimescaleDB version doesn't support hypercore");
        return Ok(());
    }

    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Setup hypertable and data
    psql(&source_container, PsqlInput::Sql(SETUP_HYPERTABLE))?;
    psql(&source_container, PsqlInput::Sql(INSERT_DATA_FOR_MAY))?;

    copy_skeleton_schema(&source_container, &target_container)?;

    // Enable hypercore access method on source
    psql(
        &source_container,
        PsqlInput::Sql(ENABLE_HYPERCORE_ACCESS_METHOD),
    )?;

    // Attempt to stage and expect failure
    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2025-01-01 00:00:00",
    ))
    .unwrap()
    .assert()
    .failure()
    .stderr(contains("TimescaleDB does no longer support the hypercore table access method. Convert the following tables to heap access method before upgrading: public.metrics"));

    Ok(())
}

#[test]
fn panic_on_copy_if_source_has_compressed_chunk_not_present_in_target_with_different_compression_settings(
) -> Result<()> {
    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    configure_cloud_setup(&target_container)?;

    let conn_tsdbadmin = target_container
        .connection_string()
        .user("tsdbadmin")
        .dbname("tsdb");

    // Given a hypertable with compression enable.
    let setup_sql = vec![
        PsqlInput::Sql(SETUP_HYPERTABLE),
        PsqlInput::Sql("ALTER TABLE metrics ADD COLUMN label TEXT DEFAULT 'my-label';"),
        PsqlInput::Sql(ENABLE_HYPERTABLE_COMPRESSION),
        PsqlInput::Sql(INSERT_DATA_FOR_MAY),
        PsqlInput::Sql(COMPRESS_ONE_CHUNK),
    ];
    for sql in setup_sql {
        psql(&source_container, sql)?;
    }

    // And the skeleton copied over.
    copy_skeleton_schema(&source_container, &conn_tsdbadmin)?;

    let post_skeleton_source_sql = vec![
        // If after the skeleton is copied over, the compression settings
        // change only in source.
        PsqlInput::Sql(
            "ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device_id,label')"
        ),
        // And a new compressed chunk to be backfilled is created in source
        // with the new compression settings.
        PsqlInput::Sql(COMPRESS_ONE_CHUNK),
    ];
    for sql in post_skeleton_source_sql {
        psql(&source_container, sql)?;
    }

    let stage_config =
        TestConfigStage::new(&source_container, &conn_tsdbadmin, "2023-07-01T00:00:00");

    run_backfill(stage_config).unwrap().assert().success();

    // When running the copy the operation.
    let result = run_backfill(TestConfigCopy::new(&source_container, &conn_tsdbadmin))
        .unwrap()
        .assert()
        .failure();

    // Then an error will be raised because the backfill tool needs to create
    // the new compressed chunk, but the compression settings for the
    // hypertable in target are different than the chunk's compression settings
    // in the source.
    result.stderr(
        contains(
            r#"1: failed to create compressed chunk "_timescaledb_internal"."compress_hyper_2_7_chunk" for hypertable "public"."metrics""#
        ).and(contains(
            "2: Compression settings mismatch."
        )).and(contains(
            "Compression settings for the compressed chunk '_timescaledb_internal.compress_hyper_2_7_chunk'"
        )).and(contains(
            "in source are different than the settings for the hypertable 'public.metrics'"
        )).and(contains(
            r#"- SOURCE: CompressionSettings { segmentby: ["device_id", "label"], orderby: ["time"], orderby_desc: [false], orderby_nullsfirst: [false] }"#
        )).and(contains(
            r#"- TARGET: CompressionSettings { segmentby: ["device_id"], orderby: ["time"], orderby_desc: [false], orderby_nullsfirst: [false] }"#
        )));

    Ok(())
}

#[test]
fn assert_that_we_create_compressed_chunks_the_same_way_that_timescaledb_does() -> Result<()> {
    // Note: If this test fails, it means that something changed in how TimescaleDB behaves.

    let _ = pretty_env_logger::try_init();

    let docker = Cli::default();

    let source_container = docker.run(timescaledb(pg_version(), ts_version()));
    let target_container = docker.run(timescaledb(pg_version(), ts_version()));

    // Initial setup
    psql(
        &source_container,
        vec![
            PsqlInput::Sql(SETUP_HYPERTABLE),
            PsqlInput::Sql(r#"ALTER TABLE public.metrics ADD COLUMN "column< needs+quotes" TEXT"#),
            PsqlInput::Sql(
                r#"
    ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id,"column< needs+quotes"')"#,
            ),
            PsqlInput::Sql(INSERT_DATA_FOR_MAY),
        ],
    )?;

    copy_skeleton_schema(&source_container, &target_container)?;

    // Compress a chunk after skeleton schema copy
    psql(&source_container, vec![PsqlInput::Sql(COMPRESS_ONE_CHUNK)])?;

    run_backfill(TestConfigStage::new(
        &source_container,
        &target_container,
        "2023-07-01T00:00:00",
    ))
    .unwrap()
    .assert()
    .success()
    .stdout(contains(
        "Staged 5 chunks to copy.\nExecute the 'copy' command to migrate the data.",
    ));

    run_backfill(TestConfigCopy::new(&source_container, &target_container))
        .unwrap()
        .assert()
        .success()
        .stdout(contains(
            r#"Copied chunk "_timescaledb_internal"."_hyper_1_5_chunk" in"#,
        ));

    fn dump_table(database: &dyn HasConnectionString, table: &str) -> Result<String> {
        let output = Command::new("pg_dump")
            .args(["-d", database.connection_string().as_str()])
            .args(["--format", "plain"])
            .args(["--schema-only"])
            .args(["--table", table])
            .output()?;
        Ok(String::from_utf8(output.stdout).unwrap())
    }

    let source_table_definition = dump_table(
        &source_container,
        "_timescaledb_internal.compress_hyper_2_6_chunk",
    )?;
    let mut target_table_definition = dump_table(
        &target_container,
        "_timescaledb_internal.bf_compress_hyper_2_6_chunk",
    )?
    .replace("bf_compress_hyper_2_6_chunk", "compress_hyper_2_6_chunk")
    // Since we add the `bf` prefix the indexes names don't match.
    .replace(
        "compress_hyper_2_6_chunk_device_id_column< needs+quotes__idx",
        "compress_hyper_2_6_chunk_device_id_column< needs+quotes__ts_idx",
    );

    // When creating the compressed table with inheritance the indexes names
    // don't match because of a suffix.
    target_table_definition = target_table_definition.replace(
        "compress_hyper_2_6_chunk__compressed_hypertable_2_device_id_",
        "compress_hyper_2_6_chunk__compressed_hypertable_2_device_id_col",
    );

    if source_table_definition != target_table_definition {
        let patch = create_patch(&source_table_definition, &target_table_definition);
        let f = PatchFormatter::new().with_color();
        let diff_msg = format!(
            "Compressed chunk table definition not the same:\n```diff\n{}```",
            f.fmt_patch(&patch)
        );
        bail!(diff_msg);
    }

    let mut source_dbassert = DbAssert::new(&source_container.connection_string())
        .unwrap()
        .with_name("source");
    let mut target_dbassert = DbAssert::new(&target_container.connection_string())
        .unwrap()
        .with_name("target");

    source_dbassert.has_chunk_count("public", "metrics", 5);
    target_dbassert.has_chunk_count("public", "metrics", 5);

    Ok(())
}
