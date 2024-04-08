use crate::connect::{Source, Target};
use crate::execute::TOTAL_BYTES_COPIED;
use crate::logging::setup_logging;
use crate::task::TaskType;
use crate::timescale::{
    fetch_tsdb_version, initialize_source_proc_schema, initialize_target_proc_schema,
};
use crate::workers::{PoolMessage, PROCESSED_COUNT};
use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use console::Term;
use futures_lite::FutureExt;
use human_repr::{HumanCount, HumanDuration};
use once_cell::sync::Lazy;
use semver::{Version, VersionReq};
use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use telemetry::{report, Telemetry};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Duration, Instant};
use tokio_postgres::Config;
use tracing::{debug, error, warn};
use verify::FAILED_VERIFICATIONS;

mod caggs;
mod connect;
mod execute;
mod logging;
mod sql;
mod storage;
mod task;
mod telemetry;
mod timescale;
mod verify;
mod workers;

static CTRLC_COUNT: AtomicU32 = AtomicU32::new(0);

static TERM: Lazy<Term> = Lazy::new(Term::stdout);

thread_local! {
    static BACKTRACE: RefCell<Option<Backtrace>> = RefCell::new(None);
}

#[derive(Debug)]
struct PanicError {
    reason: String,
    backtrace: Backtrace,
}

impl std::error::Error for PanicError {}

impl Display for PanicError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "caught panic: {}", self.reason)
    }
}

#[derive(Args, Debug)]
pub struct StageConfig {
    /// Connection string to the source database.
    #[arg(long)]
    source: String,

    /// Connection string to the target database.
    #[arg(long)]
    target: String,

    /// The completion point to copy chunk data until. The backfill process
    /// will copy all chunk rows where the time dimension column is less than
    /// this value.
    ///
    /// It accepts any representation of a valid time dimension value:
    ///
    /// - Timestamp: TIMESTAMP, TIMESTAMPTZ
    /// - Date: DATE
    /// - Integer: SMALLINT, INT, BIGINT
    ///
    /// The value will be parsed to the correct type as defined by the time
    /// dimension column type.
    ///
    /// A combination of `--until` and `--filter` can be used to specify
    /// different completion points for different tables. For example, in the
    /// case with hypertables that use auto-increment integers for their time
    /// dimensions, or a combination of hypertables with some using timestamp
    /// and others using integer.
    ///
    /// timescaledb-backfill stage --filter epoch_schema.* --until 1692696465
    /// timescaledb-backfill stage --filter public.table_with_auto_increment_integer --until 424242
    /// timescaledb-backfill stage --filter public.table_with_timestamptz --until '2016-02-01T18:20:00'
    #[arg(short, long, verbatim_doc_comment)]
    until: String,

    /// The starting point to copy chunk data from. The backfill process
    /// will copy all chunk rows where the time dimension column is greater
    /// than or equal to this value.
    ///
    /// If not specify, the data will be copy from the beggining of time up
    /// to the completion point defined by the mandatory `--until` flag.
    ///
    /// It accepts any representation of a valid time dimension value:
    ///
    /// - Timestamp: TIMESTAMP, TIMESTAMPTZ
    /// - Date: DATE
    /// - Integer: SMALLINT, INT, BIGINT
    ///
    /// The value will be parsed to the correct type as defined by the time
    /// dimension column type.
    ///
    /// Refer to the `--until` flag documentation if you require different
    /// values or value type for specific tables or schemas.
    #[arg(short = 'F', long, verbatim_doc_comment)]
    from: Option<String>,

    /// Posix regular expression used to match `schema.table` for hypertables
    /// and `schema.view` for continuous aggregates
    #[arg(short, long, verbatim_doc_comment)]
    filter: Option<String>,

    /// If filter is provided, automatically include continuous aggregates and
    /// hypertables which depend upon hypertables and continuous aggregates
    /// that match the filter
    #[arg(short = 'U', long = "cascade-up", verbatim_doc_comment)]
    cascade_up: bool,

    /// If filter is provided, automatically include continuous aggregates and
    /// hypertables on which continuous aggregates matching the filter depend.
    #[arg(short = 'D', long = "cascade-down", verbatim_doc_comment)]
    cascade_down: bool,

    /// A postgres snapshot exported from source to use when copying.
    #[arg(short, long)]
    snapshot: Option<String>,

    /// Ignore the fact that this tool is not compatible with TimescaleDB 2.14.
    #[arg(long, default_value_t = false)]
    ignore_tsdb_214_compatibility: bool,
}

#[derive(Args, Debug)]
pub struct CopyConfig {
    /// Connection string to the source database.
    #[arg(long)]
    source: String,

    /// Connection string to the target database.
    #[arg(long)]
    target: String,

    /// Parallelism for copy.
    #[arg(short, long, default_value_t = 8)]
    parallelism: u8,

    /// Ignore the fact that this tool is not compatible with TimescaleDB 2.14.
    #[arg(long, default_value_t = false)]
    ignore_tsdb_214_compatibility: bool,
}

#[derive(Args, Debug)]
pub struct VerifyConfig {
    /// Connection string to the source database.
    #[arg(long)]
    source: String,

    /// Connection string to the target database.
    #[arg(long)]
    target: String,

    /// Parallelism for verification.
    #[arg(short, long, default_value_t = 8)]
    parallelism: u8,
}

#[derive(Args, Debug)]
pub struct RefreshCaggsConfig {
    /// Connection string to the source database.
    #[arg(long)]
    source: String,

    /// Connection string to the target database.
    #[arg(long)]
    target: String,

    /// Posix regular expression used to match `schema.view`.
    #[arg(short, long)]
    filter: Option<String>,

    /// If filter is provided, automatically include continuous aggregates
    /// which depends upon continuous aggregates that match the filter.
    #[arg(short = 'U', long = "cascade-up", verbatim_doc_comment)]
    cascade_up: bool,

    /// If filter is provided, automatically include continuous aggregates on
    /// which continuous aggregates matching the filter depend.
    #[arg(short = 'D', long = "cascade-down", verbatim_doc_comment)]
    cascade_down: bool,
}

#[derive(Args, Debug)]
pub struct CleanConfig {
    /// Connection string to the target database.
    #[arg(long)]
    target: String,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Creates copy tasks for hypertable and continuous aggregates chunks.
    ///
    /// The tasks are based on the specified completion point (--until). An
    /// optional filter (--filter) can be used to refine the hypertables and
    /// continuous aggregates targeted for staging.
    #[command(verbatim_doc_comment)]
    Stage(StageConfig),
    /// Processes the tasks created during the staging phase.
    ///
    /// Copies the corresponding hypertable and continuous aggregates chunks to
    /// the target Timescale service.
    #[command(verbatim_doc_comment)]
    Copy(CopyConfig),
    /// Checks for discrepancies between the source and target chunks' data.
    ///
    /// Compares the results of the count for each chunk's table, as well as
    /// per-column count, max, min, and sum values (when applicable, depending
    /// on the column data type).
    #[command(verbatim_doc_comment)]
    Verify(VerifyConfig),
    /// Removes the administrative schema (__backfill).
    ///
    /// The administrative schema is used to store the tasks created during
    /// the stage command. Any tasks still pending won't be able to be resumed
    /// and will have to be staged again if the schema is deleted.
    #[command(verbatim_doc_comment)]
    Clean(CleanConfig),
    /// Refreshes the continuous aggregates of the target system.
    ///
    /// It covers the period from the last refresh in the target to the last
    /// refresh in the source, solving the problem of continuous aggregates
    /// being outdated beyond the coverage of the refresh policies.
    #[command(verbatim_doc_comment)]
    RefreshCaggs(RefreshCaggsConfig),
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Stage(_) => write!(f, "stage"),
            Command::Copy(_) => write!(f, "copy"),
            Command::Verify(_) => write!(f, "verify"),
            Command::Clean(_) => write!(f, "clean"),
            Command::RefreshCaggs(_) => write!(f, "refresh_caggs"),
        }
    }
}

/// The timescaledb-backfill tool is a command-line utility designed to support
/// migrations from Timescale instances by copying historic data from one
/// database to another ("backfilling"). timescaledb-backfill efficiently
/// copies hypertable and continuous aggregates chunks directly, without the
/// need for intermediate storage or decompressing compressed chunks. It
/// operates transactionally, ensuring data integrity throughout the migration
/// process.
///
/// For more information visit the documentation page at:
///
/// https://docs.timescale.com/migrate/latest/dual-write-and-backfill/timescaledb-backfill/
#[derive(Parser, Debug)]
#[command(author, version, about, long_about, verbatim_doc_comment)]
pub struct CliArgs {
    #[command(subcommand)]
    command: Command,
    #[arg(long, default_value_t = false, global = true)]
    disable_telemetry: bool,
}

#[derive(Debug)]
enum CommandResult {
    Stage(StageResult),
    Copy(CopyResult),
    Verify(VerifyResult),
    Clean(CleanResult),
    RefreshCaggs(RefreshCaggsResult),
}

#[derive(Debug)]
struct StageResult {
    staged_tasks: usize,
}

#[derive(Debug)]
struct CopyResult {
    tasks_finished: usize,
    tasks_total_bytes: usize,
}

#[derive(Debug)]
struct VerifyResult {
    tasks_finished: usize,
    tasks_failures: usize,
}

#[derive(Debug)]
struct CleanResult {}

#[derive(Debug)]
struct RefreshCaggsResult {
    refreshed_caggs: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let args = CliArgs::parse();

    std::panic::set_hook(Box::new(|_| {
        // Smuggle the backtrace on panic, see: https://stackoverflow.com/a/73711057/867412
        let trace = Backtrace::force_capture();
        BACKTRACE.with(move |b| b.borrow_mut().replace(trace));
    }));

    let start = Instant::now();

    let command_result = match AssertUnwindSafe(run(&args)).catch_unwind().await {
        Ok(r) => r,
        Err(e) => {
            let backtrace = BACKTRACE.with(|b| b.borrow_mut().take()).unwrap();

            // The content of the error is the object which was passed to
            // `panic!()`, so effectively it can be anything. It can always
            // be downcast to &str.
            let reason = match e.downcast::<&str>() {
                Ok(r) => (*r).to_string(),
                Err(_) => String::from("unknown"),
            };
            Err(anyhow!(PanicError { reason, backtrace }))
        }
    };

    let command_duration = start.elapsed();

    if let Ok(command_result) = command_result.as_ref() {
        print_summary(command_result, command_duration)?;
    }

    if !args.disable_telemetry {
        let telemetry_report =
            report_telemetry(&args.command, command_duration, command_result.as_ref()).await;
        // We don't want to return an error to users if we fail to write
        // telemetry.
        if cfg!(debug_assertions) {
            telemetry_report?;
        }
    }

    if let Err(err) = command_result {
        bail!(err);
    }

    Ok(())
}

async fn run(args: &CliArgs) -> Result<CommandResult> {
    match args.command {
        Command::Stage(ref args) => stage(args).await.map(CommandResult::Stage),
        Command::Copy(ref args) => copy(args).await.map(CommandResult::Copy),
        Command::Verify(ref args) => verify(args).await.map(CommandResult::Verify),
        Command::Clean(ref args) => clean(args).await.map(CommandResult::Clean),
        Command::RefreshCaggs(ref args) => {
            refresh_caggs(args).await.map(CommandResult::RefreshCaggs)
        }
    }
}

/// Spawns a task to intercept the ctrl-c signal and publish the action which
/// should be taken to an mpsc channel, returning the rx end of the channel.
async fn create_ctrl_c_handler() -> Result<UnboundedReceiver<PoolMessage>> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    tokio::spawn(async move {
        loop {
            tokio::signal::ctrl_c().await.unwrap();
            debug!("received ctrl-c");
            let count = CTRLC_COUNT.fetch_add(1, Relaxed);
            let result = if count == 0 {
                debug!("sending graceful shutdown");
                TERM.clear_line()?;
                TERM.write_line("Shutting down, waiting for in-progress copies to complete...")?;
                sender.send(PoolMessage::GracefulShutdown)
            } else {
                debug!("sending hard shutdown");
                TERM.clear_line()?;
                TERM.write_line("Terminating immediately.")?;
                sender.send(PoolMessage::HardShutdown)
            };
            if let Err(e) = result {
                error!("unable to send message: {}", e);
                return Err(anyhow!("unable to send message: {}", e)) as Result<()>;
            };
        }
    });

    Ok(receiver)
}

fn connection_config(constr: &str) -> Result<Config> {
    let mut config = Config::from_str(constr)?;
    Ok(config.application_name("timescaledb-backfill").to_owned())
}

async fn stage(config: &StageConfig) -> Result<StageResult> {
    let source_config = connection_config(&config.source)?;
    let target_config = connection_config(&config.target)?;

    let mut source = Source::connect(&source_config).await?;
    let mut target = Target::connect(&target_config).await?;

    abort_on_incompatible_timescaledb_versions(
        &mut source,
        &mut target,
        config.ignore_tsdb_214_compatibility,
    )
    .await?;

    initialize_source_proc_schema(&mut source).await?;
    initialize_target_proc_schema(&target).await?;

    let staged_tasks = task::load_queue(
        &mut source,
        &mut target,
        config.filter.as_ref(),
        config.cascade_up,
        config.cascade_down,
        &config.until,
        config.from.as_ref(),
        config.snapshot.as_ref(),
    )
    .await?;

    Ok(StageResult { staged_tasks })
}

async fn abort_on_incompatible_timescaledb_versions(
    source: &mut Source,
    target: &mut Target,
    ignore_214_warning: bool,
) -> Result<()> {
    let source_version = fetch_tsdb_version(&mut source.client).await?;
    let target_version = fetch_tsdb_version(&mut target.client).await?;

    let ge_214 = VersionReq::parse(">=2.14.0").unwrap();

    let run_check = |name: &str, version: &str| -> Result<()> {
        if let Ok(v) = Version::parse(version) {
            if ge_214.matches(&v) {
                if ignore_214_warning {
                    warn!("ignoring timescaledb version >= 2.14.0, {name}={version}")
                } else {
                    bail!("timescaledb-backfill does not yet support timescaledb >= 2.14.0, {name}={version}")
                }
            }
        }
        Ok(())
    };

    run_check("source", &source_version)?;
    run_check("target", &target_version)?;

    if source_version != target_version {
        bail!("timescaledb extension version is different in source and target: source={source_version}, target={target_version}");
    }
    Ok(())
}

async fn copy(config: &CopyConfig) -> Result<CopyResult> {
    let source_config = connection_config(&config.source)?;
    let target_config = connection_config(&config.target)?;

    // Enclose DB clients in a new block to ensure they go out of scope
    // as soon as possible. This helps to drop the database connections
    // promptly.
    let task_count = {
        let mut source = Source::connect(&source_config).await?;
        initialize_source_proc_schema(&mut source).await?;
        let mut target = Target::connect(&target_config).await?;
        initialize_target_proc_schema(&target).await?;
        abort_on_incompatible_timescaledb_versions(
            &mut source,
            &mut target,
            config.ignore_tsdb_214_compatibility,
        )
        .await?;
        task::get_and_assert_staged_task_count_greater_zero(&target, TaskType::Copy).await?
    };

    TERM.write_line(&format!(
        "Copying {task_count} chunks with {} workers",
        config.parallelism
    ))?;

    let receiver = create_ctrl_c_handler().await?;

    let pool = workers::Pool::new(
        config.parallelism.into(),
        &source_config,
        &target_config,
        task_count,
        receiver,
        TaskType::Copy,
    )
    .await?;

    pool.join().await.with_context(|| "worker pool error")?;

    Ok(CopyResult {
        tasks_finished: PROCESSED_COUNT.load(Relaxed),
        tasks_total_bytes: TOTAL_BYTES_COPIED.load(Relaxed),
    })
}

async fn verify(config: &VerifyConfig) -> Result<VerifyResult> {
    let source_config = connection_config(&config.source)?;
    let target_config = connection_config(&config.target)?;

    // Enclose DB clients in a new block to ensure they go out of scope
    // as soon as possible. This helps to drop the database connections
    // promptly.
    {
        let mut source = Source::connect(&source_config).await?;
        initialize_source_proc_schema(&mut source).await?;
    }

    let task_count = {
        let target = Target::connect(&target_config).await?;
        initialize_target_proc_schema(&target).await?;
        task::get_and_assert_staged_task_count_greater_zero(&target, TaskType::Verify).await?
    };

    TERM.write_line(&format!(
        "Verifying {task_count} chunks with {} workers",
        config.parallelism
    ))?;

    let receiver = create_ctrl_c_handler().await?;

    let pool = workers::Pool::new(
        config.parallelism.into(),
        &source_config,
        &target_config,
        task_count,
        receiver,
        TaskType::Verify,
    )
    .await?;

    pool.join().await.with_context(|| "worker pool error")?;

    Ok(VerifyResult {
        tasks_finished: PROCESSED_COUNT.load(Relaxed),
        tasks_failures: FAILED_VERIFICATIONS.load(Relaxed),
    })
}

async fn clean(config: &CleanConfig) -> Result<CleanResult> {
    let target_config = connection_config(&config.target)?;
    task::clean(&target_config).await?;
    Ok(CleanResult {})
}

async fn refresh_caggs(config: &RefreshCaggsConfig) -> Result<RefreshCaggsResult> {
    let source = Source::connect(&connection_config(&config.source)?).await?;
    let target = Target::connect(&connection_config(&config.target)?).await?;
    let refreshed_caggs = caggs::refresh_caggs(
        &source,
        &target,
        config.filter.as_ref(),
        config.cascade_up,
        config.cascade_down,
    )
    .await?;
    Ok(RefreshCaggsResult { refreshed_caggs })
}

fn print_summary(command_result: &CommandResult, duration: Duration) -> Result<()> {
    match command_result {
        CommandResult::Stage(result) => TERM
            .write_line(&format!(
                "Staged {} chunks to copy.\nExecute the 'copy' command to migrate the data.",
                result.staged_tasks,
            ))
            .map_err(anyhow::Error::from),
        CommandResult::Copy(result) => TERM
            .write_line(&format!(
                "Copied {} from {} chunks in {}.\nExecute the 'verify' command to assert data integrity.",
                result.tasks_total_bytes.human_count_bytes(),
                result.tasks_finished,
                duration.human_duration(),
            ))
            .map_err(anyhow::Error::from),
        CommandResult::Verify(result) => TERM
            .write_line(&format!(
                "Verifed {} chunks in {}.\nExecute the 'clean' command to remove the backfill administrative schema from the target database.",
                result.tasks_finished,
                duration.human_duration(),
            ))
            .map_err(anyhow::Error::from),
        CommandResult::Clean(_) => TERM
            .write_line("Removed backfill administrative schema from target database")
            .map_err(anyhow::Error::from),
        CommandResult::RefreshCaggs(_) => TERM
            .write_line("Refreshed continuous aggregates")
            .map_err(anyhow::Error::from),
    }
}

async fn target_from_command(command: &Command) -> Result<Target> {
    let raw_target_config = match command {
        Command::Stage(args) => &args.target,
        Command::Verify(args) => &args.target,
        Command::Copy(args) => &args.target,
        Command::RefreshCaggs(args) => &args.target,
        Command::Clean(args) => &args.target,
    };
    let target_config = connection_config(raw_target_config)?;
    Target::connect(&target_config).await
}

async fn source_from_command(command: &Command) -> Result<Option<Source>> {
    let raw_source_config = match command {
        Command::Stage(args) => &args.source,
        Command::Copy(args) => &args.source,
        Command::Verify(args) => &args.source,
        Command::RefreshCaggs(args) => &args.source,
        Command::Clean(_) => return Ok(None),
    };
    let source_config = connection_config(raw_source_config)?;
    Ok(Some(Source::connect(&source_config).await?))
}

async fn report_telemetry(
    command: &Command,
    command_duration: Duration,
    command_result: Result<&CommandResult, &anyhow::Error>,
) -> Result<()> {
    let mut target = target_from_command(command).await?;

    let mut telemetry = Telemetry::from_target_session(&mut target)
        .await?
        .with_command(command.to_string())
        .with_command_duration(command_duration);

    // If there's any issue connecting to source we keep going and report
    // the available telemetry.
    if let Ok(Some(mut source)) = source_from_command(command).await {
        telemetry = telemetry.with_source_db(&mut source).await;
    }

    telemetry = match command_result {
        Ok(command_result) => match command_result {
            CommandResult::Copy(result) => {
                telemetry.with_copied_tasks(result.tasks_finished, result.tasks_total_bytes)
            }
            CommandResult::Stage(result) => telemetry.with_staged_tasks(result.staged_tasks),
            CommandResult::Verify(result) => {
                telemetry.with_verified_tasks(result.tasks_finished, result.tasks_failures)
            }
            CommandResult::RefreshCaggs(result) => {
                telemetry.with_refreshed_caggs(result.refreshed_caggs)
            }
            CommandResult::Clean(_) => return Ok(()),
        },
        Err(e) => telemetry.with_error(e),
    };
    report(&target, &telemetry).await?;
    Ok(())
}
