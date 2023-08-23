use crate::connect::{Source, Target};
use crate::execute::TOTAL_BYTES_COPIED;
use crate::logging::setup_logging;
use crate::task::TaskType;
use crate::workers::{PoolMessage, PROCESSED_COUNT};
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use console::Term;
use human_repr::{HumanCount, HumanDuration};
use once_cell::sync::Lazy;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;
use tokio_postgres::Config;
use tracing::{debug, error};

mod connect;
mod execute;
mod logging;
mod sql;
mod task;
mod timescale;
mod verify;
mod workers;

static CTRLC_COUNT: AtomicU32 = AtomicU32::new(0);

static TERM: Lazy<Term> = Lazy::new(Term::stdout);

#[derive(Parser, Debug)]
pub struct StageConfig {
    /// Connection string to the source database
    #[arg(long)]
    source: String,

    /// Connection string to the target database
    #[arg(long)]
    target: String,

    /// The completion point to copy chunk data until. The backfill process
    /// will copy all chunk rows where the time dimension column is less than
    /// or equal to this value.
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
    /// timescaledb-backfill stage --filter public.table_with_timestampt --until '2016-02-01T18:20:00'
    #[arg(short, long)]
    until: String,

    /// Posix regular expression used to match `schema.table` for hypertables
    #[arg(short, long = "filter")]
    table_filter: Option<String>,

    /// A postgres snapshot exported from source to use when copying
    #[arg(short, long)]
    snapshot: Option<String>,
}

#[derive(Parser, Debug)]
pub struct CopyConfig {
    /// Connection string to the source database
    #[arg(long)]
    source: String,

    /// Connection string to the target database
    #[arg(long)]
    target: String,

    /// Parallelism for copy
    #[arg(short, long, default_value_t = 8)]
    parallelism: u8,
}

#[derive(Parser, Debug)]
pub struct VerifyConfig {
    /// Connection string to the source database
    #[arg(long)]
    source: String,

    /// Connection string to the target database
    #[arg(long)]
    target: String,

    /// Parallelism for verification
    #[arg(short, long, default_value_t = 8)]
    parallelism: u8,
}

#[derive(Parser, Debug)]
pub struct CleanConfig {
    /// Connection string to the target database
    #[arg(long)]
    target: String,
}

#[derive(Parser, Debug)]
pub enum Command {
    Stage(StageConfig),
    Copy(CopyConfig),
    Verify(VerifyConfig),
    Clean(CleanConfig),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let args = Args::parse();
    debug!("{args:?}");

    match args.command {
        Command::Stage(args) => {
            let source_config = Config::from_str(&args.source)?;
            let target_config = Config::from_str(&args.target)?;

            let mut source = Source::connect(&source_config).await?;
            let mut target = Target::connect(&target_config).await?;

            task::load_queue(
                &mut source,
                &mut target,
                args.table_filter,
                args.until,
                args.snapshot,
            )
            .await
        }
        Command::Copy(args) => {
            let start = Instant::now();
            let source_config = Config::from_str(&args.source)?;
            let target_config = Config::from_str(&args.target)?;

            let task_count =
                task::get_and_assert_staged_task_count_greater_zero(&target_config, TaskType::Copy)
                    .await?;

            TERM.write_line(&format!(
                "Copying {task_count} chunks with {} workers",
                args.parallelism
            ))?;

            let receiver = create_ctrl_c_handler().await?;

            let pool = workers::Pool::new(
                args.parallelism.into(),
                &source_config,
                &target_config,
                task_count,
                receiver,
                TaskType::Copy,
            )
            .await?;

            pool.join().await.with_context(|| "worker pool error")?;
            TERM.write_line(&format!(
                "Copied {} from {} chunks in {}",
                TOTAL_BYTES_COPIED.load(Relaxed).human_count_bytes(),
                PROCESSED_COUNT.load(Relaxed),
                start.elapsed().human_duration()
            ))?;
            Ok(())
        }
        Command::Verify(args) => {
            let start = Instant::now();
            let source_config = Config::from_str(&args.source)?;
            let target_config = Config::from_str(&args.target)?;

            let task_count = task::get_and_assert_staged_task_count_greater_zero(
                &target_config,
                TaskType::Verify,
            )
            .await?;

            TERM.write_line(&format!(
                "Verifying {task_count} chunks with {} workers",
                args.parallelism
            ))?;

            let receiver = create_ctrl_c_handler().await?;

            let pool = workers::Pool::new(
                args.parallelism.into(),
                &source_config,
                &target_config,
                task_count,
                receiver,
                TaskType::Verify,
            )
            .await?;

            pool.join().await.with_context(|| "worker pool error")?;

            TERM.write_line(&format!(
                "Verifed {task_count} chunks in {}",
                start.elapsed().human_duration(),
            ))?;

            Ok(())
        }
        Command::Clean(args) => {
            let target_config = Config::from_str(&args.target)?;
            task::clean(&target_config).await?;
            TERM.write_line("Cleaned target")?;
            Ok(())
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
