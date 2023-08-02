use crate::connect::{Source, Target};
use crate::logging::setup_logging;
use crate::workers::PROCESSED_COUNT;
use anyhow::{Context, Result};
use clap::Parser;
use human_repr::HumanDuration;
use std::str::FromStr;
use std::sync::atomic::Ordering::Relaxed;
use tokio::time::Instant;
use tokio_postgres::Config;
use tracing::debug;

mod connect;
mod execute;
mod logging;
mod sql;
mod task;
mod timescale;
mod workers;

#[derive(Parser, Debug)]
pub struct StageConfig {
    /// Connection string to the source database
    #[arg(long)]
    source: String,

    /// Connection string to the target database
    #[arg(long)]
    target: String,

    /// Posix regular expression used to match `schema.table` for hypertables
    #[arg(short, long = "filter")]
    table_filter: Option<String>,

    /// The completion point to copy chunk data until
    #[arg(short, long)]
    until: Option<String>,

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
pub struct CleanConfig {
    /// Connection string to the target database
    #[arg(long)]
    target: String,
}

#[derive(Parser, Debug)]
pub enum Command {
    Stage(StageConfig),
    Copy(CopyConfig),
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
                task::get_and_assert_staged_task_count_greater_zero(&target_config).await?;

            println!("Preparing to copy {task_count} chunks");

            let mut pool = workers::Pool::new(
                args.parallelism.into(),
                &source_config,
                &target_config,
                task_count,
            )
            .await;

            pool.join().await.with_context(|| "worker pool error")?;
            println!(
                "Copied {} chunks in {}",
                PROCESSED_COUNT.load(Relaxed),
                start.elapsed().human_duration()
            );
            Ok(())
        }
        Command::Clean(args) => {
            let target_config = Config::from_str(&args.target)?;
            task::clean(&target_config).await?;
            println!("Cleaned target");
            Ok(())
        }
    }
}
