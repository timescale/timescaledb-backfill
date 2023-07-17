use crate::connect::Source;
use crate::logging::setup_logging;
use crate::prepare::{get_chunk_information, get_hypertable_information};
use crate::timescale::{CompressionState::CompressedHypertable, Hypertable};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::str::FromStr;
use tokio::task::JoinSet;
use tokio_postgres::Config;
use tracing::debug;

mod connect;
mod execute;
mod logging;
mod prepare;
mod timescale;
mod work_items;
mod workers;

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

    /// The completion point to copy chunk data until
    #[arg(short, long)]
    until: DateTime<Utc>,
}

#[derive(Parser, Debug)]
pub enum Command {
    Copy(CopyConfig),
    Clean,
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
        Command::Copy(args) => {
            let source_config = Config::from_str(&args.source)?;
            let target_config = Config::from_str(&args.target)?;

            let mut source = Source::connect(&source_config).await?;
            let hypertables = get_hypertable_information(&mut source).await?;
            abort_if_hypertable_setup_not_supported(&hypertables);

            let hypertables = get_hypertable_information(&mut source).await?;

            abort_if_hypertable_setup_not_supported(&hypertables);

            let chunks = get_chunk_information(&mut source, &args.until).await?;

            let mut work_items_manager = work_items::Manager::new(chunks);

            let mut pool = workers::Pool::new(
                args.parallelism.into(),
                work_items_manager.rx.clone(),
                args.until,
                &source_config,
                &target_config,
            )
            .await;

            let mut tasks = JoinSet::new();
            tasks.spawn(async move {
                work_items_manager
                    .dispatch_work()
                    .await
                    .with_context(|| "dispatch work error")
            });
            tasks.spawn(async move { pool.join().await.with_context(|| "worker pool error") });

            while let Some(res) = tasks.join_next().await {
                res??;
            }
        }
        Command::Clean => {
            todo!()
        }
    }

    Ok(())
}

fn abort_if_hypertable_setup_not_supported(hypertables: &[Hypertable]) {
    for hypertable in hypertables {
        if hypertable.compression_state == CompressedHypertable {
            // We don't care about the hypertable containing compressed data
            continue;
        }
        for dimension in &hypertable.dimensions {
            if &dimension.column_type != "timestamp with time zone" {
                todo!(
                    "Cannot handle hypertables with non-timestamptz time column: {}",
                    dimension.column_type
                )
            }
        }
    }
}
