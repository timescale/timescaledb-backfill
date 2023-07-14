use crate::connect::{Source, Target};
use crate::execute::copy_chunk;
use crate::logging::setup_logging;
use crate::prepare::CompressionState::CompressedHypertable;
use crate::prepare::{get_chunk_information, get_hypertable_information, Hypertable};
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use std::str::FromStr;
use tokio_postgres::Config;
use tracing::debug;

mod connect;
mod execute;
mod logging;
mod prepare;

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
            let mut target = Target::connect(&target_config).await?;

            let hypertables = get_hypertable_information(&mut source).await?;

            abort_if_hypertable_setup_not_supported(&hypertables);

            let chunks = get_chunk_information(&mut source, &args.until).await?;

            for chunk in chunks {
                debug!("copying chunk: {chunk:?}");
                copy_chunk(&mut source, &mut target, chunk).await?;
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
        if hypertable.dimensions.len() > 1 {
            todo!("Cannot handle hypertables with multiple dimensions")
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
