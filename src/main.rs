use crate::connect::{Source, Target};
use crate::execute::copy_chunk;
use crate::logging::setup_logging;
use crate::prepare::get_chunk_information;
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use std::str::FromStr;
use tokio_postgres::Config;
use tracing::debug;
use crate::task::{claim_task, load_queue};

mod connect;
mod execute;
mod logging;
mod prepare;
mod task;

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

            task::init(&mut target).await?;
            load_queue(&mut target).await?;

            loop {
                let mut target_tx = target.client.transaction().await?;
                match claim_task(&mut target_tx, &args.until).await? {
                    Some(task) => {
                        debug!("claimed task: {task:?}");

                        target_tx.commit().await?;
                    },
                    None => {
                        target_tx.rollback().await?;
                        break;
                    }
                }
            }
            /*
            let chunks = get_chunk_information(&mut source, &args.until).await?;

            for chunk in chunks {
                debug!("copying chunk: {chunk:?}");
                copy_chunk(&mut source, &mut target, chunk).await?;
            }
            */
        }
        Command::Clean => {
            todo!()
        }
    }

    Ok(())
}
