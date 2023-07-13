use crate::execute::copy_chunk;
use anyhow::{Context, Result};
use async_channel::Receiver;
use chrono::{DateTime, Utc};
use tokio::task::JoinSet;
use tokio_postgres::Config;
use tracing::{debug, trace};

use crate::{
    connect::{Source, Target},
    prepare::Chunk,
};

/// Spawns asyncronous tasks (workers) that wait until a `Chunk` is
/// passed via a `Receiver`, and copy the data of the `Chunk` from the source
/// to the target.
pub struct Pool {
    num_workers: usize,
    tasks: JoinSet<Result<()>>,
    rx: Receiver<Chunk>,
    until: DateTime<Utc>,
    source_config: Config,
    target_config: Config,
}

impl Pool {
    /// Create a new worker pool. Workers will each open a connection to
    /// both source and target with the given `Config`s, and processes the
    /// `Chunk`s received on the channel.
    pub async fn new(
        num_workers: usize,
        rx: Receiver<Chunk>,
        until: DateTime<Utc>,
        source_config: &Config,
        target_config: &Config,
    ) -> Self {
        let mut pool = Self {
            num_workers,
            tasks: JoinSet::new(),
            rx: rx.clone(),
            until,
            source_config: source_config.clone(),
            target_config: target_config.clone(),
        };

        for _ in 0..pool.num_workers {
            pool.add_worker().await;
        }

        pool
    }

    /// Wait until all the workers finish their execution. A worker will finish
    /// when the work items channel is close or when an error is encountered.
    pub async fn join(&mut self) -> Result<()> {
        while let Some(res) = self.tasks.join_next().await {
            res.with_context(|| "worker failed to execute to completion")?
                .with_context(|| "worker execution error")?;
        }
        trace!("workers finished successfully");
        Ok(())
    }

    async fn add_worker(&mut self) {
        let rx_clone = self.rx.clone();
        let source_config_clone = self.source_config.clone();
        let target_config_clone = self.target_config.clone();
        let until = self.until;
        self.tasks.spawn(async move {
            worker_run(&rx_clone, &source_config_clone, &target_config_clone, until).await
        });
    }
}

/// Execution loop of a worker. It receives chunks from a channel and executes
/// the `copy_chunk` function.
async fn worker_run(
    rx: &Receiver<Chunk>,
    source_config: &Config,
    target_config: &Config,
    until: DateTime<Utc>,
) -> Result<()> {
    let mut source = Source::connect(source_config).await?;
    let mut target = Target::connect(target_config).await?;

    while let Ok(chunk) = rx.recv().await {
        debug!("copying chunk: {chunk:?}");
        copy_chunk(&mut source, &mut target, chunk, &until).await?;
    }
    Ok(())
}
