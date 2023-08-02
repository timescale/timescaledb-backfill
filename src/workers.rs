use crate::connect::{Source, Target};
use crate::execute::copy_chunk;
use crate::task::{claim_copy_task, complete_copy_task};
use anyhow::{Context, Result};
use human_repr::HumanDuration;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_postgres::Config;
use tracing::trace;

pub static PROCESSED_COUNT: AtomicU64 = AtomicU64::new(0);

/// Spawns asyncronous tasks (workers) that wait until a `Chunk` is
/// passed via a `Receiver`, and copy the data of the `Chunk` from the source
/// to the target.
pub struct Pool {
    num_workers: usize,
    tasks: JoinSet<Result<()>>,
    source_config: Config,
    target_config: Config,
    task_count: u64,
}

impl Pool {
    /// Create a new worker pool. Workers will each open a connection to
    /// both source and target with the given `Config`s, and processes the
    /// `Chunk`s received on the channel.
    pub async fn new(
        num_workers: usize,
        source_config: &Config,
        target_config: &Config,
        task_count: u64,
    ) -> Self {
        let mut pool = Self {
            num_workers,
            tasks: JoinSet::new(),
            source_config: source_config.clone(),
            target_config: target_config.clone(),
            task_count,
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
        let source_config_clone = self.source_config.clone();
        let target_config_clone = self.target_config.clone();
        let task_count = self.task_count;
        self.tasks.spawn(async move {
            worker_run(&source_config_clone, &target_config_clone, task_count).await
        });
    }
}

/// Execution loop of a worker. It receives chunks from a channel and executes
/// the `copy_chunk` function.
async fn worker_run(source_config: &Config, target_config: &Config, task_count: u64) -> Result<()> {
    let mut source = Source::connect(source_config).await?;
    let mut target = Target::connect(target_config).await?;

    loop {
        let target_tx = target.client.transaction().await?;
        match claim_copy_task(&target_tx)
            .await
            .with_context(|| "error claiming copy task")?
        {
            Some(copy_task) => {
                let start = Instant::now();

                let source_tx = source.transaction().await?;
                copy_chunk(&source_tx, &target_tx, &copy_task).await?;
                complete_copy_task(&target_tx, &copy_task).await?;
                source_tx.commit().await?;

                let prev = PROCESSED_COUNT.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[{}/{}] Copied chunk {} in {}",
                    prev + 1,
                    task_count,
                    copy_task.source_chunk.quoted_name(),
                    start.elapsed().human_duration()
                );
            }
            None => break,
        }
        target_tx.commit().await?;
    }
    Ok(())
}
