use crate::connect::{Source, Target};
use crate::execute::{chunk_exists, copy_chunk};
use crate::task::{
    claim_copy_task, claim_verify_task, complete_copy_task, complete_verify_task, TaskType,
};
use crate::timescale::QuotedName;
use crate::verify::{verify_chunk_data, VerificationError};
use crate::workers::TaskResult::{NoItem, Processed};
use crate::TERM;
use anyhow::{bail, Context, Result};
use human_repr::{HumanDuration, HumanThroughput};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_postgres::Config;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

pub static PROCESSED_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Spawns asyncronous tasks (workers) that wait until a `Chunk` is
/// passed via a `Receiver`, and copy the data of the `Chunk` from the source
/// to the target.
pub struct Pool {
    num_workers: usize,
    tasks: JoinSet<Result<WorkerResult>>,
}

#[derive(Clone, Debug)]
pub enum PoolMessage {
    GracefulShutdown,
    HardShutdown,
}

#[derive(Clone, Debug)]
struct GracefulShutdown(CancellationToken);

#[derive(Clone, Debug)]
struct HardShutdown(CancellationToken);

impl Pool {
    /// Create a new worker pool. Each worker opens a connection to the source
    /// and target with the given `Config`s, and fetched tasks to process from
    /// the target database.
    /// The worker pool can be instructed to perform a graceful or hard
    /// shutdown through the shutdown channel.
    pub async fn new(
        num_workers: usize,
        source_config: &Config,
        target_config: &Config,
        task_count: u64,
        shutdown: UnboundedReceiver<PoolMessage>,
        task: TaskType,
    ) -> Result<Self> {
        let mut pool = Self {
            num_workers,
            tasks: JoinSet::new(),
        };

        let graceful_shutdown = GracefulShutdown(CancellationToken::new());
        let hard_shutdown = HardShutdown(CancellationToken::new());

        {
            let graceful_shutdown = graceful_shutdown.clone();
            let hard_shutdown = hard_shutdown.clone();
            tokio::spawn(async move {
                Self::handle_shutdown(shutdown, graceful_shutdown, hard_shutdown).await;
            });
        }

        for _ in 0..pool.num_workers {
            let worker = Worker::new(
                source_config,
                target_config,
                task_count,
                graceful_shutdown.clone(),
                hard_shutdown.clone(),
            )
            .await?;
            let task_type = task.clone();
            pool.tasks.spawn(async move { worker.run(task_type).await });
        }

        Ok(pool)
    }

    /// Wait for the worker pool to finish executing, either because all jobs finished,
    /// or because the worker pool was requested to shut down.
    pub async fn join(mut self) -> Result<()> {
        while let Some(res) = self.tasks.join_next().await {
            res.with_context(|| "worker failed to execute to completion")?
                .with_context(|| "worker execution error")?;
        }
        trace!("workers finished successfully");
        Ok(())
    }

    async fn handle_shutdown(
        mut shutdown: UnboundedReceiver<PoolMessage>,
        graceful_shutdown: GracefulShutdown,
        hard_shutdown: HardShutdown,
    ) {
        loop {
            if let Some(message) = shutdown.recv().await {
                debug!("received shutdown message: {message:?}");
                match message {
                    PoolMessage::GracefulShutdown => {
                        graceful_shutdown.0.cancel();
                    }
                    PoolMessage::HardShutdown => {
                        hard_shutdown.0.cancel();
                    }
                }
            } else {
                debug!("the shutdown channel was closed");
                break;
            }
        }
    }
}

struct Worker {
    source: Source,
    target: Target,
    task_count: u64,
    graceful_shutdown: GracefulShutdown,
    hard_shutdown: HardShutdown,
}

#[derive(Clone, Debug)]
enum WorkerResult {
    HardShutdown,
    GracefulShutdown,
    Complete,
}

#[derive(Debug, PartialEq)]
enum TaskResult {
    Processed,
    NoItem,
}

impl Worker {
    pub async fn new(
        source_config: &Config,
        target_config: &Config,
        task_count: u64,
        graceful_shutdown: GracefulShutdown,
        hard_shutdown: HardShutdown,
    ) -> Result<Self> {
        let source = Source::connect(source_config).await?;
        let target = Target::connect(target_config).await?;
        Ok(Self {
            source,
            target,
            task_count,
            graceful_shutdown,
            hard_shutdown,
        })
    }

    /// Execution loop of a worker.
    pub async fn run(self, task: TaskType) -> Result<WorkerResult> {
        let hard_shutdown = &self.hard_shutdown;
        let graceful_shutdown = self.graceful_shutdown;
        let source = self.source;
        let target = self.target;
        let task_count = self.task_count;

        tokio::select! {
            _ = hard_shutdown.0.cancelled() => {
                debug!("worker received hard shutdown");
                Ok(WorkerResult::HardShutdown)
            },
            // Note: we spawn run_inner as a separate task, to prevent it from blocking this select.
            res = tokio::spawn(Self::run_inner(graceful_shutdown.clone(), source, target, task_count, task)) => {
                match res {
                    Ok(r) => r,
                    Err(join_error) => {
                        if join_error.is_panic() {
                            let panic_payload = join_error.into_panic();
                            let err: String = {
                                if let Some(s) = panic_payload.downcast_ref::<String>() {
                                    format!("Task panicked: {}", s)
                                } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                    format!("Task panicked: {}", s)
                                } else {
                                    format!("Task panicked: {:?}", panic_payload)
                                }
                            };
                            bail!(err)
                        }
                        Err(join_error.into())
                    }
                }
            }
        }
    }

    async fn run_inner(
        graceful_shutdown: GracefulShutdown,
        mut source: Source,
        mut target: Target,
        task_count: u64,
        task: TaskType,
    ) -> Result<WorkerResult> {
        loop {
            if graceful_shutdown.0.is_cancelled() {
                debug!("worker received shutdown signal");
                return Ok(WorkerResult::GracefulShutdown);
            } else {
                let result = match task {
                    TaskType::Copy => {
                        Self::process_copy_task(&mut source, &mut target, task_count).await?
                    }
                    TaskType::Verify => {
                        Self::process_verify_task(&mut source, &mut target, task_count).await?
                    }
                };
                if result == NoItem {
                    debug!("worker has no more work to do, stopping");
                    break;
                }
            }
        }
        Ok(WorkerResult::Complete)
    }

    /// Processes a Copy task in the target database's task queue. When no
    /// tasks are available in the task queue, returns `TaskResult::NoItem`.
    async fn process_copy_task(
        source: &mut Source,
        target: &mut Target,
        task_count: u64,
    ) -> Result<TaskResult> {
        let target_tx = target.client.transaction().await?;
        let result = match claim_copy_task(&target_tx)
            .await
            .with_context(|| "error claiming copy task")?
        {
            None => NoItem,
            Some(copy_task) => {
                let start = Instant::now();

                let source_tx = source.transaction().await?;

                let copy_result_message: String =
                    // The chunk could've been deleted by a retention job while
                    // waiting to be processed.
                    if !chunk_exists(&source_tx, &copy_task.source_chunk).await? {
                        format!(
                            "Skipping chunk {} because it no longer exists on source",
                            copy_task.source_chunk.quoted_name()
                        )
                    } else {
                        let copy_result = copy_chunk(&source_tx, &target_tx, &copy_task).await?;

                        let elapsed = start.elapsed();
                        let throughput = if copy_result.bytes == 0 {
                            String::new()
                        } else {
                            format!(
                                "({})",
                                (copy_result.bytes as f64 / elapsed.as_secs_f64())
                                    .human_throughput_bytes()
                            )
                        };

                        format!(
                            "Copied chunk {} in {} {}",
                            copy_task.source_chunk.quoted_name(),
                            start.elapsed().human_duration(),
                            throughput
                        )
                    };
                complete_copy_task(&target_tx, &copy_task, &copy_result_message).await?;
                source_tx.commit().await?;
                let prev = PROCESSED_COUNT.fetch_add(1, Ordering::Relaxed);
                TERM.write_line(&format!(
                    "[{}/{}] {}",
                    prev + 1,
                    task_count,
                    copy_result_message
                ))?;
                Processed
            }
        };
        target_tx.commit().await?;
        Ok(result)
    }

    /// Processes a Verify task in the target database's task queue. When no
    /// tasks are available in the task queue, returns `TaskResult::NoItem`.
    async fn process_verify_task(
        source: &mut Source,
        target: &mut Target,
        task_count: u64,
    ) -> Result<TaskResult> {
        let target_tx = target.client.transaction().await?;
        let result = match claim_verify_task(&target_tx)
            .await
            .with_context(|| "error claiming verify task")?
        {
            None => NoItem,
            Some(verify_task) => {
                let start = Instant::now();
                let source_tx = source.transaction().await?;
                let verify_message =
                    match verify_chunk_data(&source_tx, &target_tx, &verify_task).await {
                        Ok(_) => format!(
                            "Verified chunk {} in {}",
                            verify_task.source_chunk.quoted_name(),
                            start.elapsed().human_duration()
                        ),
                        Err(error) => match error.downcast_ref::<VerificationError>() {
                            Some(e) => format!("{}", e),
                            None => bail!(error),
                        },
                    };
                complete_verify_task(&target_tx, &verify_task, &verify_message).await?;
                source_tx.commit().await?;

                let prev = PROCESSED_COUNT.fetch_add(1, Ordering::Relaxed);

                TERM.write_line(&format!(
                    "[{}/{}] {}",
                    prev + 1,
                    task_count,
                    &verify_message,
                ))?;
                Processed
            }
        };
        target_tx.commit().await?;
        Ok(result)
    }
}
