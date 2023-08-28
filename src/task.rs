use crate::connect::{Source, Target};
use crate::timescale::{Hypertable, SourceChunk, TargetChunk};
use crate::TERM;
use anyhow::{bail, Context, Result};
use regex::Regex;
use tokio_postgres::{Client, Config, GenericClient, Transaction};
use tracing::debug;

#[derive(Debug, Clone)]
pub enum TaskType {
    Copy,
    Verify,
}

#[derive(Debug)]
pub struct Task {
    pub priority: i64,
    pub source_chunk: SourceChunk,
    pub target_chunk: Option<TargetChunk>,
    pub filter: Option<String>,
    pub snapshot: Option<String>,
}

pub type CopyTask = Task;
pub type VerifyTask = Task;

pub async fn claim_task(target_tx: &Transaction<'_>, claim_query: &str) -> Result<Option<Task>> {
    let row = target_tx.query_opt(claim_query, &[]).await?;
    match row {
        Some(row) => {
            let priority: i64 = row.get("priority");

            let source_chunk: SourceChunk = SourceChunk {
                schema: row.get("chunk_schema"),
                table: row.get("chunk_name"),
                hypertable: Hypertable {
                    schema: row.get("hypertable_schema"),
                    table: row.get("hypertable_name"),
                },
                dimensions: serde_json::from_value(row.get("dimensions"))?,
            };
            let filter: Option<String> = row.get("filter");
            let snapshot: Option<String> = row.get("snapshot");

            let target_chunk =
                find_target_chunk_with_same_dimensions(target_tx, &source_chunk).await?;

            Ok(Some(Task {
                priority,
                source_chunk,
                target_chunk,
                filter,
                snapshot,
            }))
        }
        None => Ok(None),
    }
}

pub async fn claim_copy_task(target_tx: &Transaction<'_>) -> Result<Option<CopyTask>> {
    static CLAIM_COPY_TASK: &str = include_str!("claim_source_chunk_for_copy.sql");
    claim_task(target_tx, CLAIM_COPY_TASK).await
}

pub async fn claim_verify_task(target_tx: &Transaction<'_>) -> Result<Option<VerifyTask>> {
    static CLAIM_VERIFY_TASK: &str = include_str!("claim_source_chunk_for_verification.sql");
    claim_task(target_tx, CLAIM_VERIFY_TASK).await
}

pub async fn find_target_chunk_with_same_dimensions(
    target_tx: &Transaction<'_>,
    source_chunk: &SourceChunk,
) -> Result<Option<TargetChunk>> {
    static FIND_TARGET_CHUNK: &str = include_str!("find_target_chunk.sql");

    let row = target_tx
        .query_opt(
            FIND_TARGET_CHUNK,
            &[
                &source_chunk.hypertable.schema,
                &source_chunk.hypertable.table,
                &serde_json::to_string(&source_chunk.dimensions)?,
            ],
        )
        .await?;

    Ok(row.map(|row| TargetChunk {
        schema: row.get("chunk_schema"),
        table: row.get("chunk_name"),
        hypertable: Hypertable {
            schema: row.get("hypertable_schema"),
            table: row.get("hypertable_name"),
        },
        dimensions: source_chunk.dimensions.clone(),
    }))
}

pub async fn complete_copy_task(
    target_tx: &Transaction<'_>,
    copy_task: &CopyTask,
    copy_message: &str,
) -> Result<()> {
    static COMPLETE_SOURCE_CHUNK: &str = include_str!("complete_source_chunk.sql");
    target_tx
        .execute(COMPLETE_SOURCE_CHUNK, &[&copy_task.priority, &copy_message])
        .await?;
    Ok(())
}

pub async fn complete_verify_task(
    target_tx: &Transaction<'_>,
    verify_task: &VerifyTask,
    verify_message: &str,
) -> Result<()> {
    static COMPLETE_SOURCE_CHUNK_VERIFICATION: &str =
        include_str!("complete_source_chunk_verification.sql");
    target_tx
        .execute(
            COMPLETE_SOURCE_CHUNK_VERIFICATION,
            &[&verify_task.priority, &verify_message],
        )
        .await?;
    Ok(())
}

async fn backfill_schema_exists<T>(client: &T) -> Result<bool>
where
    T: GenericClient,
{
    let row = client
        .query_one(
            "select count(*) > 0 as schema_exists from pg_namespace where nspname = '__backfill'",
            &[],
        )
        .await?;
    Ok(row.get("schema_exists"))
}

async fn init_schema(target: &mut Target) -> Result<()> {
    let tx = target.client.transaction().await?;
    if !backfill_schema_exists(&tx).await? {
        static SCHEMA: &str = include_str!("schema.sql");
        tx.simple_query(SCHEMA).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn load_queue(
    source: &mut Source,
    target: &mut Target,
    table_filter: Option<Regex>,
    until: String,
    snapshot: Option<String>,
) -> Result<()> {
    init_schema(target).await?;

    let target_tx = target.client.transaction().await?;
    static FIND_SOURCE_CHUNKS: &str = include_str!("find_source_chunks.sql");
    let source_tx = source.transaction().await?;
    let rows = source_tx
        .query(FIND_SOURCE_CHUNKS, &[&table_filter.map(|e| e.as_str().to_owned()), &until])
        .await.with_context(|| "failed to find hypertable/chunks in source. DB invalid errors might be related to the `until` and `filter` flags.")?;

    let chunk_count = rows.len();
    let mut skipped_chunks = 0;

    let stmt = target_tx
        .prepare(include_str!("insert_source_chunks.sql"))
        .await?;

    for row in rows.iter() {
        let chunk_schema: &str = row.get("chunk_schema");
        let chunk_name: &str = row.get("chunk_name");
        let hypertable_schema: &str = row.get("hypertable_schema");
        let hypertable_name: &str = row.get("hypertable_name");
        let row = target_tx
            .query_opt(
                &stmt,
                &[
                    &chunk_schema,
                    &chunk_name,
                    &hypertable_schema,
                    &hypertable_name,
                    &row.get::<&str, String>("dimensions"),
                    &row.get::<&str, Option<String>>("filter"),
                    &snapshot,
                ],
            )
            .await?;

        // Check for duplicate tasks and notify the user of which ones have
        // been skipped.
        if row.is_none() {
            debug!("Task for chunk \"{chunk_schema}\".\"{chunk_name}\" of hypertable \"{hypertable_schema}\".\"{hypertable_name}\" already exists; skipping");
            skipped_chunks += 1;
        }
    }
    target_tx.commit().await?;

    let task_count = chunk_count - skipped_chunks;
    TERM.write_line(&format!("Staged {task_count} chunks to copy"))?;
    if skipped_chunks > 0 {
        TERM.write_line(&format!(
            "Skipping {skipped_chunks} chunks that were already staged. To re-stage run the `clean` command first."
        ))?;
    }

    Ok(())
}

async fn get_pending_task_count(client: &Client, task: &TaskType) -> Result<u64> {
    let query: &str = match task {
        TaskType::Copy => "select count(*) from __backfill.task where worked is null",
        TaskType::Verify => {
            "select count(*) from __backfill.task where verified is null and worked is not null"
        }
    };
    let pending_task_count = client.query_one(query, &[]).await?;
    // NOTE: count(*) in postgres returns int8, which is an i64, but it will never be negative
    Ok(pending_task_count.get::<'_, _, i64>(0) as u64)
}

pub async fn get_and_assert_staged_task_count_greater_zero(
    target_config: &Config,
    task: TaskType,
) -> Result<u64> {
    let target = Target::connect(target_config).await?;
    if !backfill_schema_exists(&target.client).await? {
        bail!("administrative schema `__backfill` not found. Run the `stage` command once before running `copy`.");
    }
    let pending_task_count = get_pending_task_count(&target.client, &task).await?;
    if pending_task_count == 0 {
        match task {
        TaskType::Copy => bail!("there are no pending copy tasks. Use the `stage` command to add more."),
        TaskType::Verify => bail!(
        "there are no pending verification tasks. If tasks are already staged, run the `copy` command to complete them for verification. Otherwise, use the `stage` command to add more copy tasks."
        ),
        };
    };
    Ok(pending_task_count)
}

pub async fn clean(target_config: &Config) -> Result<()> {
    let target = Target::connect(target_config).await?;
    target
        .client
        .execute("drop schema __backfill cascade", &[])
        .await?;
    Ok(())
}
