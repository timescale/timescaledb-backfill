use crate::connect::{Source, Target};
use crate::storage::{backfill_schema_exists, init_schema};
use crate::timescale::{set_query_source_proc_schema, Hypertable, SourceChunk, TargetChunk};
use crate::TERM;
use anyhow::{bail, Result};
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, Transaction};
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

async fn check_filter(source: &mut Source, table_filter: &String) -> Result<()> {
    let source_tx = source.transaction().await?;
    let x = source_tx
        .query(
            "select regexp_like('this is only a test', $1::text)",
            &[&table_filter],
        )
        .await;
    if x.is_err()
        && x.unwrap_err().code().unwrap().code() == SqlState::INVALID_REGULAR_EXPRESSION.code()
    {
        bail!("filter argument '{table_filter}' is not a valid regular expression");
    }
    Ok(())
}

async fn check_until(source: &mut Source, until: &String) -> Result<()> {
    let mut err_count = 0;
    {
        let source_tx = source.transaction().await?;
        let x = source_tx.query("select $1::text::bigint", &[&until]).await;
        if x.is_err()
            && x.unwrap_err().code().unwrap().code() == SqlState::INVALID_TEXT_REPRESENTATION.code()
        {
            err_count += 1;
        }
    }
    {
        let source_tx = source.transaction().await?;
        let x = source_tx
            .query("select $1::text::timestamptz", &[&until])
            .await;
        if x.is_err()
            && [
                SqlState::INVALID_DATETIME_FORMAT.code(),
                SqlState::DATETIME_VALUE_OUT_OF_RANGE.code(),
            ]
            .contains(&x.unwrap_err().code().unwrap().code())
        {
            err_count += 1;
        }
    }
    if err_count == 2 {
        bail!("until argument '{until}' is neither a valid bigint nor a valid datetime")
    }
    Ok(())
}

pub async fn load_queue(
    source: &mut Source,
    target: &mut Target,
    filter: Option<&String>,
    cascade_up: bool,
    cascade_down: bool,
    until: &String,
    snapshot: Option<&String>,
) -> Result<usize> {
    if filter.is_some() {
        check_filter(source, filter.unwrap()).await?;
    }
    check_until(source, until).await?;

    init_schema(target).await?;

    let target_tx = target.client.transaction().await?;
    static FIND_SOURCE_CHUNKS: &str = include_str!("find_source_chunks.sql");
    let query = set_query_source_proc_schema(FIND_SOURCE_CHUNKS);
    let source_tx = source.transaction().await?;

    let rows = match source_tx
        .query(&query, &[&filter, &until, &cascade_up, &cascade_down])
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            let dberr = err.as_db_error().unwrap();
            if dberr.code() == &SqlState::INVALID_TEXT_REPRESENTATION {
                bail!("until argument '{until} is not a valid bigint and some matching hypertables use bigint time columns");
            }
            if dberr.code() == &SqlState::INVALID_DATETIME_FORMAT
                || dberr.code() == &SqlState::DATETIME_VALUE_OUT_OF_RANGE
            {
                bail!("until argument '{until}' is not a valid datetime and some matching hypertables use date, timestamp, or timestamptz time columns")
            }
            bail!(err);
        }
    };

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

    if skipped_chunks > 0 {
        TERM.write_line(&format!(
            "Skipping {skipped_chunks} chunks that were already staged. To re-stage run the `clean` command first."
        ))?;
    }

    let staged_tasks = chunk_count - skipped_chunks;

    Ok(staged_tasks)
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
    target: &Target,
    task: TaskType,
) -> Result<u64> {
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
