use crate::connect::{Source, Target};
use crate::timescale::{Hypertable, SourceChunk, TargetChunk};
use crate::TERM;
use anyhow::{bail, Context, Result};
use tokio_postgres::{Client, Config, GenericClient, Transaction};

#[derive(Debug)]
pub struct CopyTask {
    pub priority: i64,
    pub source_chunk: SourceChunk,
    pub target_chunk: Option<TargetChunk>,
    pub filter: Option<String>,
    pub snapshot: Option<String>,
}

pub async fn claim_copy_task(target_tx: &Transaction<'_>) -> Result<Option<CopyTask>> {
    static CLAIM_COPY_TASK: &str = include_str!("claim_source_chunk.sql");

    let row = target_tx.query_opt(CLAIM_COPY_TASK, &[]).await?;
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
                dimensions: serde_json::from_value(row.get("dimension_slices"))?,
                target_dimensions: row.get("target_dimensions"),
            };
            let filter: Option<String> = row.get("filter");
            let snapshot: Option<String> = row.get("snapshot");

            let target_chunk =
                find_target_chunk_with_same_dimensions(target_tx, &source_chunk).await?;

            Ok(Some(CopyTask {
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
                &source_chunk.target_dimensions,
            ],
        )
        .await?;

    Ok(row.map(|row| {
        // TODO: do we still need this remapping? DO NOT MERGE THIS!
        let target_dimensions = source_chunk.target_dimensions.clone();
        let dimensions = source_chunk
            .dimensions
            .clone()
            .into_iter()
            .filter(|d| target_dimensions.contains(&d.column_name))
            .collect();
        dbg!("remapped shizzle", &source_chunk.dimensions, &dimensions);
        TargetChunk {
            schema: row.get("chunk_schema"),
            table: row.get("chunk_name"),
            hypertable: Hypertable {
                schema: row.get("hypertable_schema"),
                table: row.get("hypertable_name"),
            },
            dimensions,
            target_dimensions,
        }
    }))
}

pub async fn complete_copy_task(target_tx: &Transaction<'_>, copy_task: &CopyTask) -> Result<()> {
    static COMPLETE_SOURCE_CHUNK: &str = include_str!("complete_source_chunk.sql");
    target_tx
        .execute(COMPLETE_SOURCE_CHUNK, &[&copy_task.priority])
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
    table_filter: Option<String>,
    until: Option<String>,
    snapshot: Option<String>,
) -> Result<()> {
    init_schema(target).await?;

    let target_tx = target.client.transaction().await?;
    static FIND_SOURCE_CHUNKS: &str = include_str!("find_source_chunks.sql");
    let source_tx = source.transaction().await?;
    let rows = source_tx
        .query(FIND_SOURCE_CHUNKS, &[&table_filter, &until])
        // TODO: determine if the problem is with until or filter and show a better error.
        // until error if it's a string - ERROR: invalid input syntax for type bigint: "hello"
        // filter error if it's an invalid regex. Eg `filter=[(` - ERROR: invalid regular expression: brackets [] not balanced
        .await.with_context(|| "failed to find hypertable/chunks in source. DB invalid errors might be related to the `until` and `filter` flags.")?;

    let row_count = rows.len();

    let stmt = target_tx
        .prepare(include_str!("insert_source_chunks.sql"))
        .await?;

    for row in rows.iter() {
        target_tx
            .execute(
                &stmt,
                &[
                    &row.get::<&str, String>("chunk_schema"),
                    &row.get::<&str, String>("chunk_name"),
                    &row.get::<&str, String>("hypertable_schema"),
                    &row.get::<&str, String>("hypertable_name"),
                    &row.get::<&str, String>("hypertable_dimensions"),
                    &row.get::<&str, String>("dimension_slices"),
                    &row.get::<&str, Option<String>>("filter"),
                    &snapshot,
                ],
            )
            .await?;
    }
    target_tx.commit().await?;

    TERM.write_line(&format!("Staged {row_count} chunks to copy"))?;

    Ok(())
}

async fn get_pending_task_count(client: &Client) -> Result<u64> {
    let pending_task_count = client
        .query_one(
            "select count(*) from __backfill.task where worked is null",
            &[],
        )
        .await?;
    // NOTE: count(*) in postgres returns int8, which is an i64, but it will never be negative
    Ok(pending_task_count.get::<'_, _, i64>(0) as u64)
}

pub async fn get_and_assert_staged_task_count_greater_zero(target_config: &Config) -> Result<u64> {
    let target = Target::connect(target_config).await?;
    if !backfill_schema_exists(&target.client).await? {
        bail!("administrative schema `__backfill` not found. Run the `stage` command once before running `copy`.");
    }
    let pending_task_count = get_pending_task_count(&target.client).await?;
    if pending_task_count == 0 {
        bail!("there are no pending copy tasks. Use the `stage` command to add more.");
    }
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
