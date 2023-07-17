use crate::connect::{Source, Target};
use anyhow::Result;
use tokio_postgres::Transaction;

#[derive(Debug)]
pub struct Hypertable {
    pub schema: String,
    pub table: String,
}

#[derive(Debug)]
pub struct Chunk {
    pub schema: String,
    pub table: String,
}

pub type SourceHypertable = Hypertable;
pub type TargetHypertable = Hypertable;

pub type SourceChunk = Chunk;
pub type TargetChunk = Chunk;

#[derive(Debug)]
pub struct CopyTask {
    pub priority: i64,
    pub source_hypertable: SourceHypertable,
    pub source_chunk: SourceChunk,
    pub target_hypertable: TargetHypertable,
    pub target_chunk: TargetChunk,
    pub filter: Option<String>,
    pub snapshot: Option<String>,
}

pub async fn claim_copy_task(target_tx: &mut Transaction<'_>) -> Result<Option<CopyTask>> {
    static CLAIM_COPY_TASK: &str = include_str!("claim_source_chunk.sql");

    let row = target_tx.query_opt(CLAIM_COPY_TASK, &[]).await?;
    match row {
        Some(row) => {
            let priority: i64 = row.get("priority");
            let source_hypertable: SourceHypertable = SourceHypertable {
                schema: row.get("hypertable_schema"),
                table: row.get("hypertable_name"),
            };
            let source_chunk: SourceChunk = SourceChunk {
                schema: row.get("chunk_schema"),
                table: row.get("chunk_name"),
            };
            let dimensions: String = row.get("dimensions");
            let filter: Option<String> = row.get("filter");
            let snapshot: Option<String> = row.get("snapshot");

            static FIND_TARGET_CHUNK: &str = include_str!("find_target_chunk.sql");

            let row = target_tx
                .query_one(
                    FIND_TARGET_CHUNK,
                    &[
                        &source_hypertable.schema,
                        &source_hypertable.table,
                        &dimensions,
                    ],
                )
                .await?;

            Ok(Some(CopyTask {
                priority,
                source_hypertable,
                source_chunk,
                target_hypertable: TargetHypertable {
                    schema: row.get("hypertable_schema"),
                    table: row.get("hypertable_name"),
                },
                target_chunk: TargetChunk {
                    schema: row.get("chunk_schema"),
                    table: row.get("chunk_name"),
                },
                filter,
                snapshot,
            }))
        }
        None => Ok(None),
    }
}

pub async fn complete_copy_task(
    target_tx: &mut Transaction<'_>,
    copy_task: &CopyTask,
) -> Result<()> {
    static COMPLETE_SOURCE_CHUNK: &str = include_str!("complete_source_chunk.sql");
    target_tx
        .execute(COMPLETE_SOURCE_CHUNK, &[&copy_task.priority])
        .await?;
    Ok(())
}

async fn init_schema(target: &mut Target) -> Result<()> {
    static SCHEMA: &str = include_str!("schema.sql");
    let tx = target.client.transaction().await?;
    let row = tx
        .query_one(
            "select count(*) > 0 from pg_namespace where nspname = '__backfill'",
            &[],
        )
        .await?;
    let schema_exists: bool = row.get(0);
    if !schema_exists {
        tx.execute(SCHEMA, &[]).await?;
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
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

    struct Item {
        chunk_id: i64,
        chunk_schema: String,
        chunk_name: String,
        hypertable_id: i64,
        hypertable_schema: String,
        hypertable_name: String,
        dimensions: String,
        filter: Option<String>,
    }

    static FIND_SOURCE_CHUNKS: &str = include_str!("find_source_chunks.sql");
    let source_tx = source.transaction().await?;
    let rows = source_tx
        .query(FIND_SOURCE_CHUNKS, &[&table_filter, &until])
        .await?;

    static INSERT_SOURCE_CHUNKS: &str = include_str!("insert_source_chunks.sql");
    let target_tx = target.client.transaction().await?;

    for row in rows.iter() {
        let item = Item {
            chunk_id: row.get("chunk_id"),
            chunk_schema: row.get("chunk_schema"),
            chunk_name: row.get("chunk_name"),
            hypertable_id: row.get("hypertable_id"),
            hypertable_schema: row.get("hypertable_schema"),
            hypertable_name: row.get("hypertable_name"),
            dimensions: row.get("dimensions"),
            filter: row.get("filter"),
        };

        target_tx
            .execute(
                INSERT_SOURCE_CHUNKS,
                &[
                    &item.chunk_id,
                    &item.chunk_schema,
                    &item.chunk_name,
                    &item.hypertable_id,
                    &item.hypertable_schema,
                    &item.hypertable_name,
                    &item.dimensions,
                    &item.filter,
                    &snapshot,
                ],
            )
            .await?;
    }
    target_tx.commit().await?;

    Ok(())
}
