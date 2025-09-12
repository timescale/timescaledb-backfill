use crate::connect::{Source, Target};
use crate::execute::create_uncompressed_chunk;
use crate::sql::assert_regex;
use crate::storage::{backfill_schema_exists, init_schema};
use crate::timescale::{
    set_query_source_proc_schema, Hypertable, QuotedName, SourceChunk, TargetChunk,
};
use crate::{features, TERM};
use anyhow::{bail, Result};
use std::collections::HashSet;
use tokio_postgres::error::SqlState;
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
    pub filter: Option<String>,
    #[allow(dead_code)]
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

            Ok(Some(Task {
                priority,
                source_chunk,
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
) -> Result<TargetChunk> {
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

    match row {
        Some(row) => Ok(TargetChunk {
            schema: row.get("chunk_schema"),
            table: row.get("chunk_name"),
            hypertable: Hypertable {
                schema: row.get("hypertable_schema"),
                table: row.get("hypertable_name"),
            },
            dimensions: source_chunk.dimensions.clone(),
        }),
        None => bail!(
            "target chunk for {} not found - staging failed or chunk was deleted",
            source_chunk.quoted_name()
        ),
    }
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

#[allow(clippy::too_many_arguments)]
pub async fn load_queue(
    source: &mut Source,
    target: &mut Target,
    filter: Option<&String>,
    cascade_up: bool,
    cascade_down: bool,
    until: &String,
    from: Option<&String>,
    snapshot: Option<&String>,
) -> Result<usize> {
    if let Some(filter) = filter {
        assert_regex(&source.client, filter).await?;
    }
    check_until(source, until).await?;

    init_schema(target).await?;

    let target_tx = target.client.transaction().await?;
    static FIND_SOURCE_CHUNKS: &str = include_str!("find_source_chunks.sql");
    let query = set_query_source_proc_schema(FIND_SOURCE_CHUNKS);
    let source_tx = source.transaction().await?;

    let rows = match source_tx
        .query(
            &query,
            &[&filter, &until, &from, &cascade_up, &cascade_down],
        )
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

    if chunk_count == 0 {
        return Ok(0);
    }

    let mut skipped_chunks = 0;

    let hypertables: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}.{}",
                row.get::<&str, String>("hypertable_schema"),
                row.get::<&str, String>("hypertable_name")
            )
        })
        .collect::<HashSet<String>>()
        .into_iter()
        .collect();

    validate_hypertables(&source_tx, &target_tx, hypertables).await?;

    let stmt = target_tx
        .prepare(include_str!("insert_source_chunks.sql"))
        .await?;

    for row in rows.iter() {
        let chunk_schema: &str = row.get("chunk_schema");
        let chunk_name: &str = row.get("chunk_name");
        let hypertable_schema: &str = row.get("hypertable_schema");
        let hypertable_name: &str = row.get("hypertable_name");
        let dimensions: String = row.get("dimensions");

        let row = target_tx
            .query_opt(
                &stmt,
                &[
                    &chunk_schema,
                    &chunk_name,
                    &hypertable_schema,
                    &hypertable_name,
                    &dimensions,
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
        } else {
            let source_chunk = SourceChunk {
                schema: chunk_schema.to_string(),
                table: chunk_name.to_string(),
                hypertable: Hypertable {
                    schema: hypertable_schema.to_string(),
                    table: hypertable_name.to_string(),
                },
                dimensions: serde_json::from_str(&dimensions)?,
            };

            // Ensure target chunk exists, create if it doesn't
            if find_target_chunk_with_same_dimensions(&target_tx, &source_chunk)
                .await
                .is_err()
            {
                // Target chunk doesn't exist, create it
                let target_chunk = create_uncompressed_chunk(&target_tx, &source_chunk).await?;
                debug!(
                    "Created target chunk {} for source chunk {}",
                    target_chunk.quoted_name(),
                    source_chunk.quoted_name()
                );
            }
        }
    }
    source_tx.rollback().await?;
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

// Hypercore TAM was deprecated and sunsetted in TS 2.22
async fn validate_hypertables_and_hypercore_tam<T: GenericClient>(
    source: &T,
    target: &T,
    hypertables: Vec<String>,
) -> Result<()> {
    let source_schema = r"
WITH agg AS (
SELECT
  c.table_schema,
  c.table_name,
  am.amname as access_method,
  JSON_AGG(
    JSON_BUILD_OBJECT (
      'column_name', c.column_name,
      'data_type', c.data_type,
      'udt_schema', c.udt_schema,
      'udt_name', c.udt_name
    )
    ORDER BY c.ordinal_position
  ) AS columns
FROM
  information_schema.columns c
  JOIN pg_class pc ON pc.relname = c.table_name
  JOIN pg_namespace n ON n.oid = pc.relnamespace AND n.nspname = c.table_schema
  JOIN pg_am am ON pc.relam = am.oid
WHERE FORMAT('%s.%s', c.table_schema, c.table_name) = ANY($1::TEXT[])
GROUP BY 1, 2, 3
)
SELECT
  JSON_AGG(
    JSON_BUILD_OBJECT(
      'table_schema', table_schema,
      'table_name', table_name,
      'access_method', access_method,
      'columns', columns
    )
  )::text
FROM agg;
";

    let Some(row) = source.query_opt(source_schema, &[&hypertables]).await? else {
        bail!(
            "Couldn't retrieve the source columns information from information_schema.columns for the hypertables: {}",
            hypertables.join(",")
        );
    };

    let source_tables_json: String = row.get(0);

    // Check for hypercore access method
    let source_tables: serde_json::Value = serde_json::from_str(&source_tables_json)?;
    let hypercore_tables: Vec<String> = source_tables
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|table| {
            if table["access_method"].as_str() == Some("hypercore") {
                Some(format!(
                    "{}.{}",
                    table["table_schema"].as_str().unwrap_or(""),
                    table["table_name"].as_str().unwrap_or("")
                ))
            } else {
                None
            }
        })
        .collect();

    if !hypercore_tables.is_empty() {
        bail!(
            "TimescaleDB does no longer support the hypercore table access method. Convert the following tables to heap access method before upgrading: {}",
            hypercore_tables.join(", ")
        );
    }

    let query = r#"
WITH
  target AS (
    SELECT
      table_schema,
      table_name,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'column_name', column_name,
          'data_type', data_type,
          'udt_schema', udt_schema,
          'udt_name', udt_name
        )
        ORDER BY
          ordinal_position
      ) AS columns
    FROM
      information_schema.columns
    WHERE
      FORMAT('%s.%s', table_schema, table_name) = ANY ($1::TEXT[])
    GROUP BY
      1,
      2
  )
SELECT
  format('%s.%s', source.table_schema, source.table_name) as hypertable,
  (
    SELECT
      STRING_AGG(
        FORMAT(
          '%s %s (%s.%s)',
          elements ->> 'column_name',
          elements ->> 'data_type',
          elements ->> 'udt_schema',
          elements ->> 'udt_name'
        ),
        ', '
      )
    FROM
      jsonb_array_elements(source.columns) AS elements
  ) AS source_columns,
  (
    SELECT
      STRING_AGG(
        FORMAT(
          '%s %s (%s.%s)',
          elements ->> 'column_name',
          elements ->> 'data_type',
          elements ->> 'udt_schema',
          elements ->> 'udt_name'
        ),
        ', '
      )
    FROM
      jsonb_array_elements(target.columns) AS elements
  ) AS target_columns
FROM
  JSONB_TO_RECORDSET($2::TEXT::JSONB) source (table_schema TEXT, table_name TEXT, columns JSONB)
  LEFT JOIN target ON (
    target.table_schema = source.table_schema
    AND target.table_name = source.table_name
  )
WHERE
  target.columns IS NULL
  OR target.columns != source.columns
"#;
    let rows = target
        .query(query, &[&hypertables, &source_tables_json])
        .await?;

    let errors: Vec<String> = rows
        .into_iter()
        .map(
            |row| match row.get::<&str, Option<String>>("target_columns") {
                Some(target_columns) => format!(
                    "- '{}' columns mismatch:\n    * source columns: {}\n    * target columns: {}",
                    row.get::<&str, String>("hypertable"),
                    row.get::<&str, String>("source_columns"),
                    target_columns,
                ),
                None => format!(
                    "- '{}' not found in target:\n    * source columns: {}",
                    row.get::<&str, String>("hypertable"),
                    row.get::<&str, String>("source_columns"),
                ),
            },
        )
        .collect();

    if !errors.is_empty() {
        bail!(
            "Found issues between the source and target hypertables:\n{}",
            errors.join("\n")
        )
    }

    Ok(())
}

async fn validate_hypertables<T: GenericClient>(
    source: &T,
    target: &T,
    hypertables: Vec<String>,
) -> Result<()> {
    if features::hypercore_tam() {
        return validate_hypertables_and_hypercore_tam(source, target, hypertables).await;
    }
    let source_schema = r"
WITH agg AS (
SELECT
  table_schema,
  table_name,
  JSON_AGG(
    JSON_BUILD_OBJECT (
      'column_name', column_name,
      'data_type', data_type,
      'udt_schema', udt_schema,
      'udt_name', udt_name
    )
    ORDER BY ordinal_position
  ) AS columns
FROM
  information_schema.columns
WHERE FORMAT('%s.%s', table_schema, table_name) = ANY($1::TEXT[])
GROUP BY 1, 2
)
SELECT
  JSON_AGG(
    JSON_BUILD_OBJECT(
      'table_schema', table_schema,
      'table_name', table_name,
      'columns', columns
    )
  )::text
FROM agg;
";

    let Some(row) = source.query_opt(source_schema, &[&hypertables]).await? else {
        bail!(
            "Couldn't retrieve the source columns information from information_schema.columns for the hypertables: {}",
            hypertables.join(",")
        );
    };

    let source_tables_json: String = row.get(0);

    let query = r#"
WITH
  target AS (
    SELECT
      table_schema,
      table_name,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'column_name', column_name,
          'data_type', data_type,
          'udt_schema', udt_schema,
          'udt_name', udt_name
        )
        ORDER BY
          ordinal_position
      ) AS columns
    FROM
      information_schema.columns
    WHERE
      FORMAT('%s.%s', table_schema, table_name) = ANY ($1::TEXT[])
    GROUP BY
      1,
      2
  )
SELECT
  format('%s.%s', source.table_schema, source.table_name) as hypertable,
  (
    SELECT
      STRING_AGG(
        FORMAT(
          '%s %s (%s.%s)',
          elements ->> 'column_name',
          elements ->> 'data_type',
          elements ->> 'udt_schema',
          elements ->> 'udt_name'
        ),
        ', '
      )
    FROM
      jsonb_array_elements(source.columns) AS elements
  ) AS source_columns,
  (
    SELECT
      STRING_AGG(
        FORMAT(
          '%s %s (%s.%s)',
          elements ->> 'column_name',
          elements ->> 'data_type',
          elements ->> 'udt_schema',
          elements ->> 'udt_name'
        ),
        ', '
      )
    FROM
      jsonb_array_elements(target.columns) AS elements
  ) AS target_columns
FROM
  JSONB_TO_RECORDSET($2::TEXT::JSONB) source (table_schema TEXT, table_name TEXT, columns JSONB)
  LEFT JOIN target ON (
    target.table_schema = source.table_schema
    AND target.table_name = source.table_name
  )
WHERE
  target.columns IS NULL
  OR target.columns != source.columns
"#;
    let rows = target
        .query(query, &[&hypertables, &source_tables_json])
        .await?;

    let errors: Vec<String> = rows
        .into_iter()
        .map(
            |row| match row.get::<&str, Option<String>>("target_columns") {
                Some(target_columns) => format!(
                    "- '{}' columns mismatch:\n    * source columns: {}\n    * target columns: {}",
                    row.get::<&str, String>("hypertable"),
                    row.get::<&str, String>("source_columns"),
                    target_columns,
                ),
                None => format!(
                    "- '{}' not found in target:\n    * source columns: {}",
                    row.get::<&str, String>("hypertable"),
                    row.get::<&str, String>("source_columns"),
                ),
            },
        )
        .collect();

    if !errors.is_empty() {
        bail!(
            "Found issues between the source and target hypertables:\n{}",
            errors.join("\n")
        )
    }

    Ok(())
}
