use crate::sql::quote_table_name;
use crate::task::{find_target_chunk_with_same_dimensions, CopyTask};
use crate::timescale::{
    Chunk, CompressedChunk, CompressionSize, SourceChunk, SourceCompressedChunk, TargetChunk,
    TargetCompressedChunk,
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_lite::future::block_on;
use futures_lite::StreamExt;
use futures_util::pin_mut;
use futures_util::SinkExt;
use once_cell::sync::OnceCell;
use tokio::task::spawn_blocking;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{CopyInSink, CopyOutStream, Transaction};
use tracing::{debug, trace};

static MAX_IDENTIFIER_LENGTH: OnceCell<usize> = OnceCell::new();
const COMPRESS_TABLE_NAME_PREFIX: &str = "bf_";

pub async fn copy_chunk(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    task: &CopyTask,
) -> Result<()> {
    let target_chunk = match task.target_chunk.as_ref() {
        Some(target_chunk) => target_chunk.clone(),
        None => create_uncompressed_chunk(target_tx, &task.source_chunk).await?,
    };

    copy_uncompressed_chunk_data(
        source_tx,
        target_tx,
        &task.source_chunk,
        &target_chunk,
        &task.filter,
    )
    .await?;

    if let Some(source_compressed_chunk) =
        get_compressed_chunk(source_tx, &task.source_chunk).await?
    {
        if let Some(target_compressed_chunk) =
            get_compressed_chunk(target_tx, &target_chunk).await?
        {
            copy_compressed_chunk_data(
                source_tx,
                target_tx,
                &source_compressed_chunk,
                &target_compressed_chunk,
            )
            .await?;
        } else {
            let target_compressed_chunk_data_table = create_compressed_chunk_data_table(
                target_tx,
                &target_chunk,
                &source_compressed_chunk,
            )
            .await?;
            copy_compressed_chunk_data(
                source_tx,
                target_tx,
                &source_compressed_chunk,
                &target_compressed_chunk_data_table,
            )
            .await?;
            create_compressed_chunk_from_data_table(
                source_tx,
                target_tx,
                &source_compressed_chunk,
                &target_chunk,
                &target_compressed_chunk_data_table,
            )
            .await?;
        };
    }
    Ok(())
}

async fn copy_uncompressed_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk: &SourceChunk,
    target_chunk: &TargetChunk,
    filter: &Option<String>,
) -> Result<()> {
    debug!("Copying uncompressed chunk {}", source_chunk.quoted_name());

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;

    if let Some(filter) = filter {
        delete_data_using_filter(target_tx, target_chunk, filter).await?;
    } else {
        delete_all_rows_from_chunk(target_tx, &target_chunk.quoted_name()).await?;
    }

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk.quoted_name(),
        &target_chunk.quoted_name(),
        filter,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }
    debug!(
        "Finished copying uncompressed chunk {}",
        source_chunk.quoted_name()
    );
    Ok(())
}

async fn delete_data_using_filter(
    tx: &Transaction<'_>,
    chunk: &TargetChunk,
    filter: &str,
) -> Result<()> {
    let chunk_name = chunk.quoted_name();
    debug!("Deleting rows from chunk {chunk_name} with filter {filter}");
    let rows = tx
        .execute(&format!("DELETE FROM {chunk_name} WHERE {filter}"), &[])
        .await?;
    debug!("deleted '{rows}' rows");
    Ok(())
}

async fn drop_invalidation_trigger(tx: &Transaction<'_>, chunk_name: &str) -> Result<bool> {
    debug!("Attempting to drop invalidation trigger on '{chunk_name}'");
    // NOTE: It's not possible to selectively disable the trigger using
    // `ALTER TABLE ... DISABLE TRIGGER ...` because timescaledb intercepts the
    // statement and prohibits it. So we must actually drop and recreate it.
    let trigger_exists = tx
        .query_one(
            r"
        SELECT EXISTS (
            SELECT 1
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname != 'pg_toast' -- In cloud pg_toast gives permission denied
              AND t.tgname = 'ts_cagg_invalidation_trigger'
              AND format('%I.%I', n.nspname, c.relname)::regclass = $1::text::regclass
            )",
            &[&chunk_name],
        )
        .await?
        .get(0);
    if trigger_exists {
        tx.execute(
            &format!("DROP TRIGGER ts_cagg_invalidation_trigger ON {chunk_name}"),
            &[],
        )
        .await?;
    }
    Ok(trigger_exists)
}

async fn create_invalidation_trigger(tx: &Transaction<'_>, chunk_name: &str) -> Result<()> {
    debug!("Creating invalidation trigger on '{chunk_name}'");
    let hypertable_id: i32 = tx
        .query_one(
            r"
        SELECT hypertable_id
        FROM _timescaledb_catalog.chunk
        WHERE format('%I.%I', schema_name, table_name)::regclass = $1::text::regclass
    ",
            &[&chunk_name],
        )
        .await?
        .get("hypertable_id");

    tx.execute(&format!(r"
        CREATE TRIGGER ts_cagg_invalidation_trigger
        AFTER INSERT OR DELETE OR UPDATE ON {chunk_name}
        FOR EACH ROW
        EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('{hypertable_id}')
    "), &[]).await?;
    Ok(())
}

async fn delete_all_rows_from_chunk(target_tx: &Transaction<'_>, chunk_name: &str) -> Result<()> {
    debug!("Deleting all rows from chunk {chunk_name}");
    // NOTE: We're not using `TRUNCATE` on purpose here. We're trying to avoid
    // having rows written into the cagg hypertable invalidation log (which we
    // don't have permissions to modify).
    // We _can_ disable the cagg invalidation trigger, but if we issue a
    // `TRUNCATE`, timescale intercepts it and helpfully writes the
    // invalidation log entries.
    target_tx
        .execute(&format!("DELETE FROM ONLY {chunk_name}"), &[])
        .await?;
    Ok(())
}

async fn copy_compressed_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk: &CompressedChunk,
    target_chunk: &CompressedChunk,
) -> Result<()> {
    debug!("Copying compressed chunk {}", source_chunk.quoted_name());

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk.quoted_name(),
        &target_chunk.quoted_name(),
        &None,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }

    debug!(
        "Finished copying compressed chunk {}",
        source_chunk.quoted_name()
    );
    Ok(())
}

async fn copy_chunk_from_source_to_target(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk_name: &str,
    target_chunk_name: &str,
    filter: &Option<String>,
) -> Result<()> {
    let copy_out = filter.as_ref().map(|filter| {
        format!("COPY (SELECT * FROM ONLY {source_chunk_name} WHERE {filter}) TO STDOUT WITH (FORMAT BINARY)")
    }).unwrap_or(
        format!("COPY (SELECT * FROM ONLY {source_chunk_name}) TO STDOUT WITH (FORMAT BINARY)")
    );

    debug!("{copy_out}");

    let copy_in = format!("COPY {target_chunk_name} FROM STDIN WITH (FORMAT BINARY)");
    debug!("{copy_in}");

    let stream = source_tx.copy_out(&copy_out).await?;
    let sink: CopyInSink<Bytes> = target_tx.copy_in(&copy_in).await?;

    // Note: we must spawn copy_from_source_to_sink on a separate (blocking)
    // thread, because it blocks the thread, breaking async.
    let rows = spawn_blocking(move || block_on(copy_from_source_to_sink(stream, sink))).await??;

    debug!("Copied {rows} for table {source_chunk_name}",);

    Ok(())
}

async fn copy_from_source_to_sink(stream: CopyOutStream, sink: CopyInSink<Bytes>) -> Result<u64> {
    let buffer_size = 1024 * 1024; // 1MiB
    let mut buf = BytesMut::with_capacity(buffer_size);

    pin_mut!(stream);
    pin_mut!(sink);

    while let Some(row) = stream.next().await {
        let row = row?;
        buf.extend(row);
        if buf.len() > buffer_size {
            sink.send(buf.split().freeze()).await?;
        }
    }
    if !buf.is_empty() {
        sink.send(buf.split().freeze()).await?;
    }

    let rows = sink.finish().await?;
    Ok(rows)
}

async fn get_compressed_chunk(
    source_tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<Option<SourceCompressedChunk>> {
    let row = source_tx
        .query_opt(
            r#"
    SELECT
      cch.schema_name
    , cch.table_name
    FROM _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.chunk cch ON ch.compressed_chunk_id = cch.id
    WHERE ch.schema_name = $1
      AND ch.table_name = $2
    "#,
            &[&chunk.schema, &chunk.table],
        )
        .await?;
    Ok(row.map(|r| SourceCompressedChunk {
        schema: r.get("schema_name"),
        table: r.get("table_name"),
    }))
}

/// Creates a Chunk in the same Hypertable and with the same slices as the
/// given Chunk.
async fn create_uncompressed_chunk(
    tx: &Transaction<'_>,
    source_chunk: &SourceChunk,
) -> Result<TargetChunk> {
    trace!(
        "creating uncompressed chunk from {:?} with slices {}",
        source_chunk,
        source_chunk.slices()?,
    );

    tx.execute(
        r#"
SELECT _timescaledb_internal.create_chunk(
    $1::text::regclass,
    slices => $2::TEXT::JSONB
)
"#,
        &[
            &quote_table_name(
                &source_chunk.hypertable.schema,
                &source_chunk.hypertable.table,
            ),
            &source_chunk.slices()?,
        ],
    )
    .await?;

    find_target_chunk_with_same_dimensions(tx, source_chunk)
        .await?
        .ok_or_else(|| anyhow!("couldn't retrieve recently created chunk"))
}

/// Creates a compressed chunk data table. The table is created in the given
/// `schema_name` and the `table_name` is prefixed with
/// `COMPRESS_TABLE_NAME_PREFIX`.
///
/// The generated table is not part of the chunks catalog, it's not associated
/// with any uncompressed chunk and it's missing the corresponding indexes,
/// constraints and triggers. To convert the table into a propper compressed
/// chunk, first the compressed data has to be inserted into it, then it needs
/// to be passed as argument to the
/// `_timescaledb_internal.create_compressed_chunk` function.
///
/// Trying to generate the same chunk name as TimescaleDB (TS) might produce
/// inconsistencies because TS uses the chunk ID as part of the name. Since we
/// are not directly inserting into the Chunk's catalog, we cannot guarantee
/// that the ID we use for the name will match what is generated by the
/// sequence used for the catalog's IDs.
async fn create_compressed_chunk_data_table(
    tx: &Transaction<'_>,
    uncompressed_chunk: &TargetChunk,
    source_compressed_chunk: &SourceCompressedChunk,
) -> Result<TargetCompressedChunk> {
    let table = add_backfill_prefix(tx, &source_compressed_chunk.table).await?;
    let data_table_name = quote_table_name(&source_compressed_chunk.schema, &table);
    let parent_table_name = compressed_hypertable_name(
        tx,
        &uncompressed_chunk.hypertable.schema,
        &uncompressed_chunk.hypertable.table,
    )
    .await?;

    trace!(
        "creating compressed chunk data table {} as a child of {}",
        data_table_name,
        parent_table_name
    );

    let query = format!(
        "create table {}() inherits ({})",
        data_table_name, parent_table_name
    );

    tx.execute(&query, &[]).await?;

    Ok(TargetCompressedChunk {
        schema: source_compressed_chunk.schema.clone(),
        table,
    })
}

/// Adds the backfill prefix `COMPRESS_TABLE_NAME_PREFIX` to the table name.
///
/// If adding the prefix exceeds the Postgres limit for identifiers, the table
/// name is truncated by removing characters from the beginning instead of the
/// end. This is done to preserve the chunk identifiers at the end of the table
/// name, ensuring uniqueness.
async fn add_backfill_prefix(tx: &Transaction<'_>, table_name: &str) -> Result<String> {
    let table_name = if table_name.len() + COMPRESS_TABLE_NAME_PREFIX.len()
        >= get_max_identifier_length(tx).await?
    {
        &table_name[COMPRESS_TABLE_NAME_PREFIX.len()..]
    } else {
        table_name
    };

    Ok(format!("bf_{}", table_name))
}

/// Returns the max identifier length as set in the DB.
///
/// The value is cached in a `OnceCell`, ideally only one query to the DB will
/// be made. Worst case scenario, concurrent access to the unitialized value
/// would make it so that multiple queries for the setting are executed
/// but the query is simple enough that it won't matter.
///
/// Ideally we should use `get_or_init` but that'd require an async
/// clousure, and those are not supported yet. Another option is wrapping
/// the closure that fetches the value in a `block_on`, but when that was tried
/// the execution didn't resume.
async fn get_max_identifier_length(tx: &Transaction<'_>) -> Result<usize> {
    match MAX_IDENTIFIER_LENGTH.get() {
        Some(length) => Ok(*length),
        None => {
            let l: i32 = tx
                .query_one("select current_setting('max_identifier_length')::int", &[])
                .await?
                .get(0);
            let length = l as usize;
            // An error here means that the `OnceCell` was set by a concurrent
            // operation. It's safe to ignore.
            _ = MAX_IDENTIFIER_LENGTH.set(length);
            Ok(length)
        }
    }
}

/// Returns the name of the compressed hypertable associated to the given
/// uncompressed hypertable.
///
/// The name is returned as "{schema}"."{name}" with both identifiers quoted.
async fn compressed_hypertable_name(
    tx: &Transaction<'_>,
    uncompressed_hypertable_schema: &String,
    uncompressed_hypertable_table: &String,
) -> Result<String> {
    let row = tx
        .query_one(
            r#"
SELECT ch.schema_name, ch.table_name
FROM _timescaledb_catalog.hypertable ch
JOIN
    _timescaledb_catalog.hypertable h ON h.compressed_hypertable_id = ch.id
WHERE
    h.schema_name = $1 AND h.table_name = $2
 "#,
            &[
                uncompressed_hypertable_schema,
                uncompressed_hypertable_table,
            ],
        )
        .await?;

    let schema_name: String = row.get("schema_name");
    let table_name: String = row.get("table_name");
    Ok(quote_table_name(&schema_name, &table_name))
}

/// Uses `_timescaledb_internal.create_compressed_chunk` to convert the
/// `target_data_table` into a compressed chunk of `target_chunk`.
///
/// This takes care of creating the triggers, indexes and constraints missing
/// on the data table, and updating the catalog to reflect the new compressed
/// chunk.
///
/// The function requires the compression size information for the table. This
/// information is fetched from the `source_compressed_chunk`.
async fn create_compressed_chunk_from_data_table(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    target_chunk: &TargetChunk,
    target_data_table: &TargetCompressedChunk,
) -> Result<()> {
    let compression_size = fetch_compression_size(source_tx, source_compressed_chunk).await?;

    target_tx
        .execute(
            r#"
SELECT _timescaledb_internal.create_compressed_chunk(
    $1::TEXT::REGCLASS,
    $2::TEXT::REGCLASS,
    $3,$4,$5,$6,$7,$8,$9,$10)
    "#,
            &[
                &target_chunk.quoted_name(),
                &target_data_table.quoted_name(),
                &compression_size.uncompressed_heap_size,
                &compression_size.uncompressed_toast_size,
                &compression_size.uncompressed_index_size,
                &compression_size.compressed_heap_size,
                &compression_size.compressed_toast_size,
                &compression_size.compressed_index_size,
                &compression_size.numrows_pre_compression,
                &compression_size.numrows_post_compression,
            ],
        )
        .await?;

    Ok(())
}

/// Fetches the compression size of the given `CompressedChunk`
async fn fetch_compression_size(
    tx: &Transaction<'_>,
    compressed_chunk: &SourceCompressedChunk,
) -> Result<CompressionSize> {
    let row = tx
        .query_one(
            r#"
SELECT
    uncompressed_heap_size,
    uncompressed_toast_size,
    uncompressed_index_size,
    compressed_heap_size,
    compressed_toast_size,
    compressed_index_size,
    numrows_pre_compression,
    numrows_post_compression
FROM _timescaledb_catalog.compression_chunk_size s
JOIN _timescaledb_catalog.chunk c ON c.id = s.compressed_chunk_id
WHERE c.schema_name = $1 AND c.table_name = $2
        "#,
            &[&compressed_chunk.schema, &compressed_chunk.table],
        )
        .await?;

    Ok(CompressionSize {
        uncompressed_heap_size: row.get("uncompressed_heap_size"),
        uncompressed_toast_size: row.get("uncompressed_toast_size"),
        uncompressed_index_size: row.get("uncompressed_index_size"),
        compressed_heap_size: row.get("compressed_heap_size"),
        compressed_toast_size: row.get("compressed_toast_size"),
        compressed_index_size: row.get("compressed_index_size"),
        numrows_pre_compression: row.get("numrows_pre_compression"),
        numrows_post_compression: row.get("numrows_post_compression"),
    })
}
