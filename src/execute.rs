use crate::execute::CopyMode::UncompressedOnly;
use crate::sql::{quote_ident, quote_table_name};
use crate::task::{find_target_chunk_with_same_dimensions, CopyTask};
use crate::timescale::{
    set_query_target_proc_schema, Chunk, CompressedChunk, CompressionSize, Hypertable, QuotedName,
    SourceChunk, SourceCompressedChunk, TargetChunk, TargetCompressedChunk,
};
use crate::{features, sql};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures_lite::StreamExt;
use futures_util::pin_mut;
use futures_util::SinkExt;
use human_repr::HumanCount;
use once_cell::sync::OnceCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{CopyInSink, CopyOutStream, GenericClient, Row, Transaction};
use tracing::{debug, trace, warn};

static MAX_IDENTIFIER_LENGTH: OnceCell<usize> = OnceCell::new();
pub static TOTAL_BYTES_COPIED: AtomicUsize = AtomicUsize::new(0);
const COMPRESS_TABLE_NAME_PREFIX: &str = "bf_";
const WITH_TOAST_TUPLE_TARGET: &str = "WITH (toast_tuple_target = 128)";
const TIMESCALE_INTERNAL_SCHEMA: &str = "_timescaledb_internal";
const TIMESCALE_COMPRESSED_DATA_TYPE: &str = "compressed_data";

pub async fn copy_chunk(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    task: &CopyTask,
) -> Result<CopyResult> {
    let target_chunk =
        find_target_chunk_with_same_dimensions(target_tx, &task.source_chunk).await?;

    let source_chunk_compressed = is_chunk_compressed(source_tx, &task.source_chunk).await?;
    let source_compressed_chunk = get_compressed_chunk(source_tx, &task.source_chunk).await?;

    // A source chunk compressed under an older format can't be copied in
    // compressed form: its physical layout doesn't back the sparse-index
    // metadata the target hypertable declares (issue #195). Copy its rows in
    // uncompressed form instead and let the target compress them in its own
    // format.
    //
    // This must be decided before the completion-filter branch below, which
    // recompresses the target chunk only when it was already compressed:
    // `stage` deliberately leaves the affected chunks uncompressed, so taking
    // that branch would leave the chunk holding the `--until` boundary
    // uncompressed forever. Detection is re-run here instead of read from the
    // task, so a `stage` warning has not necessarily preceded it: tasks staged
    // by a binary without this check reach the fallback too.
    if let Some(source_compressed_chunk) = &source_compressed_chunk {
        let target_hypertable_index =
            fetch_hypertable_sparse_index(target_tx, &target_chunk.hypertable).await?;
        let unbacked = unbacked_sparse_index_metadata(
            source_tx,
            source_compressed_chunk,
            &target_hypertable_index,
        )
        .await?;
        if !unbacked.is_empty() {
            warn!(
                "chunk {} was compressed under an older format which cannot back the sparse-index metadata of hypertable {} ({}), copying its rows uncompressed and recompressing in target",
                task.source_chunk.quoted_name(),
                target_chunk.hypertable.quoted_name(),
                unbacked.join(", "),
            );
            return copy_chunk_data_and_recompress(source_tx, target_tx, task, &target_chunk).await;
        }
    }

    // If we're trying to filter on a compressed chunk, fall back to reading rows directly
    // from the uncompressed chunk, and write the uncompressed rows into the target.
    // Note: we must check the compression status in this transaction to ensure correctness.
    if task.filter.is_some() && source_chunk_compressed {
        let target_chunk_compressed = is_chunk_compressed(target_tx, &target_chunk).await?;
        // We must decompress the target chunk if it's compressed, otherwise
        // DELETING uncompressed rows will never delete anything
        // on the compressed chunk.
        // For example, `SELECT * FROM chunk WHERE time > '2023-01-01'` will
        // decompress the chunk on-the-fly, but the DELETE will not.
        // Note: This behavior has been fixed in TimescaleDB 2.23.
        // See: https://github.com/timescale/timescaledb/pull/8704
        if target_chunk_compressed {
            warn!(
                "Completion filter is within a compressed chunk, decompressing chunk {}",
                target_chunk.quoted_name()
            );
            decompress_chunk(target_tx, &target_chunk).await?;
        }

        // Copy rows in uncompressed form from source
        let result = copy_chunk_data(
            source_tx,
            target_tx,
            &task.source_chunk,
            &target_chunk,
            &target_chunk,
            &task.filter,
            CopyMode::UncompressedAndCompressed,
        )
        .await?;

        if source_chunk_compressed && target_chunk_compressed {
            // The source chunk was compressed, and the target chunk was
            // compressed (before we decompressed it), so it should be safe to
            // compress it again.
            compress_chunk(target_tx, &target_chunk).await?;
        }
        return Ok(result);
    }

    let target_chunk_is_compressed = is_chunk_compressed(target_tx, &target_chunk).await?;

    if target_chunk_is_compressed {
        // The source chunk was compressed at the time of schema dump, but has
        // been decompressed in the meantime. We need to update the status of
        // the target chunk to "decompressed", the simplest way to do this is
        // to decompress it. We expect that the chunk is empty, so this should
        // not incur any real overhead.
        let source_chunk_decompressed = source_compressed_chunk.is_none();
        // The target doesn't support mutable compression, so we won't be able
        // to remove any rows which would be present in the target chunk. To
        // work around this, we we'll decompress the target chunk
        let no_mutable_compression = !features::mutation_of_compressed_hypertables();
        if source_chunk_decompressed || no_mutable_compression {
            decompress_chunk(target_tx, &target_chunk).await?;
        }
    }

    let source_chunk_has_uncompressed_rows =
        hss_uncompressed_rows(source_tx, &task.source_chunk).await?;
    let target_chunk_is_partial = chunk_status_is_partial(target_tx, &target_chunk).await?;

    let uncompressed_result = if source_chunk_has_uncompressed_rows
        && target_chunk_is_compressed
        && !target_chunk_is_partial
    {
        // If the source chunk has uncompressed rows, and the target chunk status
        // is compressed but not partial, we need to copy the uncompressed rows
        // into the hypertable instead of directly into the chunk.
        // The alternative would be to write into the chunk, and then set the chunk
        // status to partial, but on Timescale the `tsdbadmin` user doesn't have
        // permissions to do this.
        warn!("chunk {} is partial in source, but not in target, copying into hypertable instead of chunk, this may cause a reduction in parallelism", task.source_chunk.quoted_name());
        copy_chunk_data(
            source_tx,
            target_tx,
            &task.source_chunk,
            &target_chunk,
            &target_chunk.hypertable,
            &task.filter,
            UncompressedOnly,
        )
        .await?
    } else {
        copy_chunk_data(
            source_tx,
            target_tx,
            &task.source_chunk,
            &target_chunk,
            &target_chunk,
            &task.filter,
            UncompressedOnly,
        )
        .await?
    };

    let mut compressed_result = None;

    if let Some(source_compressed_chunk) = source_compressed_chunk {
        if let Some(target_compressed_chunk) =
            get_compressed_chunk(target_tx, &target_chunk).await?
        {
            let result = copy_compressed_chunk_data(
                source_tx,
                target_tx,
                &source_compressed_chunk,
                &target_compressed_chunk,
            )
            .await?;
            compressed_result = Some(result);
        } else {
            let result = create_compressed_chunk_from_source_chunk(
                source_tx,
                target_tx,
                &source_compressed_chunk,
                &target_chunk,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to create compressed chunk {} for hypertable {}",
                    source_compressed_chunk.quoted_name(),
                    target_chunk.hypertable.quoted_name(),
                )
            })?;
            compressed_result = Some(result);
        };
    }
    Ok(CopyResult {
        rows: uncompressed_result.rows + compressed_result.as_ref().map(|r| r.rows).unwrap_or(0),
        bytes: uncompressed_result.bytes + compressed_result.as_ref().map(|r| r.bytes).unwrap_or(0),
    })
}

/// Copies a compressed source chunk's rows in uncompressed form and compresses
/// the target chunk afterwards, so the target holds data compressed in its own
/// format instead of a byte-level copy of the source's compressed layout.
async fn copy_chunk_data_and_recompress(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    task: &CopyTask,
    target_chunk: &TargetChunk,
) -> Result<CopyResult> {
    // The target chunk must be uncompressed to receive the rows: deleting or
    // inserting uncompressed rows doesn't reach its compressed chunk.
    if is_chunk_compressed(target_tx, target_chunk).await? {
        // On a resumed backfill the target chunk can be compressed only because
        // an earlier `stage` (from a binary without the check above)
        // pre-created its compressed chunk carrying the copied, unbacked
        // metadata. `decompress_chunk` plans over that chunk, which is exactly
        // what fails with `cache lookup failed for attribute 0` on PG16, so
        // report what actually happened instead of surfacing the planner error.
        // PG17 tolerates the planning, so there the decompression succeeds and
        // the recompression below rebuilds the chunk in a backed layout.
        let unbacked = unbacked_target_chunk_metadata(target_tx, target_chunk).await?;
        decompress_chunk(target_tx, target_chunk)
            .await
            .with_context(|| decompress_failure_context(target_chunk, &unbacked))?;
    }

    let result = copy_chunk_data(
        source_tx,
        target_tx,
        &task.source_chunk,
        target_chunk,
        target_chunk,
        &task.filter,
        CopyMode::UncompressedAndCompressed,
    )
    .await?;

    compress_chunk(target_tx, target_chunk).await?;

    Ok(result)
}

async fn create_compressed_chunk_from_source_chunk(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    target_uncompressed_chunk: &TargetChunk,
) -> Result<CopyResult> {
    let target_compressed_chunk_data_table = create_compressed_chunk_data_table(
        source_tx,
        target_tx,
        source_compressed_chunk,
        target_uncompressed_chunk,
    )
    .await?;
    let result = copy_compressed_chunk_data(
        source_tx,
        target_tx,
        source_compressed_chunk,
        &target_compressed_chunk_data_table,
    )
    .await?;
    create_compressed_chunk_from_data_table(
        source_tx,
        target_tx,
        source_compressed_chunk,
        target_uncompressed_chunk,
        &target_compressed_chunk_data_table,
    )
    .await?;
    Ok(result)
}

/// Creates a compressed chunk structure without copying data.
/// This is used during staging to pre-create compressed chunks, avoiding lock
/// contention during the copy phase.
pub async fn create_compressed_chunk_without_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    target_uncompressed_chunk: &TargetChunk,
) -> Result<TargetCompressedChunk> {
    let target_compressed_chunk_data_table = create_compressed_chunk_data_table(
        source_tx,
        target_tx,
        source_compressed_chunk,
        target_uncompressed_chunk,
    )
    .await?;
    create_compressed_chunk_from_data_table(
        source_tx,
        target_tx,
        source_compressed_chunk,
        target_uncompressed_chunk,
        &target_compressed_chunk_data_table,
    )
    .await?;
    Ok(target_compressed_chunk_data_table)
}

async fn hss_uncompressed_rows(source_tx: &Transaction<'_>, chunk: &SourceChunk) -> Result<bool> {
    let row = source_tx
        .query_one(
            &format!("SELECT exists(SELECT 1 FROM ONLY {})", &chunk.quoted_name()),
            &[],
        )
        .await?;
    Ok(row.get("exists"))
}

async fn chunk_status_is_partial(
    target_tx: &Transaction<'_>,
    target_chunk: &TargetChunk,
) -> Result<bool> {
    // Note: status is a bitfield, the 4th bit indicates whether the chunk is partially compressed
    let query = if features::chunk_catalog_uses_relid() {
        r"
        SELECT (status & 8)::bool as is_partial
        FROM _timescaledb_catalog.chunk
        WHERE relid = to_regclass(format('%I.%I', $1::text, $2::text))"
    } else {
        r"
        SELECT (status & 8)::bool as is_partial
        FROM _timescaledb_catalog.chunk
        WHERE schema_name = $1
          AND table_name = $2"
    };
    let row = target_tx
        .query_one(query, &[&target_chunk.schema, &target_chunk.table])
        .await?;
    Ok(row.get("is_partial"))
}

async fn compress_chunk(tx: &Transaction<'_>, chunk: &TargetChunk) -> Result<()> {
    tx.execute(
        "SELECT public.compress_chunk(format('%I.%I', $1::text, $2::text)::regclass)",
        &[&chunk.schema, &chunk.table],
    )
    .await?;
    Ok(())
}

async fn decompress_chunk(tx: &Transaction<'_>, chunk: &TargetChunk) -> Result<()> {
    tx.execute(
        "SELECT public.decompress_chunk(format('%I.%I', $1::text, $2::text)::regclass)",
        &[&chunk.schema, &chunk.table],
    )
    .await?;
    Ok(())
}

async fn is_chunk_compressed(tx: &Transaction<'_>, chunk: &Chunk) -> Result<bool> {
    tx.query_one(
        r"
        SELECT is_compressed
        FROM timescaledb_information.chunks
        WHERE chunk_schema = $1
          AND chunk_name = $2",
        &[&chunk.schema, &chunk.table],
    )
    .await
    .map(|r| r.get("is_compressed"))
    .context("failed to get chunk compression status")
}

#[derive(Debug, PartialEq)]
enum CopyMode {
    UncompressedOnly,
    UncompressedAndCompressed,
}

/// Copies a source chunk's uncompressed rows into the target.
///
/// `target_chunk` is the chunk the rows belong to. Clearing the range and
/// suspending the cagg invalidation trigger both act on it, because it is the
/// relation that ends up holding the rows. `copy_into` is where the rows are
/// written, which is that same chunk except when the caller has to route them
/// through the hypertable to let TimescaleDB set the chunk's status. Deleting
/// from the hypertable instead would clear nothing: `DELETE FROM ONLY
/// <hypertable>` reaches only the hypertable's own heap, and a hypertable keeps
/// every row in its chunks.
async fn copy_chunk_data<S: QuotedName, T: QuotedName>(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_table: &S,
    target_chunk: &TargetChunk,
    copy_into: &T,
    filter: &Option<String>,
    mode: CopyMode,
) -> Result<CopyResult> {
    debug!("Copying uncompressed chunk {}", source_table.quoted_name());

    let trigger_dropped = if features::cagg_invalidation_trigger() {
        drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?
    } else {
        false
    };

    if let Some(filter) = filter {
        delete_data_using_filter(target_tx, target_chunk, filter).await?;
    } else {
        delete_all_rows_from_chunk(target_tx, &target_chunk.quoted_name()).await?;
    }

    let copy_result = copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_table.quoted_name(),
        &copy_into.quoted_name(),
        filter,
        mode == UncompressedOnly,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }
    debug!(
        "Finished copying uncompressed chunk {}. Starting analysis.",
        source_table.quoted_name()
    );

    target_tx
        .execute(&format!("analyze {}", copy_into.quoted_name()), &[])
        .await?;

    debug!(
        "Finished analyzing uncompressed chunk {}",
        source_table.quoted_name()
    );

    Ok(copy_result)
}

async fn delete_data_using_filter(
    tx: &Transaction<'_>,
    table: &impl QuotedName,
    filter: &str,
) -> Result<()> {
    let chunk_name = table.quoted_name();
    debug!("Deleting rows from chunk {chunk_name} with filter {filter}");
    let rows = tx
        .execute(&format!("DELETE FROM {chunk_name} WHERE {filter}"), &[])
        .await?;
    debug!("Deleted {} rows from {chunk_name}", rows.human_count_bare());
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
            WHERE t.tgname = 'ts_cagg_invalidation_trigger'
              AND format('%I.%I', n.nspname, c.relname)::text = $1::text::regclass::text
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
    // Note: In the pre-relid catalog there is non-obvious stuff going on here.
    // The ::text::regclass::text cast dance reformats the relation name to the
    // database's canonical format. We _explicitly avoid_ casting the left hand
    // side to regclass, because the _timescaledb_catalog.chunk table contains
    // entries which refer to non-existent relations. This only happens when the
    // hypertable has a continuous aggregate on it, and the chunk was dropped.
    // Casting the non-existent relation to regclass throws an error.
    //
    // In the relid catalog (TS >= 2.29) dropped chunks are removed from the
    // catalog and `relid` is never null, so we can match on it directly.
    let query = if features::chunk_catalog_uses_relid() {
        r"
        SELECT hypertable_id
        FROM _timescaledb_catalog.chunk
        WHERE relid = $1::text::regclass
    "
    } else {
        r"
        SELECT hypertable_id
        FROM _timescaledb_catalog.chunk
        WHERE format('%I.%I', schema_name, table_name)::text = $1::text::regclass::text
    "
    };
    let hypertable_id: i32 = tx
        .query_one(query, &[&chunk_name])
        .await?
        .get("hypertable_id");

    tx.execute(
        &set_query_target_proc_schema(&format!(
            r"CREATE TRIGGER ts_cagg_invalidation_trigger
            AFTER INSERT OR DELETE OR UPDATE ON {chunk_name}
            FOR EACH ROW
            EXECUTE FUNCTION @extschema@.continuous_agg_invalidation_trigger('{hypertable_id}')"
        )),
        &[],
    )
    .await?;
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
) -> Result<CopyResult> {
    debug!("Copying compressed chunk {}", source_chunk.quoted_name());

    let trigger_dropped = if features::cagg_invalidation_trigger() {
        drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?
    } else {
        false
    };

    // Replace the target's compressed rows instead of appending to them, the
    // same way the uncompressed path does. Without this a chunk copied twice
    // ends up holding both copies of its compressed batches (issue #204), and
    // nothing in the target catches it: the batches are written into the
    // internal compressed table, underneath the uncompressed chunk that carries
    // the hypertable's indexes and constraints.
    //
    // Where a `DELETE` reaches compressed rows this is already done: the
    // `DELETE FROM ONLY <chunk>` that `copy_chunk_data` issued for the
    // uncompressed rows decompressed the chunk's batches and removed them too,
    // so there is nothing left here to delete.
    //
    // Deleting everything is the right scope here: a copy filtered by the
    // completion point never reaches this function, it takes the
    // decompress-and-copy-uncompressed branch in `copy_chunk` instead.
    if !features::delete_reaches_compressed_rows() {
        delete_all_rows_from_chunk(target_tx, &target_chunk.quoted_name()).await?;
    }

    let copy_result = copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk.quoted_name(),
        &target_chunk.quoted_name(),
        &None,
        true,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }

    debug!(
        "Finished copying compressed chunk {}. Starting analysis",
        source_chunk.quoted_name()
    );

    target_tx
        .execute(&format!("analyze {}", target_chunk.quoted_name()), &[])
        .await?;

    debug!(
        "Finished analyzing compressed chunk {}",
        source_chunk.quoted_name()
    );
    Ok(copy_result)
}

async fn copy_chunk_from_source_to_target(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk_name: &str,
    target_chunk_name: &str,
    filter: &Option<String>,
    use_only: bool,
) -> Result<CopyResult> {
    let only = if use_only { "ONLY" } else { "" };

    let format_str = if table_requires_text_format(source_tx, source_chunk_name).await? {
        warn!(
            "Table {} contains types that don't support binary I/O, using text format",
            source_chunk_name
        );
        "TEXT"
    } else {
        "BINARY"
    };

    let copy_out = filter.as_ref().map(|filter| {
        format!("COPY (SELECT * FROM {only} {source_chunk_name} WHERE {filter}) TO STDOUT WITH (FORMAT {format_str})")
    }).unwrap_or(
        format!("COPY (SELECT * FROM {only} {source_chunk_name}) TO STDOUT WITH (FORMAT {format_str})")
    );

    debug!("{copy_out}");

    let copy_in = format!("COPY {target_chunk_name} FROM STDIN WITH (FORMAT {format_str})");
    debug!("{copy_in}");

    let stream = source_tx.copy_out(&copy_out).await?;
    let sink: CopyInSink<Bytes> = target_tx.copy_in(&copy_in).await?;

    let result = copy_from_source_to_sink(stream, sink).await?;

    debug!(
        "Copied {} in {} rows for table {source_chunk_name}",
        result.bytes.human_count_bytes(),
        result.rows.human_count_bare()
    );

    Ok(result)
}

pub struct CopyResult {
    pub rows: u64,
    pub bytes: usize,
}

async fn copy_from_source_to_sink(
    stream: CopyOutStream,
    sink: CopyInSink<Bytes>,
) -> Result<CopyResult> {
    let buffer_size = 1024 * 1024; // 1MiB
    let mut buf = BytesMut::with_capacity(buffer_size);

    pin_mut!(stream);
    pin_mut!(sink);

    let mut bytes = 0;

    while let Some(row) = stream.next().await {
        let row = row?;
        buf.extend_from_slice(&row);
        if buf.len() > buffer_size {
            bytes += buf.len();
            sink.feed(buf.split().freeze()).await?;
        }
    }
    if !buf.is_empty() {
        bytes += buf.len();
        sink.feed(buf.split().freeze()).await?;
    }

    let rows = sink.finish().await?;

    TOTAL_BYTES_COPIED.fetch_add(bytes, Relaxed);

    Ok(CopyResult { rows, bytes })
}

async fn table_requires_text_format(
    tx: &Transaction<'_>,
    qualified_table_name: &str,
) -> Result<bool> {
    let row = tx
        .query_one(
            r#"
            SELECT COUNT(*) > 0 as needs_text_format
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE c.oid = $1::text::regclass::oid
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND (t.typsend = 0 OR t.typreceive = 0)
            "#,
            &[&qualified_table_name],
        )
        .await?;
    Ok(row.get("needs_text_format"))
}

pub async fn get_compressed_chunk(
    source_tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<Option<SourceCompressedChunk>> {
    // In the relid catalog (TS >= 2.29) compressed chunks are no longer chunk
    // rows. The compressed relation for a chunk is found through
    // `compression_settings.compress_relid` (only present for compressed
    // chunks, so an uncompressed chunk yields no row).
    let query = if features::chunk_catalog_uses_relid() {
        r#"
    SELECT
      n.nspname AS schema_name
    , cl.relname AS table_name
    FROM _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = ch.relid
    JOIN pg_class cl ON cl.oid = cs.compress_relid
    JOIN pg_namespace n ON n.oid = cl.relnamespace
    WHERE ch.relid = to_regclass(format('%I.%I', $1::text, $2::text))
    "#
    } else {
        r#"
    SELECT
      cch.schema_name
    , cch.table_name
    FROM _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.chunk cch ON ch.compressed_chunk_id = cch.id
    WHERE ch.schema_name = $1
      AND ch.table_name = $2
    "#
    };
    let row = source_tx
        .query_opt(query, &[&chunk.schema, &chunk.table])
        .await?;
    Ok(row.map(|r| SourceCompressedChunk {
        schema: r.get("schema_name"),
        table: r.get("table_name"),
    }))
}

/// Creates a Chunk in the same Hypertable and with the same slices as the
/// given Chunk.
pub async fn create_uncompressed_chunk(
    tx: &Transaction<'_>,
    source_chunk: &SourceChunk,
) -> Result<TargetChunk> {
    trace!(
        "creating uncompressed chunk from {:?} with slices {}",
        source_chunk,
        source_chunk.slices()?,
    );

    tx.execute(
        &set_query_target_proc_schema(
            r#"SELECT @extschema@.create_chunk(
            $1::text::regclass,
            slices => $2::TEXT::JSONB)"#,
        ),
        &[
            &quote_table_name(
                &source_chunk.hypertable.schema,
                &source_chunk.hypertable.table,
            ),
            &source_chunk.slices()?,
        ],
    )
    .await?;

    find_target_chunk_with_same_dimensions(tx, source_chunk).await
}

/// Creates a compressed chunk data table. The table is created in the given
/// `schema_name` and the `table_name` is prefixed with
/// `COMPRESS_TABLE_NAME_PREFIX`.
///
/// The generated table is not part of the chunks catalog, it's not associated
/// with any uncompressed chunk and it's missing the corresponding indexes,
/// constraints and triggers. To convert the table into a proper compressed
/// chunk, first the compressed data has to be inserted into it, then it needs
/// to be passed as argument to the
/// `_timescaledb_functions.create_compressed_chunk` function.
///
/// Trying to generate the same chunk name as TimescaleDB (TS) might produce
/// inconsistencies because TS uses the chunk ID as part of the name. Since we
/// are not directly inserting into the Chunk's catalog, we cannot guarantee
/// that the ID we use for the name will match what is generated by the
/// sequence used for the catalog's IDs.
async fn create_compressed_chunk_data_table(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    uncompressed_chunk: &TargetChunk,
) -> Result<TargetCompressedChunk> {
    let data_table_name = add_backfill_prefix(target_tx, &source_compressed_chunk.table).await?;
    let qualified_data_table_name =
        quote_table_name(&source_compressed_chunk.schema, &data_table_name);
    if features::per_chunk_compression() {
        create_compressed_chunk_data_table_from_source_chunk(
            source_tx,
            target_tx,
            source_compressed_chunk,
            uncompressed_chunk,
            &qualified_data_table_name,
        )
        .await?;
    } else {
        create_compressed_chunk_data_table_from_parent(
            target_tx,
            uncompressed_chunk,
            &qualified_data_table_name,
        )
        .await?;
    }

    Ok(TargetCompressedChunk {
        schema: source_compressed_chunk.schema.clone(),
        table: data_table_name,
    })
}

// When creating a compressed chunk by going through the public API
// with `SELECT compress_chunk(...)` or the private one
// `_timescaledb_functions.create_compressed_chunk` both methods end up
// calling the [create_compress_chunk] function.
//
// [create_compress_chunk]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/create.c#L218
//
// The public method creates the data table while the private takes an
// existing table as argument. When the data table is created by the
// extension, it takes care of setting related to statistics, storage,
// indexes, constraints. These are all handled by the
// [compression_chunk_create] function.
//
// [compression_chunk_create]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L103
//
// We use the private API to avoid decompressing the data
// from source inserting into a chunk and compressing again, thus, we
// have to take care of setting everything appropriately and replicate
// most of the logic.
async fn create_compressed_chunk_data_table_from_source_chunk(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    uncompressed_chunk: &TargetChunk,
    qualified_data_table_name: &str,
) -> Result<()> {
    let compression_settings = validate_and_fetch_compression_settings(
        source_tx,
        target_tx,
        source_compressed_chunk,
        &uncompressed_chunk.hypertable,
    )
    .await?;
    let compressed_chunk_schema = fetch_compressed_chunk_schema_from_source(
        source_tx,
        qualified_data_table_name,
        source_compressed_chunk,
    )
    .await?;
    trace!(
        "Creating compressed chunk data table {} as `{}`",
        qualified_data_table_name,
        compressed_chunk_schema.ddl_query,
    );

    target_tx
        .execute(&compressed_chunk_schema.ddl_query, &[])
        .await?;
    trace!(
        "Setting statistics for compressed chunk data table {}",
        qualified_data_table_name,
    );
    set_compressed_chunk_statistics(
        target_tx,
        &compressed_chunk_schema,
        qualified_data_table_name,
    )
    .await?;
    trace!(
        "Creating index for compressed chunk data table {}",
        qualified_data_table_name,
    );
    create_compressed_chunk_index(target_tx, qualified_data_table_name, &compression_settings)
        .await?;
    trace!(
        "Cloning constraints from hypertable {} to data table {}",
        uncompressed_chunk.hypertable.quoted_name(),
        qualified_data_table_name,
    );
    clone_constraints_to_chunk(
        target_tx,
        &uncompressed_chunk.hypertable,
        qualified_data_table_name,
    )
    .await?;
    Ok(())
}

// FUNCTION ONLY WORKS ON TS VERSIONS OLDER THAN 2.17
//
// Create the btree index for the compressed chunk which contains all the
// segment by columns plus the metadata sequence number.
//
// Analogous to the extension function [create_compressed_chunk_indexes].
//
// [create_compressed_chunk_indexes]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L271
const COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME: &str = "_ts_meta_sequence_num";
async fn create_compressed_chunk_index_pre_ts_217(
    target_tx: &Transaction<'_>,
    qualified_data_table_name: &str,
    compression_settings: &CompressionSettings,
) -> Result<()> {
    let mut index_columns = compression_settings.segmentby.clone();
    index_columns.push(COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME.to_string());
    let quoted_index_columns_list: String = index_columns
        .iter()
        .map(|c| sql::quote_ident(c))
        .reduce(|acc, c| acc + "," + &c)
        .unwrap_or_default();

    let query = format!(
        "create index on {qualified_data_table_name} using btree ({})",
        quoted_index_columns_list
    );

    target_tx.execute(&query, &[]).await?;
    Ok(())
}

// Create the btree index for the compressed chunk which contains all the
// segment by columns plus the metadata sequence number.
//
// Analogous to the extension function [create_compressed_chunk_indexes].
//
// [create_compressed_chunk_indexes]: https://github.com/timescale/timescaledb/blob/2.21.0/tsl/src/compression/compression_storage.c#L283
async fn create_compressed_chunk_index(
    target_tx: &Transaction<'_>,
    qualified_data_table_name: &str,
    compression_settings: &CompressionSettings,
) -> Result<()> {
    if !features::no_sequence_number_in_compressed_hypertables() {
        return create_compressed_chunk_index_pre_ts_217(
            target_tx,
            qualified_data_table_name,
            compression_settings,
        )
        .await;
    }

    // Newer TimescaleDB (~2.28) stores the compressed sparse-index metadata in
    // `_ts_meta_v2_first_<col>` / `_ts_meta_v2_last_<col>` columns (named after
    // the orderby column) and indexes those, instead of the older
    // `_ts_meta_min_<n>` / `_ts_meta_max_<n>` columns (indexed by position).
    // Some versions keep both column sets, so we detect the v2 columns on the
    // data table (a copy of the source compressed chunk) rather than gate on a
    // version, and prefer them to match what TimescaleDB itself builds.
    let use_v2_metadata = compressed_chunk_uses_v2_metadata(target_tx, qualified_data_table_name)
        .await
        .context("failed to detect compressed chunk metadata format")?;

    let mut index_columns: Vec<String> = compression_settings
        .segmentby
        .clone()
        .iter()
        .map(|c| quote_ident(c))
        .collect();

    // Add the metadata columns for the orderby settings
    for (i, orderby_column) in compression_settings.orderby.iter().enumerate() {
        let column_index = i + 1; // 1-based indexing as in C code
        let order_by = if compression_settings.orderby_desc[i] {
            // DESC ordering
            if compression_settings.orderby_nullsfirst[i] {
                // DESC with nulls first uses default nulls ordering
                "DESC"
            } else {
                // DESC with nulls last
                "DESC NULLS LAST"
            }
        } else {
            // ASC ordering
            if compression_settings.orderby_nullsfirst[i] {
                // ASC with nulls first
                "ASC NULLS FIRST"
            } else {
                // ASC with nulls last uses default nulls ordering
                "ASC"
            }
        };
        let (first_column, last_column) = if use_v2_metadata {
            (
                quote_ident(&format!("_ts_meta_v2_first_{orderby_column}")),
                quote_ident(&format!("_ts_meta_v2_last_{orderby_column}")),
            )
        } else {
            (
                quote_ident(&format!("_ts_meta_min_{column_index}")),
                quote_ident(&format!("_ts_meta_max_{column_index}")),
            )
        };
        index_columns.push(format!("{first_column} {order_by}"));
        index_columns.push(format!("{last_column} {order_by}"));
    }

    let quoted_index_columns_list: String = index_columns
        .into_iter()
        .reduce(|acc, c| acc + "," + &c)
        .unwrap_or_default();
    let query = format!(
        "create index on {qualified_data_table_name} using btree ({})",
        quoted_index_columns_list
    );
    target_tx
        .execute(&query, &[])
        .await
        .with_context(|| "failed to create compress chunk index")?;
    Ok(())
}

/// Detects whether a compressed chunk data table uses the v2 sparse-index
/// metadata columns (`_ts_meta_v2_first_<col>` / `_ts_meta_v2_last_<col>`).
async fn compressed_chunk_uses_v2_metadata(
    tx: &Transaction<'_>,
    qualified_data_table_name: &str,
) -> Result<bool> {
    let row = tx
        .query_one(
            r"
            SELECT EXISTS (
                SELECT 1
                FROM pg_attribute
                WHERE attrelid = $1::text::regclass
                  AND attname ~ '^_ts_meta_v2_first_'
                  AND NOT attisdropped
            )",
            &[&qualified_data_table_name],
        )
        .await?;
    Ok(row.get(0))
}

async fn fetch_compressed_chunk_compression_settings(
    tx: &Transaction<'_>,
    table_name: &String,
) -> Result<CompressionSettings> {
    let mut relid_column = quote_ident("compress_relid");
    if !features::compression_settings_with_compress_relid() {
        relid_column = quote_ident("relid");
    }
    let compression_settings_query = format!(
        r"
        SELECT segmentby, orderby, orderby_desc, orderby_nullsfirst
        FROM _timescaledb_catalog.compression_settings
        WHERE {relid_column} = $1::text::regclass
    "
    );
    let settings: CompressionSettings = tx
        .query_one(&compression_settings_query, &[&table_name])
        .await?
        .into();
    Ok(settings)
}

async fn fetch_hypertable_compression_settings(
    tx: &Transaction<'_>,
    table_name: &String,
) -> Result<CompressionSettings> {
    let compression_settings_query = r"
        SELECT segmentby, orderby, orderby_desc, orderby_nullsfirst
        FROM _timescaledb_catalog.compression_settings
        WHERE relid = $1::text::regclass
    ";
    let settings: CompressionSettings = tx
        .query_one(compression_settings_query, &[&table_name])
        .await?
        .into();
    Ok(settings)
}

#[derive(Debug, PartialEq)]
struct CompressionSettings {
    segmentby: Vec<String>,
    orderby: Vec<String>,
    orderby_desc: Vec<bool>,
    orderby_nullsfirst: Vec<bool>,
}

impl From<Row> for CompressionSettings {
    fn from(row: Row) -> Self {
        // In the relid catalog (TS >= 2.29) the hypertable-level
        // `compression_settings` row only stores explicitly-configured values,
        // leaving defaulted columns (e.g. the default time `orderby`) NULL. The
        // per-chunk rows still materialize them. Treat NULL as empty.
        fn col(row: &Row, name: &str) -> Vec<String> {
            row.get::<_, Option<Vec<String>>>(name).unwrap_or_default()
        }
        fn flags(row: &Row, name: &str) -> Vec<bool> {
            row.get::<_, Option<Vec<bool>>>(name).unwrap_or_default()
        }
        CompressionSettings {
            segmentby: col(&row, "segmentby"),
            orderby: col(&row, "orderby"),
            orderby_desc: flags(&row, "orderby_desc"),
            orderby_nullsfirst: flags(&row, "orderby_nullsfirst"),
        }
    }
}

// Compares if compression settings for the chunk in source match the
// compression settings for the hypertable in target.
//
// With per chunk compression settings new compressed chunks will be created
// with the current hypertable's compression settings. We might want to create
// an old chunk using and old compression setting. If we don't make this check
// we might end up creating a chunk with a table definition that doesn't match
// the compression settings.
async fn validate_and_fetch_compression_settings(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    target_hypertable: &Hypertable,
) -> Result<CompressionSettings> {
    let target_hypertable_name =
        &format!("{}.{}", target_hypertable.schema, target_hypertable.table);
    let target_settings =
        fetch_hypertable_compression_settings(target_tx, target_hypertable_name).await?;
    let source_chunk_name = &format!(
        "{}.{}",
        &source_compressed_chunk.schema, &source_compressed_chunk.table
    );
    let source_settings =
        fetch_compressed_chunk_compression_settings(source_tx, source_chunk_name).await?;

    // In the relid catalog (TS >= 2.29) the hypertable-level settings omit a
    // defaulted `orderby`, while the source chunk carries the materialized
    // settings. Consider them compatible when the segment-by columns match and
    // the target either shares the same explicit `orderby` or relies on the
    // default (empty), which matches the source chunk's materialized default.
    let compatible = if features::chunk_catalog_uses_relid() {
        source_settings.segmentby == target_settings.segmentby
            && (target_settings.orderby.is_empty()
                || (source_settings.orderby == target_settings.orderby
                    && source_settings.orderby_desc == target_settings.orderby_desc
                    && source_settings.orderby_nullsfirst == target_settings.orderby_nullsfirst))
    } else {
        source_settings == target_settings
    };
    if !compatible {
        bail!(
            r"Compression settings mismatch.

Compression settings for the compressed chunk '{source_chunk_name}'
in source are different than the settings for the hypertable '{target_hypertable_name}'
in target:

- SOURCE: {source_settings:?}
- TARGET: {target_settings:?}

Stop compression jobs in the source, set the compression settings in the
target to be the same as those of the compressed chunk, and restart the copy
operation. Once the chunks with the old compression settings have been
backfilled, you can change the settings back and restart the compression jobs.
"
        );
    }

    // The compressed chunk data table (and its index) must reflect the
    // materialized layout of the actual compressed data. Pre-2.29 the hypertable
    // settings already were materialized; in the relid catalog we must use the
    // source chunk's settings so a defaulted `orderby` is not lost.
    if features::chunk_catalog_uses_relid() {
        Ok(source_settings)
    } else {
        Ok(target_settings)
    }
}

/// A sparse-index metadata entry from `compression_settings.index`, identified
/// by its type (e.g. `minmax`, `firstlast`) and the column it indexes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparseIndexEntry {
    kind: String,
    column: String,
}

impl std::fmt::Display for SparseIndexEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} on {}", self.kind, self.column)
    }
}

/// Parses the `index` jsonb column of `compression_settings` into a list of
/// sparse-index entries. An absent or NULL value yields an empty list; any
/// other shape the parser cannot positively interpret is an error.
///
/// This fails closed on purpose. The empty list means "declares no sparse
/// index", which routes a chunk down the compressed-copy path, so silently
/// answering it for a catalog shape we don't recognize would let through
/// exactly the corruption the caller exists to prevent, and that corruption is
/// invisible on PG17 and unrepairable as `tsdbadmin`.
fn parse_sparse_index(value: Option<serde_json::Value>) -> Result<Vec<SparseIndexEntry>> {
    let entries = match value {
        None | Some(serde_json::Value::Null) => return Ok(vec![]),
        Some(serde_json::Value::Array(entries)) => entries,
        Some(other) => bail!("unrecognized compression_settings.index shape: {other}"),
    };
    entries
        .iter()
        .map(|entry| {
            let field = |name: &str| -> Result<String> {
                entry
                    .get(name)
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string())
                    .with_context(|| {
                        format!("compression_settings.index entry has no string `{name}`: {entry}")
                    })
            };
            Ok(SparseIndexEntry {
                kind: field("type")?,
                column: field("column")?,
            })
        })
        .collect()
}

/// The physical columns of a compressed chunk that back a sparse-index entry,
/// or `None` for an entry type whose physical layout we don't know.
///
/// `firstlast` is materialized as `_ts_meta_v2_first_<column>` /
/// `_ts_meta_v2_last_<column>`, the same columns `create_compressed_chunk_index`
/// builds its index on.
fn sparse_index_backing_columns(entry: &SparseIndexEntry) -> Option<Vec<String>> {
    match entry.kind.as_str() {
        "firstlast" => Some(vec![
            format!("_ts_meta_v2_first_{}", entry.column),
            format!("_ts_meta_v2_last_{}", entry.column),
        ]),
        _ => None,
    }
}

/// Filters `entries` down to those the compressed chunk's `columns` cannot back.
///
/// `minmax` is skipped: it always has a working representation on a compressed
/// chunk, either the positional `_ts_meta_min_N` columns or the v2
/// `_ts_meta_v2_min_<col>` ones, so it never dangles.
fn unbacked_entries<'a>(
    entries: &'a [SparseIndexEntry],
    columns: &std::collections::HashSet<String>,
) -> Vec<&'a SparseIndexEntry> {
    entries
        .iter()
        .filter(|entry| entry.kind != "minmax")
        .filter(|entry| match sparse_index_backing_columns(entry) {
            Some(required) => !required.iter().all(|column| columns.contains(column)),
            // An entry type whose physical layout we don't know can't be
            // verified. Call it unbacked: the fallback is only slower, while a
            // wrong "compatible" answer corrupts the target chunk's catalog.
            None => true,
        })
        .collect()
}

/// The `_ts_meta_*` column names present on a compressed chunk's physical table.
async fn fetch_metadata_columns(
    tx: &Transaction<'_>,
    table_name: &String,
) -> Result<std::collections::HashSet<String>> {
    let rows = tx
        .query(
            r"
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = $1::text::regclass
              AND attname ~ '^_ts_meta_'
              AND attnum > 0
              AND NOT attisdropped",
            &[table_name],
        )
        .await
        .with_context(|| format!("failed to read metadata columns of {table_name}"))?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>("attname"))
        .collect())
}

/// Fetches the sparse-index metadata (`compression_settings.index`) for the
/// relation matching `relid_column = $1`. A relation without a compression
/// settings row is an error: an empty index and a missing row mean different
/// things to the callers, and the sibling
/// `fetch_compressed_chunk_compression_settings` treats a missing row the same
/// way.
async fn fetch_sparse_index(
    tx: &Transaction<'_>,
    relid_column: &str,
    table_name: &String,
) -> Result<Vec<SparseIndexEntry>> {
    let query = format!(
        r"
        SELECT index
        FROM _timescaledb_catalog.compression_settings
        WHERE {relid_column} = $1::text::regclass
    "
    );
    let row = tx
        .query_opt(&query, &[&table_name])
        .await?
        .with_context(|| format!("no compression settings found for {table_name}"))?;
    parse_sparse_index(row.get("index"))
}

/// Fetches the sparse-index entries the target hypertable declares, which
/// `create_compressed_chunk` stamps onto every compressed chunk it creates.
///
/// Invariant per hypertable, so `stage` fetches it once per hypertable rather
/// than once per chunk.
pub async fn fetch_hypertable_sparse_index(
    target_tx: &Transaction<'_>,
    target_hypertable: &Hypertable,
) -> Result<Vec<SparseIndexEntry>> {
    if !features::hypertable_sparse_index_metadata() {
        return Ok(vec![]);
    }
    let target_hypertable_name =
        format!("{}.{}", target_hypertable.schema, target_hypertable.table);
    fetch_sparse_index(target_tx, "relid", &target_hypertable_name).await
}

/// Detects the sparse-index metadata mismatch described in issue #195, and
/// returns the entries of `target_hypertable_index` that the source compressed
/// chunk's physical layout cannot back (empty when it backs them all).
///
/// From TS 2.28 the hypertable-level `compression_settings.index` carries
/// entries such as `firstlast` that `create_compressed_chunk` copies verbatim
/// onto every new chunk. When a source chunk was compressed under an older
/// format its physical table lacks the `_ts_meta_*` columns backing those
/// entries (the target chunk data table is a copy of that layout), so the
/// copied metadata references columns that do not exist. On PG16 targets this
/// makes every query that plans over the chunk fail with a `cache lookup
/// failed for attribute 0` error; on PG17 the planner silently tolerates it.
///
/// The mismatch is catalog-versus-physical, so this reads the source chunk's
/// actual columns rather than comparing the two catalogs: a chunk's
/// `compression_settings` row materializes defaults the hypertable row omits,
/// which makes catalog-to-catalog equality report compatible layouts as
/// mismatched.
///
/// A chunk with unbacked entries cannot be copied in compressed form. `copy`
/// falls back to copying its rows uncompressed and letting the target compress
/// them in its own format; `stage` only skips pre-creating the target
/// compressed chunk and warns, leaving the copy decision to `copy`.
pub async fn unbacked_sparse_index_metadata(
    source_tx: &Transaction<'_>,
    source_compressed_chunk: &SourceCompressedChunk,
    target_hypertable_index: &[SparseIndexEntry],
) -> Result<Vec<String>> {
    if target_hypertable_index
        .iter()
        .all(|entry| entry.kind == "minmax")
    {
        return Ok(vec![]);
    }

    let source_chunk_name = format!(
        "{}.{}",
        source_compressed_chunk.schema, source_compressed_chunk.table
    );
    let source_columns = fetch_metadata_columns(source_tx, &source_chunk_name).await?;

    Ok(unbacked_entries(target_hypertable_index, &source_columns)
        .into_iter()
        .map(|entry| entry.to_string())
        .collect())
}

/// Returns the entries in the target chunk's own compressed-chunk metadata that
/// its physical layout cannot back, i.e. the corruption of issue #195 already
/// present in the target.
async fn unbacked_target_chunk_metadata(
    target_tx: &Transaction<'_>,
    target_chunk: &TargetChunk,
) -> Result<Vec<String>> {
    if !features::hypertable_sparse_index_metadata() {
        return Ok(vec![]);
    }
    let Some(target_compressed_chunk) = get_compressed_chunk(target_tx, target_chunk).await? else {
        return Ok(vec![]);
    };
    let target_compressed_chunk_name = format!(
        "{}.{}",
        target_compressed_chunk.schema, target_compressed_chunk.table
    );
    // Chunk-level settings are keyed by `compress_relid`; reaching this code
    // requires TS >= 2.28, which implies the column exists.
    let index =
        fetch_sparse_index(target_tx, "compress_relid", &target_compressed_chunk_name).await?;
    let columns = fetch_metadata_columns(target_tx, &target_compressed_chunk_name).await?;
    Ok(unbacked_entries(&index, &columns)
        .into_iter()
        .map(|entry| entry.to_string())
        .collect())
}

/// The context for a failed decompression of a target chunk, naming the issue
/// #195 corruption as the cause when the chunk's metadata carries it.
fn decompress_failure_context(target_chunk: &TargetChunk, unbacked: &[String]) -> String {
    let chunk = target_chunk.quoted_name();
    if unbacked.is_empty() {
        return format!("failed to decompress target chunk {chunk}");
    }
    format!(
        r"failed to decompress target chunk {chunk}.

Its compressed chunk was pre-created by an earlier `stage` with sparse-index
metadata that its physical layout does not back ({entries}), which PG16 cannot
plan over (issue #195). Drop the compressed chunk in the target, then run
`stage` again with this version of timescaledb-backfill, which leaves the
affected chunks uncompressed until `copy` recompresses them in the target's own
format.",
        entries = unbacked.join(", "),
    )
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

/// Returns the name the compressed hypertable associated to the given
/// uncompressed hypertable.
async fn fetch_compressed_hypertable(
    tx: &Transaction<'_>,
    uncompressed_hypertable_schema: &String,
    uncompressed_hypertable_table: &String,
) -> Result<Hypertable> {
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

    let schema: String = row.get("schema_name");
    let table: String = row.get("table_name");
    Ok(Hypertable { schema, table })
}

/// Uses `_timescaledb_functions.create_compressed_chunk` to convert the
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
    let query = r#"
        SELECT @extschema@.create_compressed_chunk(
            $1::TEXT::REGCLASS,
            $2::TEXT::REGCLASS,
            $3,$4,$5,$6,$7,$8,$9,$10)"#;
    target_tx
        .execute(
            &set_query_target_proc_schema(query),
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
    // The numrows_{pre,post}_compression columns were introduced in TimescaleDB
    // 2.0. The upgrade path does not populate default values for older
    // installations. Ensure 0 is returned instead of NULL to avoid handling
    // Option<> in the struct.
    //
    // In the relid catalog (TS >= 2.29) `compression_chunk_size` is keyed by the
    // uncompressed chunk's `chunk_id` (the `compressed_chunk_id` column is dead),
    // so we reach it from the compressed relation through
    // `compression_settings.compress_relid`.
    let query = if features::chunk_catalog_uses_relid() {
        r#"
SELECT
    uncompressed_heap_size,
    uncompressed_toast_size,
    uncompressed_index_size,
    compressed_heap_size,
    compressed_toast_size,
    compressed_index_size,
    COALESCE(numrows_pre_compression, 0) AS numrows_pre_compression,
    COALESCE(numrows_post_compression, 0) AS numrows_post_compression
FROM _timescaledb_catalog.compression_chunk_size s
JOIN _timescaledb_catalog.chunk c ON c.id = s.chunk_id
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = c.relid
WHERE cs.compress_relid = to_regclass(format('%I.%I', $1::text, $2::text))
        "#
    } else {
        r#"
SELECT
    uncompressed_heap_size,
    uncompressed_toast_size,
    uncompressed_index_size,
    compressed_heap_size,
    compressed_toast_size,
    compressed_index_size,
    COALESCE(numrows_pre_compression, 0) AS numrows_pre_compression,
    COALESCE(numrows_post_compression, 0) AS numrows_post_compression
FROM _timescaledb_catalog.compression_chunk_size s
JOIN _timescaledb_catalog.chunk c ON c.id = s.compressed_chunk_id
WHERE c.schema_name = $1 AND c.table_name = $2
        "#
    };
    let row = tx
        .query_one(query, &[&compressed_chunk.schema, &compressed_chunk.table])
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

pub async fn chunk_exists<T>(client: &T, chunk: &Chunk) -> Result<bool>
where
    T: GenericClient,
{
    // In the relid catalog (TS >= 2.29) the relation is matched by `relid`;
    // `to_regclass` yields NULL for a non-existent relation, so the comparison
    // is simply never true.
    let query: &str = if features::chunk_catalog_uses_relid() {
        r#"
SELECT EXISTS (
  SELECT 1
  FROM _timescaledb_catalog.chunk
  WHERE relid = to_regclass(format('%I.%I', $1::text, $2::text))
)
"#
    } else {
        r#"
SELECT EXISTS (
  SELECT 1
  FROM _timescaledb_catalog.chunk
  WHERE schema_name = $1
    AND table_name = $2
)
"#
    };
    let exists = client
        .query_one(query, &[&chunk.schema, &chunk.table])
        .await?;
    Ok(exists.get(0))
}

/// Only for TS < 2.14.
///
/// Creates the compressed chunk data table inheriting from the parent
/// compressed hypertable. From TS >= 2.14 the parent table doesn't have any
/// columns, in that scenario use
/// `create_compressed_chunk_data_table_query_from_source_chunk` instead.
async fn create_compressed_chunk_data_table_from_parent(
    target_tx: &Transaction<'_>,
    uncompressed_chunk: &TargetChunk,
    qualified_data_table_name: &str,
) -> Result<()> {
    let parent_table = fetch_compressed_hypertable(
        target_tx,
        &uncompressed_chunk.hypertable.schema,
        &uncompressed_chunk.hypertable.table,
    )
    .await?;

    let schema = fetch_compressed_chunk_schema_from_parent(
        target_tx,
        qualified_data_table_name,
        &parent_table,
    )
    .await?;

    trace!(
        "Creating compressed chunk data table {} as `{}`",
        &qualified_data_table_name,
        schema.ddl_query
    );

    target_tx.execute(&schema.ddl_query, &[]).await?;

    trace!(
        "Setting statistics for compressed chunk data table {}",
        qualified_data_table_name,
    );

    set_compressed_chunk_statistics(target_tx, &schema, qualified_data_table_name).await?;

    Ok(())
}

struct CompressedChunkSchema {
    compressed_columns: Vec<String>,
    uncompressed_columns: Vec<String>,
    ddl_query: String,
}

/// Only for TS >= 2.14.
///
/// Returns the query to create the compressed chunk by inspecting the
/// columns definition of the chunk in the source database.
async fn fetch_compressed_chunk_schema_from_source(
    source_tx: &Transaction<'_>,
    data_table_name: &str,
    source_chunk: &CompressedChunk,
) -> Result<CompressedChunkSchema> {
    let columns_query: &str = r"
SELECT
  column_name, udt_schema, udt_name, character_maximum_length,
  is_nullable, collation_name, column_default, is_identity,
  CASE attstorage
    WHEN 'x' THEN 'EXTENDED'
    WHEN 'p' THEN 'PLAIN'
    WHEN 'e' THEN 'EXTERNAL'
    WHEN 'm' THEN 'MAIN'
  ELSE 'UNKNOWN'
END AS toast_storage_type
FROM information_schema.columns cols
INNER JOIN pg_catalog.pg_attribute att
  ON cols.column_name = att.attname
  AND att.attrelid = (quote_ident(cols.table_schema) || '.' || quote_ident(cols.table_name))::regclass::oid
WHERE cols.table_schema = $1 AND cols.table_name = $2
    ";

    trace!("Fetching columns definition for chunk {source_chunk:?}");
    let rows = source_tx
        .query(columns_query, &[&source_chunk.schema, &source_chunk.table])
        .await
        .with_context(|| "couldn't fetch source chunk table definition")?;

    // Generate the CREATE TABLE query statement
    let mut create_table_query = format!("CREATE TABLE {} (", data_table_name);
    let mut compressed_columns: Vec<String> = vec![];
    let mut uncompressed_columns: Vec<String> = vec![];
    for (i, row) in rows.iter().enumerate() {
        let column_name: String = sql::quote_ident(row.get("column_name"));
        let udt_schema: &str = row.get(1);
        let udt_name: &str = row.get(2);
        let character_max_length: Option<i32> = row.get(3);
        let is_nullable: &str = row.get(4);
        let collation_name: Option<&str> = row.get(5);
        let column_default: Option<String> = row.get(6);
        let is_identity: &str = row.get(7);
        let attstorage: &str = row.get(8);

        create_table_query.push_str(&format!(
            "{} {}.{}",
            column_name,
            sql::quote_ident(udt_schema),
            sql::quote_ident(udt_name)
        ));

        if is_compressed_data_type(udt_schema, udt_name) {
            compressed_columns.push(column_name);
        } else {
            uncompressed_columns.push(column_name);
        }

        // Add character maximum length if applicable
        if let Some(max_length) = character_max_length {
            create_table_query.push_str(&format!("({})", max_length));
        }

        if features::storage_type_in_create_table() && !attstorage.is_empty() {
            // The STORAGE is set by the extension when creating the compressed
            // hypertable with the [modify_compressed_toast_table_storage].
            //
            // [modify_compressed_toast_table_storage]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L226
            create_table_query.push_str(&format!(" STORAGE {}", attstorage));
        }

        // Add collation information if applicable
        if let Some(collation) = collation_name {
            create_table_query.push_str(&format!(" COLLATE {}", collation));
        }

        // Add default value if applicable
        if let Some(default_value) = column_default {
            create_table_query.push_str(&format!(" DEFAULT {}", default_value));
        }

        // Add identity constraint if applicable
        if is_identity == "YES" {
            create_table_query.push_str(" GENERATED ALWAYS AS IDENTITY");
        }

        // Add nullable constraint
        if is_nullable == "YES" {
            create_table_query.push_str(" NULL");
        } else {
            create_table_query.push_str(" NOT NULL");
        }

        // Add comma if it's not the last column
        if i != rows.len() - 1 {
            create_table_query.push(',');
        }
    }

    // The toast_tuple_target is set by the extension when creating the
    // compressed hypertable by calling the [set_toast_tuple_target_on_chunk]
    // function.
    //
    // [set_toast_tuple_target_on_chunk]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L157
    create_table_query.push_str(") ");
    create_table_query.push_str(WITH_TOAST_TUPLE_TARGET);

    Ok(CompressedChunkSchema {
        compressed_columns,
        uncompressed_columns,
        ddl_query: create_table_query,
    })
}

/// Only for TS < 2.14.
///
/// Returns the query to create the compressed chunk by inspecting the
/// columns definition of the chunk in the source database.
async fn fetch_compressed_chunk_schema_from_parent(
    target_tx: &Transaction<'_>,
    qualified_data_table_name: &str,
    parent: &Hypertable,
) -> Result<CompressedChunkSchema> {
    let columns_query: &str = r"
        SELECT column_name, udt_schema, udt_name
        FROM information_schema.columns cols
        WHERE cols.table_schema = $1 AND cols.table_name = $2
    ";

    trace!("Fetching columns definition for parent compressed hypertable {parent:?}");
    let rows = target_tx
        .query(columns_query, &[&parent.schema, &parent.table])
        .await
        .with_context(|| "couldn't fetch parent compressed hypertable definition")?;

    // Generate the CREATE TABLE query statement
    //
    // The toast_tuple_target is set by the extension when creating the
    // compressed hypertable. This produces the same result as the extension
    // function [set_toast_tuple_target_on_chunk].
    //
    // [set_toast_tuple_target_on_chunk]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L157.
    let create_table_query = format!(
        "CREATE TABLE {}() INHERITS ({}) {}",
        qualified_data_table_name,
        parent.quoted_name(),
        WITH_TOAST_TUPLE_TARGET
    );
    let mut compressed_columns: Vec<String> = vec![];
    let mut uncompressed_columns: Vec<String> = vec![];
    for row in rows.iter() {
        let column_name = sql::quote_ident(row.get("column_name"));
        let udt_schema: &str = row.get(1);
        let udt_name: &str = row.get(2);

        if is_compressed_data_type(udt_schema, udt_name) {
            compressed_columns.push(column_name);
        } else {
            uncompressed_columns.push(column_name);
        }
    }

    Ok(CompressedChunkSchema {
        compressed_columns,
        uncompressed_columns,
        ddl_query: create_table_query,
    })
}

fn is_compressed_data_type(udt_schema: &str, udt_name: &str) -> bool {
    udt_schema == TIMESCALE_INTERNAL_SCHEMA && udt_name == TIMESCALE_COMPRESSED_DATA_TYPE
}

// Sets the statistics to the compressed chunk columns.
//
// The planner should never look at compressed column statistics because it
// will not understand them. Statistics on the other columns, segmentbys and
// metadata, are very important, so their targets are increased.
//
// Analogous to the extension function [set_statistics_on_compressed_chunk].
//
// [set_statistics_on_compressed_chunk]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L175
async fn set_compressed_chunk_statistics(
    target_tx: &Transaction<'_>,
    chunk_schema: &CompressedChunkSchema,
    qualified_data_table_name: &str,
) -> Result<()> {
    for compressed_column in chunk_schema.compressed_columns.iter() {
        target_tx
            .execute(
                &format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET STATISTICS 0",
                    qualified_data_table_name, compressed_column
                ),
                &[],
            )
            .await?;
    }
    for uncompressed_column in chunk_schema.uncompressed_columns.iter() {
        target_tx
            .execute(
                &format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET STATISTICS 1000",
                    qualified_data_table_name, uncompressed_column
                ),
                &[],
            )
            .await?;
    }
    Ok(())
}

// Clone the hypertable's constraints to the compressed chunk data table.
//
// Analogous to the extension function [clone_constraints_to_chunk].
//
// [clone_constraints_to_chunk]: https://github.com/timescale/timescaledb/blob/2.14.2/tsl/src/compression/compression_storage.c#L343
async fn clone_constraints_to_chunk(
    target_tx: &Transaction<'_>,
    hypertable: &Hypertable,
    qualified_data_table_name: &str,
) -> Result<()> {
    let query = &set_query_target_proc_schema(
        r"
      SELECT @extschema@.constraint_clone(
        oid,
        $2::text::regclass::oid)
      FROM pg_constraint
      WHERE conrelid = $1::text::regclass::oid AND contype = 'f'",
    );
    target_tx
        .execute(
            query,
            &[&hypertable.quoted_name(), &qualified_data_table_name],
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry(kind: &str, column: &str) -> SparseIndexEntry {
        SparseIndexEntry {
            kind: kind.to_string(),
            column: column.to_string(),
        }
    }

    fn columns(names: &[&str]) -> std::collections::HashSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn parse_sparse_index_reads_type_and_column() {
        let value = json!([
            {"type": "minmax", "column": "time"},
            {"type": "firstlast", "column": "value"},
        ]);
        assert_eq!(
            parse_sparse_index(Some(value)).unwrap(),
            vec![entry("minmax", "time"), entry("firstlast", "value")],
        );
    }

    #[test]
    fn parse_sparse_index_null_yields_empty() {
        assert_eq!(parse_sparse_index(None).unwrap(), vec![]);
        assert_eq!(
            parse_sparse_index(Some(serde_json::Value::Null)).unwrap(),
            vec![]
        );
    }

    #[test]
    fn parse_sparse_index_errors_on_malformed_entries() {
        for malformed in [
            json!([{"type": "firstlast"}]),
            json!([{"column": "orphan"}]),
            json!([{"type": 42, "column": "value"}]),
            json!(["not-an-object"]),
        ] {
            assert!(
                parse_sparse_index(Some(malformed.clone())).is_err(),
                "expected {malformed} to be rejected"
            );
        }
    }

    #[test]
    fn parse_sparse_index_errors_on_non_array() {
        assert!(parse_sparse_index(Some(json!({"type": "firstlast"}))).is_err());
    }

    #[test]
    fn unbacked_flags_firstlast_without_backing_columns() {
        let index = vec![entry("minmax", "time"), entry("firstlast", "value")];
        let present = columns(&["_ts_meta_min_1", "_ts_meta_max_1"]);
        assert_eq!(
            unbacked_entries(&index, &present),
            vec![&entry("firstlast", "value")],
        );
    }

    #[test]
    fn unbacked_flags_firstlast_with_only_one_backing_column() {
        let index = vec![entry("firstlast", "value")];
        let present = columns(&["_ts_meta_v2_first_value"]);
        assert_eq!(
            unbacked_entries(&index, &present),
            vec![&entry("firstlast", "value")],
        );
    }

    #[test]
    fn unbacked_ignores_minmax() {
        let index = vec![entry("minmax", "time"), entry("minmax", "value")];
        assert!(unbacked_entries(&index, &columns(&[])).is_empty());
    }

    #[test]
    fn unbacked_empty_when_columns_back_all_entries() {
        let index = vec![entry("minmax", "time"), entry("firstlast", "value")];
        let present = columns(&["_ts_meta_v2_first_value", "_ts_meta_v2_last_value"]);
        assert!(unbacked_entries(&index, &present).is_empty());
    }

    #[test]
    fn unbacked_matches_on_column_not_just_kind() {
        let index = vec![entry("firstlast", "value")];
        let present = columns(&["_ts_meta_v2_first_other", "_ts_meta_v2_last_other"]);
        assert_eq!(
            unbacked_entries(&index, &present),
            vec![&entry("firstlast", "value")],
        );
    }

    #[test]
    fn unbacked_flags_unknown_entry_types() {
        // An entry type whose physical layout we don't know must fail closed,
        // even when the chunk carries plenty of metadata columns.
        let index = vec![entry("bloom", "value")];
        let present = columns(&[
            "_ts_meta_v2_first_value",
            "_ts_meta_v2_last_value",
            "_ts_meta_v2_bloom1_value",
        ]);
        assert_eq!(
            unbacked_entries(&index, &present),
            vec![&entry("bloom", "value")],
        );
    }

    #[test]
    fn backing_columns_are_the_v2_first_and_last_columns() {
        assert_eq!(
            sparse_index_backing_columns(&entry("firstlast", "time")),
            Some(vec![
                "_ts_meta_v2_first_time".to_string(),
                "_ts_meta_v2_last_time".to_string(),
            ]),
        );
        assert_eq!(sparse_index_backing_columns(&entry("bloom", "time")), None);
    }

    #[test]
    fn decompress_failure_context_names_issue_195_only_when_unbacked() {
        let chunk = TargetChunk {
            schema: "_timescaledb_internal".to_string(),
            table: "_hyper_1_1_chunk".to_string(),
            hypertable: Hypertable {
                schema: "public".to_string(),
                table: "metrics".to_string(),
            },
            dimensions: vec![],
        };
        assert!(!decompress_failure_context(&chunk, &[]).contains("195"));
        let with_unbacked = decompress_failure_context(&chunk, &["firstlast on time".to_string()]);
        assert!(with_unbacked.contains("195"));
        assert!(with_unbacked.contains("firstlast on time"));
    }
}
