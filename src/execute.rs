use crate::execute::CopyMode::UncompressedOnly;
use crate::sql::{quote_ident, quote_table_name};
use crate::task::{find_target_chunk_with_same_dimensions, CopyTask};
use crate::timescale::{
    set_query_target_proc_schema, Chunk, CompressedChunk, CompressionSize, Hypertable, QuotedName,
    SourceChunk, SourceCompressedChunk, TargetChunk, TargetCompressedChunk,
};
use crate::{features, sql};
use anyhow::{anyhow, bail, Context, Result};
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
    let target_chunk = match task.target_chunk.as_ref() {
        Some(target_chunk) => target_chunk.clone(),
        None => create_uncompressed_chunk(target_tx, &task.source_chunk).await?,
    };

    let source_chunk_compressed = is_chunk_compressed(source_tx, &task.source_chunk).await?;

    // If we're trying to filter on a compressed chunk, fall back to reading rows directly
    // from the uncompressed chunk, and write the uncompressed rows into the target.
    // Note: we must check the compression status in this transaction to ensure correctness.
    if task.filter.is_some() && source_chunk_compressed {
        let target_chunk_compressed = is_chunk_compressed(target_tx, &target_chunk).await?;
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

    let source_compressed_chunk = get_compressed_chunk(source_tx, &task.source_chunk).await?;

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
    let row = target_tx
        .query_one(
            r"
        SELECT (status & 8)::bool as is_partial
        FROM _timescaledb_catalog.chunk
        WHERE schema_name = $1
          AND table_name = $2",
            &[&target_chunk.schema, &target_chunk.table],
        )
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

async fn copy_chunk_data<S: QuotedName, T: QuotedName>(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_table: &S,
    target_table: &T,
    filter: &Option<String>,
    mode: CopyMode,
) -> Result<CopyResult> {
    debug!("Copying uncompressed chunk {}", source_table.quoted_name());

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_table.quoted_name()).await?;

    if let Some(filter) = filter {
        delete_data_using_filter(target_tx, target_table, filter).await?;
    } else {
        delete_all_rows_from_chunk(target_tx, &target_table.quoted_name()).await?;
    }

    let copy_result = copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_table.quoted_name(),
        &target_table.quoted_name(),
        filter,
        mode == UncompressedOnly,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_table.quoted_name()).await?;
    }
    debug!(
        "Finished copying uncompressed chunk {}. Starting analysis.",
        source_table.quoted_name()
    );

    target_tx
        .execute(&format!("analyze {}", target_table.quoted_name()), &[])
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
    let hypertable_id: i32 = tx
        .query_one(
            r"
        SELECT hypertable_id
        FROM _timescaledb_catalog.chunk
        -- Note: There is non-obvious stuff going on here.
        -- The ::text::regclass::text cast dance reformats the relation name to
        -- the database's canonical format. We _explicitly avoid_ casting the
        -- left hand side to regclass, because the _timescaledb_catalog.chunk
        -- table contains entries which refer to non-existent relations. This
        -- only happens when the hypertable has a continuous aggregate on it,
        -- and the chunk was dropped.
        -- Casting the non-existent relation to regclass throws an error.
        WHERE format('%I.%I', schema_name, table_name)::text = $1::text::regclass::text
    ",
            &[&chunk_name],
        )
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

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;

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

    let mut index_columns: Vec<String> = compression_settings
        .segmentby
        .clone()
        .iter()
        .map(|c| quote_ident(c))
        .collect();

    // Add min/max columns for orderby settings
    for (i, _) in compression_settings.orderby.iter().enumerate() {
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
        let min_column = quote_ident(&format!("_ts_meta_min_{}", column_index));
        let max_column = quote_ident(&format!("_ts_meta_max_{}", column_index));
        index_columns.push(format!("{min_column} {order_by}"));
        index_columns.push(format!("{max_column} {order_by}"));
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
        CompressionSettings {
            segmentby: row.get("segmentby"),
            orderby: row.get("orderby"),
            orderby_desc: row.get("orderby_desc"),
            orderby_nullsfirst: row.get("orderby_nullsfirst"),
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
    if source_settings != target_settings {
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

    Ok(target_settings)
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
    -- The numrows_{pre,post}_compression columns were introduced
    -- in TimescaleDB 2.0. The upgrade path does not populate default
    -- values for older installations.
    -- Ensure 0 is returned instead of NULL to avoid handling Option<>
    -- in the struct.
    COALESCE(numrows_pre_compression, 0) AS numrows_pre_compression,
    COALESCE(numrows_post_compression, 0) AS numrows_post_compression
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

pub async fn chunk_exists<T>(client: &T, chunk: &Chunk) -> Result<bool>
where
    T: GenericClient,
{
    let query: &str = r#"
SELECT EXISTS (
  SELECT 1
  FROM _timescaledb_catalog.chunk
  WHERE schema_name = $1
    AND table_name = $2
    AND dropped = false
)
"#;
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
