use crate::connect::{Source, Target};
use crate::timescale::{quote_table_name, Chunk, CompressedChunk, CompressionSize};
use anyhow::{bail, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_lite::StreamExt;
use futures_util::pin_mut;
use futures_util::SinkExt;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{CopyInSink, Transaction};
use tracing::{debug, trace};

const COMPRESS_TABLE_NAME_PREFIX: &str = "bf_";
const PG_NAME_DATA_LEN: usize = 64;

pub async fn copy_chunk(
    source: &mut Source,
    target: &mut Target,
    source_chunk: Chunk,
    until: &DateTime<Utc>,
) -> Result<()> {
    let source_tx = source.transaction().await?;
    let target_tx = target.client.transaction().await?;

    let target_chunk = match get_chunk_with_same_dimensions(&target_tx, &source_chunk).await? {
        Some(target_chunk) => target_chunk,
        None => create_uncompressed_chunk(&target_tx, &source_chunk).await?,
    };

    copy_uncompressed_chunk_data(&source_tx, &target_tx, &source_chunk, &target_chunk, until)
        .await?;

    if let Some(source_compressed_chunk) = get_compressed_chunk(&source_tx, &source_chunk).await? {
        if let Some(target_compressed_chunk) =
            get_compressed_chunk(&target_tx, &target_chunk).await?
        {
            copy_compressed_chunk_data(
                &source_tx,
                &target_tx,
                &source_compressed_chunk,
                &target_compressed_chunk,
            )
            .await?;
        } else {
            let target_compressed_chunk_data_table = create_compressed_chunk_data_table(
                &target_tx,
                &target_chunk,
                &source_compressed_chunk.chunk_schema,
                &source_compressed_chunk.chunk_name,
            )
            .await?;
            copy_compressed_chunk_data(
                &source_tx,
                &target_tx,
                &source_compressed_chunk,
                &target_compressed_chunk_data_table,
            )
            .await?;
            create_compressed_chunk_from_data_table(
                &source_tx,
                &target_tx,
                &source_compressed_chunk,
                &target_chunk,
                &target_compressed_chunk_data_table,
            )
            .await?;
        };
    }

    target_tx.commit().await?;
    source_tx.commit().await?;
    Ok(())
}

async fn copy_uncompressed_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk: &Chunk,
    target_chunk: &Chunk,
    until: &DateTime<Utc>,
) -> Result<()> {
    debug!("Copying uncompressed chunk");

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;

    if !target_chunk.active_chunk {
        delete_all_rows_from_chunk(target_tx, &target_chunk.quoted_name()).await?;
    } else {
        delete_data_until(target_tx, &target_chunk.quoted_name(), until).await?;
    }

    let until = if target_chunk.active_chunk {
        Some(until)
    } else {
        None
    };

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk.quoted_name(),
        &target_chunk.quoted_name(),
        until,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }
    Ok(())
}

async fn delete_data_until(
    tx: &Transaction<'_>,
    chunk_name: &str,
    until: &DateTime<Utc>,
) -> Result<()> {
    debug!("Deleting rows from chunk {chunk_name} until {until}");
    let rows = tx
        .execute(
            // TODO: handle the case when the time dimension is not called `time`
            &format!("DELETE FROM {chunk_name} WHERE time < $1"),
            &[&until],
        )
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
            WHERE t.tgname = 'ts_cagg_invalidation_trigger'
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
    debug!("Copying compressed chunk");

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk.quoted_name(),
        &target_chunk.quoted_name(),
        None,
    )
    .await?;

    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk.quoted_name()).await?;
    }

    Ok(())
}

async fn copy_chunk_from_source_to_target(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk_name: &str,
    target_chunk_name: &str,
    until: Option<&DateTime<Utc>>,
) -> Result<()> {
    let copy_out = until.map(|until| {
        // Unfortunately we can't use binds in the statement to a COPY, so we
        // must manually convert our DateTime to a timestamptz.
        // TODO: handle the case when the time dimension is not called `time`
        format!("COPY (SELECT * FROM ONLY {source_chunk_name} WHERE time < '{}'::timestamptz) TO STDOUT WITH (FORMAT BINARY)", until.to_rfc3339())
    }).unwrap_or(
        format!("COPY (SELECT * FROM ONLY {source_chunk_name}) TO STDOUT WITH (FORMAT BINARY)")
    );

    debug!("{copy_out}");

    let copy_in = format!("COPY {target_chunk_name} FROM STDIN WITH (FORMAT BINARY)");
    debug!("{copy_in}");

    let stream = source_tx.copy_out(&copy_out).await?;
    let sink: CopyInSink<Bytes> = target_tx.copy_in(&copy_in).await?;
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

    debug!("Copied {rows} for table {source_chunk_name}",);

    Ok(())
}

async fn get_compressed_chunk(
    source_tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<Option<CompressedChunk>> {
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
            &[&chunk.chunk_schema, &chunk.chunk_name],
        )
        .await?;
    Ok(row.map(|r| CompressedChunk {
        chunk_schema: r.get("schema_name"),
        chunk_name: r.get("table_name"),
    }))
}

/// Fetches a Chunk in the DB that matches the given Chunk's Hypertable and
/// dimensions.
///
/// We can't rely on matching the Chunk's ID because there's no guarantee that
/// the name or ID hasn't been assigned to a new Chunk with a different
/// dimension range.
async fn get_chunk_with_same_dimensions(
    tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<Option<Chunk>> {
    let row = tx
        .query_opt(
            r#"
with
    chunk_dimensions as (
        select
            h.schema_name as hypertable_schema,
            h.table_name as hypertable_name,
            ch.schema_name as chunk_schema,
            ch.table_name as chunk_name,
            jsonb_agg(
                jsonb_build_object(
                    'column_name',
                    di.column_name,
                    'column_type',
                    di.column_type,
                    'range_start',
                    ds.range_start,
                    'range_end',
                    ds.range_end
                )
                order by di.id
            ) dimensions
        from _timescaledb_catalog.chunk ch
        join _timescaledb_catalog.chunk_constraint cons on cons.chunk_id = ch.id
        join _timescaledb_catalog.dimension_slice ds on ds.id = cons.dimension_slice_id
        join _timescaledb_catalog.hypertable h on h.id = ch.hypertable_id
        join
            _timescaledb_catalog.dimension di
            on h.id = di.hypertable_id
            and di.id = ds.dimension_id
        where h.schema_name = $1 and h.table_name = $2
        group by 1, 2, 3, 4
    )
select *
from chunk_dimensions
where dimensions = $3::TEXT::JSONB
"#,
            &[
                &chunk.hypertable_schema,
                &chunk.hypertable_name,
                &chunk.dimension_json()?,
            ],
        )
        .await?;

    trace!("chunk match {row:?}");
    row.map_or(Ok(None), |r| {
        Ok(Some(Chunk {
            hypertable_schema: r.get("hypertable_schema"),
            hypertable_name: r.get("hypertable_name"),
            chunk_schema: r.get("chunk_schema"),
            chunk_name: r.get("chunk_name"),
            dimensions: serde_json::from_value(r.get("dimensions"))?,
            active_chunk: false,
        }))
    })
}

/// Creates a Chunk in the same Hypertable and with the same slices as the
/// given Chunk.
async fn create_uncompressed_chunk(tx: &Transaction<'_>, chunk: &Chunk) -> Result<Chunk> {
    trace!(
        "creating uncompressed chunk from {:?} with slices {}",
        chunk,
        chunk.slices()?,
    );

    tx.execute(
        r#"
SELECT _timescaledb_internal.create_chunk(
    $1::text::regclass,
    slices => $2::TEXT::JSONB
)
"#,
        &[
            &format!("{}.{}", chunk.hypertable_schema, chunk.hypertable_name),
            &chunk.slices()?,
        ],
    )
    .await?;

    match get_chunk_with_same_dimensions(tx, chunk).await? {
        Some(target_chunk) => Ok(target_chunk),
        None => bail!("failed to create chunk in target"),
    }
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
    uncompressed_chunk: &Chunk,
    chunk_schema: &str,
    chunk_name: &str,
) -> Result<CompressedChunk> {
    let chunk_name = add_backfill_prefix(chunk_name);
    let data_table_name = quote_table_name(chunk_schema, &chunk_name);
    let parent_table_name = compressed_hypertable_name(
        tx,
        &uncompressed_chunk.hypertable_schema,
        &uncompressed_chunk.hypertable_name,
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

    Ok(CompressedChunk {
        chunk_schema: chunk_schema.into(),
        chunk_name,
    })
}

/// Adds the backfill prefix `COMPRESS_TABLE_NAME_PREFIX` to the table name.
///
/// If adding the prefix exceeds the Postgres limit for identifiers, the table
/// name is truncated by removing characters from the beginning instead of the
/// end. This is done to preserve the chunk identifiers at the end of the table
/// name, ensuring uniqueness.
fn add_backfill_prefix(table_name: &str) -> String {
    let table_name = if table_name.len() + COMPRESS_TABLE_NAME_PREFIX.len() >= PG_NAME_DATA_LEN {
        &table_name[COMPRESS_TABLE_NAME_PREFIX.len()..]
    } else {
        table_name
    };

    format!("bf_{}", table_name)
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
    source_compressed_chunk: &CompressedChunk,
    target_chunk: &Chunk,
    target_data_table: &CompressedChunk,
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
    compressed_chunk: &CompressedChunk,
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
            &[&compressed_chunk.chunk_schema, &compressed_chunk.chunk_name],
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
