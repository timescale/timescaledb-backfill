use crate::connect::{Source, Target};
use crate::timescale::Chunk;
use anyhow::{bail, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_lite::StreamExt;
use futures_util::pin_mut;
use futures_util::SinkExt;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{CopyInSink, Transaction};
use tracing::{debug, trace};

pub async fn copy_chunk(
    source: &mut Source,
    target: &mut Target,
    chunk: Chunk,
    until: &DateTime<Utc>,
) -> Result<()> {
    let source_tx = source.transaction().await?;
    let target_tx = target.client.transaction().await?;

    copy_uncompressed_chunk_data(&source_tx, &target_tx, &chunk, until).await?;

    let compressed_chunk = get_compressed_chunk(&source_tx, &chunk).await?;
    if compressed_chunk.is_some() {
        // TODO: create compressed chunk in target, if missing
        let compressed_chunk = compressed_chunk.unwrap();
        copy_compressed_chunk_data(&source_tx, &target_tx, &compressed_chunk).await?;
    }

    target_tx.commit().await?;
    source_tx.commit().await?;
    Ok(())
}

async fn copy_uncompressed_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    chunk: &Chunk,
    until: &DateTime<Utc>,
) -> Result<()> {
    debug!("Copying uncompressed chunk");
    let target_chunk = match get_chunk_with_same_dimensions(target_tx, chunk).await? {
        Some(target_chunk) => target_chunk,
        None => create_uncompressed_chunk(target_tx, chunk).await?,
    };

    let source_chunk_name = format!(
        "{}.{}",
        quote_ident(&chunk.chunk_schema),
        quote_ident(&chunk.chunk_name)
    );
    let target_chunk_name = format!(
        "{}.{}",
        quote_ident(&target_chunk.chunk_schema),
        quote_ident(&target_chunk.chunk_name)
    );

    let trigger_dropped = drop_invalidation_trigger(target_tx, &target_chunk_name).await?;
    if !chunk.active_chunk {
        delete_all_rows_from_chunk(target_tx, &target_chunk_name).await?;
    } else {
        delete_data_until(target_tx, &target_chunk_name, until).await?;
    }
    let until = if chunk.active_chunk {
        Some(until)
    } else {
        None
    };

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk_name,
        &target_chunk_name,
        until,
    )
    .await?;
    if trigger_dropped {
        create_invalidation_trigger(target_tx, &target_chunk_name).await?;
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
    chunk: &CompressedChunk,
) -> Result<()> {
    debug!("Copying compressed chunk");
    // TODO get actual compressed chunk name

    let source_chunk_name = format!(
        "{}.{}",
        quote_ident(&chunk.chunk_schema),
        quote_ident(&chunk.chunk_name)
    );
    let target_chunk_name = format!(
        "{}.{}",
        quote_ident(&chunk.chunk_schema),
        quote_ident(&chunk.chunk_name)
    );

    copy_chunk_from_source_to_target(
        source_tx,
        target_tx,
        &source_chunk_name,
        &target_chunk_name,
        None,
    )
    .await
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

/// `quote_ident` quotes the given value as an identifier (table, schema) safely for use in a `simple_query` call.
/// Implementation matches that of `quote_identifier` in ruleutils.c of the `PostgreSQL` code,
/// with `quote_all_identifiers` = true.
pub fn quote_ident(value: &str) -> String {
    let mut result = String::with_capacity(value.len() + 4);
    result.push('"');
    for c in value.chars() {
        if c == '"' {
            result.push(c);
        }
        result.push(c);
    }
    result.push('"');
    result
}

struct CompressedChunk {
    chunk_schema: String,
    chunk_name: String,
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
            &format!("{}.{}", chunk.hypertable_schema, chunk.hypertable_name,),
            &chunk.slices()?,
        ],
    )
    .await?;

    match get_chunk_with_same_dimensions(tx, chunk).await? {
        Some(target_chunk) => Ok(target_chunk),
        None => bail!("failed to create chunk in target"),
    }
}
