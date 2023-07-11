use crate::connect::{Source, Target};
use crate::prepare::Chunk;
use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt;
use futures_util::pin_mut;
use futures_util::SinkExt;
use tokio_postgres::types::private::BytesMut;
use tokio_postgres::{CopyInSink, Transaction};
use tracing::debug;

pub async fn copy_chunk(source: &mut Source, target: &mut Target, chunk: Chunk) -> Result<()> {
    let source_tx = source.transaction().await?;
    let target_tx = target.client.transaction().await?;

    let compressed_chunk = get_compressed_chunk(&source_tx, &chunk).await?;
    if compressed_chunk.is_none() {
        //create_compressed_chunk(target, &chunk).await?;
    }
    copy_uncompressed_chunk_data(&source_tx, &target_tx, &chunk).await?;
    if compressed_chunk.is_some() {
        let compressed_chunk = compressed_chunk.unwrap();
        copy_compressed_chunk_data(&source_tx, &target_tx, &compressed_chunk).await?;
    }
    target_tx.commit().await?;
    source_tx.commit().await?;
    Ok(())
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

async fn get_chunk_with_same_dimensions(
    tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<Option<Chunk>> {
    let row = tx
        .query_opt(
            r#"
    SELECT
          h.schema_name as hypertable_schema
        , h.table_name as hypertable_name
        , ch.schema_name as chunk_schema
        , ch.table_name as chunk_name
        , ds.range_start as dimension_start
        , ds.range_end as dimension_end
    FROM
         _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.chunk_constraint cons ON cons.chunk_id = ch.id
    JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cons.dimension_slice_id
    JOIN _timescaledb_catalog.hypertable h ON h.id = ch.hypertable_id
    WHERE h.schema_name = $1
      AND h.table_name = $2
      AND ds.range_start = $3
      AND ds.range_end = $4
      ;
    "#,
            &[
                &chunk.hypertable_schema,
                &chunk.hypertable_name,
                &chunk.dimension_start,
                &chunk.dimension_end,
            ],
        )
        .await?;
    Ok(row.map(|r| Chunk {
        hypertable_schema: r.get("hypertable_schema"),
        hypertable_name: r.get("hypertable_name"),
        chunk_schema: r.get("chunk_schema"),
        chunk_name: r.get("chunk_name"),
        dimension_start: r.get("dimension_start"),
        dimension_end: r.get("dimension_end"),
        active_chunk: false,
    }))
}

async fn copy_uncompressed_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    chunk: &Chunk,
) -> Result<()> {
    debug!("Copying uncompressed chunk");
    let target_chunk = get_chunk_with_same_dimensions(target_tx, chunk)
        .await?
        .unwrap();

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

    truncate_chunk(target_tx, &target_chunk_name).await?;
    copy_chunk_from_source_to_target(source_tx, target_tx, &source_chunk_name, &target_chunk_name)
        .await

    // TODO: handle current chunk is active chunk
}

async fn truncate_chunk(target_tx: &Transaction<'_>, chunk_name: &str) -> Result<()> {
    debug!("Truncating chunk {chunk_name}");
    target_tx
        .execute(&format!("TRUNCATE TABLE {chunk_name}"), &[])
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

    copy_chunk_from_source_to_target(source_tx, target_tx, &source_chunk_name, &target_chunk_name)
        .await
}

async fn copy_chunk_from_source_to_target(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    source_chunk_name: &str,
    target_chunk_name: &str,
) -> Result<()> {
    let copy_out =
        format!("COPY (SELECT * FROM ONLY {source_chunk_name}) TO STDOUT WITH (FORMAT BINARY)");
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
