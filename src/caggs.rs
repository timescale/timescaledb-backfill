// Copyright 2023 Timescale, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use crate::connect::{Source, Target};
use crate::sql::assert_regex;
use crate::timescale::{initialize_target_proc_schema, set_query_target_proc_schema};
use anyhow::{Context, Result};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

/// Refresh the continuous aggregates in `target` based on the watermark values
/// in `source`.
pub(crate) async fn refresh_caggs(
    source: &Source,
    target: &Target,
    filter: Option<&String>,
    cascade_up: bool,
    cascade_down: bool,
) -> Result<usize> {
    if let Some(filter) = filter {
        assert_regex(&source.client, filter).await?;
    }
    initialize_target_proc_schema(target).await?;
    let watermarks = get_watermarks(source, filter, cascade_up, cascade_down).await?;
    refresh_up_to_watermarks(target, watermarks).await
}

#[derive(Debug)]
struct Watermark {
    /// The schema that the continuous aggregate's view is in
    user_view_schema: String,
    /// The name of the continuous aggregate's view is in
    user_view_name: String,
    /// The value of the watermark in timescaledb's internal representation
    watermark: i64,
}

/// Run `refresh_continuous_aggregate` for all watermark values in `watermarks`
async fn refresh_up_to_watermarks(target: &Target, watermarks: Vec<Watermark>) -> Result<usize> {
    let (values_string, values) = build_values_list(&watermarks);
    let snippet = if has_watermark_table(&target.client).await? {
        "INNER JOIN _timescaledb_catalog.continuous_aggs_watermark w ON (c.mat_hypertable_id = w.mat_hypertable_id)"
    } else {
        "CROSS JOIN LATERAL _timescaledb_internal.cagg_watermark(c.mat_hypertable_id) w(watermark)"
    };
    let query = set_query_target_proc_schema(&format!(
        r"
        WITH RECURSIVE src AS (
                SELECT * FROM {values_string} as _(user_view_schema, user_view_name, watermark)
            ), caggs AS (
            SELECT
                0 as lvl
              , c.*
              , w.watermark as window_start
              , src.watermark as window_end
            FROM _timescaledb_catalog.continuous_agg c
            LEFT OUTER JOIN src ON (c.user_view_schema = src.user_view_schema AND c.user_view_name = src.user_view_name)
            {snippet}
            WHERE c.parent_mat_hypertable_id is null
            UNION ALL
            SELECT
                caggs.lvl + 1
              , c.*
              , w.watermark as window_start
              , src.watermark as window_end
            FROM caggs
            INNER JOIN _timescaledb_catalog.continuous_agg c ON (caggs.mat_hypertable_id = c.parent_mat_hypertable_id)
            LEFT OUTER JOIN src ON (c.user_view_schema = src.user_view_schema AND c.user_view_name = src.user_view_name)
            {snippet}
        )
        SELECT
              caggs.user_view_schema
            , caggs.user_view_name
            , CASE
                WHEN td.column_type = 'timestamp'::regtype then @extschema@.to_timestamp_without_timezone(caggs.window_start)::text
                WHEN td.column_type = 'timestamptz'::regtype then @extschema@.to_timestamp(caggs.window_start)::text
                WHEN td.column_type = 'date'::regtype then @extschema@.to_date(caggs.window_start)::text
                WHEN td.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) then caggs.window_start::text
              END as window_start
            , CASE
                WHEN td.column_type = 'timestamp'::regtype then @extschema@.to_timestamp_without_timezone(caggs.window_end)::text
                WHEN td.column_type = 'timestamptz'::regtype then @extschema@.to_timestamp(caggs.window_end)::text
                WHEN td.column_type = 'date'::regtype then @extschema@.to_date(caggs.window_end)::text
                WHEN td.column_type in ('bigint'::regtype, 'int'::regtype, 'smallint'::regtype) then caggs.window_end::text
              END as window_end
            , td.column_type::text
            FROM caggs
            INNER JOIN LATERAL
            (
                SELECT column_type
                FROM _timescaledb_catalog.dimension d
                WHERE d.hypertable_id = caggs.mat_hypertable_id
                ORDER BY d.id
                limit 1
            ) td ON (true)
            WHERE caggs.window_start IS NOT NULL
            AND caggs.window_end IS NOT NULL
            AND caggs.window_start < caggs.window_end
            ORDER BY caggs.lvl, caggs.window_start
    "
    ));
    let rows = target
        .client
        .query(&query, &values[..])
        .await
        .context("unable to get detailed refresh information")?;
    let refreshed_caggs = rows.len();
    for row in rows {
        let user_view_schema: String = row.get(0);
        let user_view_name: String = row.get(1);
        let window_start: String = row.get(2);
        let window_end: String = row.get(3);
        let column_type: String = row.get(4);
        println!("Refreshing continuous aggregate '{user_view_schema}'.'{user_view_name}' in range [{window_start}, {window_end})");
        target.client.execute(&format!("CALL refresh_continuous_aggregate(format('%I.%I', $1::text, $2::text)::regclass, $3::text::{column_type}, $4::text::{column_type})"), &[&user_view_schema, &user_view_name, &window_start, &window_end]).await?;
    }
    Ok(refreshed_caggs)
}

/// Builds a parameterized query string and parameter array for building a
/// VALUES list of `watermarks` of arbitrary length.
///
/// Given watermarks:
/// ```
///     vec![
///         Watermark{"foo", "bar", 1000},
///         Watermark{"public", "baz", 2000},
///     ]
/// ```
///
/// This function will return the tuple:
/// ```
///     (
///         "(VALUES ($1, $2, $3::bigint), ($4, $5, $6::bigint))",
///         vec!["foo", "bar", 1000, "public", "baz", 2000]
///     )
/// ```
///
fn build_values_list(watermarks: &[Watermark]) -> (String, Vec<&(dyn ToSql + Sync)>) {
    // This madness  is the recommended way to populate a VALUES list, see:
    // https://github.com/sfackler/rust-postgres/issues/336#issuecomment-379560207
    let mut counter = 1;
    let mut values: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let tuples: String = watermarks
        .iter()
        .map(|w| {
            values.push(&w.user_view_schema);
            values.push(&w.user_view_name);
            values.push(&w.watermark);
            let s = format!(
                r#"(${}, ${}, ${}::bigint)"#,
                counter,
                counter + 1,
                counter + 2
            );
            counter += 3;
            s
        })
        .collect::<Vec<_>>()
        .join(", ");
    let string = format!("(VALUES {tuples})");
    (string, values)
}

/// `get_watermarks` returns a `Watermark` for every continuous aggregate in
/// `source`.
async fn get_watermarks(
    source: &Source,
    filter: Option<&String>,
    cascade_up: bool,
    cascade_down: bool,
) -> Result<Vec<Watermark>> {
    let snippet = if has_watermark_table(&source.client).await? {
        "INNER JOIN _timescaledb_catalog.continuous_aggs_watermark w ON (c.mat_hypertable_id = w.mat_hypertable_id)"
    } else {
        "CROSS JOIN LATERAL _timescaledb_internal.cagg_watermark(c.mat_hypertable_id) w(watermark)"
    };

    let query = format!(
        r"
WITH RECURSIVE
    src AS (
        SELECT c.user_view_schema, c.user_view_name, w.watermark, c.mat_hypertable_id, c.raw_hypertable_id
        FROM _timescaledb_catalog.continuous_agg c
        {snippet}
        WHERE
            $1::text IS NULL
            OR format('%I.%I', c.user_view_schema, c.user_view_name) ~* $1
    )
    , up as
    (
        SELECT c.user_view_schema, c.user_view_name, w.watermark, c.mat_hypertable_id
        FROM src
        JOIN _timescaledb_catalog.continuous_agg c ON (src.mat_hypertable_id = c.raw_hypertable_id)
        {snippet}
        WHERE $1::text IS NOT NULL AND $2::bool -- cascade up?
        UNION
        SELECT c.user_view_schema, c.user_view_name, w.watermark, c.mat_hypertable_id
        FROM up
        INNER JOIN _timescaledb_catalog.continuous_agg c ON (up.mat_hypertable_id = c.raw_hypertable_id)
        {snippet}
    )
    , down as
    (
        SELECT c.user_view_schema, c.user_view_name, w.watermark, c.raw_hypertable_id
        FROM src
        INNER JOIN _timescaledb_catalog.continuous_agg c ON (src.raw_hypertable_id = c.mat_hypertable_id)
        {snippet}
        WHERE $1::text IS NOT NULL AND $3::bool -- cascade down ?
        UNION
        SELECT c.user_view_schema, c.user_view_name, w.watermark, c.raw_hypertable_id
        FROM down
        INNER JOIN _timescaledb_catalog.continuous_agg c ON (down.raw_hypertable_id = c.mat_hypertable_id)
        {snippet}
    )
    SELECT user_view_schema, user_view_name, watermark
    FROM src
    UNION
    SELECT user_view_schema, user_view_name, watermark
    FROM up
    UNION
    SELECT user_view_schema, user_view_name, watermark
    FROM down;"
    );

    let rows = source
        .client
        .query(&query, &[&filter, &cascade_up, &cascade_down])
        .await
        .context("get watermarks")?;
    Ok(rows
        .iter()
        .map(|r| Watermark {
            user_view_schema: r.get(0),
            user_view_name: r.get(1),
            watermark: r.get(2),
        })
        .collect())
}

/// In TimescaleDB 2.11 the concept of the watermark table was introduced for
/// continuous aggregates. `has_watermark_table` returns true if the watermark
/// table is present in the specified client.
async fn has_watermark_table(client: &Client) -> Result<bool> {
    let row = client
        .query_one(
            r"
        SELECT exists(
            SELECT 1
            FROM pg_class k
            INNER JOIN pg_namespace n ON (k.relnamespace = n.oid)
            WHERE n.nspname = '_timescaledb_catalog'
                AND k.relname = 'continuous_aggs_watermark'
        )
    ",
            &[],
        )
        .await
        .context("has watermark table query")?;
    Ok(row.get(0))
}
