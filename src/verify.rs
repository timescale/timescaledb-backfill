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
use crate::{execute::chunk_exists, task::VerifyTask};
use anyhow::{bail, Ok, Result};
use diffy::{create_patch, PatchFormatter};
use serde::Serialize;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::{collections::BTreeMap, fmt};
use tokio_postgres::Transaction;
use tracing::debug;

pub static FAILED_VERIFICATIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct VerificationError {
    message: String,
}

impl VerificationError {
    pub fn new(message: &str) -> Self {
        VerificationError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for VerificationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for VerificationError {}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    #[serde(default)]
    min: BTreeMap<String, Option<String>>,
    #[serde(default)]
    max: BTreeMap<String, Option<String>>,
    #[serde(default)]
    sum: BTreeMap<String, Option<f64>>,
    #[serde(default)]
    count: BTreeMap<String, Option<i128>>,
    #[serde(default)]
    total_count: i64,
}

impl TableSummary {
    pub fn new(
        min: BTreeMap<String, Option<String>>,
        max: BTreeMap<String, Option<String>>,
        sum: BTreeMap<String, Option<f64>>,
        count: BTreeMap<String, Option<i128>>,
        total_count: i64,
    ) -> Self {
        Self {
            min,
            max,
            sum,
            count,
            total_count,
        }
    }
}

impl PartialEq for TableSummary {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min
            && self.max == other.max
            && self.count == other.count
            && self.total_count == other.total_count
            && sum_eq(&self.sum, &other.sum, &self.count)
    }
}

fn sum_eq(
    a: &BTreeMap<String, Option<f64>>,
    b: &BTreeMap<String, Option<f64>>,
    sum: &BTreeMap<String, Option<i128>>,
) -> bool {
    let iter = a.iter().zip(b.iter());
    for ((a_key, a_value), (b_key, b_value)) in iter {
        if a_key != b_key {
            return false;
        }
        match (a_value, b_value, sum.get(a_key)) {
            (Some(a), Some(b), Some(count)) => {
                // The error for summing may compound to larger values. This isn't perfect,
                // but taking the max EPSILON prevents a lot of false positive hits.
                // At some point in all the json parsing and summing of these values,
                // they have been stored as a f32, so we take that as our minimal value.
                #[allow(clippy::cast_precision_loss)]
                let margin = f32::EPSILON * count.unwrap_or_default() as f32;
                if (a - b).abs() > f64::from(margin) {
                    return false;
                }
            }
            (None, None, _) => (),
            _ => return false,
        }
    }
    true
}

async fn fetch_summary_query_select_columns(
    target_tx: &Transaction<'_>,
    target_table: &str,
) -> Result<String> {
    static GET_VERIFY_QUERY_SELECT_COLUMNS: &str =
        include_str!("get_verify_query_select_columns.sql");

    let select_columns_row = target_tx
        .query_one(GET_VERIFY_QUERY_SELECT_COLUMNS, &[&target_table])
        .await?;

    let select_columns: &str = select_columns_row.get(0);
    Ok(select_columns.to_owned())
}

async fn fetch_summary(
    tx: &Transaction<'_>,
    table_name: &str,
    query_select_columns: &str,
    filter: &Option<String>,
) -> Result<TableSummary> {
    let summary_query = filter
        .as_ref()
        .map(|filter| format!("SELECT {query_select_columns} FROM {table_name} WHERE {filter}"))
        .unwrap_or(format!("SELECT {query_select_columns} FROM {table_name}"));
    let summary_row = tx.query_one(&summary_query, &[]).await?;

    let min: String = summary_row.get("min");
    let max: String = summary_row.get("max");
    let sum: String = summary_row.get("sum");
    let count: String = summary_row.get("count");
    let total_count: i64 = summary_row.get("total_count");
    let min = serde_json::from_str(&min).unwrap_or_default();
    let max = serde_json::from_str(&max).unwrap_or_default();
    let sum = serde_json::from_str(&sum).unwrap_or_default();
    let count = serde_json::from_str(&count).unwrap_or_default();

    Ok(TableSummary::new(min, max, sum, count, total_count))
}

pub async fn verify_chunk_data(
    source_tx: &Transaction<'_>,
    target_tx: &Transaction<'_>,
    verify_task: &VerifyTask,
) -> Result<()> {
    let source_chunk = &verify_task.source_chunk;
    let source_table = source_chunk.quoted_name();
    debug!("verifying data copy from source={source_table}");

    if !chunk_exists(source_tx, source_chunk).await? {
        bail!(VerificationError::new(&format!(
            "source chunk {source_table} no longer exists"
        )));
    }

    let target_chunk = verify_task
        .target_chunk
        .as_ref()
        .ok_or_else(|| VerificationError::new("target chunk does not exist"))?;
    let target_table = target_chunk.quoted_name();

    if !chunk_exists(target_tx, target_chunk).await? {
        bail!(VerificationError::new(&format!(
            "target chunk {target_table} no longer exists"
        ),));
    }

    let summary_query_select_columns =
        fetch_summary_query_select_columns(target_tx, &target_table).await?;

    let target_summary = fetch_summary(
        target_tx,
        &target_table,
        &summary_query_select_columns,
        &verify_task.filter,
    )
    .await?;

    let source_summary = fetch_summary(
        source_tx,
        &source_table,
        &summary_query_select_columns,
        &verify_task.filter,
    )
    .await?;

    if source_summary != target_summary {
        FAILED_VERIFICATIONS.fetch_add(1, Relaxed);
        let source_yaml = serde_yaml::to_string(&source_summary)?;
        let target_yaml = serde_yaml::to_string(&target_summary)?;
        let patch = create_patch(&source_yaml, &target_yaml);
        let f = PatchFormatter::new().with_color();
        let diff_msg = format!(
            "Chunk verification failed, source={source_table} target={target_table} diff\n```diff\n{}```",
            f.fmt_patch(&patch)
        );
        bail!(VerificationError::new(&diff_msg));
    }

    Ok(())
}
