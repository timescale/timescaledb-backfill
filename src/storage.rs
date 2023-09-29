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
use crate::connect::Target;
use anyhow::Result;
use tokio_postgres::GenericClient;
use uuid::Uuid;

pub async fn backfill_schema_exists<T>(client: &T) -> Result<bool>
where
    T: GenericClient,
{
    let row = client
        .query_one(
            "select count(*) > 0 as schema_exists from pg_namespace where nspname = '__backfill'",
            &[],
        )
        .await?;
    Ok(row.get("schema_exists"))
}

pub async fn init_schema(target: &mut Target) -> Result<()> {
    let tx = target.client.transaction().await?;
    if !backfill_schema_exists(&tx).await? {
        static SCHEMA: &str = include_str!("schema.sql");
        tx.simple_query(SCHEMA).await?;
        // When support from PG12 is dropped we can just declare the column with
        // id uuid default gen_random_uuid()
        // and add the insert statement in `schema.sql` as
        // insert into __backfill.session default values;
        tx.query(
            "insert into __backfill.session(id) values($1)",
            &[&Uuid::new_v4()],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}
