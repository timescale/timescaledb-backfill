use crate::connect::{Source, Target};
use crate::storage::backfill_schema_exists;
use anyhow::{Context, Result};
use serde::Serialize;
use tokio::time::Duration;
use uuid::Uuid;

const METADATA_KEY: &str = "backfill_stats";

#[derive(Debug)]
struct Session {
    pub id: Uuid,
    pub created_at: String,
}

async fn fetch_session(target: &Target) -> Result<Option<Session>> {
    if !backfill_schema_exists(&target.client).await? {
        return Ok(None);
    }
    let query = "select id, created_at::text from __backfill.session";
    let row = target.client.query_one(query, &[]).await?;
    Ok(Some(Session {
        created_at: row.get("created_at"),
        id: row.get("id"),
    }))
}

async fn fetch_pg_version(source: &mut Source) -> anyhow::Result<String> {
    // We replicate the same way TimescaleDB does for determining Postgres version.
    // https://github.com/timescale/timescaledb/blob/fb65086b5542a871dc3d9757724e886dca904ef6/src/telemetry/telemetry.c#L552-L574
    let tx = source.transaction().await?;
    let pg_version = tx
        .query_one(
            "SELECT (current_setting('server_version_num')::int/10000 || '.' || current_setting('server_version_num')::int%100)::text;",
            &[],
        )
        .await?
        .get(0);
    tx.commit().await?;
    Ok(pg_version)
}

async fn fetch_tsdb_version(source: &mut Source) -> anyhow::Result<String> {
    let tx = source.transaction().await?;
    let tsdb_version = tx
        .query_one(
            "select extversion::text from pg_extension where extname = 'timescaledb'",
            &[],
        )
        .await?
        .get(0);
    tx.commit().await?;
    Ok(tsdb_version)
}

#[derive(Debug, Serialize)]
pub struct Telemetry {
    session_id: Option<Uuid>,
    session_created_at: Option<String>,
    version: String,
    command: String,
    debug_mode: bool,
    command_duration_secs: u64,
    source_db_pg_version: Option<String>,
    source_db_tsdb_version: Option<String>,

    refreshed_caggs: Option<usize>,
    staged_tasks: Option<usize>,
    copy_tasks_finished: Option<usize>,
    copy_tasks_total_bytes: Option<usize>,
    verify_tasks_finished: Option<usize>,
    verify_tasks_failures: Option<usize>,

    success: bool,
    error_reason: Option<String>,
    error_backtrace: Option<String>,
}

impl Telemetry {
    pub async fn from_target_session(target: &Target) -> Result<Self> {
        let session = fetch_session(target).await?;
        Ok(Self {
            session_id: session.as_ref().map(|s| s.id),
            session_created_at: session.map(|s| s.created_at),
            version: env!("CARGO_PKG_VERSION").to_string(),
            debug_mode: cfg!(debug_assertions),
            command: String::default(),
            command_duration_secs: 0,
            source_db_pg_version: None,
            source_db_tsdb_version: None,
            staged_tasks: None,
            copy_tasks_finished: None,
            copy_tasks_total_bytes: None,
            verify_tasks_finished: None,
            verify_tasks_failures: None,
            success: true,
            error_reason: None,
            error_backtrace: None,
            refreshed_caggs: None,
        })
    }

    pub async fn with_source_db(self, source: &mut Source) -> Telemetry {
        let pg_version = fetch_pg_version(source).await;
        let tsdb_version = fetch_tsdb_version(source).await;

        Telemetry {
            // At this point the command finished executing. We rather report
            // the available telemetry than abort because we couldn't connect
            // to source at this point.
            source_db_pg_version: pg_version.ok(),
            source_db_tsdb_version: tsdb_version.ok(),
            ..self
        }
    }

    pub fn with_error(self, error_reason: String, error_backtrace: String) -> Telemetry {
        Telemetry {
            error_reason: Some(error_reason),
            error_backtrace: Some(error_backtrace),
            success: false,
            ..self
        }
    }

    pub fn with_command(self, command: String) -> Telemetry {
        Telemetry { command, ..self }
    }

    pub fn with_command_duration(self, command_duration: Duration) -> Telemetry {
        Telemetry {
            command_duration_secs: command_duration.as_secs(),
            ..self
        }
    }

    pub fn with_copied_tasks(
        self,
        copy_tasks_finished: usize,
        copy_tasks_total_bytes: usize,
    ) -> Telemetry {
        Telemetry {
            copy_tasks_finished: Some(copy_tasks_finished),
            copy_tasks_total_bytes: Some(copy_tasks_total_bytes),
            ..self
        }
    }

    pub fn with_verified_tasks(
        self,
        verify_tasks_finished: usize,
        verify_tasks_failures: usize,
    ) -> Telemetry {
        Telemetry {
            verify_tasks_finished: Some(verify_tasks_finished),
            verify_tasks_failures: Some(verify_tasks_failures),
            ..self
        }
    }

    pub fn with_staged_tasks(self, staged_tasks: usize) -> Telemetry {
        Telemetry {
            staged_tasks: Some(staged_tasks),
            ..self
        }
    }

    pub fn with_refreshed_caggs(self, refreshed_caggs: usize) -> Telemetry {
        Telemetry {
            refreshed_caggs: Some(refreshed_caggs),
            ..self
        }
    }
}

pub async fn report(target: &Target, telemetry: &Telemetry) -> Result<()> {
    let data =
        serde_json::to_value(telemetry).with_context(|| "error converting data to JSON string")?;

    // Instead of checking explicit versions, we check if the
    // telemetry_event table exists and that we have access to it. If
    // not, we assume that it does not exist and write messages to
    // metadata instead.
    //
    // This might generate a strange error message if the user does
    // not have access to either table since it will say that the user
    // cannot insert into the metadata table when in actuality she do
    // not have access to either table.
    let has_telemetry_event_table: bool = target
        .client
            .query_one(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = '_timescaledb_catalog' AND tablename = 'telemetry_event')",
                &[],
            )
            .await?
            .get(0);

    if has_telemetry_event_table {
        target
            .client
            .execute(
                "INSERT INTO _timescaledb_catalog.telemetry_event(tag, body) VALUES ($1, $2)",
                &[&METADATA_KEY, &data],
            )
            .await
            .with_context(|| "error inserting into telemetry_event table")?;
        return Ok(());
    }

    let can_update_metadata: bool = target
        .client
        .query_one(
            "SELECT has_table_privilege('_timescaledb_catalog.metadata', 'UPDATE')",
            &[],
        )
        .await?
        .get(0);

    let query = if can_update_metadata {
        r"
        INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry)
        VALUES ($1, json_build_array($2::jsonb)::text, TRUE)
        ON CONFLICT (key)
        DO UPDATE SET value = (metadata.value::jsonb || EXCLUDED.value::jsonb)::text
        "
    } else {
        r"
        INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry)
        VALUES ($1, json_build_array($2::jsonb)::text, TRUE)
        ON CONFLICT (key)
        DO NOTHING
        "
    };

    target
        .client
        .execute(query, &[&METADATA_KEY, &data])
        .await
        .with_context(|| "error inserting into metadata table")?;

    Ok(())
}
